// Package dblab is a thin Go binding over the C ABI of the dblab engine (one process owns a database directory).
package dblab

/*
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -ldblab -Wl,-rpath,${SRCDIR}/../../../target/release
#include <stdlib.h>
#include <stddef.h>
typedef struct Db Db; typedef struct Stmt Stmt; typedef struct Sess Sess; typedef struct Rows Rows;
typedef struct { int tag; long long i; double f; const unsigned char* ptr; size_t len; } CValue;
Db* dblab_open(const char* path, int durable); void dblab_close(Db*); const char* dblab_last_error(Db*);
Sess* dblab_session(Db*); void dblab_session_free(Sess*); int dblab_login(Db*, Sess*, const char*);
Stmt* dblab_prepare(Db*, const char*); void dblab_finalize(Stmt*);
Rows* dblab_exec(Db*, Sess*, Stmt*, const CValue*, int); Rows* dblab_exec_text(Db*, Sess*, const char*, const CValue*, int);
int dblab_rows_count(Rows*); int dblab_rows_cols(Rows*); int dblab_rows_get(Rows*, int, int, CValue*); int dblab_rows_all(Rows*, CValue*, int); void dblab_rows_free(Rows*);
long long dblab_snapshot(Db*);
typedef struct Sub Sub; Sub* dblab_subscribe(Db*, Sess*, const char*, const CValue*, int); int dblab_sub_next(Sub*, int, Rows**); void dblab_sub_free(Sub*);
*/
import "C"
import (
	"errors"
	"runtime"
	"unsafe"
)

type DB struct{ p *C.Db }
type Stmt struct{ p *C.Stmt }
type Session struct {
	db *DB
	p  *C.Sess
}

func Open(path string, durable bool) (*DB, error) {
	cp := C.CString(path)
	defer C.free(unsafe.Pointer(cp))
	d := 0
	if durable {
		d = 1
	}
	p := C.dblab_open(cp, C.int(d))
	if p == nil {
		return nil, errors.New("dblab: open failed")
	}
	return &DB{p: p}, nil
}
func (db *DB) Close()            { C.dblab_close(db.p) }
func (db *DB) Snapshot() int64   { return int64(C.dblab_snapshot(db.p)) }
func (db *DB) lastError() error  { return errors.New(C.GoString(C.dblab_last_error(db.p))) }
func (db *DB) Session() *Session { return &Session{db: db, p: C.dblab_session(db.p)} }
func (s *Session) Close()        { C.dblab_session_free(s.p) }
func (db *DB) Prepare(sql string) (*Stmt, error) {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	p := C.dblab_prepare(db.p, cs)
	if p == nil {
		return nil, db.lastError()
	}
	return &Stmt{p: p}, nil
}
func (st *Stmt) Close() { C.dblab_finalize(st.p) }
func (s *Session) Login(user string) error {
	cu := C.CString(user)
	defer C.free(unsafe.Pointer(cu))
	if C.dblab_login(s.db.p, s.p, cu) != 0 {
		return s.db.lastError()
	}
	return nil
}

// marshal Go arguments into CValues; strings and byte slices point at Go memory, pinned for the duration of the call.
func args(a []any, pin *runtime.Pinner) ([]C.CValue, []any) {
	vals := make([]C.CValue, len(a))
	for i, v := range a {
		switch x := v.(type) {
		case nil:
			vals[i].tag = 0
		case int:
			vals[i].tag, vals[i].i = 1, C.longlong(x)
		case int64:
			vals[i].tag, vals[i].i = 1, C.longlong(x)
		case float64:
			vals[i].tag, vals[i].f = 2, C.double(x)
		case bool:
			vals[i].tag = 5
			if x {
				vals[i].i = 1
			}
		case string:
			vals[i].tag, vals[i].len = 3, C.size_t(len(x))
			if len(x) > 0 {
				p := unsafe.StringData(x)
				pin.Pin(p)
				vals[i].ptr = (*C.uchar)(unsafe.Pointer(p))
			}
		case []byte:
			vals[i].tag, vals[i].len = 4, C.size_t(len(x))
			if len(x) > 0 {
				pin.Pin(&x[0])
				vals[i].ptr = (*C.uchar)(unsafe.Pointer(&x[0]))
			}
		}
	}
	return vals, a
}
func collect(r *C.Rows) [][]any {
	n, m := int(C.dblab_rows_count(r)), int(C.dblab_rows_cols(r))
	out := make([][]any, n)
	if n == 0 || m == 0 {
		C.dblab_rows_free(r)
		return out
	}
	flat := make([]C.CValue, n*m)
	C.dblab_rows_all(r, &flat[0], C.int(len(flat)))
	for i := 0; i < n; i++ {
		row := make([]any, m)
		for j := 0; j < m; j++ {
			cv := flat[i*m+j]
			switch cv.tag {
			case 1:
				row[j] = int64(cv.i)
			case 2:
				row[j] = float64(cv.f)
			case 3:
				row[j] = C.GoStringN((*C.char)(unsafe.Pointer(cv.ptr)), C.int(cv.len))
			case 4:
				row[j] = C.GoBytes(unsafe.Pointer(cv.ptr), C.int(cv.len))
			case 5:
				row[j] = cv.i != 0
			}
		}
		out[i] = row
	}
	C.dblab_rows_free(r)
	return out
}

// Exec runs a prepared statement and returns all rows (empty for writes).
func (s *Session) Exec(st *Stmt, a ...any) ([][]any, error) {
	var pin runtime.Pinner
	defer pin.Unpin()
	vals, keep := args(a, &pin)
	var vp *C.CValue
	if len(vals) > 0 {
		pin.Pin(&vals[0])
		vp = &vals[0]
	}
	r := C.dblab_exec(s.db.p, s.p, st.p, vp, C.int(len(vals)))
	runtime.KeepAlive(keep)
	if r == nil {
		return nil, s.db.lastError()
	}
	return collect(r), nil
}

// ExecText runs SQL text (DDL, admin statements, one-off queries).
func (s *Session) ExecText(sql string, a ...any) ([][]any, error) {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	var pin runtime.Pinner
	defer pin.Unpin()
	vals, keep := args(a, &pin)
	var vp *C.CValue
	if len(vals) > 0 {
		pin.Pin(&vals[0])
		vp = &vals[0]
	}
	r := C.dblab_exec_text(s.db.p, s.p, cs, vp, C.int(len(vals)))
	runtime.KeepAlive(keep)
	if r == nil {
		return nil, s.db.lastError()
	}
	return collect(r), nil
}

// Subscription delivers the changes a SELECT would see, as rows [op, table, key, columns...]; op is 1 insert, 2 update, 3 delete.
type Subscription struct {
	db *DB
	p  *C.Sub
}

func (s *Session) Subscribe(sql string, a ...any) (*Subscription, error) {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	var pin runtime.Pinner
	defer pin.Unpin()
	vals, keep := args(a, &pin)
	var vp *C.CValue
	if len(vals) > 0 {
		pin.Pin(&vals[0])
		vp = &vals[0]
	}
	p := C.dblab_subscribe(s.db.p, s.p, cs, vp, C.int(len(vals)))
	runtime.KeepAlive(keep)
	if p == nil {
		return nil, s.db.lastError()
	}
	return &Subscription{db: s.db, p: p}, nil
}

// Next blocks up to timeout for an event; it returns (nil, nil) on timeout and an error when the subscription was dropped.
func (sub *Subscription) Next(timeoutMs int) ([]any, error) {
	var r *C.Rows
	switch C.dblab_sub_next(sub.p, C.int(timeoutMs), &r) {
	case 0:
		rows := collect(r)
		return rows[0], nil
	case 1:
		return nil, nil
	default:
		return nil, errors.New("dblab: subscription fell behind and was dropped; resubscribe")
	}
}
func (sub *Subscription) Close() { C.dblab_sub_free(sub.p) }
