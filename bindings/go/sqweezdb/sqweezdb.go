// Package sqweezdb is a thin Go binding over the C ABI of the sqweezdb engine (one process owns a database directory).
package sqweezdb

/*
#cgo LDFLAGS: -L${SRCDIR}/../../../target/release -lsqweezdb -Wl,-rpath,${SRCDIR}/../../../target/release
#include <stdlib.h>
#include <stddef.h>
typedef struct Db Db; typedef struct Stmt Stmt; typedef struct Sess Sess; typedef struct Rows Rows;
typedef struct { int tag; long long i; double f; const unsigned char* ptr; size_t len; } CValue;
Db* sqweezdb_open(const char* path, int durable); Db* sqweezdb_open_with(const char* path, const char* json); const char* sqweezdb_open_error(); void sqweezdb_close(Db*); const char* sqweezdb_last_error(Db*);
Sess* sqweezdb_session(Db*); void sqweezdb_session_free(Sess*); int sqweezdb_login(Db*, Sess*, const char*);
Stmt* sqweezdb_prepare(Db*, const char*); void sqweezdb_finalize(Stmt*);
Rows* sqweezdb_exec(Db*, Sess*, Stmt*, const CValue*, int); Rows* sqweezdb_exec_text(Db*, Sess*, const char*, const CValue*, int);
int sqweezdb_rows_count(Rows*); int sqweezdb_rows_cols(Rows*); int sqweezdb_rows_get(Rows*, int, int, CValue*); int sqweezdb_rows_all(Rows*, CValue*, int); void sqweezdb_rows_free(Rows*);
long long sqweezdb_snapshot(Db*);
typedef struct Sub Sub; Sub* sqweezdb_subscribe(Db*, Sess*, const char*, const CValue*, int); int sqweezdb_sub_next(Sub*, int, Rows**); void sqweezdb_sub_free(Sub*);
*/
import "C"
import (
	"encoding/json"
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

// Options mirror the engine's configuration keys (see src/config.rs). Durations are strings like "500ms", "1s", "2m";
// sizes like "256MB". Zero values are omitted, so an empty Options means the defaults.
type Options struct {
	ConfigFile string    `json:"configFile,omitempty"` // a TOML file with these keys, read first and overridden by the options
	Durable    any       `json:"durable,omitempty"`    // false, true, or "cloud"
	Create     *bool     `json:"create,omitempty"`
	Backup     *Backup   `json:"backup,omitempty"`
	Follow     *Follow   `json:"follow,omitempty"`
	Snapshot   *Snapshot `json:"snapshot,omitempty"`
	Listen     *Listen   `json:"listen,omitempty"`
	Live       *Live     `json:"live,omitempty"`
	Log        *Log      `json:"log,omitempty"`
}
type Backup struct {
	Endpoint     string `json:"endpoint,omitempty"`
	Bucket       string `json:"bucket,omitempty"`
	Prefix       string `json:"prefix,omitempty"`
	Region       string `json:"region,omitempty"`
	AccessKey    string `json:"accessKey,omitempty"`
	SecretKey    string `json:"secretKey,omitempty"`
	Passphrase   string `json:"passphrase,omitempty"`
	Interval     string `json:"interval,omitempty"`
	Compress     *bool  `json:"compress,omitempty"`
	CompactAfter string `json:"compactAfter,omitempty"`
	CompactEvery string `json:"compactEvery,omitempty"`
	Bootstrap    bool   `json:"bootstrap,omitempty"`
}
type Follow struct {
	Dir    string  `json:"dir,omitempty"`
	Bucket *Backup `json:"bucket,omitempty"`
	Poll   string  `json:"poll,omitempty"`
}
type Snapshot struct {
	LogBytes  any `json:"logBytes,omitempty"`  // "256MB" or bytes; 0 disables
	IdleAfter any `json:"idleAfter,omitempty"` // "30s" or milliseconds; 0 disables
}
type Listen struct {
	Socket string `json:"socket,omitempty"`
	TCP    string `json:"tcp,omitempty"`
	HTTP   string `json:"http,omitempty"` // the web console, served from this process; needs Token
	Token  string `json:"token,omitempty"`
}
type Live struct {
	Debounce string `json:"debounce,omitempty"`
	Queue    int    `json:"queue,omitempty"`
}
type Log struct {
	ThreadsIdleAfter string `json:"threadsIdleAfter,omitempty"`
}

// Open opens or creates the database directory; durable waits for the disk on every commit.
func Open(path string, durable bool) (*DB, error) { return OpenWith(path, Options{Durable: durable}) }

// OpenWith opens or creates the database directory with the given options. If ConfigFile names a TOML file it is read
// first and the options win. Nothing is read from the data directory itself. An existing directory is never wiped.
func OpenWith(path string, opts Options) (*DB, error) {
	js, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	cpath := C.CString(path)
	cjson := C.CString(string(js))
	defer C.free(unsafe.Pointer(cpath))
	defer C.free(unsafe.Pointer(cjson))
	p := C.sqweezdb_open_with(cpath, cjson)
	if p == nil {
		return nil, errors.New(C.GoString(C.sqweezdb_open_error()))
	}
	return &DB{p: p}, nil
}
func (db *DB) Close()            { C.sqweezdb_close(db.p) }
func (db *DB) Snapshot() int64   { return int64(C.sqweezdb_snapshot(db.p)) }
func (db *DB) lastError() error  { return errors.New(C.GoString(C.sqweezdb_last_error(db.p))) }
func (db *DB) Session() *Session { return &Session{db: db, p: C.sqweezdb_session(db.p)} }
func (s *Session) Close()        { C.sqweezdb_session_free(s.p) }
func (db *DB) Prepare(sql string) (*Stmt, error) {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	p := C.sqweezdb_prepare(db.p, cs)
	if p == nil {
		return nil, db.lastError()
	}
	return &Stmt{p: p}, nil
}
func (st *Stmt) Close() { C.sqweezdb_finalize(st.p) }
func (s *Session) Login(user string) error {
	cu := C.CString(user)
	defer C.free(unsafe.Pointer(cu))
	if C.sqweezdb_login(s.db.p, s.p, cu) != 0 {
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
	n, m := int(C.sqweezdb_rows_count(r)), int(C.sqweezdb_rows_cols(r))
	out := make([][]any, n)
	if n == 0 || m == 0 {
		C.sqweezdb_rows_free(r)
		return out
	}
	flat := make([]C.CValue, n*m)
	C.sqweezdb_rows_all(r, &flat[0], C.int(len(flat)))
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
	C.sqweezdb_rows_free(r)
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
	r := C.sqweezdb_exec(s.db.p, s.p, st.p, vp, C.int(len(vals)))
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
	r := C.sqweezdb_exec_text(s.db.p, s.p, cs, vp, C.int(len(vals)))
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
	p := C.sqweezdb_subscribe(s.db.p, s.p, cs, vp, C.int(len(vals)))
	runtime.KeepAlive(keep)
	if p == nil {
		return nil, s.db.lastError()
	}
	return &Subscription{db: s.db, p: p}, nil
}

// Next blocks up to timeout for an event; it returns (nil, nil) on timeout and an error when the subscription was dropped.
func (sub *Subscription) Next(timeoutMs int) ([]any, error) {
	var r *C.Rows
	switch C.sqweezdb_sub_next(sub.p, C.int(timeoutMs), &r) {
	case 0:
		rows := collect(r)
		return rows[0], nil
	case 1:
		return nil, nil
	default:
		return nil, errors.New("sqweezdb: subscription fell behind and was dropped; resubscribe")
	}
}
func (sub *Subscription) Close() { C.sqweezdb_sub_free(sub.p) }
