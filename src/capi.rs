//! C ABI for language bindings (Go via cgo, Python via PyO3/ctypes, Node via N-API or FFI). Cursor-free: a statement is
//! prepared once, executed with an array of values, and returns a result handle whose cells are read by index.
use crate::sql::{Prepared, Session, Sql};
use crate::table::Value;
use std::ffi::{c_char, c_int, CStr, CString};
use std::sync::Arc;

pub struct Db { sql: Arc<Sql>, err: std::sync::Mutex<CString> }
pub struct Stmt { p: Arc<Prepared> }
pub struct Sess { s: Session, err: CString }
pub struct Rows { rows: Vec<Vec<Value>>, strings: Vec<CString> }

/// A value crossing the boundary: tag 0 NULL, 1 INT (i), 2 DOUBLE (f), 3 TEXT (ptr,len), 4 BYTES (ptr,len), 5 BOOL (i).
#[repr(C)] #[derive(Clone, Copy)] pub struct CValue { pub tag: c_int, pub i: i64, pub f: f64, pub ptr: *const u8, pub len: usize }

fn set_err(db: &Db, e: String) { *db.err.lock().unwrap() = CString::new(e.replace('\0', " ")).unwrap(); }
unsafe fn cstr(p: *const c_char) -> String { if p.is_null() { String::new() } else { CStr::from_ptr(p).to_string_lossy().into_owned() } }
fn to_value(v: &CValue) -> Value { unsafe { match v.tag { 1 => Value::Int(v.i), 2 => Value::Float(v.f), 3 => Value::Text(String::from_utf8_lossy(std::slice::from_raw_parts(v.ptr, v.len)).into_owned()), 4 => Value::Bytes(std::slice::from_raw_parts(v.ptr, v.len).to_vec()), 5 => Value::Bool(v.i != 0), _ => Value::Null } } }

/// Open (or create) a database directory. `durable` != 0 waits for the disk on every commit. Null on failure.
#[unsafe(no_mangle)] pub unsafe extern "C" fn dblab_open(path: *const c_char, durable: c_int) -> *mut Db {
    let path = std::path::PathBuf::from(unsafe { cstr(path) });
    let sql = if path.join("snap").exists() || !crate::table::GLog::list_segments(&path).is_empty() { match Sql::open(&path, durable != 0, 4) { Ok((s, _)) => s, Err(_) => return std::ptr::null_mut() } } else { Sql::create(&path, durable != 0) };
    Box::into_raw(Box::new(Db { sql: Arc::new(sql), err: std::sync::Mutex::new(CString::default()) }))
}
#[unsafe(no_mangle)] pub unsafe extern "C" fn dblab_close(db: *mut Db) { if !db.is_null() { drop(unsafe { Box::from_raw(db) }); } }
/// The last error message of this database handle (valid until the next call).
#[unsafe(no_mangle)] pub unsafe extern "C" fn dblab_last_error(db: *mut Db) -> *const c_char { let db = unsafe { &*db }; let g = db.err.lock().unwrap(); g.as_ptr() }
#[unsafe(no_mangle)] pub unsafe extern "C" fn dblab_session(_db: *mut Db) -> *mut Sess { Box::into_raw(Box::new(Sess { s: Session::default(), err: CString::default() })) }
#[unsafe(no_mangle)] pub unsafe extern "C" fn dblab_session_free(s: *mut Sess) { if !s.is_null() { drop(unsafe { Box::from_raw(s) }); } }
/// Bind a session to a user (access control). 0 on success.
#[unsafe(no_mangle)] pub unsafe extern "C" fn dblab_login(db: *mut Db, s: *mut Sess, user: *const c_char) -> c_int { let (db, s) = unsafe { (&*db, &mut *s) }; match db.sql.login(&mut s.s, &unsafe { cstr(user) }) { Ok(()) => 0, Err(e) => { set_err(db, e); 1 } } }
/// Prepare a statement; null on error (see dblab_last_error).
#[unsafe(no_mangle)] pub unsafe extern "C" fn dblab_prepare(db: *mut Db, sql: *const c_char) -> *mut Stmt { let db = unsafe { &*db }; match db.sql.prepare(&unsafe { cstr(sql) }) { Ok(p) => Box::into_raw(Box::new(Stmt { p })), Err(e) => { set_err(db, e); std::ptr::null_mut() } } }
#[unsafe(no_mangle)] pub unsafe extern "C" fn dblab_finalize(st: *mut Stmt) { if !st.is_null() { drop(unsafe { Box::from_raw(st) }); } }
/// Execute a prepared statement with `n` parameters. Returns a result handle (possibly with zero rows) or null on error.
#[unsafe(no_mangle)] pub unsafe extern "C" fn dblab_exec(db: *mut Db, s: *mut Sess, st: *mut Stmt, params: *const CValue, n: c_int) -> *mut Rows {
    let (db, s, st) = unsafe { (&*db, &mut *s, &*st) };
    let params: Vec<Value> = if n > 0 { unsafe { std::slice::from_raw_parts(params, n as usize) }.iter().map(to_value).collect() } else { vec![] };
    match db.sql.execute_s(&mut s.s, &st.p, &params) { Ok(rows) => Box::into_raw(Box::new(Rows { rows, strings: vec![] })), Err(e) => { set_err(db, e.clone()); s.err = CString::new(e.replace('\0', " ")).unwrap(); std::ptr::null_mut() } }
}
/// Execute SQL text directly (admin statements, DDL, one-off queries).
#[unsafe(no_mangle)] pub unsafe extern "C" fn dblab_exec_text(db: *mut Db, s: *mut Sess, sql: *const c_char, params: *const CValue, n: c_int) -> *mut Rows {
    let (db, s) = unsafe { (&*db, &mut *s) };
    let params: Vec<Value> = if n > 0 { unsafe { std::slice::from_raw_parts(params, n as usize) }.iter().map(to_value).collect() } else { vec![] };
    match db.sql.exec_s(&mut s.s, &unsafe { cstr(sql) }, &params) { Ok(rows) => Box::into_raw(Box::new(Rows { rows, strings: vec![] })), Err(e) => { set_err(db, e); std::ptr::null_mut() } }
}
#[unsafe(no_mangle)] pub unsafe extern "C" fn dblab_rows_count(r: *mut Rows) -> c_int { unsafe { &*r }.rows.len() as c_int }
#[unsafe(no_mangle)] pub unsafe extern "C" fn dblab_rows_cols(r: *mut Rows) -> c_int { let r = unsafe { &*r }; r.rows.first().map(|x| x.len()).unwrap_or(0) as c_int }
/// Read one cell. TEXT/BYTES pointers stay valid until the result is freed.
#[unsafe(no_mangle)] pub unsafe extern "C" fn dblab_rows_get(r: *mut Rows, row: c_int, col: c_int, out: *mut CValue) -> c_int {
    let r = unsafe { &mut *r }; let Some(v) = r.rows.get(row as usize).and_then(|x| x.get(col as usize)) else { return 1 };
    let o = unsafe { &mut *out }; *o = CValue { tag: 0, i: 0, f: 0.0, ptr: std::ptr::null(), len: 0 };
    match v { Value::Null => {}, Value::Int(i) => { o.tag = 1; o.i = *i; } Value::Float(f) => { o.tag = 2; o.f = *f; } Value::Bool(b) => { o.tag = 5; o.i = *b as i64; } Value::Text(s) => { o.tag = 3; o.ptr = s.as_ptr(); o.len = s.len(); } Value::Bytes(b) => { o.tag = 4; o.ptr = b.as_ptr(); o.len = b.len(); } }
    0
}
/// Read every cell at once into `out` (row-major, rows*cols entries; the caller sizes it from count*cols). Returns the number written.
#[unsafe(no_mangle)] pub unsafe extern "C" fn dblab_rows_all(r: *mut Rows, out: *mut CValue, cap: c_int) -> c_int {
    let r = unsafe { &mut *r }; let cols = r.rows.first().map(|x| x.len()).unwrap_or(0); let total = r.rows.len() * cols; if total > cap as usize { return -1; }
    let out = unsafe { std::slice::from_raw_parts_mut(out, total) }; let mut k = 0;
    for row in &r.rows { for v in row { let o = &mut out[k]; *o = CValue { tag: 0, i: 0, f: 0.0, ptr: std::ptr::null(), len: 0 }; match v { Value::Null => {}, Value::Int(i) => { o.tag = 1; o.i = *i; } Value::Float(f) => { o.tag = 2; o.f = *f; } Value::Bool(b) => { o.tag = 5; o.i = *b as i64; } Value::Text(s) => { o.tag = 3; o.ptr = s.as_ptr(); o.len = s.len(); } Value::Bytes(b) => { o.tag = 4; o.ptr = b.as_ptr(); o.len = b.len(); } } k += 1; } }
    total as c_int
}
#[unsafe(no_mangle)] pub unsafe extern "C" fn dblab_rows_free(r: *mut Rows) { if !r.is_null() { drop(unsafe { Box::from_raw(r) }); } }
/// Take a snapshot (compaction + fast restart). Returns the number of rows written.
#[unsafe(no_mangle)] pub unsafe extern "C" fn dblab_snapshot(db: *mut Db) -> i64 { unsafe { &*db }.sql.snapshot().rows as i64 }
