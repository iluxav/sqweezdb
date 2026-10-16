//! Node addon over the dblab engine (napi-rs): the native half. `index.js` is the API developers see.
//! Rows cross into JavaScript as objects built here from the plan's column names and types:
//! INT -> number (BigInt past 2^53), DOUBLE -> number, BOOL -> boolean, TEXT -> string, BYTES -> Buffer,
//! TIMESTAMP -> Date, NULL -> null. Errors carry a code: the message is "CODE|text" and index.js splits it.
use dblab::live::Event;
use dblab::sql::{self, ChangeOp, Prepared, Session, Sql};
use dblab::table::{ColType, Value};
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ErrorStrategy, ThreadSafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::{JsBigInt, JsBoolean, JsBuffer, JsDate, JsFunction, JsNumber, JsObject, JsString, JsUnknown, ValueType};
use napi_derive::napi;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

type Cols = Vec<(String, Option<ColType>)>;
/// One engine per directory per process: a second `open` of the same path (another module, a worker thread) joins it.
static OPEN: Mutex<Option<HashMap<PathBuf, Weak<Sql>>>> = Mutex::new(None);

fn err(msg: String) -> Error { Error::from_reason(format!("{}|{}", sql::error_code(&msg), msg)) }
fn err_code(code: &str, msg: &str) -> Error { Error::from_reason(format!("{code}|{msg}")) }

fn to_js(env: &Env, v: &Value, ty: Option<&ColType>) -> Result<JsUnknown> {
    Ok(match v {
        Value::Int(i) => if matches!(ty, Some(ColType::Timestamp)) { env.create_date(*i as f64 / 1000.0)?.into_unknown() }
                         else if i.unsigned_abs() <= (1u64 << 53) { env.create_double(*i as f64)?.into_unknown() }
                         else { env.create_bigint_from_i64(*i)?.into_unknown()? },
        Value::Float(f) => env.create_double(*f)?.into_unknown(),
        Value::Bool(b) => env.get_boolean(*b)?.into_unknown(),
        Value::Text(s) => env.create_string(s)?.into_unknown(),
        Value::Bytes(b) => env.create_buffer_copy(b)?.into_raw().into_unknown(),
        Value::Null => env.get_null()?.into_unknown(),
    })
}
fn row_obj(env: &Env, cols: &Cols, row: &[Value]) -> Result<JsObject> {
    let mut o = env.create_object()?;
    for (i, v) in row.iter().enumerate() {
        match cols.get(i) { Some((n, t)) => o.set_named_property(n, to_js(env, v, t.as_ref())?)?, None => o.set_named_property(&format!("col{i}"), to_js(env, v, None)?)? }
    }
    Ok(o)
}
fn rows_arr(env: &Env, cols: &Cols, rows: &[Vec<Value>]) -> Result<JsObject> {
    let mut a = env.create_array_with_length(rows.len())?;
    for (i, r) in rows.iter().enumerate() { a.set_element(i as u32, row_obj(env, cols, r)?)?; }
    Ok(a)
}
fn from_js(v: JsUnknown) -> Result<Value> {
    Ok(match v.get_type()? {
        ValueType::Null | ValueType::Undefined => Value::Null,
        ValueType::Boolean => Value::Bool(unsafe { v.cast::<JsBoolean>() }.get_value()?),
        ValueType::Number => { let f = unsafe { v.cast::<JsNumber>() }.get_double()?; if f.fract() == 0.0 && f.abs() <= 9007199254740992.0 { Value::Int(f as i64) } else { Value::Float(f) } }
        ValueType::String => Value::Text(unsafe { v.cast::<JsString>() }.into_utf8()?.as_str()?.to_string()),
        ValueType::BigInt => { let (i, lossless) = unsafe { v.cast::<JsBigInt>() }.get_i64()?; if !lossless { return Err(err_code("TYPE", "BigInt does not fit in 64 bits")); } Value::Int(i) }
        ValueType::Object => {
            if v.is_date()? { let ms = unsafe { v.cast::<JsDate>() }.value_of()?; Value::Int((ms * 1000.0).round() as i64) }
            else if v.is_buffer()? { Value::Bytes(unsafe { v.cast::<JsBuffer>() }.into_value()?.to_vec()) }
            else { return Err(err_code("TYPE", "unsupported parameter: pass a number, string, boolean, BigInt, Date, Buffer or null")); }
        }
        _ => return Err(err_code("TYPE", "unsupported parameter type")),
    })
}
fn params(v: Vec<JsUnknown>) -> Result<Vec<Value>> { v.into_iter().map(from_js).collect() }
/// Rows as one binary frame, decoded into objects by index.js: [u32 rows][u32 cols] then per cell a tag byte and its
/// payload (0 null; 1 f64; 2 i64 BigInt; 3 u32 len + utf8; 4 u32 len + bytes; 5 bool byte; 6 f64 ms since epoch -> Date).
fn pack(cols: &Cols, rows: &[Vec<Value>]) -> Vec<u8> {
    let mut b = Vec::with_capacity(8 + rows.len() * cols.len().max(1) * 9);
    b.extend_from_slice(&(rows.len() as u32).to_le_bytes()); b.extend_from_slice(&(cols.len() as u32).to_le_bytes());
    for r in rows { for (i, v) in r.iter().enumerate() {
        match v {
            Value::Null => b.push(0),
            Value::Int(x) => if matches!(cols.get(i).and_then(|c| c.1.as_ref()), Some(ColType::Timestamp)) { b.push(6); b.extend_from_slice(&(*x as f64 / 1000.0).to_le_bytes()) }
                             else if x.unsigned_abs() <= (1u64 << 53) { b.push(1); b.extend_from_slice(&(*x as f64).to_le_bytes()) } else { b.push(2); b.extend_from_slice(&x.to_le_bytes()) },
            Value::Float(f) => { b.push(1); b.extend_from_slice(&f.to_le_bytes()) }
            Value::Bool(x) => { b.push(5); b.push(*x as u8) }
            Value::Text(s) => { b.push(3); b.extend_from_slice(&(s.len() as u32).to_le_bytes()); b.extend_from_slice(s.as_bytes()) }
            Value::Bytes(x) => { b.push(4); b.extend_from_slice(&(x.len() as u32).to_le_bytes()); b.extend_from_slice(x) }
        }
    } }
    b
}

#[napi] pub struct Db { sql: Mutex<Option<Arc<Sql>>>, durable: bool }
#[napi] pub struct Stmt { p: Arc<Prepared>, cols: Cols, kind: u8 }
#[napi] pub struct Sess { sql: Arc<Sql>, s: Arc<Mutex<Session>> }
#[napi] pub struct Sub { stop: Arc<AtomicBool>, tsfn: Option<ThreadsafeFunction<Payload, ErrorStrategy::Fatal>>, cols: Arc<Cols>, initial: Vec<Vec<Value>> }
enum Payload { Change(Arc<sql::Change>), Live { rows: Vec<Vec<Value>>, added: Vec<Vec<Value>>, removed: Vec<Vec<Value>> }, Overflow(u64) }

/// Open (or create) the database in `path`. Durable=false returns from writes when they are visible; `waitDurable` covers the fsync.
#[napi]
pub fn open(path: String, durable: Option<bool>) -> Result<Db> {
    let p = std::path::absolute(&path).map_err(|e| err_code("ERROR", &e.to_string()))?;
    let d = durable.unwrap_or(false);
    let mut g = OPEN.lock().unwrap(); let reg = g.get_or_insert_with(HashMap::new);
    if let Some(sql) = reg.get(&p).and_then(|w| w.upgrade()) { return Ok(Db { sql: Mutex::new(Some(sql)), durable: d }); }
    let existing = p.join("snap").exists() || !dblab::table::GLog::list_segments(&p).is_empty();
    let sql = if existing { Sql::open(&p, d, 4).map_err(err)?.0 } else { Sql::create(&p, d) };
    let sql = Arc::new(sql); reg.insert(p, Arc::downgrade(&sql));
    Ok(Db { sql: Mutex::new(Some(sql)), durable: d })
}

#[napi] impl Db {
    fn sql(&self) -> Result<Arc<Sql>> { self.sql.lock().unwrap().clone().ok_or_else(|| err_code("CLOSED", "database is closed")) }
    #[napi] pub fn prepare(&self, text: String) -> Result<Stmt> {
        let sql = self.sql()?; let p = sql.prepare(&text).map_err(err)?; let cols = sql.columns(&p);
        let kind = match &p.plan { sql::Plan::Select(_) => 1, sql::Plan::Insert { .. } | sql::Plan::Update { .. } => 2, sql::Plan::Delete { .. } => 3, _ => 0 };
        Ok(Stmt { p, cols, kind })
    }
    #[napi] pub fn session(&self) -> Result<Sess> { Ok(Sess { sql: self.sql()?, s: Arc::new(Mutex::new(Session::default())) }) }
    #[napi] pub fn close(&self) { self.sql.lock().unwrap().take(); }
    #[napi] pub fn is_durable(&self) -> bool { self.durable }
    #[napi] pub fn rows(&self) -> Result<f64> { Ok(self.sql()?.snapshot().rows as f64) }
    /// Resolves once commit `seq` is on disk.
    #[napi(ts_return_type = "Promise<void>")] pub fn wait_durable(&self, seq: f64) -> AsyncTask<WaitDurable> { AsyncTask::new(WaitDurable { sql: self.sql.lock().unwrap().clone(), seq: seq as u64 }) }
}
#[napi] impl Stmt {
    #[napi] pub fn columns(&self) -> Vec<String> { self.cols.iter().map(|(n, _)| n.clone()).collect() }
    #[napi] pub fn kind(&self) -> u32 { self.kind as u32 }
}
#[napi] impl Sess {
    #[napi] pub fn login(&self, user: String) -> Result<()> { self.sql.login(&mut self.s.lock().unwrap(), &user).map_err(err) }
    #[napi] pub fn in_txn(&self) -> bool { self.s.lock().unwrap().txn.is_some() }
    #[napi] pub fn last_seq(&self) -> f64 { self.s.lock().unwrap().last_seq as f64 }
    #[napi(ts_return_type = "Array<Record<string, unknown>>")] pub fn all(&self, env: Env, stmt: &Stmt, args: Vec<JsUnknown>) -> Result<JsObject> {
        let vals = params(args)?; let rows = self.sql.execute_s(&mut self.s.lock().unwrap(), &stmt.p, &vals).map_err(err)?; rows_arr(&env, &stmt.cols, &rows)
    }
    #[napi(ts_return_type = "Record<string, unknown> | null")] pub fn get(&self, env: Env, stmt: &Stmt, args: Vec<JsUnknown>) -> Result<JsUnknown> {
        let vals = params(args)?; let rows = self.sql.execute_s(&mut self.s.lock().unwrap(), &stmt.p, &vals).map_err(err)?;
        match rows.first() { Some(r) => Ok(row_obj(&env, &stmt.cols, r)?.into_unknown()), None => Ok(env.get_null()?.into_unknown()) }
    }
    /// A write. Returns the commit's sequence (0 when no commit happened, i.e. inside a transaction); a DELETE returns
    /// -(rows deleted) - 1 instead, and index.js asks for the sequence separately.
    #[napi] pub fn run(&self, stmt: &Stmt, args: Vec<JsUnknown>) -> Result<f64> {
        let vals = params(args)?; let mut s = self.s.lock().unwrap(); let before = s.last_seq;
        let rows = self.sql.execute_s(&mut s, &stmt.p, &vals).map_err(err)?;
        if stmt.kind == 3 { return Ok(-(rows.first().and_then(|r| r.first()).map(|v| if let Value::Int(n) = v { *n as f64 } else { 0.0 }).unwrap_or(0.0)) - 1.0); }
        Ok(if s.last_seq != before { s.last_seq as f64 } else { 0.0 })
    }
    /// The rows of a SELECT as one packed frame (see `pack`); index.js turns it into objects.
    #[napi] pub fn packed(&self, stmt: &Stmt, args: Vec<JsUnknown>) -> Result<Buffer> {
        let vals = params(args)?; let rows = self.sql.execute_s(&mut self.s.lock().unwrap(), &stmt.p, &vals).map_err(err)?;
        Ok(pack(&stmt.cols, &rows).into())
    }
    /// SQL text, any statement; rows as objects (empty for writes and DDL).
    #[napi(ts_return_type = "Array<Record<string, unknown>>")] pub fn exec(&self, env: Env, text: String, args: Vec<JsUnknown>) -> Result<JsObject> {
        let vals = params(args)?; let mut s = self.s.lock().unwrap();
        let rows = self.sql.exec_s(&mut s, &text, &vals).map_err(err)?;
        let cols = if rows.is_empty() { vec![] } else { self.sql.prepare(&text).map(|p| self.sql.columns(&p)).unwrap_or_default() };
        rows_arr(&env, &cols, &rows)
    }
    /// A SELECT off the JavaScript thread (long scans); resolves with the rows.
    #[napi(ts_return_type = "Promise<Array<Record<string, unknown>>>")] pub fn all_async(&self, stmt: &Stmt, args: Vec<JsUnknown>) -> Result<AsyncTask<Run>> {
        Ok(AsyncTask::new(Run { sql: self.sql.clone(), s: self.s.clone(), p: stmt.p.clone(), cols: stmt.cols.clone(), vals: params(args)? }))
    }
    /// Push every change a single-table SELECT would see to `callback` as { type:'change', op, table, key, seq, row }.
    #[napi] pub fn subscribe(&self, text: String, args: Vec<JsUnknown>, callback: JsFunction) -> Result<Sub> {
        let vals = params(args)?; let sub = self.sql.reactor().subscribe(&self.s.lock().unwrap(), &text, &vals).map_err(err)?;
        start_push(self.sql.clone(), sub, Arc::new(vec![]), vec![], callback)
    }
    /// Watch any SELECT: the current rows are in `initial()`, then { type:'live', rows, added, removed } on every change.
    #[napi] pub fn watch(&self, text: String, args: Vec<JsUnknown>, callback: JsFunction) -> Result<Sub> {
        let vals = params(args)?; let cols = self.sql.prepare(&text).map(|p| self.sql.columns(&p)).unwrap_or_default();
        let (sub, rows) = self.sql.reactor().watch(&self.s.lock().unwrap(), &text, &vals).map_err(err)?;
        start_push(self.sql.clone(), sub, Arc::new(cols), rows, callback)
    }
}
fn start_push(sql: Arc<Sql>, sub: dblab::live::Subscription, cols: Arc<Cols>, initial: Vec<Vec<Value>>, callback: JsFunction) -> Result<Sub> {
    let (sql2, cols2) = (sql.clone(), cols.clone()); let mut table_cols: HashMap<String, Cols> = HashMap::new();
    let tsfn: ThreadsafeFunction<Payload, ErrorStrategy::Fatal> = callback.create_threadsafe_function(0, move |ctx: ThreadSafeCallContext<Payload>| {
        let env = ctx.env; let mut o = env.create_object()?;
        match ctx.value {
            Payload::Change(c) => {
                o.set_named_property("type", env.create_string("change")?)?;
                o.set_named_property("op", env.create_string(match c.op { ChangeOp::Insert => "insert", ChangeOp::Update => "update", ChangeOp::Delete => "delete" })?)?;
                o.set_named_property("table", env.create_string(&c.table)?)?;
                o.set_named_property("key", env.create_double(c.pk as f64)?)?;
                o.set_named_property("seq", env.create_double(c.seq as f64)?)?;
                match &c.row {
                    Some(r) => { let tc = table_cols.entry(c.table.clone()).or_insert_with(|| sql2.db.table(&c.table).map(|t| t.schema.columns.iter().map(|col| (col.name.clone(), Some(col.ty.clone()))).collect()).unwrap_or_default()); o.set_named_property("row", row_obj(&env, tc, r)?)?; }
                    None => o.set_named_property("row", env.get_null()?)?,
                }
            }
            Payload::Live { rows, added, removed } => {
                o.set_named_property("type", env.create_string("live")?)?;
                o.set_named_property("rows", rows_arr(&env, &cols2, &rows)?)?; o.set_named_property("added", rows_arr(&env, &cols2, &added)?)?; o.set_named_property("removed", rows_arr(&env, &cols2, &removed)?)?;
            }
            Payload::Overflow(seq) => { o.set_named_property("type", env.create_string("overflow")?)?; o.set_named_property("seq", env.create_double(seq as f64)?)?; }
        }
        Ok(vec![o.into_unknown()])
    })?;
    let stop = Arc::new(AtomicBool::new(false)); let (stop2, tsfn2) = (stop.clone(), tsfn.clone());
    std::thread::spawn(move || {
        loop {
            if stop2.load(Ordering::Relaxed) { break; }
            match sub.recv_timeout(std::time::Duration::from_millis(100)) {
                Some(Event::Change(c)) => { tsfn2.call(Payload::Change(c), ThreadsafeFunctionCallMode::NonBlocking); }
                Some(Event::Live { rows, added, removed, .. }) => { tsfn2.call(Payload::Live { rows, added, removed }, ThreadsafeFunctionCallMode::NonBlocking); }
                Some(Event::Overflow { last_seq }) => { tsfn2.call(Payload::Overflow(last_seq), ThreadsafeFunctionCallMode::NonBlocking); break; }
                None => {}
            }
        }
        drop(sub);
    });
    Ok(Sub { stop, tsfn: Some(tsfn), cols, initial })
}
#[napi] impl Sub {
    #[napi] pub fn close(&mut self) { self.stop.store(true, Ordering::Relaxed); self.tsfn.take(); }
    /// Let the process exit while this subscription is still open.
    #[napi] pub fn unref(&mut self, env: Env) -> Result<()> { if let Some(t) = &mut self.tsfn { t.unref(&env)?; } Ok(()) }
    #[napi(ts_return_type = "Array<Record<string, unknown>>")] pub fn initial(&mut self, env: Env) -> Result<JsObject> { let rows = std::mem::take(&mut self.initial); rows_arr(&env, &self.cols, &rows) }
}
pub struct WaitDurable { sql: Option<Arc<Sql>>, seq: u64 }
impl Task for WaitDurable {
    type Output = (); type JsValue = ();
    fn compute(&mut self) -> Result<()> { match &self.sql { Some(s) => { s.wait_durable(self.seq); Ok(()) } None => Err(err_code("CLOSED", "database is closed")) } }
    fn resolve(&mut self, _env: Env, _o: ()) -> Result<()> { Ok(()) }
}
pub struct Run { sql: Arc<Sql>, s: Arc<Mutex<Session>>, p: Arc<Prepared>, cols: Cols, vals: Vec<Value> }
impl Task for Run {
    type Output = Vec<Vec<Value>>; type JsValue = JsObject;
    fn compute(&mut self) -> Result<Self::Output> { self.sql.execute_s(&mut self.s.lock().unwrap(), &self.p, &self.vals).map_err(err) }
    fn resolve(&mut self, env: Env, out: Self::Output) -> Result<JsObject> { rows_arr(&env, &self.cols, &out) }
}
