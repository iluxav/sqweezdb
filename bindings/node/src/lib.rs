//! Node addon over the dblab engine (napi-rs). Synchronous calls like node:sqlite's DatabaseSync, plus an async
//! variant that runs the statement on the libuv thread pool.
use dblab::sql::{Prepared, Session, Sql};
use dblab::table::Value;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::{Arc, Mutex};

#[napi] pub struct Database { sql: Arc<Sql> }
#[napi] pub struct Statement { p: Arc<Prepared> }
#[napi] pub struct SessionJs { sql: Arc<Sql>, s: Arc<Mutex<Session>> }

fn to_value(v: &Either4<f64, String, bool, Null>) -> Value { match v { Either4::A(f) => if f.fract() == 0.0 && f.abs() < 9.0e15 { Value::Int(*f as i64) } else { Value::Float(*f) }, Either4::B(s) => Value::Text(s.clone()), Either4::C(b) => Value::Bool(*b), Either4::D(_) => Value::Null } }
fn to_js(v: &Value) -> Either4<f64, String, bool, Null> { match v { Value::Int(i) => Either4::A(*i as f64), Value::Float(f) => Either4::A(*f), Value::Text(s) => Either4::B(s.clone()), Value::Bool(b) => Either4::C(*b), Value::Bytes(b) => Either4::B(String::from_utf8_lossy(b).into_owned()), Value::Null => Either4::D(Null) } }
type Row = Vec<Either4<f64, String, bool, Null>>;

#[napi] impl Database {
    #[napi(constructor)] pub fn new(path: String, durable: Option<bool>) -> Result<Self> {
        let p = std::path::PathBuf::from(&path); let d = durable.unwrap_or(false);
        let sql = if p.join("snap").exists() || !dblab::table::GLog::list_segments(&p).is_empty() { Sql::open(&p, d, 4).map_err(|e| Error::from_reason(e))?.0 } else { Sql::create(&p, d) };
        Ok(Database { sql: Arc::new(sql) })
    }
    #[napi] pub fn prepare(&self, sql: String) -> Result<Statement> { Ok(Statement { p: self.sql.prepare(&sql).map_err(Error::from_reason)? }) }
    #[napi] pub fn session(&self) -> SessionJs { SessionJs { sql: self.sql.clone(), s: Arc::new(Mutex::new(Session::default())) } }
    #[napi] pub fn snapshot(&self) -> f64 { self.sql.snapshot().rows as f64 }
}
#[napi] impl SessionJs {
    #[napi] pub fn login(&self, user: String) -> Result<()> { self.sql.login(&mut self.s.lock().unwrap(), &user).map_err(Error::from_reason) }
    /// Run a prepared statement synchronously; rows as arrays of number | string | boolean | null.
    #[napi] pub fn execute(&self, stmt: &Statement, params: Option<Vec<Either4<f64, String, bool, Null>>>) -> Result<Vec<Row>> {
        let vals: Vec<Value> = params.unwrap_or_default().iter().map(to_value).collect();
        let rows = self.sql.execute_s(&mut self.s.lock().unwrap(), &stmt.p, &vals).map_err(Error::from_reason)?;
        Ok(rows.iter().map(|r| r.iter().map(to_js).collect()).collect())
    }
    /// Run SQL text synchronously.
    #[napi] pub fn exec(&self, sql: String, params: Option<Vec<Either4<f64, String, bool, Null>>>) -> Result<Vec<Row>> {
        let vals: Vec<Value> = params.unwrap_or_default().iter().map(to_value).collect();
        let rows = self.sql.exec_s(&mut self.s.lock().unwrap(), &sql, &vals).map_err(Error::from_reason)?;
        Ok(rows.iter().map(|r| r.iter().map(to_js).collect()).collect())
    }
    /// Run a prepared statement off the JavaScript thread; resolves with the rows.
    #[napi(ts_return_type = "Promise<Array<Array<number | string | boolean | null>>>")]
    pub fn execute_async(&self, stmt: &Statement, params: Option<Vec<Either4<f64, String, bool, Null>>>) -> AsyncTask<Run> {
        let vals: Vec<Value> = params.unwrap_or_default().iter().map(to_value).collect();
        AsyncTask::new(Run { sql: self.sql.clone(), s: self.s.clone(), p: stmt.p.clone(), vals })
    }
}
pub struct Run { sql: Arc<Sql>, s: Arc<Mutex<Session>>, p: Arc<Prepared>, vals: Vec<Value> }
impl Task for Run {
    type Output = Vec<Vec<Value>>; type JsValue = Vec<Row>;
    fn compute(&mut self) -> Result<Self::Output> { self.sql.execute_s(&mut self.s.lock().unwrap(), &self.p, &self.vals).map_err(Error::from_reason) }
    fn resolve(&mut self, _env: Env, out: Self::Output) -> Result<Self::JsValue> { Ok(out.iter().map(|r| r.iter().map(to_js).collect()).collect()) }
}
