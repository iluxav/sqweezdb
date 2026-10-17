//! Python extension over the sqweezdb engine (PyO3). The interpreter lock is released while a statement runs, so Python
//! threads get the engine's parallel reads.
use sqweezdb::sql::{Prepared, Session, Sql};
use sqweezdb::table::Value;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};
use std::sync::{Arc, Mutex};

#[pyclass(name = "Database")] struct PyDb { sql: Arc<Sql> }
#[pyclass(name = "Statement")] struct PyStmt { p: Arc<Prepared> }
#[pyclass(name = "Session")] struct PySess { sql: Arc<Sql>, s: Mutex<Session> }
#[pyclass(name = "Subscription")] struct PySub { s: sqweezdb::live::Subscription }
#[pymethods] impl PySub {
    /// Wait up to timeout_ms for the next change: (op, table, key, columns...) with op 1 insert, 2 update, 3 delete; None on timeout.
    #[pyo3(signature = (timeout_ms = 1000))]
    fn next<'py>(&self, py: Python<'py>, timeout_ms: u64) -> PyResult<Option<Bound<'py, PyTuple>>> {
        let ev = py.detach(|| self.s.recv_timeout(std::time::Duration::from_millis(timeout_ms)));
        match ev {
            Some(sqweezdb::live::Event::Change(c)) => { let mut cells: Vec<Bound<'py, PyAny>> = vec![to_py(py, &Value::Int(match c.op { sqweezdb::sql::ChangeOp::Insert => 1, sqweezdb::sql::ChangeOp::Update => 2, sqweezdb::sql::ChangeOp::Delete => 3 }))?, to_py(py, &Value::Text(c.table.clone()))?, to_py(py, &c.pk.clone())?]; if let Some(r) = &c.row { for v in r { cells.push(to_py(py, v)?); } } Ok(Some(PyTuple::new(py, cells)?)) }
            Some(sqweezdb::live::Event::Overflow { .. }) => Err(pyo3::exceptions::PyRuntimeError::new_err("subscription fell behind and was dropped; resubscribe")),
            _ => Ok(None),
        }
    }
}

fn to_value(v: &Bound<'_, PyAny>) -> PyResult<Value> {
    if v.is_none() { return Ok(Value::Null); }
    if let Ok(b) = v.extract::<bool>() { if v.is_instance_of::<pyo3::types::PyBool>() { return Ok(Value::Bool(b)); } }
    if let Ok(i) = v.extract::<i64>() { return Ok(Value::Int(i)); }
    if let Ok(f) = v.extract::<f64>() { return Ok(Value::Float(f)); }
    if let Ok(s) = v.extract::<String>() { return Ok(Value::Text(s)); }
    if let Ok(b) = v.extract::<Vec<u8>>() { return Ok(Value::Bytes(b)); }
    Err(pyo3::exceptions::PyTypeError::new_err("unsupported parameter type"))
}
fn to_py<'py>(py: Python<'py>, v: &Value) -> PyResult<Bound<'py, PyAny>> {
    Ok(match v { Value::Null => py.None().into_bound(py), Value::Int(i) => i.into_pyobject(py)?.into_any(), Value::Float(f) => f.into_pyobject(py)?.into_any(), Value::Bool(b) => pyo3::types::PyBool::new(py, *b).to_owned().into_any(), Value::Text(s) => s.into_pyobject(py)?.into_any(), Value::Bytes(b) => PyBytes::new(py, b).into_any() })
}
fn rows_to_py<'py>(py: Python<'py>, rows: Vec<Vec<Value>>) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for r in rows { let cells: Vec<Bound<'py, PyAny>> = r.iter().map(|v| to_py(py, v)).collect::<PyResult<_>>()?; out.append(PyTuple::new(py, cells)?)?; }
    Ok(out)
}
#[pymethods] impl PyDb {
    /// Database(path, durable=False, **options): options are the engine's configuration keys (backup=..., snapshot=...,
    /// listen=..., follow=..., live=..., log=..., create=..., config_file=path of a TOML file read first and overridden by the options).
    #[new] #[pyo3(signature = (path, durable = false, **options))]
    fn new(py: Python<'_>, path: &str, durable: bool, options: Option<&Bound<'_, pyo3::types::PyDict>>) -> PyResult<Self> {
        let p = std::path::PathBuf::from(path);
        let dict = match options { Some(d) => d.copy()?, None => pyo3::types::PyDict::new(py) };
        if !dict.contains("durable")? { dict.set_item("durable", durable)?; }
        let json: String = py.import("json")?.call_method1("dumps", (&dict,))?.extract()?;
        let cfg = sqweezdb::config::Config::load(&p, Some(&json)).map_err(pyo3::exceptions::PyValueError::new_err)?;
        let sql = Sql::open_with(&p, cfg).map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
        Ok(PyDb { sql })
    }
    /// Stop the background services (backup, snapshot policy, follower).
    fn close(&self) { self.sql.close() }
    fn prepare(&self, sql: &str) -> PyResult<PyStmt> { Ok(PyStmt { p: self.sql.prepare(sql).map_err(pyo3::exceptions::PyValueError::new_err)? }) }
    fn session(&self) -> PySess { PySess { sql: self.sql.clone(), s: Mutex::new(Session::default()) } }
    fn snapshot(&self) -> u64 { self.sql.snapshot().rows }
}
#[pymethods] impl PySess {
    /// Primary key of the last row this session inserted (an automatic id, or the one given); 0 before any insert.
    #[getter] fn last_insert_id<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> { match self.s.lock().unwrap().last_insert_key.clone() { Some(v) => to_py(py, &v), None => Ok(py.None().into_bound(py)) } }
    fn login(&self, user: &str) -> PyResult<()> { self.sql.login(&mut self.s.lock().unwrap(), user).map_err(pyo3::exceptions::PyPermissionError::new_err) }
    /// Act as an identity your application vouches for after its own login: `roles` is a role name or a list of them,
    /// keyword arguments are the session attributes row policies read (SESSION.tenant). Nothing is stored in the database.
    /// `s = db.session(); s.assume("customer", tenant=42)`
    #[pyo3(signature = (roles, **attrs))]
    fn assume(&self, roles: &Bound<'_, PyAny>, attrs: Option<&Bound<'_, pyo3::types::PyDict>>) -> PyResult<()> {
        let roles: Vec<String> = if let Ok(r) = roles.extract::<String>() { vec![r] } else { roles.extract::<Vec<String>>().map_err(|_| pyo3::exceptions::PyTypeError::new_err("roles: a role name or a list of role names"))? };
        let mut map = std::collections::HashMap::new();
        if let Some(d) = attrs { for (k, v) in d.iter() { map.insert(k.extract::<String>()?, to_value(&v)?); } }
        self.sql.assume(&mut self.s.lock().unwrap(), &roles, map).map_err(pyo3::exceptions::PyPermissionError::new_err)
    }
    /// Execute a prepared statement with a sequence of parameters; returns a list of tuples.
    #[pyo3(signature = (stmt, params = None))]
    fn execute<'py>(&self, py: Python<'py>, stmt: &PyStmt, params: Option<&Bound<'py, PyAny>>) -> PyResult<Bound<'py, PyList>> {
        let vals: Vec<Value> = match params { Some(p) => p.try_iter()?.map(|x| to_value(&x?)).collect::<PyResult<_>>()?, None => vec![] };
        let (sql, p) = (self.sql.clone(), stmt.p.clone());
        let mut sess = std::mem::take(&mut *self.s.lock().unwrap());
        let r = py.detach(|| sql.execute_s(&mut sess, &p, &vals)); *self.s.lock().unwrap() = sess;
        rows_to_py(py, r.map_err(pyo3::exceptions::PyRuntimeError::new_err)?)
    }
    /// Subscribe to the changes a single-table SELECT would see; the WHERE is the filter, row policies apply.
    #[pyo3(signature = (sql, params = None))]
    fn subscribe<'py>(&self, sql: &str, params: Option<&Bound<'py, PyAny>>) -> PyResult<PySub> {
        let vals: Vec<Value> = match params { Some(p) => p.try_iter()?.map(|x| to_value(&x?)).collect::<PyResult<_>>()?, None => vec![] };
        let sess = std::mem::take(&mut *self.s.lock().unwrap()); let r = self.sql.reactor().subscribe(&sess, sql, &vals); *self.s.lock().unwrap() = sess;
        Ok(PySub { s: r.map_err(pyo3::exceptions::PyValueError::new_err)? })
    }
    /// Execute SQL text (DDL, admin statements, one-off queries).
    #[pyo3(signature = (sql, params = None))]
    fn exec<'py>(&self, py: Python<'py>, sql: &str, params: Option<&Bound<'py, PyAny>>) -> PyResult<Bound<'py, PyList>> {
        let vals: Vec<Value> = match params { Some(p) => p.try_iter()?.map(|x| to_value(&x?)).collect::<PyResult<_>>()?, None => vec![] };
        let (db, text) = (self.sql.clone(), sql.to_string());
        let mut sess = std::mem::take(&mut *self.s.lock().unwrap());
        let r = py.detach(|| db.exec_s(&mut sess, &text, &vals)); *self.s.lock().unwrap() = sess;
        rows_to_py(py, r.map_err(pyo3::exceptions::PyRuntimeError::new_err)?)
    }
}
#[pymodule(name = "sqweezdb")] fn sqweezdb_module(m: &Bound<'_, PyModule>) -> PyResult<()> { m.add_class::<PyDb>()?; m.add_class::<PyStmt>()?; m.add_class::<PySess>()?; m.add_class::<PySub>()?; Ok(()) }
