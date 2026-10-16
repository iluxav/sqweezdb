//! Python extension over the dblab engine (PyO3). The interpreter lock is released while a statement runs, so Python
//! threads get the engine's parallel reads.
use dblab::sql::{Prepared, Session, Sql};
use dblab::table::Value;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};
use std::sync::{Arc, Mutex};

#[pyclass(name = "Database")] struct PyDb { sql: Arc<Sql> }
#[pyclass(name = "Statement")] struct PyStmt { p: Arc<Prepared> }
#[pyclass(name = "Session", unsendable)] struct PySess { sql: Arc<Sql>, s: Mutex<Session> }

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
    #[new] #[pyo3(signature = (path, durable = false))]
    fn new(path: &str, durable: bool) -> PyResult<Self> {
        let p = std::path::PathBuf::from(path);
        let sql = if p.join("snap").exists() || !dblab::table::GLog::list_segments(&p).is_empty() { Sql::open(&p, durable, 4).map_err(pyo3::exceptions::PyRuntimeError::new_err)?.0 } else { Sql::create(&p, durable) };
        Ok(PyDb { sql: Arc::new(sql) })
    }
    fn prepare(&self, sql: &str) -> PyResult<PyStmt> { Ok(PyStmt { p: self.sql.prepare(sql).map_err(pyo3::exceptions::PyValueError::new_err)? }) }
    fn session(&self) -> PySess { PySess { sql: self.sql.clone(), s: Mutex::new(Session::default()) } }
    fn snapshot(&self) -> u64 { self.sql.snapshot().rows }
}
#[pymethods] impl PySess {
    fn login(&self, user: &str) -> PyResult<()> { self.sql.login(&mut self.s.lock().unwrap(), user).map_err(pyo3::exceptions::PyPermissionError::new_err) }
    /// Execute a prepared statement with a sequence of parameters; returns a list of tuples.
    #[pyo3(signature = (stmt, params = None))]
    fn execute<'py>(&self, py: Python<'py>, stmt: &PyStmt, params: Option<&Bound<'py, PyAny>>) -> PyResult<Bound<'py, PyList>> {
        let vals: Vec<Value> = match params { Some(p) => p.try_iter()?.map(|x| to_value(&x?)).collect::<PyResult<_>>()?, None => vec![] };
        let (sql, p) = (self.sql.clone(), stmt.p.clone());
        let mut sess = std::mem::take(&mut *self.s.lock().unwrap());
        let r = py.detach(|| sql.execute_s(&mut sess, &p, &vals)); *self.s.lock().unwrap() = sess;
        rows_to_py(py, r.map_err(pyo3::exceptions::PyRuntimeError::new_err)?)
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
#[pymodule] fn pydblab(m: &Bound<'_, PyModule>) -> PyResult<()> { m.add_class::<PyDb>()?; m.add_class::<PyStmt>()?; m.add_class::<PySess>()?; Ok(()) }
