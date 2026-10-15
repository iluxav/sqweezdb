//! Experiment 19a: NULL and DELETE semantics, statement by statement against SQLite; deletes in transactions; recovery after deletes.
use dblab::sql::{Session, Sql};
use dblab::table::Value;
use dblab::util::Rng;
use rusqlite::{params, Connection};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

fn render_ours(r: &[Vec<Value>]) -> Vec<String> { r.iter().map(|row| row.iter().map(|v| match v { Value::Null => "NULL".into(), Value::Int(i) => i.to_string(), Value::Float(f) => format!("{f:.3}"), Value::Text(s) => s.clone(), Value::Bool(b) => b.to_string(), Value::Bytes(b) => format!("{b:?}") }).collect::<Vec<_>>().join("|")).collect() }
fn render_sqlite(c: &Connection, sql: &str, p: &[&dyn rusqlite::ToSql]) -> Result<Vec<String>, String> {
    let mut st = c.prepare(sql).map_err(|e| e.to_string())?; let n = st.column_count();
    let rows = st.query_map(p, |r| Ok((0..n).map(|i| match r.get_ref(i).unwrap() { rusqlite::types::ValueRef::Null => "NULL".into(), rusqlite::types::ValueRef::Integer(i) => i.to_string(), rusqlite::types::ValueRef::Real(f) => format!("{f:.3}"), rusqlite::types::ValueRef::Text(t) => String::from_utf8_lossy(t).into_owned(), rusqlite::types::ValueRef::Blob(b) => format!("{b:?}") }).collect::<Vec<_>>().join("|"))).map_err(|e| e.to_string())?;
    rows.collect::<Result<Vec<_>, _>>().map_err(|e| e.to_string())
}
fn to_sqlite_params(v: &[Value]) -> Vec<Box<dyn rusqlite::ToSql>> { v.iter().map(|x| -> Box<dyn rusqlite::ToSql> { match x { Value::Null => Box::new(rusqlite::types::Null), Value::Int(i) => Box::new(*i), Value::Float(f) => Box::new(*f), Value::Text(s) => Box::new(s.clone()), Value::Bool(b) => Box::new(*b), Value::Bytes(b) => Box::new(b.clone()) } }).collect() }

struct Both { sql: Sql, sess: Session, c: Connection, pass: usize, fail: usize }
impl Both {
    fn run(&mut self, label: &str, stmt: &str, p: &[Value]) {
        let ours = self.sql.exec_s(&mut self.sess, stmt, p).map(|r| render_ours(&r));
        let sp = to_sqlite_params(p); let refs: Vec<&dyn rusqlite::ToSql> = sp.iter().map(|b| b.as_ref()).collect();
        let theirs = if stmt.trim_start().to_uppercase().starts_with("SELECT") { render_sqlite(&self.c, stmt, &refs) } else { self.c.execute(stmt, &refs[..]).map(|n| vec![format!("{n} affected")]).map_err(|e| e.to_string()) };
        let ok = match (&ours, &theirs) { (Ok(a), Ok(b)) => if stmt.trim_start().to_uppercase().starts_with("SELECT") { a == b } else { true }, (Err(_), Err(_)) => true, _ => false };
        if ok { self.pass += 1; } else { self.fail += 1; }
        println!("{} {:<52} ours={:?}  sqlite={:?}", if ok { "ok  " } else { "DIFF" }, label, ours.as_ref().map(|v| v.join(" ; ")).unwrap_or_else(|e| format!("ERR {e}")), theirs.as_ref().map(|v| v.join(" ; ")).unwrap_or_else(|e| format!("ERR {e}")));
    }
}

fn main() {
    println!("=== E19a: NULL and DELETE semantics vs SQLite ===");
    let dir = PathBuf::from("data/nocow/e19dir");
    let c = Connection::open_in_memory().unwrap();
    let mut b = Both { sql: Sql::create(&dir, false), sess: Session::default(), c, pass: 0, fail: 0 };
    let create = "CREATE TABLE t(id INTEGER PRIMARY KEY, a INT, b DOUBLE, s VARCHAR(8), n INT NOT NULL)";
    b.sql.exec_s(&mut b.sess, create, &[]).unwrap(); b.c.execute_batch(create).unwrap();
    let ins = "INSERT INTO t VALUES (?,?,?,?,?)";
    for r in [vec![Value::Int(1), Value::Int(10), Value::Float(1.5), Value::Text("x".into()), Value::Int(1)], vec![Value::Int(2), Value::Null, Value::Float(2.5), Value::Text("y".into()), Value::Int(2)], vec![Value::Int(3), Value::Int(20), Value::Null, Value::Null, Value::Int(3)], vec![Value::Int(4), Value::Null, Value::Null, Value::Text("x".into()), Value::Int(4)], vec![Value::Int(5), Value::Int(10), Value::Float(4.0), Value::Text("z".into()), Value::Int(5)]] { b.run("insert", ins, &r); }
    b.run("NULL into NOT NULL column", ins, &[Value::Int(6), Value::Int(1), Value::Float(1.0), Value::Text("q".into()), Value::Null]);
    b.run("NULL into primary key", ins, &[Value::Null, Value::Int(1), Value::Float(1.0), Value::Text("q".into()), Value::Int(6)]);
    b.run("count(*), count(a), count(b), sum, avg, min, max", "SELECT count(*), count(a), count(b), sum(a), avg(b), min(b), max(b) FROM t", &[]);
    b.run("GROUP BY nullable column", "SELECT a, count(*), sum(b) FROM t GROUP BY a", &[]);
    b.run("WHERE a > 10 (NULLs excluded)", "SELECT id FROM t WHERE a > 10", &[]);
    b.run("WHERE a IS NULL", "SELECT id FROM t WHERE a IS NULL", &[]);
    b.run("WHERE b IS NOT NULL AND s = 'x'", "SELECT id FROM t WHERE b IS NOT NULL AND s = 'x'", &[]);
    b.run("WHERE a = NULL (never true)", "SELECT id FROM t WHERE a = NULL", &[]);
    b.run("sum over an all-NULL set", "SELECT sum(a), avg(a) FROM t WHERE id = 2", &[]);
    b.run("rows with NULLs rendered", "SELECT id, a, b, s FROM t ORDER BY id", &[]);
    b.run("UPDATE to NULL", "UPDATE t SET a = NULL WHERE id = ?", &[Value::Int(1)]);
    b.run("WHERE a IS NULL after update", "SELECT id FROM t WHERE a IS NULL", &[]);
    b.run("UPDATE NOT NULL column to NULL", "UPDATE t SET n = NULL WHERE id = ?", &[Value::Int(1)]);
    b.run("DELETE by pk", "DELETE FROM t WHERE id = ?", &[Value::Int(2)]);
    b.run("DELETE WHERE a IS NULL (multi-row, atomic)", "DELETE FROM t WHERE a IS NULL", &[]);
    b.run("remaining rows", "SELECT id, a FROM t ORDER BY id", &[]);
    b.run("DELETE missing row", "DELETE FROM t WHERE id = ?", &[Value::Int(999)]);
    b.run("count after deletes", "SELECT count(*) FROM t", &[]);
    // transactions with deletes
    b.run("BEGIN", "BEGIN", &[]); b.run("DELETE in txn", "DELETE FROM t WHERE id = ?", &[Value::Int(5)]); b.run("visible inside txn?", "SELECT id FROM t WHERE id = ?", &[Value::Int(5)]); b.run("ROLLBACK", "ROLLBACK", &[]);
    b.run("row 5 back after rollback", "SELECT id FROM t WHERE id = ?", &[Value::Int(5)]);
    b.run("BEGIN", "BEGIN", &[]); b.run("insert 9 in txn", ins, &[Value::Int(9), Value::Int(1), Value::Float(1.0), Value::Text("n".into()), Value::Int(9)]); b.run("delete 9 in same txn", "DELETE FROM t WHERE id = ?", &[Value::Int(9)]); b.run("COMMIT", "COMMIT", &[]);
    b.run("row 9 absent", "SELECT id FROM t WHERE id = ?", &[Value::Int(9)]);
    b.run("BEGIN", "BEGIN", &[]); b.run("delete 3 in txn", "DELETE FROM t WHERE id = ?", &[Value::Int(3)]); b.run("re-insert 3 in same txn", ins, &[Value::Int(3), Value::Int(77), Value::Float(7.0), Value::Text("re".into()), Value::Int(3)]); b.run("COMMIT", "COMMIT", &[]);
    b.run("row 3 replaced", "SELECT id, a, s FROM t WHERE id = ?", &[Value::Int(3)]);
    println!("--- recovery after deletes ---");
    let before = render_ours(&b.sql.exec_s(&mut b.sess, "SELECT id, a, b, s FROM t ORDER BY id", &[]).unwrap());
    b.sql.snapshot();
    b.sql.exec_s(&mut b.sess, "DELETE FROM t WHERE id = ?", &[Value::Int(1)]).unwrap();   // after the snapshot: replayed from the log
    b.sql.exec_s(&mut b.sess, ins, &[Value::Int(10), Value::Null, Value::Null, Value::Null, Value::Int(10)]).unwrap();
    let before2 = render_ours(&b.sql.exec_s(&mut b.sess, "SELECT id, a, b, s FROM t ORDER BY id", &[]).unwrap());
    let Both { sql, c, pass, fail, .. } = b; drop(sql);
    let (sql, rs) = Sql::open(&dir, false, 4).unwrap(); let mut sess = Session::default();
    let after = render_ours(&sql.exec_s(&mut sess, "SELECT id, a, b, s FROM t ORDER BY id", &[]).unwrap());
    println!("{} reopen: snapshot {} rows + {} replayed records; rows after = {:?}", if after == before2 { "ok  " } else { "DIFF" }, rs.snap_rows, rs.replayed, after);
    let _ = before; drop(sql); drop(c); let _ = fs::remove_dir_all(&dir);
    println!("semantic checks: {pass} ok, {fail} different");

    println!("--- delete throughput (1M rows, 200k deletes by pk; one 100k-row DELETE statement) ---");
    let dir = PathBuf::from("data/nocow/e19perf"); let sql = Sql::create(&dir, false); let mut sess = Session::default();
    sql.exec_s(&mut sess, "CREATE TABLE d(id INTEGER PRIMARY KEY, v INT, p BLOB(64))", &[]).unwrap();
    let ins = sql.prepare("INSERT INTO d VALUES (?,?,?)").unwrap();
    for id in 1..=1_000_000i64 { if id % 1000 == 1 { sql.exec_s(&mut sess, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut sess, &ins, &[Value::Int(id), Value::Int(id % 10), Value::Bytes(vec![1; 64])]).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut sess, "COMMIT", &[]).unwrap(); } }
    let del = sql.prepare("DELETE FROM d WHERE id = ?").unwrap(); let mut rng = Rng(5);
    let t = Instant::now(); let mut n = 0; for _ in 0..200_000 { let id = (rng.next() % 1_000_000) as i64 + 1; if let Ok(r) = sql.execute_s(&mut sess, &del, &[Value::Int(id)]) { if let Value::Int(k) = r[0][0] { n += k; } } }
    println!("[ours]   DELETE by pk: {:.2} us each ({n} rows deleted)", t.elapsed().as_secs_f64() * 1e6 / 200_000.0);
    let t = Instant::now(); let r = sql.exec_s(&mut sess, "DELETE FROM d WHERE v = 3", &[]).unwrap(); println!("[ours]   DELETE WHERE v = 3 (scan, one atomic statement): {:?} rows in {:.1} ms", r[0][0], t.elapsed().as_secs_f64() * 1e3);
    let r = sql.exec_s(&mut sess, "SELECT count(*) FROM d", &[]).unwrap(); println!("[ours]   remaining: {:?}", r[0][0]);
    drop(del); drop(ins); drop(sql); let _ = fs::remove_dir_all(&dir);
    let p = PathBuf::from("data/nocow/e19.db"); for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
    let c = Connection::open(&p).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-262144; CREATE TABLE d(id INTEGER PRIMARY KEY, v INT, p BLOB)").unwrap();
    let tx = c.unchecked_transaction().unwrap(); { let mut st = tx.prepare("INSERT INTO d VALUES (?,?,?)").unwrap(); for id in 1..=1_000_000i64 { st.execute(params![id, id % 10, &[1u8; 64][..]]).unwrap(); } } tx.commit().unwrap();
    let mut st = c.prepare("DELETE FROM d WHERE id = ?").unwrap(); let mut rng = Rng(5);
    let t = Instant::now(); let mut n = 0; for _ in 0..200_000 { let id = (rng.next() % 1_000_000) as i64 + 1; n += st.execute(params![id]).unwrap(); }
    println!("[sqlite] DELETE by pk, 1 row/txn NORMAL: {:.2} us each ({n} rows deleted)", t.elapsed().as_secs_f64() * 1e6 / 200_000.0);
    let t = Instant::now(); let n = c.execute("DELETE FROM d WHERE v = 3", []).unwrap(); println!("[sqlite] DELETE WHERE v = 3: {n} rows in {:.1} ms", t.elapsed().as_secs_f64() * 1e3);
    drop(st); drop(c); for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
}
