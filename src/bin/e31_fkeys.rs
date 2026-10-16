//! Experiment 31: foreign keys enforced by default. 1M users; orders (ON DELETE CASCADE), invoices (RESTRICT), payments
//! (SET NULL). SQLite runs twice: its default (REFERENCES parsed, not enforced) and with PRAGMA foreign_keys=ON plus the
//! child indexes its documentation tells you to create. Ours enforces always and indexes the key columns itself.
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::Value;
use sqweezdb::util::Rng;
use rusqlite::{params, Connection};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const NU: i64 = 1_000_000; const NO: i64 = 1_000_000;
const DB_OFF: &str = "data/nocow/e31off.db"; const DB_ON: &str = "data/nocow/e31on.db"; const DIR: &str = "data/nocow/e31dir";
static NEXT: AtomicI64 = AtomicI64::new(10_000_000);
fn us(d: Duration, n: i64) -> f64 { d.as_secs_f64() * 1e6 / n as f64 }
fn sq(c: &Connection, sql: &str) -> Result<String, String> { let mut st = c.prepare(sql).map_err(|e| e.to_string())?; if st.column_count() == 0 { return st.execute([]).map(|n| format!("{n} affected")).map_err(|e| e.to_string()); } let n = st.column_count(); let rows: Vec<String> = st.query_map([], |r| Ok((0..n).map(|i| match r.get::<_, rusqlite::types::Value>(i).unwrap() { rusqlite::types::Value::Null => "NULL".into(), rusqlite::types::Value::Integer(i) => i.to_string(), rusqlite::types::Value::Real(f) => format!("{f}"), rusqlite::types::Value::Text(s) => s, rusqlite::types::Value::Blob(b) => format!("blob{}", b.len()) }).collect::<Vec<_>>().join("|"))).map_err(|e| e.to_string())?.collect::<Result<_, _>>().map_err(|e| e.to_string())?; Ok(rows.join(" ; ")) }
fn ou(sql: &Sql, s: &mut Session, q: &str) -> Result<String, String> { sql.exec_s(s, q, &[]).map(|rows| rows.iter().map(|r| r.iter().map(|v| match v { Value::Null => "NULL".into(), Value::Int(i) => i.to_string(), Value::Float(f) => format!("{f}"), Value::Bool(b) => b.to_string(), Value::Text(s) => s.clone(), Value::Bytes(b) => format!("blob{}", b.len()) }).collect::<Vec<_>>().join("|")).collect::<Vec<_>>().join(" ; ")) }
fn show(r: Result<String, String>) -> String { match r { Ok(s) if s.is_empty() => "ok".into(), Ok(s) => s, Err(e) => format!("ERROR: {}", e.chars().take(110).collect::<String>()) } }
const ORPHANS: &str = "SELECT count(*) FROM orders o WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.id = o.user_id)";

fn sqlite_build(path: &str, fk_on: bool) -> Connection {
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{path}{suf}")).ok(); }
    let c = Connection::open(path).unwrap(); c.busy_timeout(Duration::from_secs(600)).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap(); if fk_on { c.execute_batch("PRAGMA foreign_keys=ON").unwrap(); }
    c.execute_batch("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT); CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id) ON DELETE CASCADE, amount REAL); CREATE TABLE invoices(id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id), total REAL); CREATE TABLE payments(id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id) ON DELETE SET NULL, amt REAL); CREATE INDEX orders_user ON orders(user_id); CREATE INDEX invoices_user ON invoices(user_id); CREATE INDEX payments_user ON payments(user_id);").unwrap();
    let tx = c.unchecked_transaction().unwrap(); { let mut st = tx.prepare("INSERT INTO users VALUES (?,?)").unwrap(); for id in 1..=NU { st.execute(params![id, format!("u{id}")]).unwrap(); } } tx.commit().unwrap(); c
}
fn ours_build(dir: &PathBuf) -> Sql {
    let sql = Sql::create(dir, false); let mut s = Session::default();
    for q in ["CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR(32))", "CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INT REFERENCES users(id) ON DELETE CASCADE, amount DOUBLE)", "CREATE TABLE invoices(id INTEGER PRIMARY KEY, user_id INT REFERENCES users(id), total DOUBLE)", "CREATE TABLE payments(id INTEGER PRIMARY KEY, user_id INT REFERENCES users(id) ON DELETE SET NULL, amt DOUBLE)"] { sql.exec_s(&mut s, q, &[]).unwrap(); }
    let ins = sql.prepare("INSERT INTO users VALUES (?,?)").unwrap(); for id in 1..=NU { if id % 1000 == 1 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Text(format!("u{id}"))]).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } }
    sql
}
fn main() {
    println!("=== E31: foreign keys enforced by default (1M users, 1M orders) ===");
    let off = sqlite_build(DB_OFF, false); let on = sqlite_build(DB_ON, true); let dir = PathBuf::from(format!("{DIR}/db")); let sql = Arc::new(ours_build(&dir)); let mut s = Session::default();
    // 1. load orders (every row checks its parent where enforced)
    let mut rng = Rng(3); let mut ins_s = |c: &Connection| { let t = Instant::now(); for chunk in (1..=NO).step_by(1000) { let tx = c.unchecked_transaction().unwrap(); { let mut st = tx.prepare_cached("INSERT INTO orders VALUES (?,?,?)").unwrap(); for id in chunk..chunk + 1000 { st.execute(params![id, (rng.next() % NU as u64) as i64 + 1, (id % 500) as f64]).unwrap(); } } tx.commit().unwrap(); } t.elapsed() };
    let t_off = ins_s(&off); let t_on = ins_s(&on);
    let ins = sql.prepare("INSERT INTO orders VALUES (?,?,?)").unwrap(); let mut rng = Rng(3); let t = Instant::now(); for id in 1..=NO { if id % 1000 == 1 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Int((rng.next() % NU as u64) as i64 + 1), Value::Float((id % 500) as f64)]).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } } let t_o = t.elapsed();
    println!("insert 1M orders, 1000-row txns: sqlite FK off {:.2} us/row | sqlite FK on {:.2} us/row | ours (always on) {:.2} us/row", us(t_off, NO), us(t_on, NO), us(t_o, NO));
    for (c, l) in [(&off, "sqlite off"), (&on, "sqlite on ")] { for q in ["INSERT INTO invoices VALUES (1, 7, 10.0)", "INSERT INTO invoices VALUES (2, 8, 20.0)", "INSERT INTO payments VALUES (1, 9, 5.0)"] { sq(c, q).unwrap(); } let _ = l; }
    for q in ["INSERT INTO invoices VALUES (1, 7, 10.0)", "INSERT INTO invoices VALUES (2, 8, 20.0)", "INSERT INTO payments VALUES (1, 9, 5.0)"] { sql.exec_s(&mut s, q, &[]).unwrap(); }
    // 2. an orphan
    let q = "INSERT INTO orders VALUES (9000001, 999999999, 1.0)";
    println!("orphan insert (user 999999999 does not exist):\n  sqlite off: {}\n  sqlite on:  {}\n  ours:       {}", show(sq(&off, q)), show(sq(&on, q)), show(ou(&sql, &mut s, q)));
    // 3. delete a parent with an invoice (RESTRICT)
    let q = "DELETE FROM users WHERE id = 7";
    println!("delete user 7 who has an invoice (RESTRICT):\n  sqlite off: {} (invoice now orphaned)\n  sqlite on:  {}\n  ours:       {}", show(sq(&off, q)), show(sq(&on, q)), show(ou(&sql, &mut s, q)));
    // 4. SET NULL
    let q = "DELETE FROM users WHERE id = 9"; let chk = "SELECT user_id FROM payments WHERE id = 1";
    println!("delete user 9 who has a payment (SET NULL) -> payment.user_id: sqlite off {} / sqlite on {} / ours {}", { sq(&off, q).unwrap(); show(sq(&off, chk)) }, { sq(&on, q).unwrap(); show(sq(&on, chk)) }, { ou(&sql, &mut s, q).unwrap(); show(ou(&sql, &mut s, chk)) });
    // 5. cascade: delete 10k parents
    let del = |c: &Connection| { let t = Instant::now(); let tx = c.unchecked_transaction().unwrap(); { let mut st = tx.prepare("DELETE FROM users WHERE id = ?").unwrap(); for id in 500_001..510_001i64 { st.execute(params![id]).unwrap(); } } tx.commit().unwrap(); t.elapsed() };
    let d_off = del(&off); let d_on = del(&on);
    let dq = sql.prepare("DELETE FROM users WHERE id = ?").unwrap(); let t = Instant::now(); for id in 500_001..510_001i64 { sql.execute_s(&mut s, &dq, &[Value::Int(id)]).unwrap(); } let d_o = t.elapsed();
    println!("delete 10k users with ~10k orders (CASCADE): sqlite off {:.0} ms, orphans left {} | sqlite on {:.0} ms, orphans {} | ours {:.0} ms (autocommit each), orphans {}", d_off.as_secs_f64() * 1e3, show(sq(&off, ORPHANS)), d_on.as_secs_f64() * 1e3, show(sq(&on, ORPHANS)), d_o.as_secs_f64() * 1e3, show(ou(&sql, &mut s, ORPHANS)));
    // 6. concurrency: 8 inserters of orders for random users, 1 deleter of random users (cascade), 2 s; then count orphans
    let run_sqlite = |path: &'static str| -> (u64, u64, u64) { let stop = Arc::new(AtomicBool::new(false)); let (ins_n, del_n, fails) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
        let mut hs = vec![]; for k in 0..8u64 { let (stop, ins_n, fails) = (stop.clone(), ins_n.clone(), fails.clone()); hs.push(std::thread::spawn(move || { let c = Connection::open(path).unwrap(); c.busy_timeout(Duration::from_secs(600)).unwrap(); c.execute_batch("PRAGMA foreign_keys=ON; PRAGMA synchronous=NORMAL").unwrap(); let mut r = Rng(70 + k); while !stop.load(Ordering::Relaxed) { let id = NEXT.fetch_add(1, Ordering::Relaxed); match c.execute("INSERT INTO orders VALUES (?,?,?)", params![id, 600_000 + (r.next() % 400_000) as i64, 1.0]) { Ok(_) => { ins_n.fetch_add(1, Ordering::Relaxed); } Err(_) => { fails.fetch_add(1, Ordering::Relaxed); } } } })); }
        { let (stop, del_n) = (stop.clone(), del_n.clone()); hs.push(std::thread::spawn(move || { let c = Connection::open(path).unwrap(); c.busy_timeout(Duration::from_secs(600)).unwrap(); c.execute_batch("PRAGMA foreign_keys=ON; PRAGMA synchronous=NORMAL").unwrap(); let mut r = Rng(99); while !stop.load(Ordering::Relaxed) { let id = 600_000 + (r.next() % 400_000) as i64; if c.execute("DELETE FROM users WHERE id = ?", params![id]).map(|n| n > 0).unwrap_or(false) { del_n.fetch_add(1, Ordering::Relaxed); } } })); }
        std::thread::sleep(Duration::from_secs(2)); stop.store(true, Ordering::Relaxed); for h in hs { h.join().unwrap(); } (ins_n.load(Ordering::Relaxed), del_n.load(Ordering::Relaxed), fails.load(Ordering::Relaxed)) };
    let (i_on, d_on2, f_on) = run_sqlite(DB_ON);
    println!("concurrent 2 s, 8 inserters + 1 cascading deleter: sqlite on: {i_on} orders inserted, {d_on2} users deleted, {f_on} inserts refused (parent gone) -> orphans {}", show(sq(&on, ORPHANS)));
    { let stop = Arc::new(AtomicBool::new(false)); let (ins_n, del_n, fails) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
      let mut hs = vec![]; for k in 0..8u64 { let (sql, stop, ins_n, fails) = (sql.clone(), stop.clone(), ins_n.clone(), fails.clone()); hs.push(std::thread::spawn(move || { let mut s = Session::default(); let ins = sql.prepare("INSERT INTO orders VALUES (?,?,?)").unwrap(); let mut r = Rng(70 + k); while !stop.load(Ordering::Relaxed) { let id = NEXT.fetch_add(1, Ordering::Relaxed); match sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Int(600_000 + (r.next() % 400_000) as i64), Value::Float(1.0)]) { Ok(_) => { ins_n.fetch_add(1, Ordering::Relaxed); } Err(_) => { fails.fetch_add(1, Ordering::Relaxed); } } } })); }
      { let (sql, stop, del_n) = (sql.clone(), stop.clone(), del_n.clone()); hs.push(std::thread::spawn(move || { let mut s = Session::default(); let dq = sql.prepare("DELETE FROM users WHERE id = ?").unwrap(); let mut r = Rng(99); while !stop.load(Ordering::Relaxed) { let id = 600_000 + (r.next() % 400_000) as i64; if let Ok(rows) = sql.execute_s(&mut s, &dq, &[Value::Int(id)]) { if rows.first().map(|r| r[0] == Value::Int(1)).unwrap_or(false) { del_n.fetch_add(1, Ordering::Relaxed); } } } })); }
      std::thread::sleep(Duration::from_secs(2)); stop.store(true, Ordering::Relaxed); for h in hs { h.join().unwrap(); }
      println!("concurrent 2 s, 8 inserters + 1 cascading deleter: ours:      {} orders inserted, {} users deleted, {} inserts refused (parent gone) -> orphans {}", ins_n.load(Ordering::Relaxed), del_n.load(Ordering::Relaxed), fails.load(Ordering::Relaxed), show(ou(&sql, &mut s, ORPHANS))); }
    // 7. a schema change on the parent keeps the links
    sql.exec_s(&mut s, "ALTER TABLE users ADD COLUMN vip BOOL NOT NULL DEFAULT FALSE", &[]).unwrap(); sql.wait_alters().unwrap();
    println!("after ALTER TABLE users ADD COLUMN: orphan insert -> {} | valid insert -> {} | delete user 8 (has invoice) -> {}", show(ou(&sql, &mut s, "INSERT INTO orders VALUES (9000002, 999999999, 1.0)")), show(ou(&sql, &mut s, "INSERT INTO orders VALUES (9000003, 1, 1.0)")), show(ou(&sql, &mut s, "DELETE FROM users WHERE id = 8")));
    drop(dq); drop(ins); drop(sql); drop(off); drop(on); for p in [DB_OFF, DB_ON] { for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{p}{suf}")).ok(); } } let _ = fs::remove_dir_all(DIR);
}
