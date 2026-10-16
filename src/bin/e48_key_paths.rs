//! Experiment 48: the primary-key paths, measured before and after widening keys to 128 bits (UUID primary keys).
//! INT keys must not get slower; UUID keys are measured on the same paths once supported (pass `uuid`).
use sqweezdb::config::{Config, SnapshotPolicy};
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::Value;
use std::time::Instant;

const N: i64 = 1_000_000; const ROOT: &str = "data/nocow/e48";
fn pr(label: &str, n: i64, el: f64) { println!("{label:<62} {:>9.3} us/op {:>11.0}/s", el * 1e6 / n as f64, n as f64 / el); }
fn cfg() -> Config { Config { snapshot: SnapshotPolicy { log_bytes: None, idle_after: None }, ..Config::default() } }
fn main() {
    let uuid = std::env::args().nth(1).as_deref() == Some("uuid");
    let kt = if uuid { "UUID" } else { "INT" };
    println!("=== E48: primary-key paths with {kt} keys (1M rows, in-process) ===");
    let d = std::path::Path::new(ROOT).join(kt); let _ = std::fs::remove_dir_all(&d);
    let key = |i: i64| -> Value { if uuid { Value::Text(format!("0190f3a2-{:04x}-7000-8000-{:012x}", (i >> 48) & 0xffff, i & 0xffff_ffff_ffff)) } else { Value::Int(i) } };
    let sql = Sql::open_with(&d, cfg()).unwrap(); let mut s = Session::default();
    sql.exec_s(&mut s, &format!("CREATE TABLE users(id {kt} PRIMARY KEY, name VARCHAR(16))"), &[]).unwrap();
    sql.exec_s(&mut s, &format!("CREATE TABLE orders(id {kt} PRIMARY KEY, user_id {kt} NOT NULL REFERENCES users(id) ON DELETE CASCADE, amount DOUBLE)"), &[]).unwrap();
    let ins = sql.prepare("INSERT INTO users VALUES (?, ?)").unwrap(); let t = Instant::now();
    for i in 1..=N { if i % 1000 == 1 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &[key(i), Value::Text("u".into())]).unwrap(); if i % 1000 == 0 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } }
    pr("insert users, 1000-row transactions", N, t.elapsed().as_secs_f64());
    let ino = sql.prepare("INSERT INTO orders VALUES (?, ?, ?)").unwrap(); let t = Instant::now();
    for i in 1..=N { if i % 1000 == 1 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ino, &[key(10_000_000 + i), key((i * 7) % N + 1), Value::Float(1.0)]).unwrap(); if i % 1000 == 0 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } }
    pr("insert orders (foreign key checked), 1000-row transactions", N, t.elapsed().as_secs_f64());
    let get = sql.prepare("SELECT name FROM users WHERE id = ?").unwrap(); let t = Instant::now();
    for i in 0..300_000i64 { assert_eq!(sql.execute_s(&mut s, &get, &[key((i * 7919) % N + 1)]).unwrap().len(), 1); }
    pr("point read by primary key", 300_000, t.elapsed().as_secs_f64());
    if !uuid { let rg = sql.prepare("SELECT count(*) FROM users WHERE id >= ? AND id <= ?").unwrap(); let t = Instant::now(); for i in 0..2000i64 { sql.execute_s(&mut s, &rg, &[Value::Int(i * 400 + 1), Value::Int(i * 400 + 1000)]).unwrap(); } pr("range count of 1000 keys", 2000, t.elapsed().as_secs_f64()); }
    let j = sql.prepare("SELECT o.amount, u.name FROM orders o JOIN users u ON o.user_id = u.id WHERE o.id = ?").unwrap(); let t = Instant::now();
    for i in 0..100_000i64 { assert_eq!(sql.execute_s(&mut s, &j, &[key(10_000_000 + (i * 7919) % N + 1)]).unwrap().len(), 1); }
    pr("join order -> user through the foreign key, by order key", 100_000, t.elapsed().as_secs_f64());
    let t = Instant::now(); let r = sql.exec_s(&mut s, "SELECT count(*) FROM orders o JOIN users u ON o.user_id = u.id", &[]).unwrap(); pr(&format!("full join of 1M orders to users ({:?} rows)", r[0][0]), 1, t.elapsed().as_secs_f64());
    let del = sql.prepare("DELETE FROM orders WHERE id = ?").unwrap(); let t = Instant::now();
    for i in 1..=100_000i64 { sql.execute_s(&mut s, &del, &[key(10_000_000 + i)]).unwrap(); }
    pr("delete by primary key, autocommit", 100_000, t.elapsed().as_secs_f64());
    let delu = sql.prepare("DELETE FROM users WHERE id = ?").unwrap(); let t = Instant::now();
    for i in 1..=20_000i64 { sql.execute_s(&mut s, &delu, &[key(i * 50)]).unwrap(); }
    pr("delete user, cascading to its orders", 20_000, t.elapsed().as_secs_f64());
    let users = sql.exec_s(&mut s, "SELECT count(*) FROM users", &[]).unwrap()[0][0].clone(); let orders = sql.exec_s(&mut s, "SELECT count(*) FROM orders", &[]).unwrap()[0][0].clone();
    drop(get); drop(ins); drop(ino); drop(j); drop(del); drop(delu); drop(sql);
    let t = Instant::now(); let sql = Sql::open_with(&d, cfg()).unwrap(); let el = t.elapsed().as_secs_f64();
    let (u2, o2) = (sql.exec_s(&mut s, "SELECT count(*) FROM users", &[]).unwrap()[0][0].clone(), sql.exec_s(&mut s, "SELECT count(*) FROM orders", &[]).unwrap()[0][0].clone());
    println!("{:<62} {:>9.1} ms (users {u2:?} = {users:?}, orders {o2:?} = {orders:?})", "restart replaying the log", el * 1e3); assert_eq!((u2, o2), (users.clone(), orders.clone()));
    sql.snapshot(); drop(sql);
    let t = Instant::now(); let sql = Sql::open_with(&d, cfg()).unwrap(); let el = t.elapsed().as_secs_f64();
    let get = sql.prepare("SELECT name FROM users WHERE id = ?").unwrap(); let t2 = Instant::now(); let mut hits = 0; for i in 0..300_000i64 { hits += sql.execute_s(&mut s, &get, &[key((i * 7919) % N + 1)]).unwrap().len(); }
    println!("{:<62} {:>9.1} ms; reads from the mapped snapshot {:.3} us ({hits} hits)", "restart from the snapshot", el * 1e3, t2.elapsed().as_secs_f64() * 1e6 / 300_000.0);
    assert_eq!(sql.exec_s(&mut s, "SELECT count(*) FROM orders", &[]).unwrap()[0][0], orders);
    drop(get); drop(sql); let _ = std::fs::remove_dir_all(ROOT);
}
