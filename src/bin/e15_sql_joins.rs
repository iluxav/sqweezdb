//! Experiment 15 (H26): joins through SQL, same statements on both sides. 1M users, 5M orders.
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::Value;
use sqweezdb::util::Rng;
use rusqlite::{params, Connection};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

const USERS: i64 = 1_000_000; const ORDERS: i64 = 5_000_000;
const CU: &str = "CREATE TABLE users(id INTEGER PRIMARY KEY, score DOUBLE, country INT, age INT, name VARCHAR(32) UNIQUE, payload BLOB(64))";
const CO: &str = "CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INT, amount DOUBLE, status INT, payload BLOB(32))";
const J1: &str = "SELECT count(*), sum(o.amount) FROM orders o JOIN users u ON u.id = o.user_id WHERE u.country = 7";
const J2: &str = "SELECT u.country, sum(o.amount) FROM orders o JOIN users u ON u.id = o.user_id GROUP BY u.country";
const J3: &str = "SELECT count(*) FROM orders o, users u WHERE o.user_id = u.id AND u.age > 40 AND o.status = 2";
const J4: &str = "SELECT u.id, sum(o.amount) FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id >= ? AND u.id < ? GROUP BY u.id";
const J5: &str = "SELECT u.name, o.amount FROM orders o JOIN users u ON u.id = o.user_id WHERE o.id = ?";

fn user(id: i64, rng: &mut Rng) -> (i64, f64, i64, i64, String, Vec<u8>) { (id, (rng.next() % 100000) as f64 / 100.0, id % 20, 18 + id % 60, format!("u{id}@example.com"), vec![7u8; 64]) }
fn order(id: i64, rng: &mut Rng) -> (i64, i64, f64, i64, Vec<u8>) { (id, (rng.next() % USERS as u64) as i64 + 1, (rng.next() % 50000) as f64 / 100.0, id % 4, vec![3u8; 32]) }
fn median(iters: usize, mut f: impl FnMut() -> f64) -> (f64, f64) { let mut ts = vec![]; let mut v = 0.0; for _ in 0..iters { let t = Instant::now(); v = f(); ts.push(t.elapsed().as_secs_f64() * 1e3); } ts.sort_by(|a, b| a.partial_cmp(b).unwrap()); (ts[iters / 2], v) }
fn prm(label: &str, (ms, v): (f64, f64)) { println!("{:<66} median {:>10.3} ms   (check {v:.1})", label, ms); }
fn prc(label: &str, n: usize, el: f64, v: f64) { println!("{:<66} {:>9.2} us/call  {:>9.0} calls/s   (check {v:.1})", label, el * 1e6 / n as f64, n as f64 / el); }

fn sqlite_side() {
    let p = PathBuf::from("data/nocow/e15.db");
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
    let c = Connection::open(&p).unwrap();
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-262144; PRAGMA mmap_size=1073741824;").unwrap();
    c.execute_batch(CU).unwrap(); c.execute_batch(CO).unwrap();
    let mut rng = Rng(7);
    let tx = c.unchecked_transaction().unwrap();
    { let mut st = tx.prepare("INSERT INTO users VALUES (?,?,?,?,?,?)").unwrap(); for id in 1..=USERS { let u = user(id, &mut rng); st.execute(params![u.0, u.1, u.2, u.3, u.4, u.5]).unwrap(); } }
    { let mut st = tx.prepare("INSERT INTO orders VALUES (?,?,?,?,?)").unwrap(); for id in 1..=ORDERS { let o = order(id, &mut rng); st.execute(params![o.0, o.1, o.2, o.3, o.4]).unwrap(); } }
    tx.commit().unwrap();
    c.execute_batch("CREATE INDEX orders_user ON orders(user_id); ANALYZE; PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    let _: f64 = c.query_row("SELECT sum(amount) FROM orders", [], |r| r.get(0)).unwrap();
    prm("[sqlite] J1 join + filter on users side: count, sum", median(5, || c.query_row(J1, [], |r| r.get::<_, f64>(1)).unwrap()));
    prm("[sqlite] J2 join + GROUP BY users.country", median(3, || { let mut st = c.prepare_cached(J2).unwrap(); st.query_map([], |r| r.get::<_, f64>(1)).unwrap().map(|x| x.unwrap()).sum() }));
    prm("[sqlite] J3 comma join, filters on both sides: count", median(5, || c.query_row(J3, [], |r| r.get::<_, i64>(0)).unwrap() as f64));
    let mut rng = Rng(99);
    { let mut st = c.prepare(J4).unwrap(); let t = Instant::now(); let mut v = 0.0; for _ in 0..2000 { let s = (rng.next() % (USERS - 100) as u64) as i64 + 1; v += st.query_map(params![s, s + 100], |r| r.get::<_, f64>(1)).unwrap().map(|x| x.unwrap()).sum::<f64>(); } prc("[sqlite] J4 100 users -> their orders, group by user (index on orders.user_id)", 2000, t.elapsed().as_secs_f64(), v); }
    { let mut st = c.prepare(J5).unwrap(); let t = Instant::now(); let mut v = 0.0; for _ in 0..200_000 { let id = (rng.next() % ORDERS as u64) as i64 + 1; v += st.query_row(params![id], |r| r.get::<_, f64>(1)).unwrap(); } prc("[sqlite] J5 one order with its user (point join)", 200_000, t.elapsed().as_secs_f64(), v); }
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
}

fn ours_side() {
    let dir = PathBuf::from("data/nocow/e15dir"); let _ = fs::remove_dir_all(&dir);
    let sql = Arc::new(Sql::recreate(&dir, false));
    sql.exec(CU, &[]).unwrap(); sql.exec(CO, &[]).unwrap();
    let iu = sql.prepare("INSERT INTO users VALUES (?,?,?,?,?,?)").unwrap(); let io = sql.prepare("INSERT INTO orders VALUES (?,?,?,?,?)").unwrap();
    let mut rng = Rng(7); let mut sess = Session::default();
    let t = Instant::now();
    for id in 1..=USERS { if id % 1000 == 1 { sql.exec_s(&mut sess, "BEGIN", &[]).unwrap(); } let u = user(id, &mut rng); sql.execute_s(&mut sess, &iu, &[Value::Int(u.0), Value::Float(u.1), Value::Int(u.2), Value::Int(u.3), Value::Text(u.4), Value::Bytes(u.5)]).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut sess, "COMMIT", &[]).unwrap(); } }
    for id in 1..=ORDERS { if id % 1000 == 1 { sql.exec_s(&mut sess, "BEGIN", &[]).unwrap(); } let o = order(id, &mut rng); sql.execute_s(&mut sess, &io, &[Value::Int(o.0), Value::Int(o.1), Value::Float(o.2), Value::Int(o.3), Value::Bytes(o.4)]).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut sess, "COMMIT", &[]).unwrap(); } }
    println!("[ours]   loaded 1M users + 5M orders through SQL in {:.1}s (no index on orders.user_id: we have none yet)", t.elapsed().as_secs_f64());
    let f = |r: &Vec<Vec<Value>>, i: usize, j: usize| -> f64 { match &r[i][j] { Value::Float(f) => *f, Value::Int(i) => *i as f64, _ => 0.0 } };
    prm("[ours]   J1 join + filter on users side: count, sum", median(5, || { let r = sql.exec(J1, &[]).unwrap(); f(&r, 0, 1) }));
    prm("[ours]   J2 join + GROUP BY users.country", median(5, || { let r = sql.exec(J2, &[]).unwrap(); r.iter().map(|x| match x[1] { Value::Float(v) => v, _ => 0.0 }).sum() }));
    prm("[ours]   J3 comma join, filters on both sides: count", median(5, || { let r = sql.exec(J3, &[]).unwrap(); f(&r, 0, 0) }));
    let mut rng = Rng(99);
    { let q = sql.prepare(J4).unwrap(); let t = Instant::now(); let mut v = 0.0; let n = 20; for _ in 0..n { let s = (rng.next() % (USERS - 100) as u64) as i64 + 1; let r = sql.execute(&q, &[Value::Int(s), Value::Int(s + 100)]).unwrap(); v += r.iter().map(|x| match x[1] { Value::Float(v) => v, _ => 0.0 }).sum::<f64>(); } prc("[ours]   J4 100 users -> their orders, group by user (full scan of orders)", n, t.elapsed().as_secs_f64(), v); }
    { let q = sql.prepare(J5).unwrap(); let t = Instant::now(); let mut v = 0.0; for _ in 0..200_000 { let id = (rng.next() % ORDERS as u64) as i64 + 1; let r = sql.execute(&q, &[Value::Int(id)]).unwrap(); v += f(&r, 0, 1); } prc("[ours]   J5 one order with its user (point join)", 200_000, t.elapsed().as_secs_f64(), v); }
    drop(iu); drop(io); drop(sql); let _ = fs::remove_dir_all(&dir);
}

fn main() { println!("=== E15 / H26: joins through SQL ==="); sqlite_side(); ours_side(); }
