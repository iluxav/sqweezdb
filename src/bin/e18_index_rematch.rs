//! Experiment 18 (H27 + H28): non-unique indexes (rematch of J4 and the Q5 shape) and the monomorphized scan path (re-measure E13/E15 queries).
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
const Q5: &str = "SELECT count(*), sum(amount) FROM orders WHERE user_id = ?";
const J4: &str = "SELECT u.id, sum(o.amount) FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id >= ? AND u.id < ? GROUP BY u.id";
const J6: &str = "SELECT o.id, o.amount FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id = ?";
const J1: &str = "SELECT count(*), sum(o.amount) FROM orders o JOIN users u ON u.id = o.user_id WHERE u.country = 7";
const J2: &str = "SELECT u.country, sum(o.amount) FROM orders o JOIN users u ON u.id = o.user_id GROUP BY u.country";
const J3: &str = "SELECT count(*) FROM orders o, users u WHERE o.user_id = u.id AND u.age > 40 AND o.status = 2";
const Q3: &str = "SELECT count(*) FROM users WHERE age > 40";
const Q4: &str = "SELECT country, avg(score), count(*) FROM users GROUP BY country";
const Q7: &str = "SELECT sum(score) FROM users WHERE country = 7 AND age BETWEEN 30 AND 50";
const Q6: &str = "SELECT id, score FROM users ORDER BY score DESC LIMIT 10";

fn user(id: i64, rng: &mut Rng) -> Vec<Value> { vec![Value::Int(id), Value::Float((rng.next() % 100000) as f64 / 100.0), Value::Int(id % 20), Value::Int(18 + id % 60), Value::Text(format!("u{id}@example.com")), Value::Bytes(vec![7u8; 64])] }
fn order(id: i64, rng: &mut Rng) -> Vec<Value> { vec![Value::Int(id), Value::Int((rng.next() % USERS as u64) as i64 + 1), Value::Float((rng.next() % 50000) as f64 / 100.0), Value::Int(id % 4), Value::Bytes(vec![3u8; 32])] }
fn fv(v: &Value) -> f64 { match v { Value::Float(f) => *f, Value::Int(i) => *i as f64, _ => 0.0 } }
fn median(iters: usize, mut f: impl FnMut() -> f64) -> (f64, f64) { let mut ts = vec![]; let mut v = 0.0; for _ in 0..iters { let t = Instant::now(); v = f(); ts.push(t.elapsed().as_secs_f64() * 1e3); } ts.sort_by(|a, b| a.partial_cmp(b).unwrap()); (ts[iters / 2], v) }
fn prm(label: &str, (ms, v): (f64, f64), reference: &str) { println!("{:<58} median {:>9.3} ms   {reference}   (check {v:.1})", label, ms); }
fn prc(label: &str, n: usize, el: f64, v: f64, reference: &str) { println!("{:<58} {:>9.3} us/call   {reference}   (check {v:.1})", label, el * 1e6 / n as f64); }

fn sqlite_refs() -> (f64, f64, f64) {
    let p = PathBuf::from("data/nocow/e18.db"); for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
    let c = Connection::open(&p).unwrap();
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-262144; PRAGMA mmap_size=1073741824;").unwrap();
    c.execute_batch(CU).unwrap(); c.execute_batch(CO).unwrap();
    let mut rng = Rng(7); let tx = c.unchecked_transaction().unwrap();
    { let mut st = tx.prepare("INSERT INTO users VALUES (?,?,?,?,?,?)").unwrap(); for id in 1..=USERS { let u = user(id, &mut rng); st.execute(params![id, fv(&u[1]), id % 20, 18 + id % 60, format!("u{id}@example.com"), &[7u8; 64][..]]).unwrap(); } }
    { let mut st = tx.prepare("INSERT INTO orders VALUES (?,?,?,?,?)").unwrap(); for id in 1..=ORDERS { let o = order(id, &mut rng); st.execute(params![id, fv(&o[1]) as i64, fv(&o[2]), id % 4, &[3u8; 32][..]]).unwrap(); } }
    tx.commit().unwrap();
    let t = Instant::now(); c.execute_batch("CREATE INDEX orders_user ON orders(user_id); ANALYZE; PRAGMA wal_checkpoint(TRUNCATE);").unwrap(); let ib = t.elapsed().as_secs_f64();
    let mut rng = Rng(99);
    let q5 = { let mut st = c.prepare(Q5).unwrap(); let t = Instant::now(); for _ in 0..200_000 { let u = (rng.next() % USERS as u64) as i64 + 1; let _: Option<f64> = st.query_row(params![u], |r| r.get(1)).unwrap(); } t.elapsed().as_secs_f64() * 1e6 / 200_000.0 };
    let j4 = { let mut st = c.prepare(J4).unwrap(); let t = Instant::now(); for _ in 0..2000 { let s = (rng.next() % (USERS - 100) as u64) as i64 + 1; let _: f64 = st.query_map(params![s, s + 100], |r| r.get::<_, f64>(1)).unwrap().map(|x| x.unwrap()).sum(); } t.elapsed().as_secs_f64() * 1e6 / 2000.0 };
    let j6 = { let mut st = c.prepare(J6).unwrap(); let t = Instant::now(); for _ in 0..200_000 { let u = (rng.next() % USERS as u64) as i64 + 1; let _: f64 = st.query_map(params![u], |r| r.get::<_, f64>(1)).unwrap().map(|x| x.unwrap()).sum(); } t.elapsed().as_secs_f64() * 1e6 / 200_000.0 };
    println!("[sqlite] index build on 5M orders {ib:.2}s; Q5 {q5:.2} us; J4 {j4:.1} us; J6 {j6:.2} us");
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
    (q5, j4, j6)
}

fn main() {
    println!("=== E18: non-unique indexes (rematch) + monomorphized scans ===");
    let (sq5, sj4, sj6) = sqlite_refs();
    let dir = PathBuf::from("data/nocow/e18dir");
    let sql = Arc::new(Sql::create(&dir, false)); let mut app = Session::default();
    sql.exec_s(&mut app, CU, &[]).unwrap(); sql.exec_s(&mut app, CO, &[]).unwrap();
    let iu = sql.prepare("INSERT INTO users VALUES (?,?,?,?,?,?)").unwrap(); let io = sql.prepare("INSERT INTO orders VALUES (?,?,?,?,?)").unwrap();
    let mut rng = Rng(7);
    for id in 1..=USERS { if id % 1000 == 1 { sql.exec_s(&mut app, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut app, &iu, &user(id, &mut rng)).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut app, "COMMIT", &[]).unwrap(); } }
    let t = Instant::now();
    for id in 1..=ORDERS { if id % 1000 == 1 { sql.exec_s(&mut app, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut app, &io, &order(id, &mut rng)).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut app, "COMMIT", &[]).unwrap(); } }
    let load_no_index = t.elapsed().as_secs_f64();
    println!("--- monomorphized scan path (compare E13/E15) ---");
    prm("Q3 count(*) WHERE age > 40", median(7, || { let r = sql.exec_s(&mut app, Q3, &[]).unwrap(); fv(&r[0][0]) }), "E13: 3.77 ms, SQLite 21.1 ms");
    prm("Q4 GROUP BY country", median(7, || { let r = sql.exec_s(&mut app, Q4, &[]).unwrap(); r.len() as f64 }), "E13: 5.44 ms, SQLite 109.9 ms");
    prm("Q7 filtered sum", median(7, || { let r = sql.exec_s(&mut app, Q7, &[]).unwrap(); fv(&r[0][0]) }), "E13: 3.76 ms, SQLite 20.7 ms");
    prm("Q6 ORDER BY score DESC LIMIT 10", median(7, || { let r = sql.exec_s(&mut app, Q6, &[]).unwrap(); r.len() as f64 }), "E13: 16.8 ms, SQLite 27.2 ms");
    prm("J1 join + filter", median(5, || { let r = sql.exec_s(&mut app, J1, &[]).unwrap(); fv(&r[0][1]) }), "E15: 96.8 ms, SQLite 181 ms");
    prm("J2 join + GROUP BY", median(5, || { let r = sql.exec_s(&mut app, J2, &[]).unwrap(); r.iter().map(|x| fv(&x[1])).sum() }), "E15: 122 ms, SQLite 3,594 ms");
    prm("J3 comma join, filters both sides", median(5, || { let r = sql.exec_s(&mut app, J3, &[]).unwrap(); fv(&r[0][0]) }), "E15: 27.5 ms, SQLite 454 ms");
    println!("--- non-unique index: CREATE INDEX orders_user ON orders(user_id) ---");
    let t = Instant::now(); sql.exec_s(&mut app, "CREATE INDEX orders_user ON orders(user_id)", &[]).unwrap(); let ib = t.elapsed().as_secs_f64();
    let entries = sql.db.table("orders").unwrap().index_on(1).unwrap().entries();
    println!("index built over 5M rows in {ib:.3}s ({entries} entries)   SQLite: see above");
    let mut rng = Rng(99);
    { let q = sql.prepare(Q5).unwrap(); let t = Instant::now(); let mut v = 0.0; for _ in 0..200_000 { let u = (rng.next() % USERS as u64) as i64 + 1; let r = sql.execute_s(&mut app, &q, &[Value::Int(u)]).unwrap(); v += fv(&r[0][1]); } prc("Q5 orders of one user via index: count, sum", 200_000, t.elapsed().as_secs_f64(), v, &format!("SQLite {sq5:.2} us")); }
    { let q = sql.prepare(J4).unwrap(); let t = Instant::now(); let mut v = 0.0; for _ in 0..2000 { let s = (rng.next() % (USERS - 100) as u64) as i64 + 1; let r = sql.execute_s(&mut app, &q, &[Value::Int(s), Value::Int(s + 100)]).unwrap(); v += r.iter().map(|x| fv(&x[1])).sum::<f64>(); } prc("J4 100 users -> their orders, group by user (index join)", 2000, t.elapsed().as_secs_f64(), v, &format!("SQLite {sj4:.1} us, E15 (no index) 24,459 us")); }
    { let q = sql.prepare(J6).unwrap(); let t = Instant::now(); let mut v = 0.0; for _ in 0..200_000 { let u = (rng.next() % USERS as u64) as i64 + 1; let r = sql.execute_s(&mut app, &q, &[Value::Int(u)]).unwrap(); v += r.iter().map(|x| fv(&x[1])).sum::<f64>(); } prc("J6 one user -> its orders (PK + index join)", 200_000, t.elapsed().as_secs_f64(), v, &format!("SQLite {sj6:.2} us")); }
    // maintenance cost: insert 1M more orders with the index present
    let t = Instant::now();
    for id in ORDERS + 1..=ORDERS + 1_000_000 { if id % 1000 == 1 { sql.exec_s(&mut app, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut app, &io, &order(id, &mut rng)).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut app, "COMMIT", &[]).unwrap(); } }
    let with = t.elapsed().as_secs_f64();
    println!("insert rate: {:.0} rows/s without the index, {:.0} rows/s with it (1000-row txns)", ORDERS as f64 / load_no_index, 1_000_000.0 / with);
    let r = sql.exec_s(&mut app, Q5, &[Value::Int(1)]).unwrap(); println!("sanity after inserts: user 1 has {:?} orders", r[0][0]);
    drop(iu); drop(io); drop(sql); let _ = fs::remove_dir_all(&dir);
}
