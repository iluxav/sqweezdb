//! Experiment 24 (H33): correlated subqueries, same SQL both sides. 1M users, 5M orders, index on orders(user_id).
use dblab::sql::{Session, Sql};
use dblab::table::Value;
use dblab::util::Rng;
use rusqlite::{params, Connection};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

const USERS: i64 = 1_000_000; const ORDERS: i64 = 5_000_000;
const CU: &str = "CREATE TABLE users(id INTEGER PRIMARY KEY, score DOUBLE, country INT, age INT, name VARCHAR(32) UNIQUE, payload BLOB(64))";
const CO: &str = "CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INT, amount DOUBLE, status INT, payload BLOB(32))";
const QS: &[(&str, &str, usize)] = &[
    ("C1 EXISTS (orders of the user above 499)", "SELECT count(*) FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 499)", 3),
    ("C2 NOT EXISTS (users with no orders)", "SELECT count(*) FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)", 3),
    ("C3 scalar in WHERE: amount above the user's own average (outer bounded to 500k)", "SELECT count(*) FROM orders o WHERE o.id <= 500000 AND o.amount > (SELECT avg(o2.amount) FROM orders o2 WHERE o2.user_id = o.user_id)", 3),
    ("C4 scalar in SELECT: order count per user, 1000 users", "SELECT u.id, (SELECT count(*) FROM orders o WHERE o.user_id = u.id) FROM users u WHERE u.id >= 1 AND u.id < 1001", 3),
    ("C5 correlated IN: user_id IN (users whose country = order status), outer bounded to 1000", "SELECT count(*) FROM orders o WHERE o.id <= 1000 AND o.user_id IN (SELECT u.id FROM users u WHERE u.country = o.status)", 3),
    ("C6 scalar in SELECT: max order per user, 50k users", "SELECT u.id, (SELECT max(o.amount) FROM orders o WHERE o.user_id = u.id) FROM users u WHERE u.country = 7", 3),
];
fn user(id: i64, rng: &mut Rng) -> (i64, f64, i64, i64, String, Vec<u8>) { (id, (rng.next() % 100000) as f64 / 100.0, id % 20, 18 + id % 60, format!("u{id}@example.com"), vec![7u8; 64]) }
fn order(id: i64, rng: &mut Rng) -> (i64, i64, f64, i64, Vec<u8>) { (id, (rng.next() % USERS as u64) as i64 + 1, (rng.next() % 50000) as f64 / 100.0, id % 4, vec![3u8; 32]) }
fn fv(v: &Value) -> f64 { match v { Value::Float(f) => *f, Value::Int(i) => *i as f64, _ => 0.0 } }
fn median(iters: usize, mut f: impl FnMut() -> f64) -> (f64, f64) { let mut ts = vec![]; let mut v = 0.0; for _ in 0..iters { let t = Instant::now(); v = f(); ts.push(t.elapsed().as_secs_f64() * 1e3); } ts.sort_by(|a, b| a.partial_cmp(b).unwrap()); (ts[iters / 2], v) }

fn main() {
    println!("=== E24 / H33: correlated subqueries (same SQL both sides) ===");
    let p = PathBuf::from("data/nocow/e24.db"); for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
    let c = Connection::open(&p).unwrap();
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-262144; PRAGMA mmap_size=1073741824;").unwrap();
    c.execute_batch(CU).unwrap(); c.execute_batch(CO).unwrap();
    let mut rng = Rng(7); let tx = c.unchecked_transaction().unwrap();
    { let mut st = tx.prepare("INSERT INTO users VALUES (?,?,?,?,?,?)").unwrap(); for id in 1..=USERS { let u = user(id, &mut rng); st.execute(params![u.0, u.1, u.2, u.3, u.4, u.5]).unwrap(); } }
    { let mut st = tx.prepare("INSERT INTO orders VALUES (?,?,?,?,?)").unwrap(); for id in 1..=ORDERS { let o = order(id, &mut rng); st.execute(params![o.0, o.1, o.2, o.3, o.4]).unwrap(); } }
    tx.commit().unwrap();
    c.execute_batch("CREATE INDEX o_user ON orders(user_id); CREATE INDEX u_country ON users(country); ANALYZE; PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    let _: f64 = c.query_row("SELECT sum(amount) FROM orders", [], |r| r.get(0)).unwrap();
    let mut sq: Vec<(f64, f64)> = vec![];
    for (_, q, it) in QS { sq.push(median(*it, || { let mut st = c.prepare_cached(q).unwrap(); let n = st.column_count(); st.query_map([], |r| r.get::<_, Option<f64>>(n - 1)).unwrap().map(|x| x.unwrap().unwrap_or(0.0)).sum() })); }
    drop(c); for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }

    let dir = PathBuf::from("data/nocow/e24dir"); let sql = Sql::create(&dir, false); let mut app = Session::default();
    sql.exec_s(&mut app, CU, &[]).unwrap(); sql.exec_s(&mut app, CO, &[]).unwrap();
    let iu = sql.prepare("INSERT INTO users VALUES (?,?,?,?,?,?)").unwrap(); let io = sql.prepare("INSERT INTO orders VALUES (?,?,?,?,?)").unwrap();
    let mut rng = Rng(7);
    for id in 1..=USERS { if id % 1000 == 1 { sql.exec_s(&mut app, "BEGIN", &[]).unwrap(); } let u = user(id, &mut rng); sql.execute_s(&mut app, &iu, &[Value::Int(u.0), Value::Float(u.1), Value::Int(u.2), Value::Int(u.3), Value::Text(u.4), Value::Bytes(u.5)]).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut app, "COMMIT", &[]).unwrap(); } }
    for id in 1..=ORDERS { if id % 1000 == 1 { sql.exec_s(&mut app, "BEGIN", &[]).unwrap(); } let o = order(id, &mut rng); sql.execute_s(&mut app, &io, &[Value::Int(o.0), Value::Int(o.1), Value::Float(o.2), Value::Int(o.3), Value::Bytes(o.4)]).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut app, "COMMIT", &[]).unwrap(); } }
    sql.exec_s(&mut app, "CREATE INDEX o_user ON orders(user_id)", &[]).unwrap();
    for (i, (label, q, it)) in QS.iter().enumerate() {
        let r = median(*it, || match sql.exec_s(&mut app, q, &[]) { Ok(rows) => rows.iter().map(|x| fv(x.last().unwrap())).sum(), Err(e) => { println!("ERROR {label}: {e}"); f64::NAN } });
        let same = (r.1 - sq[i].1).abs() < 1e-3 * sq[i].1.abs().max(1.0);
        println!("{:<78} sqlite {:>9.1} ms   ours {:>9.1} ms   {:>4}x   {}", label, sq[i].0, r.0, (sq[i].0 / r.0) as i64, if same { "same result".to_string() } else { format!("RESULT DIFFERS: {} vs {}", r.1, sq[i].1) });
    }
    drop(iu); drop(io); drop(sql); let _ = fs::remove_dir_all(&dir);
}
