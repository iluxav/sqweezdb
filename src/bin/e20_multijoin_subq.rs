//! Experiment 20: three-table joins and subqueries through SQL, same statements on both sides. 1M users, 10k products, 5M orders.
use dblab::sql::{Session, Sql};
use dblab::table::Value;
use dblab::util::Rng;
use rusqlite::{params, Connection};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

const USERS: i64 = 1_000_000; const PRODUCTS: i64 = 10_000; const ORDERS: i64 = 5_000_000;
const CU: &str = "CREATE TABLE users(id INTEGER PRIMARY KEY, score DOUBLE, country INT, age INT, name VARCHAR(32) UNIQUE, payload BLOB(64))";
const CP: &str = "CREATE TABLE products(id INTEGER PRIMARY KEY, category INT, price DOUBLE, title VARCHAR(16))";
const CO: &str = "CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INT, product_id INT, amount DOUBLE, status INT, payload BLOB(32))";
const QS: &[(&str, &str, usize)] = &[
    ("T1 3-table join, filters on both small sides", "SELECT count(*), sum(o.amount) FROM orders o JOIN users u ON u.id = o.user_id JOIN products p ON p.id = o.product_id WHERE u.country = 7 AND p.category = 3", 5),
    ("T2 3-table join + GROUP BY product category", "SELECT p.category, sum(o.amount) FROM orders o JOIN users u ON u.id = o.user_id JOIN products p ON p.id = o.product_id WHERE u.age > 40 GROUP BY p.category", 3),
    ("T4 2-table join, category filter (SQLite has the index)", "SELECT count(*) FROM orders o JOIN products p ON p.id = o.product_id WHERE p.category = 3", 5),
    ("S1 IN (subquery) semi-join", "SELECT count(*), sum(amount) FROM orders WHERE user_id IN (SELECT id FROM users WHERE country = 7)", 5),
    ("S2 NOT IN (subquery)", "SELECT count(*) FROM orders WHERE product_id NOT IN (SELECT id FROM products WHERE category = 3)", 5),
    ("S3 IN (list)", "SELECT count(*) FROM orders WHERE status IN (1, 2)", 5),
    ("S4 scalar subquery", "SELECT count(*) FROM orders WHERE amount > (SELECT avg(amount) FROM orders)", 3),
    ("S5 IN (subquery over a non-key column)", "SELECT count(*) FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 499)", 3),
];
const T3: &str = "SELECT u.name, p.price FROM orders o JOIN users u ON u.id = o.user_id JOIN products p ON p.id = o.product_id WHERE o.id = ?";

fn user(id: i64, rng: &mut Rng) -> (i64, f64, i64, i64, String, Vec<u8>) { (id, (rng.next() % 100000) as f64 / 100.0, id % 20, 18 + id % 60, format!("u{id}@example.com"), vec![7u8; 64]) }
fn product(id: i64, rng: &mut Rng) -> (i64, i64, f64, String) { (id, id % 50, (rng.next() % 10000) as f64 / 100.0, format!("p{id}")) }
fn order(id: i64, rng: &mut Rng) -> (i64, i64, i64, f64, i64, Vec<u8>) { (id, (rng.next() % USERS as u64) as i64 + 1, (rng.next() % PRODUCTS as u64) as i64 + 1, (rng.next() % 50000) as f64 / 100.0, id % 4, vec![3u8; 32]) }
fn fv(v: &Value) -> f64 { match v { Value::Float(f) => *f, Value::Int(i) => *i as f64, _ => 0.0 } }
fn median(iters: usize, mut f: impl FnMut() -> f64) -> (f64, f64) { let mut ts = vec![]; let mut v = 0.0; for _ in 0..iters { let t = Instant::now(); v = f(); ts.push(t.elapsed().as_secs_f64() * 1e3); } ts.sort_by(|a, b| a.partial_cmp(b).unwrap()); (ts[iters / 2], v) }

fn main() {
    println!("=== E20: three-table joins and subqueries (same SQL both sides) ===");
    let p = PathBuf::from("data/nocow/e20.db"); for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
    let c = Connection::open(&p).unwrap();
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-262144; PRAGMA mmap_size=1073741824;").unwrap();
    for st in [CU, CP, CO] { c.execute_batch(st).unwrap(); }
    let mut rng = Rng(7); let tx = c.unchecked_transaction().unwrap();
    { let mut st = tx.prepare("INSERT INTO users VALUES (?,?,?,?,?,?)").unwrap(); for id in 1..=USERS { let u = user(id, &mut rng); st.execute(params![u.0, u.1, u.2, u.3, u.4, u.5]).unwrap(); } }
    { let mut st = tx.prepare("INSERT INTO products VALUES (?,?,?,?)").unwrap(); for id in 1..=PRODUCTS { let x = product(id, &mut rng); st.execute(params![x.0, x.1, x.2, x.3]).unwrap(); } }
    { let mut st = tx.prepare("INSERT INTO orders VALUES (?,?,?,?,?,?)").unwrap(); for id in 1..=ORDERS { let o = order(id, &mut rng); st.execute(params![o.0, o.1, o.2, o.3, o.4, o.5]).unwrap(); } }
    tx.commit().unwrap();
    c.execute_batch("CREATE INDEX o_user ON orders(user_id); CREATE INDEX o_product ON orders(product_id); ANALYZE; PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    let _: f64 = c.query_row("SELECT sum(amount) FROM orders", [], |r| r.get(0)).unwrap();
    let mut sq: Vec<(f64, f64)> = vec![];
    for (_, q, it) in QS { sq.push(median(*it, || { let mut st = c.prepare_cached(q).unwrap(); let n = st.column_count(); st.query_map([], |r| r.get::<_, f64>(n - 1)).unwrap().map(|x| x.unwrap()).sum() })); }
    let mut rng = Rng(99);
    let st3 = { let mut st = c.prepare(T3).unwrap(); let t = Instant::now(); let mut v = 0.0; for _ in 0..200_000 { let id = (rng.next() % ORDERS as u64) as i64 + 1; v += st.query_row(params![id], |r| r.get::<_, f64>(1)).unwrap(); } (t.elapsed().as_secs_f64() * 1e6 / 200_000.0, v) };
    drop(c); for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }

    let dir = PathBuf::from("data/nocow/e20dir"); let sql = Sql::create(&dir, false); let mut app = Session::default();
    for st in [CU, CP, CO] { sql.exec_s(&mut app, st, &[]).unwrap(); }
    let iu = sql.prepare("INSERT INTO users VALUES (?,?,?,?,?,?)").unwrap(); let ip = sql.prepare("INSERT INTO products VALUES (?,?,?,?)").unwrap(); let io = sql.prepare("INSERT INTO orders VALUES (?,?,?,?,?,?)").unwrap();
    let mut rng = Rng(7);
    for id in 1..=USERS { if id % 1000 == 1 { sql.exec_s(&mut app, "BEGIN", &[]).unwrap(); } let u = user(id, &mut rng); sql.execute_s(&mut app, &iu, &[Value::Int(u.0), Value::Float(u.1), Value::Int(u.2), Value::Int(u.3), Value::Text(u.4), Value::Bytes(u.5)]).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut app, "COMMIT", &[]).unwrap(); } }
    for id in 1..=PRODUCTS { let x = product(id, &mut rng); sql.execute_s(&mut app, &ip, &[Value::Int(x.0), Value::Int(x.1), Value::Float(x.2), Value::Text(x.3)]).unwrap(); }
    for id in 1..=ORDERS { if id % 1000 == 1 { sql.exec_s(&mut app, "BEGIN", &[]).unwrap(); } let o = order(id, &mut rng); sql.execute_s(&mut app, &io, &[Value::Int(o.0), Value::Int(o.1), Value::Int(o.2), Value::Float(o.3), Value::Int(o.4), Value::Bytes(o.5)]).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut app, "COMMIT", &[]).unwrap(); } }
    sql.exec_s(&mut app, "CREATE INDEX o_user ON orders(user_id)", &[]).unwrap(); sql.exec_s(&mut app, "CREATE INDEX o_product ON orders(product_id)", &[]).unwrap();
    for (i, (label, q, it)) in QS.iter().enumerate() {
        let r = median(*it, || match sql.exec_s(&mut app, q, &[]) { Ok(rows) => rows.iter().map(|x| fv(x.last().unwrap())).sum(), Err(e) => { println!("ERROR {label}: {e}"); f64::NAN } });
        let same = (r.1 - sq[i].1).abs() < 1e-3 * sq[i].1.abs().max(1.0);
        println!("{:<58} sqlite {:>9.3} ms   ours {:>9.3} ms   {}x   {}", label, sq[i].0, r.0, (sq[i].0 / r.0) as i64, if same { "same result".to_string() } else { format!("RESULT DIFFERS: {} vs {}", r.1, sq[i].1) });
    }
    let mut rng = Rng(99);
    let q3 = sql.prepare(T3).unwrap();
    let t = Instant::now(); let mut v = 0.0; for _ in 0..200_000 { let id = (rng.next() % ORDERS as u64) as i64 + 1; let r = sql.execute_s(&mut app, &q3, &[Value::Int(id)]).unwrap(); v += fv(&r[0][1]); }
    let us = t.elapsed().as_secs_f64() * 1e6 / 200_000.0;
    println!("{:<58} sqlite {:>9.3} us   ours {:>9.3} us   {}x   {}", "T3 one order with its user and product (point, 3 tables)", st3.0, us, (st3.0 / us) as i64, if (v - st3.1).abs() < 1e-3 * st3.1.abs() { "same result" } else { "RESULT DIFFERS" });
    drop(q3); drop(iu); drop(ip); drop(io); drop(sql); let _ = fs::remove_dir_all(&dir);
}
