//! Experiment 19b (H28): where does the scan time go? Same 5M-row orders table, measured layer by layer.
use dblab::sql::{Session, Sql};
use dblab::table::{read_i64, Value, CHUNK_ROWS};
use dblab::util::Rng;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

const USERS: i64 = 1_000_000; const ORDERS: i64 = 5_000_000;
fn median(iters: usize, mut f: impl FnMut() -> f64) -> (f64, f64) { let mut ts = vec![]; let mut v = 0.0; for _ in 0..iters { let t = Instant::now(); v = f(); ts.push(t.elapsed().as_secs_f64() * 1e3); } ts.sort_by(|a, b| a.partial_cmp(b).unwrap()); (ts[iters / 2], v) }
fn pr(label: &str, (ms, v): (f64, f64), rows: f64) { println!("{:<70} {:>9.3} ms  {:>6.2} ns/row   (check {v:.1})", label, ms, ms * 1e6 / rows); }

fn main() {
    println!("=== E19b: scan layers over 5M orders (stride = 16 B header + row) ===");
    let dir = PathBuf::from("data/nocow/e19b"); let sql = Sql::create(&dir, false); let mut app = Session::default();
    sql.exec_s(&mut app, "CREATE TABLE users(id INTEGER PRIMARY KEY, score DOUBLE, country INT, age INT, name VARCHAR(32) UNIQUE, payload BLOB(64))", &[]).unwrap();
    sql.exec_s(&mut app, "CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INT, amount DOUBLE, status INT, payload BLOB(32))", &[]).unwrap();
    let iu = sql.prepare("INSERT INTO users VALUES (?,?,?,?,?,?)").unwrap(); let io = sql.prepare("INSERT INTO orders VALUES (?,?,?,?,?)").unwrap();
    let mut rng = Rng(7);
    for id in 1..=USERS { if id % 1000 == 1 { sql.exec_s(&mut app, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut app, &iu, &[Value::Int(id), Value::Float((rng.next() % 100000) as f64 / 100.0), Value::Int(id % 20), Value::Int(18 + id % 60), Value::Text(format!("u{id}@example.com")), Value::Bytes(vec![7; 64])]).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut app, "COMMIT", &[]).unwrap(); } }
    for id in 1..=ORDERS { if id % 1000 == 1 { sql.exec_s(&mut app, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut app, &io, &[Value::Int(id), Value::Int((rng.next() % USERS as u64) as i64 + 1), Value::Float((rng.next() % 50000) as f64 / 100.0), Value::Int(id % 4), Value::Bytes(vec![3; 32])]).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut app, "COMMIT", &[]).unwrap(); } }
    let orders = sql.db.table("orders").unwrap(); let users = sql.db.table("users").unwrap();
    let (stride, uoff, aoff) = (orders.stride, orders.schema.offsets[1], orders.schema.offsets[2]);
    println!("orders: row {} B, stride {} B, {} chunks", orders.schema.row_size, stride, orders.chunk_count());
    let n = ORDERS as f64;
    // L0: a plain Vec<i64> of the keys (bandwidth floor for reading one column)
    let mut keys: Vec<i64> = Vec::with_capacity(ORDERS as usize); orders.scan(|r| { keys.push(read_i64(r, uoff)); true });
    pr("L0 sum over Vec<i64> keys (8 B/row, the column floor)", median(5, || keys.iter().fold(0i64, |a, &k| a.wrapping_add(k)) as f64), n);
    // L1: raw chunk walk, no version check
    pr("L1 raw chunk walk: read key at stride, no header check", median(5, || { let mut s = 0i64; for ck in 0..orders.chunk_count() { orders.with_chunk(ck, |d| { let mut off = 16 + uoff; for _ in 0..CHUNK_ROWS { if off + 8 <= d.len() { s = s.wrapping_add(read_i64(d, off)); } off += stride; } }); } s as f64 }), n);
    // L2: raw chunk walk with the version check
    pr("L2 raw chunk walk + version>0 check", median(5, || { let mut s = 0i64; for ck in 0..orders.chunk_count() { orders.with_chunk(ck, |d| { let mut off = 0; for _ in 0..CHUNK_ROWS { if off + stride <= d.len() && u64::from_le_bytes(d[off..off + 8].try_into().unwrap()) > 0 { s = s.wrapping_add(read_i64(d, off + 16 + uoff)); } off += stride; } }); } s as f64 }), n);
    // L3: Table::scan closure
    pr("L3 Table::scan closure summing the key", median(5, || { let mut s = 0i64; orders.scan(|r| { s = s.wrapping_add(read_i64(r, uoff)); true }); s as f64 }), n);
    // L4: scan + bitmap probe (J1 shape, hand-rolled over the engine)
    let mut bits = vec![0u64; (USERS as usize >> 6) + 2]; users.scan(|r| { if read_i64(r, users.schema.offsets[2]) == 7 { let id = read_i64(r, users.schema.offsets[0]) as usize; bits[id >> 6] |= 1 << (id & 63); } true });
    pr("L4 Table::scan + bitmap probe + count/sum on hits (J1 by hand)", median(5, || { let mut c = 0u64; let mut s = 0.0; orders.scan(|r| { let k = read_i64(r, uoff) as usize; if bits[k >> 6] >> (k & 63) & 1 == 1 { c += 1; s += dblab::table::read_f64(r, aoff); } true }); let _ = c; s }), n);
    // L5: through SQL
    pr("L5 SQL: SELECT count(*), sum(amount) FROM orders", median(5, || { let r = sql.exec_s(&mut app, "SELECT count(*), sum(amount) FROM orders", &[]).unwrap(); match r[0][1] { Value::Float(f) => f, _ => 0.0 } }), n);
    pr("L6 SQL: SELECT count(*) FROM orders WHERE status = 2", median(5, || { let r = sql.exec_s(&mut app, "SELECT count(*) FROM orders WHERE status = 2", &[]).unwrap(); match r[0][0] { Value::Int(i) => i as f64, _ => 0.0 } }), n);
    pr("L7 SQL J1: join + filter on users (E18: 91 ms)", median(5, || { let r = sql.exec_s(&mut app, "SELECT count(*), sum(o.amount) FROM orders o JOIN users u ON u.id = o.user_id WHERE u.country = 7", &[]).unwrap(); match r[0][1] { Value::Float(f) => f, _ => 0.0 } }), n);
    pr("L8 SQL J2: join + GROUP BY country (E18: 118 ms)", median(3, || { let r = sql.exec_s(&mut app, "SELECT u.country, sum(o.amount) FROM orders o JOIN users u ON u.id = o.user_id GROUP BY u.country", &[]).unwrap(); r.len() as f64 }), n);
    drop(iu); drop(io); drop(sql); let _ = fs::remove_dir_all(&dir);
}
