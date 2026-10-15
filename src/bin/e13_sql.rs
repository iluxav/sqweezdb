//! Experiment 13: the SQL round. Identical SQL text on both sides; ours through sqlparser -> plan -> typed storage.
use dblab::sql::Sql;
use dblab::table::Value;
use dblab::util::Rng;
use rusqlite::{Connection, params};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::Instant;

const N: i64 = 1_000_000;
const CREATE: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, age INT, name VARCHAR(32) UNIQUE, payload BLOB(64))";
const INSERT: &str = "INSERT INTO t VALUES (?,?,?,?,?,?)";
const Q1: &str = "SELECT score FROM t WHERE id = ?";
const Q2: &str = "SELECT id FROM t WHERE name = ?";
const Q3: &str = "SELECT count(*) FROM t WHERE age > 40";
const Q4: &str = "SELECT country, avg(score), count(*) FROM t GROUP BY country";
const Q5: &str = "SELECT sum(score) FROM t WHERE id >= ? AND id < ?";
const Q6: &str = "SELECT id, score FROM t ORDER BY score DESC LIMIT 10";
const Q7: &str = "SELECT sum(score) FROM t WHERE country = 7 AND age BETWEEN 30 AND 50";
const W1: &str = "UPDATE t SET score = ? WHERE id = ?";

fn row_vals(id: i64, rng: &mut Rng) -> (i64, f64, i64, i64, String, Vec<u8>) { (id, (rng.next() % 100000) as f64 / 100.0, id % 20, 18 + id % 60, format!("u{id}@example.com"), vec![7u8; 64]) }
fn pr(label: &str, calls: usize, el: f64) { println!("{:<58} {:>10.0} calls/s  {:>9.3} us/call", label, calls as f64 / el, el * 1e6 / calls as f64); }
fn prm(label: &str, ms: f64) { println!("{:<58} median {:>9.3} ms", label, ms); }
fn median(iters: usize, mut f: impl FnMut()) -> f64 { let mut ts = vec![]; for _ in 0..iters { let t = Instant::now(); f(); ts.push(t.elapsed().as_secs_f64() * 1e3); } ts.sort_by(|a, b| a.partial_cmp(b).unwrap()); ts[iters / 2] }

fn sqlite_side() {
    let p = PathBuf::from("data/nocow/e13.db");
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
    let c = Connection::open(&p).unwrap();
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-262144; PRAGMA mmap_size=1073741824;").unwrap();
    c.execute_batch(CREATE).unwrap();
    let t = Instant::now(); let mut rng = Rng(7);
    let tx = c.unchecked_transaction().unwrap();
    { let mut st = tx.prepare(INSERT).unwrap(); for id in 1..=N { let r = row_vals(id, &mut rng); st.execute(params![r.0, r.1, r.2, r.3, r.4, r.5]).unwrap(); } }
    tx.commit().unwrap();
    pr("[sqlite] load 1M rows via INSERT (one transaction)", N as usize, t.elapsed().as_secs_f64());
    // type enforcement check
    match c.execute(INSERT, params![N + 1, "abc", 1, 2, "x@example.com", &[1u8; 4][..]]) { Ok(_) => { let v: String = c.query_row("SELECT typeof(score) || ':' || score FROM t WHERE id = ?", params![N + 1], |r| r.get(0)).unwrap(); println!("[sqlite] text 'abc' into DOUBLE column: accepted, stored as {v}"); } Err(e) => println!("[sqlite] text into DOUBLE column: rejected ({e})") }
    let mut rng = Rng(99);
    { let mut st = c.prepare(Q1).unwrap(); let t = Instant::now(); let mut s = 0.0; for _ in 0..1_000_000 { let id = (rng.next() % N as u64) as i64 + 1; s += st.query_row(params![id], |r| r.get::<_, f64>(0)).unwrap(); } pr(&format!("[sqlite] Q1 point read by id (check {s:.0})"), 1_000_000, t.elapsed().as_secs_f64()); }
    { let threads = 8; let per = 500_000; let b = Arc::new(Barrier::new(threads + 1)); let hs: Vec<_> = (0..threads).map(|k| { let p = p.clone(); let b = b.clone(); std::thread::spawn(move || { let c = Connection::open(&p).unwrap(); c.execute_batch("PRAGMA cache_size=-262144; PRAGMA mmap_size=1073741824;").unwrap(); let mut st = c.prepare(Q1).unwrap(); let mut rng = Rng(1000 + k as u64); b.wait(); let mut s = 0.0; for _ in 0..per { let id = (rng.next() % N as u64) as i64 + 1; s += st.query_row(params![id], |r| r.get::<_, f64>(0)).unwrap(); } s }) }).collect();
      b.wait(); let t = Instant::now(); for h in hs { h.join().unwrap(); } pr("[sqlite] Q1 point read by id, 8 threads", threads * per, t.elapsed().as_secs_f64()); }
    { let mut st = c.prepare(Q2).unwrap(); let t = Instant::now(); let mut s = 0i64; for _ in 0..500_000 { let id = (rng.next() % N as u64) as i64 + 1; s += st.query_row(params![format!("u{id}@example.com")], |r| r.get::<_, i64>(0)).unwrap(); } pr(&format!("[sqlite] Q2 lookup by name (unique index) (check {s})"), 500_000, t.elapsed().as_secs_f64()); }
    prm("[sqlite] Q3 count(*) WHERE age > 40", median(7, || { let _: i64 = c.query_row(Q3, [], |r| r.get(0)).unwrap(); }));
    prm("[sqlite] Q4 GROUP BY country avg,count", median(7, || { let mut st = c.prepare_cached(Q4).unwrap(); let n = st.query_map([], |r| r.get::<_, f64>(1)).unwrap().count(); assert_eq!(n, 20); }));
    { let mut st = c.prepare(Q5).unwrap(); let t = Instant::now(); let mut s = 0.0; for _ in 0..20_000 { let a = (rng.next() % (N - 1000) as u64) as i64 + 1; s += st.query_row(params![a, a + 1000], |r| r.get::<_, f64>(0)).unwrap(); } pr(&format!("[sqlite] Q5 range sum, 1000 rows (check {s:.0})"), 20_000, t.elapsed().as_secs_f64()); }
    prm("[sqlite] Q6 ORDER BY score DESC LIMIT 10", median(7, || { let mut st = c.prepare_cached(Q6).unwrap(); let n = st.query_map([], |r| r.get::<_, i64>(0)).unwrap().count(); assert_eq!(n, 10); }));
    prm("[sqlite] Q7 sum WHERE country=7 AND age BETWEEN 30 AND 50", median(7, || { let _: f64 = c.query_row(Q7, [], |r| r.get(0)).unwrap(); }));
    { let mut st = c.prepare(W1).unwrap(); let t = Instant::now(); for i in 0..200_000 { let id = (rng.next() % N as u64) as i64 + 1; st.execute(params![i as f64, id]).unwrap(); } pr("[sqlite] W1 UPDATE by id, 1 row/txn, sync=NORMAL", 200_000, t.elapsed().as_secs_f64()); }
    { let mut st = c.prepare(INSERT).unwrap(); let t = Instant::now(); let mut rng = Rng(5); for i in 0..200_000 { let r = row_vals(N + 100 + i, &mut rng); st.execute(params![r.0, r.1, r.2, r.3, r.4, r.5]).unwrap(); } pr("[sqlite] W2 INSERT 1 row/txn, sync=NORMAL (not durable)", 200_000, t.elapsed().as_secs_f64()); }
    c.execute_batch("PRAGMA synchronous=FULL").unwrap();
    { let mut st = c.prepare(INSERT).unwrap(); let t = Instant::now(); let mut rng = Rng(6); for i in 0..2000 { let r = row_vals(N + 1_000_000 + i, &mut rng); st.execute(params![r.0, r.1, r.2, r.3, r.4, r.5]).unwrap(); } pr("[sqlite] W3 INSERT 1 row/txn, sync=FULL (durable), 1 thread", 2000, t.elapsed().as_secs_f64()); }
    drop(c);
    { let threads = 4; let per = 500; let b = Arc::new(Barrier::new(threads + 1)); let hs: Vec<_> = (0..threads).map(|k| { let p = p.clone(); let b = b.clone(); std::thread::spawn(move || { let c = Connection::open(&p).unwrap(); c.execute_batch("PRAGMA synchronous=FULL; PRAGMA busy_timeout=20000;").unwrap(); let mut st = c.prepare(INSERT).unwrap(); let mut rng = Rng(70 + k as u64); b.wait(); for i in 0..per { let r = row_vals(N + 2_000_000 + k as i64 * 10_000 + i as i64, &mut rng); st.execute(params![r.0, r.1, r.2, r.3, r.4, r.5]).unwrap(); } }) }).collect();
      b.wait(); let t = Instant::now(); for h in hs { h.join().unwrap(); } pr("[sqlite] W3 INSERT 1 row/txn, sync=FULL, 4 threads", threads * per, t.elapsed().as_secs_f64()); }
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
}

fn vals(r: &(i64, f64, i64, i64, String, Vec<u8>)) -> Vec<Value> { vec![Value::Int(r.0), Value::Float(r.1), Value::Int(r.2), Value::Int(r.3), Value::Text(r.4.clone()), Value::Bytes(r.5.clone())] }

fn ours_side() {
    let dir = PathBuf::from("data/nocow/e13dir"); let _ = fs::remove_dir_all(&dir);
    let sql = Arc::new(Sql::create(&dir, false));
    sql.exec(CREATE, &[]).unwrap();
    let ins = sql.prepare(INSERT).unwrap();
    let t = Instant::now(); let mut rng = Rng(7);
    for id in 1..=N { sql.execute(&ins, &vals(&row_vals(id, &mut rng))).unwrap(); }
    pr("[ours]   load 1M rows via INSERT (1 row per call, not durable)", N as usize, t.elapsed().as_secs_f64());
    match sql.execute(&ins, &[Value::Int(N + 1), Value::Text("abc".into()), Value::Int(1), Value::Int(2), Value::Text("x@example.com".into()), Value::Bytes(vec![1; 4])]) { Ok(_) => println!("[ours]   text 'abc' into DOUBLE column: accepted (BUG)"), Err(e) => println!("[ours]   text 'abc' into DOUBLE column: rejected ({e})") }
    let mut rng = Rng(99);
    let q1 = sql.prepare(Q1).unwrap();
    { let t = Instant::now(); let mut s = 0.0; for _ in 0..1_000_000 { let id = (rng.next() % N as u64) as i64 + 1; if let Value::Float(f) = sql.execute(&q1, &[Value::Int(id)]).unwrap()[0][0] { s += f; } } pr(&format!("[ours]   Q1 point read by id (check {s:.0})"), 1_000_000, t.elapsed().as_secs_f64()); }
    { let threads = 8; let per = 500_000; let b = Arc::new(Barrier::new(threads + 1)); let hs: Vec<_> = (0..threads).map(|k| { let sql = sql.clone(); let q1 = q1.clone(); let b = b.clone(); std::thread::spawn(move || { let mut rng = Rng(1000 + k as u64); b.wait(); let mut s = 0.0; for _ in 0..per { let id = (rng.next() % N as u64) as i64 + 1; if let Value::Float(f) = sql.execute(&q1, &[Value::Int(id)]).unwrap()[0][0] { s += f; } } s }) }).collect();
      b.wait(); let t = Instant::now(); for h in hs { h.join().unwrap(); } pr("[ours]   Q1 point read by id, 8 threads", threads * per, t.elapsed().as_secs_f64()); }
    { let q = sql.prepare(Q2).unwrap(); let t = Instant::now(); let mut s = 0i64; for _ in 0..500_000 { let id = (rng.next() % N as u64) as i64 + 1; if let Value::Int(i) = sql.execute(&q, &[Value::Text(format!("u{id}@example.com"))]).unwrap()[0][0] { s += i; } } pr(&format!("[ours]   Q2 lookup by name (unique index) (check {s})"), 500_000, t.elapsed().as_secs_f64()); }
    prm("[ours]   Q3 count(*) WHERE age > 40", median(7, || { let r = sql.exec(Q3, &[]).unwrap(); assert!(matches!(r[0][0], Value::Int(_))); }));
    prm("[ours]   Q4 GROUP BY country avg,count", median(7, || { let r = sql.exec(Q4, &[]).unwrap(); assert_eq!(r.len(), 20); }));
    { let q = sql.prepare(Q5).unwrap(); let t = Instant::now(); let mut s = 0.0; for _ in 0..20_000 { let a = (rng.next() % (N - 1000) as u64) as i64 + 1; if let Value::Float(f) = sql.execute(&q, &[Value::Int(a), Value::Int(a + 1000)]).unwrap()[0][0] { s += f; } } pr(&format!("[ours]   Q5 range sum, 1000 rows (check {s:.0})"), 20_000, t.elapsed().as_secs_f64()); }
    prm("[ours]   Q6 ORDER BY score DESC LIMIT 10", median(7, || { let r = sql.exec(Q6, &[]).unwrap(); assert_eq!(r.len(), 10); }));
    prm("[ours]   Q7 sum WHERE country=7 AND age BETWEEN 30 AND 50", median(7, || { let r = sql.exec(Q7, &[]).unwrap(); assert!(matches!(r[0][0], Value::Float(_))); }));
    { let w = sql.prepare(W1).unwrap(); let t = Instant::now(); for i in 0..200_000 { let id = (rng.next() % N as u64) as i64 + 1; sql.execute(&w, &[Value::Float(i as f64), Value::Int(id)]).unwrap(); } pr("[ours]   W1 UPDATE by id, 1 row/txn (not durable)", 200_000, t.elapsed().as_secs_f64()); }
    { let t = Instant::now(); let mut rng = Rng(5); for i in 0..200_000 { sql.execute(&ins, &vals(&row_vals(N + 100 + i, &mut rng))).unwrap(); } pr("[ours]   W2 INSERT 1 row/txn (not durable)", 200_000, t.elapsed().as_secs_f64()); }
    // sanity: results agree with the SQLite side on the deterministic queries
    let r = sql.exec(Q3, &[]).unwrap(); let r4 = sql.exec(Q4, &[]).unwrap(); let r7 = sql.exec(Q7, &[]).unwrap();
    println!("[ours]   checks: Q3={:?} Q4[7]={:?} Q7={:?}", r[0][0], r4[7], r7[0][0]);
    drop(sql); let _ = fs::remove_dir_all(&dir);

    // durable inserts
    let dir = PathBuf::from("data/nocow/e13dur"); let _ = fs::remove_dir_all(&dir);
    let sql = Arc::new(Sql::create(&dir, true));
    sql.exec(CREATE, &[]).unwrap();
    let ins = sql.prepare(INSERT).unwrap();
    for (threads, per) in [(1usize, 2000i64), (4, 2000), (64, 2000), (256, 2000)] {
        let b = Arc::new(Barrier::new(threads + 1));
        let hs: Vec<_> = (0..threads).map(|k| { let sql = sql.clone(); let ins = ins.clone(); let b = b.clone(); std::thread::spawn(move || { let mut rng = Rng(70 + k as u64); b.wait(); for i in 0..per { sql.execute(&ins, &vals(&row_vals(10_000_000 + threads as i64 * 1_000_000 + k as i64 * 10_000 + i, &mut rng))).unwrap(); } }) }).collect();
        b.wait(); let t = Instant::now(); for h in hs { h.join().unwrap(); } pr(&format!("[ours]   W3 INSERT 1 row/txn, durable (fsync), {threads} threads"), threads * per as usize, t.elapsed().as_secs_f64());
    }
    drop(sql); let _ = fs::remove_dir_all(&dir);
}

fn main() {
    println!("=== E13: the SQL round (same SQL text on both sides) ===");
    sqlite_side();
    ours_side();
}
