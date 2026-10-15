//! Experiment 04: SQLite baseline beyond point lookups — scans, aggregates, PK ranges, secondary indexes, joins, sorts, indexed writes.
//! Two configs: default (2MB cache, no mmap) and tuned (256MB cache + 1GB mmap).
use rusqlite::{Connection, params};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

struct Rng(u64);
impl Rng { fn next(&mut self) -> u64 { let mut x = self.0; x ^= x << 13; x ^= x >> 7; x ^= x << 17; self.0 = x; x } }
const USERS: i64 = 1_000_000;
const ORDERS: i64 = 5_000_000;

fn open(p: &Path, tuned: bool) -> Connection {
    let c = Connection::open(p).unwrap();
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=10000;").unwrap();
    if tuned { c.execute_batch("PRAGMA cache_size=-262144; PRAGMA mmap_size=1073741824;").unwrap(); }
    c
}

fn build(p: &Path) {
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
    let c = open(p, false);
    c.execute_batch("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL, country TEXT NOT NULL, age INTEGER NOT NULL, score REAL NOT NULL);
                     CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, amount REAL NOT NULL, status TEXT NOT NULL);").unwrap();
    let t = Instant::now();
    let mut rng = Rng(7);
    let tx = c.unchecked_transaction().unwrap();
    { let mut st = tx.prepare("INSERT INTO users VALUES(?,?,?,?,?,?)").unwrap();
      for i in 1..=USERS { st.execute(params![i, format!("user{i}"), format!("u{i}@example.com"), format!("C{:02}", i % 20), 18 + (i % 60), (rng.next() % 100000) as f64 / 100.0]).unwrap(); } }
    tx.commit().unwrap();
    let tx = c.unchecked_transaction().unwrap();
    { let mut st = tx.prepare("INSERT INTO orders VALUES(?,?,?,?)").unwrap();
      let status = ["new", "paid", "shipped", "done"];
      for i in 1..=ORDERS { st.execute(params![i, (rng.next() % USERS as u64) as i64 + 1, (rng.next() % 50000) as f64 / 100.0, status[(i % 4) as usize]]).unwrap(); } }
    tx.commit().unwrap();
    let t2 = Instant::now();
    c.execute_batch("CREATE UNIQUE INDEX users_email ON users(email); CREATE INDEX orders_user ON orders(user_id); ANALYZE; PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    println!("build: {} users + {} orders in {:.1}s, indexes+analyze {:.1}s, file {:.0} MB", USERS, ORDERS, t2.duration_since(t).as_secs_f64(), t2.elapsed().as_secs_f64(), fs::metadata(p).unwrap().len() as f64 / 1e6);
}

fn timed(label: &str, iters: usize, rows_per_call: i64, mut f: impl FnMut()) {
    let mut ts = Vec::with_capacity(iters);
    for _ in 0..iters { let t = Instant::now(); f(); ts.push(t.elapsed()); }
    ts.sort();
    let med = ts[iters / 2];
    let rows = if rows_per_call > 0 { format!("  rows/s={:>12.0}", rows_per_call as f64 / med.as_secs_f64()) } else { String::new() };
    println!("{:<60} median={:>10.3}ms  calls/s={:>9.0}{}", label, med.as_secs_f64() * 1e3, 1.0 / med.as_secs_f64(), rows);
}

fn queries(p: &Path, tuned: bool) {
    let cfg = if tuned { "tuned" } else { "default" };
    let c = open(p, tuned);
    let mut rng = Rng(99);
    // warm the OS/page cache once
    let _: i64 = c.query_row("SELECT count(*) FROM users WHERE age > 40", [], |r| r.get(0)).unwrap();
    let _: i64 = c.query_row("SELECT count(*) FROM orders WHERE amount > 1", [], |r| r.get(0)).unwrap();

    timed(&format!("[{cfg}] Q1 full scan filter: count(*) users WHERE age>40"), 5, USERS, || { let _: i64 = c.query_row("SELECT count(*) FROM users WHERE age > 40", [], |r| r.get(0)).unwrap(); });
    timed(&format!("[{cfg}] Q2 group by country avg(score),count(*)"), 5, USERS, || { let mut st = c.prepare_cached("SELECT country, avg(score), count(*) FROM users GROUP BY country").unwrap(); let n = st.query_map([], |r| r.get::<_, f64>(1)).unwrap().count(); assert_eq!(n, 20); });
    { let mut st = c.prepare("SELECT sum(score) FROM users WHERE id >= ? AND id < ?").unwrap();
      timed(&format!("[{cfg}] Q3 PK range scan 1000 rows sum(score)"), 2000, 1000, || { let s = (rng.next() % (USERS - 1000) as u64) as i64 + 1; let _: f64 = st.query_row(params![s, s + 1000], |r| r.get(0)).unwrap(); }); }
    { let mut st = c.prepare("SELECT id FROM users WHERE email = ?").unwrap();
      timed(&format!("[{cfg}] Q4 secondary UNIQUE index lookup by email"), 200000, 1, || { let i = (rng.next() % USERS as u64) as i64 + 1; let _: i64 = st.query_row(params![format!("u{i}@example.com")], |r| r.get(0)).unwrap(); }); }
    { let mut st = c.prepare("SELECT count(*), sum(amount) FROM orders WHERE user_id = ?").unwrap();
      timed(&format!("[{cfg}] Q5 secondary index range: orders of one user (~5 rows)"), 200000, 5, || { let i = (rng.next() % USERS as u64) as i64 + 1; let _: (i64, Option<f64>) = st.query_row(params![i], |r| Ok((r.get(0)?, r.get(1)?))).unwrap(); }); }
    { let mut st = c.prepare("SELECT u.id, sum(o.amount) FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id >= ? AND u.id < ? GROUP BY u.id").unwrap();
      timed(&format!("[{cfg}] Q6 join 100 users -> their ~500 orders, group by"), 2000, 500, || { let s = (rng.next() % (USERS - 100) as u64) as i64 + 1; let n = st.query_map(params![s, s + 100], |r| r.get::<_, f64>(1)).unwrap().count(); assert!(n > 0); }); }
    timed(&format!("[{cfg}] Q7 big join: orders x users WHERE country='C07' (~250k)"), 3, 250_000, || { let _: (i64, f64) = c.query_row("SELECT count(*), sum(o.amount) FROM orders o JOIN users u ON u.id = o.user_id WHERE u.country = 'C07'", [], |r| Ok((r.get(0)?, r.get(1)?))).unwrap(); });
    timed(&format!("[{cfg}] Q8 ORDER BY score DESC LIMIT 10 (no index, 1M rows)"), 5, USERS, || { let mut st = c.prepare_cached("SELECT id FROM users ORDER BY score DESC LIMIT 10").unwrap(); let n = st.query_map([], |r| r.get::<_, i64>(0)).unwrap().count(); assert_eq!(n, 10); });
    timed(&format!("[{cfg}] Q9 full scan sum(amount) over 5M orders"), 3, ORDERS, || { let _: f64 = c.query_row("SELECT sum(amount) FROM orders", [], |r| r.get(0)).unwrap(); });

    // Q4 across 8 threads
    let threads = 8; let per = 100_000;
    let barrier = Arc::new(Barrier::new(threads + 1));
    let hs: Vec<_> = (0..threads).map(|k| { let p = p.to_path_buf(); let b = barrier.clone(); std::thread::spawn(move || {
        let c = open(&p, tuned); let mut st = c.prepare("SELECT id FROM users WHERE email = ?").unwrap(); let mut rng = Rng(1000 + k as u64);
        b.wait(); for _ in 0..per { let i = (rng.next() % USERS as u64) as i64 + 1; let _: i64 = st.query_row(params![format!("u{i}@example.com")], |r| r.get(0)).unwrap(); } })}).collect();
    barrier.wait(); let t = Instant::now(); for h in hs { h.join().unwrap(); } let el = t.elapsed();
    println!("{:<60} lookups/s={:>9.0}", format!("[{cfg}] Q4 secondary index lookup, 8 threads"), (threads * per) as f64 / el.as_secs_f64());
}

fn indexed_writes(p: &Path) {
    let c = open(p, false);
    // bulk insert with a UNIQUE secondary index to maintain
    let t = Instant::now();
    let n = 200_000i64;
    let mut i = USERS + 1;
    while i <= USERS + n {
        let tx = c.unchecked_transaction().unwrap();
        { let mut st = tx.prepare_cached("INSERT INTO users VALUES(?,?,?,?,?,?)").unwrap();
          for _ in 0..1000 { st.execute(params![i, format!("user{i}"), format!("u{i}@example.com"), "C01", 30, 1.5]).unwrap(); i += 1; } }
        tx.commit().unwrap();
    }
    let el = t.elapsed();
    println!("{:<60} rows/s={:>9.0}  (E00 without secondary index: ~2,000,000)", "W1 bulk insert 1000/txn WITH unique secondary index", n as f64 / el.as_secs_f64());
    // single-row updates touching the indexed column, 1 row/txn, NORMAL
    let mut st = c.prepare("UPDATE users SET email = ? WHERE id = ?").unwrap();
    let mut rng = Rng(5);
    let mut lat = Vec::new();
    for j in 0..20000 { let id = (rng.next() % USERS as u64) as i64 + 1; let t = Instant::now(); st.execute(params![format!("x{j}-{id}@example.com"), id]).unwrap(); lat.push(t.elapsed()); }
    lat.sort();
    let tot: Duration = lat.iter().sum();
    println!("{:<60} txn/s={:>9.0}  p50={:.1}us  (E00 non-indexed column: ~120k-165k txn/s, 3.5us)", "W2 1 row/txn UPDATE of indexed column, WAL NORMAL", 20000.0 / tot.as_secs_f64(), lat[10000].as_secs_f64() * 1e6);
    let mut st = c.prepare("UPDATE users SET score = ? WHERE id = ?").unwrap();
    let mut lat = Vec::new();
    for j in 0..20000 { let id = (rng.next() % USERS as u64) as i64 + 1; let t = Instant::now(); st.execute(params![j as f64, id]).unwrap(); lat.push(t.elapsed()); }
    lat.sort();
    let tot: Duration = lat.iter().sum();
    println!("{:<60} txn/s={:>9.0}  p50={:.1}us", "W3 1 row/txn UPDATE of non-indexed column (same table)", 20000.0 / tot.as_secs_f64(), lat[10000].as_secs_f64() * 1e6);
}

fn main() {
    println!("=== E04 SQLite query baseline (system libsqlite3) ===");
    let p = PathBuf::from("data/nocow/q.db");
    build(&p);
    queries(&p, false);
    queries(&p, true);
    indexed_writes(&p);
}
