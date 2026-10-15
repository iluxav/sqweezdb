//! Experiment 01: WHY don't SQLite point reads scale across threads?
//! Isolate: per-read lock acquisition vs file-level contention vs process-global.
use rusqlite::{Connection, params};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::time::Instant;

struct Rng(u64);
impl Rng { fn next(&mut self) -> u64 { let mut x = self.0; x ^= x << 13; x ^= x >> 7; x ^= x << 17; self.0 = x; x } }
const ROWS: usize = 1_000_000;
const TOTAL_READS: usize = 4_000_000;

fn build(p: &Path) {
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
    let c = Connection::open(p).unwrap();
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;
        CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL NOT NULL, payload BLOB NOT NULL)").unwrap();
    let payload = [7u8; 64];
    let tx = c.unchecked_transaction().unwrap();
    { let mut st = tx.prepare("INSERT INTO t(name,score,payload) VALUES(?,?,?)").unwrap();
      for i in 0..ROWS { st.execute(params![format!("user{i}"), i as f64, &payload[..]]).unwrap(); } }
    tx.commit().unwrap();
    c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
}

#[derive(Clone, Copy, PartialEq)]
enum Mode { Auto, LongTxn }

fn run(label: &str, threads: usize, paths: Arc<Vec<PathBuf>>, mode: Mode, mmap: bool) {
    let per = TOTAL_READS / threads;
    let barrier = Arc::new(Barrier::new(threads + 1));
    let hs: Vec<_> = (0..threads).map(|k| {
        let paths = paths.clone(); let b = barrier.clone();
        std::thread::spawn(move || {
            let c = Connection::open(&paths[k % paths.len()]).unwrap();
            c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=10000;").unwrap();
            if mmap { c.execute_batch("PRAGMA mmap_size=1073741824;").unwrap(); }
            let mut st = c.prepare("SELECT score FROM t WHERE id=?").unwrap();
            let mut rng = Rng(0x9E3779B97F4A7C15 ^ (k as u64 + 1));
            b.wait();
            if mode == Mode::LongTxn { c.execute_batch("BEGIN").unwrap(); }
            let mut sum = 0f64;
            for _ in 0..per {
                let id = (rng.next() % ROWS as u64) as i64 + 1;
                let s: f64 = st.query_row(params![id], |r| r.get(0)).unwrap();
                sum += s;
            }
            if mode == Mode::LongTxn { c.execute_batch("COMMIT").unwrap(); }
            sum
        })
    }).collect();
    barrier.wait();
    let t = Instant::now();
    let _ = hs.into_iter().map(|h| h.join().unwrap()).sum::<f64>();
    let el = t.elapsed();
    let n = per * threads;
    println!("{:<52} thr={:<2} reads/s={:>10.0}  per-read={:.0}ns", label, threads, n as f64 / el.as_secs_f64(), el.as_nanos() as f64 * threads as f64 / n as f64);
}

fn main() {
    let root = PathBuf::from("data/nocow");
    let main = root.join("rs.db");
    build(&main);
    let same = Arc::new(vec![main.clone()]);
    // per-thread private copies
    let mut copies = vec![];
    for k in 0..12 { let p = root.join(format!("rs_copy{k}.db")); fs::copy(&main, &p).unwrap(); copies.push(p); }
    let copies = Arc::new(copies);

    println!("=== E01 read scaling diagnosis ===");
    for thr in [1, 2, 4, 8, 12] { run("same file, autocommit per read", thr, same.clone(), Mode::Auto, false); }
    for thr in [1, 2, 4, 8, 12] { run("same file, ONE long read txn per thread", thr, same.clone(), Mode::LongTxn, false); }
    for thr in [8, 12] { run("same file, mmap 1GB, long read txn", thr, same.clone(), Mode::LongTxn, true); }
    for thr in [8, 12] { run("same file, mmap 1GB, autocommit", thr, same.clone(), Mode::Auto, true); }
    for thr in [8, 12] { run("SEPARATE file per thread, autocommit", thr, copies.clone(), Mode::Auto, false); }
    for thr in [8, 12] { run("SEPARATE file per thread, long read txn", thr, copies.clone(), Mode::LongTxn, false); }
    for k in 0..12 { fs::remove_file(root.join(format!("rs_copy{k}.db"))).ok(); }
}
