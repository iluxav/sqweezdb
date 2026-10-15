//! Experiment 01b: pin down WHICH process-global thing serializes SQLite reads.
//! Knobs: SQLITE_CONFIG_MEMSTATUS (global malloc-stats mutex), cache_size (syscalls vs cache), mmap.
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

fn run(label: &str, threads: usize, paths: Arc<Vec<PathBuf>>, long_txn: bool, cache_kb: i64, mmap: bool) {
    let per = TOTAL_READS / threads;
    let barrier = Arc::new(Barrier::new(threads + 1));
    let hs: Vec<_> = (0..threads).map(|k| {
        let paths = paths.clone(); let b = barrier.clone();
        std::thread::spawn(move || {
            let c = Connection::open(&paths[k % paths.len()]).unwrap();
            c.execute_batch(&format!("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=10000; PRAGMA cache_size=-{cache_kb};")).unwrap();
            if mmap { c.execute_batch("PRAGMA mmap_size=1073741824;").unwrap(); }
            let mut st = c.prepare("SELECT score FROM t WHERE id=?").unwrap();
            let mut rng = Rng(0x9E3779B97F4A7C15 ^ (k as u64 + 1));
            let mut sum = 0f64;
            for _ in 0..300_000 { let id = (rng.next() % ROWS as u64) as i64 + 1; sum += st.query_row(params![id], |r| r.get::<_, f64>(0)).unwrap(); } // warm cache
            b.wait();
            if long_txn { c.execute_batch("BEGIN").unwrap(); }
            for _ in 0..per {
                let id = (rng.next() % ROWS as u64) as i64 + 1;
                sum += st.query_row(params![id], |r| r.get::<_, f64>(0)).unwrap();
            }
            if long_txn { c.execute_batch("COMMIT").unwrap(); }
            sum
        })
    }).collect();
    barrier.wait();
    let t = Instant::now();
    let _ = hs.into_iter().map(|h| h.join().unwrap()).sum::<f64>();
    let el = t.elapsed();
    let n = per * threads;
    println!("{:<58} thr={:<2} reads/s={:>10.0}  per-read={:.0}ns", label, threads, n as f64 / el.as_secs_f64(), el.as_nanos() as f64 * threads as f64 / n as f64);
}

fn main() {
    let memstatus: i32 = std::env::args().nth(1).map(|s| s.parse().unwrap()).unwrap_or(1);
    let rc = unsafe { rusqlite::ffi::sqlite3_config(rusqlite::ffi::SQLITE_CONFIG_MEMSTATUS, memstatus) };
    println!("=== E01b sqlite knobs: MEMSTATUS={memstatus} (config rc={rc}) ===");
    let root = PathBuf::from("data/nocow");
    let main = root.join("rs.db");
    build(&main);
    let same = Arc::new(vec![main.clone()]);
    let mut copies = vec![];
    for k in 0..8 { let p = root.join(format!("rs_copy{k}.db")); fs::copy(&main, &p).unwrap(); copies.push(p); }
    let copies = Arc::new(copies);
    if std::env::args().nth(2).as_deref() == Some("only") {
        run("cache 256MB, long txn (profiling run)", 8, same.clone(), true, 256000, false);
        return;
    }
    run("cache 2MB (default), autocommit", 8, same.clone(), false, 2000, false);
    run("cache 2MB (default), long txn", 8, same.clone(), true, 2000, false);
    run("cache 256MB, autocommit", 8, same.clone(), false, 256000, false);
    run("cache 256MB, long txn", 8, same.clone(), true, 256000, false);
    run("cache 256MB, long txn", 12, same.clone(), true, 256000, false);
    run("cache 256MB, long txn", 1, same.clone(), true, 256000, false);
    run("SEPARATE files, cache 256MB, autocommit", 8, copies.clone(), false, 256000, false);
    run("mmap 1GB, long txn (reference)", 8, same.clone(), true, 2000, true);
    run("mmap 1GB, long txn (reference)", 1, same.clone(), true, 2000, true);
    for k in 0..8 { fs::remove_file(root.join(format!("rs_copy{k}.db"))).ok(); }
}
