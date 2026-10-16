//! Experiment 00: physics floor + SQLite baseline on THIS machine.
//! Everything later is compared against these numbers.
use rusqlite::{Connection, params};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 { let mut x = self.0; x ^= x << 13; x ^= x >> 7; x ^= x << 17; self.0 = x; x }
}

fn stats(name: &str, lat: &mut Vec<Duration>) {
    lat.sort();
    let n = lat.len();
    let total: Duration = lat.iter().sum();
    let p = |q: f64| lat[((n as f64 - 1.0) * q) as usize];
    println!("{:<44} n={:<7} ops/s={:>9.0}  p50={:>8.1}us p99={:>8.1}us max={:>8.1}us",
        name, n, n as f64 / total.as_secs_f64(),
        p(0.5).as_secs_f64()*1e6, p(0.99).as_secs_f64()*1e6, p(1.0).as_secs_f64()*1e6);
}

fn fsync_bench(dir: &Path, label: &str) {
    let path = dir.join("fsync.bin");
    let mut f = OpenOptions::new().create(true).write(true).truncate(true).open(&path).unwrap();
    let buf = vec![0xABu8; 4096];
    for (name, full) in [("fdatasync 4KB append", false), ("fsync 4KB append", true)] {
        let mut lat = Vec::new();
        for _ in 0..2000 {
            let t = Instant::now();
            f.write_all(&buf).unwrap();
            if full { f.sync_all().unwrap() } else { f.sync_data().unwrap() }
            lat.push(t.elapsed());
        }
        stats(&format!("[{label}] {name}"), &mut lat);
    }
    // overwrite in place (what a B-tree page write looks like)
    let mut lat = Vec::new();
    for _ in 0..2000 {
        use std::io::{Seek, SeekFrom};
        f.seek(SeekFrom::Start(0)).unwrap();
        let t = Instant::now();
        f.write_all(&buf).unwrap();
        f.sync_data().unwrap();
        lat.push(t.elapsed());
    }
    stats(&format!("[{label}] fdatasync 4KB overwrite"), &mut lat);
    fs::remove_file(path).ok();
}

fn open(path: &Path, sync: &str, journal: &str) -> Connection {
    let c = Connection::open(path).unwrap();
    c.execute_batch(&format!("PRAGMA journal_mode={journal}; PRAGMA synchronous={sync}; PRAGMA busy_timeout=10000;")).unwrap();
    c
}

fn fresh(dir: &Path, name: &str) -> PathBuf {
    let p = dir.join(format!("{name}.db"));
    for suf in ["", "-wal", "-shm", "-journal"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
    p
}

const SCHEMA: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL NOT NULL, payload BLOB NOT NULL)";

fn single_row_txns(dir: &Path, label: &str, sync: &str, journal: &str, n: usize) {
    let p = fresh(dir, "single");
    let c = open(&p, sync, journal);
    c.execute_batch(SCHEMA).unwrap();
    let mut st = c.prepare("INSERT INTO t(name,score,payload) VALUES(?,?,?)").unwrap();
    let payload = [7u8; 64];
    let mut lat = Vec::with_capacity(n);
    for i in 0..n {
        let t = Instant::now();
        st.execute(params![format!("user{i}"), i as f64 * 0.5, &payload[..]]).unwrap();
        lat.push(t.elapsed());
    }
    stats(&format!("[{label}] sqlite 1 row/txn {journal} sync={sync}"), &mut lat);
}

fn batched(dir: &Path, label: &str, rows: usize, per_txn: usize) {
    let p = fresh(dir, "batched");
    let c = open(&p, "NORMAL", "WAL");
    c.execute_batch(SCHEMA).unwrap();
    let payload = [7u8; 64];
    let t = Instant::now();
    let mut i = 0;
    while i < rows {
        let tx = c.unchecked_transaction().unwrap();
        {
            let mut st = tx.prepare_cached("INSERT INTO t(name,score,payload) VALUES(?,?,?)").unwrap();
            for _ in 0..per_txn { st.execute(params![format!("user{i}"), i as f64 * 0.5, &payload[..]]).unwrap(); i += 1; }
        }
        tx.commit().unwrap();
    }
    let el = t.elapsed();
    println!("{:<44} rows={} rows/s={:.0} ({:.2}s)", format!("[{label}] sqlite bulk insert {per_txn}/txn WAL NORMAL"), rows, rows as f64 / el.as_secs_f64(), el.as_secs_f64());
}

fn point_reads(dir: &Path, label: &str, rows: usize, reads: usize, threads: usize) {
    let p = fresh(dir, "reads");
    {
        let c = open(&p, "NORMAL", "WAL");
        c.execute_batch(SCHEMA).unwrap();
        let payload = [7u8; 64];
        let tx = c.unchecked_transaction().unwrap();
        { let mut st = tx.prepare("INSERT INTO t(name,score,payload) VALUES(?,?,?)").unwrap();
          for i in 0..rows { st.execute(params![format!("user{i}"), i as f64, &payload[..]]).unwrap(); } }
        tx.commit().unwrap();
    }
    let barrier = Arc::new(Barrier::new(threads));
    let per = reads / threads;
    let t = Instant::now();
    let hs: Vec<_> = (0..threads).map(|k| {
        let p = p.clone(); let b = barrier.clone();
        std::thread::spawn(move || {
            let c = open(&p, "NORMAL", "WAL");
            let mut st = c.prepare("SELECT name, score FROM t WHERE id=?").unwrap();
            let mut rng = Rng(0x9E3779B97F4A7C15 ^ (k as u64 + 1));
            b.wait();
            let mut sum = 0f64;
            for _ in 0..per {
                let id = (rng.next() % rows as u64) as i64 + 1;
                let s: f64 = st.query_row(params![id], |r| r.get(1)).unwrap();
                sum += s;
            }
            sum
        })
    }).collect();
    let _ = hs.into_iter().map(|h| h.join().unwrap()).sum::<f64>();
    let el = t.elapsed();
    println!("{:<44} reads={} reads/s={:.0} ({:.2}s)", format!("[{label}] sqlite point reads by PK, {threads} thr"), reads, reads as f64 / el.as_secs_f64(), el.as_secs_f64());
}

fn concurrent_writers(dir: &Path, label: &str, threads: usize, per_thread: usize, sync: &str) {
    let p = fresh(dir, "cw");
    { let c = open(&p, sync, "WAL"); c.execute_batch(SCHEMA).unwrap(); }
    let barrier = Arc::new(Barrier::new(threads));
    let t = Instant::now();
    let hs: Vec<_> = (0..threads).map(|k| {
        let p = p.clone(); let b = barrier.clone(); let sync = sync.to_string();
        std::thread::spawn(move || {
            let c = open(&p, &sync, "WAL");
            let mut st = c.prepare("INSERT INTO t(name,score,payload) VALUES(?,?,?)").unwrap();
            let payload = [7u8; 64];
            b.wait();
            let mut lat = Vec::with_capacity(per_thread);
            for i in 0..per_thread {
                let t = Instant::now();
                st.execute(params![format!("w{k}-{i}"), i as f64, &payload[..]]).unwrap();
                lat.push(t.elapsed());
            }
            lat
        })
    }).collect();
    let mut all: Vec<Duration> = hs.into_iter().flat_map(|h| h.join().unwrap()).collect();
    let el = t.elapsed();
    let n = all.len();
    all.sort();
    println!("{:<44} txns={} txn/s={:.0} p50={:.1}us p99={:.1}us ({:.2}s)",
        format!("[{label}] sqlite {threads} writer thr, 1 row/txn, sync={sync}"), n, n as f64 / el.as_secs_f64(),
        all[n/2].as_secs_f64()*1e6, all[(n as f64*0.99) as usize].as_secs_f64()*1e6, el.as_secs_f64());
}

fn main() {
    let root = PathBuf::from(std::env::var("SQWEEZDB_DATA").unwrap_or_else(|_| "data".into()));
    println!("=== E00 baseline ===");
    for (label, sub) in [("cow", "cow"), ("nocow", "nocow")] {
        let dir = root.join(sub);
        println!("--- physics: {label} ---");
        fsync_bench(&dir, label);
    }
    for (label, sub) in [("cow", "cow"), ("nocow", "nocow")] {
        let dir = root.join(sub);
        println!("--- sqlite single thread: {label} ---");
        single_row_txns(&dir, label, "FULL", "WAL", 2000);
        single_row_txns(&dir, label, "NORMAL", "WAL", 20000);
        single_row_txns(&dir, label, "OFF", "WAL", 200000);
        single_row_txns(&dir, label, "OFF", "MEMORY", 200000);
        batched(&dir, label, 1_000_000, 1000);
        point_reads(&dir, label, 1_000_000, 2_000_000, 1);
        point_reads(&dir, label, 1_000_000, 4_000_000, 8);
    }
    let dir = root.join("nocow");
    println!("--- sqlite concurrent writers (nocow) ---");
    for thr in [1, 2, 4, 8] { concurrent_writers(&dir, "nocow", thr, 20000 / thr * 1, "NORMAL"); }
    for thr in [1, 4] { concurrent_writers(&dir, "nocow", thr, 2000 / thr, "FULL"); }
}
