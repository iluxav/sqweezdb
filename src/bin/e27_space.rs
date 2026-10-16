//! Experiment 27: reclaiming space without stopping. (a) delete half of 2M rows, compare file sizes; (b) reclaim while a
//! writer keeps inserting: SQLite VACUUM vs our snapshot, measuring the writer's worst stall; (c) churn: 1M live rows,
//! 3M insert+delete pairs: SQLite file growth (default and auto_vacuum=FULL) vs our slot reuse and memory.
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::Value;
use sqweezdb::util::Rng;
use rusqlite::{params, Connection};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const N: i64 = 2_000_000;
const DB: &str = "data/nocow/e27.db"; const DB_AV: &str = "data/nocow/e27av.db"; const DIR: &str = "data/nocow/e27dir";
const CREATE: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, age INT, name VARCHAR(32) UNIQUE, payload BLOB(64))";
const CREATE_SQLITE: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL NOT NULL, country INTEGER NOT NULL, age INTEGER NOT NULL, name TEXT NOT NULL, payload BLOB NOT NULL); CREATE UNIQUE INDEX t_name ON t(name);";
fn row(id: i64, rng: &mut Rng) -> Vec<Value> { vec![Value::Int(id), Value::Float((rng.next() % 100000) as f64 / 100.0), Value::Int(id % 20), Value::Int(18 + id % 60), Value::Text(format!("u{id}@example.com")), Value::Bytes(vec![7; 64])] }
fn mb(p: &str) -> f64 { fs::metadata(p).map(|m| m.len() as f64 / 1e6).unwrap_or(0.0) }
fn dir_mb(p: &str) -> (f64, f64) { let mut snap = 0.0; let mut log = 0.0; if let Ok(rd) = fs::read_dir(p) { for e in rd.flatten() { let n = e.file_name().to_string_lossy().to_string(); let sz = e.metadata().map(|m| m.len() as f64 / 1e6).unwrap_or(0.0); if n.starts_with("log.") { log += sz; } else { snap += sz; } } } (snap, log) }
fn rss_mb() -> f64 { let s = fs::read_to_string("/proc/self/status").unwrap(); s.lines().find(|l| l.starts_with("VmRSS:")).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0) / 1024.0 }
fn sqlite_build(path: &str, auto_vacuum: bool, n: i64) -> Connection {
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{path}{suf}")).ok(); }
    let c = Connection::open(path).unwrap(); if auto_vacuum { c.execute_batch("PRAGMA auto_vacuum=FULL;").unwrap(); }
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap(); c.execute_batch(CREATE_SQLITE).unwrap();
    let mut rng = Rng(7); let tx = c.unchecked_transaction().unwrap(); { let mut st = tx.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); for id in 1..=n { let r = row(id, &mut rng); st.execute(params![id, match r[1] { Value::Float(f) => f, _ => 0.0 }, id % 20, 18 + id % 60, format!("u{id}@example.com"), &[7u8; 64][..]]).unwrap(); } } tx.commit().unwrap();
    c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap(); c
}
fn ours_build(dir: &Path, n: i64) -> Sql { let sql = Sql::create(dir, false); let mut s = Session::default(); sql.exec_s(&mut s, CREATE, &[]).unwrap(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let mut rng = Rng(7);
    for id in 1..=n { if id % 1000 == 1 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &row(id, &mut rng)).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } } drop(ins); sql }
/// A writer that inserts one row at a time until told to stop; returns (rows inserted, worst single-insert latency).
fn ours_writer(sql: Arc<Sql>, stop: Arc<AtomicBool>, start_id: i64) -> std::thread::JoinHandle<(u64, Duration)> { std::thread::spawn(move || { let mut s = Session::default(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let mut rng = Rng(99); let (mut n, mut worst) = (0u64, Duration::ZERO); let mut id = start_id; while !stop.load(Ordering::Relaxed) { let t = Instant::now(); sql.execute_s(&mut s, &ins, &row(id, &mut rng)).unwrap(); worst = worst.max(t.elapsed()); n += 1; id += 1; } (n, worst) }) }
fn sqlite_writer(path: &'static str, stop: Arc<AtomicBool>, start_id: i64) -> std::thread::JoinHandle<(u64, Duration, u64)> { std::thread::spawn(move || { let c = Connection::open(path).unwrap(); c.busy_timeout(Duration::from_secs(600)).unwrap(); c.execute_batch("PRAGMA synchronous=NORMAL;").unwrap(); let mut st = c.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let mut rng = Rng(99); let (mut n, mut worst, mut busy) = (0u64, Duration::ZERO, 0u64); let mut id = start_id; while !stop.load(Ordering::Relaxed) { let r = row(id, &mut rng); let t = Instant::now(); match st.execute(params![id, match r[1] { Value::Float(f) => f, _ => 0.0 }, id % 20, 18 + id % 60, format!("u{id}@example.com"), &[7u8; 64][..]]) { Ok(_) => { n += 1; id += 1; } Err(_) => { busy += 1; } } worst = worst.max(t.elapsed()); } (n, worst, busy) }) }

fn main() {
    if std::env::args().nth(1).as_deref() == Some("ours-churn") {
        // 1M live rows, then 3M times: insert a new row, delete the oldest. Memory and slots with reuse.
        let dir = PathBuf::from(format!("{DIR}/churn")); let sql = ours_build(&dir, 1_000_000); let mut s = Session::default();
        let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let del = sql.prepare("DELETE FROM t WHERE id = ?").unwrap(); let mut rng = Rng(3);
        let r0 = rss_mb(); let t = Instant::now();
        for k in 0..3_000_000i64 { sql.execute_s(&mut s, &ins, &row(1_000_001 + k, &mut rng)).unwrap(); sql.execute_s(&mut s, &del, &[Value::Int(1 + k)]).unwrap(); }
        let el = t.elapsed(); let tb = sql.db.table("t").unwrap(); let live = match sql.exec_s(&mut s, "SELECT count(*) FROM t", &[]).unwrap()[0][0] { Value::Int(i) => i, _ => 0 };
        let (_, log) = dir_mb(dir.to_str().unwrap());
        println!("ours churn: 3M insert+delete pairs in {:.1} s ({:.2} us per pair) | live rows {live}, slots allocated {} (free list {}), rss {:.0} -> {:.0} MB, log written {log:.0} MB", el.as_secs_f64(), el.as_secs_f64() * 1e6 / 3e6, tb.rows_allocated(), tb.free_slots(), r0, rss_mb());
        let st = sql.snapshot(); println!("ours churn: snapshot after churn {:.0} MB in {:.2} s, {} segments deleted", st.bytes as f64 / 1e6, st.secs, st.deleted_segments); drop(ins); drop(del); drop(sql); let _ = fs::remove_dir_all(&dir); return;
    }
    println!("=== E27: reclaiming space without stopping (2M rows) ===");
    let c = sqlite_build(DB, false, N); let dir = PathBuf::from(format!("{DIR}/main")); let sql = Arc::new(ours_build(&dir, N)); let st = sql.snapshot();
    println!("start: sqlite {:.0} MB; ours snapshot {:.0} MB ({:.2} s)", mb(DB), st.bytes as f64 / 1e6, st.secs);
    // (a) delete half
    let t = Instant::now(); let n = c.execute("DELETE FROM t WHERE country < 10", []).unwrap(); let ds = t.elapsed(); c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let mut s = Session::default(); let t = Instant::now(); let r = sql.exec_s(&mut s, "DELETE FROM t WHERE country < 10", &[]).unwrap(); let d_o = t.elapsed();
    let (snap, log) = dir_mb(dir.to_str().unwrap());
    println!("delete half: sqlite {n} rows in {:.2} s, file still {:.0} MB (freelist pages, nothing returned) | ours {:?} rows in {:.2} s, snapshot {snap:.0} MB + log {log:.0} MB (deletes log 8 bytes each)", ds.as_secs_f64(), mb(DB), r.first().map(|x| x[0].clone()).unwrap_or(Value::Null), d_o.as_secs_f64());
    // (b) reclaim with a concurrent writer
    let stop = Arc::new(AtomicBool::new(false)); let w = sqlite_writer(DB, stop.clone(), 10_000_000); std::thread::sleep(Duration::from_millis(300));
    let t = Instant::now(); c.execute_batch("VACUUM").unwrap(); let vt = t.elapsed(); std::thread::sleep(Duration::from_millis(300)); stop.store(true, Ordering::Relaxed); let (wn, worst, busy) = w.join().unwrap(); c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    println!("reclaim, sqlite VACUUM: {:.2} s, file {:.0} MB after | concurrent writer: {wn} inserts in {:.1} s, worst stall {:.3} s, busy errors {busy}", vt.as_secs_f64(), mb(DB), vt.as_secs_f64() + 0.6, worst.as_secs_f64());
    let stop = Arc::new(AtomicBool::new(false)); let w = ours_writer(sql.clone(), stop.clone(), 10_000_000); std::thread::sleep(Duration::from_millis(300));
    let st = sql.snapshot(); std::thread::sleep(Duration::from_millis(300)); stop.store(true, Ordering::Relaxed); let (wn, worst) = w.join().unwrap();
    let (snap, log) = dir_mb(dir.to_str().unwrap());
    println!("reclaim, ours snapshot: {:.2} s, snapshot {snap:.0} MB + log {log:.0} MB after ({} rows, {} old segments deleted) | concurrent writer: {wn} inserts in {:.1} s, worst stall {:.3} ms", st.secs, st.rows, st.deleted_segments, st.secs + 0.6, worst.as_secs_f64() * 1e3);
    let cnt_s: i64 = c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap(); let cnt_o = sql.exec_s(&mut s, "SELECT count(*) FROM t", &[]).unwrap(); println!("row counts after: sqlite {cnt_s}, ours {:?}", cnt_o[0][0]);
    drop(c); drop(sql); let _ = fs::remove_dir_all(&dir);
    // (c) churn
    println!("-- churn: 1M live rows, 3M insert+delete pairs --");
    for (path, av) in [(DB, false), (DB_AV, true)] {
        let c = sqlite_build(path, av, 1_000_000); let mut ins = c.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let mut del = c.prepare("DELETE FROM t WHERE id = ?").unwrap(); let mut rng = Rng(3);
        let t = Instant::now(); for k in 0..3_000_000i64 { let id = 1_000_001 + k; let r = row(id, &mut rng); ins.execute(params![id, match r[1] { Value::Float(f) => f, _ => 0.0 }, id % 20, 18 + id % 60, format!("u{id}@example.com"), &[7u8; 64][..]]).unwrap(); del.execute(params![1 + k]).unwrap(); }
        let el = t.elapsed(); drop(ins); drop(del); c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap(); let fl: i64 = c.query_row("PRAGMA freelist_count", [], |r| r.get(0)).unwrap();
        println!("sqlite churn{}: {:.1} s ({:.2} us per pair) | file {:.0} MB, {fl} free pages", if av { " auto_vacuum=FULL" } else { "" }, el.as_secs_f64(), el.as_secs_f64() * 1e6 / 3e6, mb(path));
        drop(c); for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{path}{suf}")).ok(); }
    }
    let o = Command::new(std::env::current_exe().unwrap()).arg("ours-churn").stderr(Stdio::inherit()).output().unwrap(); print!("{}", String::from_utf8_lossy(&o.stdout));
    let _ = fs::remove_dir_all(DIR);
}
