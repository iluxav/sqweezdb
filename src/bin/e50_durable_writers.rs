//! Experiment 50: durable single-row commits with many concurrent writers, the same test on SQLite (system lib, WAL,
//! synchronous=FULL), Turso 0.8.3 (MVCC, BEGIN CONCURRENT, synchronous=FULL) and ours (durable). Every commit waits
//! for its fsync. Writers insert disjoint keys; at the end the row count must equal the acknowledged commits.
use rusqlite::{params, Connection};
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::Value;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SECS: u64 = 3;
const WRITERS: &[usize] = &[1, 16, 64, 256];
const DBS: &str = "data/nocow/e50s.db"; const DBT: &str = "data/nocow/e50t.db"; const DIR: &str = "data/nocow/e50dir";

fn report(engine: &str, writers: usize, ok: u64, retried: u64, rows: i64) {
    println!("{engine:<8} {writers:>3} writers: {:>9.0} durable commits/s  ({ok} acknowledged, {retried} retried, {rows} rows: {})",
             ok as f64 / SECS as f64, if rows as u64 == ok { "exact" } else { "MISMATCH" });
}
fn key(round: usize, writer: usize, i: u64) -> i64 { (round as i64) * 1_000_000_000 + (writer as i64) * 1_000_000 + i as i64 }

fn sqlite() {
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{DBS}{suf}")).ok(); }
    let c = Connection::open(DBS).unwrap();
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL, note TEXT NOT NULL);").unwrap();
    for (round, &w) in WRITERS.iter().enumerate() {
        let (ok, retried) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
        let end = Instant::now() + Duration::from_secs(SECS);
        let hs: Vec<_> = (0..w).map(|k| { let (ok, retried) = (ok.clone(), retried.clone()); std::thread::spawn(move || {
            let c = Connection::open(DBS).unwrap(); c.busy_timeout(Duration::from_secs(10)).unwrap(); c.execute_batch("PRAGMA synchronous=FULL").unwrap();
            let mut st = c.prepare("INSERT INTO t VALUES (?, ?, ?)").unwrap(); let mut i = 0u64;
            while Instant::now() < end { match st.execute(params![key(round, k, i), i as i64, "durable"]) { Ok(_) => { ok.fetch_add(1, Ordering::Relaxed); i += 1; } Err(_) => { retried.fetch_add(1, Ordering::Relaxed); } } }
        }) }).collect();
        for h in hs { h.join().unwrap(); }
        let rows: i64 = c.query_row("SELECT count(*) FROM t WHERE id >= ?1 AND id < ?2", params![key(round, 0, 0), key(round + 1, 0, 0)], |r| r.get(0)).unwrap();
        report("sqlite", w, ok.load(Ordering::Relaxed), retried.load(Ordering::Relaxed), rows);
    }
}

fn rt() -> tokio::runtime::Runtime { tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap() }
fn turso() {
    use turso::{Builder, Value as TV};
    for suf in ["", "-wal", "-shm", "-tshm", "-log"] { fs::remove_file(format!("{DBT}{suf}")).ok(); }
    let rt0 = rt();
    let db = rt0.block_on(async { Builder::new_local(DBT).build().await.unwrap() });
    let conn = db.connect().unwrap();
    rt0.block_on(async {
        conn.execute("PRAGMA journal_mode = 'mvcc'", ()).await.ok(); conn.execute("PRAGMA synchronous = FULL", ()).await.ok();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v INTEGER NOT NULL, note TEXT NOT NULL)", ()).await.unwrap();
    });
    for (round, &w) in WRITERS.iter().enumerate() {
        let (ok, retried) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
        let end = Instant::now() + Duration::from_secs(SECS);
        let hs: Vec<_> = (0..w).map(|k| { let (ok, retried, db) = (ok.clone(), retried.clone(), db.clone()); std::thread::spawn(move || { let rt = rt(); let conn = db.connect().unwrap(); rt.block_on(async {
            conn.execute("PRAGMA synchronous = FULL", ()).await.ok();
            let mut i = 0u64;
            while Instant::now() < end {
                let r: Result<(), turso::Error> = async { conn.execute("BEGIN CONCURRENT", ()).await?; conn.execute("INSERT INTO t VALUES (?, ?, ?)", vec![TV::Integer(key(round, k, i)), TV::Integer(i as i64), TV::Text("durable".into())]).await?; conn.execute("COMMIT", ()).await?; Ok(()) }.await;
                match r { Ok(()) => { ok.fetch_add(1, Ordering::Relaxed); i += 1; } Err(_) => { conn.execute("ROLLBACK", ()).await.ok(); retried.fetch_add(1, Ordering::Relaxed); } }
            }
        }) }) }).collect();
        for h in hs { h.join().unwrap(); }
        let rows: i64 = rt0.block_on(async { let mut r = conn.query("SELECT count(*) FROM t WHERE id >= ? AND id < ?", vec![TV::Integer(key(round, 0, 0)), TV::Integer(key(round + 1, 0, 0))]).await.unwrap(); r.next().await.unwrap().unwrap().get(0).unwrap() });
        report("turso", w, ok.load(Ordering::Relaxed), retried.load(Ordering::Relaxed), rows);
    }
}

fn ours() {
    let sql = Arc::new(Sql::recreate(&PathBuf::from(DIR), true));
    let mut s0 = Session::default(); sql.exec_s(&mut s0, "CREATE TABLE t(id INTEGER PRIMARY KEY, v INT NOT NULL, note VARCHAR(16) NOT NULL)", &[]).unwrap();
    for (round, &w) in WRITERS.iter().enumerate() {
        let (ok, retried) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
        let end = Instant::now() + Duration::from_secs(SECS);
        let hs: Vec<_> = (0..w).map(|k| { let (ok, retried, sql) = (ok.clone(), retried.clone(), sql.clone()); std::thread::spawn(move || {
            let ins = sql.prepare("INSERT INTO t VALUES (?, ?, ?)").unwrap(); let mut s = Session::default(); let mut i = 0u64;
            while Instant::now() < end { match sql.execute_s(&mut s, &ins, &[Value::Int(key(round, k, i)), Value::Int(i as i64), Value::Text("durable".into())]) { Ok(_) => { ok.fetch_add(1, Ordering::Relaxed); i += 1; } Err(_) => { retried.fetch_add(1, Ordering::Relaxed); } } }
        }) }).collect();
        for h in hs { h.join().unwrap(); }
        let rows = match &sql.exec_s(&mut s0, "SELECT count(*) FROM t WHERE id >= ? AND id < ?", &[Value::Int(key(round, 0, 0)), Value::Int(key(round + 1, 0, 0))]).unwrap()[0][0] { Value::Int(n) => *n, _ => -1 };
        report("sqweezdb", w, ok.load(Ordering::Relaxed), retried.load(Ordering::Relaxed), rows);
    }
}

fn main() {
    println!("=== E50: durable single-row commits, many writers, {SECS} s per point (fsync per commit on every engine) ===");
    match std::env::args().nth(1).as_deref() { Some("sqlite") => sqlite(), Some("turso") => turso(), Some("ours") => ours(), _ => {
        // each engine in its own process, one after another; before each, flush the previous engine's writeback and let
        // the disk settle, so no engine pays for another's dirty pages. E50_ORDER=ours,turso,sqlite changes the order.
        let order = std::env::var("E50_ORDER").unwrap_or_else(|_| "sqlite,turso,ours".into());
        for e in order.split(',') {
            unsafe { libc::sync(); } std::thread::sleep(Duration::from_secs(5));
            let st = std::process::Command::new(std::env::current_exe().unwrap()).arg(e).status().unwrap(); if !st.success() { println!("{e}: exited with {st}"); }
        }
    } }
}
