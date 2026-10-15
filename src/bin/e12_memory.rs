//! Experiment 12 (H20): memory footprint for the same 2M rows. Each configuration in its own process; RSS and peak RSS from /proc.
use dblab::engine::*;
use dblab::util::Rng;
use rusqlite::{Connection, params};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;

const N: u64 = 2_000_000;
const DB: &str = "data/nocow/mem.db";
const DIR: &str = "data/nocow/memdir";

fn rss_mb() -> (f64, f64) {
    let s = fs::read_to_string("/proc/self/status").unwrap();
    let get = |k: &str| s.lines().find(|l| l.starts_with(k)).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0) / 1024.0;
    (get("VmRSS:"), get("VmHWM:"))
}
fn report(label: &str) { let (r, h) = rss_mb(); println!("{label}: rss={r:.0} MB peak={h:.0} MB"); }

fn sqlite_child(tuned: bool) {
    let c = Connection::open(DB).unwrap();
    c.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
    if tuned { c.execute_batch("PRAGMA cache_size=-262144; PRAGMA mmap_size=1073741824;").unwrap(); }
    let n: i64 = c.query_row("SELECT count(*) FROM t WHERE age > 40", [], |r| r.get(0)).unwrap();
    let mut st = c.prepare("SELECT score FROM t WHERE id = ?").unwrap();
    let mut rng = Rng(5); let mut s = 0.0;
    for _ in 0..100_000 { let id = (rng.next() % N) as i64 + 1; s += st.query_row(params![id], |r| r.get::<_, f64>(0)).unwrap(); }
    let _: f64 = c.query_row("SELECT sum(score) FROM t", [], |r| r.get(0)).unwrap();
    report(&format!("sqlite {} after scan+100k lookups (check {n} {s:.0})", if tuned { "tuned 256MB cache + mmap" } else { "default 2MB cache" }));
}

fn ours_child() {
    let dir = PathBuf::from(format!("{DIR}/live")); let _ = fs::remove_dir_all(&dir);
    report("ours empty engine");
    let e = Engine::new(false, &dir);
    let mut rng = Rng(7);
    for id in 1..=N { e.load_committed(Row::make(id, (rng.next() % 100000) as f64 / 100.0), 1); }
    let n = e.scan(0..e.chunk_count(), 0u64, |a, r| if r.age > 40 { *a += 1 });
    let mut rng = Rng(5); let mut s = 0.0;
    for _ in 0..100_000 { s += e.get(rng.next() % N + 1).unwrap().score; }
    report(&format!("ours 2M rows loaded, after scan+100k lookups (check {n} {s:.0})"));
    let _ = fs::remove_dir_all(&dir);
}

fn ours_parts_child() {
    let (b, _) = rss_mb();
    let arena: Vec<Slot> = (0..N).map(|i| Slot { version: 1, pending: 0, row: Row::make(i + 1, 1.0) }).collect();
    let (a1, _) = rss_mb(); println!("  arena Vec<Slot> 2M x {} B: {:.0} MB", std::mem::size_of::<Slot>(), a1 - b);
    let pk: BTreeMap<u64, u32> = (0..N).map(|i| (i + 1, i as u32)).collect();
    let (a2, _) = rss_mb(); println!("  PK index BTreeMap<u64,u32> 2M: {:.0} MB", a2 - a1);
    let sec: HashMap<[u8; 32], u32> = (0..N).map(|i| (name_for(i + 1), i as u32)).collect();
    let (a3, _) = rss_mb(); println!("  secondary index HashMap<[u8;32],u32> 2M: {:.0} MB", a3 - a2);
    std::hint::black_box((&arena, &pk, &sec));
}

fn ours_restart_child() {
    let dir = PathBuf::from(format!("{DIR}/snap"));
    let (e, rs) = Engine::recover(&dir, false, 12);
    let n = e.scan(0..e.chunk_count(), 0u64, |a, r| if r.age > 40 { *a += 1 });
    report(&format!("ours after restart from snapshot of {} rows (check {n})", rs.snap_rows));
}

fn child(mode: &str) -> String { let o = Command::new(std::env::current_exe().unwrap()).arg(mode).stderr(Stdio::inherit()).output().unwrap(); String::from_utf8_lossy(&o.stdout).trim_end().to_string() }

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("sqlite-default") => return sqlite_child(false),
        Some("sqlite-tuned") => return sqlite_child(true),
        Some("ours") => return ours_child(),
        Some("ours-parts") => return ours_parts_child(),
        Some("ours-restart") => return ours_restart_child(),
        _ => {}
    }
    println!("=== E12 / H20: memory footprint, 2M rows of 120 B (240 MB of raw row data) ===");
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{DB}{suf}")).ok(); }
    { let c = Connection::open(DB).unwrap();
      c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL NOT NULL, country INTEGER NOT NULL, age INTEGER NOT NULL, name TEXT NOT NULL, payload BLOB NOT NULL); CREATE UNIQUE INDEX t_name ON t(name);").unwrap();
      let mut rng = Rng(7);
      let tx = c.unchecked_transaction().unwrap();
      { let mut st = tx.prepare("INSERT INTO t VALUES(?,?,?,?,?,?)").unwrap();
        for id in 1..=N { let r = Row::make(id, (rng.next() % 100000) as f64 / 100.0); st.execute(params![id as i64, r.score, r.country, r.age, std::str::from_utf8(&r.name).unwrap().trim_end_matches('\0'), &r.payload[..]]).unwrap(); } }
      tx.commit().unwrap(); c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap(); }
    println!("sqlite file on disk: {:.0} MB (with the unique index on name)", fs::metadata(DB).unwrap().len() as f64 / 1e6);
    { let dir = PathBuf::from(format!("{DIR}/snap")); let _ = fs::remove_dir_all(&dir);
      let e = Arc::new(Engine::new(false, &dir)); let mut rng = Rng(7);
      for id in 1..=N { e.load_committed(Row::make(id, (rng.next() % 100000) as f64 / 100.0), 1); }
      let s = e.snapshot(); println!("ours snapshot file on disk: {:.0} MB", s.bytes as f64 / 1e6); }
    println!("{}", child("sqlite-default"));
    println!("{}", child("sqlite-tuned"));
    println!("{}", child("ours"));
    println!("ours, where it goes:"); println!("{}", child("ours-parts"));
    println!("{}", child("ours-restart"));
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{DB}{suf}")).ok(); }
    let _ = fs::remove_dir_all(DIR);
}
