//! Experiment 09: crash recovery round. kill -9 a writing process, recover, verify every acknowledged commit survived.
//! Both engines get the same treatment. Also: restart time after a crash with a large log / large WAL.
use dblab::engine::*;
use dblab::util::Rng;
use rusqlite::{Connection, params};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

const DIR: &str = "data/nocow";
const OUR_THREADS: usize = 32;
const SQL_THREADS: usize = 8;

fn ack_path(mode: &str, k: usize) -> PathBuf { PathBuf::from(format!("{DIR}/ack_{mode}_{k}.bin")) }
fn write_ack(f: &mut File, id: u64, seq: u64, score: f64) { let mut b = [0u8; 24]; b[..8].copy_from_slice(&id.to_le_bytes()); b[8..16].copy_from_slice(&seq.to_le_bytes()); b[16..].copy_from_slice(&score.to_bits().to_le_bytes()); f.write_all(&b).unwrap(); }
fn read_acks(mode: &str, threads: usize) -> Vec<(u64, u64, f64)> {
    let mut v = vec![];
    for k in 0..threads { if let Ok(d) = fs::read(ack_path(mode, k)) { for c in d.chunks_exact(24) { v.push((u64::from_le_bytes(c[..8].try_into().unwrap()), u64::from_le_bytes(c[8..16].try_into().unwrap()), f64::from_bits(u64::from_le_bytes(c[16..].try_into().unwrap())))); } } }
    v
}
fn clear_acks(mode: &str, threads: usize) { for k in 0..threads { fs::remove_file(ack_path(mode, k)).ok(); } }

fn victim_ours() -> ! {
    let dir = PathBuf::from(format!("{DIR}/e09dir")); let _ = fs::remove_dir_all(&dir);
    let e = Arc::new(Engine::new(true, &dir));
    let log = e.log.clone(); std::thread::spawn(move || log.committer());
    for k in 0..OUR_THREADS { let e = e.clone(); std::thread::spawn(move || {
        let mut ack = OpenOptions::new().create(true).write(true).truncate(true).open(ack_path("ours", k)).unwrap();
        let mut rng = Rng(1 + k as u64); let mut i = 0u64;
        loop {
            let id = (k as u64) * 1_000_000 + i;
            let seq = e.insert(Row::make(id, i as f64)).unwrap();
            write_ack(&mut ack, id, seq, i as f64);
            if i % 2 == 1 { let tid = (k as u64) * 1_000_000 + rng.next() % i; let sc = (1_000_000 + i) as f64;
                let seq = e.update(tid, |r| Row { score: sc, ..*r }).unwrap(); write_ack(&mut ack, tid, seq, sc); }
            i += 1;
        }
    }); }
    loop { std::thread::sleep(Duration::from_secs(3600)); }
}

fn victim_sqlite() -> ! {
    let p = PathBuf::from(format!("{DIR}/e09.db"));
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
    { let c = Connection::open(&p).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL NOT NULL, payload BLOB NOT NULL)").unwrap(); }
    for k in 0..SQL_THREADS { let p = p.clone(); std::thread::spawn(move || {
        let c = Connection::open(&p).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=60000;").unwrap();
        let mut st = c.prepare("INSERT INTO t VALUES(?,?,?)").unwrap();
        let mut ack = OpenOptions::new().create(true).write(true).truncate(true).open(ack_path("sqlite", k)).unwrap();
        let payload = [7u8; 100]; let mut i = 0u64;
        loop { let id = (k as u64) * 1_000_000 + i; st.execute(params![id as i64, i as f64, &payload[..]]).unwrap(); write_ack(&mut ack, id, 0, i as f64); i += 1; }
    }); }
    loop { std::thread::sleep(Duration::from_secs(3600)); }
}

fn victim_sqlite_bigwal() -> ! {
    let p = PathBuf::from(format!("{DIR}/e09big.db"));
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
    let c = Connection::open(&p).unwrap();
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL NOT NULL, country INTEGER NOT NULL, age INTEGER NOT NULL, name TEXT NOT NULL, payload BLOB NOT NULL)").unwrap();
    let payload = [7u8; 64];
    let mut id = 1i64;
    while id <= 2_000_000 { let tx = c.unchecked_transaction().unwrap(); { let mut st = tx.prepare_cached("INSERT INTO t VALUES(?,?,?,?,?,?)").unwrap(); for _ in 0..1000 { st.execute(params![id, id as f64, id % 20, 18 + id % 60, format!("u{id}@example.com"), &payload[..]]).unwrap(); id += 1; } } tx.commit().unwrap(); }
    let mut rng = Rng(3); let mut n = 0;
    while n < 1_000_000 { let tx = c.unchecked_transaction().unwrap(); { let mut st = tx.prepare_cached("UPDATE t SET score=? WHERE id=?").unwrap(); for _ in 0..1000 { st.execute(params![n as f64, (rng.next() % 2_000_000) as i64 + 1]).unwrap(); n += 1; } } tx.commit().unwrap(); }
    let wal = fs::metadata(format!("{DIR}/e09big.db-wal")).map(|m| m.len()).unwrap_or(0);
    println!("READY wal={} MB", wal / 1_000_000); std::io::stdout().flush().unwrap();
    loop { std::thread::sleep(Duration::from_secs(3600)); }
}

fn spawn(mode: &str) -> std::process::Child { Command::new(std::env::current_exe().unwrap()).arg(mode).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap() }


fn round_ours(delay_ms: u64) {
    clear_acks("ours", OUR_THREADS);
    let mut child = spawn("victim-ours");
    std::thread::sleep(Duration::from_millis(delay_ms));
    child.kill().unwrap(); child.wait().unwrap();
    let t = Instant::now();
    let dir = PathBuf::from(format!("{DIR}/e09dir"));
    let data = fs::read(Log::seg_path(&Log::new(&dir), 1)).unwrap();
    let (_, _, torn) = parse_log(&data);
    let (e, rs) = Engine::recover(&dir, false, 12);
    let n = rs.replayed as usize;
    let rt = t.elapsed();
    let acks = read_acks("ours", OUR_THREADS);
    let mut latest: HashMap<u64, (u64, f64)> = HashMap::new();
    for (id, seq, sc) in &acks { let x = latest.entry(*id).or_insert((0, 0.0)); if *seq > x.0 { *x = (*seq, *sc); } }
    let mut bad = 0u64;
    for (id, (seq, sc)) in &latest { match e.get_versioned(*id) { Some((v, r)) => if v < *seq || (v == *seq && r.score != *sc) { bad += 1 }, None => bad += 1 } }
    println!("[ours]   kill -9 after {delay_ms:>5}ms: log {:>5.1} MB, {n:>7} records, torn tail {torn} B | acked commits {:>7} on {:>6} ids | LOST: {bad} | replay {:.3}s",
        data.len() as f64 / 1e6, acks.len(), latest.len(), rt.as_secs_f64());
}

fn round_sqlite(delay_ms: u64) {
    clear_acks("sqlite", SQL_THREADS);
    let mut child = spawn("victim-sqlite");
    std::thread::sleep(Duration::from_millis(delay_ms));
    child.kill().unwrap(); child.wait().unwrap();
    let t = Instant::now();
    let c = Connection::open(format!("{DIR}/e09.db")).unwrap();
    let total: i64 = c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    let rt = t.elapsed();
    let acks = read_acks("sqlite", SQL_THREADS);
    let mut st = c.prepare("SELECT score FROM t WHERE id=?").unwrap();
    let mut bad = 0u64;
    for (id, _, sc) in &acks { match st.query_row(params![*id as i64], |r| r.get::<_, f64>(0)) { Ok(v) => if v != *sc { bad += 1 }, Err(_) => bad += 1 } }
    println!("[sqlite] kill -9 after {delay_ms:>5}ms: rows in db {total:>7} | acked commits {:>7} | LOST: {bad} | open+recover {:.3}s", acks.len(), rt.as_secs_f64());
}

fn restart_time() {
    // ours: build a 3M-record log quickly (visible mode), then time a cold replay
    let dir = PathBuf::from(format!("{DIR}/e09bigdir")); let _ = fs::remove_dir_all(&dir);
    let e = Arc::new(Engine::new(false, &dir));
    let log = e.log.clone(); let c = std::thread::spawn(move || log.committer());
    let hs: Vec<_> = (0..8).map(|k| { let e = e.clone(); std::thread::spawn(move || { let mut rng = Rng(9 + k as u64);
        for i in 0..250_000u64 { e.insert(Row::make(1 + k as u64 * 250_000 + i, i as f64)).unwrap(); }
        for i in 0..125_000u64 { let _ = e.update(1 + rng.next() % 2_000_000, |r| Row { score: i as f64, ..*r }); } }) }).collect();
    for h in hs { h.join().unwrap(); }
    e.log.stop(); c.join().unwrap(); drop(e);
    let t = Instant::now();
    let data_len = fs::metadata(Log::seg_path(&Log::new(&dir), 1)).unwrap().len();
    let (e, rs) = Engine::recover(&dir, false, 12);
    let n = rs.replayed; let rt = t.elapsed();
    println!("[ours]   restart: replay {n} records ({:.0} MB) into memory: {:.3}s = {:.1}M records/s, rows {}", data_len as f64 / 1e6, rt.as_secs_f64(), n as f64 / rt.as_secs_f64() / 1e6, e.rows_allocated());
    let _ = fs::remove_dir_all(&dir);

    // sqlite: process writes 2M rows + 1M updates with checkpointing OFF, gets killed, then time open + first query (WAL recovery)
    let mut child = spawn("victim-sqlite-bigwal");
    let mut rd = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    loop { line.clear(); if rd.read_line(&mut line).unwrap() == 0 { break; } if line.starts_with("READY") { break; } }
    child.kill().unwrap(); child.wait().unwrap();
    let t = Instant::now();
    let c = Connection::open(format!("{DIR}/e09big.db")).unwrap();
    let total: i64 = c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    let open = t.elapsed();
    println!("[sqlite] restart after kill with un-checkpointed WAL ({}): open + count(*) = {:.3}s, rows {total}", line.trim(), open.as_secs_f64());
    let t = Instant::now();
    c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    println!("[sqlite]   (+ full checkpoint of that WAL: {:.3}s)", t.elapsed().as_secs_f64());
}

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("victim-ours") => victim_ours(),
        Some("victim-sqlite") => victim_sqlite(),
        Some("victim-sqlite-bigwal") => victim_sqlite_bigwal(),
        _ => {}
    }
    println!("=== E09: crash recovery round (kill -9 while writing) ===");
    for d in [600, 1200, 2500] { round_ours(d); }
    for d in [1200, 2500] { round_sqlite(d); }
    restart_time();
    for suf in ["e09.log", "e09.db", "e09.db-wal", "e09.db-shm", "e09big.db", "e09big.db-wal", "e09big.db-shm"] { fs::remove_file(format!("{DIR}/{suf}")).ok(); }
    clear_acks("ours", OUR_THREADS); clear_acks("sqlite", SQL_THREADS);
}
