//! Experiment 10 (H17): snapshots. (a) kill -9 while writing WITH periodic snapshots, recover from snapshot + log tail,
//! verify acks. (b) restart-time rematch vs E09. (c) cost of snapshots to running durable writers.
use dblab::engine::*;
use dblab::util::Rng;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const DIR: &str = "data/nocow";
const THREADS: usize = 32;

fn ack_path(k: usize) -> PathBuf { PathBuf::from(format!("{DIR}/ack_snap_{k}.bin")) }
fn write_ack(f: &mut File, id: u64, seq: u64, score: f64) { let mut b = [0u8; 24]; b[..8].copy_from_slice(&id.to_le_bytes()); b[8..16].copy_from_slice(&seq.to_le_bytes()); b[16..].copy_from_slice(&score.to_bits().to_le_bytes()); f.write_all(&b).unwrap(); }
fn read_acks() -> Vec<(u64, u64, f64)> { let mut v = vec![]; for k in 0..THREADS { if let Ok(d) = fs::read(ack_path(k)) { for c in d.chunks_exact(24) { v.push((u64::from_le_bytes(c[..8].try_into().unwrap()), u64::from_le_bytes(c[8..16].try_into().unwrap()), f64::from_bits(u64::from_le_bytes(c[16..].try_into().unwrap())))); } } } v }
fn clear_acks() { for k in 0..THREADS { fs::remove_file(ack_path(k)).ok(); } }

fn victim() -> ! {
    let dir = PathBuf::from(format!("{DIR}/e10dir")); let _ = fs::remove_dir_all(&dir);
    let e = Arc::new(Engine::new(true, &dir));
    { let l = e.log.clone(); std::thread::spawn(move || l.committer()); }
    { let e = e.clone(); std::thread::spawn(move || loop { std::thread::sleep(Duration::from_millis(700)); let s = e.snapshot();
        eprintln!("    victim snapshot: {} rows, {:.1} MB, low_water {}, {:.3}s, deleted {} old segment(s)", s.rows, s.bytes as f64 / 1e6, s.low_water, s.secs, s.deleted_segments); }); }
    for k in 0..THREADS { let e = e.clone(); std::thread::spawn(move || {
        let mut ack = OpenOptions::new().create(true).write(true).truncate(true).open(ack_path(k)).unwrap();
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

fn kill_round(delay_ms: u64) {
    clear_acks();
    let mut child = Command::new(std::env::current_exe().unwrap()).arg("victim").stdout(Stdio::null()).stderr(Stdio::inherit()).spawn().unwrap();
    std::thread::sleep(Duration::from_millis(delay_ms));
    child.kill().unwrap(); child.wait().unwrap();
    let dir = PathBuf::from(format!("{DIR}/e10dir"));
    let t = Instant::now();
    let (e, rs) = Engine::recover(&dir, false, 12);
    let rt = t.elapsed();
    let acks = read_acks();
    let mut latest: HashMap<u64, (u64, f64)> = HashMap::new();
    for (id, seq, sc) in &acks { let x = latest.entry(*id).or_insert((0, 0.0)); if *seq > x.0 { *x = (*seq, *sc); } }
    let mut bad = 0u64;
    for (id, (seq, sc)) in &latest { match e.get_versioned(*id) { Some((v, r)) => if v < *seq || (v == *seq && r.score != *sc) { bad += 1 }, None => bad += 1 } }
    let l = Log::new(&dir); let segs = Log::list_segments(&dir);
    let seg_mb: f64 = segs.iter().map(|f| fs::metadata(l.seg_path(*f)).map(|m| m.len()).unwrap_or(0)).sum::<u64>() as f64 / 1e6;
    println!("[ours+snapshots] kill -9 after {delay_ms}ms: snapshot {} rows (low_water {}), {} log segment(s) {:.1} MB, replayed {} skipped {} | acked {} on {} ids | LOST: {bad} | restart {:.3}s (load {:.3}s + replay {:.3}s)",
        rs.snap_rows, rs.low_water, rs.segments, seg_mb, rs.replayed, rs.skipped, acks.len(), latest.len(), rt.as_secs_f64(), rs.snap_secs, rs.replay_secs);
    let _ = fs::remove_dir_all(&dir); clear_acks();
}

fn writers_visible(e: &Arc<Engine>, inserts_per: u64, updates_per: u64, id_base: u64, total_ids: u64) {
    let hs: Vec<_> = (0..8).map(|k| { let e = e.clone(); std::thread::spawn(move || { let mut rng = Rng(9 + k as u64 + id_base);
        for i in 0..inserts_per { e.insert(Row::make(id_base + k * inserts_per + i, i as f64)).unwrap(); }
        for i in 0..updates_per { let _ = e.update(1 + rng.next() % total_ids, |r| Row { score: i as f64, ..*r }); } }) }).collect();
    for h in hs { h.join().unwrap(); }
}

fn restart_rematch() {
    println!("--- restart rematch (E09: log-only replay of 3M records took 1.43s; SQLite ~1ms normally, 0.66s with a 4.3 GB WAL) ---");
    let dir = PathBuf::from(format!("{DIR}/e10big")); let _ = fs::remove_dir_all(&dir);
    let e = Arc::new(Engine::new(false, &dir));
    let c = { let l = e.log.clone(); std::thread::spawn(move || l.committer()) };
    writers_visible(&e, 250_000, 125_000, 1, 2_000_000);
    let s = e.snapshot();
    println!("snapshot of {} rows: {:.0} MB in {:.3}s, low_water {}, deleted {} segment(s)", s.rows, s.bytes as f64 / 1e6, s.secs, s.low_water, s.deleted_segments);
    writers_visible(&e, 0, 125_000, 1, 2_000_000);   // 1M updates AFTER the snapshot = the tail
    e.log.stop(); c.join().unwrap();
    let check: Vec<(u64, f64)> = (0..50).map(|i| { let id = 1 + i * 39_999; (id, e.get(id).unwrap().score) }).collect();
    drop(e);
    let t = Instant::now(); let (e2, rs) = Engine::recover(&dir, false, 12);
    println!("restart A (snapshot {} rows + 1M-record tail), 12 threads: {:.3}s = load {:.3}s + replay {} records {:.3}s (skipped {})", rs.snap_rows, t.elapsed().as_secs_f64(), rs.snap_secs, rs.replayed, rs.replay_secs, rs.skipped);
    for (id, sc) in &check { assert_eq!(e2.get(*id).unwrap().score, *sc, "row {id} differs after restart"); }
    let c = { let l = e2.log.clone(); std::thread::spawn(move || l.committer()) };
    let s = e2.snapshot(); println!("snapshot again: {:.3}s, deleted {} segment(s)", s.secs, s.deleted_segments);
    e2.log.stop(); c.join().unwrap(); drop(e2);
    let t = Instant::now(); let (e3, rs) = Engine::recover(&dir, false, 12);
    println!("restart B (snapshot {} rows, empty tail), 12 threads: {:.3}s = load {:.3}s + replay {} records {:.3}s", rs.snap_rows, t.elapsed().as_secs_f64(), rs.snap_secs, rs.replayed, rs.replay_secs);
    for (id, sc) in &check { assert_eq!(e3.get(*id).unwrap().score, *sc); }
    drop(e3);
    let t = Instant::now(); let (e4, rs) = Engine::recover(&dir, false, 1);
    println!("restart B, 1 thread: {:.3}s = load {:.3}s + replay {:.3}s", t.elapsed().as_secs_f64(), rs.snap_secs, rs.replay_secs);
    drop(e4);
    let _ = fs::remove_dir_all(&dir);
}

fn snapshot_impact() {
    println!("--- cost of snapshots to 64 durable writers (E07 without snapshots: ~58k txn/s) ---");
    let dir = PathBuf::from(format!("{DIR}/e10imp")); let _ = fs::remove_dir_all(&dir);
    let e = Arc::new(Engine::new(true, &dir));
    for id in 1..=1_000_000u64 { e.load_committed(Row::make(id, id as f64), 1); }
    let c = { let l = e.log.clone(); std::thread::spawn(move || l.committer()) };
    let stop = Arc::new(AtomicBool::new(false));
    let hs: Vec<_> = (0..64).map(|k| { let e = e.clone(); let stop = stop.clone(); std::thread::spawn(move || { let mut rng = Rng(100 + k as u64); let mut n = 0u64;
        while !stop.load(Ordering::Relaxed) { loop { match e.update(1 + rng.next() % 1_000_000, |r| Row { score: r.score + 1.0, ..*r }) { Ok(_) => break, Err(Abort::Conflict) => continue, Err(x) => panic!("{x:?}") } } n += 1; } n }) }).collect();
    let snaps = { let e = e.clone(); let stop = stop.clone(); std::thread::spawn(move || { let mut v = vec![]; while !stop.load(Ordering::Relaxed) { std::thread::sleep(Duration::from_millis(1000)); if stop.load(Ordering::Relaxed) { break; } v.push(e.snapshot()); } v }) };
    let t = Instant::now(); std::thread::sleep(Duration::from_millis(4500)); stop.store(true, Ordering::Relaxed);
    let n: u64 = hs.into_iter().map(|h| h.join().unwrap()).sum(); let el = t.elapsed();
    let snaps = snaps.join().unwrap();
    e.log.stop(); c.join().unwrap();
    println!("64 durable writers for {:.1}s with a snapshot every 1s: {:.0} txn/s; {} snapshots, each {:.0} MB in {:.3}s avg, deleted {} old segments total",
        el.as_secs_f64(), n as f64 / el.as_secs_f64(), snaps.len(), snaps.iter().map(|s| s.bytes).sum::<u64>() as f64 / 1e6 / snaps.len().max(1) as f64, snaps.iter().map(|s| s.secs).sum::<f64>() / snaps.len().max(1) as f64, snaps.iter().map(|s| s.deleted_segments).sum::<usize>());
    let _ = fs::remove_dir_all(&dir);
}

fn main() {
    if std::env::args().nth(1).as_deref() == Some("victim") { victim(); }
    println!("=== E10 / H17: snapshots ===");
    for d in [2000, 3600] { kill_round(d); }
    restart_rematch();
    snapshot_impact();
}
