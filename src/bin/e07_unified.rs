//! Experiment 07 (H16 + index round): ONE engine, replay the writer tests (E05) and the scan tests (E06) on it,
//! plus the unplayed events: lookup by unique secondary key (email) and ordered PK range scan.
use sqweezdb::engine::*;
use sqweezdb::util::Rng;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

const PRELOAD: u64 = 1_000_000;
static NEXT_ID: AtomicU64 = AtomicU64::new(PRELOAD + 1);
static RUN: AtomicU64 = AtomicU64::new(0);

fn median_ms(label: &str, iters: usize, mut f: impl FnMut() -> f64) -> f64 {
    let mut ts = Vec::new(); let mut v = 0.0;
    for _ in 0..iters { let t = Instant::now(); v = f(); ts.push(t.elapsed()); }
    ts.sort(); let m = ts[iters / 2].as_secs_f64() * 1e3;
    println!("{:<62} median={:>9.3}ms   (check {:.1})", label, m, v);
    m
}

fn scans(e: &Arc<Engine>, tag: &str) {
    median_ms(&format!("[{tag}] S1 count WHERE age > 40, 1 thread"), 7, || e.scan(0..e.chunk_count(), 0u64, |a, r| if r.age > 40 { *a += 1 }) as f64);
    for thr in [4, 12] {
        let e2 = e.clone();
        median_ms(&format!("[{tag}] S1 count WHERE age > 40, {thr} threads"), 7, move || {
            let n = e2.chunk_count(); let per = (n + thr - 1) / thr;
            let hs: Vec<_> = (0..thr).map(|k| { let e = e2.clone(); std::thread::spawn(move || e.scan(k * per..((k + 1) * per).min(n), 0u64, |a, r| if r.age > 40 { *a += 1 })) }).collect();
            hs.into_iter().map(|h| h.join().unwrap()).sum::<u64>() as f64 });
    }
    median_ms(&format!("[{tag}] S3 GROUP BY country avg,count, 1 thread"), 7, || { let (s, n) = e.scan(0..e.chunk_count(), ([0f64; 20], [0u64; 20]), |a, r| { a.0[r.country as usize] += r.score; a.1[r.country as usize] += 1; }); (0..20).map(|i| s[i] / n[i].max(1) as f64).sum() });
    median_ms(&format!("[{tag}] S4 top-10 by score, 1 thread"), 7, || { let h = e.scan(0..e.chunk_count(), BinaryHeap::new(), |h, r| { let k = (r.score * 100.0) as i64; if h.len() < 10 { h.push(Reverse(k)); } else if let Some(&Reverse(m)) = h.peek() { if k > m { h.pop(); h.push(Reverse(k)); } } }); h.into_iter().map(|Reverse(v)| v as f64 / 100.0).sum() });
}

fn lookups(e: &Arc<Engine>, tag: &str) {
    { let mut rng = Rng(5); let n = 500_000; let t = Instant::now(); let mut s = 0f64;
      for _ in 0..n { let id = rng.next() % PRELOAD + 1; s += e.get_by_name(&name_for(id)).map(|r| r.score).unwrap_or(-1e9); }
      println!("{:<62} lookups/s={:>10.0}   (check {:.1})", format!("[{tag}] L1 lookup by email (unique secondary index), 1 thread"), n as f64 / t.elapsed().as_secs_f64(), s); }
    for thr in [8] {
        let per = 500_000; let b = Arc::new(Barrier::new(thr + 1));
        let hs: Vec<_> = (0..thr).map(|k| { let e = e.clone(); let b = b.clone(); std::thread::spawn(move || { let mut rng = Rng(50 + k as u64); b.wait(); let mut s = 0f64; for _ in 0..per { let id = rng.next() % PRELOAD + 1; s += e.get_by_name(&name_for(id)).map(|r| r.score).unwrap_or(-1e9); } s }) }).collect();
        b.wait(); let t = Instant::now(); let _: f64 = hs.into_iter().map(|h| h.join().unwrap()).sum(); 
        println!("{:<62} lookups/s={:>10.0}", format!("[{tag}] L1 lookup by email, {thr} threads"), (thr * per) as f64 / t.elapsed().as_secs_f64());
    }
    { let mut rng = Rng(6); let n = 500_000; let t = Instant::now(); let mut s = 0f64;
      for _ in 0..n { s += e.get(rng.next() % PRELOAD + 1).map(|r| r.score).unwrap_or(-1e9); }
      println!("{:<62} lookups/s={:>10.0}   (check {:.1})", format!("[{tag}] L2 lookup by id, 1 thread"), n as f64 / t.elapsed().as_secs_f64(), s); }
    { let mut rng = Rng(7); let n = 20_000; let t = Instant::now(); let mut s = 0f64; let mut rows = 0;
      for _ in 0..n { let from = rng.next() % (PRELOAD - 1000) + 1; let (c, v) = e.range_sum(from, from + 1000); rows += c; s += v; }
      println!("{:<62} calls/s={:>10.0}  us/call={:.1}  rows/s={:.0}   (check {:.1})", format!("[{tag}] R1 ordered PK range scan, 1000 rows, sum(score)"), n as f64 / t.elapsed().as_secs_f64(), t.elapsed().as_secs_f64() * 1e6 / n as f64, rows as f64 / t.elapsed().as_secs_f64(), s); }
}

#[derive(Clone, Copy, PartialEq)] enum W { Insert, UpdateScore, UpdateHot, UpdateName }

fn run(e: &Arc<Engine>, w: W, threads: usize, per: usize, readers_id: usize, readers_name: usize, scanners: usize) {
    let run_id = RUN.fetch_add(1, Ordering::Relaxed);
    let barrier = Arc::new(Barrier::new(threads + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let rh: Vec<_> = (0..readers_id).map(|k| { let e = e.clone(); let stop = stop.clone(); std::thread::spawn(move || { let mut rng = Rng(777 + k as u64); let mut n = 0u64; let mut s = 0f64; while !stop.load(Ordering::Relaxed) { s += e.get(rng.next() % PRELOAD + 1).map(|r| r.score).unwrap_or(0.0); n += 1; } (n, s) }) }).collect();
    let nh: Vec<_> = (0..readers_name).map(|k| { let e = e.clone(); let stop = stop.clone(); std::thread::spawn(move || { let mut rng = Rng(999 + k as u64); let mut n = 0u64; let mut s = 0f64; while !stop.load(Ordering::Relaxed) { s += e.get_by_name(&name_for(rng.next() % PRELOAD + 1)).map(|r| r.score).unwrap_or(0.0); n += 1; } (n, s) }) }).collect();
    let sh: Vec<_> = (0..scanners).map(|_| { let e = e.clone(); let stop = stop.clone(); std::thread::spawn(move || { let mut n = 0u64; let mut ts = Vec::new(); while !stop.load(Ordering::Relaxed) { let t = Instant::now(); let c = e.scan(0..e.chunk_count(), 0u64, |a, r| if r.age > 40 { *a += 1 }); ts.push(t.elapsed()); n += c; } ts.sort(); (ts.len(), if ts.is_empty() { 0.0 } else { ts[ts.len() / 2].as_secs_f64() * 1e3 }, n) }) }).collect();
    let hs: Vec<_> = (0..threads).map(|k| { let e = e.clone(); let b = barrier.clone(); std::thread::spawn(move || {
        let mut rng = Rng(0x9E3779B97F4A7C15 ^ (k as u64 + 1) ^ (run_id << 40));
        let base = NEXT_ID.fetch_add(per as u64, Ordering::Relaxed);
        let mut lat = Vec::with_capacity(per); let mut attempts = 0u64;
        b.wait();
        for i in 0..per {
            let t = Instant::now();
            loop {
                attempts += 1;
                let r = match w {
                    W::Insert => e.insert(Row::make(base + i as u64, i as f64)),
                    W::UpdateScore => e.update(rng.next() % PRELOAD + 1, |r| Row { score: r.score + 1.0, ..*r }),
                    W::UpdateHot => { let id = if rng.next() % 2 == 0 { rng.next() % 1000 + 1 } else { rng.next() % PRELOAD + 1 }; e.update(id, |r| Row { score: r.score + 1.0, ..*r }) }
                    W::UpdateName => e.update_name(rng.next() % PRELOAD + 1, name_from(&format!("x{run_id}-{k}-{i}-{attempts}"))),
                };
                match r { Ok(_) => break, Err(Abort::Conflict) => continue, Err(x) => panic!("{x:?}") }
            }
            lat.push(t.elapsed());
        }
        (lat, attempts) })}).collect();
    barrier.wait();
    let t = Instant::now();
    let mut all = Vec::new(); let mut attempts = 0;
    for h in hs { let (l, a) = h.join().unwrap(); all.extend(l); attempts += a; }
    let el = t.elapsed();
    stop.store(true, Ordering::Relaxed);
    let reads: u64 = rh.into_iter().map(|h| h.join().unwrap().0).sum();
    let nreads: u64 = nh.into_iter().map(|h| h.join().unwrap().0).sum();
    let scans: Vec<(usize, f64, u64)> = sh.into_iter().map(|h| h.join().unwrap()).collect();
    all.sort();
    let n = all.len();
    let name = match w { W::Insert => "insert (2 indexes)", W::UpdateScore => "update score", W::UpdateHot => "update 50% hot", W::UpdateName => "update email (indexed)" };
    let mut extra = String::new();
    if readers_id > 0 { extra += &format!("  | {readers_id} id-readers {:.1}M/s", reads as f64 / el.as_secs_f64() / 1e6); }
    if readers_name > 0 { extra += &format!("  | {readers_name} email-readers {:.1}M/s", nreads as f64 / el.as_secs_f64() / 1e6); }
    if scanners > 0 { extra += &format!("  | scanner: {} full scans, median {:.1}ms", scans[0].0, scans[0].1); }
    println!("{:<24} {:<8} thr={:<4} txn/s={:>10.0} p50={:>7.1}us p99={:>7.1}us retries={:<6} ({:.2}s){}",
        name, if e.durable { "DURABLE" } else { "visible" }, threads, n as f64 / el.as_secs_f64(),
        all[n / 2].as_secs_f64() * 1e6, all[(n as f64 * 0.99) as usize].as_secs_f64() * 1e6, attempts - n as u64, el.as_secs_f64(), extra);
}

fn with_engine(durable: bool, f: impl FnOnce(&Arc<Engine>)) {
    let dir = PathBuf::from("data/nocow/e07dir"); let _ = fs::remove_dir_all(&dir);
    let e = Arc::new(Engine::new(durable, &dir));
    let t = Instant::now();
    let mut rng = Rng(7);
    for id in 1..=PRELOAD { e.load_committed(Row::make(id, (rng.next() % 100000) as f64 / 100.0), 1); }
    println!("--- engine durable={durable}: preload 1M rows {:.2}s ---", t.elapsed().as_secs_f64());
    let log = e.log.clone();
    let c = std::thread::spawn(move || log.committer());
    f(&e);
    e.log.stop(); c.join().unwrap();
    let s = e.log.st.lock().unwrap();
    println!("  (log: {} txns, {} fsync batches, {:.0} MB; rows allocated {})", s.seq, s.batches, s.bytes as f64 / 1e6, e.rows_allocated());
    let _ = fs::remove_dir_all(&dir);
}

fn main() {
    println!("=== E07 / H16: one engine, all events ===");
    println!("SQLite refs: scans S1 25.9ms S3 120.7ms S4 29.1ms (E06) | email lookup 800k-917k/s 1thr, 3.5-4.9M/s 8thr; PK range 1000 rows 25-28us (E04)");
    println!("             writes 1 row/txn FULL 2,160/s, 4 writers FULL 1,730; NORMAL 1thr 115-165k, 8 writers 88k | bulk insert w/ unique text index 935k rows/s; update indexed col 24k/s (E00/E04)");
    with_engine(true, |e| {
        scans(e, "ours");
        lookups(e, "ours");
        for thr in [1, 4, 16, 64, 256] { run(e, W::Insert, thr, 2000, 0, 0, 0); }
        for thr in [1, 4, 64, 256] { run(e, W::UpdateScore, thr, 2000, 0, 0, 0); }
        for thr in [64, 256] { run(e, W::UpdateHot, thr, 2000, 0, 0, 0); }
        for thr in [16, 64, 256] { run(e, W::UpdateName, thr, 2000, 0, 0, 0); }
        run(e, W::UpdateScore, 64, 4000, 4, 2, 1);
        run(e, W::Insert, 64, 4000, 4, 2, 1);
        scans(e, "ours after writes");
    });
    with_engine(false, |e| {
        for thr in [1, 4, 8, 12] { run(e, W::Insert, thr, 2_000_000 / thr, 0, 0, 0); }
        for thr in [1, 4, 8, 12] { run(e, W::UpdateScore, thr, 2_000_000 / thr, 0, 0, 0); }
        run(e, W::UpdateName, 8, 250_000, 0, 0, 0);
        run(e, W::UpdateScore, 8, 250_000, 4, 2, 1);
    });
}
