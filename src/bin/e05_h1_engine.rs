//! Experiment 05 (H1): a real engine core — typed fixed-width rows, sharded in-memory index, pending-version
//! conflict detection, append-only log with group commit. Readers never block. Compare against SQLite E00 numbers.
//! Simplifications (tracked): no CRC on log records, no recovery (H8), hash-sharded index (range scans later).
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Condvar, Mutex, RwLock};
use std::time::Instant;

struct Rng(u64);
impl Rng { fn next(&mut self) -> u64 { let mut x = self.0; x ^= x << 13; x ^= x >> 7; x ^= x << 17; self.0 = x; x } }

#[repr(C)] #[derive(Clone, Copy)]
struct Row { id: u64, score: f64, name: [u8; 32], payload: [u8; 64] }
impl Row {
    fn new(id: u64, score: f64) -> Row { Row { id, score, name: [b'n'; 32], payload: [7; 64] } }
    fn bytes(&self) -> &[u8] { unsafe { std::slice::from_raw_parts(self as *const Row as *const u8, std::mem::size_of::<Row>()) } }
}
struct Entry { committed: Option<(u64, Row)>, pending: Option<(u64, Row)> }

const SHARDS: usize = 256;
const PRELOAD: u64 = 1_000_000;
fn shard_of(id: u64) -> usize { (id.wrapping_mul(0x9E3779B97F4A7C15) >> 56) as usize }

struct LogState { buf: Vec<u8>, seq: u64, durable: u64, stop: bool, batches: u64, bytes: u64 }
struct Log { st: Mutex<LogState>, cv_batch: Condvar, cv_durable: Condvar }
impl Log {
    fn new() -> Log { Log { st: Mutex::new(LogState { buf: Vec::with_capacity(1 << 22), seq: 0, durable: 0, stop: false, batches: 0, bytes: 0 }), cv_batch: Condvar::new(), cv_durable: Condvar::new() } }
    fn append(&self, op: u8, row: &Row) -> u64 {
        let mut s = self.st.lock().unwrap();
        s.seq += 1; let seq = s.seq;
        let b = row.bytes();
        s.buf.extend_from_slice(&((9 + b.len()) as u32).to_le_bytes());
        s.buf.extend_from_slice(&seq.to_le_bytes());
        s.buf.push(op);
        s.buf.extend_from_slice(b);
        self.cv_batch.notify_one();
        seq
    }
    fn wait_durable(&self, seq: u64) {
        let mut s = self.st.lock().unwrap();
        while s.durable < seq { s = self.cv_durable.wait(s).unwrap(); }
    }
    fn committer(&self, mut f: File) {
        let mut local: Vec<u8> = Vec::with_capacity(1 << 22);
        loop {
            let seq_end;
            {
                let mut s = self.st.lock().unwrap();
                while s.buf.is_empty() && !s.stop { s = self.cv_batch.wait(s).unwrap(); }
                if s.buf.is_empty() { return; }
                std::mem::swap(&mut local, &mut s.buf);
                seq_end = s.seq; s.batches += 1; s.bytes += local.len() as u64;
            }
            f.write_all(&local).unwrap();
            f.sync_data().unwrap();
            local.clear();
            let mut s = self.st.lock().unwrap();
            s.durable = seq_end;
            self.cv_durable.notify_all();
        }
    }
}

#[derive(Debug)] enum Abort { NotFound, Conflict, Duplicate }

struct Engine { shards: Vec<RwLock<BTreeMap<u64, Entry>>>, log: Arc<Log>, durable: bool }
impl Engine {
    fn new(durable: bool) -> Engine {
        Engine { shards: (0..SHARDS).map(|_| RwLock::new(BTreeMap::new())).collect(), log: Arc::new(Log::new()), durable }
    }
    fn preload(&self, n: u64) { for id in 1..=n { self.shards[shard_of(id)].write().unwrap().insert(id, Entry { committed: Some((1, Row::new(id, id as f64))), pending: None }); } }
    fn get(&self, id: u64) -> Option<Row> { self.shards[shard_of(id)].read().unwrap().get(&id).and_then(|e| e.committed.map(|(_, r)| r)) }
    fn insert(&self, row: Row) -> Result<(), Abort> {
        let sh = &self.shards[shard_of(row.id)];
        let seq;
        { let mut m = sh.write().unwrap();
          if m.contains_key(&row.id) { return Err(Abort::Duplicate); }
          seq = self.log.append(1, &row);
          m.insert(row.id, Entry { committed: None, pending: Some((seq, row)) }); }
        if self.durable { self.log.wait_durable(seq); }
        let mut m = sh.write().unwrap();
        let e = m.get_mut(&row.id).unwrap(); e.committed = Some((seq, row)); e.pending = None;
        Ok(())
    }
    fn update(&self, id: u64, f: impl Fn(&Row) -> Row) -> Result<(), Abort> {
        let sh = &self.shards[shard_of(id)];
        let (ver, row) = { let m = sh.read().unwrap(); match m.get(&id).and_then(|e| e.committed) { Some(x) => x, None => return Err(Abort::NotFound) } };
        let new = f(&row);
        let seq;
        { let mut m = sh.write().unwrap(); let e = m.get_mut(&id).unwrap();
          if e.pending.is_some() { return Err(Abort::Conflict); }
          if e.committed.map(|(v, _)| v) != Some(ver) { return Err(Abort::Conflict); }
          seq = self.log.append(2, &new);
          e.pending = Some((seq, new)); }
        if self.durable { self.log.wait_durable(seq); }
        let mut m = sh.write().unwrap(); let e = m.get_mut(&id).unwrap(); e.committed = Some((seq, new)); e.pending = None;
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq)] enum W { Insert, UpdateUniform, UpdateHot }
static NEXT_ID: AtomicU64 = AtomicU64::new(PRELOAD + 1);

fn run(engine: &Arc<Engine>, w: W, threads: usize, per: usize, readers: usize) {
    let barrier = Arc::new(Barrier::new(threads + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let rh: Vec<_> = (0..readers).map(|k| { let e = engine.clone(); let stop = stop.clone(); std::thread::spawn(move || {
        let mut rng = Rng(777 + k as u64); let mut n = 0u64; let mut sum = 0f64;
        while !stop.load(Ordering::Relaxed) { sum += e.get(rng.next() % PRELOAD + 1).map(|r| r.score).unwrap_or(0.0); n += 1; }
        (n, sum) })}).collect();
    let hs: Vec<_> = (0..threads).map(|k| { let e = engine.clone(); let b = barrier.clone(); std::thread::spawn(move || {
        let mut rng = Rng(0x9E3779B97F4A7C15 ^ (k as u64 + 1));
        let base = NEXT_ID.fetch_add(per as u64, Ordering::Relaxed);
        let mut lat = Vec::with_capacity(per); let mut attempts = 0u64;
        b.wait();
        for i in 0..per {
            let t = Instant::now();
            loop {
                attempts += 1;
                let r = match w {
                    W::Insert => e.insert(Row::new(base + i as u64, i as f64)),
                    W::UpdateUniform => e.update(rng.next() % PRELOAD + 1, |r| Row { score: r.score + 1.0, ..*r }),
                    W::UpdateHot => { let id = if rng.next() % 2 == 0 { rng.next() % 1000 + 1 } else { rng.next() % PRELOAD + 1 }; e.update(id, |r| Row { score: r.score + 1.0, ..*r }) }
                };
                match r { Ok(()) => break, Err(Abort::Conflict) => continue, Err(x) => panic!("{x:?}") }
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
    all.sort();
    let n = all.len();
    let name = match w { W::Insert => "insert new rows", W::UpdateUniform => "update random row (1M)", W::UpdateHot => "update 50% hot (1000 keys)" };
    let rd = if readers > 0 { format!("  + {readers} readers: {:.0} reads/s", reads as f64 / el.as_secs_f64()) } else { String::new() };
    println!("{:<30} {:<8} thr={:<4} txn/s={:>10.0} p50={:>8.1}us p99={:>8.1}us retries={:<6} ({:.2}s){}",
        name, if engine.durable { "DURABLE" } else { "visible" }, threads, n as f64 / el.as_secs_f64(),
        all[n / 2].as_secs_f64() * 1e6, all[(n as f64 * 0.99) as usize].as_secs_f64() * 1e6, attempts - n as u64, el.as_secs_f64(), rd);
}

fn with_engine(durable: bool, f: impl FnOnce(&Arc<Engine>)) {
    let path = PathBuf::from("data/nocow/h1.log");
    let file = OpenOptions::new().create(true).write(true).truncate(true).open(&path).unwrap();
    let engine = Arc::new(Engine::new(durable));
    let t = Instant::now(); engine.preload(PRELOAD); let pl = t.elapsed();
    let log = engine.log.clone();
    let c = std::thread::spawn(move || log.committer(file));
    f(&engine);
    { let mut s = engine.log.st.lock().unwrap(); s.stop = true; engine.log.cv_batch.notify_one(); }
    c.join().unwrap();
    let s = engine.log.st.lock().unwrap();
    println!("  (preload 1M rows {:.2}s; log: {} txns, {} fsync batches, {:.0} MB)", pl.as_secs_f64(), s.seq, s.batches, s.bytes as f64 / 1e6);
    fs::remove_file(path).ok();
}

fn main() {
    println!("=== E05 / H1: engine core with group commit, typed rows, conflict checks ===");
    println!("SQLite refs (E00, system lib): 1 row/txn FULL 2,000-2,160 txn/s | NORMAL 115k-165k | 8 writers NORMAL 88k, p99 565us | 4 writers FULL 1,730");
    with_engine(true, |e| {
        for thr in [1, 4, 16, 64, 256] { run(e, W::Insert, thr, 2000, 0); }
        for thr in [1, 4, 16, 64, 256] { run(e, W::UpdateUniform, thr, 2000, 0); }
        for thr in [16, 64, 256] { run(e, W::UpdateHot, thr, 2000, 0); }
        run(e, W::UpdateUniform, 64, 2000, 4);
    });
    with_engine(false, |e| {
        for thr in [1, 4, 8, 12] { run(e, W::Insert, thr, 2_000_000 / thr, 0); }
        for thr in [1, 4, 8, 12] { run(e, W::UpdateUniform, thr, 2_000_000 / thr, 0); }
        run(e, W::UpdateHot, 8, 250_000, 0);
        run(e, W::UpdateUniform, 8, 250_000, 4);
    });
}
