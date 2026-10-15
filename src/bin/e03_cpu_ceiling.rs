//! Experiment 03: in-process ceilings with zero I/O. What is the best case a Rust engine could reach?
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Barrier, Mutex, RwLock};
use std::time::Instant;

struct Rng(u64);
impl Rng { fn next(&mut self) -> u64 { let mut x = self.0; x ^= x << 13; x ^= x >> 7; x ^= x << 17; self.0 = x; x } }
const ROWS: usize = 1_000_000;
const TOTAL: usize = 24_000_000;

#[repr(C)] #[derive(Clone, Copy)]
struct Row { id: u64, score: f64, name: [u8; 32], payload: [u8; 64] }

fn bench<F: Fn(usize) -> f64 + Send + Sync + 'static>(label: &str, threads: usize, f: Arc<F>) {
    let per = TOTAL / threads;
    let barrier = Arc::new(Barrier::new(threads + 1));
    let hs: Vec<_> = (0..threads).map(|k| { let f = f.clone(); let b = barrier.clone(); std::thread::spawn(move || {
        let mut rng = Rng(0x9E3779B97F4A7C15 ^ (k as u64 + 1)); b.wait();
        let mut sum = 0f64; for _ in 0..per { sum += f((rng.next() % ROWS as u64) as usize); } sum })}).collect();
    barrier.wait();
    let t = Instant::now();
    let _ = hs.into_iter().map(|h| h.join().unwrap()).sum::<f64>();
    let el = t.elapsed();
    println!("{:<48} thr={:<2} ops/s={:>12.0}  per-op={:.1}ns", label, threads, TOTAL as f64 / el.as_secs_f64(), el.as_nanos() as f64 * threads as f64 / TOTAL as f64);
}

fn main() {
    println!("=== E03 CPU ceilings (no I/O) ===");
    let rows: Arc<Vec<Row>> = Arc::new((0..ROWS).map(|i| Row { id: i as u64, score: i as f64, name: [b'a'; 32], payload: [7; 64] }).collect());
    let btree: Arc<BTreeMap<u64, Row>> = Arc::new(rows.iter().map(|r| (r.id, *r)).collect());
    let hmap: Arc<HashMap<u64, Row>> = Arc::new(rows.iter().map(|r| (r.id, *r)).collect());
    let rw: Arc<RwLock<BTreeMap<u64, Row>>> = Arc::new(RwLock::new((*btree).clone()));
    let mx: Arc<Mutex<BTreeMap<u64, Row>>> = Arc::new(Mutex::new((*btree).clone()));

    for thr in [1, 8, 12] { let r = rows.clone(); bench("fixed-width rows, direct offset (ideal)", thr, Arc::new(move |i: usize| r[i].score)); }
    for thr in [1, 8, 12] { let r = btree.clone(); bench("BTreeMap immutable, no lock", thr, Arc::new(move |i: usize| btree_get(&r, i))); }
    for thr in [1, 8, 12] { let r = hmap.clone(); bench("HashMap immutable, no lock", thr, Arc::new(move |i: usize| r[&(i as u64)].score)); }
    for thr in [1, 8, 12] { let r = rw.clone(); bench("BTreeMap behind std RwLock (read lock/op)", thr, Arc::new(move |i: usize| r.read().unwrap()[&(i as u64)].score)); }
    for thr in [1, 8] { let r = mx.clone(); bench("BTreeMap behind std Mutex (lock/op)", thr, Arc::new(move |i: usize| r.lock().unwrap()[&(i as u64)].score)); }

    // write path CPU: append fixed row to log + index insert, single thread
    let t = Instant::now();
    let mut log: Vec<u8> = Vec::with_capacity(ROWS * 112);
    let mut idx: HashMap<u64, u64> = HashMap::with_capacity(ROWS);
    for i in 0..ROWS { let r = Row { id: i as u64, score: i as f64, name: [b'a'; 32], payload: [7; 64] };
        let off = log.len() as u64; log.extend_from_slice(unsafe { std::slice::from_raw_parts(&r as *const Row as *const u8, 112) }); idx.insert(i as u64, off); }
    let el = t.elapsed();
    println!("{:<48} thr=1  ops/s={:>12.0}  per-op={:.1}ns", "append 112B row to log + hash index insert", ROWS as f64 / el.as_secs_f64(), el.as_nanos() as f64 / ROWS as f64);
    let t = Instant::now();
    let mut bt: BTreeMap<u64, Row> = BTreeMap::new();
    for i in 0..ROWS { bt.insert(i as u64, Row { id: i as u64, score: i as f64, name: [b'a'; 32], payload: [7; 64] }); }
    let el = t.elapsed();
    println!("{:<48} thr=1  ops/s={:>12.0}  per-op={:.1}ns", "BTreeMap insert sequential 112B rows", ROWS as f64 / el.as_secs_f64(), el.as_nanos() as f64 / ROWS as f64);
    let t = Instant::now();
    let mut rng = Rng(42);
    let mut bt2: BTreeMap<u64, Row> = BTreeMap::new();
    for i in 0..ROWS { bt2.insert(rng.next(), Row { id: i as u64, score: i as f64, name: [b'a'; 32], payload: [7; 64] }); }
    let el = t.elapsed();
    println!("{:<48} thr=1  ops/s={:>12.0}  per-op={:.1}ns", "BTreeMap insert RANDOM keys 112B rows", ROWS as f64 / el.as_secs_f64(), el.as_nanos() as f64 / ROWS as f64);
    std::hint::black_box((&bt, &bt2, &log, &idx));
}
fn btree_get(m: &BTreeMap<u64, Row>, i: usize) -> f64 { m[&(i as u64)].score }
