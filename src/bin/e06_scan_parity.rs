//! Experiment 06 (H14): read-every-row parity. SQLite's strongest event (E04: 43–52M rows/s).
//! Same 1M rows in both: SQLite (tuned: 256MB cache + mmap) vs ours (contiguous typed rows, and the engine's sharded BTreeMap).
use rusqlite::{Connection, params};
use std::collections::{BTreeMap, BinaryHeap};
use std::cmp::Reverse;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

struct Rng(u64);
impl Rng { fn next(&mut self) -> u64 { let mut x = self.0; x ^= x << 13; x ^= x >> 7; x ^= x << 17; self.0 = x; x } }
const ROWS: u64 = 1_000_000;

#[repr(C)] #[derive(Clone, Copy)]
struct Row { id: u64, score: f64, country: u32, age: u32, name: [u8; 32], payload: [u8; 64] }

fn gen_rows() -> Vec<Row> {
    let mut rng = Rng(7);
    (1..=ROWS).map(|i| Row { id: i, score: (rng.next() % 100000) as f64 / 100.0, country: (i % 20) as u32, age: (18 + i % 60) as u32, name: [b'n'; 32], payload: [7; 64] }).collect()
}

fn median(label: &str, iters: usize, mut f: impl FnMut() -> f64) {
    let mut ts = Vec::new(); let mut v = 0.0;
    for _ in 0..iters { let t = Instant::now(); v = f(); ts.push(t.elapsed()); }
    ts.sort(); let m = ts[iters / 2];
    println!("{:<62} median={:>9.3}ms  rows/s={:>12.0}   (check {:.1})", label, m.as_secs_f64() * 1e3, ROWS as f64 / m.as_secs_f64(), v);
}

fn sqlite_side(rows: &[Row]) {
    let p = PathBuf::from("data/nocow/scan.db");
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
    let c = Connection::open(&p).unwrap();
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-262144; PRAGMA mmap_size=1073741824;
        CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL NOT NULL, country INTEGER NOT NULL, age INTEGER NOT NULL, name TEXT NOT NULL, payload BLOB NOT NULL)").unwrap();
    let tx = c.unchecked_transaction().unwrap();
    { let mut st = tx.prepare("INSERT INTO t VALUES(?,?,?,?,?,?)").unwrap();
      for r in rows { st.execute(params![r.id as i64, r.score, r.country, r.age, std::str::from_utf8(&r.name).unwrap(), &r.payload[..]]).unwrap(); } }
    tx.commit().unwrap();
    c.execute_batch("ANALYZE; PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    let _: i64 = c.query_row("SELECT count(*) FROM t WHERE age > 40", [], |r| r.get(0)).unwrap(); // warm
    median("[sqlite] S1 count(*) WHERE age > 40", 7, || c.query_row("SELECT count(*) FROM t WHERE age > 40", [], |r| r.get::<_, i64>(0)).unwrap() as f64);
    median("[sqlite] S2 sum(score) all rows", 7, || c.query_row("SELECT sum(score) FROM t", [], |r| r.get::<_, f64>(0)).unwrap());
    median("[sqlite] S3 GROUP BY country (int) avg(score), count(*)", 7, || { let mut st = c.prepare_cached("SELECT country, avg(score), count(*) FROM t GROUP BY country").unwrap(); st.query_map([], |r| r.get::<_, f64>(1)).unwrap().map(|x| x.unwrap()).sum::<f64>() });
    median("[sqlite] S4 ORDER BY score DESC LIMIT 10", 7, || { let mut st = c.prepare_cached("SELECT score FROM t ORDER BY score DESC LIMIT 10").unwrap(); st.query_map([], |r| r.get::<_, f64>(0)).unwrap().map(|x| x.unwrap()).sum::<f64>() });
    median("[sqlite] S5 sum(score) WHERE country = 7 AND age BETWEEN 30 AND 50", 7, || c.query_row("SELECT sum(score) FROM t WHERE country = 7 AND age BETWEEN 30 AND 50", [], |r| r.get::<_, f64>(0)).unwrap());
}

fn par<F: Fn(usize) -> f64 + Send + Sync + 'static>(threads: usize, f: Arc<F>) -> f64 {
    let hs: Vec<_> = (0..threads).map(|k| { let f = f.clone(); std::thread::spawn(move || f(k)) }).collect();
    hs.into_iter().map(|h| h.join().unwrap()).sum()
}

fn ours_contiguous(rows: Arc<Vec<Row>>) {
    { let r = rows.clone(); median("[ours vec] S1 count WHERE age > 40, 1 thread", 7, move || r.iter().filter(|x| x.age > 40).count() as f64); }
    { let r = rows.clone(); median("[ours vec] S2 sum(score), 1 thread", 7, move || r.iter().map(|x| x.score).sum()); }
    { let r = rows.clone(); median("[ours vec] S3 GROUP BY country avg,count, 1 thread", 7, move || { let mut sum = [0f64; 20]; let mut n = [0u64; 20]; for x in r.iter() { sum[x.country as usize] += x.score; n[x.country as usize] += 1; } (0..20).map(|i| sum[i] / n[i] as f64).sum() }); }
    { let r = rows.clone(); median("[ours vec] S4 top-10 by score, 1 thread", 7, move || { let mut h = BinaryHeap::new(); for x in r.iter() { if h.len() < 10 { h.push(Reverse(x.score as i64 * 1000)); } else if let Some(&Reverse(m)) = h.peek() { if (x.score as i64 * 1000) > m { h.pop(); h.push(Reverse(x.score as i64 * 1000)); } } } h.into_iter().map(|Reverse(v)| v as f64 / 1000.0).sum() }); }
    { let r = rows.clone(); median("[ours vec] S5 sum WHERE country=7 AND age in 30..=50, 1 thread", 7, move || r.iter().filter(|x| x.country == 7 && (30..=50).contains(&x.age)).map(|x| x.score).sum()); }
    for thr in [4, 12] {
        let r = rows.clone();
        median(&format!("[ours vec] S1 count WHERE age > 40, {thr} threads"), 7, move || { let r = r.clone(); let chunk = r.len() / thr; par(thr, Arc::new(move |k: usize| r[k * chunk..(k + 1) * chunk].iter().filter(|x| x.age > 40).count() as f64)) });
        let r = rows.clone();
        median(&format!("[ours vec] S3 GROUP BY country, {thr} threads"), 7, move || { let r = r.clone(); let chunk = r.len() / thr; par(thr, Arc::new(move |k: usize| { let mut sum = [0f64; 20]; for x in r[k * chunk..(k + 1) * chunk].iter() { sum[x.country as usize] += x.score; } sum.iter().sum::<f64>() })) });
    }
}

fn ours_engine_shards(rows: &[Row]) {
    // the structure the E05 engine really has today: 256 hash shards of BTreeMap<u64, Row>
    const SHARDS: usize = 256;
    let mut shards: Vec<BTreeMap<u64, Row>> = (0..SHARDS).map(|_| BTreeMap::new()).collect();
    for r in rows { shards[(r.id.wrapping_mul(0x9E3779B97F4A7C15) >> 56) as usize].insert(r.id, *r); }
    let shards = Arc::new(shards);
    { let s = shards.clone(); median("[ours sharded btree] S1 count WHERE age > 40, 1 thread", 7, move || s.iter().map(|m| m.values().filter(|x| x.age > 40).count()).sum::<usize>() as f64); }
    { let s = shards.clone(); median("[ours sharded btree] S3 GROUP BY country, 1 thread", 7, move || { let mut sum = [0f64; 20]; for m in s.iter() { for x in m.values() { sum[x.country as usize] += x.score; } } sum.iter().sum() }); }
    for thr in [4, 12] {
        let s = shards.clone();
        median(&format!("[ours sharded btree] S1 count WHERE age > 40, {thr} threads"), 7, move || { let s = s.clone(); let per = SHARDS / thr; par(thr, Arc::new(move |k: usize| s[k * per..(k + 1) * per].iter().map(|m| m.values().filter(|x| x.age > 40).count()).sum::<usize>() as f64)) });
    }
}

fn main() {
    println!("=== E06 / H14: read-every-row parity, 1M rows of {} B ===", std::mem::size_of::<Row>());
    let rows = gen_rows();
    let t = Instant::now(); sqlite_side(&rows); let _ = t;
    let rows = Arc::new(rows);
    ours_contiguous(rows.clone());
    ours_engine_shards(&rows);
}
