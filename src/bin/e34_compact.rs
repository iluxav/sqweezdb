//! Experiment 34: compression of every object and compaction of small chunks in the bucket (H45/H46), with a bucket-fed
//! follower running through the compaction; Litestream's bucket for the same data on the SQLite side.
use sqweezdb::cloud::{clear_prefix, download, start_puller, start_uploader, CloudConfig};
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::Value;
use rusqlite::{params, Connection};
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const N: i64 = 1_000_000;
const DIR: &str = "data/nocow/e34dir"; const STORE: &str = "data/nocow/s3store"; const SQDB: &str = "data/nocow/e34lite/t.db";
const CREATE: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, ts INT)";
fn count(sql: &Sql) -> i64 { let mut s = Session::default(); match sql.exec_s(&mut s, "SELECT count(*) FROM t", &[]).unwrap()[0][0] { Value::Int(i) => i, _ => -1 } }
fn max_id(sql: &Sql) -> i64 { let mut s = Session::default(); match sql.exec_s(&mut s, "SELECT max(id) FROM t", &[]).unwrap()[0][0] { Value::Int(i) => i, _ => -1 } }
fn bucket_objects(bucket: &str, sub: &str) -> (usize, u64) { let mut n = 0; let mut b = 0; fn walk(p: &std::path::Path, n: &mut usize, b: &mut u64) { if let Ok(rd) = fs::read_dir(p) { for e in rd.flatten() { let m = e.metadata().unwrap(); if m.is_dir() { walk(&e.path(), n, b); } else { *n += 1; *b += m.len(); } } } } walk(&PathBuf::from(format!("{STORE}/{bucket}/{sub}")), &mut n, &mut b); (n, b) }
fn build(dir: &PathBuf) -> Sql { let sql = Sql::recreate(dir, false); let mut s = Session::default(); sql.exec_s(&mut s, CREATE, &[]).unwrap(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?)").unwrap(); for id in 1..=N { if id % 1000 == 1 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Float((id % 1000) as f64), Value::Int(id % 20), Value::Int(id * 7919 % 1_000_003)]).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } } sql }
fn mb(b: u64) -> f64 { b as f64 / 1e6 }

fn main() {
    println!("=== E34: compression and compaction in the bucket (1M rows; local rclone S3 server; Litestream 0.5.17) ===");
    clear_prefix(&CloudConfig::local("sqweezdb", "")); clear_prefix(&CloudConfig::local("litestream", "")); std::thread::sleep(Duration::from_secs(2));
    let dir = PathBuf::from(format!("{DIR}/primary")); let sql = Arc::new(build(&dir)); let st = sql.snapshot();
    // ---- A. compression ----
    let mut cfg = CloudConfig::local("sqweezdb", "primary"); cfg.compact_after = Duration::from_secs(5); cfg.compact_every = Duration::from_secs(2);
    let t = Instant::now(); let up = start_uploader(cfg.clone(), dir.clone(), sql.db.log.clone());
    while up.stats.snapshots.load(Ordering::Relaxed) == 0 { std::thread::sleep(Duration::from_millis(20)); }
    println!("A. snapshot: {:.1} MB on disk -> {:.1} MB in the bucket (zstd level 3, {:.0}x), compressed in {} ms, uploaded in {:.2} s total; E33 uncompressed: 49 MB in 1.1 s", mb(st.bytes), mb(up.stats.bytes.load(Ordering::Relaxed)), st.bytes as f64 / up.stats.bytes.load(Ordering::Relaxed).max(1) as f64, up.stats.compress_ms.load(Ordering::Relaxed), t.elapsed().as_secs_f64());
    let (b0, raw0, c0) = (up.stats.bytes.load(Ordering::Relaxed), up.stats.raw_bytes.load(Ordering::Relaxed), up.stats.compress_ms.load(Ordering::Relaxed));
    { let mut s = Session::default(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?)").unwrap(); let t = Instant::now(); let mut id = N + 1; while t.elapsed() < Duration::from_secs(5) { sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Float((id % 977) as f64), Value::Int(id % 20), Value::Int(id * 7919 % 1_000_003)]).unwrap(); id += 1; std::thread::sleep(Duration::from_micros(400)); } let seq = sql.current_seq(); while up.stats.cloud_seq.load(Ordering::Relaxed) < seq { std::thread::sleep(Duration::from_millis(20)); } }
    let (b1, raw1, c1) = (up.stats.bytes.load(Ordering::Relaxed) - b0, up.stats.raw_bytes.load(Ordering::Relaxed) - raw0, up.stats.compress_ms.load(Ordering::Relaxed) - c0);
    println!("A. log chunks for 5 s of writes: {:.2} MB of records -> {:.2} MB in the bucket ({:.1}x), compression {} ms total ({:.0} MB/s)", mb(raw1), mb(b1), raw1 as f64 / b1.max(1) as f64, c1, if c1 > 0 { mb(raw1) / (c1 as f64 / 1e3) } else { 0.0 });
    // ---- B. a follower, then a cloud-durable burst that makes thousands of small objects, then compaction ----
    let fdir = PathBuf::from(format!("{DIR}/follower")); let (_, _, dlb) = download(&cfg, &fdir).unwrap(); let (fol, fh) = Sql::follow(&fdir, Duration::from_millis(5)).unwrap(); let puller = start_puller(cfg.clone(), fdir.clone(), Duration::from_millis(20));
    println!("B. follower built from the bucket ({:.1} MB downloaded): {} rows (primary {})", mb(dlb), count(&fol), count(&sql));
    sql.db.log.set_cloud_wait(true, Some(up.kick.clone()));
    let done = Arc::new(AtomicU64::new(0)); let stop = Arc::new(AtomicBool::new(false)); let puts0 = up.stats.puts.load(Ordering::Relaxed);
    let hs: Vec<_> = (0..64usize).map(|k| { let (sql, done, stop) = (sql.clone(), done.clone(), stop.clone()); std::thread::spawn(move || { let mut s = Session::default(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?)").unwrap(); let mut id = 10_000_000 + k as i64 * 1_000_000; while !stop.load(Ordering::Relaxed) { sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Float(1.0), Value::Int(1), Value::Int(id)]).unwrap(); done.fetch_add(1, Ordering::Relaxed); id += 1; } }) }).collect();
    std::thread::sleep(Duration::from_secs(6)); stop.store(true, Ordering::Relaxed); for h in hs { h.join().unwrap(); } sql.db.log.set_cloud_wait(false, None);
    let (n_before, b_before) = bucket_objects("sqweezdb", "primary/log"); let puts = up.stats.puts.load(Ordering::Relaxed) - puts0;
    println!("B. cloud-durable burst, 64 writers for 6 s: {} commits, {puts} uploads -> the log prefix holds {n_before} objects, {:.1} MB", done.load(Ordering::Relaxed), mb(b_before));
    let t = Instant::now(); let merges0 = up.stats.merges.load(Ordering::Relaxed);
    let mut last_n = n_before; loop { std::thread::sleep(Duration::from_secs(2)); let (n, _) = bucket_objects("sqweezdb", "primary/log"); if n == last_n && t.elapsed() > Duration::from_secs(8) { break; } last_n = n; if t.elapsed() > Duration::from_secs(60) { break; } }
    let (n_after, b_after) = bucket_objects("sqweezdb", "primary/log");
    println!("B. compaction (chunks older than 5 s, checked every 2 s): {} merges, {} deletes in {:.0} s -> {n_after} objects, {:.1} MB", up.stats.merges.load(Ordering::Relaxed) - merges0, up.stats.deletes.load(Ordering::Relaxed), t.elapsed().as_secs_f64(), mb(b_after));
    // the follower through all of it
    let seq = sql.current_seq(); let t = Instant::now(); while fh.applied() < seq && t.elapsed() < Duration::from_secs(30) { std::thread::sleep(Duration::from_millis(10)); }
    let mut s = Session::default(); let get = fol.prepare("SELECT id FROM t WHERE id = ?").unwrap(); let mut fs_ = Session::default(); let mut lags = vec![];
    for k in 0..10i64 { let id = 90_000_000 + k; let t = Instant::now(); sql.exec_s(&mut s, "INSERT INTO t VALUES (?,?,?,?)", &[Value::Int(id), Value::Float(1.0), Value::Int(1), Value::Int(0)]).unwrap(); loop { if !fol.execute_s(&mut fs_, &get, &[Value::Int(id)]).unwrap().is_empty() { break; } std::thread::sleep(Duration::from_millis(1)); if t.elapsed() > Duration::from_secs(30) { break; } } lags.push(t.elapsed().as_secs_f64() * 1e3); std::thread::sleep(Duration::from_millis(100)); }
    lags.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!("B. follower after the burst and the compaction: {} rows vs primary {} ({}), max id {} vs {}; a new commit is visible after {:.0} ms (median of 10, 1 s cadence), {} chunks pulled", count(&fol), count(&sql), if count(&fol) == count(&sql) { "EXACT" } else { "MISMATCH" }, max_id(&fol), max_id(&sql), lags[5], puller.chunks.load(Ordering::Relaxed));
    // ---- C. restore from the compacted bucket ----
    let rdir = PathBuf::from(format!("{DIR}/restored")); let t = Instant::now(); let (_, chunks, bytes) = download(&cfg, &rdir).unwrap(); let (r, rs) = Sql::open(&rdir, false, 12).unwrap(); let rt = t.elapsed();
    println!("C. restore from the compacted bucket: {chunks} objects, {:.1} MB downloaded and decompressed, open {:.2} s total (replayed {} records) -> {} rows vs primary {} ({})", mb(bytes), rt.as_secs_f64(), rs.replayed, count(&r), count(&sql), if count(&r) == count(&sql) { "EXACT" } else { "MISMATCH" });
    drop(r); drop(get); drop(puller); drop(fh); drop(fol); drop(up); drop(sql); let _ = fs::remove_dir_all(DIR);
    // ---- D. Litestream's bucket for the same data ----
    let _ = fs::remove_dir_all("data/nocow/e34lite"); fs::create_dir_all("data/nocow/e34lite").unwrap();
    { let c = Connection::open(SQDB).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL, country INTEGER, ts INTEGER);").unwrap(); let tx = c.unchecked_transaction().unwrap(); { let mut st = tx.prepare("INSERT INTO t VALUES (?,?,?,?)").unwrap(); for id in 1..=N { st.execute(params![id, (id % 1000) as f64, id % 20, id * 7919 % 1_000_003]).unwrap(); } } tx.commit().unwrap(); c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap(); }
    let dbsize = fs::metadata(SQDB).unwrap().len();
    fs::write("data/nocow/e34lite/litestream.yml", "dbs:\n  - path: data/nocow/e34lite/t.db\n    replicas:\n      - type: s3\n        bucket: litestream\n        path: t\n        endpoint: http://127.0.0.1:9000\n        region: us-east-1\n        access-key-id: minioadmin\n        secret-access-key: minioadmin\n        force-path-style: true\n        sync-interval: 1s\n").unwrap();
    let mut ls = Command::new("tools/litestream").args(["replicate", "-config", "data/nocow/e34lite/litestream.yml"]).stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap();
    let t = Instant::now(); loop { let (n, _) = bucket_objects("litestream", ""); if n > 0 { break; } if t.elapsed() > Duration::from_secs(120) { break; } std::thread::sleep(Duration::from_millis(200)); } std::thread::sleep(Duration::from_secs(3));
    let (n0, b0) = bucket_objects("litestream", ""); println!("D. litestream: SQLite file {:.1} MB -> {:.1} MB in the bucket ({n0} objects, LZ4) after {:.1} s", mb(dbsize), mb(b0), t.elapsed().as_secs_f64());
    { let c = Connection::open(SQDB).unwrap(); c.execute_batch("PRAGMA synchronous=NORMAL").unwrap(); let t = Instant::now(); let mut id = 10_000_000i64; let mut n = 0u64; while t.elapsed() < Duration::from_secs(6) { c.execute("INSERT INTO t VALUES (?,?,?,?)", params![id, 1.0f64, 1, id]).unwrap(); id += 1; n += 1; } println!("D. litestream: {n} autocommit inserts in 6 s", ); }
    std::thread::sleep(Duration::from_secs(3)); let (n1, b1) = bucket_objects("litestream", ""); std::thread::sleep(Duration::from_secs(30)); let (n2, b2) = bucket_objects("litestream", "");
    ls.kill().unwrap(); ls.wait().unwrap();
    let t = Instant::now(); let st = Command::new("tools/litestream").args(["restore", "-config", "data/nocow/e34lite/litestream.yml", "-o", "data/nocow/e34lite/restored.db", "data/nocow/e34lite/t.db"]).output().unwrap(); let rt = t.elapsed();
    let got: i64 = Connection::open("data/nocow/e34lite/restored.db").ok().and_then(|c| c.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).ok()).unwrap_or(-1);
    println!("D. litestream bucket after the burst: {n1} objects, {:.1} MB; 30 s later: {n2} objects, {:.1} MB; restore {:.2} s -> {got} rows{}", mb(b1), mb(b2), rt.as_secs_f64(), if st.status.success() { "" } else { " (restore reported an error)" });
    let _ = fs::remove_dir_all("data/nocow/e34lite");
}
