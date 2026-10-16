//! Experiment 33: continuous backup to an S3-compatible bucket, restore (plain and point-in-time), a follower fed only
//! by the bucket, and a cloud-durable commit mode; against Litestream doing the same for SQLite. The bucket is a local
//! rclone S3 server (real S3 API over loopback), so request latency here is ~1 ms; a real cloud adds 20–80 ms per request.
use sqweezdb::cloud::{clear_prefix, download, start_puller, start_uploader, CloudConfig};
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::Value;
use rusqlite::{params, Connection};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const N: i64 = 1_000_000;
const DIR: &str = "data/nocow/e33dir"; const STORE: &str = "data/nocow/s3store"; const SQDB: &str = "data/nocow/e33lite/t.db";
const CREATE: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, ts INT)";
fn pct(v: &mut Vec<f64>, p: f64) -> f64 { if v.is_empty() { return 0.0; } v.sort_by(|a, b| a.partial_cmp(b).unwrap()); v[((v.len() as f64 - 1.0) * p) as usize] }
fn count(sql: &Sql) -> i64 { let mut s = Session::default(); match sql.exec_s(&mut s, "SELECT count(*) FROM t", &[]).unwrap()[0][0] { Value::Int(i) => i, _ => -1 } }
fn max_id(sql: &Sql) -> i64 { let mut s = Session::default(); match sql.exec_s(&mut s, "SELECT max(id) FROM t", &[]).unwrap()[0][0] { Value::Int(i) => i, _ => -1 } }
fn bucket_objects(bucket: &str) -> (usize, u64) { let mut n = 0; let mut b = 0; fn walk(p: &std::path::Path, n: &mut usize, b: &mut u64) { if let Ok(rd) = fs::read_dir(p) { for e in rd.flatten() { let m = e.metadata().unwrap(); if m.is_dir() { walk(&e.path(), n, b); } else { *n += 1; *b += m.len(); } } } } walk(&PathBuf::from(format!("{STORE}/{bucket}")), &mut n, &mut b); (n, b) }
fn build(dir: &PathBuf) -> Sql { let sql = Sql::recreate(dir, false); let mut s = Session::default(); sql.exec_s(&mut s, CREATE, &[]).unwrap(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?)").unwrap(); for id in 1..=N { if id % 1000 == 1 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Float((id % 1000) as f64), Value::Int(id % 20), Value::Int(0)]).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } } sql }

fn main() {
    let a: Vec<String> = std::env::args().collect();
    // ---- crash victim: primary + uploader, writes sequential ids at ~2k/s and prints each acknowledged id ----
    if a.get(1).map(|s| s.as_str()) == Some("victim") {
        let dir = PathBuf::from(format!("{DIR}/victim")); let sql = build(&dir); sql.snapshot();
        let mut cfg = CloudConfig::local("sqweezdb", "victim"); if a.get(2).map(|s| s.as_str()) == Some("fast") { cfg.interval = Duration::from_millis(100); } let up = start_uploader(cfg, dir.clone(), sql.db.log.clone());
        while up.stats.snapshots.load(Ordering::Relaxed) == 0 { std::thread::sleep(Duration::from_millis(50)); }
        if a.get(2).map(|s| s.as_str()) == Some("durable") { sql.db.log.set_cloud_wait(true, Some(up.kick.clone())); }
        println!("ready"); std::io::stdout().flush().unwrap();
        let mut s = Session::default(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?)").unwrap(); let mut id = N + 1; let out = std::io::stdout(); let mut o = out.lock();
        loop { sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Float(1.0), Value::Int(1), Value::Int(0)]).unwrap(); writeln!(o, "{id}").unwrap(); o.flush().unwrap(); id += 1; std::thread::sleep(Duration::from_micros(400)); }
    }
    if a.get(1).map(|s| s.as_str()) == Some("lite-victim") {
        // SQLite writer at ~2k/s; litestream runs beside it (started by the parent); prints each acknowledged id
        let c = Connection::open(SQDB).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();
        let mut id: i64 = c.query_row("SELECT max(id) + 1 FROM t", [], |r| r.get(0)).unwrap();
        println!("ready"); std::io::stdout().flush().unwrap();
        let out = std::io::stdout(); let mut o = out.lock();
        loop { c.execute("INSERT INTO t VALUES (?,?,?,?)", params![id, 1.0f64, 1, 0]).unwrap(); writeln!(o, "{id}").unwrap(); o.flush().unwrap(); id += 1; std::thread::sleep(Duration::from_micros(400)); }
    }
    println!("=== E33: backup to an S3-compatible bucket (1M rows; local rclone S3 server; Litestream 0.5.17 for SQLite) ===");
    clear_prefix(&CloudConfig::local("sqweezdb", "")); clear_prefix(&CloudConfig::local("litestream", "")); std::thread::sleep(Duration::from_secs(2));
    // ---- A. continuous backup: upload cadence, lag, cost ----
    let dir = PathBuf::from(format!("{DIR}/primary")); let sql = Arc::new(build(&dir)); let st = sql.snapshot();
    let cfg = CloudConfig::local("sqweezdb", "primary"); let t = Instant::now(); let mut up = Some(start_uploader(cfg.clone(), dir.clone(), sql.db.log.clone()));
    let upr = |up: &Option<sqweezdb::cloud::Uploader>| up.as_ref().unwrap().stats.clone();
    while upr(&up).snapshots.load(Ordering::Relaxed) == 0 { std::thread::sleep(Duration::from_millis(20)); }
    println!("A. snapshot of {} rows ({:.0} MB) uploaded in {:.2} s", st.rows, st.bytes as f64 / 1e6, t.elapsed().as_secs_f64());
    let commits: Arc<std::sync::Mutex<Vec<(u64, Instant)>>> = Arc::new(std::sync::Mutex::new(vec![])); let stop = Arc::new(AtomicBool::new(false));
    let w = { let (sql, commits, stop) = (sql.clone(), commits.clone(), stop.clone()); std::thread::spawn(move || { let mut s = Session::default(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?)").unwrap(); let mut id = N + 1; let mut n = 0u64; while !stop.load(Ordering::Relaxed) { sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Float(1.0), Value::Int(1), Value::Int(0)]).unwrap(); commits.lock().unwrap().push((sql.current_seq(), Instant::now())); id += 1; n += 1; std::thread::sleep(Duration::from_micros(400)); } n }) };
    let puts0 = upr(&up).puts.load(Ordering::Relaxed); let t = Instant::now(); let mut ages = vec![];
    while t.elapsed() < Duration::from_secs(10) { std::thread::sleep(Duration::from_millis(100)); let cs = upr(&up).cloud_seq.load(Ordering::Relaxed); let c = commits.lock().unwrap(); if let Some((_, at)) = c.iter().rev().find(|(s, _)| *s <= cs) { ages.push(at.elapsed().as_secs_f64() * 1e3); } }
    stop.store(true, Ordering::Relaxed); let n = w.join().unwrap(); let seq = sql.current_seq(); while upr(&up).cloud_seq.load(Ordering::Relaxed) < seq { std::thread::sleep(Duration::from_millis(20)); }
    let puts = upr(&up).puts.load(Ordering::Relaxed) - puts0;
    println!("A. 10 s at {:.0} writes/s, upload every 1 s: {puts} chunk uploads ({:.1}/s), newest record in the bucket was {:.0} ms old (median), {:.0} ms (max); cost at this cadence: {:.0} requests/day = ${:.2}/month on S3 (R2 ${:.2})", n as f64 / 10.0, puts as f64 / 10.0, pct(&mut ages, 0.5), pct(&mut ages, 1.0), puts as f64 / 10.0 * 86400.0, puts as f64 / 10.0 * 86400.0 * 30.0 * 5e-6, puts as f64 / 10.0 * 86400.0 * 30.0 * 4.5e-6);
    // ---- B. cloud-durable commits: acknowledge only once the record is in the bucket ----
    sql.db.log.set_cloud_wait(true, Some(up.as_ref().unwrap().kick.clone()));
    for (round, thr) in [1usize, 16, 256].into_iter().enumerate() {
        let done = Arc::new(AtomicU64::new(0)); let lat = Arc::new(std::sync::Mutex::new(Vec::<f64>::new())); let stop = Arc::new(AtomicBool::new(false)); let puts0 = upr(&up).puts.load(Ordering::Relaxed);
        let hs: Vec<_> = (0..thr).map(|k| { let (sql, done, lat, stop) = (sql.clone(), done.clone(), lat.clone(), stop.clone()); std::thread::spawn(move || { let mut s = Session::default(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?)").unwrap(); let mut id = 20_000_000 + round as i64 * 1_000_000_000 + k as i64 * 1_000_000; let mut l = vec![]; while !stop.load(Ordering::Relaxed) { let t = Instant::now(); sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Float(1.0), Value::Int(1), Value::Int(0)]).unwrap(); l.push(t.elapsed().as_secs_f64() * 1e3); done.fetch_add(1, Ordering::Relaxed); id += 1; } lat.lock().unwrap().extend(l); }) }).collect();
        std::thread::sleep(Duration::from_secs(3)); stop.store(true, Ordering::Relaxed); for h in hs { h.join().unwrap(); }
        let mut l = lat.lock().unwrap().clone(); let puts = upr(&up).puts.load(Ordering::Relaxed) - puts0;
        println!("B. cloud-durable commits, {thr:>3} writers: {:>7.0} commits/s, latency p50 {:.1} ms p99 {:.1} ms, {:.0} uploads/s (each upload carries {:.0} commits)", done.load(Ordering::Relaxed) as f64 / 3.0, pct(&mut l, 0.5), pct(&mut l, 0.99), puts as f64 / 3.0, done.load(Ordering::Relaxed) as f64 / puts.max(1) as f64);
    }
    sql.db.log.set_cloud_wait(false, None);
    { up = None; let mut c3 = cfg.clone(); c3.rtt = Duration::from_millis(30); let up3 = start_uploader(c3, dir.clone(), sql.db.log.clone()); sql.db.log.set_cloud_wait(true, Some(up3.kick.clone()));
      for (round, thr) in [1usize, 256].into_iter().enumerate() {
        let done = Arc::new(AtomicU64::new(0)); let lat = Arc::new(std::sync::Mutex::new(Vec::<f64>::new())); let stop = Arc::new(AtomicBool::new(false)); let puts0 = up3.stats.puts.load(Ordering::Relaxed);
        let hs: Vec<_> = (0..thr).map(|k| { let (sql, done, lat, stop) = (sql.clone(), done.clone(), lat.clone(), stop.clone()); std::thread::spawn(move || { let mut s = Session::default(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?)").unwrap(); let mut id = 5_000_000_000 + round as i64 * 1_000_000_000 + k as i64 * 1_000_000; let mut l = vec![]; while !stop.load(Ordering::Relaxed) { let t = Instant::now(); sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Float(1.0), Value::Int(1), Value::Int(0)]).unwrap(); l.push(t.elapsed().as_secs_f64() * 1e3); done.fetch_add(1, Ordering::Relaxed); id += 1; } lat.lock().unwrap().extend(l); }) }).collect();
        std::thread::sleep(Duration::from_secs(3)); stop.store(true, Ordering::Relaxed); for h in hs { h.join().unwrap(); }
        let mut l = lat.lock().unwrap().clone(); let puts = up3.stats.puts.load(Ordering::Relaxed) - puts0;
        println!("B. cloud-durable commits with a simulated 30 ms round trip, {thr:>3} writers: {:>6.0} commits/s, latency p50 {:.1} ms p99 {:.1} ms, {:.0} uploads/s (each upload carries {:.0} commits)", done.load(Ordering::Relaxed) as f64 / 3.0, pct(&mut l, 0.5), pct(&mut l, 0.99), puts as f64 / 3.0, done.load(Ordering::Relaxed) as f64 / puts.max(1) as f64);
      }
      sql.db.log.set_cloud_wait(false, None); drop(up3); up = Some(start_uploader(cfg.clone(), dir.clone(), sql.db.log.clone())); }
    // ---- C. point-in-time restore from the bucket ----
    let good = count(&sql); let good_seq = sql.current_seq(); let mut s = Session::default();
    let bad = sql.exec_s(&mut s, "DELETE FROM t WHERE country < 10", &[]).unwrap(); for id in 30_000_000..30_010_000i64 { sql.exec_s(&mut s, "INSERT INTO t VALUES (?,?,?,?)", &[Value::Int(id), Value::Float(1.0), Value::Int(1), Value::Int(0)]).unwrap(); }
    let seq = sql.current_seq(); while upr(&up).cloud_seq.load(Ordering::Relaxed) < seq { std::thread::sleep(Duration::from_millis(20)); }
    let (objs, bytes) = bucket_objects("sqweezdb");
    let rdir = PathBuf::from(format!("{DIR}/restored")); let t = Instant::now(); let (s0, chunks, dl) = download(&cfg, &rdir).unwrap(); let dlt = t.elapsed();
    let t = Instant::now(); let (r, rs) = Sql::restore(&rdir, &PathBuf::from(format!("{DIR}/restored2")), good_seq, 12).unwrap(); let rt = t.elapsed();
    println!("C. bucket holds {objs} objects, {:.0} MB. Bad DELETE of {:?} rows then 10k inserts. Restore to the moment before it: download {chunks} chunks + snapshot (s0 {s0}) {:.0} MB in {:.2} s, replay to seq {good_seq} ({} records) {:.2} s -> {} rows vs {good} then: {}", bytes as f64 / 1e6, bad[0][0], dl as f64 / 1e6, dlt.as_secs_f64(), rs.replayed, rt.as_secs_f64(), count(&r), if count(&r) == good { "EXACT" } else { "MISMATCH" });
    drop(r);
    // ---- D. a follower fed only by the bucket ----
    let fdir = PathBuf::from(format!("{DIR}/follower")); download(&cfg, &fdir).unwrap();
    let (fol, fh) = Sql::follow(&fdir, Duration::from_millis(5)).unwrap(); let puller = start_puller(cfg.clone(), fdir.clone(), Duration::from_millis(20));
    let seq = sql.current_seq(); let t = Instant::now(); while fh.applied() < seq { std::thread::sleep(Duration::from_millis(10)); if t.elapsed() > Duration::from_secs(30) { break; } }
    println!("D. follower built from the bucket: {} rows (primary {}), caught up in {:.2} s", count(&fol), count(&sql), t.elapsed().as_secs_f64());
    for (interval_ms, label) in [(1000u64, "1 s cadence"), (100, "100 ms cadence")] {
        up = None; let mut c2 = cfg.clone(); c2.interval = Duration::from_millis(interval_ms); let up2 = start_uploader(c2, dir.clone(), sql.db.log.clone());
        let mut lags = vec![]; let get = fol.prepare("SELECT id FROM t WHERE id = ?").unwrap(); let mut fs_ = Session::default(); let puts0 = up2.stats.puts.load(Ordering::Relaxed); let t0 = Instant::now();
        for k in 0..20i64 { let id = 40_000_000 + interval_ms as i64 * 1000 + k; let t = Instant::now(); sql.exec_s(&mut s, "INSERT INTO t VALUES (?,?,?,?)", &[Value::Int(id), Value::Float(1.0), Value::Int(1), Value::Int(0)]).unwrap(); loop { if !fol.execute_s(&mut fs_, &get, &[Value::Int(id)]).unwrap().is_empty() { break; } std::thread::sleep(Duration::from_millis(1)); if t.elapsed() > Duration::from_secs(30) { break; } } lags.push(t.elapsed().as_secs_f64() * 1e3); std::thread::sleep(Duration::from_millis(50)); }
        let el = t0.elapsed().as_secs_f64(); let puts = up2.stats.puts.load(Ordering::Relaxed) - puts0;
        println!("D. commit on primary -> visible on the bucket follower, {label}: p50 {:.0} ms, p99 {:.0} ms ({:.1} uploads/s while writing)", pct(&mut lags, 0.5), pct(&mut lags, 0.99), puts as f64 / el);
        drop(get); up = Some(up2);
    }
    drop(puller); drop(fh); drop(fol); up = None; let _ = up; drop(sql); let _ = fs::remove_dir_all(DIR);
    // ---- E. crash: how much is lost? ours vs litestream, 1 s cadence both ----
    for (i, (mode, label)) in [("plain", "1 s upload cadence"), ("plain", "1 s upload cadence"), ("plain", "1 s upload cadence"), ("fast", "100 ms cadence"), ("durable", "cloud-durable commits")].into_iter().enumerate() { let kill_after = Duration::from_millis(5000 + 370 * i as u64);
        clear_prefix(&CloudConfig::local("sqweezdb", "")); std::thread::sleep(Duration::from_millis(1500)); let _ = fs::remove_dir_all(format!("{DIR}/victim"));
        let mut child = Command::new(std::env::current_exe().unwrap()).args(["victim", mode]).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap();
        let out = child.stdout.take().unwrap(); let mut lines = BufReader::new(out).lines(); while let Some(Ok(l)) = lines.next() { if l == "ready" { break; } }
        let t = Instant::now(); let mut last_ack = 0i64; let mut n = 0u64; while t.elapsed() < kill_after { if let Some(Ok(l)) = lines.next() { if let Ok(id) = l.parse::<i64>() { last_ack = id; n += 1; } } else { break; } }
        child.kill().unwrap(); child.wait().unwrap();
        let vcfg = CloudConfig::local("sqweezdb", "victim"); let vdir = PathBuf::from(format!("{DIR}/victim-restored")); let t = Instant::now(); let (_, chunks, bytes) = download(&vcfg, &vdir).unwrap(); let (r, _) = Sql::open(&vdir, false, 12).unwrap(); let rt = t.elapsed();
        let got = max_id(&r); let rate = n as f64 / kill_after.as_secs_f64(); println!("E. ours, {label}: {:.0} acknowledged writes/s for {:.1} s, kill -9, restore from the bucket in {:.2} s ({chunks} chunks, {:.0} MB); last acknowledged id {last_ack}, last id in the bucket {got} -> {} rows lost (the last {:.0} ms of writes)", rate, kill_after.as_secs_f64(), rt.as_secs_f64(), bytes as f64 / 1e6, last_ack - got, (last_ack - got) as f64 / rate * 1e3); drop(r);
    }
    // litestream
    let _ = fs::remove_dir_all("data/nocow/e33lite"); fs::create_dir_all("data/nocow/e33lite").unwrap();
    { let c = Connection::open(SQDB).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL, country INTEGER, ts INTEGER);").unwrap(); let tx = c.unchecked_transaction().unwrap(); { let mut st = tx.prepare("INSERT INTO t VALUES (?,?,?,?)").unwrap(); for id in 1..=N { st.execute(params![id, (id % 1000) as f64, id % 20, 0i64]).unwrap(); } } tx.commit().unwrap(); }
    fs::write("data/nocow/e33lite/litestream.yml", "dbs:\n  - path: data/nocow/e33lite/t.db\n    replicas:\n      - type: s3\n        bucket: litestream\n        path: t\n        endpoint: http://127.0.0.1:9000\n        region: us-east-1\n        access-key-id: minioadmin\n        secret-access-key: minioadmin\n        force-path-style: true\n        sync-interval: 1s\n").unwrap();
    let mut ls = Command::new("tools/litestream").args(["replicate", "-config", "data/nocow/e33lite/litestream.yml"]).stdout(Stdio::null()).stderr(Stdio::piped()).spawn().unwrap();
    let t = Instant::now(); loop { let (n, _) = bucket_objects("litestream"); if n > 0 { break; } if t.elapsed() > Duration::from_secs(120) { println!("E. litestream: no objects after 120 s"); break; } std::thread::sleep(Duration::from_millis(200)); }
    let (n0, b0) = bucket_objects("litestream"); println!("E. litestream: initial snapshot in the bucket after {:.1} s ({n0} objects, {:.0} MB)", t.elapsed().as_secs_f64(), b0 as f64 / 1e6);
    for trial in 0..3 { let kill_after = Duration::from_millis(5000 + 370 * trial as u64);
        if trial > 0 { ls = Command::new("tools/litestream").args(["replicate", "-config", "data/nocow/e33lite/litestream.yml"]).stdout(Stdio::null()).stderr(Stdio::piped()).spawn().unwrap(); std::thread::sleep(Duration::from_secs(3)); }
        let mut child = Command::new(std::env::current_exe().unwrap()).arg("lite-victim").stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap();
        let out = child.stdout.take().unwrap(); let mut lines = BufReader::new(out).lines(); while let Some(Ok(l)) = lines.next() { if l == "ready" { break; } }
        let t = Instant::now(); let mut last_ack = 0i64; let mut n = 0u64; while t.elapsed() < kill_after { if let Some(Ok(l)) = lines.next() { if let Ok(id) = l.parse::<i64>() { last_ack = id; n += 1; } } else { break; } }
        child.kill().unwrap(); child.wait().unwrap(); ls.kill().unwrap(); ls.wait().unwrap();
        let (n1, b1) = bucket_objects("litestream"); let _ = fs::remove_file("data/nocow/e33lite/restored.db");
        let t = Instant::now(); let st = Command::new("tools/litestream").args(["restore", "-config", "data/nocow/e33lite/litestream.yml", "-o", "data/nocow/e33lite/restored.db", "data/nocow/e33lite/t.db"]).output().unwrap(); let rt = t.elapsed();
        if !st.status.success() { println!("E. litestream restore failed: {}", String::from_utf8_lossy(&st.stderr).chars().take(300).collect::<String>()); }
        let got: i64 = Connection::open("data/nocow/e33lite/restored.db").ok().and_then(|c| c.query_row("SELECT max(id) FROM t", [], |r| r.get(0)).ok()).unwrap_or(-1); let rate = n as f64 / kill_after.as_secs_f64();
        println!("E. litestream, 1 s sync interval: {:.0} acknowledged writes/s for {:.1} s, kill -9 writer and litestream, restore {:.2} s ({} objects, {:.0} MB in the bucket); last acknowledged id {last_ack}, last id restored {got} -> {} rows lost (the last {:.0} ms of writes)", rate, kill_after.as_secs_f64(), rt.as_secs_f64(), n1, b1 as f64 / 1e6, last_ack - got, (last_ack - got) as f64 / rate * 1e3);
    }
    let _ = fs::remove_dir_all(DIR); let _ = fs::remove_dir_all("data/nocow/e33lite");
}
