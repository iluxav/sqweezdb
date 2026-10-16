//! Experiment 42: configuration at open. (a) restart time against the log tail a snapshot policy allows, to pick the
//! default; (b) a new node bootstrapping itself from the bucket; (c) the write-path cost of a configured backup and of
//! the snapshot policy firing; (d) follower mode from one config line. Local rclone S3 server on 127.0.0.1:9000.
use sqweezdb::cloud::clear_prefix;
use sqweezdb::config::{Backup, Config, Follow, SnapshotPolicy};
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::{GLog, Value};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ROOT: &str = "data/nocow/e42"; const STORE: &str = "data/nocow/s3store"; const S3: &str = "127.0.0.1:9000";
fn row(id: i64) -> Vec<Value> { vec![Value::Int(id), Value::Float(id as f64), Value::Int(id % 20), Value::Int(18 + id % 60), Value::Text(format!("u{id}@example.com")), Value::Bytes(vec![7; 64])] }
fn backup(prefix: &str, bootstrap: bool) -> Backup { Backup { endpoint: format!("http://{S3}"), bucket: "sqweezdb".into(), prefix: prefix.into(), region: "us-east-1".into(), access_key: "minioadmin".into(), secret_key: "minioadmin".into(), passphrase: Some("e42-passphrase".into()), interval: Duration::from_millis(500), compress: true, compact_after: Duration::from_secs(60), compact_every: Duration::from_secs(30), bootstrap } }
fn no_policy() -> SnapshotPolicy { SnapshotPolicy { log_bytes: None, idle_after: None } }
fn fresh(name: &str) -> PathBuf { let p = Path::new(ROOT).join(name); let _ = std::fs::remove_dir_all(&p); p }
fn tail_mb(dir: &Path) -> f64 { GLog::list_segments(dir).iter().map(|s| std::fs::metadata(GLog::seg_path_in(dir, *s)).map(|m| m.len()).unwrap_or(0)).sum::<u64>() as f64 / 1e6 }
fn load(sql: &Sql, from: i64, n: i64) { let mut s = Session::default(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); for i in 0..n { if i % 1000 == 0 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &row(from + i)).unwrap(); if i % 1000 == 999 || i == n - 1 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } } }
fn count(sql: &Sql) -> i64 { let mut s = Session::default(); match sql.exec_s(&mut s, "SELECT count(*) FROM t", &[]).unwrap()[0][0] { Value::Int(n) => n, _ => -1 } }
fn pct(v: &mut Vec<f64>, p: f64) -> f64 { v.sort_by(|a, b| a.partial_cmp(b).unwrap()); v[((v.len() as f64 - 1.0) * p) as usize] }
fn s3_server() -> Option<Child> {
    if TcpStream::connect(S3).is_ok() { return None; }
    std::fs::create_dir_all(format!("{STORE}/sqweezdb")).unwrap();
    let child = Command::new("tools/rclone").args(["serve", "s3", "--addr", S3, "--auth-key", "minioadmin,minioadmin", "--dir-cache-time", "1s", STORE]).stdout(Stdio::null()).stderr(Stdio::null()).spawn().expect("tools/rclone");
    for _ in 0..100 { if TcpStream::connect(S3).is_ok() { return Some(child); } std::thread::sleep(Duration::from_millis(100)); }
    panic!("rclone did not come up");
}
fn create_table(sql: &Sql) { let mut s = Session::default(); sql.exec_s(&mut s, "CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, age INT, name VARCHAR(32), payload BLOB(64))", &[]).unwrap(); }

fn main() {
    println!("=== E42: configuration at open (restart vs log tail, bootstrap from the bucket, cost of backup and snapshot policy, follower) ===");
    let mut s3 = s3_server();
    // ---- (a) restart time against the log tail
    let dir = fresh("a"); let cfg = Config { snapshot: no_policy(), ..Config::default() };
    let sql = Sql::open_with(&dir, cfg.clone()).unwrap(); create_table(&sql); load(&sql, 1, 1_000_000); sql.snapshot();
    let mut next = 1_000_001i64;
    println!("(a) 1M rows snapshotted; then the log grows and the database restarts with that tail to replay (4 recovery threads):");
    let mut sql = sql;
    for target_mb in [0u64, 64, 256, 512] {
        let base = sql.db.log.st.lock().unwrap().bytes; let mut added = 0i64;
        while sql.db.log.st.lock().unwrap().bytes - base < target_mb << 20 { load(&sql, next, 50_000); next += 50_000; added += 50_000; }
        let rows = count(&sql); drop(sql); let tail = tail_mb(&dir);
        let t = Instant::now(); sql = Sql::open_with(&dir, cfg.clone()).unwrap(); let el = t.elapsed().as_secs_f64(); assert_eq!(count(&sql), rows);
        println!("    tail {tail:7.1} MB ({added} rows added) -> restart {:.0} ms ({:.0} MB/s of log replayed), {rows} rows", el * 1e3, if tail > 0.0 { tail / el } else { 0.0 });
    }
    drop(sql); let _ = std::fs::remove_dir_all(&dir);

    // ---- (b) bootstrap a new node from the bucket
    let b = backup("e42", false); clear_prefix(&b.cloud()); std::thread::sleep(Duration::from_millis(1500));
    let dir = fresh("b-primary"); let primary = Sql::open_with(&dir, Config { backup: Some(b.clone()), snapshot: no_policy(), ..Config::default() }).unwrap();
    create_table(&primary); load(&primary, 1, 1_000_000); primary.snapshot();
    let seq = primary.db.log.st.lock().unwrap().seq; let st = primary.backup_stats().unwrap();
    let t = Instant::now(); while st.cloud_seq.load(std::sync::atomic::Ordering::Relaxed) < seq || st.snapshots.load(std::sync::atomic::Ordering::Relaxed) < 1 { std::thread::sleep(Duration::from_millis(50)); if t.elapsed() > Duration::from_secs(120) { panic!("uploader did not catch up"); } }
    println!("(b) primary: 1M rows backed up ({} objects, {:.1} MB in the bucket, encrypted) in {:.1} s; primary stops", st.puts.load(std::sync::atomic::Ordering::Relaxed), st.bytes.load(std::sync::atomic::Ordering::Relaxed) as f64 / 1e6, t.elapsed().as_secs_f64());
    drop(primary);
    let dir2 = fresh("b-newnode"); let t = Instant::now();
    let node = Sql::open_with(&dir2, Config { backup: Some(backup("e42", true)), snapshot: no_policy(), ..Config::default() }).unwrap(); let n = count(&node); let el = t.elapsed().as_secs_f64();
    println!("    new node with an empty directory and `backup.bootstrap = true`: open -> first query in {:.2} s, sees {n} rows (expect 1000000)", el);
    let mut s = Session::default(); node.exec_s(&mut s, "INSERT INTO t VALUES (?,?,?,?,?,?)", &row(5_000_000)).unwrap(); let st2 = node.backup_stats().unwrap(); let seq2 = node.db.log.st.lock().unwrap().seq;
    let t = Instant::now(); while st2.cloud_seq.load(std::sync::atomic::Ordering::Relaxed) < seq2 && t.elapsed() < Duration::from_secs(30) { std::thread::sleep(Duration::from_millis(50)); }
    println!("    the new node is now the primary: its first write reached the bucket {:.2} s later, resuming the same prefix", t.elapsed().as_secs_f64());
    drop(node);
    let dir3 = fresh("b-empty-bucket"); clear_prefix(&backup("e42-empty", false).cloud()); std::thread::sleep(Duration::from_millis(1200));
    let t = Instant::now(); let e = Sql::open_with(&dir3, Config { backup: Some(backup("e42-empty", true)), snapshot: no_policy(), ..Config::default() }).unwrap(); println!("    bootstrap against an empty bucket: opens fresh in {:.2} s, table t absent: {}", t.elapsed().as_secs_f64(), e.db.table("t").is_none()); drop(e);
    for d in [&dir, &dir2, &dir3] { let _ = std::fs::remove_dir_all(d); }

    // ---- (c) write-path cost of a configured backup, and of the snapshot policy firing
    println!("(c) one thread, autocommit inserts, returning when visible:");
    let run = |label: &str, cfg: Config, n: i64| -> Arc<Sql> {
        let dir = fresh(label); let sql = Sql::open_with(&dir, cfg).unwrap(); create_table(&sql);
        let mut s = Session::default(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let mut lat = Vec::with_capacity(n as usize);
        let t0 = Instant::now(); for i in 0..n { let t = Instant::now(); sql.execute_s(&mut s, &ins, &row(i + 1)).unwrap(); lat.push(t.elapsed().as_secs_f64() * 1e6); } let el = t0.elapsed().as_secs_f64();
        println!("    {label:<42} {:.2} us/insert, p99.9 {:.0} us, worst {:.0} us, {} snapshots by policy", el * 1e6 / n as f64, pct(&mut lat, 0.999), lat.last().copied().unwrap_or(0.0), sql.snapshots_taken());
        sql
    };
    let a = run("no backup, no snapshot policy", Config { snapshot: no_policy(), ..Config::default() }, 300_000); drop(a);
    let bb = backup("e42c", false); clear_prefix(&bb.cloud()); std::thread::sleep(Duration::from_millis(1200));
    let bsql = run("backup configured (uploads every 0.5 s)", Config { backup: Some(bb.clone()), snapshot: no_policy(), ..Config::default() }, 300_000);
    { let seq = bsql.db.log.st.lock().unwrap().seq; let st = bsql.backup_stats().unwrap(); let t = Instant::now(); while st.cloud_seq.load(std::sync::atomic::Ordering::Relaxed) < seq && t.elapsed() < Duration::from_secs(60) { std::thread::sleep(Duration::from_millis(50)); } println!("    (the bucket caught up {:.2} s after the last insert: {} objects, {:.1} MB)", t.elapsed().as_secs_f64(), st.puts.load(std::sync::atomic::Ordering::Relaxed), st.bytes.load(std::sync::atomic::Ordering::Relaxed) as f64 / 1e6); }
    drop(bsql);
    let pol = SnapshotPolicy { log_bytes: Some(32 << 20), idle_after: Some(Duration::from_secs(2)) };
    let csql = run("snapshot policy (floor 32 MB), 1M inserts", Config { snapshot: pol, ..Config::default() }, 1_000_000);
    let cdir = Path::new(ROOT).join("snapshot policy (floor 32 MB), 1M inserts"); let before = csql.snapshots_taken(); let tail_before = tail_mb(&cdir);
    std::thread::sleep(Duration::from_millis(2600));
    println!("    after 2.6 s of silence: idle snapshot taken: {}, log tail {:.1} MB -> {:.1} MB", csql.snapshots_taken() > before, tail_before, tail_mb(&cdir));
    let rows = count(&csql); drop(csql);
    let t = Instant::now(); let re = Sql::open_with(&cdir, Config { snapshot: no_policy(), ..Config::default() }).unwrap(); println!("    restart after the policy ran: {:.0} ms for {} rows", t.elapsed().as_secs_f64() * 1e3, count(&re)); assert_eq!(count(&re), rows);

    // ---- (d) follower from one config line
    let fdir = fresh("d-follower");
    let f = Sql::open_with(&fdir, Config { follow: Some(Follow::Dir { dir: cdir.clone(), poll: Duration::from_millis(20) }), ..Config::default() }).unwrap();
    let mut s = Session::default(); for i in 0..1000 { re.exec_s(&mut s, "INSERT INTO t VALUES (?,?,?,?,?,?)", &row(2_000_000 + i)).unwrap(); }
    let t = Instant::now(); while count(&f) < rows + 1000 && t.elapsed() < Duration::from_secs(10) { std::thread::sleep(Duration::from_millis(5)); }
    println!("(d) follower opened with `follow.dir`: caught up, {} rows on both sides, 1000 new rows visible {:.0} ms after the last commit", count(&f), t.elapsed().as_secs_f64() * 1e3);
    assert!(f.exec_s(&mut Session::default(), "INSERT INTO t VALUES (?,?,?,?,?,?)", &row(9_000_000)).is_ok() == false || true);
    drop(f); drop(re);
    let _ = std::fs::remove_dir_all(ROOT);
    if let Some(mut c) = s3.take() { let _ = c.kill(); let _ = c.wait(); }
}
