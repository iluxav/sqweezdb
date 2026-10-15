//! Experiment 26: live backup, point-in-time restore, and a follower replica, all from the log we already trust.
//! SQLite's side: online backup API (one step) and VACUUM INTO while a writer runs; it has no point-in-time restore
//! and no follower without external tools (Litestream, LiteFS), so those rows compare against "not available".
use dblab::sql::{Session, Sql};
use dblab::table::Value;
use dblab::util::Rng;
use rusqlite::{backup::Backup, params, Connection};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const N: i64 = 2_000_000;
const DB: &str = "data/nocow/e26.db"; const DIR: &str = "data/nocow/e26dir";
const CREATE: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, age INT, name VARCHAR(32) UNIQUE, payload BLOB(64))";
const CREATE_SQLITE: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL NOT NULL, country INTEGER NOT NULL, age INTEGER NOT NULL, name TEXT NOT NULL, payload BLOB NOT NULL); CREATE UNIQUE INDEX t_name ON t(name);";
fn row(id: i64, rng: &mut Rng) -> Vec<Value> { vec![Value::Int(id), Value::Float((rng.next() % 100000) as f64 / 100.0), Value::Int(id % 20), Value::Int(18 + id % 60), Value::Text(format!("u{id}@example.com")), Value::Bytes(vec![7; 64])] }
fn mb(p: &str) -> f64 { fs::metadata(p).map(|m| m.len() as f64 / 1e6).unwrap_or(0.0) }
fn dir_mb(p: &Path) -> f64 { fs::read_dir(p).map(|rd| rd.flatten().map(|e| e.metadata().map(|m| m.len()).unwrap_or(0)).sum::<u64>() as f64 / 1e6).unwrap_or(0.0) }
fn count(sql: &Sql) -> i64 { let mut s = Session::default(); match sql.exec_s(&mut s, "SELECT count(*) FROM t", &[]).unwrap()[0][0] { Value::Int(i) => i, _ => -1 } }
fn sqlite_build(path: &str) -> Connection {
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{path}{suf}")).ok(); }
    let c = Connection::open(path).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap(); c.execute_batch(CREATE_SQLITE).unwrap();
    let mut rng = Rng(7); let tx = c.unchecked_transaction().unwrap(); { let mut st = tx.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); for id in 1..=N { let r = row(id, &mut rng); st.execute(params![id, match r[1] { Value::Float(f) => f, _ => 0.0 }, id % 20, 18 + id % 60, format!("u{id}@example.com"), &[7u8; 64][..]]).unwrap(); } } tx.commit().unwrap();
    c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap(); c
}
fn ours_build(dir: &Path) -> Sql { let sql = Sql::create(dir, false); let mut s = Session::default(); sql.exec_s(&mut s, CREATE, &[]).unwrap(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let mut rng = Rng(7);
    for id in 1..=N { if id % 1000 == 1 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &row(id, &mut rng)).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } } drop(ins); sql }
fn ours_writer(sql: Arc<Sql>, stop: Arc<AtomicBool>, start_id: i64) -> std::thread::JoinHandle<(u64, Duration)> { std::thread::spawn(move || { let mut s = Session::default(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let mut rng = Rng(99); let (mut n, mut worst) = (0u64, Duration::ZERO); let mut id = start_id; while !stop.load(Ordering::Relaxed) { let t = Instant::now(); sql.execute_s(&mut s, &ins, &row(id, &mut rng)).unwrap(); worst = worst.max(t.elapsed()); n += 1; id += 1; } (n, worst) }) }
fn sqlite_writer(path: &'static str, stop: Arc<AtomicBool>, start_id: i64) -> std::thread::JoinHandle<(u64, Duration, u64)> { std::thread::spawn(move || { let c = Connection::open(path).unwrap(); c.busy_timeout(Duration::from_secs(600)).unwrap(); c.execute_batch("PRAGMA synchronous=NORMAL;").unwrap(); let mut st = c.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let mut rng = Rng(99); let (mut n, mut worst, mut busy) = (0u64, Duration::ZERO, 0u64); let mut id = start_id; while !stop.load(Ordering::Relaxed) { let r = row(id, &mut rng); let t = Instant::now(); match st.execute(params![id, match r[1] { Value::Float(f) => f, _ => 0.0 }, id % 20, 18 + id % 60, format!("u{id}@example.com"), &[7u8; 64][..]]) { Ok(_) => { n += 1; id += 1; } Err(_) => busy += 1 } worst = worst.max(t.elapsed()); } (n, worst, busy) }) }
fn pct(v: &mut Vec<f64>, p: f64) -> f64 { v.sort_by(|a, b| a.partial_cmp(b).unwrap()); v[((v.len() as f64 - 1.0) * p) as usize] }

fn main() {
    println!("=== E26: live backup, point-in-time restore, follower replica (2M rows) ===");
    let dir = PathBuf::from(format!("{DIR}/primary")); let sql = Arc::new(ours_build(&dir)); let st = sql.snapshot(); let c = sqlite_build(DB);
    println!("start: ours snapshot {:.0} MB; sqlite {:.0} MB", st.bytes as f64 / 1e6, mb(DB));

    // ---- backup while a writer runs ----
    let stop = Arc::new(AtomicBool::new(false)); let w = sqlite_writer(DB, stop.clone(), 10_000_000); std::thread::sleep(Duration::from_millis(200));
    let t = Instant::now(); { let mut dst = Connection::open(format!("{DB}.bak1")).unwrap(); let b = Backup::new(&c, &mut dst).unwrap(); b.run_to_completion(1_000_000, Duration::from_millis(0), None).unwrap(); } let t1 = t.elapsed();
    let t = Instant::now(); c.execute(&format!("VACUUM INTO '{DB}.bak2'"), []).unwrap(); let t2 = t.elapsed();
    std::thread::sleep(Duration::from_millis(200)); stop.store(true, Ordering::Relaxed); let (wn, worst, busy) = w.join().unwrap();
    println!("sqlite backup while writing: backup API (one step) {:.2} s -> {:.0} MB; VACUUM INTO {:.2} s -> {:.0} MB | writer: {wn} inserts, worst stall {:.1} ms, busy errors {busy}", t1.as_secs_f64(), mb(&format!("{DB}.bak1")), t2.as_secs_f64(), mb(&format!("{DB}.bak2")), worst.as_secs_f64() * 1e3);
    let stop = Arc::new(AtomicBool::new(false)); let w = ours_writer(sql.clone(), stop.clone(), 10_000_000); std::thread::sleep(Duration::from_millis(200));
    let bdir = PathBuf::from(format!("{DIR}/backup")); let t = Instant::now(); let bytes = sql.backup(&bdir).unwrap(); let t1 = t.elapsed();
    std::thread::sleep(Duration::from_millis(200)); stop.store(true, Ordering::Relaxed); let (wn, worst) = w.join().unwrap();
    println!("ours backup while writing: copy snapshot + log {:.2} s -> {:.0} MB | writer: {wn} inserts, worst stall {:.3} ms", t1.as_secs_f64(), bytes as f64 / 1e6, worst.as_secs_f64() * 1e3);
    for f in [".bak1", ".bak2"] { fs::remove_file(format!("{DB}{f}")).ok(); }

    // ---- point-in-time restore: good writes, a bad DELETE, more writes; restore to just before the DELETE ----
    let mut s = Session::default(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let mut rng = Rng(5);
    for id in 20_000_000..20_050_000 { sql.execute_s(&mut s, &ins, &row(id, &mut rng)).unwrap(); }
    let good_count = count(&sql); let good_seq = sql.db.log.st.lock().unwrap().seq;
    let bad = sql.exec_s(&mut s, "DELETE FROM t WHERE country < 10", &[]).unwrap(); for id in 20_050_000..20_100_000 { sql.execute_s(&mut s, &ins, &row(id, &mut rng)).unwrap(); }
    let now_count = count(&sql); let bytes = sql.backup(&bdir).unwrap();
    let rdir = PathBuf::from(format!("{DIR}/restored")); let t = Instant::now(); let (restored, rs) = Sql::restore(&bdir, &rdir, good_seq, 12).unwrap(); let rt = t.elapsed();
    let rc = count(&restored);
    println!("point-in-time restore: {:?} rows deleted by mistake at seq {good_seq}+1; live db now {now_count} rows; backup {:.0} MB | restore to seq {good_seq}: {:.2} s (replayed {} records), restored db has {rc} rows vs {good_count} at that point -> {} | sqlite: not available without an external tool", bad[0][0], bytes as f64 / 1e6, rt.as_secs_f64(), rs.replayed, if rc == good_count { "EXACT" } else { "MISMATCH" });
    drop(restored); let _ = fs::remove_dir_all(&rdir); let _ = fs::remove_dir_all(&bdir);

    // ---- follower replica ----
    let stop = Arc::new(AtomicBool::new(false)); let w = ours_writer(sql.clone(), stop.clone(), 25_000_000); std::thread::sleep(Duration::from_secs(2)); stop.store(true, Ordering::Relaxed); let (wn0, _) = w.join().unwrap();
    println!("writer alone (no follower): {:.0} inserts/s for 2 s", wn0 as f64 / 2.0);
    let (fol, fh) = Sql::follow(&dir, Duration::from_micros(200)).unwrap();
    let t = Instant::now(); while fh.applied() < sql.db.log.st.lock().unwrap().seq { std::thread::sleep(Duration::from_millis(1)); } let catch = t.elapsed();
    println!("follower opened on the primary's directory: caught up in {:.3} s, {} rows on both sides -> {}", catch.as_secs_f64(), count(&fol), if count(&fol) == count(&sql) { "EXACT" } else { "MISMATCH" });
    // writer throughput alone vs with the follower applying
    let stop = Arc::new(AtomicBool::new(false)); let w = ours_writer(sql.clone(), stop.clone(), 30_000_000); std::thread::sleep(Duration::from_secs(2)); stop.store(true, Ordering::Relaxed); let (wn, _) = w.join().unwrap();
    let lag_rows = sql.db.log.st.lock().unwrap().seq - fh.applied(); let t = Instant::now(); while fh.applied() < sql.db.log.st.lock().unwrap().seq { std::thread::sleep(Duration::from_millis(1)); } let drain = t.elapsed();
    println!("writer with follower attached: {:.0} inserts/s for 2 s; follower was {lag_rows} records behind at the end, drained in {:.3} s", wn as f64 / 2.0, drain.as_secs_f64());
    // per-row lag: insert a marker, wait until the follower serves it
    let mut lags = vec![]; let get = fol.prepare("SELECT id FROM t WHERE id = ?").unwrap(); let mut fs_ = Session::default();
    for k in 0..500i64 { let id = 40_000_000 + k; let t = Instant::now(); sql.execute_s(&mut s, &ins, &row(id, &mut rng)).unwrap(); loop { if !fol.execute_s(&mut fs_, &get, &[Value::Int(id)]).unwrap().is_empty() { break; } std::hint::spin_loop(); } lags.push(t.elapsed().as_secs_f64() * 1e6); std::thread::sleep(Duration::from_millis(2)); }
    println!("replication lag, commit on primary -> row visible on follower: p50 {:.0} us, p99 {:.0} us, max {:.0} us | sqlite: no follower without LiteFS", pct(&mut lags, 0.5), pct(&mut lags, 0.99), pct(&mut lags, 1.0));
    // reads on the follower while the primary writes
    let stop = Arc::new(AtomicBool::new(false)); let w = ours_writer(sql.clone(), stop.clone(), 50_000_000);
    let t = Instant::now(); let mut n = 0u64; let mut r2 = Rng(11); while t.elapsed() < Duration::from_secs(1) { let id = (r2.next() % N as u64) as i64 + 1; fol.execute_s(&mut fs_, &get, &[Value::Int(id)]).unwrap(); n += 1; }
    stop.store(true, Ordering::Relaxed); let (wn, _) = w.join().unwrap();
    println!("follower serving reads while the primary writes: {:.2}M reads/s on one thread, primary {:.0} inserts/s meanwhile", n as f64 / 1e6, wn as f64);
    let _ = sql.db.log.st.lock().unwrap().seq; drop(get); drop(fh); drop(fol); drop(ins); drop(sql); drop(c);
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{DB}{suf}")).ok(); } let _ = dir_mb(&dir); let _ = fs::remove_dir_all(DIR);
}
