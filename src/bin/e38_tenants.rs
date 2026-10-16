//! Experiment 38: database per tenant at scale. 10,000 tenants with 10 rows each, in one process: creation, memory,
//! threads, random access across all tenants, eviction and reopen under a residency limit, fleet schema migration with
//! writers running, per-tenant backup to a bucket prefix with a per-tenant key. SQLite and Turso: 10,000 files each.
use sqweezdb::cloud::{clear_prefix, download, CloudConfig};
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::Value;
use sqweezdb::tenants::TenantHost;
use sqweezdb::util::Rng;
use rusqlite::{params, Connection};
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const NT: usize = 10_000; const ROWS: i64 = 10;
const ROOT: &str = "data/nocow/e38"; 
fn rss_mb() -> f64 { let s = fs::read_to_string("/proc/self/status").unwrap(); s.lines().find(|l| l.starts_with("VmRSS:")).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0) / 1024.0 }
fn threads() -> usize { fs::read_dir("/proc/self/task").map(|r| r.count()).unwrap_or(0) }
unsafe extern "C" { fn malloc_trim(pad: usize) -> i32; }
const INIT: &[&str] = &["CREATE TABLE notes(id INTEGER PRIMARY KEY, body VARCHAR(200), tag INT)"];

fn ours() {
    let root = PathBuf::from(format!("{ROOT}/ours")); let _ = fs::remove_dir_all(&root);
    let host = Arc::new(TenantHost::new(&root, NT + 1, Duration::from_millis(300), false));
    let (r0, t0) = (rss_mb(), Instant::now());
    for i in 0..NT { let s = host.get(&format!("t{i:05}"), INIT).unwrap(); let mut se = Session::default(); let ins = s.prepare("INSERT INTO notes VALUES (?,?,?)").unwrap(); for r in 1..=ROWS { s.execute_s(&mut se, &ins, &[Value::Int(r), Value::Text(format!("note {r} of tenant {i}")), Value::Int(r % 3)]).unwrap(); } }
    let el = t0.elapsed(); let r1 = rss_mb(); let th1 = threads();
    std::thread::sleep(Duration::from_secs(3)); let (r2, th2) = (rss_mb(), threads()); unsafe { malloc_trim(0); } let r3 = rss_mb();
    println!("ours    create {NT} tenants x {ROWS} rows: {:.2} s ({:.0} us each); right after: {:.0} MB ({:.0} KB/tenant) with {th1} log threads still winding down; 3 s later: {:.0} MB ({:.0} KB/tenant), {th2} thread; after malloc_trim: {:.0} MB", el.as_secs_f64(), el.as_secs_f64() * 1e6 / NT as f64, r1 - r0, (r1 - r0) * 1024.0 / NT as f64, r2 - r0, (r2 - r0) * 1024.0 / NT as f64, r3 - r0);
    // random access across all tenants, 8 threads, all resident
    let work = |host: &Arc<TenantHost>, secs: f64| -> (u64, u64) { let stop = Arc::new(AtomicBool::new(false)); let reads = Arc::new(AtomicU64::new(0)); let writes = Arc::new(AtomicU64::new(0));
        let hs: Vec<_> = (0..8u64).map(|k| { let (host, stop, reads, writes) = (host.clone(), stop.clone(), reads.clone(), writes.clone()); std::thread::spawn(move || { let mut rng = Rng(11 + k); let mut se = Session::default(); let mut n = 0u64; while !stop.load(Ordering::Relaxed) { let t = (rng.next() % NT as u64) as usize; let s = host.get(&format!("t{t:05}"), INIT).unwrap(); if n % 10 == 0 { if s.exec_s(&mut se, "UPDATE notes SET tag = 9 WHERE id = ?", &[Value::Int((n % ROWS as u64) as i64 + 1)]).is_ok() { writes.fetch_add(1, Ordering::Relaxed); } } else { s.exec_s(&mut se, "SELECT body FROM notes WHERE id = ?", &[Value::Int((n % ROWS as u64) as i64 + 1)]).unwrap(); reads.fetch_add(1, Ordering::Relaxed); } n += 1; } }) }).collect();
        std::thread::sleep(Duration::from_secs_f64(secs)); stop.store(true, Ordering::Relaxed); for h in hs { h.join().unwrap(); } (reads.load(Ordering::Relaxed), writes.load(Ordering::Relaxed)) };
    let (r, w) = work(&host, 3.0); println!("ours    random tenant access, 8 threads, all {NT} resident: {:.0} reads/s + {:.0} writes/s; {:.0} MB, {} threads", r as f64 / 3.0, w as f64 / 3.0, rss_mb() - r0, threads());
    // residency limit: only 1,000 may stay open; the rest are evicted (snapshot + drop) and reopened on demand
    drop(host); std::thread::sleep(Duration::from_millis(500));
    let host = Arc::new(TenantHost::new(&root, 1_000, Duration::from_millis(300), false));
    let (r, w) = work(&host, 3.0); let ev = host.evictions.load(Ordering::Relaxed); let op = host.opens.load(Ordering::Relaxed);
    println!("ours    same access with at most 1,000 tenants resident (evicted tenants are dropped and reopened from disk): {:.0} reads/s + {:.0} writes/s; {op} reopens ({:.0} us each), {ev} evictions; process {:.0} MB", r as f64 / 3.0, w as f64 / 3.0, host.open_us.load(Ordering::Relaxed) as f64 / op.max(1) as f64, rss_mb());
    // fleet migration on 1,000 tenants while writers run on them
    drop(host); let host = Arc::new(TenantHost::new(&root, NT + 1, Duration::from_secs(5), false));
    let stop = Arc::new(AtomicBool::new(false)); let worst = Arc::new(AtomicU64::new(0)); let wn = Arc::new(AtomicU64::new(0));
    let hs: Vec<_> = (0..4u64).map(|k| { let (host, stop, worst, wn) = (host.clone(), stop.clone(), worst.clone(), wn.clone()); std::thread::spawn(move || { let mut rng = Rng(77 + k); let mut se = Session::default(); let mut id = 1000 + k as i64 * 1_000_000; while !stop.load(Ordering::Relaxed) { let t = (rng.next() % 1000) as usize; let s = host.get(&format!("t{t:05}"), INIT).unwrap(); let t0 = Instant::now(); s.exec_s(&mut se, "INSERT INTO notes(id, body, tag) VALUES (?,?,?)", &[Value::Int(id), Value::Text("x".into()), Value::Int(1)]).unwrap(); let us = t0.elapsed().as_micros() as u64; worst.fetch_max(us, Ordering::Relaxed); wn.fetch_add(1, Ordering::Relaxed); id += 1; std::thread::sleep(Duration::from_micros(100)); } }) }).collect();
    std::thread::sleep(Duration::from_millis(300));
    let tenants_1k: Vec<String> = (0..1000).map(|i| format!("t{i:05}")).collect(); let t = Instant::now(); let mut bad = 0;
    for name in &tenants_1k { let s = host.get(name, INIT).unwrap(); let mut se = Session::default(); if s.exec_s(&mut se, "ALTER TABLE notes ADD COLUMN flag BOOL NOT NULL DEFAULT FALSE", &[]).is_err() { bad += 1; } }
    for name in &tenants_1k { let s = host.get(name, INIT).unwrap(); if s.wait_alters().is_err() { bad += 1; } } let mig = t.elapsed();
    stop.store(true, Ordering::Relaxed); for h in hs { h.join().unwrap(); }
    let s = host.get("t00007", INIT).unwrap(); let mut se = Session::default(); let chk = s.exec_s(&mut se, "SELECT flag FROM notes WHERE id = 1", &[]).unwrap();
    println!("ours    fleet migration ADD COLUMN on 1,000 tenants with 4 writer threads running on them: {:.2} s ({:.1} ms per tenant), {bad} failures, writers' worst stall {:.1} ms over {} inserts; tenant 7 reads flag = {:?}", mig.as_secs_f64(), mig.as_secs_f64() * 1e3 / 1000.0, worst.load(Ordering::Relaxed) as f64 / 1e3, wn.load(Ordering::Relaxed), chk[0][0]);
    // per-tenant backup: 200 tenants, each under its own prefix with its own passphrase
    clear_prefix(&CloudConfig::local("sqweezdb", "tenants")); std::thread::sleep(Duration::from_millis(1500));
    let cfg = CloudConfig::local("sqweezdb", "tenants"); let t = Instant::now();
    for i in 0..200 { host.backup(&format!("t{i:05}"), cfg.clone(), Some(&format!("secret-of-tenant-{i}"))).unwrap(); }
    let deadline = Instant::now() + Duration::from_secs(60); let mut done = 0; while done < 200 && Instant::now() < deadline { done = (0..200).filter(|i| host.backup_done(&format!("t{i:05}"))).count(); std::thread::sleep(Duration::from_millis(50)); }
    let bt = t.elapsed(); for i in 0..200 { host.stop_backup(&format!("t{i:05}")); }
    let rdir = PathBuf::from(format!("{ROOT}/restored")); let mut c7 = cfg.clone(); c7.prefix = "tenants/t00007".into(); let c7 = c7.with_passphrase("secret-of-tenant-7"); let t = Instant::now(); let (_, objs, bytes) = download(&c7, &rdir).unwrap(); let (r, _) = Sql::open(&rdir, false, 2).unwrap(); let mut se = Session::default(); let n = r.exec_s(&mut se, "SELECT count(*) FROM notes", &[]).unwrap(); let rt = t.elapsed();
    let mut c8 = cfg.clone(); c8.prefix = "tenants/t00008".into(); let c8 = c8.with_passphrase("secret-of-tenant-7"); let wrong = download(&c8, &PathBuf::from(format!("{ROOT}/restored2"))).is_err();
    println!("ours    per-tenant backup of 200 tenants to 200 prefixes with 200 passphrases: {done} done in {:.2} s; restore tenant 7 alone with its passphrase: {objs} objects, {:.1} KB, {:.1} ms -> {:?} rows; tenant 8 with tenant 7's passphrase refused: {wrong}", bt.as_secs_f64(), bytes as f64 / 1e3, rt.as_secs_f64() * 1e3, n[0][0]);
    drop(r); drop(host); let _ = fs::remove_dir_all(ROOT);
}
fn sqlite() {
    let root = PathBuf::from(format!("{ROOT}/sqlite")); let _ = fs::remove_dir_all(&root); fs::create_dir_all(&root).unwrap();
    let (r0, t0) = (rss_mb(), Instant::now());
    for i in 0..NT { let c = Connection::open(root.join(format!("t{i:05}.db"))).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT, tag INTEGER);").unwrap(); let mut st = c.prepare("INSERT INTO notes VALUES (?,?,?)").unwrap(); for r in 1..=ROWS { st.execute(params![r, format!("note {r} of tenant {i}"), r % 3]).unwrap(); } }
    let create = t0.elapsed();
    let t = Instant::now(); let conns: Arc<Vec<std::sync::Mutex<Connection>>> = Arc::new((0..NT).map(|i| { let c = Connection::open(root.join(format!("t{i:05}.db"))).unwrap(); c.busy_timeout(Duration::from_secs(10)).unwrap(); c.query_row("SELECT body FROM notes WHERE id = 1", [], |r| r.get::<_, String>(0)).unwrap(); std::sync::Mutex::new(c) }).collect()); let open = t.elapsed();
    println!("sqlite  create {NT} files x {ROWS} rows: {:.2} s ({:.0} us each); open all {NT} connections + one read each: {:.2} s ({:.0} us each); resident {:.0} MB ({:.0} KB/connection), {} threads", create.as_secs_f64(), create.as_secs_f64() * 1e6 / NT as f64, open.as_secs_f64(), open.as_secs_f64() * 1e6 / NT as f64, rss_mb() - r0, (rss_mb() - r0) * 1024.0 / NT as f64, threads());
    // random access across all connections, 8 threads (each thread its own connections to the random files)
    let stop = Arc::new(AtomicBool::new(false)); let reads = Arc::new(AtomicU64::new(0)); let writes = Arc::new(AtomicU64::new(0));
    let hs: Vec<_> = (0..8u64).map(|k| { let (conns, stop, reads, writes) = (conns.clone(), stop.clone(), reads.clone(), writes.clone()); std::thread::spawn(move || { let mut rng = Rng(11 + k); let mut n = 0u64; while !stop.load(Ordering::Relaxed) { let t = (rng.next() % NT as u64) as usize; let c = conns[t].lock().unwrap(); if n % 10 == 0 { if c.execute("UPDATE notes SET tag = 9 WHERE id = ?", params![(n % ROWS as u64) as i64 + 1]).is_ok() { writes.fetch_add(1, Ordering::Relaxed); } } else { c.query_row("SELECT body FROM notes WHERE id = ?", params![(n % ROWS as u64) as i64 + 1], |r| r.get::<_, String>(0)).unwrap(); reads.fetch_add(1, Ordering::Relaxed); } n += 1; } }) }).collect();
    std::thread::sleep(Duration::from_secs(3)); stop.store(true, Ordering::Relaxed); for h in hs { h.join().unwrap(); }
    println!("sqlite  random tenant access, 8 threads, all {NT} connections open: {:.0} reads/s + {:.0} writes/s; {:.0} MB", reads.load(Ordering::Relaxed) as f64 / 3.0, writes.load(Ordering::Relaxed) as f64 / 3.0, rss_mb() - r0);
    // fleet migration: ALTER TABLE ADD COLUMN on 1,000 files (SQLite does this as metadata), serially
    let t = Instant::now(); for c in conns.iter().take(1000) { c.lock().unwrap().execute_batch("ALTER TABLE notes ADD COLUMN flag INTEGER NOT NULL DEFAULT 0").unwrap(); } let mig = t.elapsed();
    let t = Instant::now(); for c in conns.iter().take(1000) { c.lock().unwrap().execute_batch("BEGIN; CREATE TABLE n2(id INTEGER PRIMARY KEY, body TEXT, tag INTEGER NOT NULL, flag INTEGER NOT NULL DEFAULT 0); INSERT INTO n2 SELECT id, body, tag, flag FROM notes; DROP TABLE notes; ALTER TABLE n2 RENAME TO notes; COMMIT;").unwrap(); } let mig2 = t.elapsed();
    println!("sqlite  fleet migration on 1,000 files: ADD COLUMN {:.2} s ({:.1} ms each); SET NOT NULL via the copy-the-table recipe {:.2} s ({:.1} ms each); each file is locked for writes during its own step; no backup/encryption in the engine", mig.as_secs_f64(), mig.as_secs_f64() * 1e3 / 1000.0, mig2.as_secs_f64(), mig2.as_secs_f64() * 1e3 / 1000.0);
    drop(conns); let _ = fs::remove_dir_all(&root);
}
fn turso() {
    use turso::{Builder, Value as TV};
    let root = PathBuf::from(format!("{ROOT}/turso")); let _ = fs::remove_dir_all(&root); fs::create_dir_all(&root).unwrap();
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    let (r0, t0) = (rss_mb(), Instant::now());
    rt.block_on(async { for i in 0..NT { let db = Builder::new_local(root.join(format!("t{i:05}.db")).to_str().unwrap()).build().await.unwrap(); let c = db.connect().unwrap(); c.execute("PRAGMA synchronous = OFF", ()).await.ok(); c.execute_batch("CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT, tag INTEGER);").await.unwrap(); for r in 1..=ROWS { c.execute("INSERT INTO notes VALUES (?,?,?)", vec![TV::Integer(r), TV::Text(format!("note {r} of tenant {i}")), TV::Integer(r % 3)]).await.unwrap(); } } });
    let create = t0.elapsed();
    let t = Instant::now(); let dbs: Vec<(turso::Database, turso::Connection)> = rt.block_on(async { let mut v = vec![]; for i in 0..NT { let db = Builder::new_local(root.join(format!("t{i:05}.db")).to_str().unwrap()).build().await.unwrap(); let c = db.connect().unwrap(); let mut rows = c.query("SELECT body FROM notes WHERE id = 1", ()).await.unwrap(); let _ = rows.next().await.unwrap(); v.push((db, c)); } v }); let open = t.elapsed();
    println!("turso   create {NT} files x {ROWS} rows: {:.2} s ({:.0} us each); open all {NT} + one read each: {:.2} s ({:.0} us each); resident {:.0} MB ({:.0} KB/database), {} threads", create.as_secs_f64(), create.as_secs_f64() * 1e6 / NT as f64, open.as_secs_f64(), open.as_secs_f64() * 1e6 / NT as f64, rss_mb() - r0, (rss_mb() - r0) * 1024.0 / NT as f64, threads());
    let t = Instant::now(); let mut n = 0u64; let mut rng = Rng(11); rt.block_on(async { while t.elapsed() < Duration::from_secs(3) { let i = (rng.next() % NT as u64) as usize; let c = &dbs[i].1; if n % 10 == 0 { c.execute("UPDATE notes SET tag = 9 WHERE id = ?", vec![TV::Integer((n % ROWS as u64) as i64 + 1)]).await.unwrap(); } else { let mut rows = c.query("SELECT body FROM notes WHERE id = ?", vec![TV::Integer((n % ROWS as u64) as i64 + 1)]).await.unwrap(); let _ = rows.next().await.unwrap(); } n += 1; } });
    println!("turso   random tenant access, 1 thread (connections are not Send): {:.0} ops/s", n as f64 / 3.0);
    drop(dbs); let _ = fs::remove_dir_all(&root);
}
fn main() {
    match std::env::args().nth(1).as_deref() { Some("ours") => return ours(), Some("sqlite") => return sqlite(), Some("turso") => return turso(), _ => {} }
    println!("=== E38: database per tenant, 10,000 tenants x 10 rows, each engine in its own process ===");
    for e in ["ours", "sqlite", "turso"] { let o = Command::new(std::env::current_exe().unwrap()).arg(e).stderr(Stdio::inherit()).output().unwrap(); print!("{}", String::from_utf8_lossy(&o.stdout)); if !o.status.success() { println!("{e:<7} exited with {:?}", o.status.code()); } }
    let _ = fs::remove_dir_all(ROOT);
}
