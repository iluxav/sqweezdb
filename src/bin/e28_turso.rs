//! Experiment 28: the same workloads against Turso (Rust rewrite of SQLite, MVCC "BEGIN CONCURRENT"), SQLite (system lib),
//! and ours. Each engine runs in its own child process so memory is measured cleanly. 1M rows, identical statements.
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::Value;
use sqweezdb::util::Rng;
use rusqlite::{params, Connection};
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const N: i64 = 1_000_000;
const DBS: &str = "data/nocow/e28s.db"; const DBT: &str = "data/nocow/e28t.db"; const DIR: &str = "data/nocow/e28dir";
const CREATE_OURS: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, age INT, name VARCHAR(32) UNIQUE, payload BLOB(64))";
const CREATE_SQL: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL NOT NULL, country INTEGER NOT NULL, age INTEGER NOT NULL, name TEXT NOT NULL, payload BLOB NOT NULL); CREATE UNIQUE INDEX t_name ON t(name);";
fn score(id: i64) -> f64 { let mut r = Rng(id as u64 * 7 + 1); (r.next() % 100000) as f64 / 100.0 }
fn rss_mb() -> f64 { let s = fs::read_to_string("/proc/self/status").unwrap(); s.lines().find(|l| l.starts_with("VmRSS:")).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0) / 1024.0 }
fn ids(seed: u64, n: usize) -> Vec<i64> { let mut r = Rng(seed); (0..n).map(|_| (r.next() % N as u64) as i64 + 1).collect() }
fn us(d: Duration, n: usize) -> f64 { d.as_secs_f64() * 1e6 / n as f64 }
fn line(engine: &str, what: &str, v: String) { println!("{engine:<7} {what:<52} {v}"); }

// ---------------- SQLite ----------------
fn run_sqlite() {
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{DBS}{suf}")).ok(); }
    let c = Connection::open(DBS).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap(); c.execute_batch(CREATE_SQL).unwrap();
    let t = Instant::now(); for chunk in (1..=N).step_by(1000) { let tx = c.unchecked_transaction().unwrap(); { let mut st = tx.prepare_cached("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); for id in chunk..chunk + 1000 { st.execute(params![id, score(id), id % 20, 18 + id % 60, format!("u{id}@example.com"), &[7u8; 64][..]]).unwrap(); } } tx.commit().unwrap(); }
    line("sqlite", "load 1M rows (1000-row transactions)", format!("{:.2} us/row", us(t.elapsed(), N as usize)));
    let mut st = c.prepare("SELECT score FROM t WHERE id = ?").unwrap(); let t = Instant::now(); let mut v = 0.0; for id in ids(5, 200_000) { v += st.query_row(params![id], |r| r.get::<_, f64>(0)).unwrap(); } line("sqlite", "point read by id, 1 thread", format!("{:.2} us", us(t.elapsed(), 200_000)));
    let mut st2 = c.prepare("SELECT score FROM t WHERE name = ?").unwrap(); let t = Instant::now(); for id in ids(6, 100_000) { v += st2.query_row(params![format!("u{id}@example.com")], |r| r.get::<_, f64>(0)).unwrap(); } line("sqlite", "lookup by name (unique index), 1 thread", format!("{:.2} us", us(t.elapsed(), 100_000)));
    drop(st); drop(st2);
    let t = Instant::now(); let hs: Vec<_> = (0..8).map(|k| std::thread::spawn(move || { let c = Connection::open(DBS).unwrap(); let mut st = c.prepare("SELECT score FROM t WHERE id = ?").unwrap(); let mut v = 0.0; for id in ids(100 + k, 200_000) { v += st.query_row(params![id], |r| r.get::<_, f64>(0)).unwrap(); } v })).collect(); for h in hs { v += h.join().unwrap(); } line("sqlite", "point reads, 8 threads", format!("{:.2}M/s", 1.6 / t.elapsed().as_secs_f64()));
    c.execute_batch("PRAGMA synchronous=OFF").unwrap(); let t = Instant::now(); for id in 5_000_000..5_020_000i64 { c.execute("INSERT INTO t VALUES (?,?,?,?,?,?)", params![id, score(id), id % 20, 18 + id % 60, format!("u{id}@example.com"), &[7u8; 64][..]]).unwrap(); } line("sqlite", "single-row inserts, synchronous=OFF (CPU cost)", format!("{:.2} us", us(t.elapsed(), 20_000)));
    c.execute_batch("PRAGMA synchronous=FULL").unwrap(); let t = Instant::now(); for id in 5_100_000..5_102_000i64 { c.execute("INSERT INTO t VALUES (?,?,?,?,?,?)", params![id, score(id), id % 20, 18 + id % 60, format!("u{id}@example.com"), &[7u8; 64][..]]).unwrap(); } line("sqlite", "single-row inserts, synchronous=FULL (durable)", format!("{:.0} us", us(t.elapsed(), 2_000)));
    c.execute_batch("PRAGMA synchronous=NORMAL").unwrap();
    for thr in [4usize, 12] { let ok = Arc::new(AtomicU64::new(0)); let busy = Arc::new(AtomicU64::new(0)); let t = Instant::now(); let hs: Vec<_> = (0..thr).map(|k| { let (ok, busy) = (ok.clone(), busy.clone()); std::thread::spawn(move || { let mut c = Connection::open(DBS).unwrap(); c.busy_timeout(Duration::from_secs(60)).unwrap(); let mut r = Rng(200 + k as u64); let end = Instant::now() + Duration::from_secs(2); while Instant::now() < end { let a = (r.next() % N as u64) as i64 + 1; let b = (r.next() % N as u64) as i64 + 1; if a == b { continue; } let res = (|| -> rusqlite::Result<()> { let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?; let sa: f64 = tx.query_row("SELECT score FROM t WHERE id = ?", params![a], |r| r.get(0))?; let sb: f64 = tx.query_row("SELECT score FROM t WHERE id = ?", params![b], |r| r.get(0))?; tx.execute("UPDATE t SET score = ? WHERE id = ?", params![sa - 1.0, a])?; tx.execute("UPDATE t SET score = ? WHERE id = ?", params![sb + 1.0, b])?; tx.commit() })(); match res { Ok(_) => { ok.fetch_add(1, Ordering::Relaxed); } Err(_) => { busy.fetch_add(1, Ordering::Relaxed); } } } }) }).collect(); for h in hs { h.join().unwrap(); } let el = t.elapsed().as_secs_f64(); line("sqlite", &format!("transfer txns (2 reads + 2 updates), {thr} threads, NORMAL"), format!("{:.0}/s, {} failed/retried", ok.load(Ordering::Relaxed) as f64 / el, busy.load(Ordering::Relaxed))); }
    let sum: f64 = c.query_row("SELECT sum(score) FROM t WHERE id <= ?", params![N], |r| r.get(0)).unwrap(); line("sqlite", "invariant: sum of scores over the 1M rows", format!("{sum:.2}"));
    let t = Instant::now(); let n: i64 = c.query_row("SELECT count(*) FROM t WHERE age > 40", [], |r| r.get(0)).unwrap(); line("sqlite", "filtered count over the table", format!("{:.1} ms ({n})", t.elapsed().as_secs_f64() * 1e3));
    let t = Instant::now(); let mut st = c.prepare("SELECT country, count(*), avg(score) FROM t GROUP BY country").unwrap(); let g = st.query_map([], |r| r.get::<_, i64>(0)).unwrap().count(); line("sqlite", "group by country", format!("{:.1} ms ({g} groups)", t.elapsed().as_secs_f64() * 1e3)); drop(st);
    line("sqlite", "memory (RSS) at the end", format!("{:.0} MB", rss_mb()));
    drop(c); let t = Instant::now(); let c = Connection::open(DBS).unwrap(); let _: f64 = c.query_row("SELECT score FROM t WHERE id = 777", [], |r| r.get(0)).unwrap(); line("sqlite", "reopen + first read", format!("{:.2} ms", t.elapsed().as_secs_f64() * 1e3));
    let _ = v;
}

// ---------------- Turso ----------------
fn rt() -> tokio::runtime::Runtime { tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap() }
fn run_turso() {
    use turso::{Builder, Value as TV};
    for suf in ["", "-wal", "-shm", "-tshm"] { fs::remove_file(format!("{DBT}{suf}")).ok(); }
    let rt0 = rt();
    let db = rt0.block_on(async { Builder::new_local(DBT).build().await.unwrap() });
    let conn = db.connect().unwrap();
    rt0.block_on(async {
        let mvcc = std::env::var("TURSO_WAL").is_err(); if mvcc { conn.execute("PRAGMA journal_mode = 'mvcc'", ()).await.ok(); } conn.execute("PRAGMA synchronous = OFF", ()).await.ok();
        line("turso", "mode", if mvcc { "journal_mode=mvcc (BEGIN CONCURRENT available)".into() } else { "journal_mode=wal (default; BEGIN CONCURRENT not used)".into() });
        conn.execute_batch(CREATE_SQL).await.unwrap();
        let t = Instant::now();
        for chunk in (1..=N).step_by(1000) { conn.execute("BEGIN", ()).await.unwrap(); let mut st = conn.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").await.unwrap(); for id in chunk..chunk + 1000 { st.execute(vec![TV::Integer(id), TV::Real(score(id)), TV::Integer(id % 20), TV::Integer(18 + id % 60), TV::Text(format!("u{id}@example.com")), TV::Blob(vec![7u8; 64])]).await.unwrap(); } conn.execute("COMMIT", ()).await.unwrap(); }
        line("turso", "load 1M rows (1000-row transactions)", format!("{:.2} us/row", us(t.elapsed(), N as usize)));
        let mut st = conn.prepare("SELECT score FROM t WHERE id = ?").await.unwrap(); let t = Instant::now(); let mut v = 0.0; for id in ids(5, 200_000) { let mut rows = st.query(vec![TV::Integer(id)]).await.unwrap(); if let Some(r) = rows.next().await.unwrap() { v += r.get::<f64>(0).unwrap(); } } line("turso", "point read by id, 1 thread", format!("{:.2} us", us(t.elapsed(), 200_000)));
        let mut st2 = conn.prepare("SELECT score FROM t WHERE name = ?").await.unwrap(); let t = Instant::now(); for id in ids(6, 100_000) { let mut rows = st2.query(vec![TV::Text(format!("u{id}@example.com"))]).await.unwrap(); if let Some(r) = rows.next().await.unwrap() { v += r.get::<f64>(0).unwrap(); } } line("turso", "lookup by name (unique index), 1 thread", format!("{:.2} us", us(t.elapsed(), 100_000)));
        let _ = v;
    });
    let t = Instant::now(); let hs: Vec<_> = (0..8).map(|k| { let db = db.clone(); std::thread::spawn(move || { let rt = rt(); let conn = db.connect().unwrap(); rt.block_on(async { let mut st = conn.prepare("SELECT score FROM t WHERE id = ?").await.unwrap(); let mut v = 0.0; for id in ids(100 + k, 200_000) { let mut rows = st.query(vec![TV::Integer(id)]).await.unwrap(); if let Some(r) = rows.next().await.unwrap() { v += r.get::<f64>(0).unwrap(); } } v }) }) }).collect(); let mut v = 0.0; for h in hs { v += h.join().unwrap(); } line("turso", "point reads, 8 threads", format!("{:.2}M/s", 1.6 / t.elapsed().as_secs_f64()));
    rt0.block_on(async {
        let t = Instant::now(); for id in 5_000_000..5_020_000i64 { conn.execute("INSERT INTO t VALUES (?,?,?,?,?,?)", vec![TV::Integer(id), TV::Real(score(id)), TV::Integer(id % 20), TV::Integer(18 + id % 60), TV::Text(format!("u{id}@example.com")), TV::Blob(vec![7u8; 64])]).await.unwrap(); } line("turso", "single-row inserts, synchronous=OFF (CPU cost)", format!("{:.2} us", us(t.elapsed(), 20_000)));
        conn.execute("PRAGMA synchronous = FULL", ()).await.ok(); let t = Instant::now(); for id in 5_100_000..5_102_000i64 { conn.execute("INSERT INTO t VALUES (?,?,?,?,?,?)", vec![TV::Integer(id), TV::Real(score(id)), TV::Integer(id % 20), TV::Integer(18 + id % 60), TV::Text(format!("u{id}@example.com")), TV::Blob(vec![7u8; 64])]).await.unwrap(); } line("turso", "single-row inserts, synchronous=FULL (durable)", format!("{:.0} us", us(t.elapsed(), 2_000)));
        conn.execute("PRAGMA synchronous = OFF", ()).await.ok();
    });
    for thr in [4usize, 12] { let ok = Arc::new(AtomicU64::new(0)); let busy = Arc::new(AtomicU64::new(0)); let t = Instant::now(); let hs: Vec<_> = (0..thr).map(|k| { let (ok, busy, db) = (ok.clone(), busy.clone(), db.clone()); std::thread::spawn(move || { let rt = rt(); let conn = db.connect().unwrap(); rt.block_on(async { let mut r = Rng(200 + k as u64); let end = Instant::now() + Duration::from_secs(2); while Instant::now() < end { let a = (r.next() % N as u64) as i64 + 1; let b = (r.next() % N as u64) as i64 + 1; if a == b { continue; } let res: Result<(), turso::Error> = async { conn.execute(if std::env::var("TURSO_WAL").is_err() { "BEGIN CONCURRENT" } else { "BEGIN IMMEDIATE" }, ()).await?; let conn = &conn; let get = |id: i64| async move { let mut rows = conn.query("SELECT score FROM t WHERE id = ?", vec![TV::Integer(id)]).await?; Ok::<f64, turso::Error>(rows.next().await?.map(|r| r.get::<f64>(0).unwrap()).unwrap_or(0.0)) }; let sa = get(a).await?; let sb = get(b).await?; conn.execute("UPDATE t SET score = ? WHERE id = ?", vec![TV::Real(sa - 1.0), TV::Integer(a)]).await?; conn.execute("UPDATE t SET score = ? WHERE id = ?", vec![TV::Real(sb + 1.0), TV::Integer(b)]).await?; conn.execute("COMMIT", ()).await?; Ok(()) }.await; match res { Ok(_) => { ok.fetch_add(1, Ordering::Relaxed); } Err(_) => { busy.fetch_add(1, Ordering::Relaxed); let _ = conn.execute("ROLLBACK", ()).await; } } } }) }) }).collect(); for h in hs { h.join().unwrap(); } let el = t.elapsed().as_secs_f64(); line("turso", &format!("transfer txns (2 reads + 2 updates), {thr} threads, BEGIN CONCURRENT"), format!("{:.0}/s, {} failed/retried", ok.load(Ordering::Relaxed) as f64 / el, busy.load(Ordering::Relaxed))); }
    rt0.block_on(async {
        let mut rows = conn.query("SELECT sum(score) FROM t WHERE id <= ?", vec![TV::Integer(N)]).await.unwrap(); let sum: f64 = rows.next().await.unwrap().unwrap().get(0).unwrap(); line("turso", "invariant: sum of scores over the 1M rows", format!("{sum:.2}"));
        let t = Instant::now(); let mut rows = conn.query("SELECT count(*) FROM t WHERE age > 40", ()).await.unwrap(); let n: i64 = rows.next().await.unwrap().unwrap().get(0).unwrap(); line("turso", "filtered count over the table", format!("{:.1} ms ({n})", t.elapsed().as_secs_f64() * 1e3));
        let t = Instant::now(); let mut rows = conn.query("SELECT country, count(*), avg(score) FROM t GROUP BY country", ()).await.unwrap(); let mut g = 0; while let Some(_) = rows.next().await.unwrap() { g += 1; } line("turso", "group by country", format!("{:.1} ms ({g} groups)", t.elapsed().as_secs_f64() * 1e3));
    });
    line("turso", "memory (RSS) at the end", format!("{:.0} MB", rss_mb()));
    drop(conn); drop(db);
    let t = Instant::now(); rt0.block_on(async { let db = Builder::new_local(DBT).build().await.unwrap(); let conn = db.connect().unwrap(); let mut rows = conn.query("SELECT score FROM t WHERE id = 777", ()).await.unwrap(); let _ = rows.next().await.unwrap(); }); line("turso", "reopen + first read", format!("{:.2} ms", t.elapsed().as_secs_f64() * 1e3));
    let _ = v;
}

// ---------------- ours ----------------
fn run_ours() {
    let dir = PathBuf::from(format!("{DIR}/db")); let sql = Arc::new(Sql::recreate(&dir, false)); let mut s = Session::default(); sql.exec_s(&mut s, CREATE_OURS, &[]).unwrap();
    let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let row = |id: i64| vec![Value::Int(id), Value::Float(score(id)), Value::Int(id % 20), Value::Int(18 + id % 60), Value::Text(format!("u{id}@example.com")), Value::Bytes(vec![7; 64])];
    let t = Instant::now(); for id in 1..=N { if id % 1000 == 1 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &row(id)).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } } line("ours", "load 1M rows (1000-row transactions)", format!("{:.2} us/row", us(t.elapsed(), N as usize)));
    let get = sql.prepare("SELECT score FROM t WHERE id = ?").unwrap(); let t = Instant::now(); let mut v = 0.0; for id in ids(5, 200_000) { if let Value::Float(f) = sql.execute_s(&mut s, &get, &[Value::Int(id)]).unwrap()[0][0] { v += f; } } line("ours", "point read by id, 1 thread", format!("{:.2} us", us(t.elapsed(), 200_000)));
    let byn = sql.prepare("SELECT score FROM t WHERE name = ?").unwrap(); let t = Instant::now(); for id in ids(6, 100_000) { if let Value::Float(f) = sql.execute_s(&mut s, &byn, &[Value::Text(format!("u{id}@example.com"))]).unwrap()[0][0] { v += f; } } line("ours", "lookup by name (unique index), 1 thread", format!("{:.2} us", us(t.elapsed(), 100_000)));
    let t = Instant::now(); let hs: Vec<_> = (0..8).map(|k| { let sql = sql.clone(); std::thread::spawn(move || { let mut s = Session::default(); let get = sql.prepare("SELECT score FROM t WHERE id = ?").unwrap(); let mut v = 0.0; for id in ids(100 + k, 200_000) { if let Value::Float(f) = sql.execute_s(&mut s, &get, &[Value::Int(id)]).unwrap()[0][0] { v += f; } } v }) }).collect(); for h in hs { v += h.join().unwrap(); } line("ours", "point reads, 8 threads", format!("{:.2}M/s", 1.6 / t.elapsed().as_secs_f64()));
    let t = Instant::now(); for id in 5_000_000..5_020_000i64 { sql.execute_s(&mut s, &ins, &row(id)).unwrap(); } line("ours", "single-row inserts, not waiting for disk (CPU cost)", format!("{:.2} us", us(t.elapsed(), 20_000)));
    { let ddir = PathBuf::from(format!("{DIR}/durable")); let dsql = Sql::recreate(&ddir, true); let mut ds = Session::default(); dsql.exec_s(&mut ds, CREATE_OURS, &[]).unwrap(); let dins = dsql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let t = Instant::now(); for id in 1..=2_000i64 { dsql.execute_s(&mut ds, &dins, &row(id)).unwrap(); } line("ours", "single-row inserts, durable (fsync each)", format!("{:.0} us", us(t.elapsed(), 2_000))); drop(dins); drop(dsql); let _ = fs::remove_dir_all(&ddir); }
    for id in 5_100_000..5_102_000i64 { sql.execute_s(&mut s, &ins, &row(id)).unwrap(); }  // same rows as the other engines' durable inserts, so the counts below compare
    for thr in [4usize, 12] { let ok = Arc::new(AtomicU64::new(0)); let busy = Arc::new(AtomicU64::new(0)); let t = Instant::now(); let hs: Vec<_> = (0..thr).map(|k| { let (ok, busy, sql) = (ok.clone(), busy.clone(), sql.clone()); std::thread::spawn(move || { let mut s = Session::default(); let get = sql.prepare("SELECT score FROM t WHERE id = ?").unwrap(); let upd = sql.prepare("UPDATE t SET score = ? WHERE id = ?").unwrap(); let mut r = Rng(200 + k as u64); let end = Instant::now() + Duration::from_secs(2); while Instant::now() < end { let a = (r.next() % N as u64) as i64 + 1; let b = (r.next() % N as u64) as i64 + 1; if a == b { continue; } let res = (|| -> Result<(), String> { sql.exec_s(&mut s, "BEGIN", &[])?; let sa = match sql.execute_s(&mut s, &get, &[Value::Int(a)])?[0][0] { Value::Float(f) => f, _ => 0.0 }; let sb = match sql.execute_s(&mut s, &get, &[Value::Int(b)])?[0][0] { Value::Float(f) => f, _ => 0.0 }; sql.execute_s(&mut s, &upd, &[Value::Float(sa - 1.0), Value::Int(a)])?; sql.execute_s(&mut s, &upd, &[Value::Float(sb + 1.0), Value::Int(b)])?; sql.exec_s(&mut s, "COMMIT", &[])?; Ok(()) })(); match res { Ok(_) => { ok.fetch_add(1, Ordering::Relaxed); } Err(_) => { busy.fetch_add(1, Ordering::Relaxed); let _ = sql.exec_s(&mut s, "ROLLBACK", &[]); } } } }) }).collect(); for h in hs { h.join().unwrap(); } let el = t.elapsed().as_secs_f64(); line("ours", &format!("transfer txns (2 reads + 2 updates), {thr} threads"), format!("{:.0}/s, {} failed/retried", ok.load(Ordering::Relaxed) as f64 / el, busy.load(Ordering::Relaxed))); }
    let sum = match sql.exec_s(&mut s, "SELECT sum(score) FROM t WHERE id <= ?", &[Value::Int(N)]).unwrap()[0][0] { Value::Float(f) => f, _ => 0.0 }; line("ours", "invariant: sum of scores over the 1M rows", format!("{sum:.2}"));
    let t = Instant::now(); let n = sql.exec_s(&mut s, "SELECT count(*) FROM t WHERE age > 40", &[]).unwrap(); line("ours", "filtered count over the table", format!("{:.1} ms ({:?})", t.elapsed().as_secs_f64() * 1e3, n[0][0]));
    let t = Instant::now(); let g = sql.exec_s(&mut s, "SELECT country, count(*), avg(score) FROM t GROUP BY country", &[]).unwrap().len(); line("ours", "group by country", format!("{:.1} ms ({g} groups)", t.elapsed().as_secs_f64() * 1e3));
    line("ours", "memory (RSS) at the end", format!("{:.0} MB", rss_mb()));
    sql.snapshot(); drop(ins); drop(get); drop(byn); drop(sql);
    let t = Instant::now(); let (sql, _) = Sql::open(&dir, false, 4).unwrap(); let mut s = Session::default(); sql.exec_s(&mut s, "SELECT score FROM t WHERE id = 777", &[]).unwrap(); line("ours", "reopen + first read (from a snapshot)", format!("{:.2} ms", t.elapsed().as_secs_f64() * 1e3));
    let _ = v; drop(sql); let _ = fs::remove_dir_all(DIR);
}

fn main() {
    match std::env::args().nth(1).as_deref() { Some("sqlite") => return run_sqlite(), Some("turso") => return run_turso(), Some("ours") => return run_ours(), _ => {} }
    println!("=== E28: SQLite (system lib) vs Turso {} vs ours, 1M rows, identical statements, each engine in its own process ===", std::env::var("TURSO_VERSION").unwrap_or_default());
    for e in ["sqlite", "turso", "ours"] { let o = Command::new(std::env::current_exe().unwrap()).arg(e).stderr(Stdio::inherit()).output().unwrap(); print!("{}", String::from_utf8_lossy(&o.stdout)); if !o.status.success() { println!("{e:<7} exited with {:?}", o.status.code()); } }
    for p in [DBS, DBT] { for suf in ["", "-wal", "-shm", "-tshm"] { fs::remove_file(format!("{p}{suf}")).ok(); } }
}
