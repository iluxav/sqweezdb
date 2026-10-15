//! Experiment 30: schema changes without a rebuild (the most-cited SQLite gripe). 2M rows; a reader thread and a writer
//! thread run through every ALTER on both engines. Ours: online rebuild (shadow table, mirrored writes, background copy,
//! swap). SQLite: ADD/DROP/RENAME COLUMN natively, type changes and NOT NULL through the documented copy-the-table recipe
//! inside one transaction. Then: kill -9 in the middle of our copy, restart, the change completes.
use dblab::sql::{Session, Sql};
use dblab::table::Value;
use dblab::util::Rng;
use rusqlite::{params, Connection};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
static NEXT_ID: AtomicI64 = AtomicI64::new(50_000_000);
use std::sync::Arc;
use std::time::{Duration, Instant};

const N: i64 = 2_000_000;
const DB: &str = "data/nocow/e30.db"; const DIR: &str = "data/nocow/e30dir";
fn row(id: i64, rng: &mut Rng) -> Vec<Value> { vec![Value::Int(id), Value::Float((rng.next() % 100000) as f64 / 100.0), Value::Int(id % 20), Value::Int(18 + id % 60), Value::Text(format!("u{id}@example.com")), Value::Bytes(vec![7; 64])] }
fn mem() -> (f64, f64) { let s = fs::read_to_string("/proc/self/status").unwrap(); let get = |k: &str| s.lines().find(|l| l.starts_with(k)).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0) / 1024.0; (get("VmRSS:"), get("VmHWM:")) }
fn fmt(r: &Result<Vec<Vec<Value>>, String>) -> String { match r { Ok(rows) => format!("{:?}", rows.iter().take(2).map(|r| r.iter().map(|v| match v { Value::Float(f) => format!("{f}"), Value::Int(i) => i.to_string(), Value::Bool(b) => b.to_string(), Value::Text(s) => s.clone(), Value::Bytes(b) => format!("blob{}", b.len()), Value::Null => "NULL".into() }).collect::<Vec<_>>()).collect::<Vec<_>>()), Err(e) => format!("error: {}", e.chars().take(70).collect::<String>()) } }
fn sq(c: &Connection, sql: &str) -> Result<Vec<Vec<String>>, String> { let mut st = c.prepare(sql).map_err(|e| e.to_string())?; if st.column_count() == 0 { st.execute([]).map_err(|e| e.to_string())?; return Ok(vec![]); } let n = st.column_count(); let rows = st.query_map([], |r| Ok((0..n).map(|i| match r.get::<_, rusqlite::types::Value>(i).unwrap() { rusqlite::types::Value::Null => "NULL".into(), rusqlite::types::Value::Integer(i) => i.to_string(), rusqlite::types::Value::Real(f) => format!("{f}"), rusqlite::types::Value::Text(s) => s, rusqlite::types::Value::Blob(b) => format!("blob{}", b.len()) }).collect::<Vec<_>>())).map_err(|e| e.to_string())?; rows.collect::<Result<Vec<_>, _>>().map_err(|e| e.to_string()) }
fn fmts(r: &Result<Vec<Vec<String>>, String>) -> String { match r { Ok(rows) => format!("{:?}", rows.iter().take(2).collect::<Vec<_>>()), Err(e) => format!("error: {}", e.chars().take(70).collect::<String>()) } }

struct Load { stop: Arc<AtomicBool>, h: Vec<std::thread::JoinHandle<(u64, Duration)>> }
fn load_ours(sql: &Arc<Sql>) -> Load {
    let stop = Arc::new(AtomicBool::new(false)); let mut h = vec![];
    { let (sql, stop) = (sql.clone(), stop.clone()); h.push(std::thread::spawn(move || { let mut s = Session::default(); let mut rng = Rng(31); let (mut n, mut worst) = (0u64, Duration::ZERO); while !stop.load(Ordering::Relaxed) { let id = NEXT_ID.fetch_add(1, Ordering::Relaxed); let t = Instant::now(); sql.exec_s(&mut s, "INSERT INTO t(id, country, age, name) VALUES (?,?,?,?)", &[Value::Int(id), Value::Int(id % 20), Value::Int(18 + (rng.next() % 60) as i64), Value::Text(format!("w{id}@x.com"))]).unwrap(); worst = worst.max(t.elapsed()); n += 1; std::thread::sleep(Duration::from_micros(30)); } (n, worst) })); }
    { let (sql, stop) = (sql.clone(), stop.clone()); h.push(std::thread::spawn(move || { let mut s = Session::default(); let mut rng = Rng(32); let (mut n, mut worst) = (0u64, Duration::ZERO); while !stop.load(Ordering::Relaxed) { let id = (rng.next() % N as u64) as i64 + 1; let t = Instant::now(); let r = sql.exec_s(&mut s, "SELECT id, country FROM t WHERE id = ?", &[Value::Int(id)]).unwrap(); assert!(!r.is_empty(), "row {id} vanished"); worst = worst.max(t.elapsed()); n += 1; } (n, worst) })); }
    Load { stop, h }
}
fn load_sqlite() -> Load {
    let stop = Arc::new(AtomicBool::new(false)); let mut h = vec![];
    { let stop = stop.clone(); h.push(std::thread::spawn(move || { let c = Connection::open(DB).unwrap(); c.busy_timeout(Duration::from_secs(600)).unwrap(); c.execute_batch("PRAGMA synchronous=NORMAL").unwrap(); let mut rng = Rng(31); let (mut n, mut worst) = (0u64, Duration::ZERO); while !stop.load(Ordering::Relaxed) { let id = NEXT_ID.fetch_add(1, Ordering::Relaxed); let t = Instant::now(); c.execute("INSERT INTO t(id, country, age, name) VALUES (?,?,?,?)", params![id, id % 20, 18 + (rng.next() % 60) as i64, format!("w{id}@x.com")]).unwrap(); worst = worst.max(t.elapsed()); n += 1; std::thread::sleep(Duration::from_micros(30)); } (n, worst) })); }
    { let stop = stop.clone(); h.push(std::thread::spawn(move || { let c = Connection::open(DB).unwrap(); c.busy_timeout(Duration::from_secs(600)).unwrap(); let mut rng = Rng(32); let (mut n, mut worst) = (0u64, Duration::ZERO); while !stop.load(Ordering::Relaxed) { let id = (rng.next() % N as u64) as i64 + 1; let t = Instant::now(); let r: i64 = c.query_row("SELECT id FROM t WHERE id = ?", params![id], |r| r.get(0)).unwrap(); assert_eq!(r, id); worst = worst.max(t.elapsed()); n += 1; } (n, worst) })); }
    Load { stop, h }
}
fn finish(l: Load) -> ((u64, Duration), (u64, Duration)) { l.stop.store(true, Ordering::Relaxed); let mut r = l.h.into_iter().map(|h| h.join().unwrap()); (r.next().unwrap(), r.next().unwrap()) }
fn report(engine: &str, label: &str, stmt: Duration, done: Duration, w: (u64, Duration), r: (u64, Duration)) { println!("{engine:<7} {label:<34} statement {:>8.1} ms, complete after {:>8.1} ms | writer {:>7} inserts, worst stall {:>8.2} ms | reader {:>7} reads, worst {:>7.2} ms", stmt.as_secs_f64() * 1e3, done.as_secs_f64() * 1e3, w.0, w.1.as_secs_f64() * 1e3, r.0, r.1.as_secs_f64() * 1e3); }

fn main() {
    if std::env::args().nth(1).as_deref() == Some("crash-child") {
        let dir = PathBuf::from(format!("{DIR}/crash")); let sql = Sql::create(&dir, false); let mut s = Session::default();
        sql.exec_s(&mut s, "CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, age INT, name VARCHAR(32) UNIQUE, payload BLOB(64))", &[]).unwrap();
        let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let mut rng = Rng(7);
        for id in 1..=N { if id % 1000 == 1 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &row(id, &mut rng)).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } }
        sql.snapshot();
        sql.exec_s(&mut s, "ALTER TABLE t ADD COLUMN flag BOOL NOT NULL DEFAULT FALSE", &[]).unwrap(); println!("altering"); std::io::stdout().flush().unwrap();
        loop { std::thread::sleep(Duration::from_secs(10)); }
    }
    println!("=== E30: schema changes while readers and writers keep running (2M rows; the writer is throttled to ~20k inserts/s on both sides) ===");
    // ---- build ----
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{DB}{suf}")).ok(); }
    let c = Connection::open(DB).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap(); c.busy_timeout(Duration::from_secs(600)).unwrap();
    c.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL, country INTEGER, age INTEGER, name TEXT, payload BLOB); CREATE UNIQUE INDEX t_name ON t(name);").unwrap();
    { let mut rng = Rng(7); let tx = c.unchecked_transaction().unwrap(); { let mut st = tx.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); for id in 1..=N { let r = row(id, &mut rng); st.execute(params![id, match r[1] { Value::Float(f) => f, _ => 0.0 }, id % 20, 18 + id % 60, format!("u{id}@example.com"), &[7u8; 64][..]]).unwrap(); } } tx.commit().unwrap(); c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap(); }
    let dir = PathBuf::from(format!("{DIR}/main")); let sql = Arc::new(Sql::create(&dir, false)); let mut s = Session::default();
    sql.exec_s(&mut s, "CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, age INT, name VARCHAR(32) UNIQUE, payload BLOB(64))", &[]).unwrap();
    { let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let mut rng = Rng(7); for id in 1..=N { if id % 1000 == 1 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &row(id, &mut rng)).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } } }
    let (r0, _) = mem(); println!("built: 2M rows both sides; our process {r0:.0} MB");
    let alters: Vec<(&str, Vec<&str>, &str)> = vec![
        ("ADD COLUMN flag NOT NULL DEFAULT", vec!["ALTER TABLE t ADD COLUMN flag INTEGER NOT NULL DEFAULT 0"], "ALTER TABLE t ADD COLUMN flag BOOL NOT NULL DEFAULT FALSE"),
        ("DROP COLUMN payload", vec!["ALTER TABLE t DROP COLUMN payload"], "ALTER TABLE t DROP COLUMN payload"),
        ("RENAME COLUMN score TO points", vec!["ALTER TABLE t RENAME COLUMN score TO points"], "ALTER TABLE t RENAME COLUMN score TO points"),
        ("change country INT -> DOUBLE", vec!["BEGIN IMMEDIATE", "CREATE TABLE t_new(id INTEGER PRIMARY KEY, points REAL, country REAL, age INTEGER, name TEXT, flag INTEGER NOT NULL DEFAULT 0)", "INSERT INTO t_new SELECT id, points, country, age, name, flag FROM t", "DROP TABLE t", "ALTER TABLE t_new RENAME TO t", "CREATE UNIQUE INDEX t_name ON t(name)", "COMMIT"], "ALTER TABLE t ALTER COLUMN country SET DATA TYPE DOUBLE"),
        ("age SET NOT NULL", vec!["BEGIN IMMEDIATE", "CREATE TABLE t_new(id INTEGER PRIMARY KEY, points REAL, country REAL, age INTEGER NOT NULL, name TEXT, flag INTEGER NOT NULL DEFAULT 0)", "INSERT INTO t_new SELECT id, points, country, age, name, flag FROM t", "DROP TABLE t", "ALTER TABLE t_new RENAME TO t", "CREATE UNIQUE INDEX t_name ON t(name)", "COMMIT"], "ALTER TABLE t ALTER COLUMN age SET NOT NULL"),
    ];
    let checks = ["SELECT count(*) FROM t WHERE id <= 2000000", "SELECT sum(country) FROM t WHERE id <= 2000000", "SELECT flag FROM t WHERE id = 7", "SELECT payload FROM t WHERE id = 7", "SELECT points FROM t WHERE id = 7", "SELECT score FROM t WHERE id = 7", "SELECT country FROM t WHERE id = 7"];
    for (label, sq_stmts, our_stmt) in &alters {
        // SQLite
        let l = load_sqlite(); std::thread::sleep(Duration::from_millis(300));
        let t = Instant::now(); for st in sq_stmts { if let Err(e) = c.execute_batch(st) { println!("sqlite  {label}: {e}"); let _ = c.execute_batch("ROLLBACK"); break; } } let el = t.elapsed();
        std::thread::sleep(Duration::from_millis(300)); let (w, r) = finish(l); report("sqlite", label, el, el, w, r);
        // ours
        let l = load_ours(&sql); std::thread::sleep(Duration::from_millis(300));
        let t = Instant::now(); sql.exec_s(&mut s, our_stmt, &[]).unwrap(); let st = t.elapsed(); sql.wait_alters().unwrap(); let done = t.elapsed();
        std::thread::sleep(Duration::from_millis(300)); let (w, r) = finish(l); report("ours", label, st, done, w, r);
        let (rss, peak) = mem(); println!("        after: our process {rss:.0} MB (peak {peak:.0} MB) | {}", checks.iter().map(|q| format!("{} -> sqlite {} / ours {}", q.trim_start_matches("SELECT ").split(" FROM").next().unwrap(), fmts(&sq(&c, q)), fmt(&sql.exec_s(&mut s, q, &[])))).collect::<Vec<_>>().join("; "));
    }
    println!("NULL age after SET NOT NULL -> sqlite {} / ours {}", fmts(&sq(&c, "INSERT INTO t(id, country, name) VALUES (90000001, 1, 'x1')")), fmt(&sql.exec_s(&mut s, "INSERT INTO t(id, country, name) VALUES (90000001, 1, 'x1')", &[])));
    let st = sql.snapshot(); let (rss, _) = mem(); println!("snapshot after the changes: {} rows, {:.0} MB, {:.2} s; our process {rss:.0} MB", st.rows, st.bytes as f64 / 1e6, st.secs);
    drop(sql); drop(c); for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{DB}{suf}")).ok(); }
    // ---- crash in the middle of a copy ----
    let _ = fs::remove_dir_all(format!("{DIR}/crash"));
    let mut child = Command::new(std::env::current_exe().unwrap()).arg("crash-child").stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap();
    { let out = child.stdout.take().unwrap(); let mut lines = BufReader::new(out).lines(); while let Some(Ok(l)) = lines.next() { if l == "altering" { break; } } }
    std::thread::sleep(Duration::from_millis(250)); child.kill().unwrap(); child.wait().unwrap();
    let dir = PathBuf::from(format!("{DIR}/crash")); let t = Instant::now(); let (sql, rs, pending) = Sql::open_resume(&dir, false, 12).unwrap(); let opened = t.elapsed(); sql.wait_alters().unwrap(); let done = t.elapsed(); let mut s = Session::default();
    let cnt = sql.exec_s(&mut s, "SELECT count(*) FROM t", &[]).unwrap(); let mut rng = Rng(9); let mut ok = 0; for _ in 0..1000 { let id = (rng.next() % N as u64) as i64 + 1; if let Ok(r) = sql.exec_s(&mut s, "SELECT flag, name FROM t WHERE id = ?", &[Value::Int(id)]) { if r.len() == 1 && r[0][0] == Value::Bool(false) && r[0][1] == Value::Text(format!("u{id}@example.com")) { ok += 1; } } }
    println!("kill -9 250 ms into ADD COLUMN on 2M rows: restart {:.3} s (replayed {} records, {pending} change pending), change completed {:.2} s after restart; count {:?}, 1000 random rows with the new column: {ok} correct", opened.as_secs_f64(), rs.replayed, done.as_secs_f64(), cnt[0][0]);
    drop(sql); let _ = fs::remove_dir_all(DIR);
}
