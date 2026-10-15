//! Experiment 22 (H21 + H22): memory and restart rematch for the SQL storage. 2M rows, each configuration in its own process.
use dblab::sql::{Session, Sql};
use dblab::table::Value;
use dblab::util::Rng;
use rusqlite::{params, Connection};
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Instant;

const N: i64 = 2_000_000;
const DB: &str = "data/nocow/mem22.db"; const DIR: &str = "data/nocow/mem22dir";
const CREATE: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, age INT, name VARCHAR(32) UNIQUE, payload BLOB(64))";
fn row(id: i64, rng: &mut Rng) -> Vec<Value> { vec![Value::Int(id), Value::Float((rng.next() % 100000) as f64 / 100.0), Value::Int(id % 20), Value::Int(18 + id % 60), Value::Text(format!("u{id}@example.com")), Value::Bytes(vec![7; 64])] }
fn rss_mb() -> (f64, f64) { let s = fs::read_to_string("/proc/self/status").unwrap(); let get = |k: &str| s.lines().find(|l| l.starts_with(k)).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0) / 1024.0; (get("VmRSS:"), get("VmHWM:")) }
fn work(sql: &Sql) -> (i64, f64) { let mut s = Session::default(); let n = match sql.exec_s(&mut s, "SELECT count(*) FROM t WHERE age > 40", &[]).unwrap()[0][0] { Value::Int(i) => i, _ => 0 }; let mut rng = Rng(5); let mut v = 0.0; for _ in 0..100_000 { let id = (rng.next() % N as u64) as i64 + 1; if let Value::Float(f) = sql.exec_s(&mut s, "SELECT score FROM t WHERE id = ?", &[Value::Int(id)]).unwrap()[0][0] { v += f; } } for _ in 0..100_000 { let id = (rng.next() % N as u64) as i64 + 1; sql.exec_s(&mut s, "SELECT id FROM t WHERE name = ?", &[Value::Text(format!("u{id}@example.com"))]).unwrap(); } (n, v) }
fn child(mode: &str) -> String { let o = Command::new(std::env::current_exe().unwrap()).arg(mode).stderr(Stdio::inherit()).output().unwrap(); String::from_utf8_lossy(&o.stdout).trim_end().to_string() }

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("sqlite") => { let tuned = std::env::args().nth(2).is_some(); let c = Connection::open(DB).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL;").unwrap(); if tuned { c.execute_batch("PRAGMA cache_size=-262144; PRAGMA mmap_size=1073741824;").unwrap(); }
            let n: i64 = c.query_row("SELECT count(*) FROM t WHERE age > 40", [], |r| r.get(0)).unwrap(); let mut st = c.prepare("SELECT score FROM t WHERE id = ?").unwrap(); let mut rng = Rng(5); let mut v = 0.0; for _ in 0..100_000 { let id = (rng.next() % N as u64) as i64 + 1; v += st.query_row(params![id], |r| r.get::<_, f64>(0)).unwrap(); } let mut st2 = c.prepare("SELECT id FROM t WHERE name = ?").unwrap(); for _ in 0..100_000 { let id = (rng.next() % N as u64) as i64 + 1; let _: i64 = st2.query_row(params![format!("u{id}@example.com")], |r| r.get(0)).unwrap(); }
            let (r, h) = rss_mb(); println!("sqlite {}: rss={r:.0} MB peak={h:.0} MB (check {n} {v:.0})", if tuned { "tuned 256MB cache + mmap" } else { "default 2MB cache" }); return; }
        Some("ours") => { let dir = PathBuf::from(format!("{DIR}/live")); let sql = Sql::create(&dir, false); let mut s = Session::default(); sql.exec_s(&mut s, CREATE, &[]).unwrap(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let mut rng = Rng(7);
            for id in 1..=N { if id % 1000 == 1 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &row(id, &mut rng)).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } }
            let (n, v) = work(&sql); let (r, h) = rss_mb(); println!("ours, 2M rows loaded through SQL: rss={r:.0} MB peak={h:.0} MB (check {n} {v:.0})"); drop(ins); drop(sql); let _ = fs::remove_dir_all(&dir); return; }
        Some("ours-restart") => { let t = Instant::now(); let (sql, rs) = Sql::open(&PathBuf::from(format!("{DIR}/snap")), false, 12).unwrap(); let el = t.elapsed().as_secs_f64(); let (n, v) = work(&sql); let (r, h) = rss_mb(); println!("ours, restart from a snapshot of {} rows: {el:.3}s (load {:.3}s), rss={r:.0} MB peak={h:.0} MB (check {n} {v:.0})", rs.snap_rows, rs.snap_secs); return; }
        _ => {}
    }
    println!("=== E22: memory and restart rematch (2M rows; E12: ours 470 MB, 714 MB peak; E17 restart 0.185 s) ===");
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{DB}{suf}")).ok(); }
    { let c = Connection::open(DB).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap(); c.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL NOT NULL, country INTEGER NOT NULL, age INTEGER NOT NULL, name TEXT NOT NULL, payload BLOB NOT NULL); CREATE UNIQUE INDEX t_name ON t(name);").unwrap();
      let mut rng = Rng(7); let tx = c.unchecked_transaction().unwrap(); { let mut st = tx.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); for id in 1..=N { let r = row(id, &mut rng); st.execute(params![id, match r[1] { Value::Float(f) => f, _ => 0.0 }, id % 20, 18 + id % 60, format!("u{id}@example.com"), &[7u8; 64][..]]).unwrap(); } } tx.commit().unwrap(); c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap(); }
    { let dir = PathBuf::from(format!("{DIR}/snap")); let sql = Sql::create(&dir, false); let mut s = Session::default(); sql.exec_s(&mut s, CREATE, &[]).unwrap(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let mut rng = Rng(7);
      for id in 1..=N { if id % 1000 == 1 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &row(id, &mut rng)).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } }
      let st = sql.snapshot(); println!("snapshot: {} rows, {:.0} MB, {:.3}s", st.rows, st.bytes as f64 / 1e6, st.secs); drop(ins); drop(sql); }
    println!("{}", child("sqlite")); println!("{}", Command::new(std::env::current_exe().unwrap()).args(["sqlite", "tuned"]).output().map(|o| String::from_utf8_lossy(&o.stdout).trim_end().to_string()).unwrap());
    println!("{}", child("ours")); println!("{}", child("ours-restart"));
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{DB}{suf}")).ok(); } let _ = fs::remove_dir_all(DIR);
}
