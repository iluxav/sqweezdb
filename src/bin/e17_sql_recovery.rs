//! Experiment 17 (H25 + H29): crash recovery for the SQL storage. kill -9 a process writing through SQL (autocommit inserts,
//! multi-row transactions, snapshots every 700 ms, roles and policies defined), reopen, verify every acknowledged write and the ACL.
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::Value;
use sqweezdb::util::Rng;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

const DIR: &str = "data/nocow/e17dir";
const THREADS: usize = 32;
const CREATE: &str = "CREATE TABLE acct(id INTEGER PRIMARY KEY, score DOUBLE, owner INT, name VARCHAR(32) UNIQUE, note BLOB(32))";
fn ack_path(k: usize) -> PathBuf { PathBuf::from(format!("data/nocow/ack_sql_{k}.bin")) }
fn write_ack(f: &mut File, id: i64, score: f64) { let mut b = [0u8; 16]; b[..8].copy_from_slice(&id.to_le_bytes()); b[8..].copy_from_slice(&score.to_bits().to_le_bytes()); f.write_all(&b).unwrap(); }
fn read_acks() -> Vec<(i64, f64)> { let mut v = vec![]; for k in 0..THREADS { if let Ok(d) = fs::read(ack_path(k)) { for c in d.chunks_exact(16) { v.push((i64::from_le_bytes(c[..8].try_into().unwrap()), f64::from_bits(u64::from_le_bytes(c[8..].try_into().unwrap())))); } } } v }
fn clear_acks() { for k in 0..THREADS { fs::remove_file(ack_path(k)).ok(); } }
fn row(id: i64, score: f64) -> Vec<Value> { vec![Value::Int(id), Value::Float(score), Value::Int(id % 1000), Value::Text(format!("a{id}@x")), Value::Bytes(vec![1; 32])] }

fn victim() -> ! {
    let sql = Arc::new(Sql::create(&PathBuf::from(DIR), true));
    let mut app = Session::default();
    for st in [CREATE, "CREATE ROLE tenant_user", "GRANT SELECT, INSERT ON acct TO tenant_user", "CREATE POLICY own ON acct FOR ALL TO tenant_user USING (owner = SESSION.owner)", "CREATE USER alice IN ROLE tenant_user WITH owner = 7"] { sql.exec_s(&mut app, st, &[]).unwrap(); }
    { let sql = sql.clone(); std::thread::spawn(move || loop { std::thread::sleep(Duration::from_millis(700)); let s = sql.snapshot(); eprintln!("    victim snapshot: {} rows, {:.1} MB, low_water {}, {:.3}s, deleted {} segment(s)", s.rows, s.bytes as f64 / 1e6, s.low_water, s.secs, s.deleted_segments); }); }
    let ins = sql.prepare("INSERT INTO acct VALUES (?,?,?,?,?)").unwrap(); let upd = sql.prepare("UPDATE acct SET score = ? WHERE id = ?").unwrap();
    for k in 0..THREADS { let sql = sql.clone(); let ins = ins.clone(); let upd = upd.clone(); std::thread::spawn(move || {
        let mut ack = OpenOptions::new().create(true).write(true).truncate(true).open(ack_path(k)).unwrap();
        let mut sess = Session::default(); let mut rng = Rng(1 + k as u64); let mut i = 0i64;
        loop {
            let id = (k as i64) * 1_000_000 + i;
            sql.execute_s(&mut sess, &ins, &row(id, i as f64)).unwrap(); write_ack(&mut ack, id, i as f64);
            if i % 2 == 1 {
                let a = (k as i64) * 1_000_000 + (rng.next() % i as u64) as i64; let b = (k as i64) * 1_000_000 + (rng.next() % i as u64) as i64; let sc = (1_000_000 + i) as f64;
                sql.exec_s(&mut sess, "BEGIN", &[]).unwrap(); sql.execute_s(&mut sess, &upd, &[Value::Float(sc), Value::Int(a)]).unwrap(); if b != a { sql.execute_s(&mut sess, &upd, &[Value::Float(sc), Value::Int(b)]).unwrap(); }
                sql.exec_s(&mut sess, "COMMIT", &[]).unwrap(); write_ack(&mut ack, a, sc); if b != a { write_ack(&mut ack, b, sc); }
            }
            i += 1;
        }
    }); }
    loop { std::thread::sleep(Duration::from_secs(3600)); }
}

fn kill_round(delay_ms: u64) {
    clear_acks();
    let mut child = Command::new(std::env::current_exe().unwrap()).arg("victim").stdout(Stdio::null()).stderr(Stdio::inherit()).spawn().unwrap();
    std::thread::sleep(Duration::from_millis(delay_ms)); child.kill().unwrap(); child.wait().unwrap();
    let t = Instant::now();
    let (sql, rs) = Sql::open(&PathBuf::from(DIR), false, 12).unwrap();
    let rt = t.elapsed();
    let acks = read_acks();
    let mut latest: HashMap<i64, f64> = HashMap::new(); for (id, sc) in &acks { latest.insert(*id, *sc); }   // per-id acks are in time order within a file
    let q = sql.prepare("SELECT score FROM acct WHERE id = ?").unwrap(); let mut app = Session::default();
    let mut lost = 0u64; let mut stale = 0u64;
    for (id, sc) in &latest { match sql.execute_s(&mut app, &q, &[Value::Int(*id)]).unwrap().first() { Some(r) => if let Value::Float(v) = r[0] { if v < *sc { stale += 1; } }, None => lost += 1 } }
    let total = sql.exec_s(&mut app, "SELECT count(*) FROM acct", &[]).unwrap();
    let mut alice = Session::default(); let acl = match sql.login(&mut alice, "alice") { Ok(()) => format!("alice sees {:?} rows (policy owner=7 survived)", sql.exec_s(&mut alice, "SELECT count(*) FROM acct", &[]).unwrap()[0][0]), Err(e) => format!("ACL LOST: {e}") };
    let after = sql.exec_s(&mut app, "INSERT INTO acct VALUES (?,?,?,?,?)", &row(99_000_000, 1.0)).map(|_| "writes after recovery ok".to_string()).unwrap_or_else(|e| e);
    println!("[sql+snapshots] kill -9 after {delay_ms}ms: snapshot {} rows, catalog {} stmts, {} segment(s), replayed {} skipped {} | acked {} writes on {} ids | LOST {lost}, stale {stale} | total {:?} | {acl} | {after} | restart {:.3}s (load {:.3} + replay {:.3})",
        rs.snap_rows, rs.catalog, rs.segments, rs.replayed, rs.skipped, acks.len(), latest.len(), total[0][0], rt.as_secs_f64(), rs.snap_secs, rs.replay_secs);
    drop(q); drop(sql); let _ = fs::remove_dir_all(DIR); clear_acks();
}

fn restart_time() {
    let dir = PathBuf::from("data/nocow/e17big");
    let sql = Sql::create(&dir, false); let mut app = Session::default();
    sql.exec_s(&mut app, CREATE, &[]).unwrap();
    let ins = sql.prepare("INSERT INTO acct VALUES (?,?,?,?,?)").unwrap(); let upd = sql.prepare("UPDATE acct SET score = ? WHERE id = ?").unwrap();
    for id in 1..=2_000_000i64 { if id % 1000 == 1 { sql.exec_s(&mut app, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut app, &ins, &row(id, id as f64)).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut app, "COMMIT", &[]).unwrap(); } }
    let s = sql.snapshot(); println!("snapshot of {} rows: {:.0} MB in {:.3}s", s.rows, s.bytes as f64 / 1e6, s.secs);
    let mut rng = Rng(3); for i in 0..1_000_000 { sql.execute_s(&mut app, &upd, &[Value::Float(i as f64), Value::Int((rng.next() % 2_000_000) as i64 + 1)]).unwrap(); }
    let check: Vec<(i64, f64)> = (0..50).map(|i| { let id = 1 + i * 39_999; (id, match sql.exec_s(&mut app, "SELECT score FROM acct WHERE id = ?", &[Value::Int(id)]).unwrap()[0][0] { Value::Float(f) => f, _ => -1.0 }) }).collect();
    drop(ins); drop(upd); drop(sql);
    let t = Instant::now(); let (sql, rs) = Sql::open(&dir, false, 12).unwrap();
    println!("restart A (snapshot {} rows + 1M-record tail), 12 threads: {:.3}s = load {:.3} + replay {} records {:.3}", rs.snap_rows, t.elapsed().as_secs_f64(), rs.snap_secs, rs.replayed, rs.replay_secs);
    let mut app = Session::default(); for (id, sc) in &check { let v = match sql.exec_s(&mut app, "SELECT score FROM acct WHERE id = ?", &[Value::Int(*id)]).unwrap()[0][0] { Value::Float(f) => f, _ => -2.0 }; assert_eq!(v, *sc, "row {id} differs after restart"); }
    let s = sql.snapshot(); println!("snapshot again: {:.3}s, deleted {} segment(s)", s.secs, s.deleted_segments); drop(sql);
    let t = Instant::now(); let (sql, rs) = Sql::open(&dir, false, 12).unwrap();
    println!("restart B (snapshot {} rows, empty tail), 12 threads: {:.3}s = load {:.3} + replay {} records", rs.snap_rows, t.elapsed().as_secs_f64(), rs.snap_secs, rs.replayed);
    drop(sql); let _ = fs::remove_dir_all(&dir);
}

fn main() {
    if std::env::args().nth(1).as_deref() == Some("victim") { victim(); }
    println!("=== E17: crash recovery for the SQL storage (kill -9, snapshots, catalog + ACL persisted) ===");
    for d in [2000, 3600] { kill_round(d); }
    restart_time();
}
