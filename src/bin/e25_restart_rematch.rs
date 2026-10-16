//! Experiment 25 (H19 + cold tiers): restart and memory rematch. The snapshot is now the table: slot layout sorted by
//! key, mapped privately on restart. 2M rows. Each configuration runs in its own process; "cold" evicts the files from
//! the page cache first; "limited" runs cold inside a cgroup with a hard memory cap (systemd-run --user --scope).
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::Value;
use sqweezdb::util::Rng;
use rusqlite::{params, Connection};
use std::fs;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Instant;

const N: i64 = 2_000_000;
const DB: &str = "data/nocow/mem25.db"; const DIR: &str = "data/nocow/mem25dir";
const CREATE: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, age INT, name VARCHAR(32) UNIQUE, payload BLOB(64))";
fn row(id: i64, rng: &mut Rng) -> Vec<Value> { vec![Value::Int(id), Value::Float((rng.next() % 100000) as f64 / 100.0), Value::Int(id % 20), Value::Int(18 + id % 60), Value::Text(format!("u{id}@example.com")), Value::Bytes(vec![7; 64])] }
fn mem() -> String { let s = fs::read_to_string("/proc/self/status").unwrap(); let get = |k: &str| s.lines().find(|l| l.starts_with(k)).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0) / 1024.0; format!("rss={:.0} MB (owned {:.0} + file-backed {:.0}), peak={:.0}", get("VmRSS:"), get("RssAnon:"), get("RssFile:"), get("VmHWM:")) }
unsafe extern "C" { fn posix_fadvise(fd: i32, off: i64, len: i64, advice: i32) -> i32; }
fn evict(paths: &[PathBuf]) { Command::new("sync").status().unwrap(); for p in paths { if let Ok(f) = fs::File::open(p) { unsafe { posix_fadvise(f.as_raw_fd(), 0, 0, 4); } } } }
fn ours_files() -> Vec<PathBuf> { fs::read_dir(format!("{DIR}/snap")).unwrap().filter_map(|e| e.ok()).map(|e| e.path()).collect() }
fn sqlite_files() -> Vec<PathBuf> { ["", "-wal", "-shm"].iter().map(|s| PathBuf::from(format!("{DB}{s}"))).collect() }
fn child(args: &[&str]) -> String { let o = Command::new(std::env::current_exe().unwrap()).args(args).stderr(Stdio::inherit()).output().unwrap(); String::from_utf8_lossy(&o.stdout).trim_end().to_string() }
fn limited(mb: u32, args: &[&str]) -> String {
    let exe = std::env::current_exe().unwrap();
    let o = Command::new("systemd-run").args(["--user", "--scope", "-q", "-p", &format!("MemoryMax={mb}M"), "-p", "MemorySwapMax=0", "--", "timeout", "120"]).arg(&exe).args(args).stderr(Stdio::inherit()).output().unwrap();
    let s = String::from_utf8_lossy(&o.stdout).trim_end().to_string();
    if s.is_empty() { format!("{} under {mb} MB cap: did not finish within 120 s (or out of memory)", args[0]) } else { format!("{s} under {mb} MB cap") }
}
/// ids drawn the same way in both engines: 1000 then 100k point reads, 100k unique-key reads, one filtered count.
fn ids(seed: u64, n: usize) -> Vec<i64> { let mut r = Rng(seed); (0..n).map(|_| (r.next() % N as u64) as i64 + 1).collect() }

fn main() {
    let a: Vec<String> = std::env::args().collect();
    match a.get(1).map(|s| s.as_str()) {
        Some("sqlite") => {
            let cold = a.get(2).map(|s| s == "cold").unwrap_or(false); if cold { evict(&sqlite_files()); }
            let t0 = Instant::now(); let c = Connection::open(DB).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL;").unwrap(); let mut st = c.prepare("SELECT score FROM t WHERE id = ?").unwrap(); let open = t0.elapsed();
            let t = Instant::now(); let _: f64 = st.query_row(params![777_777i64], |r| r.get(0)).unwrap(); let first = t.elapsed();
            let t = Instant::now(); let mut v = 0.0; for id in ids(11, 1000) { v += st.query_row(params![id], |r| r.get::<_, f64>(0)).unwrap(); } let k1 = t.elapsed();
            let t = Instant::now(); for id in ids(5, 100_000) { v += st.query_row(params![id], |r| r.get::<_, f64>(0)).unwrap(); } let p1 = t.elapsed();
            let t = Instant::now(); let mut st2 = c.prepare("SELECT id FROM t WHERE name = ?").unwrap(); for id in ids(6, 100_000) { let _: i64 = st2.query_row(params![format!("u{id}@example.com")], |r| r.get(0)).unwrap(); } let p2 = t.elapsed();
            let t = Instant::now(); let mut st3 = c.prepare("SELECT score FROM t WHERE name = ?").unwrap(); for id in ids(8, 100_000) { v += st3.query_row(params![format!("u{id}@example.com")], |r| r.get::<_, f64>(0)).unwrap(); } let p2b = t.elapsed();
            let t = Instant::now(); let n: i64 = c.query_row("SELECT count(*) FROM t WHERE age > 40", [], |r| r.get(0)).unwrap(); let p3 = t.elapsed();
            println!("sqlite {}: open {:.3} ms | first read {:.3} ms | 1000 reads {:.1} ms | 100k by id {:.2} s, 100k by name->id {:.2} s, 100k by name->row {:.2} s, filtered count {:.2} s | {} (check {n} {v:.0})", if cold { "cold" } else { "warm" }, open.as_secs_f64() * 1e3, first.as_secs_f64() * 1e3, k1.as_secs_f64() * 1e3, p1.as_secs_f64(), p2.as_secs_f64(), p2b.as_secs_f64(), p3.as_secs_f64(), mem()); return; }
        Some("ours") => {
            let cold = a.get(2).map(|s| s == "cold").unwrap_or(false); if cold { evict(&ours_files()); }
            let t0 = Instant::now(); let (sql, rs) = Sql::open(&PathBuf::from(format!("{DIR}/snap")), false, 12).unwrap(); let mut s = Session::default(); let st = sql.prepare("SELECT score FROM t WHERE id = ?").unwrap(); let open = t0.elapsed();
            let get = |sql: &Sql, s: &mut Session, id: i64| -> f64 { if let Value::Float(f) = sql.execute_s(s, &st, &[Value::Int(id)]).unwrap().first().map(|r| r[0].clone()).unwrap_or(Value::Null) { f } else { 0.0 } };
            let t = Instant::now(); get(&sql, &mut s, 777_777); let first = t.elapsed();
            let t = Instant::now(); let mut v = 0.0; for id in ids(11, 1000) { v += get(&sql, &mut s, id); } let k1 = t.elapsed();
            let t = Instant::now(); for id in ids(5, 100_000) { v += get(&sql, &mut s, id); } let p1 = t.elapsed();
            let t = Instant::now(); let st2 = sql.prepare("SELECT id FROM t WHERE name = ?").unwrap(); for id in ids(6, 100_000) { sql.execute_s(&mut s, &st2, &[Value::Text(format!("u{id}@example.com"))]).unwrap(); } let p2 = t.elapsed();
            let t = Instant::now(); let st3 = sql.prepare("SELECT score FROM t WHERE name = ?").unwrap(); for id in ids(8, 100_000) { if let Some(Value::Float(f)) = sql.execute_s(&mut s, &st3, &[Value::Text(format!("u{id}@example.com"))]).unwrap().first().map(|r| r[0].clone()) { v += f; } } let p2b = t.elapsed();
            let t = Instant::now(); let n = match sql.exec_s(&mut s, "SELECT count(*) FROM t WHERE age > 40", &[]).unwrap()[0][0] { Value::Int(i) => i, _ => 0 }; let p3 = t.elapsed();
            println!("ours {} ({} rows mapped, snapshot part {:.3} ms): open {:.3} ms | first read {:.3} ms | 1000 reads {:.1} ms | 100k by id {:.2} s, 100k by name->id {:.2} s, 100k by name->row {:.2} s, filtered count {:.2} s | {} (check {n} {v:.0})", if cold { "cold" } else { "warm" }, rs.snap_rows, rs.snap_secs * 1e3, open.as_secs_f64() * 1e3, first.as_secs_f64() * 1e3, k1.as_secs_f64() * 1e3, p1.as_secs_f64(), p2.as_secs_f64(), p2b.as_secs_f64(), p3.as_secs_f64(), mem()); return; }
        Some("ours-writes") => {
            // after a restart: update 100k mapped rows in place and insert 100k new rows, then snapshot again and restart once more
            let (sql, _) = Sql::open(&PathBuf::from(format!("{DIR}/snap")), false, 12).unwrap(); let mut s = Session::default();
            sql.exec_s(&mut s, "SELECT count(*) FROM t WHERE age > 40", &[]).unwrap(); // warm the page cache first: this measures the copy-on-write cost, not the disk
            let t = Instant::now(); for id in ids(21, 100_000) { sql.exec_s(&mut s, "UPDATE t SET score = 1.5 WHERE id = ?", &[Value::Int(id)]).unwrap(); } let upd = t.elapsed();
            let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let mut rng = Rng(9);
            let t = Instant::now(); for id in N + 1..=N + 100_000 { sql.execute_s(&mut s, &ins, &row(id, &mut rng)).unwrap(); } let insd = t.elapsed();
            let t = Instant::now(); for id in ids(22, 10_000) { let _ = sql.exec_s(&mut s, "DELETE FROM t WHERE id = ?", &[Value::Int(id)]); } let del = t.elapsed();
            let cnt = |sql: &Sql, s: &mut Session| -> (i64, i64) { let a = match sql.exec_s(s, "SELECT count(*) FROM t", &[]).unwrap()[0][0] { Value::Int(i) => i, _ => 0 }; let b = match sql.exec_s(s, "SELECT count(*) FROM t WHERE score = 1.5", &[]).unwrap()[0][0] { Value::Int(i) => i, _ => 0 }; (a, b) };
            let before = cnt(&sql, &mut s); let m1 = mem();
            let st = sql.snapshot(); drop(ins); drop(sql);
            let t = Instant::now(); let (sql, rs) = Sql::open(&PathBuf::from(format!("{DIR}/snap")), false, 12).unwrap(); let reopen = t.elapsed(); let mut s = Session::default(); let after = cnt(&sql, &mut s);
            println!("ours writes after restart (warm): 100k in-place updates {:.3} s, 100k inserts {:.3} s, 10k deletes {:.3} s | {m1} | counts before {before:?} after re-snapshot+restart {after:?} ({} rows, {:.0} MB, {:.3} s; reopen {:.3} ms)", upd.as_secs_f64(), insd.as_secs_f64(), del.as_secs_f64(), st.rows, st.bytes as f64 / 1e6, st.secs, reopen.as_secs_f64() * 1e3); let _ = rs; return; }
        Some("inspect") => { let (sql, rs) = Sql::open(&PathBuf::from(format!("{DIR}/snap")), false, 12).unwrap(); let mut s = Session::default(); let t = sql.db.table("t").unwrap();
            let which = a.get(2).map(|x| x.as_str()).unwrap_or("");
            if which == "prepared-first" { let st = sql.prepare("SELECT score FROM t WHERE id = ?").unwrap(); println!("prepared, 777777 first -> {:?}", sql.execute_s(&mut s, &st, &[Value::Int(777_777)]).unwrap()); println!("again -> {:?}", sql.execute_s(&mut s, &st, &[Value::Int(777_777)]).unwrap()); println!("id 1 -> {:?}", sql.execute_s(&mut s, &st, &[Value::Int(1)]).unwrap()); println!("777777 again -> {:?}", sql.execute_s(&mut s, &st, &[Value::Int(777_777)]).unwrap()); }
            if which == "text-first" { println!("text, 777777 first -> {:?}", sql.exec_s(&mut s, "SELECT score FROM t WHERE id = ?", &[Value::Int(777_777)]).unwrap()); println!("again -> {:?}", sql.exec_s(&mut s, "SELECT score FROM t WHERE id = ?", &[Value::Int(777_777)]).unwrap()); }
            println!("mapped {} replayed {} low_water {} segments {}", rs.snap_rows, rs.replayed, rs.low_water, rs.segments);
            for id in [1i64, 2, 777_777, 1_000_000, N, N + 1] { println!("  {} -> {:?} | {}", id, sql.exec_s(&mut s, "SELECT score FROM t WHERE id = ?", &[Value::Int(id)]).unwrap(), t.debug_lookup(id)); }
            println!("  count {:?}", sql.exec_s(&mut s, "SELECT count(*) FROM t", &[]).unwrap()); return; }
        Some("build") => { let dir = PathBuf::from(format!("{DIR}/snap")); let sql = Sql::create(&dir, false); let mut s = Session::default(); sql.exec_s(&mut s, CREATE, &[]).unwrap(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let mut rng = Rng(7);
            for id in 1..=N { if id % 1000 == 1 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &row(id, &mut rng)).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } }
            let st = sql.snapshot(); println!("built: {} rows, {:.0} MB", st.rows, st.bytes as f64 / 1e6); return; }
        _ => {}
    }
    println!("=== E25: restart and memory rematch, 2M rows (E22: SQLite 7 MB / ~1 ms restart; ours 424 MB / 0.177 s) ===");
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{DB}{suf}")).ok(); }
    { let c = Connection::open(DB).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap(); c.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL NOT NULL, country INTEGER NOT NULL, age INTEGER NOT NULL, name TEXT NOT NULL, payload BLOB NOT NULL); CREATE UNIQUE INDEX t_name ON t(name);").unwrap();
      let mut rng = Rng(7); let tx = c.unchecked_transaction().unwrap(); { let mut st = tx.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); for id in 1..=N { let r = row(id, &mut rng); st.execute(params![id, match r[1] { Value::Float(f) => f, _ => 0.0 }, id % 20, 18 + id % 60, format!("u{id}@example.com"), &[7u8; 64][..]]).unwrap(); } } tx.commit().unwrap(); c.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)").unwrap(); }
    { let dir = PathBuf::from(format!("{DIR}/snap")); let sql = Sql::create(&dir, false); let mut s = Session::default(); sql.exec_s(&mut s, CREATE, &[]).unwrap(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let mut rng = Rng(7);
      for id in 1..=N { if id % 1000 == 1 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &row(id, &mut rng)).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } }
      let st = sql.snapshot(); println!("snapshot: {} rows, {:.0} MB, {:.3}s (sqlite file {:.0} MB)", st.rows, st.bytes as f64 / 1e6, st.secs, fs::metadata(DB).unwrap().len() as f64 / 1e6); drop(ins); drop(sql); }
    println!("-- warm page cache --"); println!("{}", child(&["sqlite", "warm"])); println!("{}", child(&["ours", "warm"]));
    println!("-- cold page cache (files evicted first) --"); println!("{}", child(&["sqlite", "cold"])); println!("{}", child(&["ours", "cold"]));
    println!("-- cold, inside a cgroup with a hard memory cap --");
    for mb in [64u32, 32, 16] { println!("{}", limited(mb, &["sqlite", "cold"])); println!("{}", limited(mb, &["ours", "cold"])); }
    println!("-- writes against a mapped table (page cache warmed first; runs last because it changes the data) --"); println!("{}", child(&["ours-writes"]));
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{DB}{suf}")).ok(); } let _ = fs::remove_dir_all(DIR);
}
