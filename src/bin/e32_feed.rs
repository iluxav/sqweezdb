//! Experiment 32: a change feed. Ours: subscribe to the log, decoded, resumable by sequence number. SQLite has no such
//! thing; the do-it-yourself version is AFTER INSERT/UPDATE/DELETE triggers writing an audit table that a consumer polls.
//! Rows carry the writer's clock so a consumer can measure the delay from commit to visibility.
use sqweezdb::sql::{ChangeOp, Session, Sql};
use sqweezdb::table::Value;
use sqweezdb::util::Rng;
use rusqlite::{params, Connection};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const N: i64 = 1_000_000;
const DB: &str = "data/nocow/e32.db"; const DIR: &str = "data/nocow/e32dir";
fn now_us(t0: &Instant) -> i64 { t0.elapsed().as_micros() as i64 }
fn pct(v: &mut Vec<f64>, p: f64) -> f64 { if v.is_empty() { return 0.0; } v.sort_by(|a, b| a.partial_cmp(b).unwrap()); v[((v.len() as f64 - 1.0) * p) as usize] }
fn us(d: Duration, n: i64) -> f64 { d.as_secs_f64() * 1e6 / n as f64 }

fn main() {
    println!("=== E32: change feed (1M rows, then 100k inserts + 20k updates + 10k deletes while a consumer listens) ===");
    let t0 = Instant::now();
    // ---- SQLite: plain, then with audit triggers ----
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{DB}{suf}")).ok(); }
    let c = Connection::open(DB).unwrap(); c.busy_timeout(Duration::from_secs(600)).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap();
    c.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL, country INTEGER, ts INTEGER);").unwrap();
    let load = |c: &Connection| { let t = Instant::now(); for chunk in (1..=N).step_by(1000) { let tx = c.unchecked_transaction().unwrap(); { let mut st = tx.prepare_cached("INSERT INTO t VALUES (?,?,?,?)").unwrap(); for id in chunk..chunk + 1000 { st.execute(params![id, (id % 1000) as f64, id % 20, 0i64]).unwrap(); } } tx.commit().unwrap(); } t.elapsed() };
    let plain = load(&c);
    c.execute_batch("DELETE FROM t; CREATE TABLE changes(seq INTEGER PRIMARY KEY AUTOINCREMENT, tbl TEXT, op TEXT, pk INTEGER, ts INTEGER, data TEXT);
        CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN INSERT INTO changes(tbl, op, pk, ts, data) VALUES ('t', 'I', NEW.id, NEW.ts, NEW.score || ',' || NEW.country); END;
        CREATE TRIGGER t_au AFTER UPDATE ON t BEGIN INSERT INTO changes(tbl, op, pk, ts, data) VALUES ('t', 'U', NEW.id, NEW.ts, NEW.score || ',' || NEW.country); END;
        CREATE TRIGGER t_ad AFTER DELETE ON t BEGIN INSERT INTO changes(tbl, op, pk, ts, data) VALUES ('t', 'D', OLD.id, OLD.ts, NULL); END;").unwrap();
    let with_trig = load(&c);
    println!("sqlite  load 1M rows: {:.2} us/row plain, {:.2} us/row with audit triggers; changes table {:.0} MB after the load", us(plain, N), us(with_trig, N), { let n: i64 = c.query_row("SELECT count(*) FROM changes", [], |r| r.get(0)).unwrap(); n as f64 * 60.0 / 1e6 });
    // consumer polls the audit table every 1 ms
    let stop = Arc::new(AtomicBool::new(false)); let seen = Arc::new(AtomicU64::new(0)); let lags = Arc::new(std::sync::Mutex::new(Vec::<f64>::new()));
    let start_seq: i64 = c.query_row("SELECT max(seq) FROM changes", [], |r| r.get(0)).unwrap();
    let h = { let (stop, seen, lags, t0) = (stop.clone(), seen.clone(), lags.clone(), t0.clone()); std::thread::spawn(move || { let c = Connection::open(DB).unwrap(); c.busy_timeout(Duration::from_secs(600)).unwrap(); let mut last = start_seq; let mut st = c.prepare("SELECT seq, op, pk, ts FROM changes WHERE seq > ? ORDER BY seq").unwrap(); while !stop.load(Ordering::Relaxed) { let rows: Vec<(i64, String, i64, i64)> = st.query_map(params![last], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))).unwrap().map(|r| r.unwrap()).collect(); if rows.is_empty() { std::thread::sleep(Duration::from_millis(1)); continue; } let now = now_us(&t0); let mut l = lags.lock().unwrap(); for (seq, _, _, ts) in &rows { if *ts > 0 { l.push((now - ts) as f64); } last = *seq; } seen.fetch_add(rows.len() as u64, Ordering::Relaxed); } }) };
    let t = Instant::now(); { let mut st = c.prepare("INSERT INTO t VALUES (?,?,?,?)").unwrap(); for id in N + 1..=N + 100_000 { st.execute(params![id, 1.0f64, id % 20, now_us(&t0)]).unwrap(); } let mut up = c.prepare("UPDATE t SET score = 2.0, ts = ? WHERE id = ?").unwrap(); let mut rng = Rng(5); for _ in 0..20_000 { up.execute(params![now_us(&t0), (rng.next() % N as u64) as i64 + 1]).unwrap(); } let mut del = c.prepare("DELETE FROM t WHERE id = ?").unwrap(); for id in 1..=10_000i64 { del.execute(params![id]).unwrap(); } } let wt = t.elapsed();
    std::thread::sleep(Duration::from_millis(300)); stop.store(true, Ordering::Relaxed); h.join().unwrap();
    let mut l = lags.lock().unwrap().clone();
    println!("sqlite  130k single-row writes with triggers + a polling consumer: {:.2} s ({:.1} us/write); consumer saw {} changes; commit -> visible p50 {:.0} us, p99 {:.0} us", wt.as_secs_f64(), us(wt, 130_000), seen.load(Ordering::Relaxed), pct(&mut l, 0.5), pct(&mut l, 0.99));
    { let stop = Arc::new(AtomicBool::new(false)); let lags2 = Arc::new(std::sync::Mutex::new(Vec::<f64>::new())); let start_seq: i64 = c.query_row("SELECT max(seq) FROM changes", [], |r| r.get(0)).unwrap();
      let h = { let (stop, lags2, t0) = (stop.clone(), lags2.clone(), t0.clone()); std::thread::spawn(move || { let c = Connection::open(DB).unwrap(); c.busy_timeout(Duration::from_secs(600)).unwrap(); let mut last = start_seq; let mut st = c.prepare("SELECT seq, ts FROM changes WHERE seq > ? ORDER BY seq").unwrap(); while !stop.load(Ordering::Relaxed) { let rows: Vec<(i64, i64)> = st.query_map(params![last], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().map(|r| r.unwrap()).collect(); if rows.is_empty() { std::thread::sleep(Duration::from_micros(200)); continue; } let now = now_us(&t0); let mut l = lags2.lock().unwrap(); for (seq, ts) in &rows { if *ts > 0 { l.push((now - ts) as f64); } last = *seq; } } }) };
      { let mut st = c.prepare("INSERT INTO t VALUES (?,?,?,?)").unwrap(); for id in N + 400_000..N + 410_000 { st.execute(params![id, 1.0f64, id % 20, now_us(&t0)]).unwrap(); std::thread::sleep(Duration::from_micros(200)); } }
      std::thread::sleep(Duration::from_millis(100)); stop.store(true, Ordering::Relaxed); h.join().unwrap(); let mut l = lags2.lock().unwrap().clone();
      println!("sqlite  10k writes at ~5k/s with triggers + polling consumer: commit -> visible p50 {:.0} us, p99 {:.0} us", pct(&mut l, 0.5), pct(&mut l, 0.99)); }
    let t = Instant::now(); c.execute_batch("DROP TRIGGER t_ai; DROP TRIGGER t_au; DROP TRIGGER t_ad;").unwrap(); { let mut st = c.prepare("INSERT INTO t VALUES (?,?,?,?)").unwrap(); for id in N + 200_000..N + 300_000 { st.execute(params![id, 1.0f64, id % 20, 0i64]).unwrap(); } } println!("sqlite  100k single-row inserts without triggers for comparison: {:.1} us/write; audit table must be pruned by hand", us(t.elapsed(), 100_000));
    drop(c); for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{DB}{suf}")).ok(); }

    // ---- ours ----
    let dir = PathBuf::from(format!("{DIR}/db")); let sql = Arc::new(Sql::recreate(&dir, false)); let mut s = Session::default();
    sql.exec_s(&mut s, "CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, ts INT)", &[]).unwrap();
    let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?)").unwrap();
    let t = Instant::now(); for id in 1..=N { if id % 1000 == 1 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Float((id % 1000) as f64), Value::Int(id % 20), Value::Int(0)]).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } } let load_o = t.elapsed();
    println!("ours    load 1M rows: {:.2} us/row (the feed is the log itself: nothing extra is written)", us(load_o, N));
    // a consumer reads the whole load from the log: decode throughput
    let t = Instant::now(); let mut f = sql.subscribe(0); let mut n = 0u64; loop { let ch = f.poll(); if ch.is_empty() { break; } n += ch.len() as u64; } println!("ours    a new subscriber replays the 1M-row load from the log: {} changes decoded in {:.2} s ({:.2}M changes/s)", n, t.elapsed().as_secs_f64(), n as f64 / t.elapsed().as_secs_f64() / 1e6);
    let from = sql.current_seq();
    let stop = Arc::new(AtomicBool::new(false)); let seen = Arc::new(AtomicU64::new(0)); let lags = Arc::new(std::sync::Mutex::new(Vec::<f64>::new())); let ops = Arc::new(std::sync::Mutex::new([0u64; 3]));
    let h = { let (sql, stop, seen, lags, ops, t0) = (sql.clone(), stop.clone(), seen.clone(), lags.clone(), ops.clone(), t0.clone()); std::thread::spawn(move || { let mut f = sql.subscribe(from); while !stop.load(Ordering::Relaxed) { let ch = f.poll(); if ch.is_empty() { std::thread::sleep(Duration::from_micros(200)); continue; } let now = now_us(&t0); let mut l = lags.lock().unwrap(); let mut o = ops.lock().unwrap(); for c in &ch { match c.op { ChangeOp::Insert => o[0] += 1, ChangeOp::Update => o[1] += 1, ChangeOp::Delete => o[2] += 1 } if let Some(row) = &c.row { if let Value::Int(ts) = row[3] { if ts > 0 { l.push((now - ts) as f64); } } } } seen.fetch_add(ch.len() as u64, Ordering::Relaxed); } f.last_seq() }) };
    let t = Instant::now(); for id in N + 1..=N + 100_000 { sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Float(1.0), Value::Int(id % 20), Value::Int(now_us(&t0))]).unwrap(); } let up = sql.prepare("UPDATE t SET score = 2.0, ts = ? WHERE id = ?").unwrap(); let mut rng = Rng(5); for _ in 0..20_000 { sql.execute_s(&mut s, &up, &[Value::Int(now_us(&t0)), Value::Int((rng.next() % N as u64) as i64 + 1)]).unwrap(); } let del = sql.prepare("DELETE FROM t WHERE id = ?").unwrap(); for id in 1..=10_000i64 { sql.execute_s(&mut s, &del, &[Value::Int(id)]).unwrap(); } let wt = t.elapsed();
    std::thread::sleep(Duration::from_millis(300)); stop.store(true, Ordering::Relaxed); let stopped_at = h.join().unwrap();
    let mut l = lags.lock().unwrap().clone(); let o = *ops.lock().unwrap();
    println!("ours    130k single-row writes with a live subscriber: {:.2} s ({:.1} us/write); subscriber saw {} changes ({} inserts, {} updates, {} deletes); commit -> visible p50 {:.0} us, p99 {:.0} us", wt.as_secs_f64(), us(wt, 130_000), seen.load(Ordering::Relaxed), o[0], o[1], o[2], pct(&mut l, 0.5), pct(&mut l, 0.99));
    { let from = sql.current_seq(); let stop = Arc::new(AtomicBool::new(false)); let lags2 = Arc::new(std::sync::Mutex::new(Vec::<f64>::new()));
      let h = { let (sql, stop, lags2, t0) = (sql.clone(), stop.clone(), lags2.clone(), t0.clone()); std::thread::spawn(move || { let mut f = sql.subscribe(from); while !stop.load(Ordering::Relaxed) { let ch = f.poll(); if ch.is_empty() { std::thread::sleep(Duration::from_micros(50)); continue; } let now = now_us(&t0); let mut l = lags2.lock().unwrap(); for c in &ch { if let Some(row) = &c.row { if let Value::Int(ts) = row[3] { if ts > 0 { l.push((now - ts) as f64); } } } } } }) };
      for id in N + 400_000..N + 410_000 { sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Float(1.0), Value::Int(id % 20), Value::Int(now_us(&t0))]).unwrap(); std::thread::sleep(Duration::from_micros(200)); }
      std::thread::sleep(Duration::from_millis(100)); stop.store(true, Ordering::Relaxed); h.join().unwrap(); let mut l = lags2.lock().unwrap().clone();
      println!("ours    10k writes at ~5k/s with a live subscriber: commit -> visible p50 {:.0} us, p99 {:.0} us", pct(&mut l, 0.5), pct(&mut l, 0.99)); }
    let t = Instant::now(); for id in N + 500_000..N + 600_000 { sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Float(1.0), Value::Int(id % 20), Value::Int(0)]).unwrap(); } println!("ours    100k single-row inserts with nobody listening: {:.1} us/write", us(t.elapsed(), 100_000));
    // resume: a consumer that stopped at `stopped_at` comes back and gets exactly the writes it missed, in order
    let mut f = sql.subscribe(stopped_at); let mut got = vec![]; loop { let ch = f.poll(); if ch.is_empty() { break; } got.extend(ch); }
    let in_order = got.windows(2).all(|w| w[0].seq < w[1].seq); let first = got.first().map(|c| c.pk).unwrap_or(0); let last = got.last().map(|c| c.pk).unwrap_or(0);
    println!("ours    resume from sequence {stopped_at}: {} changes delivered, in order {in_order}, first pk {first}, last pk {last} (expected 110000: the 10k steady-rate inserts from {} then the 100k from {}..{})", got.len(), N + 400_000, N + 500_000, N + 599_999);
    // a schema change in the middle: one event per logical change, decoded with the right schema
    let from = sql.current_seq(); sql.exec_s(&mut s, "ALTER TABLE t ADD COLUMN flag BOOL NOT NULL DEFAULT FALSE", &[]).unwrap(); sql.exec_s(&mut s, "INSERT INTO t(id, score, country, ts) VALUES (77000001, 3.0, 1, 0)", &[]).unwrap(); sql.wait_alters().unwrap(); sql.exec_s(&mut s, "INSERT INTO t(id, score, country, ts) VALUES (77000002, 4.0, 1, 0)", &[]).unwrap();
    let mut f = sql.subscribe(from); let mut got = vec![]; loop { let ch = f.poll(); if ch.is_empty() { break; } got.extend(ch); }
    println!("ours    across an ALTER TABLE (one insert during the copy, one after): {} events for 2 inserts: {:?}", got.len(), got.iter().map(|c| format!("pk {} with {} columns", c.pk, c.row.as_ref().map(|r| r.len()).unwrap_or(0))).collect::<Vec<_>>());
    drop(ins); drop(up); drop(del); drop(sql); let _ = fs::remove_dir_all(DIR);
}
