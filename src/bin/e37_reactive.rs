//! Experiment 37: reactivity. Filtered change subscriptions with row policies, live queries by invalidation, push to a
//! second process over the socket; writers are measured with 0, 1,000 and 10,000 subscribers attached. SQLite's only
//! primitive is sqlite3_update_hook (same process, table + rowid, no filter, no policy, no cross-process).
use dblab::live::Event;
use dblab::net::{Client, Server};
use dblab::sql::{ChangeOp, Session, Sql};
use dblab::table::Value;
use dblab::util::Rng;
use rusqlite::{params, Connection};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const NU: i64 = 100_000;
const DIR: &str = "data/nocow/e37dir"; const SOCK: &str = "data/nocow/e37.sock";
fn now_us(t0: &Instant) -> i64 { t0.elapsed().as_micros() as i64 }
fn pct(v: &mut Vec<f64>, p: f64) -> f64 { if v.is_empty() { return 0.0; } v.sort_by(|a, b| a.partial_cmp(b).unwrap()); v[((v.len() as f64 - 1.0) * p) as usize] }
fn build() -> Arc<Sql> {
    let sql = Arc::new(Sql::create(&PathBuf::from(DIR), false)); let mut s = Session::default();
    for q in ["CREATE TABLE users(id INTEGER PRIMARY KEY, name VARCHAR(32))", "CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INT, country INT, amount DOUBLE, ts INT)", "CREATE INDEX orders_user ON orders(user_id)",
              "CREATE ROLE tenant", "GRANT SELECT ON orders TO tenant", "CREATE POLICY own_orders ON orders FOR SELECT TO tenant USING (user_id = SESSION.uid)", "CREATE USER u7 IN ROLE tenant WITH uid = 7"] { sql.exec_s(&mut s, q, &[]).unwrap(); }
    let ins = sql.prepare("INSERT INTO users VALUES (?,?)").unwrap(); for id in 1..=NU { if id % 1000 == 1 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Text(format!("u{id}"))]).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } }
    sql
}
/// Insert orders for random users as fast as possible for `secs`; returns (rows, rate/s). ids from `base`.
fn writer(sql: &Arc<Sql>, base: i64, secs: f64, t0: &Instant, only_user: Option<i64>) -> (u64, f64) {
    let mut s = Session::default(); let ins = sql.prepare("INSERT INTO orders VALUES (?,?,?,?,?)").unwrap(); let mut rng = Rng(base as u64 | 1); let t = Instant::now(); let mut n = 0u64; let mut id = base;
    while t.elapsed().as_secs_f64() < secs { let u = only_user.unwrap_or_else(|| (rng.next() % NU as u64) as i64 + 1); sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Int(u), Value::Int(u % 20), Value::Float((id % 1000) as f64), Value::Int(now_us(t0))]).unwrap(); id += 1; n += 1; }
    (n, n as f64 / t.elapsed().as_secs_f64())
}
fn drain(subs: &[dblab::live::Subscription], t0: &Instant, lags: &mut Vec<f64>) -> u64 { let mut n = 0; for sub in subs { while let Some(ev) = sub.try_recv() { n += 1; if let Event::Change(c) = ev { if let Some(row) = &c.row { if let Value::Int(ts) = row[4] { if ts > 0 && lags.len() < 2_000_000 { lags.push((now_us(t0) - ts) as f64); } } } } } } n }

fn main() {
    let a: Vec<String> = std::env::args().collect();
    if a.get(1).map(|s| s.as_str()) == Some("remote-sub") {
        // a second process: subscribe over the socket and report each event's age (microseconds since its row was written)
        let mut c = Client::connect(&PathBuf::from(SOCK)).unwrap(); c.subscribe("SELECT * FROM orders WHERE user_id = ?", &[Value::Int(42)]).unwrap();
        println!("ready"); std::io::stdout().flush().unwrap(); let base: i64 = a[2].parse().unwrap(); let out = std::io::stdout(); let mut o = out.lock();
        loop { match c.next_event() { Ok(row) => { if let Value::Int(ts) = row[7] { let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as i64 - base; writeln!(o, "{}", now - ts).unwrap(); o.flush().unwrap(); } } Err(_) => return }
        }
    }
    println!("=== E37: reactivity (100k users; orders inserted at full speed; subscribers filter by user) ===");
    let t0 = Instant::now(); let sql = build(); let re = sql.reactor();
    // ---- A. writer cost with 0 / 1,000 / 10,000 subscribers; fan-out and latency ----
    let (n0, r0) = writer(&sql, 1_000_000, 3.0, &t0, None); println!("A. writer alone: {:.0} inserts/s ({n0} rows)", r0);
    for nsubs in [1_000usize, 10_000] {
        let subs: Vec<_> = (0..nsubs).map(|i| re.subscribe(&Session::default(), "SELECT * FROM orders WHERE user_id = ?", &[Value::Int(i as i64 + 1)]).unwrap()).collect();
        let stop = Arc::new(AtomicBool::new(false)); let lags = Arc::new(std::sync::Mutex::new(Vec::<f64>::new())); let got = Arc::new(AtomicU64::new(0));
        let h = { let (stop, lags, got) = (stop.clone(), lags.clone(), got.clone()); let subs_ptr = &subs as *const Vec<dblab::live::Subscription> as usize; std::thread::spawn(move || { let subs = unsafe { &*(subs_ptr as *const Vec<dblab::live::Subscription>) }; let mut l = vec![]; while !stop.load(Ordering::Relaxed) { let n = drain(subs, &t0, &mut l); if n == 0 { std::thread::sleep(Duration::from_micros(100)); } else { got.fetch_add(n, Ordering::Relaxed); } } std::thread::sleep(Duration::from_millis(50)); got.fetch_add(drain(subs, &t0, &mut l), Ordering::Relaxed); *lags.lock().unwrap() = l; }) };
        let (n, r) = writer(&sql, 2_000_000 + nsubs as i64 * 1_000_000, 3.0, &t0, None); std::thread::sleep(Duration::from_millis(200)); stop.store(true, Ordering::Relaxed); h.join().unwrap();
        let mut l = lags.lock().unwrap().clone(); let expected = n as f64 * nsubs as f64 / NU as f64;
        println!("A. {nsubs:>5} filtered subscribers: writer {:.0} inserts/s ({:.0}% of alone); {} events delivered (expected ~{:.0}); commit -> subscriber p50 {:.0} us, p99 {:.0} us", r, r / r0 * 100.0, got.load(Ordering::Relaxed), expected, pct(&mut l, 0.5), pct(&mut l, 0.99));
        drop(subs);
    }
    // one subscriber, blocking receive: the true commit -> event latency without the collector's polling sleep
    { let sub = re.subscribe(&Session::default(), "SELECT * FROM orders WHERE user_id = ?", &[Value::Int(77)]).unwrap(); let mut s = Session::default(); let ins = sql.prepare("INSERT INTO orders VALUES (?,?,?,?,?)").unwrap(); let mut lags = vec![];
      for i in 0..1000i64 { let t = Instant::now(); sql.execute_s(&mut s, &ins, &[Value::Int(50_000_000 + i), Value::Int(77), Value::Int(17), Value::Float(1.0), Value::Int(0)]).unwrap(); if sub.recv_timeout(Duration::from_secs(1)).is_some() { lags.push(t.elapsed().as_secs_f64() * 1e6); } std::thread::sleep(Duration::from_micros(200)); }
      println!("A. one subscriber, blocking receive, 1000 single inserts: commit -> event p50 {:.0} us, p99 {:.0} us (the fan-out rows above include a 100 us polling sleep in the test's collector)", pct(&mut lags, 0.5), pct(&mut lags, 0.99)); }
    // hot key: 200 subscribers on the same user, writer only writes that user
    { let subs: Vec<_> = (0..200).map(|_| re.subscribe(&Session::default(), "SELECT * FROM orders WHERE user_id = ?", &[Value::Int(5)]).unwrap()).collect(); let stop = Arc::new(AtomicBool::new(false)); let got = Arc::new(AtomicU64::new(0));
      let h = { let (stop, got) = (stop.clone(), got.clone()); let subs_ptr = &subs as *const Vec<dblab::live::Subscription> as usize; std::thread::spawn(move || { let subs = unsafe { &*(subs_ptr as *const Vec<dblab::live::Subscription>) }; let mut l = vec![]; while !stop.load(Ordering::Relaxed) { let n = drain(subs, &t0, &mut l); if n == 0 { std::thread::sleep(Duration::from_micros(100)); } else { got.fetch_add(n, Ordering::Relaxed); } } std::thread::sleep(Duration::from_millis(50)); got.fetch_add(drain(subs, &t0, &mut l), Ordering::Relaxed); }) };
      let (n, r) = writer(&sql, 30_000_000, 2.0, &t0, Some(5)); std::thread::sleep(Duration::from_millis(300)); stop.store(true, Ordering::Relaxed); h.join().unwrap();
      let overflow = subs.iter().filter(|s| s.q.closed.load(Ordering::Relaxed)).count();
      println!("A. 200 subscribers on one hot user, writer {:.0} inserts/s of that user: {} events delivered of {} ({} subscribers overflowed and were dropped)", r, got.load(Ordering::Relaxed), n * 200, overflow); drop(subs); }
    // ---- B. row policies on subscriptions ----
    { let mut tenant = Session::default(); sql.login(&mut tenant, "u7").unwrap(); let sub = re.subscribe(&tenant, "SELECT * FROM orders", &[]).unwrap();
      let mut s = Session::default(); let ins = sql.prepare("INSERT INTO orders VALUES (?,?,?,?,?)").unwrap(); let mut mine = 0; let mut ids7 = vec![]; let mut others = vec![];
      for i in 0..10_000i64 { let u = if i % 50 == 0 { 7 } else { (i % NU) + 1 }; if u == 7 { mine += 1; ids7.push(60_000_000 + i); } else { others.push(60_000_000 + i); } sql.execute_s(&mut s, &ins, &[Value::Int(60_000_000 + i), Value::Int(u), Value::Int(u % 20), Value::Float(1.0), Value::Int(0)]).unwrap(); }
      for id in ids7.iter().take(5).chain(others.iter().take(5)) { sql.exec_s(&mut s, "DELETE FROM orders WHERE id = ?", &[Value::Int(*id)]).unwrap(); }
      std::thread::sleep(Duration::from_millis(100)); let mut ins_n = 0; let mut del_n = 0; let mut leaked = 0; while let Some(ev) = sub.try_recv() { if let Event::Change(c) = ev { let uid = c.row.as_ref().map(|r| r[1].clone()); if uid != Some(Value::Int(7)) { leaked += 1; } match c.op { ChangeOp::Insert => ins_n += 1, ChangeOp::Delete => del_n += 1, _ => {} } } }
      println!("B. tenant u7 subscribed to all of orders with policy user_id = SESSION.uid: 10,000 inserts ({mine} for user 7) and 10 deletes (5 of user 7) -> received {ins_n} inserts, {del_n} deletes, {leaked} events of other users"); }
    // ---- C. live queries ----
    { let mut s = Session::default(); let ins = sql.prepare("INSERT INTO orders VALUES (?,?,?,?,?)").unwrap();
      let (live, first) = re.watch(&Session::default(), "SELECT count(*), sum(amount) FROM orders WHERE user_id = ?", &[Value::Int(7)]).unwrap();
      let mut lags = vec![]; let mut last = first;
      for i in 0..100i64 { let t = Instant::now(); sql.execute_s(&mut s, &ins, &[Value::Int(70_000_000 + i), Value::Int(7), Value::Int(7), Value::Float(10.0), Value::Int(0)]).unwrap(); match live.recv_timeout(Duration::from_secs(2)) { Some(Event::Live { rows, .. }) => { lags.push(t.elapsed().as_secs_f64() * 1e3); last = rows; } _ => { lags.push(2000.0); } } }
      println!("C. live aggregate for one user, 100 inserts one by one: result now {:?}; insert -> new result p50 {:.1} ms, p99 {:.1} ms (debounce {} ms)", last[0], pct(&mut lags, 0.5), pct(&mut lags, 0.99), re.debounce.as_millis());
      let (top, first) = re.watch(&Session::default(), "SELECT id, amount FROM orders WHERE user_id = ? ORDER BY amount DESC LIMIT 3", &[Value::Int(7)]).unwrap();
      sql.execute_s(&mut s, &ins, &[Value::Int(71_000_000), Value::Int(7), Value::Int(7), Value::Float(999.0), Value::Int(0)]).unwrap();
      let ev = top.recv_timeout(Duration::from_secs(2)); let (added, removed) = match &ev { Some(Event::Live { added, removed, .. }) => (added.len(), removed.len()), _ => (99, 99) };
      sql.exec_s(&mut s, "INSERT INTO orders VALUES (71000001, 8, 8, 5000.0, 0)", &[]).unwrap(); let other = top.recv_timeout(Duration::from_millis(300)).is_some();
      println!("C. live top-3 for user 7 (was {} rows): a new top order -> event with {added} added, {removed} removed; an order for another user -> event: {other}", first.len());
      let (agg, _) = re.watch(&Session::default(), "SELECT country, count(*), sum(amount) FROM orders GROUP BY country", &[]).unwrap();
      let (reruns0, us0) = (re.reruns.load(Ordering::Relaxed), re.rerun_us.load(Ordering::Relaxed)); let t = Instant::now(); let (n, r) = writer(&sql, 80_000_000, 3.0, &t0, None); std::thread::sleep(Duration::from_millis(100)); let wall = t.elapsed().as_secs_f64();
      let (reruns, us) = (re.reruns.load(Ordering::Relaxed) - reruns0, re.rerun_us.load(Ordering::Relaxed) - us0); let mut updates = 0; while agg.try_recv().is_some() { updates += 1; }
      println!("C. live GROUP BY over the whole table ({} rows) under a {:.0} inserts/s burst: {reruns} re-runs in {:.1} s ({:.1}/s, {:.1} ms each, {:.0}% of one core), {updates} result updates delivered; writer unaffected ({n} rows)", n, r, wall, reruns as f64 / wall, us as f64 / 1e3 / reruns.max(1) as f64, us as f64 / 1e4 / wall, ); }
    // ---- D. a second process over the socket ----
    { let _ = std::fs::remove_file(SOCK); let _srv = Server::start(sql.clone(), &PathBuf::from(SOCK)).unwrap(); std::thread::sleep(Duration::from_millis(100));
      let epoch_base = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as i64 - now_us(&t0);
      let mut child = Command::new(std::env::current_exe().unwrap()).args(["remote-sub", &epoch_base.to_string()]).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap();
      let out = child.stdout.take().unwrap(); let mut lines = BufReader::new(out).lines(); while let Some(Ok(l)) = lines.next() { if l == "ready" { break; } }
      let mut s = Session::default(); let ins = sql.prepare("INSERT INTO orders VALUES (?,?,?,?,?)").unwrap();
      for i in 0..1000i64 { sql.execute_s(&mut s, &ins, &[Value::Int(90_000_000 + i), Value::Int(42), Value::Int(2), Value::Float(1.0), Value::Int(now_us(&t0))]).unwrap(); std::thread::sleep(Duration::from_micros(500)); }
      let mut lags = vec![]; let t = Instant::now(); while lags.len() < 1000 && t.elapsed() < Duration::from_secs(5) { if let Some(Ok(l)) = lines.next() { if let Ok(v) = l.parse::<f64>() { lags.push(v); } } else { break; } }
      child.kill().ok(); child.wait().ok();
      println!("D. second process subscribed over the socket to user 42: {} of 1000 events received; commit -> event in the other process p50 {:.0} us, p99 {:.0} us", lags.len(), pct(&mut lags, 0.5), pct(&mut lags, 0.99)); }
    // ---- E. SQLite's update hook ----
    { let p = "data/nocow/e37.db"; for suf in ["", "-wal", "-shm"] { let _ = std::fs::remove_file(format!("{p}{suf}")); }
      let c = Connection::open(p).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INTEGER, country INTEGER, amount REAL, ts INTEGER);").unwrap();
      let run = |c: &Connection, base: i64| { let mut st = c.prepare("INSERT INTO orders VALUES (?,?,?,?,?)").unwrap(); let t = Instant::now(); for i in 0..100_000i64 { st.execute(params![base + i, i % NU + 1, i % 20, 1.0f64, 0i64]).unwrap(); } t.elapsed().as_secs_f64() * 1e6 / 100_000.0 };
      let plain = run(&c, 1); let seen = Arc::new(AtomicU64::new(0)); let seen2 = seen.clone(); c.update_hook(Some(move |_action: rusqlite::hooks::Action, _db: &str, _table: &str, _rowid: i64| { seen2.fetch_add(1, Ordering::Relaxed); })); let hooked = run(&c, 10_000_000);
      println!("E. sqlite3_update_hook: {:.1} us per insert without, {:.1} us with the hook ({} callbacks); it reports table + rowid only, in the writer's thread, same process; no filter, no policy, no live query, no other process", plain, hooked, seen.load(Ordering::Relaxed));
      drop(c); for suf in ["", "-wal", "-shm"] { let _ = std::fs::remove_file(format!("{p}{suf}")); } }
    let _ = std::fs::remove_file(SOCK); drop(re); drop(sql); let _ = std::fs::remove_dir_all(DIR);
}
