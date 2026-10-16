//! Experiment 40: what a TCP server in front of the engine costs. Same owner process and the same binary protocol as
//! the Unix socket (E21) and the shared-memory ring (E23), plus a TCP listener on loopback. Point reads show the pure
//! transport cost; durable inserts show how much of it hides behind fsync. References: in-process ours and SQLite.
use dblab::net::{Client, Server};
use dblab::sql::{Session, Sql};
use dblab::table::Value;
use dblab::util::Rng;
use rusqlite::{params, Connection};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Barrier};
use std::time::Instant;

const N: i64 = 1_000_000; const SOCK: &str = "data/nocow/e40.sock"; const TCP: &str = "127.0.0.1:7781"; const TOKEN: &str = "e40-lab-token"; const SQLITE: &str = "data/nocow/e40.sqlite";
const SEL: &str = "SELECT score FROM t WHERE id = ?"; const INS: &str = "INSERT INTO t VALUES (?,?,?,?,?,?)";
fn row(id: i64) -> Vec<Value> { vec![Value::Int(id), Value::Float(id as f64), Value::Int(id % 20), Value::Int(18 + id % 60), Value::Text(format!("u{id}@example.com")), Value::Bytes(vec![7; 64])] }
fn pr(label: &str, n: usize, el: f64) { println!("{:<80} {:>9.2} us/op  {:>9.0} ops/s", label, el * 1e6 / n as f64, n as f64 / el); }
fn ping(host: &str) -> String { let out = Command::new("ping").args(["-c", "20", "-i", "0.2", "-q", host]).output().map(|o| String::from_utf8_lossy(&o.stdout).into_owned()).unwrap_or_default(); out.lines().find(|l| l.starts_with("rtt")).map(|l| l.to_string()).unwrap_or_else(|| "ping unavailable".into()) }

#[derive(Clone, Copy)] enum T { Tcp, Sock, Shm }
impl T {
    fn name(self) -> &'static str { match self { T::Tcp => "TCP loopback", T::Sock => "unix socket", T::Shm => "shared-memory ring" } }
    fn connect(self) -> Client { match self { T::Tcp => Client::connect_tcp(TCP, TOKEN).unwrap(), T::Sock => Client::connect(&PathBuf::from(SOCK)).unwrap(), T::Shm => Client::connect_shm(&PathBuf::from(SOCK), &PathBuf::from("/dev/shm")).unwrap() } }
}

fn client_main() {
    for (ti, t) in [T::Tcp, T::Sock, T::Shm].into_iter().enumerate() {
        let mut c = t.connect(); let mut rng = Rng(5 + ti as u64);
        let t0 = Instant::now(); let mut s = 0.0; for _ in 0..200_000 { let id = (rng.next() % N as u64) as i64 + 1; if let Value::Float(f) = c.exec(SEL, &[Value::Int(id)]).unwrap()[0][0] { s += f; } }
        pr(&format!("[2nd process, {}] point read, one connection (check {s:.0})", t.name()), 200_000, t0.elapsed().as_secs_f64());
        let t0 = Instant::now(); for _ in 0..2000 { let stmts: Vec<(&str, Vec<Value>)> = (0..100).map(|_| (SEL, vec![Value::Int((rng.next() % N as u64) as i64 + 1)])).collect(); let rs = c.exec_batch(&stmts).unwrap(); assert_eq!(rs.len(), 100); }
        pr(&format!("[2nd process, {}] point read, 100 statements per round trip", t.name()), 200_000, t0.elapsed().as_secs_f64());
        let base = 10_000_000 + ti as i64 * 1_000_000;
        let t0 = Instant::now(); for i in 0..2000 { c.exec(INS, &row(base + i)).unwrap(); }
        pr(&format!("[2nd process, {}] insert, durable, one connection", t.name()), 2000, t0.elapsed().as_secs_f64());
        let threads = 8usize;
        let b = Arc::new(Barrier::new(threads + 1)); let per = 100_000;
        let hs: Vec<_> = (0..threads).map(|k| { let b = b.clone(); std::thread::spawn(move || { let mut c = t.connect(); let mut rng = Rng(100 + k as u64); b.wait(); let mut s = 0.0; for _ in 0..per { let id = (rng.next() % N as u64) as i64 + 1; if let Value::Float(f) = c.exec(SEL, &[Value::Int(id)]).unwrap()[0][0] { s += f; } } s }) }).collect();
        b.wait(); let t0 = Instant::now(); let _: f64 = hs.into_iter().map(|h| h.join().unwrap()).sum(); pr(&format!("[2nd process, {}] point reads, {threads} connections", t.name()), threads * per, t0.elapsed().as_secs_f64());
        let b = Arc::new(Barrier::new(threads + 1)); let per = 5_000;
        let hs: Vec<_> = (0..threads).map(|k| { let b = b.clone(); std::thread::spawn(move || { let mut c = t.connect(); b.wait(); for i in 0..per { c.exec(INS, &row(base + 100_000 + k as i64 * 10_000 + i as i64)).unwrap(); } }) }).collect();
        b.wait(); let t0 = Instant::now(); for h in hs { h.join().unwrap(); } pr(&format!("[2nd process, {}] inserts, durable, {threads} connections", t.name()), threads * per, t0.elapsed().as_secs_f64());
    }
    println!("CLIENT_DONE"); std::io::stdout().flush().unwrap();
}

fn main() {
    if std::env::args().nth(1).as_deref() == Some("client") { return client_main(); }
    println!("=== E40: a TCP server in front of the engine (1M rows; owner durable; client in a second process) ===");
    println!("network round trips (ping): loopback: {}", ping("127.0.0.1"));
    let gw = Command::new("sh").args(["-c", "ip route | awk '/^default/{print $3; exit}'"]).output().map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string()).unwrap_or_default();
    if !gw.is_empty() { println!("network round trips (ping): this machine's LAN gateway {gw}: {}", ping(&gw)); }

    // SQLite in-process reference
    let _ = fs::remove_file(SQLITE); let _ = fs::remove_file(format!("{SQLITE}-wal")); let _ = fs::remove_file(format!("{SQLITE}-shm"));
    { let c = Connection::open(SQLITE).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF; PRAGMA cache_size=-262144;").unwrap();
      c.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, age INT, name TEXT UNIQUE, payload BLOB)").unwrap();
      c.execute_batch("BEGIN").unwrap(); { let mut st = c.prepare(INS).unwrap(); for id in 1..=N { st.execute(params![id, id as f64, id % 20, 18 + id % 60, format!("u{id}@example.com"), &[7u8; 64][..]]).unwrap(); } } c.execute_batch("COMMIT").unwrap();
      let mut st = c.prepare(SEL).unwrap(); let mut rng = Rng(5); let t0 = Instant::now(); let mut s = 0.0; for _ in 0..200_000 { let id = (rng.next() % N as u64) as i64 + 1; s += st.query_row(params![id], |r| r.get::<_, f64>(0)).unwrap(); }
      pr(&format!("[SQLite, in-process] point read (check {s:.0})"), 200_000, t0.elapsed().as_secs_f64());
      c.execute_batch("PRAGMA synchronous=FULL").unwrap(); let mut st = c.prepare(INS).unwrap(); let t0 = Instant::now(); for i in 0..2000 { let id = 50_000_000 + i; st.execute(params![id, id as f64, id % 20, 18 + id % 60, format!("u{id}@example.com"), &[7u8; 64][..]]).unwrap(); }
      pr("[SQLite, in-process] insert, durable (synchronous=FULL), one connection", 2000, t0.elapsed().as_secs_f64()); }

    // ours, in-process
    let dir = PathBuf::from("data/nocow/e40dir"); let sql = Arc::new(Sql::create(&dir, true)); let mut app = Session::default();
    sql.exec_s(&mut app, "CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, age INT, name VARCHAR(32) UNIQUE, payload BLOB(64))", &[]).unwrap();
    let ins = sql.prepare(INS).unwrap();
    for id in 1..=N { if id % 1000 == 1 { sql.exec_s(&mut app, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut app, &ins, &row(id)).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut app, "COMMIT", &[]).unwrap(); } }
    let sel = sql.prepare(SEL).unwrap(); let mut rng = Rng(5); let t0 = Instant::now(); let mut s = 0.0; for _ in 0..200_000 { let id = (rng.next() % N as u64) as i64 + 1; if let Value::Float(f) = sql.execute_s(&mut app, &sel, &[Value::Int(id)]).unwrap()[0][0] { s += f; } }
    pr(&format!("[ours, in-process] point read (check {s:.0})"), 200_000, t0.elapsed().as_secs_f64());
    let t0 = Instant::now(); for i in 0..2000 { sql.execute_s(&mut app, &ins, &row(50_000_000 + i)).unwrap(); }
    pr("[ours, in-process] insert, durable, one thread", 2000, t0.elapsed().as_secs_f64());
    { let threads = 8usize; let b = Arc::new(Barrier::new(threads + 1)); let per = 5_000;
      let hs: Vec<_> = (0..threads).map(|k| { let b = b.clone(); let sql = sql.clone(); std::thread::spawn(move || { let mut sess = Session::default(); let ins = sql.prepare(INS).unwrap(); b.wait(); for i in 0..per { sql.execute_s(&mut sess, &ins, &row(60_000_000 + k as i64 * 10_000 + i as i64)).unwrap(); } }) }).collect();
      b.wait(); let t0 = Instant::now(); for h in hs { h.join().unwrap(); } pr("[ours, in-process] inserts, durable, 8 threads", threads * per, t0.elapsed().as_secs_f64()); }

    // second process over the three transports
    let _unix = Server::start(sql.clone(), &PathBuf::from(SOCK)).unwrap(); let _tcp = Server::start_tcp(sql.clone(), TCP, TOKEN).unwrap();
    { let bad = Client::connect_tcp(TCP, "wrong"); let shm = Client::connect_shm(&PathBuf::from(SOCK), &PathBuf::from("/tmp")); println!("wrong token over TCP: {} | ring outside /dev/shm: {}", if bad.is_err() { "refused" } else { "ACCEPTED (bug)" }, if shm.is_err() { "refused" } else { "ACCEPTED (bug)" }); }
    let mut child = Command::new(std::env::current_exe().unwrap()).arg("client").stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap();
    let rd = BufReader::new(child.stdout.take().unwrap()); for line in rd.lines() { let l = line.unwrap(); if l == "CLIENT_DONE" { break; } println!("{l}"); } child.wait().unwrap();
    let r = sql.exec_s(&mut app, "SELECT count(*) FROM t", &[]).unwrap(); println!("owner sees count(*) = {:?} (1,000,000 + 2,000 + 40,000 in-process + 3 x 42,000 from the client = 1,168,000)", r[0][0]);
    drop(sel); drop(ins); drop(sql); let _ = fs::remove_dir_all(&dir); fs::remove_file(SOCK).ok(); let _ = fs::remove_file(SQLITE); let _ = fs::remove_file(format!("{SQLITE}-wal")); let _ = fs::remove_file(format!("{SQLITE}-shm"));
}
