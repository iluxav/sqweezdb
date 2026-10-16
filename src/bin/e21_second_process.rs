//! Experiment 21: a second program reaching the database. Ours: owner process + Unix socket; SQLite: two processes on one file.
use sqweezdb::net::{Client, Server};
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::Value;
use sqweezdb::util::Rng;
use rusqlite::{params, Connection};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Barrier};
use std::time::Instant;

const N: i64 = 1_000_000;
const SOCK: &str = "data/nocow/e21.sock";
const DB: &str = "data/nocow/e21.db";
const CREATE: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, age INT, name VARCHAR(32) UNIQUE, payload BLOB(64))";
fn row(id: i64) -> Vec<Value> { vec![Value::Int(id), Value::Float(id as f64), Value::Int(id % 20), Value::Int(18 + id % 60), Value::Text(format!("u{id}@example.com")), Value::Bytes(vec![7; 64])] }
fn pr(label: &str, n: usize, el: f64) { println!("{:<72} {:>9.2} us/op  {:>9.0} ops/s", label, el * 1e6 / n as f64, n as f64 / el); }

fn client_main(durable: bool) {
    let sock = PathBuf::from(SOCK);
    let mut c = Client::connect(&sock).unwrap();
    let tag = if durable { "ours, 2nd process, durable owner" } else { "ours, 2nd process" };
    if !durable {
        let mut rng = Rng(5);
        let t = Instant::now(); let mut s = 0.0; for _ in 0..200_000 { let id = (rng.next() % N as u64) as i64 + 1; if let Value::Float(f) = c.exec("SELECT score FROM t WHERE id = ?", &[Value::Int(id)]).unwrap()[0][0] { s += f; } }
        pr(&format!("[{tag}] point read over the socket (check {s:.0})"), 200_000, t.elapsed().as_secs_f64());
        let t = Instant::now(); let mut s = 0.0; for _ in 0..2_000 { let stmts: Vec<(&str, Vec<Value>)> = (0..100).map(|_| ("SELECT score FROM t WHERE id = ?", vec![Value::Int((rng.next() % N as u64) as i64 + 1)])).collect(); for r in c.exec_batch(&stmts).unwrap() { if let Value::Float(f) = r.unwrap()[0][0] { s += f; } } }
        pr(&format!("[{tag}] point reads batched 100 per round trip (check {s:.0})"), 200_000, t.elapsed().as_secs_f64());
        let t = Instant::now(); for i in 0..50_000 { c.exec("INSERT INTO t VALUES (?,?,?,?,?,?)", &row(N + 1 + i)).unwrap(); }
        pr(&format!("[{tag}] insert over the socket, not durable"), 50_000, t.elapsed().as_secs_f64());
        let r = c.exec("SELECT count(*) FROM t", &[]).unwrap(); println!("[{tag}] count(*) now {:?}", r[0][0]);
        for threads in [8usize] {
            let b = Arc::new(Barrier::new(threads + 1)); let per = 100_000;
            let hs: Vec<_> = (0..threads).map(|k| { let b = b.clone(); let sock = sock.clone(); std::thread::spawn(move || { let mut c = Client::connect(&sock).unwrap(); let mut rng = Rng(100 + k as u64); b.wait(); let mut s = 0.0; for _ in 0..per { let id = (rng.next() % N as u64) as i64 + 1; if let Value::Float(f) = c.exec("SELECT score FROM t WHERE id = ?", &[Value::Int(id)]).unwrap()[0][0] { s += f; } } s }) }).collect();
            b.wait(); let t = Instant::now(); let _: f64 = hs.into_iter().map(|h| h.join().unwrap()).sum(); pr(&format!("[{tag}] point reads, {threads} connections in the 2nd process"), threads * per, t.elapsed().as_secs_f64());
        }
        c.login("alice").unwrap(); let r = c.exec("SELECT count(*) FROM t", &[]).unwrap(); println!("[{tag}] after LOGIN alice (policy country = 7): count(*) = {:?}", r[0][0]);
        println!("[{tag}] alice INSERT with country 8: {:?}", c.exec("INSERT INTO t VALUES (?,?,?,?,?,?)", &[Value::Int(N + 99_000), Value::Float(1.0), Value::Int(8), Value::Int(30), Value::Text("x@x".into()), Value::Bytes(vec![])]).err());
    } else {
        for threads in [1usize, 8, 64] {
            let b = Arc::new(Barrier::new(threads + 1)); let per = if threads == 1 { 1000 } else { 2000 };
            let hs: Vec<_> = (0..threads).map(|k| { let b = b.clone(); let sock = sock.clone(); std::thread::spawn(move || { let mut c = Client::connect(&sock).unwrap(); b.wait(); for i in 0..per { c.exec("INSERT INTO t VALUES (?,?,?,?,?,?)", &row(10_000_000 + threads as i64 * 1_000_000 + k as i64 * 10_000 + i as i64)).unwrap(); } }) }).collect();
            b.wait(); let t = Instant::now(); for h in hs { h.join().unwrap(); } pr(&format!("[{tag}] durable inserts (fsync), {threads} connections in the 2nd process"), threads * per, t.elapsed().as_secs_f64());
        }
    }
    println!("CLIENT_DONE"); std::io::stdout().flush().unwrap();
}

fn sqlite_client_main() {
    let c = Connection::open(DB).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA cache_size=-262144; PRAGMA mmap_size=1073741824; PRAGMA busy_timeout=60000;").unwrap();
    let mut rng = Rng(5);
    { let mut st = c.prepare("SELECT score FROM t WHERE id = ?").unwrap(); let t = Instant::now(); let mut s = 0.0; for _ in 0..200_000 { let id = (rng.next() % N as u64) as i64 + 1; s += st.query_row(params![id], |r| r.get::<_, f64>(0)).unwrap(); } pr(&format!("[sqlite, 2nd process] point read on the shared file (check {s:.0})"), 200_000, t.elapsed().as_secs_f64()); }
    c.execute_batch("PRAGMA synchronous=NORMAL").unwrap();
    { let mut st = c.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let t = Instant::now(); for i in 0..50_000 { let id = N + 1 + i; st.execute(params![id, id as f64, id % 20, 18 + id % 60, format!("u{id}@example.com"), &[7u8; 64][..]]).unwrap(); } pr("[sqlite, 2nd process] insert, sync=NORMAL (not durable)", 50_000, t.elapsed().as_secs_f64()); }
    { let threads = 8; let per = 100_000; let b = Arc::new(Barrier::new(threads + 1));
      let hs: Vec<_> = (0..threads).map(|k| { let b = b.clone(); std::thread::spawn(move || { let c = Connection::open(DB).unwrap(); c.execute_batch("PRAGMA cache_size=-262144; PRAGMA mmap_size=1073741824;").unwrap(); let mut st = c.prepare("SELECT score FROM t WHERE id = ?").unwrap(); let mut rng = Rng(100 + k as u64); b.wait(); let mut s = 0.0; for _ in 0..per { let id = (rng.next() % N as u64) as i64 + 1; s += st.query_row(params![id], |r| r.get::<_, f64>(0)).unwrap(); } s }) }).collect();
      b.wait(); let t = Instant::now(); let _: f64 = hs.into_iter().map(|h| h.join().unwrap()).sum(); pr("[sqlite, 2nd process] point reads, 8 connections in the 2nd process", threads * per, t.elapsed().as_secs_f64()); }
    for threads in [1usize, 8, 64] {
        let b = Arc::new(Barrier::new(threads + 1)); let per = if threads == 1 { 1000 } else { 2000 };
        let hs: Vec<_> = (0..threads).map(|k| { let b = b.clone(); std::thread::spawn(move || { let c = Connection::open(DB).unwrap(); c.execute_batch("PRAGMA synchronous=FULL; PRAGMA busy_timeout=120000;").unwrap(); let mut st = c.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); b.wait(); for i in 0..per { let id = 10_000_000 + threads as i64 * 1_000_000 + k as i64 * 10_000 + i as i64; st.execute(params![id, id as f64, id % 20, 18 + id % 60, format!("u{id}@example.com"), &[7u8; 64][..]]).unwrap(); } }) }).collect();
        b.wait(); let t = Instant::now(); for h in hs { h.join().unwrap(); } pr(&format!("[sqlite, 2nd process] durable inserts (FULL), {threads} connections in the 2nd process"), threads * per, t.elapsed().as_secs_f64());
    }
    println!("CLIENT_DONE"); std::io::stdout().flush().unwrap();
}

fn run_child(mode: &str) { let mut child = Command::new(std::env::current_exe().unwrap()).arg(mode).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap(); let rd = BufReader::new(child.stdout.take().unwrap()); for line in rd.lines() { let l = line.unwrap(); if l == "CLIENT_DONE" { break; } println!("{l}"); } child.wait().unwrap(); }

fn main() {
    match std::env::args().nth(1).as_deref() { Some("client") => return client_main(false), Some("client-durable") => return client_main(true), Some("sqlite-client") => return sqlite_client_main(), _ => {} }
    println!("=== E21: a second program reaching the database ===");
    for durable in [false, true] {
        let dir = PathBuf::from("data/nocow/e21dir"); let sql = Arc::new(Sql::recreate(&dir, durable)); let mut app = Session::default();
        sql.exec_s(&mut app, CREATE, &[]).unwrap();
        let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap();
        for id in 1..=N { if id % 1000 == 1 { sql.exec_s(&mut app, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut app, &ins, &row(id)).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut app, "COMMIT", &[]).unwrap(); } }
        for st in ["CREATE ROLE tenant", "GRANT SELECT, INSERT ON t TO tenant", "CREATE POLICY p ON t FOR ALL TO tenant USING (country = SESSION.country)", "CREATE USER alice IN ROLE tenant WITH country = 7"] { sql.exec_s(&mut app, st, &[]).unwrap(); }
        let _server = Server::start(sql.clone(), &PathBuf::from(SOCK)).unwrap();
        if !durable { let mut rng = Rng(5); let t = Instant::now(); let mut s = 0.0; for _ in 0..200_000 { let id = (rng.next() % N as u64) as i64 + 1; if let Value::Float(f) = sql.exec_s(&mut app, "SELECT score FROM t WHERE id = ?", &[Value::Int(id)]).unwrap()[0][0] { s += f; } } pr(&format!("[ours, in the owner process] point read (check {s:.0})"), 200_000, t.elapsed().as_secs_f64()); }
        run_child(if durable { "client-durable" } else { "client" });
        drop(ins); drop(sql); let _ = fs::remove_dir_all(&dir); fs::remove_file(SOCK).ok();
    }
    println!("--- SQLite: the second process opens the same file ---");
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{DB}{suf}")).ok(); }
    { let c = Connection::open(DB).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap(); c.execute_batch(CREATE).unwrap();
      let tx = c.unchecked_transaction().unwrap(); { let mut st = tx.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); for id in 1..=N { st.execute(params![id, id as f64, id % 20, 18 + id % 60, format!("u{id}@example.com"), &[7u8; 64][..]]).unwrap(); } } tx.commit().unwrap(); }
    run_child("sqlite-client");
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{DB}{suf}")).ok(); }
}
