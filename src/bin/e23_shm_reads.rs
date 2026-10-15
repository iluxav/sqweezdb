//! Experiment 23 (H34): second-process reads over a shared-memory ring instead of a socket.
use dblab::net::{Client, Server};
use dblab::sql::{Session, Sql};
use dblab::table::Value;
use dblab::util::Rng;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, Barrier};
use std::time::Instant;

const N: i64 = 1_000_000; const SOCK: &str = "data/nocow/e23.sock";
fn row(id: i64) -> Vec<Value> { vec![Value::Int(id), Value::Float(id as f64), Value::Int(id % 20), Value::Int(18 + id % 60), Value::Text(format!("u{id}@example.com")), Value::Bytes(vec![7; 64])] }
fn pr(label: &str, n: usize, el: f64) { println!("{:<72} {:>9.2} us/op  {:>9.0} ops/s", label, el * 1e6 / n as f64, n as f64 / el); }

fn client_main() {
    let sock = PathBuf::from(SOCK); let shm = PathBuf::from("/dev/shm");
    let mut c = Client::connect_shm(&sock, &shm).unwrap();
    let mut rng = Rng(5);
    let t = Instant::now(); let mut s = 0.0; for _ in 0..200_000 { let id = (rng.next() % N as u64) as i64 + 1; if let Value::Float(f) = c.exec("SELECT score FROM t WHERE id = ?", &[Value::Int(id)]).unwrap()[0][0] { s += f; } }
    pr(&format!("[ours, 2nd process, shared-memory ring] point read (check {s:.0})"), 200_000, t.elapsed().as_secs_f64());
    let t = Instant::now(); for i in 0..50_000 { c.exec("INSERT INTO t VALUES (?,?,?,?,?,?)", &row(N + 1 + i)).unwrap(); }
    pr("[ours, 2nd process, shared-memory ring] insert, not durable", 50_000, t.elapsed().as_secs_f64());
    for threads in [4usize, 8] {
        let b = Arc::new(Barrier::new(threads + 1)); let per = 100_000;
        let hs: Vec<_> = (0..threads).map(|k| { let b = b.clone(); let sock = sock.clone(); let shm = shm.clone(); std::thread::spawn(move || { let mut c = Client::connect_shm(&sock, &shm).unwrap(); let mut rng = Rng(100 + k as u64); b.wait(); let mut s = 0.0; for _ in 0..per { let id = (rng.next() % N as u64) as i64 + 1; if let Value::Float(f) = c.exec("SELECT score FROM t WHERE id = ?", &[Value::Int(id)]).unwrap()[0][0] { s += f; } } s }) }).collect();
        b.wait(); let t = Instant::now(); let _: f64 = hs.into_iter().map(|h| h.join().unwrap()).sum(); pr(&format!("[ours, 2nd process, shared-memory ring] point reads, {threads} connections"), threads * per, t.elapsed().as_secs_f64());
    }
    println!("CLIENT_DONE"); std::io::stdout().flush().unwrap();
}

fn main() {
    if std::env::args().nth(1).as_deref() == Some("client") { return client_main(); }
    println!("=== E23 / H34: second-process reads over shared memory (E21 refs: socket 3.15 us / 1.45M on 8; SQLite shared file 1.02 us / 5.2M on 8) ===");
    let dir = PathBuf::from("data/nocow/e23dir"); let sql = Arc::new(Sql::create(&dir, false)); let mut app = Session::default();
    sql.exec_s(&mut app, "CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, age INT, name VARCHAR(32) UNIQUE, payload BLOB(64))", &[]).unwrap();
    let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap();
    for id in 1..=N { if id % 1000 == 1 { sql.exec_s(&mut app, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut app, &ins, &row(id)).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut app, "COMMIT", &[]).unwrap(); } }
    let _server = Server::start(sql.clone(), &PathBuf::from(SOCK)).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap()).arg("client").stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap();
    let rd = BufReader::new(child.stdout.take().unwrap()); for line in rd.lines() { let l = line.unwrap(); if l == "CLIENT_DONE" { break; } println!("{l}"); } child.wait().unwrap();
    let r = sql.exec_s(&mut app, "SELECT count(*) FROM t", &[]).unwrap(); println!("owner sees count(*) = {:?} after the client's inserts", r[0][0]);
    drop(ins); drop(sql); let _ = fs::remove_dir_all(&dir); fs::remove_file(SOCK).ok();
}
