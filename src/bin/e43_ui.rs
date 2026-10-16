//! Experiment 43: the web console and the `sqweezdb serve` command. What it costs (binary size, idle memory) and what
//! the console's HTTP path costs per query against the frame protocol over TCP; insert → live update in a browser-style
//! client; a wrong token; graceful stop with a final snapshot and the restart that follows.
use sqweezdb::net::Client;
use sqweezdb::table::Value;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const DIR: &str = "data/nocow/e43"; const UI: &str = "127.0.0.1:7790"; const TCP: &str = "127.0.0.1:7791"; const TOKEN: &str = "e43-token";
fn rss_mb(pid: u32) -> (f64, u32) { let s = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default(); let get = |k: &str| s.lines().find(|l| l.starts_with(k)).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0); (get("VmRSS:") / 1024.0, get("Threads:") as u32) }
fn wait_port(addr: &str) { for _ in 0..200 { if TcpStream::connect(addr).is_ok() { return; } std::thread::sleep(Duration::from_millis(50)); } panic!("{addr} did not come up"); }
struct Http { r: BufReader<TcpStream>, w: TcpStream }
impl Http {
    fn connect(addr: &str) -> Http { let s = TcpStream::connect(addr).unwrap(); s.set_nodelay(true).unwrap(); Http { r: BufReader::new(s.try_clone().unwrap()), w: s } }
    fn call(&mut self, method: &str, path: &str, body: &str) -> (u16, String) {
        self.w.write_all(format!("{method} {path} HTTP/1.1\r\nHost: {UI}\r\nAuthorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}", body.len()).as_bytes()).unwrap();
        let mut line = String::new(); self.r.read_line(&mut line).unwrap(); let status: u16 = line.split_whitespace().nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
        let mut len = 0usize; loop { let mut h = String::new(); self.r.read_line(&mut h).unwrap(); let h = h.trim_end(); if h.is_empty() { break; } if let Some(v) = h.to_ascii_lowercase().strip_prefix("content-length:") { len = v.trim().parse().unwrap(); } }
        let mut b = vec![0u8; len]; self.r.read_exact(&mut b).unwrap(); (status, String::from_utf8_lossy(&b).into_owned())
    }
}
fn next_event(r: &mut BufReader<TcpStream>) -> String { loop { let mut l = String::new(); if r.read_line(&mut l).unwrap() == 0 { return String::new(); } if let Some(d) = l.strip_prefix("data: ") { return d.trim().to_string(); } } }

fn main() {
    println!("=== E43: the web console (`sqweezdb serve --ui`): what it costs and how fast it is ===");
    let bin = "target/release/sqweezdb"; let stripped = format!("{DIR}-bin"); let _ = std::fs::create_dir_all(DIR); std::fs::copy(bin, &stripped).unwrap(); Command::new("strip").arg(&stripped).status().unwrap();
    println!("the sqweezdb command: {:.1} MB stripped (engine, S3 client, TLS, SQL parser, console); the embedded page is {:.1} KB", std::fs::metadata(&stripped).unwrap().len() as f64 / 1e6, include_str!("../ui/index.html").len() as f64 / 1e3);
    let _ = std::fs::remove_file(&stripped); let _ = std::fs::remove_dir_all(DIR);
    let mut child = Command::new(bin).args(["serve", DIR, "--ui", UI, "--tcp", TCP, "--token", TOKEN]).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap();
    wait_port(UI); wait_port(TCP); std::thread::sleep(Duration::from_millis(500));
    let (rss0, th0) = rss_mb(child.id()); println!("idle `sqweezdb serve --ui --tcp`, empty database: {rss0:.1} MB resident, {th0} threads");
    let mut c = Client::connect_tcp(TCP, TOKEN).unwrap();
    c.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, name VARCHAR(32), placed_at TIMESTAMP)", &[]).unwrap();
    let t = Instant::now();
    for b in 0..1000i64 { let stmts: Vec<(&str, Vec<Value>)> = (0..1000i64).map(|i| { let id = b * 1000 + i + 1; ("INSERT INTO t VALUES (?,?,?,?,?)", vec![Value::Int(id), Value::Float(id as f64), Value::Int(id % 20), Value::Text(format!("u{id}")), Value::Int(1_700_000_000_000_000 + id * 1_000_000)]) }).collect(); for r in c.exec_batch(&stmts).unwrap() { r.unwrap(); } }
    println!("loaded 1M rows over TCP, 1000 statements per round trip: {:.2} s", t.elapsed().as_secs_f64());
    std::thread::sleep(Duration::from_millis(500)); let (rss1, th1) = rss_mb(child.id()); println!("with 1M rows: {rss1:.1} MB resident, {th1} threads");
    let mut h = Http::connect(UI);
    let (st, body) = h.call("GET", "/api/status", ""); assert_eq!(st, 200, "{body}");
    let (st, body) = h.call("GET", "/api/tables", ""); assert_eq!(st, 200, "{body}"); assert!(body.contains("\"rows\":1000000"), "{body}");
    let n = 20_000i64; let t = Instant::now(); let mut bytes = 0usize;
    for i in 0..n { let (st, body) = h.call("POST", "/api/sql", &format!(r#"{{"sql":"SELECT score FROM t WHERE id = ?","params":[{}]}}"#, (i * 7919) % 1_000_000 + 1)); assert_eq!(st, 200, "{body}"); bytes += body.len(); }
    let http_us = t.elapsed().as_secs_f64() * 1e6 / n as f64;
    let t = Instant::now(); for i in 0..n { c.exec("SELECT score FROM t WHERE id = ?", &[Value::Int((i * 7919) % 1_000_000 + 1)]).unwrap(); } let tcp_us = t.elapsed().as_secs_f64() * 1e6 / n as f64;
    println!("point read, one connection: console HTTP+JSON {http_us:.1} us per query ({} B responses), frame protocol over TCP {tcp_us:.1} us", bytes / n as usize);
    let (st, body) = h.call("POST", "/api/sql", r#"{"sql":"SELECT country, count(*) AS n, avg(score) AS avg FROM t GROUP BY country"}"#); assert_eq!(st, 200, "{body}");
    let ms: f64 = body.split("\"ms\":").nth(1).and_then(|s| s.split(|c: char| !(c.is_ascii_digit() || c == '.')).next()).and_then(|s| s.parse().ok()).unwrap_or(0.0);
    println!("GROUP BY over 1M rows through the console: {ms:.1} ms in the engine, 20 groups back as JSON");
    let (st, body) = h.call("POST", "/api/sql", r#"{"sql":"SELEC 1"}"#); println!("a typo through the console: HTTP {st} {}", body.trim());
    let mut bad = Http::connect(UI); bad.w.write_all(b"GET /api/status HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer wrong\r\n\r\n").unwrap(); let mut l = String::new(); bad.r.read_line(&mut l).unwrap(); println!("wrong token: {}", l.trim());
    let s = TcpStream::connect(UI).unwrap(); s.set_nodelay(true).unwrap(); let mut sw = s.try_clone().unwrap(); let mut sr = BufReader::new(s);
    sw.write_all(format!("GET /api/live?sql=SELECT%20count(*)%20AS%20n%20FROM%20t&token={TOKEN} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes()).unwrap();
    let first = next_event(&mut sr); assert!(first.contains("\"initial\":true"), "{first}");
    let mut lags = vec![];
    for i in 0..200i64 { let t = Instant::now(); c.exec("INSERT INTO t VALUES (?,?,?,?,?)", &[Value::Int(2_000_000 + i), Value::Float(1.0), Value::Int(1), Value::Text("x".into()), Value::Null]).unwrap(); let ev = next_event(&mut sr); assert!(ev.contains("\"rows\""), "{ev}"); lags.push(t.elapsed().as_secs_f64() * 1e3); std::thread::sleep(Duration::from_millis(5)); }
    lags.sort_by(|a, b| a.partial_cmp(b).unwrap()); println!("live query in the console: insert over TCP -> updated result on the event stream, p50 {:.1} ms, p99 {:.1} ms (5 ms debounce by default)", lags[100], lags[198]);
    let t = Instant::now(); unsafe { libc::kill(child.id() as i32, libc::SIGINT); } let status = child.wait().unwrap(); let el = t.elapsed().as_secs_f64();
    let mut out = String::new(); child.stdout.take().unwrap().read_to_string(&mut out).unwrap(); let last = out.lines().last().unwrap_or("").to_string();
    println!("SIGINT: {status} after {el:.2} s; \"{last}\"; snapshot present: {}", std::path::Path::new(DIR).join("snap").exists());
    let t = Instant::now(); let mut child2 = Command::new(bin).args(["serve", DIR, "--tcp", TCP, "--token", TOKEN]).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap(); wait_port(TCP);
    let mut c2 = Client::connect_tcp(TCP, TOKEN).unwrap(); let n = c2.exec("SELECT count(*) FROM t", &[]).unwrap(); println!("restart from that snapshot: answering {:.0} ms after launch, {:?} rows", t.elapsed().as_secs_f64() * 1e3, n[0][0]);
    unsafe { libc::kill(child2.id() as i32, libc::SIGINT); } child2.wait().unwrap();
    let _ = std::fs::remove_dir_all(DIR);
}
