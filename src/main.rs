//! The `sqweezdb` command: `serve` a database directory (Unix socket, TCP, the web console) and `shell` into one.
#[cfg(target_env = "musl")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;   // musl's allocator halves the engine's speed (E45)

use serde_json::{json, Map, Value as J};
use sqweezdb::config::Config;
use sqweezdb::net::Client;
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::{format_timestamp, ColType, Value};
use std::io::{BufRead, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

static STOP: AtomicBool = AtomicBool::new(false);
extern "C" fn on_signal(_: libc::c_int) { STOP.store(true, Ordering::Relaxed); }

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(|s| s.as_str()) {
        Some("serve") => serve(&args[1..]),
        Some("shell") => shell(&args[1..]),
        Some("version") | Some("--version") => println!("sqweezdb {}", env!("CARGO_PKG_VERSION")),
        _ => usage(),
    }
}
fn usage() -> ! {
    eprintln!("usage:\n  sqweezdb serve <dir> [--config <file.toml>] [--ui <addr>] [--tcp <addr>] [--socket <path>] [--token <secret>] [--durable]\n      Serve a database directory. --ui opens the web console (bind it to 127.0.0.1 unless you mean otherwise).\n      --ui and --tcp need a token; without --token one is generated and printed once.\n  sqweezdb shell <dir>                      Open the directory in this process and run SQL interactively.\n  sqweezdb shell <host:port> --token <t>    The same over TCP against a running server.");
    std::process::exit(2)
}
fn flag(a: &[String], name: &str) -> Option<String> { a.iter().position(|s| s == name).and_then(|i| a.get(i + 1).cloned()) }
fn has(a: &[String], name: &str) -> bool { a.iter().any(|s| s == name) }
fn fail(e: impl std::fmt::Display) -> ! { eprintln!("sqweezdb: {e}"); std::process::exit(1) }
fn random_token() -> String { let mut b = [0u8; 16]; std::fs::File::open("/dev/urandom").and_then(|mut f| std::io::Read::read_exact(&mut f, &mut b)).unwrap_or_else(|e| fail(format!("/dev/urandom: {e}"))); b.iter().map(|x| format!("{x:02x}")).collect() }

fn serve(a: &[String]) {
    let dir = a.first().filter(|s| !s.starts_with("--")).map(std::path::PathBuf::from).unwrap_or_else(|| usage());
    let mut opts = Map::new(); let mut listen = Map::new();
    if let Some(f) = flag(a, "--config") { opts.insert("configFile".into(), json!(f)); }
    if has(a, "--durable") { opts.insert("durable".into(), json!(true)); }
    if let Some(v) = flag(a, "--ui") { listen.insert("http".into(), json!(v)); }
    if let Some(v) = flag(a, "--tcp") { listen.insert("tcp".into(), json!(v)); }
    if let Some(v) = flag(a, "--socket") { listen.insert("socket".into(), json!(v)); }
    let mut printed_token = None;
    match flag(a, "--token") { Some(t) => { listen.insert("token".into(), json!(t)); } None => if listen.contains_key("http") || listen.contains_key("tcp") { let t = random_token(); listen.insert("token".into(), json!(t.clone())); printed_token = Some(t); } }
    if !listen.is_empty() { opts.insert("listen".into(), J::Object(listen)); }
    let cfg = Config::load(&dir, Some(&J::Object(opts).to_string())).unwrap_or_else(|e| fail(e));
    let t = Instant::now(); let sql = Sql::open_with(&dir, cfg.clone()).unwrap_or_else(|e| fail(e));
    let rows = sql.snapshot_rows();
    println!("sqweezdb serving {} ({} tables, {} rows, opened in {:.0} ms)", dir.display(), sql.db.table_names().len(), rows, t.elapsed().as_secs_f64() * 1e3);
    println!("  durability: {:?}{}", cfg.durable, cfg.backup.as_ref().map(|b| format!("; backup to {}/{} every {:?}{}", b.bucket, b.prefix.trim_matches('/'), b.interval, if b.passphrase.is_some() { ", encrypted" } else { "" })).unwrap_or_default());
    if let Some(p) = &cfg.listen.socket { println!("  socket:     {}", p.display()); }
    if let Some(t) = &cfg.listen.tcp { println!("  tcp:        {t}"); }
    if let Some(h) = &cfg.listen.http { println!("  console:    http://{h}/"); }
    if let Some(t) = printed_token { println!("  token:      {t}   (generated; pass --token to choose one)"); }
    unsafe { libc::signal(libc::SIGINT, on_signal as libc::sighandler_t); libc::signal(libc::SIGTERM, on_signal as libc::sighandler_t); }
    while !STOP.load(Ordering::Relaxed) { std::thread::sleep(Duration::from_millis(100)); }
    let t = Instant::now(); let st = sql.snapshot(); sql.close();
    println!("\nstopped: final snapshot of {} rows in {:.0} ms", st.rows, t.elapsed().as_secs_f64() * 1e3);
}

enum Conn { Local(Arc<Sql>, Session), Remote(Client) }
fn shell(a: &[String]) {
    let target = a.first().filter(|s| !s.starts_with("--")).cloned().unwrap_or_else(|| usage());
    let mut conn = if !std::path::Path::new(&target).exists() && target.contains(':') {
        let token = flag(a, "--token").unwrap_or_else(|| usage());
        Conn::Remote(Client::connect_tcp(&target, &token).unwrap_or_else(|e| fail(format!("{target}: {e}"))))
    } else { Conn::Local(Sql::open_with(std::path::Path::new(&target), Config::default()).unwrap_or_else(|e| fail(e)), Session::default()) };
    let mut out = std::io::stdout(); let stdin = std::io::stdin(); let mut buf = String::new();
    eprintln!("sqweezdb shell: statements end with ';'   .tables   .quit");
    loop {
        eprint!("{}", if buf.is_empty() { "sqweezdb> " } else { "      ... " }); let _ = std::io::stderr().flush();
        let mut line = String::new(); if stdin.lock().read_line(&mut line).unwrap_or(0) == 0 { break; }
        let l = line.trim();
        if buf.is_empty() && l.starts_with('.') { match l { ".quit" | ".exit" => break, ".tables" => { let t = Instant::now(); match &mut conn { Conn::Local(s, _) => { let rows: Vec<Vec<Value>> = s.db.table_names().into_iter().map(|n| vec![Value::Text(n)]).collect(); print_rows(&mut out, &[("table".into(), None)], &rows, t.elapsed()) } Conn::Remote(_) => eprintln!(".tables is not available over TCP yet; query the console's /api/tables") } } other => eprintln!("unknown command {other}") } continue; }
        buf.push_str(l); buf.push(' ');
        if !l.ends_with(';') { continue; }
        let text = buf.trim().trim_end_matches(';').trim().to_string(); buf.clear(); if text.is_empty() { continue; }
        let t = Instant::now();
        match &mut conn {
            Conn::Local(s, sess) => match s.query(sess, &text, &[]) { Ok((cols, rows)) => print_rows(&mut out, &cols, &rows, t.elapsed()), Err(e) => eprintln!("error [{}]: {e}", sqweezdb::sql::error_code(&e)) },
            Conn::Remote(c) => match c.exec(&text, &[]) { Ok(rows) => print_rows(&mut out, &[], &rows, t.elapsed()), Err(e) => eprintln!("error [{}]: {e}", sqweezdb::sql::error_code(&e)) },
        }
    }
}
fn show(v: &Value, ty: Option<&ColType>) -> String { match v { Value::Null => "NULL".into(), Value::Int(i) => if matches!(ty, Some(ColType::Timestamp)) { format_timestamp(*i) } else { i.to_string() }, Value::Float(f) => format!("{f}"), Value::Bool(b) => b.to_string(), Value::Text(s) => s.clone(), Value::Bytes(b) => format!("0x{}", b.iter().map(|x| format!("{x:02x}")).collect::<String>()) } }
fn print_rows(out: &mut impl Write, cols: &[(String, Option<ColType>)], rows: &[Vec<Value>], el: Duration) {
    let ncol = cols.len().max(rows.first().map(|r| r.len()).unwrap_or(0));
    let cells: Vec<Vec<String>> = rows.iter().map(|r| r.iter().enumerate().map(|(i, v)| show(v, cols.get(i).and_then(|c| c.1.as_ref()))).collect()).collect();
    let head: Vec<String> = (0..ncol).map(|i| cols.get(i).map(|c| c.0.clone()).unwrap_or_else(|| format!("col{i}"))).collect();
    let width: Vec<usize> = (0..ncol).map(|i| cells.iter().map(|r| r.get(i).map(|s| s.chars().count()).unwrap_or(0)).chain(std::iter::once(head[i].chars().count())).max().unwrap_or(0).min(60)).collect();
    if ncol > 0 { let _ = writeln!(out, "{}", head.iter().enumerate().map(|(i, h)| format!("{:<w$}", h, w = width[i])).collect::<Vec<_>>().join("  ")); let _ = writeln!(out, "{}", width.iter().map(|w| "-".repeat(*w)).collect::<Vec<_>>().join("  ")); }
    for r in &cells { let _ = writeln!(out, "{}", r.iter().enumerate().map(|(i, c)| { let c: String = c.chars().take(60).collect(); format!("{:<w$}", c, w = width[i]) }).collect::<Vec<_>>().join("  ")); }
    let _ = writeln!(out, "({} row{}, {:.2} ms)", rows.len(), if rows.len() == 1 { "" } else { "s" }, el.as_secs_f64() * 1e3);
}
