use dblab::sql::{Session, Sql};
use dblab::table::Value;
use std::path::PathBuf;
use std::time::Instant;
fn rss() -> f64 { let s = std::fs::read_to_string("/proc/self/status").unwrap(); s.lines().find(|l| l.starts_with("VmRSS:")).and_then(|l| l.split_whitespace().nth(1)).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0) / 1024.0 }
fn threads() -> usize { std::fs::read_dir("/proc/self/task").map(|r| r.count()).unwrap_or(0) }
fn main() {
    let n: usize = std::env::args().nth(1).and_then(|a| a.parse().ok()).unwrap_or(1000);
    let base = PathBuf::from("data/nocow/tenants"); let _ = std::fs::remove_dir_all(&base);
    let (r0, th0) = (rss(), threads()); let t = Instant::now(); let mut dbs = vec![];
    for i in 0..n { let sql = Sql::create(&base.join(format!("t{i}")), false); let mut s = Session::default(); sql.exec_s(&mut s, "CREATE TABLE notes(id INTEGER PRIMARY KEY, body VARCHAR(200))", &[]).unwrap(); sql.exec_s(&mut s, "INSERT INTO notes VALUES (1, 'hello')", &[Value::Null]).ok(); sql.exec_s(&mut s, "INSERT INTO notes VALUES (1, 'hello')", &[]).ok(); dbs.push(sql); }
    let el = t.elapsed();
    println!("{n} tenant databases, one table and one row each: created in {:.2} s ({:.1} ms each); rss {:.0} -> {:.0} MB ({:.2} MB per tenant); threads {} -> {} ({} per tenant)", el.as_secs_f64(), el.as_secs_f64() * 1e3 / n as f64, r0, rss(), (rss() - r0) / n as f64, th0, threads(), (threads() - th0) as f64 / n as f64);
    let t = Instant::now(); for (i, sql) in dbs.iter().enumerate() { let mut s = Session::default(); sql.exec_s(&mut s, "SELECT body FROM notes WHERE id = 1", &[]).unwrap(); let _ = i; } println!("one read on every tenant: {:.1} us each", t.elapsed().as_secs_f64() * 1e6 / n as f64);
    drop(dbs); let _ = std::fs::remove_dir_all(&base);
}
