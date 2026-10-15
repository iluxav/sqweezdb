//! Experiment 16 (goal 4): access control. Roles, users with attributes, table grants, row policies. Tenant isolation end to end,
//! and what it costs. SQLite has none of this: every connection sees every row.
use dblab::sql::{Session, Sql};
use dblab::table::Value;
use dblab::util::Rng;
use rusqlite::{params, Connection};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::Instant;

const N: i64 = 1_000_000;
const CREATE: &str = "CREATE TABLE docs(id INTEGER PRIMARY KEY, tenant INT, title VARCHAR(32), body BLOB(64))";
fn pr(label: &str, n: usize, el: f64) { println!("{:<70} {:>10.0} /s  {:>8.3} us", label, n as f64 / el, el * 1e6 / n as f64); }
fn show(label: &str, r: Result<Vec<Vec<Value>>, String>) { match r { Ok(rows) => println!("{:<70} -> {} row(s){}", label, rows.len(), rows.first().map(|r| format!(" {:?}", r)).unwrap_or_default()), Err(e) => println!("{:<70} -> ERROR: {e}", label) } }

fn main() {
    println!("=== E16: access control (roles, grants, row policies) ===");
    let dir = PathBuf::from("data/nocow/e16dir"); let _ = fs::remove_dir_all(&dir);
    let sql = Arc::new(Sql::create(&dir, false));
    let mut app = Session::default();   // the embedding application: unrestricted
    sql.exec_s(&mut app, CREATE, &[]).unwrap();
    let ins = sql.prepare("INSERT INTO docs VALUES (?,?,?,?)").unwrap();
    for id in 1..=N { if id % 1000 == 1 { sql.exec_s(&mut app, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut app, &ins, &[Value::Int(id), Value::Int(id % 1000), Value::Text(format!("doc {id}")), Value::Bytes(vec![0; 64])]).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut app, "COMMIT", &[]).unwrap(); } }
    for stmt in ["CREATE ROLE admin", "GRANT ALL ON docs TO admin", "CREATE USER root IN ROLE admin",
                 "CREATE ROLE tenant_user", "GRANT SELECT, INSERT ON docs TO tenant_user",
                 "CREATE POLICY tenant_isolation ON docs FOR ALL TO tenant_user USING (tenant = SESSION.tenant)",
                 "CREATE USER alice IN ROLE tenant_user WITH tenant = 7", "CREATE USER bob IN ROLE tenant_user WITH tenant = 8", "CREATE USER nobody"] {
        sql.exec_s(&mut app, stmt, &[]).unwrap_or_else(|e| panic!("{stmt}: {e}"));
    }
    let mut alice = Session::default(); sql.login(&mut alice, "alice").unwrap();
    let mut bob = Session::default(); sql.login(&mut bob, "bob").unwrap();
    let mut root = Session::default(); sql.login(&mut root, "root").unwrap();
    let mut nobody = Session::default(); sql.login(&mut nobody, "nobody").unwrap();
    println!("--- behaviour ---");
    show("app:    SELECT count(*) FROM docs", sql.exec_s(&mut app, "SELECT count(*) FROM docs", &[]));
    show("root:   SELECT count(*) FROM docs", sql.exec_s(&mut root, "SELECT count(*) FROM docs", &[]));
    show("alice:  SELECT count(*) FROM docs            (policy: tenant = 7)", sql.exec_s(&mut alice, "SELECT count(*) FROM docs", &[]));
    show("alice:  SELECT title FROM docs WHERE id = 7  (tenant 7)", sql.exec_s(&mut alice, "SELECT title FROM docs WHERE id = ?", &[Value::Int(7)]));
    show("alice:  SELECT title FROM docs WHERE id = 8  (tenant 8)", sql.exec_s(&mut alice, "SELECT title FROM docs WHERE id = ?", &[Value::Int(8)]));
    show("alice:  SELECT count(*) FROM docs WHERE tenant = 8 (asks for another tenant)", sql.exec_s(&mut alice, "SELECT count(*) FROM docs WHERE tenant = 8", &[]));
    show("alice:  INSERT a doc with tenant 8", sql.exec_s(&mut alice, "INSERT INTO docs VALUES (?,?,?,?)", &[Value::Int(N + 1), Value::Int(8), Value::Text("sneaky".into()), Value::Bytes(vec![])]));
    show("alice:  INSERT a doc with tenant 7", sql.exec_s(&mut alice, "INSERT INTO docs VALUES (?,?,?,?)", &[Value::Int(N + 2), Value::Int(7), Value::Text("mine".into()), Value::Bytes(vec![])]));
    show("alice:  UPDATE docs SET title = 'x' WHERE id = 7  (no UPDATE grant)", sql.exec_s(&mut alice, "UPDATE docs SET title = 'x' WHERE id = ?", &[Value::Int(7)]));
    show("bob:    SELECT count(*) FROM docs            (policy: tenant = 8)", sql.exec_s(&mut bob, "SELECT count(*) FROM docs", &[]));
    show("nobody: SELECT count(*) FROM docs            (no grants)", sql.exec_s(&mut nobody, "SELECT count(*) FROM docs", &[]));
    show("alice:  CREATE ROLE hacker", sql.exec_s(&mut alice, "CREATE ROLE hacker", &[]));
    show("root:   UPDATE docs SET title = 'x' WHERE id = 7", sql.exec_s(&mut root, "UPDATE docs SET title = 'x' WHERE id = ?", &[Value::Int(7)]));
    show("alice:  SELECT title FROM docs WHERE id = 7  (sees root's change)", sql.exec_s(&mut alice, "SELECT title FROM docs WHERE id = ?", &[Value::Int(7)]));
    println!("--- cost ---");
    let q = sql.prepare("SELECT title FROM docs WHERE id = ?").unwrap();
    for (name, sess) in [("app (no user)", &mut app), ("root (grant check)", &mut root), ("alice (grant + row policy)", &mut alice)] {
        let mut rng = Rng(5); let t = Instant::now(); let mut n = 0;
        for _ in 0..500_000 { let id = (rng.next() % N as u64) as i64 + 1; n += sql.execute_s(sess, &q, &[Value::Int(id)]).unwrap().len(); }
        pr(&format!("point read by id as {name}: {n} rows returned of 500k"), 500_000, t.elapsed().as_secs_f64());
    }
    for (name, sess) in [("app", &mut app), ("alice (policy filters 1M rows)", &mut alice)] {
        let t = Instant::now(); for _ in 0..5 { sql.exec_s(sess, "SELECT count(*) FROM docs", &[]).unwrap(); }
        println!("{:<70} {:>8.3} ms per scan", format!("count(*) over 1M rows as {name}"), t.elapsed().as_secs_f64() * 1e3 / 5.0);
    }
    { let threads = 8; let per = 250_000; let b = Arc::new(Barrier::new(threads + 1));
      let hs: Vec<_> = (0..threads).map(|k| { let sql = sql.clone(); let q = q.clone(); let b = b.clone(); std::thread::spawn(move || { let mut s = Session::default(); sql.login(&mut s, "alice").unwrap(); let mut rng = Rng(50 + k as u64); b.wait(); let mut n = 0; for _ in 0..per { let id = (rng.next() % N as u64) as i64 + 1; n += sql.execute_s(&mut s, &q, &[Value::Int(id)]).unwrap().len(); } n }) }).collect();
      b.wait(); let t = Instant::now(); let n: usize = hs.into_iter().map(|h| h.join().unwrap()).sum(); pr(&format!("point read as alice, 8 threads ({n} rows returned of 2M)"), threads * per, t.elapsed().as_secs_f64()); }
    drop(q); drop(ins); drop(sql); let _ = fs::remove_dir_all(&dir);

    println!("--- SQLite for reference: no users, roles or policies exist ---");
    let p = PathBuf::from("data/nocow/e16.db"); for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
    let c = Connection::open(&p).unwrap(); c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;").unwrap(); c.execute_batch(CREATE).unwrap();
    let tx = c.unchecked_transaction().unwrap(); { let mut st = tx.prepare("INSERT INTO docs VALUES (?,?,?,?)").unwrap(); for id in 1..=N { st.execute(params![id, id % 1000, format!("doc {id}"), &[0u8; 64][..]]).unwrap(); } } tx.commit().unwrap();
    let n: i64 = c.query_row("SELECT count(*) FROM docs", [], |r| r.get(0)).unwrap();
    println!("any connection: SELECT count(*) FROM docs -> {n} rows; isolation must be re-implemented in every query by the application, and one forgotten WHERE tenant = ? leaks everything");
    let t = Instant::now(); for _ in 0..5 { let _: i64 = c.query_row("SELECT count(*) FROM docs WHERE tenant = 7", [], |r| r.get(0)).unwrap(); } println!("{:<70} {:>8.3} ms per scan", "count(*) WHERE tenant = 7 (the clause the app must add itself)", t.elapsed().as_secs_f64() * 1e3 / 5.0);
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
}
