//! Experiment 14 (H24): multi-statement transactions through SQL. Bulk load in 1000-row transactions, concurrent
//! money transfers with an invariant, durable multi-row commits, read-your-writes and rollback.
use dblab::sql::{Session, Sql};
use dblab::table::Value;
use dblab::util::Rng;
use rusqlite::{params, Connection};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

const N: i64 = 1_000_000;
const CREATE: &str = "CREATE TABLE acct(id INTEGER PRIMARY KEY, balance DOUBLE, owner INT, name VARCHAR(32) UNIQUE, note BLOB(64))";
const INSERT: &str = "INSERT INTO acct VALUES (?,?,?,?,?)";
fn row(id: i64) -> Vec<Value> { vec![Value::Int(id), Value::Float(100.0), Value::Int(id % 1000), Value::Text(format!("a{id}@bank.example")), Value::Bytes(vec![1u8; 64])] }
fn pr(label: &str, n: usize, el: f64) { println!("{:<64} {:>10.0} /s  {:>9.3} us each", label, n as f64 / el, el * 1e6 / n as f64); }

fn sqlite_side() {
    let p = PathBuf::from("data/nocow/e14.db");
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
    let c = Connection::open(&p).unwrap();
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-262144; PRAGMA mmap_size=1073741824;").unwrap();
    c.execute_batch(CREATE).unwrap();
    let t = Instant::now(); let mut id = 1i64;
    while id <= N { let tx = c.unchecked_transaction().unwrap(); { let mut st = tx.prepare_cached(INSERT).unwrap(); for _ in 0..1000 { st.execute(params![id, 100.0f64, id % 1000, format!("a{id}@bank.example"), &[1u8; 64][..]]).unwrap(); id += 1; } } tx.commit().unwrap(); }
    pr("[sqlite] load 1M rows, 1000-row transactions, NORMAL", N as usize, t.elapsed().as_secs_f64());
    // transfers: 4 threads, each txn moves 1.0 between two random accounts (SQLite serializes writers)
    for (sync, threads, per) in [("NORMAL", 4usize, 25_000usize), ("FULL", 4, 500)] {
        let b = Arc::new(Barrier::new(threads + 1));
        let hs: Vec<_> = (0..threads).map(|k| { let p = p.clone(); let b = b.clone(); let sync = sync.to_string(); std::thread::spawn(move || {
            let mut c = Connection::open(&p).unwrap(); c.execute_batch(&format!("PRAGMA synchronous={sync}; PRAGMA busy_timeout=60000;")).unwrap();
            let mut rng = Rng(11 + k as u64); b.wait(); let mut retries = 0u64;
            for _ in 0..per { let a = (rng.next() % N as u64) as i64 + 1; let bb = (rng.next() % N as u64) as i64 + 1;
                loop { let r: Result<(), rusqlite::Error> = (|| { let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                    let ba: f64 = tx.query_row("SELECT balance FROM acct WHERE id = ?", params![a], |r| r.get(0))?; let bb2: f64 = tx.query_row("SELECT balance FROM acct WHERE id = ?", params![bb], |r| r.get(0))?;
                    tx.execute("UPDATE acct SET balance = ? WHERE id = ?", params![ba - 1.0, a])?; tx.execute("UPDATE acct SET balance = ? WHERE id = ?", params![bb2 + 1.0, bb])?; tx.commit() })();
                    match r { Ok(()) => break, Err(_) => { retries += 1; } } } }
            retries }) }).collect();
        b.wait(); let t = Instant::now(); let retries: u64 = hs.into_iter().map(|h| h.join().unwrap()).sum(); let el = t.elapsed().as_secs_f64();
        let total: f64 = c.query_row("SELECT sum(balance) FROM acct", [], |r| r.get(0)).unwrap();
        pr(&format!("[sqlite] transfer txns (2 reads + 2 updates), {threads} thr, sync={sync}, retries {retries}, sum ok={}", (total - 100.0 * N as f64).abs() < 1e-3), threads * per, el);
    }
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
}

fn ours_side(durable: bool) {
    let dir = PathBuf::from(if durable { "data/nocow/e14dur" } else { "data/nocow/e14dir" }); let _ = fs::remove_dir_all(&dir);
    let sql = Arc::new(Sql::create(&dir, durable));
    sql.exec(CREATE, &[]).unwrap();
    let tag = if durable { "[ours durable]" } else { "[ours]  " };
    let ins = sql.prepare(INSERT).unwrap();
    if !durable {
        let mut sess = Session::default();
        let t = Instant::now(); let mut id = 1i64;
        while id <= N { sql.exec_s(&mut sess, "BEGIN", &[]).unwrap(); for _ in 0..1000 { sql.execute_s(&mut sess, &ins, &row(id)).unwrap(); id += 1; } sql.exec_s(&mut sess, "COMMIT", &[]).unwrap(); }
        pr(&format!("{tag} load 1M rows, 1000-row transactions"), N as usize, t.elapsed().as_secs_f64());
        // read-your-writes + rollback
        sql.exec_s(&mut sess, "BEGIN", &[]).unwrap(); sql.execute_s(&mut sess, &ins, &row(N + 1)).unwrap();
        let seen_inside = sql.exec_s(&mut sess, "SELECT balance FROM acct WHERE id = ?", &[Value::Int(N + 1)]).unwrap().len();
        sql.exec_s(&mut sess, "ROLLBACK", &[]).unwrap();
        let seen_after = sql.exec("SELECT balance FROM acct WHERE id = ?", &[Value::Int(N + 1)]).unwrap().len();
        println!("{tag} read-your-writes inside txn: {} row(s); after ROLLBACK: {} row(s)", seen_inside, seen_after);
        let dup = sql.exec_s(&mut sess, "BEGIN", &[]).and_then(|_| sql.execute_s(&mut sess, &ins, &row(5))).and_then(|_| sql.exec_s(&mut sess, "COMMIT", &[]));
        println!("{tag} inserting a duplicate id inside a txn then COMMIT: {:?}", dup.err().unwrap_or_else(|| "accepted (BUG)".into()));
    } else {
        // parallel durable load, 64 threads x 1000-row transactions
        let threads = 64; let per_thread_txns = 8;
        let b = Arc::new(Barrier::new(threads + 1));
        let hs: Vec<_> = (0..threads).map(|k| { let sql = sql.clone(); let ins = ins.clone(); let b = b.clone(); std::thread::spawn(move || { let mut sess = Session::default(); b.wait();
            for t in 0..per_thread_txns { sql.exec_s(&mut sess, "BEGIN", &[]).unwrap(); for i in 0..1000 { sql.execute_s(&mut sess, &ins, &row(1 + (k * per_thread_txns + t) as i64 * 1000 + i)).unwrap(); } sql.exec_s(&mut sess, "COMMIT", &[]).unwrap(); } }) }).collect();
        b.wait(); let t = Instant::now(); for h in hs { h.join().unwrap(); }
        pr(&format!("{tag} load {} rows, 1000-row transactions, {threads} threads, fsync per txn", threads * per_thread_txns * 1000), threads * per_thread_txns * 1000, t.elapsed().as_secs_f64());
        let mut sess = Session::default(); let mut id = (threads * per_thread_txns * 1000) as i64 + 1;
        while id <= N { sql.exec_s(&mut sess, "BEGIN", &[]).unwrap(); for _ in 0..1000 { sql.execute_s(&mut sess, &ins, &row(id)).unwrap(); id += 1; } sql.exec_s(&mut sess, "COMMIT", &[]).unwrap(); }
    }
    let total0: f64 = match sql.exec("SELECT sum(balance) FROM acct", &[]).unwrap()[0][0] { Value::Float(f) => f, _ => 0.0 };
    // transfers
    let configs: Vec<(usize, usize)> = if durable { vec![(4, 500), (64, 500), (256, 250)] } else { vec![(1, 100_000), (4, 50_000), (12, 50_000)] };
    let sel = sql.prepare("SELECT balance FROM acct WHERE id = ?").unwrap(); let upd = sql.prepare("UPDATE acct SET balance = ? WHERE id = ?").unwrap();
    for (threads, per) in configs {
        let b = Arc::new(Barrier::new(threads + 1)); let conflicts = Arc::new(AtomicU64::new(0));
        let hs: Vec<_> = (0..threads).map(|k| { let sql = sql.clone(); let sel = sel.clone(); let upd = upd.clone(); let b = b.clone(); let conflicts = conflicts.clone(); std::thread::spawn(move || {
            let mut sess = Session::default(); let mut rng = Rng(11 + k as u64); b.wait();
            for _ in 0..per { let a = (rng.next() % N as u64) as i64 + 1; let bb = (rng.next() % N as u64) as i64 + 1; if a == bb { continue; }
                loop {
                    sql.exec_s(&mut sess, "BEGIN", &[]).unwrap();
                    let ba = match sql.execute_s(&mut sess, &sel, &[Value::Int(a)]).unwrap()[0][0] { Value::Float(f) => f, _ => unreachable!() };
                    let bb2 = match sql.execute_s(&mut sess, &sel, &[Value::Int(bb)]).unwrap()[0][0] { Value::Float(f) => f, _ => unreachable!() };
                    sql.execute_s(&mut sess, &upd, &[Value::Float(ba - 1.0), Value::Int(a)]).unwrap(); sql.execute_s(&mut sess, &upd, &[Value::Float(bb2 + 1.0), Value::Int(bb)]).unwrap();
                    match sql.exec_s(&mut sess, "COMMIT", &[]) { Ok(_) => break, Err(e) => { assert!(e.contains("Conflict"), "{e}"); conflicts.fetch_add(1, Ordering::Relaxed); } }
                } } }) }).collect();
        b.wait(); let t = Instant::now(); for h in hs { h.join().unwrap(); } let el = t.elapsed().as_secs_f64();
        let total: f64 = match sql.exec("SELECT sum(balance) FROM acct", &[]).unwrap()[0][0] { Value::Float(f) => f, _ => 0.0 };
        pr(&format!("{tag} transfer txns (2 reads + 2 updates), {threads} thr, conflicts {}, sum ok={}", conflicts.load(Ordering::Relaxed), (total - total0).abs() < 1e-3), threads * per, el);
    }
    drop(sel); drop(upd); drop(ins); drop(sql); let _ = fs::remove_dir_all(&dir);
}

fn main() {
    println!("=== E14 / H24: multi-statement transactions ===");
    sqlite_side();
    ours_side(false);
    ours_side(true);
}
