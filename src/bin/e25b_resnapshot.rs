//! E25b: correctness of snapshots taken from a mapped table (restart -> writes -> snapshot -> restart, twice).
//! Every key is verified by id and by unique name after each step.
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::Value;
use sqweezdb::util::Rng;
use std::collections::HashMap;
use std::path::PathBuf;

fn n_rows() -> i64 { std::env::args().nth(1).and_then(|a| a.parse().ok()).unwrap_or(200_000) }
const DIR: &str = "data/nocow/e25b";
fn row(id: i64, score: f64) -> Vec<Value> { vec![Value::Int(id), Value::Float(score), Value::Int(id % 20), Value::Int(18 + id % 60), Value::Text(format!("u{id}@example.com")), Value::Bytes(vec![7; 64])] }
fn verify(sql: &Sql, exp: &HashMap<i64, Option<f64>>, label: &str) -> usize {
    let mut s = Session::default(); let by_id = sql.prepare("SELECT score FROM t WHERE id = ?").unwrap(); let by_name = sql.prepare("SELECT id FROM t WHERE name = ?").unwrap();
    let (mut bad, mut shown) = (0, 0);
    let mut keys: Vec<&i64> = exp.keys().collect(); keys.sort();
    for &id in keys {
        let want = exp[&id];
        let got = sql.execute_s(&mut s, &by_id, &[Value::Int(id)]).unwrap().first().and_then(|r| if let Value::Float(f) = r[0] { Some(f) } else { None });
        let got_n = sql.execute_s(&mut s, &by_name, &[Value::Text(format!("u{id}@example.com"))]).unwrap().first().and_then(|r| if let Value::Int(i) = r[0] { Some(i) } else { None });
        let ok = got == want && got_n == want.map(|_| id);
        if !ok { bad += 1; if shown < 3 { shown += 1; println!("  MISMATCH id {id}: want {want:?}, by id {got:?}, by name {got_n:?} | {}", sql.db.table("t").unwrap().debug_lookup(id)); } }
    }
    let cnt = match sql.exec_s(&mut s, "SELECT count(*) FROM t", &[]).unwrap()[0][0] { Value::Int(i) => i, _ => -1 };
    let live = exp.values().filter(|v| v.is_some()).count();
    println!("{label}: {} keys checked, {bad} mismatches; count(*) {cnt} vs expected {live}{}", exp.len(), if cnt as usize != live { " <- WRONG" } else { "" });
    bad + if cnt as usize != live { 1 } else { 0 }
}
fn writes(sql: &Sql, exp: &mut HashMap<i64, Option<f64>>, seed: u64, next_id: &mut i64) {
    let mut s = Session::default(); let mut rng = Rng(seed);
    let upd = sql.prepare("UPDATE t SET score = ? WHERE id = ?").unwrap(); let del = sql.prepare("DELETE FROM t WHERE id = ?").unwrap(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap();
    for k in 0..20_000 { let id = (rng.next() % n_rows() as u64) as i64 + 1; let sc = k as f64; if sql.execute_s(&mut s, &upd, &[Value::Float(sc), Value::Int(id)]).is_ok() && exp[&id].is_some() { exp.insert(id, Some(sc)); } }
    for _ in 0..2_500 { let id = (rng.next() % n_rows() as u64) as i64 + 1; let r = sql.execute_s(&mut s, &del, &[Value::Int(id)]); if r.is_ok() { exp.insert(id, None); } }
    for _ in 0..10_000 { let id = *next_id; *next_id += 1; let sc = (rng.next() % 1000) as f64; sql.execute_s(&mut s, &ins, &row(id, sc)).unwrap(); exp.insert(id, Some(sc)); }
    // re-insert some deleted ids (same key as a dead mapped row)
    let dead: Vec<i64> = exp.iter().filter(|(_, v)| v.is_none()).map(|(k, _)| *k).take(500).collect();
    for id in dead { sql.execute_s(&mut s, &ins, &row(id, 42.0)).unwrap(); exp.insert(id, Some(42.0)); }
    // deletes last: a snapshot taken now has no row carrying the latest sequence (caught a sequence-restart bug, E26)
    for _ in 0..2_500 { let id = (rng.next() % n_rows() as u64) as i64 + 1; let r = sql.execute_s(&mut s, &del, &[Value::Int(id)]); if r.is_ok() { exp.insert(id, None); } }
}
fn main() {
    let dir = PathBuf::from(DIR); let mut exp: HashMap<i64, Option<f64>> = HashMap::new(); let mut next_id = n_rows() + 1;
    let mut total_bad = 0;
    { let sql = Sql::recreate(&dir, false); let mut s = Session::default(); sql.exec_s(&mut s, "CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, age INT, name VARCHAR(32) UNIQUE, payload BLOB(64))", &[]).unwrap();
      let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?,?,?)").unwrap(); let mut rng = Rng(7);
      for id in 1..=n_rows() { let sc = (rng.next() % 100000) as f64 / 100.0; sql.execute_s(&mut s, &ins, &row(id, sc)).unwrap(); exp.insert(id, Some(sc)); }
      total_bad += verify(&sql, &exp, "fresh (heap only)"); let st = sql.snapshot(); println!("snapshot 1: {} rows, {:.1} MB", st.rows, st.bytes as f64 / 1e6); }
    for round in 1..=3 {
        let (sql, rs) = Sql::open(&dir, false, 4).unwrap();
        total_bad += verify(&sql, &exp, &format!("round {round}: after restart ({} mapped rows, {} replayed)", rs.snap_rows, rs.replayed));
        writes(&sql, &mut exp, 100 + round as u64, &mut next_id);
        total_bad += verify(&sql, &exp, &format!("round {round}: after writes on the mapped table"));
        drop(sql); let (sql, rs2) = Sql::open(&dir, false, 4).unwrap(); total_bad += verify(&sql, &exp, &format!("round {round}: restart WITHOUT a new snapshot (replay {} records)", rs2.replayed));
        writes(&sql, &mut exp, 200 + round as u64, &mut next_id);
        drop(sql); let (sql, rs3) = Sql::open(&dir, false, 4).unwrap(); total_bad += verify(&sql, &exp, &format!("round {round}: writes then restart again without a snapshot (replay {} records)", rs3.replayed));
        let st = sql.snapshot(); println!("snapshot {}: {} rows, {:.1} MB", round + 1, st.rows, st.bytes as f64 / 1e6);
    }
    let (sql, _) = Sql::open(&dir, false, 4).unwrap(); total_bad += verify(&sql, &exp, "final restart");
    println!("TOTAL mismatches: {total_bad}"); drop(sql); let _ = std::fs::remove_dir_all(&dir);
}
