//! Experiment 47: which unpredictable id is fastest here? Sequential INT keys (the baseline), random 63-bit and
//! JavaScript-safe random 53-bit INT keys (unpredictable, but on the native integer key path), and a public id made by
//! a keyed permutation of the sequential key (unpredictable outside, sequential inside, no extra index).
use sqweezdb::config::{Config, SnapshotPolicy};
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::{uuid_v4, Value};
use std::time::Instant;

const N: usize = 1_000_000; const ROOT: &str = "data/nocow/e47";
fn rnd(bits: u32) -> i64 { let b = uuid_v4(); let x = u64::from_le_bytes(b[..8].try_into().unwrap()); ((x >> (64 - bits)) as i64).max(1) }
fn pr(label: &str, n: usize, el: f64) { println!("{label:<66} {:>7.2} us {:>11.0}/s", el * 1e6 / n as f64, n as f64 / el); }
/// 4-round Feistel network on 64 bits keyed by `k`: a bijection, so every id maps to one public id and back.
/// (Cost model only: the round function is a strong mixer, not a vetted cipher; production would use e.g. Speck64/128.)
fn mix(x: u32, k: u64) -> u32 { let mut z = (x as u64) ^ k; z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9); z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb); (z ^ (z >> 31)) as u32 }
fn permute(v: u64, keys: &[u64; 4]) -> u64 { let (mut l, mut r) = ((v >> 32) as u32, v as u32); for k in keys { let t = r; r = l ^ mix(r, *k); l = t; } ((l as u64) << 32) | r as u64 }
fn unpermute(v: u64, keys: &[u64; 4]) -> u64 { let (mut l, mut r) = ((v >> 32) as u32, v as u32); for k in keys.iter().rev() { let t = l; l = r ^ mix(l, *k); r = t; } ((l as u64) << 32) | r as u64 }
fn run(label: &str, keys: &[i64]) {
    let d = std::path::Path::new(ROOT).join(label.replace(' ', "_")); let _ = std::fs::remove_dir_all(&d);
    let sql = Sql::open_with(&d, Config { snapshot: SnapshotPolicy { log_bytes: None, idle_after: None }, ..Config::default() }).unwrap(); let mut s = Session::default();
    sql.exec_s(&mut s, "CREATE TABLE t(id INT PRIMARY KEY, name VARCHAR(16))", &[]).unwrap();
    let ins = sql.prepare("INSERT INTO t VALUES (?, ?)").unwrap(); let t = Instant::now();
    for (i, k) in keys.iter().enumerate() { if i % 1000 == 0 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &[Value::Int(*k), Value::Text("x".into())]).unwrap(); if i % 1000 == 999 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } }
    pr(&format!("{label}: insert, 1000-row transactions"), N, t.elapsed().as_secs_f64());
    let get = sql.prepare("SELECT name FROM t WHERE id = ?").unwrap(); let t = Instant::now();
    for i in 0..200_000 { assert_eq!(sql.execute_s(&mut s, &get, &[Value::Int(keys[(i * 7919) % N])]).unwrap().len(), 1); }
    pr(&format!("{label}: point read by id"), 200_000, t.elapsed().as_secs_f64());
    let t = Instant::now(); let r = sql.exec_s(&mut s, "SELECT count(*) FROM t WHERE id >= 0", &[]).unwrap(); pr(&format!("{label}: full scan ({:?} rows)", r[0][0]), 1, t.elapsed().as_secs_f64());
    drop(sql); let _ = std::fs::remove_dir_all(&d);
}
fn main() {
    println!("=== E47: unpredictable ids on the integer key path (1M rows, in-process) ===");
    run("sequential 1..N", &(1..=N as i64).collect::<Vec<_>>());
    let mut seen = std::collections::HashSet::new(); let r63: Vec<i64> = (0..N).map(|_| loop { let k = rnd(63); if seen.insert(k) { break k; } }).collect();
    run("random 63-bit", &r63);
    seen.clear(); let mut dup53 = 0; let r53: Vec<i64> = (0..N).map(|_| loop { let k = rnd(53); if seen.insert(k) { break k; } dup53 += 1; }).collect();
    run("random 53-bit (JS-safe)", &r53); println!("    collisions while drawing 1M random 53-bit ids: {dup53}");
    let keys = { let b = uuid_v4(); let c = uuid_v4(); [u64::from_le_bytes(b[..8].try_into().unwrap()), u64::from_le_bytes(b[8..].try_into().unwrap()), u64::from_le_bytes(c[..8].try_into().unwrap()), u64::from_le_bytes(c[8..].try_into().unwrap())] };
    let t = Instant::now(); let mut x = 0u64; for i in 1..=10_000_000u64 { x ^= permute(i, &keys); } pr(&format!("keyed permutation id -> public id (check {})", x & 0xff), 10_000_000, t.elapsed().as_secs_f64());
    let t = Instant::now(); let mut ok = true; for i in 1..=10_000_000u64 { ok &= unpermute(permute(i, &keys), &keys) == i; } pr(&format!("permute + unpermute round trip (all exact: {ok})"), 10_000_000, t.elapsed().as_secs_f64());
    println!("    public ids for 1, 2, 3: {:016x} {:016x} {:016x}", permute(1, &keys), permute(2, &keys), permute(3, &keys));
    let _ = std::fs::remove_dir_all(ROOT);
}
