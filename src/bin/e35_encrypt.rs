//! Experiment 35: client-side encryption of everything that goes to the bucket (AES-256-GCM after zstd, key from a
//! passphrase the user holds), multipart snapshot upload, and what a wrong key or a tampered object does on restore.
use sqweezdb::cloud::{clear_prefix, download, pack_bytes, start_puller, start_uploader, unpack_bytes, CloudConfig};
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::Value;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const N: i64 = 1_000_000;
const DIR: &str = "data/nocow/e35dir"; const STORE: &str = "data/nocow/s3store";
fn count(sql: &Sql) -> i64 { let mut s = Session::default(); match sql.exec_s(&mut s, "SELECT count(*) FROM t", &[]).unwrap()[0][0] { Value::Int(i) => i, _ => -1 } }
fn mb(b: usize) -> f64 { b as f64 / 1e6 }
fn objects(sub: &str) -> Vec<PathBuf> { let mut out = vec![]; fn walk(p: &std::path::Path, out: &mut Vec<PathBuf>) { if let Ok(rd) = fs::read_dir(p) { for e in rd.flatten() { if e.metadata().unwrap().is_dir() { walk(&e.path(), out); } else { out.push(e.path()); } } } } walk(&PathBuf::from(format!("{STORE}/sqweezdb/{sub}")), &mut out); out.sort(); out }
fn build(dir: &PathBuf) -> Sql { let sql = Sql::recreate(dir, false); let mut s = Session::default(); sql.exec_s(&mut s, "CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, ts INT)", &[]).unwrap(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?)").unwrap(); for id in 1..=N { if id % 1000 == 1 { sql.exec_s(&mut s, "BEGIN", &[]).unwrap(); } sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Float((id % 1000) as f64), Value::Int(id % 20), Value::Int(id * 7919 % 1_000_003)]).unwrap(); if id % 1000 == 0 { sql.exec_s(&mut s, "COMMIT", &[]).unwrap(); } } sql }

fn main() {
    println!("=== E35: encryption before upload (AES-256-GCM after zstd; 1M rows; local rclone S3 server) ===");
    clear_prefix(&CloudConfig::local("sqweezdb", "")); std::thread::sleep(Duration::from_secs(2));
    let dir = PathBuf::from(format!("{DIR}/primary")); let sql = Arc::new(build(&dir)); let st = sql.snapshot();
    // ---- A. CPU cost ----
    let snap = fs::read(dir.join("snap")).unwrap(); let mut plain = CloudConfig::local("sqweezdb", "x"); plain.compress = false; let zonly = CloudConfig::local("sqweezdb", "x"); let eonly = { let mut c = plain.clone(); c = c.with_passphrase("correct horse battery staple"); c }; let both = CloudConfig::local("sqweezdb", "x").with_passphrase("correct horse battery staple");
    for (label, cfg) in [("zstd only", &zonly), ("AES-256-GCM only", &eonly), ("zstd then AES-256-GCM", &both)] { let t = Instant::now(); let out = pack_bytes(&snap, cfg); let el = t.elapsed(); let t2 = Instant::now(); let back = unpack_bytes(&out, cfg).unwrap(); let el2 = t2.elapsed(); println!("A. snapshot {:.1} MB, {label}: {:.1} MB in {:.0} ms ({:.0} MB/s), back in {:.0} ms ({:.0} MB/s), round trip {}", mb(snap.len()), mb(out.len()), el.as_secs_f64() * 1e3, mb(snap.len()) / el.as_secs_f64(), el2.as_secs_f64() * 1e3, mb(snap.len()) / el2.as_secs_f64(), if back == snap { "exact" } else { "MISMATCH" }); }
    // ---- B. upload encrypted; what the bucket sees ----
    let mut cfg = CloudConfig::local("sqweezdb", "primary").with_passphrase("correct horse battery staple"); cfg.compact_after = Duration::from_secs(5); cfg.compact_every = Duration::from_secs(2);
    let t = Instant::now(); let up = start_uploader(cfg.clone(), dir.clone(), sql.db.log.clone()); while up.stats.snapshots.load(Ordering::Relaxed) == 0 { std::thread::sleep(Duration::from_millis(20)); }
    let objs = objects("primary/snap"); let raw = fs::read(&objs[0]).unwrap(); let again = zstd::encode_all(&raw[..], 3).unwrap();
    println!("B. snapshot uploaded encrypted in {:.2} s: {:.1} MB in {} multipart parts; key {}; the object starts {} and compresses to {:.3}x of its size (random-looking)", t.elapsed().as_secs_f64(), mb(raw.len()), up.stats.parts.load(Ordering::Relaxed), objs[0].file_name().unwrap().to_string_lossy(), raw[..12].iter().map(|b| format!("{b:02x}")).collect::<Vec<_>>().join(" "), again.len() as f64 / raw.len() as f64);
    { let mut s = Session::default(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?)").unwrap(); let t = Instant::now(); let mut id = N + 1; while t.elapsed() < Duration::from_secs(3) { sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Float((id % 977) as f64), Value::Int(id % 20), Value::Int(id * 7919 % 1_000_003)]).unwrap(); id += 1; std::thread::sleep(Duration::from_micros(400)); } let seq = sql.current_seq(); while up.stats.cloud_seq.load(Ordering::Relaxed) < seq { std::thread::sleep(Duration::from_millis(20)); } }
    sql.db.log.set_cloud_wait(true, Some(up.kick.clone()));
    let done = Arc::new(AtomicU64::new(0)); let stop = Arc::new(AtomicBool::new(false)); let lat = Arc::new(std::sync::Mutex::new(Vec::<f64>::new()));
    let hs: Vec<_> = (0..256usize).map(|k| { let (sql, done, stop, lat) = (sql.clone(), done.clone(), stop.clone(), lat.clone()); std::thread::spawn(move || { let mut s = Session::default(); let ins = sql.prepare("INSERT INTO t VALUES (?,?,?,?)").unwrap(); let mut id = 10_000_000 + k as i64 * 1_000_000; let mut l = vec![]; while !stop.load(Ordering::Relaxed) { let t = Instant::now(); sql.execute_s(&mut s, &ins, &[Value::Int(id), Value::Float(1.0), Value::Int(1), Value::Int(id)]).unwrap(); l.push(t.elapsed().as_secs_f64() * 1e3); done.fetch_add(1, Ordering::Relaxed); id += 1; } lat.lock().unwrap().extend(l); }) }).collect();
    std::thread::sleep(Duration::from_secs(3)); stop.store(true, Ordering::Relaxed); for h in hs { h.join().unwrap(); } sql.db.log.set_cloud_wait(false, None);
    let mut l = lat.lock().unwrap().clone(); l.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!("B. cloud-durable commits with compression + encryption, 256 writers: {:.0} commits/s, latency p50 {:.1} ms (E33 without either: 41,392/s at 5.0 ms)", done.load(Ordering::Relaxed) as f64 / 3.0, l[l.len() / 2]);
    // ---- C. restore: right key, wrong key, tampered object ----
    let rdir = PathBuf::from(format!("{DIR}/restored")); let t = Instant::now(); let (_, chunks, bytes) = download(&cfg, &rdir).unwrap(); let (r, _) = Sql::open(&rdir, false, 12).unwrap(); println!("C. restore with the right key: {chunks} objects, {:.1} MB, {:.2} s -> {} rows vs primary {} ({})", mb(bytes as usize), t.elapsed().as_secs_f64(), count(&r), count(&sql), if count(&r) == count(&sql) { "EXACT" } else { "MISMATCH" }); drop(r);
    let wrong = CloudConfig::local("sqweezdb", "primary").with_passphrase("incorrect horse"); println!("C. restore with a wrong passphrase: {}", match download(&wrong, &rdir) { Ok(_) => "ACCEPTED (bad)".into(), Err(e) => format!("refused: {}", e.chars().take(120).collect::<String>()) });
    let nokey = CloudConfig::local("sqweezdb", "primary"); println!("C. restore with no key: {}", match download(&nokey, &rdir) { Ok(_) => "ACCEPTED (bad)".into(), Err(e) => format!("refused: {}", e.chars().take(120).collect::<String>()) });
    // tamper: flip one byte in the middle of the newest log chunk in the bucket's backing store
    std::thread::sleep(Duration::from_millis(1500)); let logs = objects("primary/log"); let victim = logs.last().unwrap().clone(); let mut bytes_v = fs::read(&victim).unwrap(); let mid = bytes_v.len() / 2; bytes_v[mid] ^= 0x01; fs::write(&victim, &bytes_v).unwrap(); std::thread::sleep(Duration::from_millis(1500));
    println!("C. one bit flipped inside {} in the bucket: restore {}", victim.file_name().unwrap().to_string_lossy(), match download(&cfg, &rdir) { Ok(_) => "ACCEPTED (bad)".into(), Err(e) => format!("refused: {}", e.chars().take(140).collect::<String>()) });
    bytes_v[mid] ^= 0x01; fs::write(&victim, &bytes_v).unwrap(); std::thread::sleep(Duration::from_millis(1500));
    println!("C. bit restored: restore {}", match download(&cfg, &rdir) { Ok((_, c, _)) => format!("ok, {c} objects"), Err(e) => format!("refused: {e}") });
    // ---- D. follower from the encrypted bucket ----
    let fdir = PathBuf::from(format!("{DIR}/follower")); download(&cfg, &fdir).unwrap(); let (fol, fh) = Sql::follow(&fdir, Duration::from_millis(5)).unwrap(); let puller = start_puller(cfg.clone(), fdir.clone(), Duration::from_millis(20));
    let mut s = Session::default(); let get = fol.prepare("SELECT id FROM t WHERE id = ?").unwrap(); let mut fs_ = Session::default(); let mut lags = vec![]; let mut c2 = cfg.clone(); c2.interval = Duration::from_millis(100); drop(up); let up2 = start_uploader(c2, dir.clone(), sql.db.log.clone());
    for k in 0..10i64 { let id = 900_000_000 + k; let t = Instant::now(); sql.exec_s(&mut s, "INSERT INTO t VALUES (?,?,?,?)", &[Value::Int(id), Value::Float(1.0), Value::Int(1), Value::Int(0)]).unwrap(); loop { if !fol.execute_s(&mut fs_, &get, &[Value::Int(id)]).unwrap().is_empty() { break; } std::thread::sleep(Duration::from_millis(1)); if t.elapsed() > Duration::from_secs(30) { break; } } lags.push(t.elapsed().as_secs_f64() * 1e3); std::thread::sleep(Duration::from_millis(100)); }
    lags.sort_by(|a, b| a.partial_cmp(b).unwrap()); let seq = sql.current_seq(); let t = Instant::now(); while fh.applied() < seq && t.elapsed() < Duration::from_secs(10) { std::thread::sleep(Duration::from_millis(5)); }
    println!("D. follower fed by the encrypted bucket: {} rows vs primary {} ({}); a commit is visible after {:.0} ms (median, 100 ms cadence)", count(&fol), count(&sql), if count(&fol) == count(&sql) { "EXACT" } else { "MISMATCH" }, lags[5]);
    println!("E. Litestream 0.5.17: no encryption option in the binary or its example configuration; SQLite itself encrypts only with the commercial SEE extension or third-party SQLCipher");
    drop(get); drop(puller); drop(fh); drop(fol); drop(up2); drop(sql); let _ = fs::remove_dir_all(DIR); let _ = st;
}
