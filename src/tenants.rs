//! Database-per-tenant hosting (E38): tenants are directories under a root, opened on demand, kept resident up to a
//! limit and evicted to disk (a snapshot, then drop; reopening maps the snapshot in a millisecond). Fleet operations run
//! the same statement on every tenant; backups go to one bucket prefix per tenant, optionally with a key per tenant.
use crate::cloud::{start_uploader, CloudConfig, Uploader};
use crate::sql::{Session, Sql};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct Open { sql: Arc<Sql>, last_used: Instant, uploader: Option<Uploader> }
const SHARDS: usize = 64;
/// Residency is tracked in 64 shards by tenant name, so opening one tenant never waits on another; eviction is a
/// per-shard least-recently-used choice. An evicted tenant is simply dropped: its log holds everything and reopening
/// replays it; a snapshot is taken first only when the unsnapshotted tail is large.
pub struct TenantHost { pub root: PathBuf, open: Vec<Mutex<HashMap<String, Open>>>, pub max_open: usize, pub idle: Duration, pub opens: AtomicU64, pub evictions: AtomicU64, pub open_us: AtomicU64, pub durable: bool, pub snapshot_tail: u64 }
impl TenantHost {
    pub fn new(root: &std::path::Path, max_open: usize, idle: Duration, durable: bool) -> TenantHost { std::fs::create_dir_all(root).ok(); TenantHost { root: root.to_path_buf(), open: (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect(), max_open, idle, opens: AtomicU64::new(0), evictions: AtomicU64::new(0), open_us: AtomicU64::new(0), durable, snapshot_tail: 1 << 20 } }
    fn shard(name: &str) -> usize { let mut h: u64 = 0xcbf29ce484222325; for b in name.bytes() { h ^= b as u64; h = h.wrapping_mul(0x100000001b3); } (h % SHARDS as u64) as usize }
    fn dir(&self, name: &str) -> PathBuf { self.root.join(name) }
    pub fn exists(&self, name: &str) -> bool { self.dir(name).exists() }
    /// The tenant's database, opened (or created with `init` statements) if needed; evicts the least recently used tenant beyond `max_open`.
    pub fn get(&self, name: &str, init: &[&str]) -> Result<Arc<Sql>, String> {
        let shard = &self.open[Self::shard(name)];
        { let mut open = shard.lock().unwrap(); if let Some(o) = open.get_mut(name) { o.last_used = Instant::now(); return Ok(o.sql.clone()); } }
        let t = Instant::now(); let dir = self.dir(name);
        let sql = if dir.exists() { Arc::new(Sql::open_idle(&dir, self.durable, self.idle)?.0) } else { let s = Sql::create_idle(&dir, self.durable, self.idle); let mut se = Session::default(); for q in init { s.exec_s(&mut se, q, &[])?; } Arc::new(s) };
        self.opens.fetch_add(1, Ordering::Relaxed); self.open_us.fetch_add(t.elapsed().as_micros() as u64, Ordering::Relaxed);
        let mut open = shard.lock().unwrap();
        if let Some(o) = open.get(name) { return Ok(o.sql.clone()); }  // someone else opened it meanwhile
        let victim = if open.len() >= self.max_open.div_ceil(SHARDS).max(1) { open.iter().min_by_key(|(_, o)| o.last_used).map(|(k, _)| k.clone()).and_then(|k| open.remove(&k)) } else { None };
        open.insert(name.to_string(), Open { sql: sql.clone(), last_used: Instant::now(), uploader: None });
        drop(open);
        if let Some(o) = victim { let tail = { let st = o.sql.db.log.st.lock().unwrap(); st.bytes }; if tail > self.snapshot_tail { o.sql.snapshot(); } self.evictions.fetch_add(1, Ordering::Relaxed); }
        Ok(sql)
    }
    pub fn resident(&self) -> usize { self.open.iter().map(|m| m.lock().unwrap().len()).sum() }
    /// Every tenant on disk.
    pub fn all(&self) -> Vec<String> { let mut v: Vec<String> = std::fs::read_dir(&self.root).map(|rd| rd.flatten().filter(|e| e.path().is_dir()).map(|e| e.file_name().to_string_lossy().into_owned()).collect()).unwrap_or_default(); v.sort(); v }
    /// Run one statement on every tenant (resident or not); returns (tenants, failures, seconds). Schema changes stay online per tenant.
    pub fn fleet(&self, sql_text: &str, wait_alters: bool) -> (usize, usize, f64) {
        let t = Instant::now(); let mut n = 0; let mut bad = 0;
        for name in self.all() { match self.get(&name, &[]) { Ok(s) => { let mut se = Session::default(); if s.exec_s(&mut se, sql_text, &[]).is_err() { bad += 1; } else if wait_alters && s.wait_alters().is_err() { bad += 1; } n += 1; } Err(_) => bad += 1 } }
        (n, bad, t.elapsed().as_secs_f64())
    }
    /// Continuous backup of a resident tenant under `prefix/<name>`, with its own passphrase if given.
    pub fn backup(&self, name: &str, mut cfg: CloudConfig, passphrase: Option<&str>) -> Result<(), String> {
        let mut open = self.open[Self::shard(name)].lock().unwrap(); let o = open.get_mut(name).ok_or("tenant not resident")?;
        cfg.prefix = format!("{}/{name}", cfg.prefix.trim_matches('/')); if let Some(p) = passphrase { cfg = cfg.with_passphrase(p); }
        o.sql.snapshot(); o.uploader = Some(start_uploader(cfg, self.dir(name), o.sql.db.log.clone())); Ok(())
    }
    pub fn backup_done(&self, name: &str) -> bool { self.open[Self::shard(name)].lock().unwrap().get(name).and_then(|o| o.uploader.as_ref()).map(|u| u.stats.snapshots.load(Ordering::Relaxed) > 0 && u.stats.cloud_seq.load(Ordering::Relaxed) > 0 || u.stats.snapshots.load(Ordering::Relaxed) > 0).unwrap_or(false) }
    pub fn stop_backup(&self, name: &str) { if let Some(o) = self.open[Self::shard(name)].lock().unwrap().get_mut(name) { o.uploader = None; } }
}
