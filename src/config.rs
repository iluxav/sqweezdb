//! Configuration for `Sql::open_with`. One set of keys, three sources, one precedence: options passed from code
//! (the bindings send them as JSON) win over a TOML file, which wins over the defaults. The file is read only when
//! the application names it (`configFile`): the data directory is data, and a writable one must not be able to
//! redirect backups, open listeners or read the environment. Strings in the file may reference environment
//! variables as `${NAME}`; nothing is read from the environment unless the named file asks for it.
//! Durations are `"5ms"`, `"1s"`, `"30s"`, `"2m"`, `"1h"`, `"7d"` or a number of milliseconds; sizes are
//! `"256MB"`, `"1GB"` or a number of bytes; `0` turns a policy off.
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// A write returns when it is visible; the fsync follows within a batch (default).
    Visible,
    /// Every commit waits for its fsync; concurrent writers share one (group commit).
    Fsync,
    /// Every commit waits until its record is in the backup bucket.
    Cloud,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub durable: Durability,
    /// Create the directory when it holds no database (default true). Never wipes an existing one.
    pub create: bool,
    pub recover_threads: usize,
    pub backup: Option<Backup>,
    pub follow: Option<Follow>,
    pub snapshot: SnapshotPolicy,
    pub listen: Listen,
    pub live: LivePolicy,
    pub log: LogPolicy,
}
impl Default for Config {
    fn default() -> Config { Config { durable: Durability::Visible, create: true, recover_threads: 4, backup: None, follow: None, snapshot: SnapshotPolicy::default(), listen: Listen::default(), live: LivePolicy::default(), log: LogPolicy::default() } }
}
impl Config {
    pub fn durable(mut self, d: Durability) -> Config { self.durable = d; self }
    /// The effective configuration: defaults, then the TOML file the options name as `configFile` (if any), then the
    /// options themselves. `_dir` is the database directory; nothing is read from it.
    pub fn load(_dir: &Path, json: Option<&str>) -> Result<Config, String> {
        let code: ConfigFile = match json { Some(j) if !j.trim().is_empty() && j.trim() != "null" => serde_json::from_str(j).map_err(|e| format!("options: {e}"))?, _ => ConfigFile::default() };
        let mut file = ConfigFile::default();
        if let Some(p) = &code.config_file {
            let path = Path::new(p);
            let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
            let text = interpolate(&text).map_err(|e| format!("{}: {e}", path.display()))?;
            file = toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
            if file.config_file.is_some() { return Err(format!("{}: configFile cannot be set from a file", path.display())); }
        }
        file.overlay(code).resolve()
    }
    /// Options alone (no file), as the bindings send them.
    pub fn from_json(json: &str) -> Result<Config, String> { let code: ConfigFile = serde_json::from_str(json).map_err(|e| format!("options: {e}"))?; code.resolve() }
}

/// Continuous backup to an S3-compatible bucket (see `cloud.rs`).
#[derive(Clone, Debug)]
pub struct Backup {
    pub endpoint: String, pub bucket: String, pub prefix: String, pub region: String, pub access_key: String, pub secret_key: String,
    /// AES-256-GCM under a key derived from this passphrase; `None` uploads plaintext (still compressed).
    pub passphrase: Option<String>,
    pub interval: Duration, pub compress: bool, pub compact_after: Duration, pub compact_every: Duration,
    /// When the directory is empty and the bucket is not, restore the latest state before opening.
    pub bootstrap: bool,
}
impl Backup {
    pub fn cloud(&self) -> crate::cloud::CloudConfig {
        let mut c = crate::cloud::CloudConfig::local(&self.bucket, &self.prefix);
        c.endpoint = self.endpoint.clone(); c.access_key = self.access_key.clone(); c.secret_key = self.secret_key.clone(); c.region = self.region.clone();
        c.interval = self.interval; c.compress = self.compress; c.compact_after = self.compact_after; c.compact_every = self.compact_every; c.rtt = Duration::ZERO;
        match &self.passphrase { Some(p) => c.with_passphrase(p), None => c }
    }
}
/// Open as a read-only replica of a primary on this host (its directory) or of a bucket a primary backs up to.
#[derive(Clone, Debug)]
pub enum Follow { Dir { dir: PathBuf, poll: Duration }, Bucket { source: Backup, poll: Duration } }
/// When the engine writes a snapshot on its own. By size: once the log written since the last snapshot exceeds the
/// larger of `log_bytes` and that snapshot's own size, so a big table is not rewritten for every few megabytes of log
/// (write amplification stays under 2x) and a restart never replays more than about one snapshot's worth of log
/// (E42: 350 MB/s on the lab machine, 64 MB in 0.2 s). By time: after `idle_after` without a write, when something
/// was written since the last snapshot, so a quiet database always restarts from a fresh image. `None` disables a trigger.
#[derive(Clone, Debug)]
pub struct SnapshotPolicy { pub log_bytes: Option<u64>, pub idle_after: Option<Duration> }
impl Default for SnapshotPolicy { fn default() -> Self { SnapshotPolicy { log_bytes: Some(64 << 20), idle_after: Some(Duration::from_secs(30)) } } }
/// Doors for other processes: a Unix socket path, a TCP address, the web console's HTTP address; the last two need the shared token.
#[derive(Clone, Debug, Default)]
pub struct Listen { pub socket: Option<PathBuf>, pub tcp: Option<String>, pub http: Option<String>, pub token: Option<String> }
#[derive(Clone, Debug)]
pub struct LivePolicy { pub debounce: Duration, pub queue: usize }
impl Default for LivePolicy { fn default() -> Self { LivePolicy { debounce: Duration::from_millis(5), queue: 65536 } } }
#[derive(Clone, Debug, Default)]
pub struct LogPolicy { /// Log threads leave after this long without writes and return on the next one (many idle databases per process).
    pub threads_idle_after: Option<Duration> }

// ---------- the file / JSON form: every field optional, overlaid in precedence order ----------

#[derive(Deserialize, Debug, Clone)] #[serde(untagged)] pub enum DurableIn { Flag(bool), Name(String) }
#[derive(Deserialize, Debug, Clone)] #[serde(untagged)] pub enum Dur { Millis(u64), Text(String) }
#[derive(Deserialize, Debug, Clone)] #[serde(untagged)] pub enum Size { Bytes(u64), Text(String) }

#[derive(Deserialize, Default, Debug, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ConfigFile {
    /// Path of a TOML file with these same keys, read before the options and overridden by them. Only from code.
    #[serde(alias = "config_file")] pub config_file: Option<String>,
    pub durable: Option<DurableIn>, pub create: Option<bool>,
    #[serde(alias = "recover_threads")] pub recover_threads: Option<usize>,
    pub backup: Option<BackupFile>, pub follow: Option<FollowFile>, pub snapshot: Option<SnapshotFile>, pub listen: Option<ListenFile>, pub live: Option<LiveFile>, pub log: Option<LogFile>,
}
#[derive(Deserialize, Default, Debug, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct BackupFile {
    pub endpoint: Option<String>, pub bucket: Option<String>, pub prefix: Option<String>, pub region: Option<String>,
    #[serde(alias = "access_key")] pub access_key: Option<String>, #[serde(alias = "secret_key")] pub secret_key: Option<String>,
    pub passphrase: Option<String>, pub interval: Option<Dur>, pub compress: Option<bool>,
    #[serde(alias = "compact_after")] pub compact_after: Option<Dur>, #[serde(alias = "compact_every")] pub compact_every: Option<Dur>,
    pub bootstrap: Option<bool>,
}
#[derive(Deserialize, Default, Debug, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FollowFile { pub dir: Option<String>, pub bucket: Option<BackupFile>, pub poll: Option<Dur> }
#[derive(Deserialize, Default, Debug, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SnapshotFile { #[serde(alias = "log_bytes")] pub log_bytes: Option<Size>, #[serde(alias = "idle_after")] pub idle_after: Option<Dur> }
#[derive(Deserialize, Default, Debug, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ListenFile { pub socket: Option<String>, pub tcp: Option<String>, pub http: Option<String>, pub token: Option<String> }
#[derive(Deserialize, Default, Debug, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LiveFile { pub debounce: Option<Dur>, pub queue: Option<usize> }
#[derive(Deserialize, Default, Debug, Clone)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LogFile { #[serde(alias = "threads_idle_after")] pub threads_idle_after: Option<Dur> }

fn pick<T>(base: Option<T>, over: Option<T>) -> Option<T> { over.or(base) }
fn nest<T>(base: Option<T>, over: Option<T>, f: impl Fn(T, T) -> T) -> Option<T> { match (base, over) { (Some(a), Some(b)) => Some(f(a, b)), (a, b) => b.or(a) } }
impl BackupFile { fn overlay(self, o: BackupFile) -> BackupFile { BackupFile { endpoint: pick(self.endpoint, o.endpoint), bucket: pick(self.bucket, o.bucket), prefix: pick(self.prefix, o.prefix), region: pick(self.region, o.region), access_key: pick(self.access_key, o.access_key), secret_key: pick(self.secret_key, o.secret_key), passphrase: pick(self.passphrase, o.passphrase), interval: pick(self.interval, o.interval), compress: pick(self.compress, o.compress), compact_after: pick(self.compact_after, o.compact_after), compact_every: pick(self.compact_every, o.compact_every), bootstrap: pick(self.bootstrap, o.bootstrap) } } }
impl ConfigFile {
    pub fn overlay(self, o: ConfigFile) -> ConfigFile {
        ConfigFile {
            config_file: pick(self.config_file, o.config_file),
            durable: pick(self.durable, o.durable), create: pick(self.create, o.create), recover_threads: pick(self.recover_threads, o.recover_threads),
            backup: nest(self.backup, o.backup, |a, b| a.overlay(b)),
            follow: nest(self.follow, o.follow, |a, b| FollowFile { dir: pick(a.dir, b.dir), bucket: nest(a.bucket, b.bucket, |x, y| x.overlay(y)), poll: pick(a.poll, b.poll) }),
            snapshot: nest(self.snapshot, o.snapshot, |a, b| SnapshotFile { log_bytes: pick(a.log_bytes, b.log_bytes), idle_after: pick(a.idle_after, b.idle_after) }),
            listen: nest(self.listen, o.listen, |a, b| ListenFile { socket: pick(a.socket, b.socket), tcp: pick(a.tcp, b.tcp), http: pick(a.http, b.http), token: pick(a.token, b.token) }),
            live: nest(self.live, o.live, |a, b| LiveFile { debounce: pick(a.debounce, b.debounce), queue: pick(a.queue, b.queue) }),
            log: nest(self.log, o.log, |a, b| LogFile { threads_idle_after: pick(a.threads_idle_after, b.threads_idle_after) }),
        }
    }
    /// Defaults, validation, and typed values. Errors name the key.
    pub fn resolve(self) -> Result<Config, String> {
        let mut c = Config::default();
        c.durable = match self.durable { None | Some(DurableIn::Flag(false)) => Durability::Visible, Some(DurableIn::Flag(true)) => Durability::Fsync,
            Some(DurableIn::Name(s)) => match s.to_ascii_lowercase().as_str() { "false" | "visible" => Durability::Visible, "true" | "fsync" | "disk" => Durability::Fsync, "cloud" => Durability::Cloud, other => return Err(format!("durable: expected false, true or \"cloud\", got {other:?}")) } };
        if let Some(v) = self.create { c.create = v; }
        if let Some(n) = self.recover_threads { c.recover_threads = n.max(1); }
        if let Some(b) = self.backup { c.backup = Some(resolve_backup(b, "backup")?); }
        if let Some(f) = self.follow {
            let poll = dur_or(f.poll, Duration::from_millis(100), "follow.poll")?;
            c.follow = Some(match (f.dir, f.bucket) {
                (Some(d), None) => Follow::Dir { dir: PathBuf::from(d), poll },
                (None, Some(b)) => Follow::Bucket { source: resolve_backup(b, "follow.bucket")?, poll },
                (Some(_), Some(_)) => return Err("follow: give either dir or bucket, not both".into()),
                (None, None) => return Err("follow: needs dir or bucket".into()),
            });
            if c.backup.is_some() { return Err("follow and backup together: a follower does not back up; configure backup on the primary".into()); }
        }
        if c.durable == Durability::Cloud && c.backup.is_none() { return Err("durable = \"cloud\" needs a backup section".into()); }
        if let Some(s) = self.snapshot {
            if let Some(v) = s.log_bytes { let n = size_of(v, "snapshot.logBytes")?; c.snapshot.log_bytes = if n == 0 { None } else { Some(n) }; }
            if let Some(v) = s.idle_after { let d = dur_of(v, "snapshot.idleAfter")?; c.snapshot.idle_after = if d.is_zero() { None } else { Some(d) }; }
        }
        if let Some(l) = self.listen {
            c.listen.socket = l.socket.map(PathBuf::from); c.listen.tcp = l.tcp; c.listen.http = l.http; c.listen.token = l.token.filter(|t| !t.is_empty());
            if c.listen.tcp.is_some() && c.listen.token.is_none() { return Err("listen.tcp needs listen.token (a shared secret every connection presents first)".into()); }
            if c.listen.http.is_some() && c.listen.token.is_none() { return Err("listen.http needs listen.token (the web console asks for it)".into()); }
        }
        if let Some(l) = self.live { if let Some(d) = l.debounce { c.live.debounce = dur_of(d, "live.debounce")?; } if let Some(q) = l.queue { c.live.queue = q.max(16); } }
        if let Some(l) = self.log { if let Some(d) = l.threads_idle_after { let d = dur_of(d, "log.threadsIdleAfter")?; c.log.threads_idle_after = if d.is_zero() { None } else { Some(d) }; } }
        Ok(c)
    }
}
fn resolve_backup(b: BackupFile, key: &str) -> Result<Backup, String> {
    let need = |v: Option<String>, k: &str| v.filter(|s| !s.is_empty()).ok_or_else(|| format!("{key}.{k} is required"));
    let d = crate::cloud::CloudConfig::local("", "");
    Ok(Backup {
        endpoint: need(b.endpoint, "endpoint")?, bucket: need(b.bucket, "bucket")?, prefix: b.prefix.unwrap_or_default(), region: b.region.unwrap_or_else(|| "us-east-1".into()),
        access_key: need(b.access_key, "accessKey")?, secret_key: need(b.secret_key, "secretKey")?, passphrase: b.passphrase.filter(|p| !p.is_empty()),
        interval: dur_or(b.interval, d.interval, &format!("{key}.interval"))?, compress: b.compress.unwrap_or(true),
        compact_after: dur_or(b.compact_after, d.compact_after, &format!("{key}.compactAfter"))?, compact_every: dur_or(b.compact_every, d.compact_every, &format!("{key}.compactEvery"))?,
        bootstrap: b.bootstrap.unwrap_or(false),
    })
}
fn dur_or(v: Option<Dur>, default: Duration, key: &str) -> Result<Duration, String> { match v { Some(d) => dur_of(d, key), None => Ok(default) } }
fn dur_of(v: Dur, key: &str) -> Result<Duration, String> { match v { Dur::Millis(ms) => Ok(Duration::from_millis(ms)), Dur::Text(s) => parse_duration(&s).map_err(|e| format!("{key}: {e}")) } }
fn size_of(v: Size, key: &str) -> Result<u64, String> { match v { Size::Bytes(n) => Ok(n), Size::Text(s) => parse_size(&s).map_err(|e| format!("{key}: {e}")) } }

fn split_unit(s: &str) -> Result<(f64, String), String> {
    let s = s.trim(); let i = s.find(|c: char| !(c.is_ascii_digit() || c == '.')).unwrap_or(s.len());
    let v: f64 = s[..i].parse().map_err(|_| format!("not a number: {s:?}"))?; Ok((v, s[i..].trim().to_string()))
}
/// `"5ms"`, `"1.5s"`, `"2m"`, `"1h"`, `"7d"`; a bare number is seconds.
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let (v, u) = split_unit(s)?;
    let secs = match u.as_str() { "ms" => v / 1000.0, "" | "s" => v, "m" | "min" => v * 60.0, "h" => v * 3600.0, "d" => v * 86400.0, other => return Err(format!("unknown time unit {other:?} in {s:?}")) };
    if secs < 0.0 || !secs.is_finite() { return Err(format!("bad duration {s:?}")); }
    Ok(Duration::from_secs_f64(secs))
}
/// `"256MB"`, `"1GB"`, `"64KB"`, `"4096"` (bytes); KiB/MiB/GiB mean the same as KB/MB/GB here.
pub fn parse_size(s: &str) -> Result<u64, String> {
    let (v, u) = split_unit(s)?;
    let mult = match u.to_ascii_uppercase().as_str() { "" | "B" => 1u64, "K" | "KB" | "KIB" => 1 << 10, "M" | "MB" | "MIB" => 1 << 20, "G" | "GB" | "GIB" => 1 << 30, "T" | "TB" | "TIB" => 1 << 40, other => return Err(format!("unknown size unit {other:?} in {s:?}")) };
    Ok((v * mult as f64) as u64)
}
/// Replace every `${NAME}` with the environment variable; an unset one is an error, never an empty string.
pub fn interpolate(text: &str) -> Result<String, String> {
    let mut out = String::with_capacity(text.len()); let mut rest = text;
    while let Some(i) = rest.find("${") {
        out.push_str(&rest[..i]); let after = &rest[i + 2..];
        let Some(j) = after.find('}') else { return Err("unterminated ${ in configuration".into()) };
        let name = &after[..j];
        let val = std::env::var(name).map_err(|_| format!("environment variable {name} is not set (referenced as ${{{name}}})"))?;
        out.push_str(&val); rest = &after[j + 1..];
    }
    out.push_str(rest); Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn units() {
        assert_eq!(parse_duration("5ms").unwrap(), Duration::from_millis(5)); assert_eq!(parse_duration("1.5s").unwrap(), Duration::from_millis(1500));
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120)); assert_eq!(parse_duration("7d").unwrap(), Duration::from_secs(7 * 86400));
        assert_eq!(parse_size("256MB").unwrap(), 256 << 20); assert_eq!(parse_size("1GB").unwrap(), 1 << 30); assert_eq!(parse_size("4096").unwrap(), 4096);
        assert!(parse_duration("3 weeks").is_err()); assert!(parse_size("5 bananas").is_err());
    }
    #[test] fn defaults_and_json() {
        let c = Config::from_json("{}").unwrap();
        assert_eq!(c.durable, Durability::Visible); assert!(c.create); assert_eq!(c.snapshot.log_bytes, Some(64 << 20)); assert_eq!(c.live.debounce, Duration::from_millis(5));
        let c = Config::from_json(r#"{"durable":"cloud","backup":{"endpoint":"http://x","bucket":"b","accessKey":"k","secretKey":"s","interval":"250ms","passphrase":"p"},"snapshot":{"logBytes":"64MB","idleAfter":0},"listen":{"tcp":"0.0.0.0:7781","token":"t"}}"#).unwrap();
        assert_eq!(c.durable, Durability::Cloud); let b = c.backup.unwrap(); assert_eq!(b.interval, Duration::from_millis(250)); assert!(b.passphrase.is_some()); assert!(b.cloud().key.is_some());
        assert_eq!(c.snapshot.log_bytes, Some(64 << 20)); assert_eq!(c.snapshot.idle_after, None); assert_eq!(c.listen.tcp.as_deref(), Some("0.0.0.0:7781"));
        assert!(Config::from_json(r#"{"durable":"cloud"}"#).is_err(), "cloud durability needs a bucket");
        assert!(Config::from_json(r#"{"listen":{"tcp":"0.0.0.0:1"}}"#).is_err(), "tcp needs a token");
        assert!(Config::from_json(r#"{"backup":{"bucket":"b"}}"#).unwrap_err().contains("endpoint"));
        assert!(Config::from_json(r#"{"snapsot":{}}"#).unwrap_err().contains("snapsot"), "typos are errors");
    }
    #[test] fn file_then_code_then_env() {
        let dir = std::env::temp_dir().join(format!("sqweezdb-config-test-{}", std::process::id())); std::fs::create_dir_all(&dir).unwrap();
        unsafe { std::env::set_var("SQWEEZDB_TEST_SECRET", "s3cr3t"); }
        let file = dir.join("orders.toml");
        std::fs::write(&file, "durable = true\n[backup]\nendpoint = \"http://127.0.0.1:9000\"\nbucket = \"from-file\"\naccessKey = \"k\"\nsecretKey = \"${SQWEEZDB_TEST_SECRET}\"\n[snapshot]\nlogBytes = \"64MB\"\n").unwrap();
        let json = format!(r#"{{"configFile":{:?},"backup":{{"bucket":"from-code"}},"snapshot":{{"idleAfter":"10s"}}}}"#, file.to_str().unwrap());
        let c = Config::load(&dir, Some(&json)).unwrap();
        assert_eq!(c.durable, Durability::Fsync, "from the file");
        let b = c.backup.unwrap(); assert_eq!(b.bucket, "from-code", "code wins over the file"); assert_eq!(b.secret_key, "s3cr3t", "secret from the environment"); assert_eq!(b.endpoint, "http://127.0.0.1:9000", "untouched file values survive the overlay");
        assert_eq!(c.snapshot.log_bytes, Some(64 << 20)); assert_eq!(c.snapshot.idle_after, Some(Duration::from_secs(10)));
        // a file inside the data directory is data, not configuration: it is never read unless named
        std::fs::write(dir.join("sqweezdb.toml"), "durable = true\n[listen]\ntcp = \"0.0.0.0:1\"\ntoken = \"x\"\n").unwrap();
        let c = Config::load(&dir, Some("{}")).unwrap(); assert_eq!(c.durable, Durability::Visible); assert!(c.listen.tcp.is_none());
        std::fs::write(&file, "configFile = \"other.toml\"\n").unwrap();
        assert!(Config::load(&dir, Some(&format!(r#"{{"configFile":{:?}}}"#, file.to_str().unwrap()))).unwrap_err().contains("cannot be set from a file"));
        std::fs::write(&file, "[backup]\nsecretKey = \"${SQWEEZDB_TEST_UNSET_VAR}\"\n").unwrap();
        assert!(Config::load(&dir, Some(&format!(r#"{{"configFile":{:?}}}"#, file.to_str().unwrap()))).unwrap_err().contains("SQWEEZDB_TEST_UNSET_VAR"));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
