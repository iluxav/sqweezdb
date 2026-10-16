//! Connection for other processes: a length-prefixed binary protocol over a Unix domain socket (local), a
//! shared-memory ring (local, fastest) or TCP (other hosts, E40). The owning process runs `Server`; any other
//! process uses `Client`. One session per connection; LOGIN binds a user.
use crate::sql::{Session, Sql};
use crate::table::Value;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::Arc;

const K_LOGIN: u8 = 1; const K_EXEC: u8 = 2; const K_BATCH: u8 = 3; const K_SHM: u8 = 4; const K_SUB: u8 = 5; const K_AUTH: u8 = 6;
/// Where a local client may place its shared-memory ring; the server refuses any other path (it writes into that file).
const SHM_DIR: &str = "/dev/shm/"; const SHM_PREFIX: &str = "dblab-ring-";
fn same_secret(a: &[u8], b: &[u8]) -> bool { let mut d = (a.len() != b.len()) as u8; for i in 0..a.len().min(b.len()) { d |= a[i] ^ b[i]; } d == 0 }
// shared-memory ring: one outstanding request per connection, both sides spin briefly then back off
const SHM_SIZE: usize = 2 << 20; const REQ_OFF: usize = 64; const REQ_CAP: usize = 1 << 20; const RESP_OFF: usize = REQ_OFF + REQ_CAP; const RESP_CAP: usize = SHM_SIZE - RESP_OFF;
use std::sync::atomic::{AtomicU32, Ordering};
struct Ring { map: memmap2::MmapMut }
unsafe impl Send for Ring {}
impl Ring {
    fn open(path: &Path, create: bool) -> std::io::Result<Ring> {
        let f = std::fs::OpenOptions::new().read(true).write(true).create(create).open(path)?;
        if create { f.set_len(SHM_SIZE as u64)?; }
        else { let m = f.metadata()?; if !m.is_file() || m.len() != SHM_SIZE as u64 { return Err(std::io::Error::other("ring file must be a regular file of the ring size")); } }
        Ok(Ring { map: unsafe { memmap2::MmapMut::map_mut(&f)? } })
    }
    #[inline] fn word(&self, off: usize) -> &AtomicU32 { unsafe { &*(self.map.as_ptr().add(off) as *const AtomicU32) } }
    fn req_seq(&self) -> &AtomicU32 { self.word(0) } fn resp_seq(&self) -> &AtomicU32 { self.word(4) } fn req_len(&self) -> &AtomicU32 { self.word(8) } fn resp_len(&self) -> &AtomicU32 { self.word(12) } fn closed(&self) -> &AtomicU32 { self.word(16) }
    fn wait_for(&self, w: &AtomicU32, target: u32) -> bool {
        let mut spins = 0u32;
        loop {
            if w.load(Ordering::Acquire) == target { return true; }
            if self.closed().load(Ordering::Relaxed) != 0 { return false; }
            spins += 1;
            if spins < 20_000 { std::hint::spin_loop(); } else if spins < 22_000 { std::thread::yield_now(); } else { std::thread::sleep(std::time::Duration::from_micros(50)); }
        }
    }
    fn write_at(&mut self, off: usize, b: &[u8]) { self.map[off..off + b.len()].copy_from_slice(b); }
    fn read_at(&self, off: usize, n: usize) -> &[u8] { &self.map[off..off + n] }
}
fn dispatch(sql: &Sql, sess: &mut Session, req: &[u8], out: &mut Vec<u8>) {
    let kind = req[0]; let mut p = 1;
    match kind {
        K_LOGIN => { let n = u16::from_le_bytes(req[1..3].try_into().unwrap()) as usize; let name = String::from_utf8_lossy(&req[3..3 + n]).into_owned(); let r: Result<Vec<Vec<Value>>, String> = sql.login(sess, &name).map(|_| vec![]); encode_result(out, &r); }
        K_EXEC => { let r = match decode_exec(req, &mut p) { Ok((s, ps)) => sql.exec_s(sess, &s, &ps), Err(e) => Err(e) }; encode_result(out, &r); }
        K_BATCH => { let n = u32::from_le_bytes(req[1..5].try_into().unwrap()) as usize; p = 5; for _ in 0..n { let r = match decode_exec(req, &mut p) { Ok((s, ps)) => sql.exec_s(sess, &s, &ps), Err(e) => Err(e) }; encode_result(out, &r); } }
        _ => encode_result(out, &Err(format!("bad request kind {kind}"))),
    }
}

fn put_value(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => out.push(0),
        Value::Int(i) => { out.push(1); out.extend_from_slice(&i.to_le_bytes()); }
        Value::Float(f) => { out.push(2); out.extend_from_slice(&f.to_le_bytes()); }
        Value::Text(s) => { out.push(3); out.extend_from_slice(&(s.len() as u32).to_le_bytes()); out.extend_from_slice(s.as_bytes()); }
        Value::Bytes(b) => { out.push(4); out.extend_from_slice(&(b.len() as u32).to_le_bytes()); out.extend_from_slice(b); }
        Value::Bool(b) => { out.push(5); out.push(*b as u8); }
    }
}
fn get_value(b: &[u8], p: &mut usize) -> Result<Value, String> {
    let tag = *b.get(*p).ok_or("short value")?; *p += 1;
    Ok(match tag {
        0 => Value::Null,
        1 => { let v = i64::from_le_bytes(b[*p..*p + 8].try_into().map_err(|_| "short int")?); *p += 8; Value::Int(v) }
        2 => { let v = f64::from_le_bytes(b[*p..*p + 8].try_into().map_err(|_| "short float")?); *p += 8; Value::Float(v) }
        3 | 4 => { let n = u32::from_le_bytes(b[*p..*p + 4].try_into().map_err(|_| "short len")?) as usize; *p += 4; let d = b.get(*p..*p + n).ok_or("short payload")?.to_vec(); *p += n; if tag == 3 { Value::Text(String::from_utf8_lossy(&d).into_owned()) } else { Value::Bytes(d) } }
        5 => { let v = b[*p] != 0; *p += 1; Value::Bool(v) }
        t => return Err(format!("bad value tag {t}")),
    })
}
fn encode_exec(out: &mut Vec<u8>, sql: &str, params: &[Value]) {
    out.extend_from_slice(&(sql.len() as u32).to_le_bytes()); out.extend_from_slice(sql.as_bytes());
    out.extend_from_slice(&(params.len() as u16).to_le_bytes()); for v in params { put_value(out, v); }
}
fn decode_exec(b: &[u8], p: &mut usize) -> Result<(String, Vec<Value>), String> {
    let n = u32::from_le_bytes(b[*p..*p + 4].try_into().map_err(|_| "short")?) as usize; *p += 4;
    let sql = String::from_utf8_lossy(b.get(*p..*p + n).ok_or("short sql")?).into_owned(); *p += n;
    let np = u16::from_le_bytes(b[*p..*p + 2].try_into().map_err(|_| "short")?) as usize; *p += 2;
    let mut params = Vec::with_capacity(np); for _ in 0..np { params.push(get_value(b, p)?); }
    Ok((sql, params))
}
fn encode_result(out: &mut Vec<u8>, r: &Result<Vec<Vec<Value>>, String>) {
    match r {
        Ok(rows) => { out.push(0); out.extend_from_slice(&(rows.len() as u32).to_le_bytes()); out.extend_from_slice(&(rows.first().map(|r| r.len()).unwrap_or(0) as u16).to_le_bytes()); for row in rows { for v in row { put_value(out, v); } } }
        Err(e) => { out.push(1); out.extend_from_slice(&(e.len() as u32).to_le_bytes()); out.extend_from_slice(e.as_bytes()); }
    }
}
fn decode_result(b: &[u8], p: &mut usize) -> Result<Vec<Vec<Value>>, String> {
    let status = b[*p]; *p += 1;
    if status != 0 { let n = u32::from_le_bytes(b[*p..*p + 4].try_into().unwrap()) as usize; *p += 4; let m = String::from_utf8_lossy(&b[*p..*p + n]).into_owned(); *p += n; return Err(m); }
    let nrows = u32::from_le_bytes(b[*p..*p + 4].try_into().unwrap()) as usize; *p += 4;
    let ncols = u16::from_le_bytes(b[*p..*p + 2].try_into().unwrap()) as usize; *p += 2;
    let mut rows = Vec::with_capacity(nrows);
    for _ in 0..nrows { let mut row = Vec::with_capacity(ncols); for _ in 0..ncols { row.push(get_value(b, p)?); } rows.push(row); }
    Ok(rows)
}
fn read_frame(r: &mut impl Read) -> std::io::Result<Vec<u8>> { let mut l = [0u8; 4]; r.read_exact(&mut l)?; let n = u32::from_le_bytes(l) as usize; let mut b = vec![0u8; n]; r.read_exact(&mut b)?; Ok(b) }
fn write_frame(w: &mut impl Write, b: &[u8]) -> std::io::Result<()> { w.write_all(&(b.len() as u32).to_le_bytes())?; w.write_all(b)?; w.flush() }

/// Anything the server can serve and the client can speak over: Unix sockets locally, TCP for other hosts.
pub trait Stream: Read + Write + Send + 'static { fn dup(&self) -> std::io::Result<Self> where Self: Sized; }
impl Stream for UnixStream { fn dup(&self) -> std::io::Result<Self> { self.try_clone() } }
impl Stream for TcpStream { fn dup(&self) -> std::io::Result<Self> { self.try_clone() } }

pub struct Server;
impl Server {
    /// Accept connections forever on `path`; one thread and one session per connection.
    pub fn start(sql: Arc<Sql>, path: &Path) -> std::io::Result<std::thread::JoinHandle<()>> {
        std::fs::remove_file(path).ok();
        let listener = UnixListener::bind(path)?;
        Ok(std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(stream) = conn else { continue };
                let sql = sql.clone();
                std::thread::spawn(move || { let _ = Self::serve(sql, stream, None, false); });
            }
        }))
    }
    /// The same protocol on a TCP address (e.g. "0.0.0.0:7781"), for clients on other hosts; one thread per connection.
    /// `token` is a shared secret every connection must present first (sent in the clear: pair it with a private
    /// network or a TLS tunnel until H55). Remote clients cannot use shared-memory rings.
    pub fn start_tcp(sql: Arc<Sql>, addr: &str, token: &str) -> std::io::Result<std::thread::JoinHandle<()>> {
        if token.is_empty() { return Err(std::io::Error::other("a TCP listener needs a non-empty token")); }
        let listener = TcpListener::bind(addr)?; let token: Arc<str> = Arc::from(token);
        Ok(std::thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(stream) = conn else { continue };
                stream.set_nodelay(true).ok();
                let (sql, token) = (sql.clone(), token.clone());
                std::thread::spawn(move || { let _ = Self::serve(sql, stream, Some(token), true); });
            }
        }))
    }
    fn serve<S: Stream>(sql: Arc<Sql>, stream: S, token: Option<Arc<str>>, remote: bool) -> std::io::Result<()> {
        let mut r = BufReader::with_capacity(1 << 16, stream.dup()?); let mut w = BufWriter::with_capacity(1 << 16, stream);
        let mut sess = Session::default(); let mut out = Vec::with_capacity(1 << 16);
        let mut ring_closer: Option<Arc<std::sync::atomic::AtomicBool>> = None;
        if let Some(t) = &token {
            // first frame must be the token; anything else closes the connection without a hint
            let req = read_frame(&mut r)?;
            if req.first() != Some(&K_AUTH) || !same_secret(&req[1..], t.as_bytes()) { encode_result(&mut out, &Err("authentication failed".into())); write_frame(&mut w, &out)?; return Ok(()); }
            encode_result(&mut out, &Ok(vec![])); write_frame(&mut w, &out)?;
        }
        loop {
            let req = match read_frame(&mut r) { Ok(b) => b, Err(_) => { if let Some(c) = &ring_closer { c.store(true, Ordering::Relaxed); } return Ok(()) } };
            out.clear();
            if req.is_empty() { continue; }
            if req[0] == K_SHM {
                // the client created a ring file under SHM_DIR; serve it from a dedicated thread, the socket stays for shutdown
                let path = String::from_utf8_lossy(&req[1..]).into_owned();
                let allowed = !remote && path.starts_with(SHM_DIR) && path[SHM_DIR.len()..].starts_with(SHM_PREFIX) && !path[SHM_DIR.len()..].contains('/') && !path.contains("..");
                let r: Result<Vec<Vec<Value>>, String> = match if allowed { Ring::open(Path::new(&path), false) } else { Err(std::io::Error::other(if remote { "shared memory is for local clients" } else { "ring files must be /dev/shm/dblab-ring-*" })) } {
                    Ok(ring) => { let closer = Arc::new(std::sync::atomic::AtomicBool::new(false)); ring_closer = Some(closer.clone()); let sql2 = sql.clone(); std::thread::spawn(move || Self::serve_ring(sql2, ring, closer)); Ok(vec![]) }
                    Err(e) => Err(e.to_string()),
                };
                encode_result(&mut out, &r); write_frame(&mut w, &out)?; continue;
            }
            if req[0] == K_SUB {
                // a subscription connection: acknowledge, then push one frame per event until the client goes away
                let mut p = 1; let sub = match decode_exec(&req, &mut p).and_then(|(s, ps)| sql.reactor().subscribe(&sess, &s, &ps)) { Ok(sub) => { encode_result(&mut out, &Ok(vec![])); write_frame(&mut w, &out)?; sub } Err(e) => { encode_result(&mut out, &Err(e)); write_frame(&mut w, &out)?; continue; } };
                loop {
                    let Some(ev) = sub.recv_timeout(std::time::Duration::from_millis(200)) else { continue };
                    out.clear();
                    match ev {
                        crate::live::Event::Change(c) => { let mut row = vec![Value::Int(match c.op { crate::sql::ChangeOp::Insert => 1, crate::sql::ChangeOp::Update => 2, crate::sql::ChangeOp::Delete => 3 }), Value::Text(c.table.clone()), Value::Int(c.pk)]; if let Some(r) = &c.row { row.extend(r.iter().cloned()); } encode_result(&mut out, &Ok(vec![row])); }
                        crate::live::Event::Overflow { last_seq } => encode_result(&mut out, &Err(format!("subscription overflow at {last_seq}; resubscribe"))),
                        crate::live::Event::Live { .. } => continue,
                    }
                    if write_frame(&mut w, &out).is_err() { return Ok(()); }
                }
            }
            dispatch(&sql, &mut sess, &req, &mut out);
            write_frame(&mut w, &out)?;
        }
    }
    fn serve_ring(sql: Arc<Sql>, mut ring: Ring, closer: Arc<std::sync::atomic::AtomicBool>) {
        let mut sess = Session::default(); let mut out = Vec::with_capacity(1 << 16); let mut seq = 0u32;
        loop {
            if closer.load(Ordering::Relaxed) { ring.closed().store(1, Ordering::Relaxed); return; }
            if !ring.wait_for(ring.req_seq(), seq + 1) { return; }
            seq += 1;
            let n = ring.req_len().load(Ordering::Acquire) as usize;
            let req = ring.read_at(REQ_OFF, n).to_vec();
            out.clear(); dispatch(&sql, &mut sess, &req, &mut out);
            let n = out.len().min(RESP_CAP); ring.write_at(RESP_OFF, &out[..n]); ring.resp_len().store(n as u32, Ordering::Release);
            ring.resp_seq().store(seq, Ordering::Release);
        }
    }
}

pub struct Client { r: BufReader<Box<dyn Read + Send>>, w: BufWriter<Box<dyn Write + Send>>, buf: Vec<u8>, ring: Option<(Ring, u32, std::path::PathBuf)> }
impl Client {
    fn from_stream<S: Stream>(s: S) -> std::io::Result<Client> { Ok(Client { r: BufReader::with_capacity(1 << 16, Box::new(s.dup()?)), w: BufWriter::with_capacity(1 << 16, Box::new(s)), buf: Vec::with_capacity(1 << 16), ring: None }) }
    pub fn connect(path: &Path) -> std::io::Result<Client> { Self::from_stream(UnixStream::connect(path)?) }
    /// Connect over TCP to an owner started with `Server::start_tcp`, presenting its token.
    pub fn connect_tcp(addr: &str, token: &str) -> std::io::Result<Client> {
        let s = TcpStream::connect(addr)?; s.set_nodelay(true)?; let mut c = Self::from_stream(s)?;
        c.buf.clear(); c.buf.push(K_AUTH); c.buf.extend_from_slice(token.as_bytes());
        let resp = c.round_trip().map_err(std::io::Error::other)?; decode_result(&resp, &mut 0).map_err(std::io::Error::other)?; Ok(c)
    }
    /// Connect, then switch to a shared-memory ring (a file under `shm_dir`, e.g. /dev/shm) for round trips.
    pub fn connect_shm(path: &Path, shm_dir: &Path) -> std::io::Result<Client> {
        let mut c = Client::connect(path)?;
        static N: AtomicU32 = AtomicU32::new(0);
        let ring_path = shm_dir.join(format!("{SHM_PREFIX}{}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
        let ring = Ring::open(&ring_path, true)?;
        c.buf.clear(); c.buf.push(K_SHM); c.buf.extend_from_slice(ring_path.to_string_lossy().as_bytes());
        let resp = c.round_trip().map_err(|e| std::io::Error::other(e))?;
        decode_result(&resp, &mut 0).map_err(|e| std::io::Error::other(e))?;
        c.ring = Some((ring, 0, ring_path)); Ok(c)
    }
    pub fn login(&mut self, user: &str) -> Result<(), String> { self.buf.clear(); self.buf.push(K_LOGIN); self.buf.extend_from_slice(&(user.len() as u16).to_le_bytes()); self.buf.extend_from_slice(user.as_bytes()); let resp = self.round_trip()?; decode_result(&resp, &mut 0).map(|_| ()) }
    pub fn exec(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>, String> { self.buf.clear(); self.buf.push(K_EXEC); encode_exec(&mut self.buf, sql, params); let resp = self.round_trip()?; decode_result(&resp, &mut 0) }
    /// Several statements in one round trip; results in order.
    pub fn exec_batch(&mut self, stmts: &[(&str, Vec<Value>)]) -> Result<Vec<Result<Vec<Vec<Value>>, String>>, String> {
        self.buf.clear(); self.buf.push(K_BATCH); self.buf.extend_from_slice(&(stmts.len() as u32).to_le_bytes());
        for (s, ps) in stmts { encode_exec(&mut self.buf, s, ps); }
        let resp = self.round_trip()?; let mut p = 0; let mut out = Vec::with_capacity(stmts.len());
        for _ in 0..stmts.len() { out.push(decode_result(&resp, &mut p)); }
        Ok(out)
    }
    /// Turn this connection into a subscription: events arrive through `next_event` as rows [op, table, key, columns...].
    pub fn subscribe(&mut self, sql: &str, params: &[Value]) -> Result<(), String> { self.buf.clear(); self.buf.push(K_SUB); encode_exec(&mut self.buf, sql, params); write_frame(&mut self.w, &self.buf).map_err(|e| e.to_string())?; let resp = read_frame(&mut self.r).map_err(|e| e.to_string())?; decode_result(&resp, &mut 0).map(|_| ()) }
    pub fn next_event(&mut self) -> Result<Vec<Value>, String> { let resp = read_frame(&mut self.r).map_err(|e| e.to_string())?; decode_result(&resp, &mut 0).map(|mut r| r.pop().unwrap_or_default()) }
    fn round_trip(&mut self) -> Result<Vec<u8>, String> {
        if let Some((ring, seq, _)) = &mut self.ring {
            if self.buf.len() > REQ_CAP { return Err("request too large for the ring".into()); }
            ring.write_at(REQ_OFF, &self.buf); ring.req_len().store(self.buf.len() as u32, Ordering::Release);
            *seq += 1; ring.req_seq().store(*seq, Ordering::Release);
            if !ring.wait_for(ring.resp_seq(), *seq) { return Err("connection closed".into()); }
            let n = ring.resp_len().load(Ordering::Acquire) as usize;
            return Ok(ring.read_at(RESP_OFF, n).to_vec());
        }
        write_frame(&mut self.w, &self.buf).map_err(|e| e.to_string())?; read_frame(&mut self.r).map_err(|e| e.to_string())
    }
}

impl Drop for Client { fn drop(&mut self) { if let Some((ring, _, path)) = &self.ring { ring.closed().store(1, Ordering::Relaxed); std::fs::remove_file(path).ok(); } } }
