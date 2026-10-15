//! Unified engine core: contiguous typed row arena in chunks, range-sharded ordered PK index,
//! hash-sharded unique secondary index, pending-version conflict detection, segmented append-only
//! log with CRC-32C records and group commit, fuzzy snapshots with idempotent replay.
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock, RwLockReadGuard};
use std::time::Instant;
use crate::util::crc32;

pub const CHUNK: usize = 4096;
pub const MAX_CHUNKS: usize = 4096;          // 16.7M slots
pub const PK_SHARD_KEYS: u64 = 65_536;
pub const PK_SHARDS: usize = 1024;           // 67M keys
pub const SEC_SHARDS: usize = 256;
pub const SNAP_MAGIC: u64 = 0x534E41505F444231;
pub const SLOT_BYTES: usize = 8 + std::mem::size_of::<Row>();

#[repr(C)] #[derive(Clone, Copy, Debug, PartialEq)]
pub struct Row { pub id: u64, pub score: f64, pub country: u32, pub age: u32, pub name: [u8; 32], pub payload: [u8; 64] }
impl Row {
    pub fn make(id: u64, score: f64) -> Row { Row { id, score, country: (id % 20) as u32, age: (18 + id % 60) as u32, name: name_for(id), payload: [7; 64] } }
    pub fn zero() -> Row { Row { id: 0, score: 0.0, country: 0, age: 0, name: [0; 32], payload: [0; 64] } }
    pub fn bytes(&self) -> &[u8] { unsafe { std::slice::from_raw_parts(self as *const Row as *const u8, std::mem::size_of::<Row>()) } }
    pub fn from_bytes(b: &[u8]) -> Row { assert_eq!(b.len(), std::mem::size_of::<Row>()); unsafe { std::ptr::read_unaligned(b.as_ptr() as *const Row) } }
}
pub fn name_for(id: u64) -> [u8; 32] { let s = format!("u{id}@example.com"); let mut n = [0u8; 32]; n[..s.len()].copy_from_slice(s.as_bytes()); n }
pub fn name_from(s: &str) -> [u8; 32] { let mut n = [0u8; 32]; let b = s.as_bytes(); let l = b.len().min(32); n[..l].copy_from_slice(&b[..l]); n }
fn name_hash(n: &[u8; 32]) -> usize { let mut h: u64 = 0xcbf29ce484222325; for b in n { h ^= *b as u64; h = h.wrapping_mul(0x100000001b3); } (h >> 32) as usize & (SEC_SHARDS - 1) }

#[repr(C)] #[derive(Clone, Copy)]
pub struct Slot { pub version: u64, pub pending: u64, pub row: Row }
pub struct Chunk { pub slots: Option<Box<[Slot]>> }
impl Chunk {
    fn get_or_alloc(&mut self) -> &mut [Slot] {
        if self.slots.is_none() { self.slots = Some(vec![Slot { version: 0, pending: 0, row: Row::zero() }; CHUNK].into_boxed_slice()); }
        self.slots.as_mut().unwrap()
    }
}

// ---------- segmented log with group commit ----------
pub const OP_INSERT: u8 = 1; pub const OP_UPDATE: u8 = 2; pub const OP_UPDATE_NAME: u8 = 3;
pub struct LogState { pub buf: Vec<u8>, pub seq: u64, pub durable: u64, pub stop: bool, pub batches: u64, pub bytes: u64, pub rotate: bool, pub segments: Vec<u64> }
pub struct Log { pub st: Mutex<LogState>, pub cv_batch: Condvar, pub cv_durable: Condvar, pub dir: PathBuf }
impl Log {
    pub fn new(dir: &Path) -> Log { Log { st: Mutex::new(LogState { buf: Vec::with_capacity(1 << 22), seq: 0, durable: 0, stop: false, batches: 0, bytes: 0, rotate: false, segments: vec![] }), cv_batch: Condvar::new(), cv_durable: Condvar::new(), dir: dir.to_path_buf() } }
    pub fn seg_path(&self, first: u64) -> PathBuf { self.dir.join(format!("log.{first:020}")) }
    pub fn list_segments(dir: &Path) -> Vec<u64> {
        let mut v: Vec<u64> = fs::read_dir(dir).map(|rd| rd.filter_map(|e| e.ok()).filter_map(|e| e.file_name().to_str().and_then(|n| n.strip_prefix("log.").and_then(|x| x.parse().ok()))).collect()).unwrap_or_default();
        v.sort(); v
    }
    /// record: [len u32][crc32c u32][seq u64][op u8][row]
    pub fn append(&self, op: u8, row: &Row) -> u64 {
        let mut body = [0u8; 9 + std::mem::size_of::<Row>()];
        let mut s = self.st.lock().unwrap();
        s.seq += 1; let seq = s.seq;
        body[..8].copy_from_slice(&seq.to_le_bytes()); body[8] = op; body[9..].copy_from_slice(row.bytes());
        let crc = crc32(&body);
        s.buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
        s.buf.extend_from_slice(&crc.to_le_bytes());
        s.buf.extend_from_slice(&body);
        self.cv_batch.notify_one();
        seq
    }
    pub fn wait_durable(&self, seq: u64) { let mut s = self.st.lock().unwrap(); while s.durable < seq { s = self.cv_durable.wait(s).unwrap(); } }
    pub fn request_rotate(&self) { let mut s = self.st.lock().unwrap(); s.rotate = true; self.cv_batch.notify_one(); }
    fn open_segment(&self, first: u64) -> File { OpenOptions::new().create(true).write(true).append(true).open(self.seg_path(first)).unwrap() }
    pub fn committer(&self) {
        let first = { let mut s = self.st.lock().unwrap(); let f = s.seq + 1; if s.segments.last() != Some(&f) { s.segments.push(f); } f };
        let mut f = self.open_segment(first);
        let mut local: Vec<u8> = Vec::with_capacity(1 << 22);
        loop {
            let seq_end;
            { let mut s = self.st.lock().unwrap();
              while s.buf.is_empty() && !s.stop && !s.rotate { s = self.cv_batch.wait(s).unwrap(); }
              if s.buf.is_empty() && s.stop { return; }
              if s.buf.is_empty() { // rotation requested while idle
                  s.rotate = false; let nf = s.seq + 1; if s.segments.last() != Some(&nf) { s.segments.push(nf); }
                  drop(s); f = self.open_segment(nf); continue;
              }
              std::mem::swap(&mut local, &mut s.buf); seq_end = s.seq; s.batches += 1; s.bytes += local.len() as u64; }
            f.write_all(&local).unwrap(); f.sync_data().unwrap(); local.clear();
            let rotate = { let mut s = self.st.lock().unwrap(); s.durable = seq_end; self.cv_durable.notify_all(); if s.rotate { s.rotate = false; s.segments.push(seq_end + 1); true } else { false } };
            if rotate { f = self.open_segment(seq_end + 1); }
        }
    }
    pub fn stop(&self) { let mut s = self.st.lock().unwrap(); s.stop = true; self.cv_batch.notify_one(); }
}

#[derive(Debug, Clone, Copy, PartialEq)] pub enum Abort { NotFound, Conflict, Duplicate }
#[derive(Debug, Default)] pub struct SnapStats { pub rows: u64, pub bytes: u64, pub low_water: u64, pub secs: f64, pub deleted_segments: usize }
#[derive(Debug, Default)] pub struct RecStats { pub snap_rows: u64, pub snap_secs: f64, pub replayed: u64, pub skipped: u64, pub replay_secs: f64, pub segments: usize, pub low_water: u64 }

pub struct Engine {
    pub chunks: Vec<RwLock<Chunk>>,
    pub next_slot: AtomicU32,
    pub pk: Vec<RwLock<BTreeMap<u64, u32>>>,
    pub sec: Vec<RwLock<HashMap<[u8; 32], u32>>>,
    pub log: Arc<Log>,
    pub durable: bool,
}

impl Engine {
    pub fn new(durable: bool, dir: &Path) -> Engine {
        fs::create_dir_all(dir).unwrap();
        Engine { chunks: (0..MAX_CHUNKS).map(|_| RwLock::new(Chunk { slots: None })).collect(), next_slot: AtomicU32::new(0),
                 pk: (0..PK_SHARDS).map(|_| RwLock::new(BTreeMap::new())).collect(), sec: (0..SEC_SHARDS).map(|_| RwLock::new(HashMap::new())).collect(),
                 log: Arc::new(Log::new(dir)), durable }
    }
    fn pk_shard(id: u64) -> usize { (id / PK_SHARD_KEYS) as usize }
    pub fn rows_allocated(&self) -> usize { self.next_slot.load(Ordering::Relaxed) as usize }
    pub fn chunk_count(&self) -> usize { (self.rows_allocated() / CHUNK + 1).min(MAX_CHUNKS) }

    /// Direct load without logging (preload).
    pub fn load_committed(&self, row: Row, version: u64) { let slot = self.next_slot.fetch_add(1, Ordering::Relaxed); self.load_committed_at(slot, row, version); }
    pub fn load_committed_at(&self, slot: u32, row: Row, version: u64) {
        { let mut c = self.chunks[slot as usize / CHUNK].write().unwrap(); c.get_or_alloc()[slot as usize % CHUNK] = Slot { version, pending: 0, row }; }
        self.pk[Self::pk_shard(row.id)].write().unwrap().insert(row.id, slot);
        self.sec[name_hash(&row.name)].write().unwrap().insert(row.name, slot);
    }
    /// Idempotent replay of a durable log record.
    pub fn replay_record(&self, seq: u64, row: Row) {
        let slot = self.pk[Self::pk_shard(row.id)].read().unwrap().get(&row.id).copied();
        match slot {
            None => self.load_committed(row, seq),
            Some(slot) => {
                let old = { let mut c = self.chunks[slot as usize / CHUNK].write().unwrap(); let s = &mut c.get_or_alloc()[slot as usize % CHUNK];
                            if s.version >= seq { return; } let old = s.row; *s = Slot { version: seq, pending: 0, row }; old };
                if old.name != row.name { self.sec[name_hash(&old.name)].write().unwrap().remove(&old.name); self.sec[name_hash(&row.name)].write().unwrap().insert(row.name, slot); }
            }
        }
    }

    fn read_slot(&self, slot: u32) -> Option<(u64, Row)> {
        let c = self.chunks[slot as usize / CHUNK].read().unwrap();
        let s = c.slots.as_ref()?[slot as usize % CHUNK];
        if s.version > 0 { Some((s.version, s.row)) } else { None }
    }
    pub fn get_versioned(&self, id: u64) -> Option<(u64, Row)> { let slot = *self.pk[Self::pk_shard(id)].read().unwrap().get(&id)?; self.read_slot(slot) }
    pub fn get(&self, id: u64) -> Option<Row> { self.get_versioned(id).map(|(_, r)| r) }
    pub fn get_by_name(&self, name: &[u8; 32]) -> Option<Row> {
        let slot = *self.sec[name_hash(name)].read().unwrap().get(name)?;
        let (_, r) = self.read_slot(slot)?;
        if &r.name == name { Some(r) } else { None }
    }

    pub fn insert(&self, row: Row) -> Result<u64, Abort> {
        let slot = self.next_slot.fetch_add(1, Ordering::Relaxed);
        let seq;
        {
            let mut pk = self.pk[Self::pk_shard(row.id)].write().unwrap();
            if pk.contains_key(&row.id) { return Err(Abort::Duplicate); }
            let mut sec = self.sec[name_hash(&row.name)].write().unwrap();
            if sec.contains_key(&row.name) { return Err(Abort::Duplicate); }
            let mut c = self.chunks[slot as usize / CHUNK].write().unwrap();   // chunk lock BEFORE append: a snapshot copying this chunk must see the pending mark
            seq = self.log.append(OP_INSERT, &row);
            pk.insert(row.id, slot); sec.insert(row.name, slot);
            c.get_or_alloc()[slot as usize % CHUNK] = Slot { version: 0, pending: seq, row };
        }
        if self.durable { self.log.wait_durable(seq); }
        let mut c = self.chunks[slot as usize / CHUNK].write().unwrap();
        let s = &mut c.get_or_alloc()[slot as usize % CHUNK]; s.version = seq; s.pending = 0;
        Ok(seq)
    }

    pub fn update(&self, id: u64, f: impl Fn(&Row) -> Row) -> Result<u64, Abort> {
        let slot = match self.pk[Self::pk_shard(id)].read().unwrap().get(&id) { Some(&s) => s, None => return Err(Abort::NotFound) };
        let (ver, row) = self.read_slot(slot).ok_or(Abort::NotFound)?;
        let new = f(&row);
        let seq;
        {
            let mut c = self.chunks[slot as usize / CHUNK].write().unwrap();
            let s = &mut c.get_or_alloc()[slot as usize % CHUNK];
            if s.pending != 0 || s.version != ver { return Err(Abort::Conflict); }
            seq = self.log.append(OP_UPDATE, &new);
            s.pending = seq;
        }
        if self.durable { self.log.wait_durable(seq); }
        let mut c = self.chunks[slot as usize / CHUNK].write().unwrap();
        let s = &mut c.get_or_alloc()[slot as usize % CHUNK]; s.row = new; s.version = seq; s.pending = 0;
        Ok(seq)
    }

    pub fn update_name(&self, id: u64, new_name: [u8; 32]) -> Result<u64, Abort> {
        let slot = match self.pk[Self::pk_shard(id)].read().unwrap().get(&id) { Some(&s) => s, None => return Err(Abort::NotFound) };
        let (ver, row) = self.read_slot(slot).ok_or(Abort::NotFound)?;
        let new = Row { name: new_name, ..row };
        let seq;
        {
            let mut sec = self.sec[name_hash(&new_name)].write().unwrap();
            if sec.contains_key(&new_name) { return Err(Abort::Duplicate); }
            let mut c = self.chunks[slot as usize / CHUNK].write().unwrap();
            let s = &mut c.get_or_alloc()[slot as usize % CHUNK];
            if s.pending != 0 || s.version != ver { return Err(Abort::Conflict); }
            seq = self.log.append(OP_UPDATE_NAME, &new);
            s.pending = seq;
            sec.insert(new_name, slot);
        }
        if self.durable { self.log.wait_durable(seq); }
        { let mut c = self.chunks[slot as usize / CHUNK].write().unwrap();
          let s = &mut c.get_or_alloc()[slot as usize % CHUNK]; s.row = new; s.version = seq; s.pending = 0; }
        self.sec[name_hash(&row.name)].write().unwrap().remove(&row.name);
        Ok(seq)
    }

    pub fn scan<T>(&self, chunks: std::ops::Range<usize>, mut acc: T, f: impl Fn(&mut T, &Row)) -> T {
        let max = self.chunk_count();
        for ck in chunks.start..chunks.end.min(max) {
            let c = self.chunks[ck].read().unwrap();
            if let Some(slots) = c.slots.as_ref() { for s in slots.iter() { if s.version > 0 { f(&mut acc, &s.row); } } }
        }
        acc
    }

    pub fn range_sum(&self, from: u64, to: u64) -> (u64, f64) {
        let (mut n, mut sum) = (0u64, 0f64);
        let mut cur = usize::MAX; let mut guard: Option<RwLockReadGuard<Chunk>> = None;
        for sh in Self::pk_shard(from)..=Self::pk_shard(to.saturating_sub(1)) {
            let m = self.pk[sh].read().unwrap();
            for (_, &slot) in m.range(from..to) {
                let ck = slot as usize / CHUNK;
                if ck != cur { guard = Some(self.chunks[ck].read().unwrap()); cur = ck; }
                if let Some(sl) = guard.as_ref().unwrap().slots.as_ref() { let s = &sl[slot as usize % CHUNK]; if s.version > 0 { n += 1; sum += s.row.score; } }
            }
        }
        (n, sum)
    }

    /// Fuzzy snapshot: copies committed rows chunk by chunk while writers continue. Records low_water = the
    /// smallest seq whose effect might be missing; restart replays every record with seq >= low_water (idempotent).
    pub fn snapshot(&self) -> SnapStats {
        let t = Instant::now();
        let dir = self.log.dir.clone();
        let s0 = self.log.st.lock().unwrap().seq;
        self.log.request_rotate();
        let tmp = dir.join("snap.tmp");
        let mut w = BufWriter::with_capacity(1 << 20, File::create(&tmp).unwrap());
        w.write_all(&[0u8; 24]).unwrap();
        let mut min_pending = u64::MAX; let mut rows = 0u64;
        for ck in 0..self.chunk_count() {
            let c = self.chunks[ck].read().unwrap();
            if let Some(slots) = c.slots.as_ref() {
                for s in slots.iter() {
                    if s.pending != 0 { min_pending = min_pending.min(s.pending); }
                    if s.version > 0 { w.write_all(&s.version.to_le_bytes()).unwrap(); w.write_all(s.row.bytes()).unwrap(); rows += 1; }
                }
            }
        }
        let low_water = (s0 + 1).min(min_pending);
        w.flush().unwrap();
        let mut f = w.into_inner().unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&SNAP_MAGIC.to_le_bytes()).unwrap(); f.write_all(&low_water.to_le_bytes()).unwrap(); f.write_all(&rows.to_le_bytes()).unwrap();
        f.sync_all().unwrap();
        let bytes = f.metadata().unwrap().len();
        drop(f);
        fs::rename(&tmp, dir.join("snap")).unwrap();
        let segs = self.log.st.lock().unwrap().segments.clone();
        let mut deleted: HashSet<u64> = HashSet::new();
        for i in 0..segs.len().saturating_sub(1) { if segs[i + 1] - 1 < low_water { fs::remove_file(self.log.seg_path(segs[i])).ok(); deleted.insert(segs[i]); } }
        { let mut s = self.log.st.lock().unwrap(); s.segments.retain(|f| !deleted.contains(f)); }
        SnapStats { rows, bytes, low_water, secs: t.elapsed().as_secs_f64(), deleted_segments: deleted.len() }
    }

    /// Restart: load snapshot (parallel), then replay log segments from low_water.
    pub fn recover(dir: &Path, durable: bool, threads: usize) -> (Arc<Engine>, RecStats) {
        let e = Arc::new(Engine::new(durable, dir));
        let t = Instant::now();
        let mut st = RecStats { low_water: 1, ..Default::default() };
        let mut max_seq = 0u64;
        if let Ok(data) = fs::read(dir.join("snap")) {
            assert_eq!(u64::from_le_bytes(data[..8].try_into().unwrap()), SNAP_MAGIC);
            st.low_water = u64::from_le_bytes(data[8..16].try_into().unwrap());
            st.snap_rows = u64::from_le_bytes(data[16..24].try_into().unwrap());
            let n = st.snap_rows as usize;
            let data = Arc::new(data);
            let per = (n + threads - 1) / threads;
            let hs: Vec<_> = (0..threads).map(|k| { let e = e.clone(); let data = data.clone(); std::thread::spawn(move || {
                let mut mx = 0u64;
                for i in k * per..((k + 1) * per).min(n) {
                    let off = 24 + i * SLOT_BYTES;
                    let version = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
                    e.load_committed_at(i as u32, Row::from_bytes(&data[off + 8..off + SLOT_BYTES]), version);
                    mx = mx.max(version);
                }
                mx }) }).collect();
            for h in hs { max_seq = max_seq.max(h.join().unwrap()); }
            e.next_slot.store(n as u32, Ordering::Relaxed);
        }
        st.snap_secs = t.elapsed().as_secs_f64();
        let t = Instant::now();
        let segs = Log::list_segments(dir);
        st.segments = segs.len();
        for first in &segs {
            let data = fs::read(e.log.seg_path(*first)).unwrap();
            let (recs, _, _) = parse_log(&data);
            for (seq, _op, row) in recs { if seq < st.low_water { st.skipped += 1; continue; } e.replay_record(seq, row); st.replayed += 1; max_seq = max_seq.max(seq); }
        }
        st.replay_secs = t.elapsed().as_secs_f64();
        { let mut s = e.log.st.lock().unwrap(); s.seq = max_seq; s.durable = max_seq; s.segments = segs; }
        (e, st)
    }
}

/// Parse log bytes; returns (records, bytes consumed, truncated_tail_bytes). Stops at first short or corrupt record.
pub fn parse_log(data: &[u8]) -> (Vec<(u64, u8, Row)>, usize, usize) {
    let mut out = Vec::new(); let mut pos = 0;
    while pos + 8 <= data.len() {
        let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap());
        if len != 9 + std::mem::size_of::<Row>() || pos + 8 + len > data.len() { break; }
        let body = &data[pos + 8..pos + 8 + len];
        if crc32(body) != crc { break; }
        let seq = u64::from_le_bytes(body[..8].try_into().unwrap());
        out.push((seq, body[8], Row::from_bytes(&body[9..])));
        pos += 8 + len;
    }
    (out, pos, data.len() - pos)
}
