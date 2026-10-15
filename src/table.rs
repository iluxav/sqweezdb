//! Schema-driven storage: typed fixed-width rows in chunked arenas, per-table ordered PK index and unique
//! secondary index, pending-version conflict detection, shared group-commit log with generic records.
//! Same design as engine.rs, generalized to any schema. Types are enforced at encode time.
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::time::Instant;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock, RwLockReadGuard};
use memmap2::{Advice, MmapMut, MmapOptions};
use crate::util::{crc32, hash_bytes, FastMap, FastMapU64};

pub const CHUNK_ROWS: usize = 4096;
pub const MAX_CHUNKS: usize = 4096;
pub const PK_SHARD_KEYS: i64 = 65_536;
pub const PK_SHARDS: usize = 1024;
pub const SEC_SHARDS: usize = 256;

#[derive(Clone, Debug, PartialEq)]
pub enum ColType { Int, Float, Bool, Text(usize), Bytes(usize) }
impl ColType {
    pub fn width(&self) -> usize { match self { ColType::Int | ColType::Float => 8, ColType::Bool => 1, ColType::Text(n) | ColType::Bytes(n) => 2 + *n } }
    pub fn name(&self) -> String { match self { ColType::Int => "INT".into(), ColType::Float => "DOUBLE".into(), ColType::Bool => "BOOL".into(), ColType::Text(n) => format!("TEXT({n})"), ColType::Bytes(n) => format!("BYTES({n})") } }
}
#[derive(Clone, Debug)] pub struct Column { pub name: String, pub ty: ColType, pub nullable: bool }
#[derive(Clone, Debug)] pub struct Schema { pub name: String, pub columns: Vec<Column>, pub offsets: Vec<usize>, pub row_size: usize, pub pk: usize, pub unique: Option<usize>, pub null_bytes: usize }
impl Schema {
    pub fn new(name: &str, columns: Vec<Column>, pk: usize, unique: Option<usize>) -> Result<Schema, String> {
        if columns.get(pk).map(|c| &c.ty) != Some(&ColType::Int) { return Err("primary key must be an INT column".into()); }
        let mut columns = columns; columns[pk].nullable = false;
        let null_bytes = (columns.len() + 7) / 8;
        let mut offsets = vec![]; let mut off = null_bytes;
        for c in &columns { offsets.push(off); off += c.ty.width(); }
        Ok(Schema { name: name.to_string(), columns, offsets, row_size: off, pk, unique, null_bytes })
    }
    pub fn col(&self, name: &str) -> Option<usize> { self.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name)) }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value { Int(i64), Float(f64), Bool(bool), Text(String), Bytes(Vec<u8>), Null }
impl Value {
    pub fn as_i64(&self) -> Result<i64, String> { match self { Value::Int(i) => Ok(*i), v => Err(format!("expected INT, got {v:?}")) } }
    pub fn as_f64(&self) -> Result<f64, String> { match self { Value::Int(i) => Ok(*i as f64), Value::Float(f) => Ok(*f), v => Err(format!("expected number, got {v:?}")) } }
}

/// Enforced types: a value must match the column type, except INT -> DOUBLE promotion. TEXT/BYTES must fit.
pub fn encode_value(ty: &ColType, v: &Value, out: &mut [u8]) -> Result<(), String> {
    match (ty, v) {
        (ColType::Int, Value::Int(x)) => out[..8].copy_from_slice(&x.to_le_bytes()),
        (ColType::Float, Value::Float(x)) => out[..8].copy_from_slice(&x.to_le_bytes()),
        (ColType::Float, Value::Int(x)) => out[..8].copy_from_slice(&(*x as f64).to_le_bytes()),
        (ColType::Bool, Value::Bool(b)) => out[0] = *b as u8,
        (ColType::Text(n), Value::Text(s)) => { let b = s.as_bytes(); if b.len() > *n { return Err(format!("TEXT value of {} bytes exceeds TEXT({n})", b.len())); } out[..2].copy_from_slice(&(b.len() as u16).to_le_bytes()); out[2..2 + b.len()].copy_from_slice(b); for x in &mut out[2 + b.len()..2 + n] { *x = 0; } }
        (ColType::Bytes(n), Value::Bytes(b)) => { if b.len() > *n { return Err(format!("BYTES value of {} bytes exceeds BYTES({n})", b.len())); } out[..2].copy_from_slice(&(b.len() as u16).to_le_bytes()); out[2..2 + b.len()].copy_from_slice(b); for x in &mut out[2 + b.len()..2 + n] { *x = 0; } }
        (ty, v) => return Err(format!("type mismatch: column is {}, value is {:?}", ty.name(), v)),
    }
    Ok(())
}
#[inline] pub fn is_null(row: &[u8], i: usize) -> bool { row[i >> 3] >> (i & 7) & 1 == 1 }
/// Set one column of an encoded row (handles NULL and the null bitmap).
pub fn encode_col(schema: &Schema, row: &mut [u8], i: usize, v: &Value) -> Result<(), String> {
    let c = &schema.columns[i]; let off = schema.offsets[i]; let w = c.ty.width();
    if let Value::Null = v { if !c.nullable { return Err(format!("column {} is NOT NULL", c.name)); } row[i >> 3] |= 1 << (i & 7); for b in &mut row[off..off + w] { *b = 0; } return Ok(()); }
    encode_value(&c.ty, v, &mut row[off..off + w]).map_err(|e| format!("column {}: {e}", c.name))?;
    row[i >> 3] &= !(1 << (i & 7));
    Ok(())
}
pub fn encode_row(schema: &Schema, values: &[Value]) -> Result<Vec<u8>, String> {
    if values.len() != schema.columns.len() { return Err(format!("expected {} values, got {}", schema.columns.len(), values.len())); }
    let mut row = vec![0u8; schema.row_size];
    for i in 0..values.len() { encode_col(schema, &mut row, i, &values[i])?; }
    Ok(row)
}
#[inline] pub fn read_i64(row: &[u8], off: usize) -> i64 { i64::from_le_bytes(row[off..off + 8].try_into().unwrap()) }
#[inline] pub fn read_f64(row: &[u8], off: usize) -> f64 { f64::from_le_bytes(row[off..off + 8].try_into().unwrap()) }
#[inline] pub fn read_bytes(row: &[u8], off: usize) -> &[u8] { let n = u16::from_le_bytes(row[off..off + 2].try_into().unwrap()) as usize; &row[off + 2..off + 2 + n] }
pub fn decode_col(schema: &Schema, row: &[u8], i: usize) -> Value {
    if is_null(row, i) { return Value::Null; }
    let off = schema.offsets[i];
    match &schema.columns[i].ty {
        ColType::Int => Value::Int(read_i64(row, off)), ColType::Float => Value::Float(read_f64(row, off)), ColType::Bool => Value::Bool(row[off] != 0),
        ColType::Text(_) => Value::Text(String::from_utf8_lossy(read_bytes(row, off)).into_owned()), ColType::Bytes(_) => Value::Bytes(read_bytes(row, off).to_vec()),
    }
}
/// Raw encoded bytes of a column (used as index key).
#[inline] pub fn col_key<'a>(schema: &Schema, row: &'a [u8], i: usize) -> Option<&'a [u8]> { if is_null(row, i) { return None; } let off = schema.offsets[i]; Some(&row[off..off + schema.columns[i].ty.width()]) }
#[inline] pub fn read_key(schema: &Schema, row: &[u8], i: usize) -> Option<i64> { if is_null(row, i) { None } else { Some(read_i64(row, schema.offsets[i])) } }

// ---------- generic group-commit log: segmented, with a catalog of DDL/ACL statements ----------
pub const OP_INSERT: u8 = 1; pub const OP_UPDATE: u8 = 2; pub const OP_TXN: u8 = 3; pub const OP_META: u8 = 4; pub const OP_DELETE: u8 = 5;
pub const SNAP_MAGIC: u64 = 0x534E41505F544233;
pub const SNAP_ALIGN: usize = 4096;
pub const SNAP_HDR: usize = 128;
pub struct LogState { pub buf: Vec<u8>, pub seq: u64, pub durable: u64, pub stop: bool, pub batches: u64, pub bytes: u64, pub rotate: bool, pub segments: Vec<u64>, pub catalog: Vec<String> }
pub struct GLog { pub st: Mutex<LogState>, pub cv_batch: Condvar, pub cv_durable: Condvar, pub dir: PathBuf }
impl GLog {
    pub fn new(dir: &Path) -> GLog { GLog { st: Mutex::new(LogState { buf: Vec::with_capacity(1 << 22), seq: 0, durable: 0, stop: false, batches: 0, bytes: 0, rotate: false, segments: vec![], catalog: vec![] }), cv_batch: Condvar::new(), cv_durable: Condvar::new(), dir: dir.to_path_buf() } }
    pub fn seg_path(&self, first: u64) -> PathBuf { self.dir.join(format!("log.{first:020}")) }
    pub fn list_segments(dir: &Path) -> Vec<u64> {
        let mut v: Vec<u64> = fs::read_dir(dir).map(|rd| rd.filter_map(|e| e.ok()).filter_map(|e| e.file_name().to_str().and_then(|n| n.strip_prefix("log.").and_then(|x| x.parse().ok()))).collect()).unwrap_or_default();
        v.sort(); v
    }
    fn push_record(s: &mut LogState, op: u8, table: u16, payload: &[u8]) -> u64 {
        s.seq += 1; let seq = s.seq;
        let mut body = Vec::with_capacity(11 + payload.len());
        body.extend_from_slice(&seq.to_le_bytes()); body.push(op); body.extend_from_slice(&table.to_le_bytes()); body.extend_from_slice(payload);
        let crc = crc32(&body);
        s.buf.extend_from_slice(&(body.len() as u32).to_le_bytes()); s.buf.extend_from_slice(&crc.to_le_bytes()); s.buf.extend_from_slice(&body);
        seq
    }
    /// record: [len u32][crc32c u32][seq u64][op u8][table u16][payload]
    pub fn append(&self, op: u8, table: u16, payload: &[u8]) -> u64 { let mut s = self.st.lock().unwrap(); let seq = Self::push_record(&mut s, op, table, payload); self.cv_batch.notify_one(); seq }
    /// A DDL or ACL statement: appended to the log and to the in-memory catalog under one lock, so a snapshot
    /// taken at sequence S sees exactly the statements with sequence <= S.
    pub fn append_meta(&self, text: &str) -> u64 { let mut s = self.st.lock().unwrap(); let seq = Self::push_record(&mut s, OP_META, u16::MAX, text.as_bytes()); s.catalog.push(text.to_string()); self.cv_batch.notify_one(); seq }
    pub fn snapshot_point(&self) -> (u64, Vec<String>) { let mut s = self.st.lock().unwrap(); s.rotate = true; self.cv_batch.notify_one(); (s.seq, s.catalog.clone()) }
    pub fn wait_durable(&self, seq: u64) { let mut s = self.st.lock().unwrap(); while s.durable < seq { s = self.cv_durable.wait(s).unwrap(); } }
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
              if s.buf.is_empty() { s.rotate = false; let nf = s.seq + 1; if s.segments.last() != Some(&nf) { s.segments.push(nf); } drop(s); f = self.open_segment(nf); continue; }
              std::mem::swap(&mut local, &mut s.buf); seq_end = s.seq; s.batches += 1; s.bytes += local.len() as u64; }
            f.write_all(&local).unwrap(); f.sync_data().unwrap(); local.clear();
            let rotate = { let mut s = self.st.lock().unwrap(); s.durable = seq_end; self.cv_durable.notify_all(); if s.rotate { s.rotate = false; s.segments.push(seq_end + 1); true } else { false } };
            if rotate { f = self.open_segment(seq_end + 1); }
        }
    }
    pub fn stop(&self) { let mut s = self.st.lock().unwrap(); s.stop = true; self.cv_batch.notify_one(); }
}
/// Parse a log segment: (seq, op, table, payload). Stops at the first short or corrupt record.
pub fn parse_glog(data: &[u8]) -> (Vec<(u64, u8, u16, Vec<u8>)>, usize) {
    let mut out = Vec::new(); let mut pos = 0;
    while pos + 8 <= data.len() {
        let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(data[pos + 4..pos + 8].try_into().unwrap());
        if len < 11 || pos + 8 + len > data.len() { break; }
        let body = &data[pos + 8..pos + 8 + len];
        if crc32(body) != crc { break; }
        out.push((u64::from_le_bytes(body[..8].try_into().unwrap()), body[8], u16::from_le_bytes(body[9..11].try_into().unwrap()), body[11..].to_vec()));
        pos += 8 + len;
    }
    (out, data.len() - pos)
}

#[derive(Debug, Clone, PartialEq)] pub enum Abort { NotFound, Conflict, Duplicate, Type(String) }

/// A chunk's bytes: owned on the heap, or a window into the private (copy-on-write) mapping of the snapshot file.
pub enum ChunkBuf { Heap(Box<[u8]>), Mapped(*mut u8, usize) }
unsafe impl Send for ChunkBuf {} unsafe impl Sync for ChunkBuf {}
impl std::ops::Deref for ChunkBuf { type Target = [u8]; #[inline] fn deref(&self) -> &[u8] { match self { ChunkBuf::Heap(b) => b, ChunkBuf::Mapped(p, n) => unsafe { std::slice::from_raw_parts(*p, *n) } } } }
impl std::ops::DerefMut for ChunkBuf { #[inline] fn deref_mut(&mut self) -> &mut [u8] { match self { ChunkBuf::Heap(b) => b, ChunkBuf::Mapped(p, n) => unsafe { std::slice::from_raw_parts_mut(*p, *n) } } } }
pub struct Chunk { pub data: Option<ChunkBuf> }
/// The mapped part of a table after a restart: rows [0, n) live in the snapshot file sorted by primary key, with the
/// sorted key array and an open-addressing unique-key table beside them. Never modified: later changes land in the
/// private mapping (row bytes, copy-on-write per page) or in the heap overlays (indexes for rows added since).
/// `fence[p]` is the key of the first row that starts in page p of the rows region (read eagerly: 8 bytes per 4 KB
/// page of data). A point lookup is one fence search in memory, then one page of rows.
pub struct MappedBase { pub n: usize, rows: *const u8, rows_off: usize, stride: usize, pk_off: usize, fence: Vec<i64>, uniq: *const u64, ucap: usize, kmin: i64, kmax: i64 }
unsafe impl Send for MappedBase {} unsafe impl Sync for MappedBase {}
impl MappedBase {
    /// Primary key of mapped slot i, read straight from the mapping (a slot's key never changes).
    #[inline] fn key_at(&self, i: usize) -> i64 { unsafe { std::ptr::read_unaligned(self.rows.add(i * self.stride + 16 + self.pk_off) as *const i64) } }
    #[inline] fn slot_lo(&self, page: usize) -> usize { ((page * SNAP_ALIGN + self.stride - 1) / self.stride).min(self.n) }
    /// First mapped slot whose key is >= pk (n if none).
    fn lower_bound(&self, pk: i64) -> usize {
        let p = self.fence.partition_point(|&f| f <= pk); if p == 0 { return 0; }
        let (mut lo, mut hi) = (self.slot_lo(p - 1), self.slot_lo(p));
        while lo < hi { let mid = (lo + hi) / 2; if self.key_at(mid) < pk { lo = mid + 1; } else { hi = mid; } }
        lo
    }
    pub fn chunks(&self) -> usize { (self.n + CHUNK_ROWS - 1) / CHUNK_ROWS }
}
/// Unique index shard: 64-bit key hash -> slot, verified against the row; rare collisions spill into `overflow`.
#[derive(Default)] pub struct UniqShard { main: FastMapU64<u32>, overflow: HashMap<u64, Vec<u32>> }
/// Non-unique secondary index on an INT column: key -> slots, hash-sharded. Maintained at publish time, so it only ever
/// points at committed rows (a reader still re-checks the row).
pub struct SecIndex { pub name: String, pub col: usize, shards: Vec<RwLock<FastMap<Vec<u32>>>> }
impl SecIndex {
    pub fn new(name: &str, col: usize) -> SecIndex { SecIndex { name: name.to_string(), col, shards: (0..SEC_SHARDS).map(|_| RwLock::new(FastMap::default())).collect() } }
    #[inline] fn shard(k: i64) -> usize { ((k as u64).wrapping_mul(0x9E3779B97F4A7C15) >> 56) as usize }
    pub fn add(&self, k: i64, slot: u32) { let mut m = self.shards[Self::shard(k)].write().unwrap(); let v = m.entry(k).or_default(); if !v.contains(&slot) { v.push(slot); } }
    pub fn remove(&self, k: i64, slot: u32) { let mut m = self.shards[Self::shard(k)].write().unwrap(); if let Some(v) = m.get_mut(&k) { v.retain(|&s| s != slot); if v.is_empty() { m.remove(&k); } } }
    pub fn slots(&self, k: i64) -> Vec<u32> { self.shards[Self::shard(k)].read().unwrap().get(&k).cloned().unwrap_or_default() }
    #[inline] pub fn with_slots<T>(&self, k: i64, f: impl FnOnce(&[u32]) -> T) -> Option<T> { let m = self.shards[Self::shard(k)].read().unwrap(); m.get(&k).map(|v| f(v)) }
    #[inline] pub fn count(&self, k: i64) -> usize { self.with_slots(k, |v| v.len()).unwrap_or(0) }
    pub fn entries(&self) -> usize { self.shards.iter().map(|s| s.read().unwrap().values().map(|v| v.len()).sum::<usize>()).sum() }
}
pub struct Table {
    pub id: u16, pub schema: Schema, pub stride: usize,
    chunks: Vec<RwLock<Chunk>>, next_slot: AtomicU32,
    pk: Vec<RwLock<BTreeMap<i64, u32>>>, sec: Vec<RwLock<UniqShard>>,
    pub indexes: RwLock<Vec<Arc<SecIndex>>>,
    pub log: Arc<GLog>, pub durable: bool,
    base: OnceLock<MappedBase>, map: OnceLock<Arc<MmapMut>>,
    free: Mutex<Vec<u32>>,
}
impl Table { #[inline] fn base_is_some(&self) -> bool { self.base.get().is_some() } }
impl Table {
    pub fn new(id: u16, schema: Schema, log: Arc<GLog>, durable: bool) -> Table {
        let stride = 16 + schema.row_size;
        Table { id, schema, stride, chunks: (0..MAX_CHUNKS).map(|_| RwLock::new(Chunk { data: None })).collect(), next_slot: AtomicU32::new(0),
                pk: (0..PK_SHARDS).map(|_| RwLock::new(BTreeMap::new())).collect(), sec: (0..SEC_SHARDS).map(|_| RwLock::new(UniqShard::default())).collect(), indexes: RwLock::new(vec![]), log, durable, base: OnceLock::new(), map: OnceLock::new(), free: Mutex::new(vec![]) }
    }
    #[inline] fn pk_shard(pk: i64) -> usize { ((pk as u64) / PK_SHARD_KEYS as u64) as usize % PK_SHARDS }
    #[inline] fn sec_shard_h(h: u64) -> usize { (h >> 56) as usize & (SEC_SHARDS - 1) }
    /// Does this slot's row carry exactly this unique key? (reads the row; pending rows count, so claims are visible)
    fn slot_key_eq(&self, slot: u32, key: &[u8]) -> bool {
        let u = match self.schema.unique { Some(u) => u, None => return false };
        let c = self.chunks[slot as usize / CHUNK_ROWS].read().unwrap();
        match c.data.as_ref() { Some(d) => { let off = (slot as usize % CHUNK_ROWS) * self.stride; let (v, p) = Self::hdr_at(d, off); if v == 0 && p == 0 { return false; }
            col_key(&self.schema, &d[off + 16..off + self.stride], u) == Some(key) || (v != 0 && p != 0) } None => false }  // a live row with a key change in flight: taken until it commits or aborts
    }
    /// Probe the mapped unique-key table: entries are [tag u32][slot u32], linear probing, verified against the live row.
    fn mapped_uniq(&self, h: u64, key: &[u8]) -> Option<u32> {
        let b = self.base.get()?; if b.ucap == 0 { return None; }
        let (mask, tag) = (b.ucap - 1, ((h >> 32) as u32) | 1); let mut i = (h as usize) & mask;
        loop { let e = unsafe { *b.uniq.add(i) }; if e == 0 { return None; } if (e >> 32) as u32 == tag { let s = e as u32; if self.slot_key_eq(s, key) { return Some(s); } } i = (i + 1) & mask; }
    }
    #[inline] fn mapped_slot(&self, pk: i64) -> Option<u32> { let b = self.base.get()?; if pk < b.kmin || pk > b.kmax { return None; } let i = b.lower_bound(pk); if i < b.n && b.key_at(i) == pk { Some(i as u32) } else { None } }
    /// Ask the kernel to read a mapped chunk ahead (scans).
    #[inline] fn prefetch(&self, ck: usize) { if let (Some(b), Some(m)) = (self.base.get(), self.map.get()) { if ck < b.chunks() { let _ = m.advise_range(Advice::WillNeed, b.rows_off + ck * CHUNK_ROWS * self.stride, CHUNK_ROWS * self.stride); } } }
    #[inline] fn slot_live(&self, slot: u32) -> bool { let c = self.chunks[slot as usize / CHUNK_ROWS].read().unwrap(); match c.data.as_ref() { Some(d) => Self::hdr_at(d, (slot as usize % CHUNK_ROWS) * self.stride) != (0, 0), None => false } }
    #[inline] fn mapped_live(&self, pk: i64) -> bool { self.mapped_slot(pk).map(|s| self.slot_live(s)).unwrap_or(false) }
    /// Slot of a primary key: the mapped base first (unless that row has since been deleted), then the heap index.
    #[inline] pub fn pk_slot(&self, pk: i64) -> Option<u32> { if let Some(s) = self.mapped_slot(pk) { if self.slot_live(s) { return Some(s); } } self.pk[Self::pk_shard(pk)].read().unwrap().get(&pk).copied() }
    pub fn mapped_rows(&self) -> usize { self.base.get().map(|b| b.n).unwrap_or(0) }
    /// A slot for a new row: a freed one if any, else the next fresh one.
    #[inline] fn alloc_slot(&self) -> u32 { if let Some(s) = self.free.lock().unwrap().pop() { return s; } self.next_slot.fetch_add(1, Ordering::Relaxed) }
    /// Return a dead slot for reuse. Mapped slots stay dead: the mapped region must keep its key order.
    #[inline] fn free_slot(&self, slot: u32) { if slot as usize >= self.mapped_rows() { self.free.lock().unwrap().push(slot); } }
    pub fn free_slots(&self) -> usize { self.free.lock().unwrap().len() }
    /// Diagnostics for one key: how the mapped lookup resolves it.
    pub fn debug_lookup(&self, pk: i64) -> String {
        let b = match self.base.get() { Some(b) => b, None => return "no mapped base".into() };
        let p = b.fence.partition_point(|&f| f <= pk); let lb = b.lower_bound(pk);
        let (lo, hi) = if p == 0 { (0, 0) } else { (b.slot_lo(p - 1), b.slot_lo(p)) };
        let hdr = self.chunks[lb.min(b.n - 1) / CHUNK_ROWS].read().unwrap().data.as_ref().map(|d| Self::hdr_at(d, (lb.min(b.n - 1) % CHUNK_ROWS) * self.stride));
        format!("pk {pk}: n {} kmin {} kmax {} fence {} entries; page {} window [{lo},{hi}) keys [{}..{}]; lower_bound {lb} key_at {} hdr {:?}; heap {:?}", b.n, b.kmin, b.kmax, b.fence.len(), p as i64 - 1, if lo < b.n { b.key_at(lo) } else { -1 }, if hi > 0 && hi <= b.n { b.key_at(hi - 1) } else { -1 }, if lb < b.n { b.key_at(lb) } else { -1 }, hdr, self.pk[Self::pk_shard(pk)].read().unwrap().get(&pk))
    }
    fn uniq_contains_locked(&self, sh: &UniqShard, h: u64, key: &[u8]) -> bool {
        if let Some(&s) = sh.main.get(&h) { if self.slot_key_eq(s, key) { return true; } }
        sh.overflow.get(&h).map(|v| v.iter().any(|&s| self.slot_key_eq(s, key))).unwrap_or(false) || self.mapped_uniq(h, key).is_some()
    }
    fn uniq_insert_locked(sh: &mut UniqShard, h: u64, slot: u32) { if sh.main.contains_key(&h) { sh.overflow.entry(h).or_default().push(slot); } else { sh.main.insert(h, slot); } }
    pub fn uniq_lookup(&self, key: &[u8]) -> Option<u32> {
        let h = hash_bytes(key); let sh = self.sec[Self::sec_shard_h(h)].read().unwrap();
        if let Some(&s) = sh.main.get(&h) { if self.slot_key_eq(s, key) { return Some(s); } }
        if let Some(s) = sh.overflow.get(&h).and_then(|v| v.iter().copied().find(|&s| self.slot_key_eq(s, key))) { return Some(s); }
        self.mapped_uniq(h, key)
    }
    fn uniq_insert(&self, key: &[u8], slot: u32) { let h = hash_bytes(key); let mut sh = self.sec[Self::sec_shard_h(h)].write().unwrap(); Self::uniq_insert_locked(&mut sh, h, slot); }
    fn uniq_remove(&self, key: &[u8], slot: u32) {
        let h = hash_bytes(key); let mut sh = self.sec[Self::sec_shard_h(h)].write().unwrap();
        if sh.main.get(&h) == Some(&slot) { sh.main.remove(&h); let promote = sh.overflow.get_mut(&h).and_then(|v| v.pop()); let empty = sh.overflow.get(&h).map(|v| v.is_empty()).unwrap_or(false); if let Some(p) = promote { sh.main.insert(h, p); } if empty { sh.overflow.remove(&h); } }
        else if let Some(v) = sh.overflow.get_mut(&h) { v.retain(|&x| x != slot); if v.is_empty() { sh.overflow.remove(&h); } }
    }
    fn alloc<'a>(c: &'a mut Chunk, stride: usize) -> &'a mut [u8] { if c.data.is_none() { c.data = Some(ChunkBuf::Heap(vec![0u8; CHUNK_ROWS * stride].into_boxed_slice())); } &mut **c.data.as_mut().unwrap() }
    pub fn rows_allocated(&self) -> usize { self.next_slot.load(Ordering::Relaxed) as usize }
    #[inline] fn idx_add(&self, row: &[u8], slot: u32) { let ix = self.indexes.read().unwrap(); for i in ix.iter() { if let Some(k) = read_key(&self.schema, row, i.col) { i.add(k, slot); } } }
    #[inline] fn idx_remove(&self, row: &[u8], slot: u32) { let ix = self.indexes.read().unwrap(); for i in ix.iter() { if let Some(k) = read_key(&self.schema, row, i.col) { i.remove(k, slot); } } }
    #[inline] fn idx_replace(&self, old: &[u8], new: &[u8], slot: u32) { let ix = self.indexes.read().unwrap(); for i in ix.iter() { let (a, b) = (read_key(&self.schema, old, i.col), read_key(&self.schema, new, i.col)); if a != b { if let Some(a) = a { i.remove(a, slot); } if let Some(b) = b { i.add(b, slot); } } } }
    /// Drop a committed row from every index and zero its version (slot is not reused; a vacuum can compact later).
    fn unpublish(&self, slot: u32, row: &[u8]) {
        let pk = self.pk_of(row); self.pk[Self::pk_shard(pk)].write().unwrap().remove(&pk);
        if let Some(k) = self.unique_key(row) { self.uniq_remove(k, slot); }
        self.idx_remove(row, slot);
    }
    /// Autocommit delete by primary key. The log record carries only the key (8 bytes).
    pub fn delete_by_pk(&self, pk: i64) -> Result<u64, Abort> {
        let slot = match self.pk_slot(pk) { Some(s) => s, None => return Err(Abort::NotFound) };
        let (ck, off) = (slot as usize / CHUNK_ROWS, (slot as usize % CHUNK_ROWS) * self.stride);
        let (seq, row) = {
            let mut c = self.chunks[ck].write().unwrap(); let d = Self::alloc(&mut c, self.stride);
            let (v, p) = Self::hdr_at(d, off); if v == 0 || self.pk_of(&d[off + 16..off + self.stride]) != pk { return Err(Abort::NotFound); } if p != 0 { return Err(Abort::Conflict); }
            let row = d[off + 16..off + self.stride].to_vec();
            let seq = self.log.append(OP_DELETE, self.id, &pk.to_le_bytes()); d[off + 8..off + 16].copy_from_slice(&seq.to_le_bytes()); (seq, row) };
        if self.durable { self.log.wait_durable(seq); }
        { let mut c = self.chunks[ck].write().unwrap(); let d = Self::alloc(&mut c, self.stride); d[off..off + 16].copy_from_slice(&[0u8; 16]); }
        self.unpublish(slot, &row); self.free_slot(slot);
        Ok(seq)
    }
    /// Idempotent replay of a delete record (payload: the 8-byte key; older logs carried the whole row).
    pub fn replay_delete(&self, seq: u64, payload: &[u8]) {
        let pk = if payload.len() == 8 { i64::from_le_bytes(payload.try_into().unwrap()) } else { self.pk_of(payload) };
        let slot = match self.pk_slot(pk) { Some(s) => s, None => return };
        let (ck, off) = (slot as usize / CHUNK_ROWS, (slot as usize % CHUNK_ROWS) * self.stride);
        let stored: Option<Vec<u8>> = { let mut c = self.chunks[ck].write().unwrap(); let d = Self::alloc(&mut c, self.stride); let (v, _) = Self::hdr_at(d, off); if v == 0 || v >= seq || self.pk_of(&d[off + 16..off + self.stride]) != pk { None } else { let r = d[off + 16..off + self.stride].to_vec(); d[off..off + 16].copy_from_slice(&[0u8; 16]); Some(r) } };
        if let Some(r) = stored { self.unpublish(slot, &r); self.free_slot(slot); }
    }
    pub fn index_on(&self, col: usize) -> Option<Arc<SecIndex>> { self.indexes.read().unwrap().iter().find(|i| i.col == col).cloned() }
    /// Create a non-unique index on an INT column: publish it first (writers maintain it from now on), then backfill.
    pub fn create_index(&self, name: &str, col: usize) -> Result<Arc<SecIndex>, String> {
        if self.schema.columns[col].ty != ColType::Int { return Err("only INT columns can be indexed (for now)".into()); }
        if self.index_on(col).is_some() { return Err(format!("column {} already has an index", self.schema.columns[col].name)); }
        let ix = Arc::new(SecIndex::new(name, col));
        self.indexes.write().unwrap().push(ix.clone());
        let off = self.schema.offsets[col];
        for ck in 0..self.chunk_count() {
            let c = self.chunks[ck].read().unwrap();
            if let Some(d) = c.data.as_ref() { for i in 0..CHUNK_ROWS { let o = i * self.stride; if u64::from_le_bytes(d[o..o + 8].try_into().unwrap()) > 0 { ix.add(read_i64(&d[o + 16..o + self.stride], off), (ck * CHUNK_ROWS + i) as u32); } } }
        }
        Ok(ix)
    }
    /// Rows for one key of a non-unique index (committed rows only).
    pub fn index_lookup<F: FnMut(&[u8]) -> bool>(&self, ix: &SecIndex, key: i64, mut f: F) {
        ix.with_slots(key, |slots| { for &slot in slots { if self.with_slot(slot, |_, r| f(r)) == Some(false) { return; } } });
    }
    pub fn set_rows_allocated(&self, n: u32) { self.next_slot.store(n, Ordering::Relaxed); }
    /// Direct committed load into a given slot (snapshot restore), maintaining the indexes.
    pub fn load_committed_at(&self, slot: u32, row: &[u8], version: u64) {
        let (ck, off) = (slot as usize / CHUNK_ROWS, (slot as usize % CHUNK_ROWS) * self.stride);
        { let mut c = self.chunks[ck].write().unwrap(); let d = Self::alloc(&mut c, self.stride); d[off..off + 8].copy_from_slice(&version.to_le_bytes()); d[off + 8..off + 16].copy_from_slice(&0u64.to_le_bytes()); d[off + 16..off + self.stride].copy_from_slice(row); }
        let pk = self.pk_of(row); self.pk[Self::pk_shard(pk)].write().unwrap().insert(pk, slot);
        if let Some(k) = self.unique_key(row) { self.uniq_insert(k, slot); }
        self.idx_add(row, slot);
    }
    /// Idempotent replay of a durable record: insert if the key is new, overwrite if the stored version is older.
    pub fn replay_record(&self, seq: u64, row: &[u8]) {
        let pk = self.pk_of(row);
        let slot = self.pk_slot(pk);
        match slot {
            None => { let slot = self.alloc_slot(); self.load_committed_at(slot, row, seq); }
            Some(slot) => {
                let (ck, off) = (slot as usize / CHUNK_ROWS, (slot as usize % CHUNK_ROWS) * self.stride);
                let old: Option<Vec<u8>> = { let mut c = self.chunks[ck].write().unwrap(); let d = Self::alloc(&mut c, self.stride);
                    let v = u64::from_le_bytes(d[off..off + 8].try_into().unwrap()); if v >= seq { return; }
                    let old = d[off + 16..off + self.stride].to_vec(); d[off..off + 8].copy_from_slice(&seq.to_le_bytes()); d[off + 8..off + 16].copy_from_slice(&0u64.to_le_bytes()); d[off + 16..off + self.stride].copy_from_slice(row); Some(old) };
                if let Some(old) = old { let (ok, nk) = (self.unique_key(&old).map(|k| k.to_vec()), self.unique_key(row).map(|k| k.to_vec())); if ok != nk { if let Some(k) = ok { self.uniq_remove(&k, slot); } if let Some(k) = nk { self.uniq_insert(&k, slot); } } self.idx_replace(&old, row, slot); }
            }
        }
    }
    /// Every (key, slot) the table knows, in key order: heap index entries (sorted) merged with the mapped keys.
    fn ordered_slots(&self) -> Vec<(i64, u32)> {
        let mut heap: Vec<(i64, u32)> = Vec::new();
        for sh in &self.pk { let m = sh.read().unwrap(); heap.extend(m.iter().map(|(&k, &s)| (k, s))); }
        heap.sort_unstable();
        let (nk, key) = match self.base.get() { Some(b) => (b.n, Some(b)), None => (0, None) };
        let keys = |j: usize| key.unwrap().key_at(j);
        let mut out = Vec::with_capacity(heap.len() + nk);
        let (mut i, mut j) = (0, 0);
        while i < heap.len() || j < nk {
            if j >= nk || (i < heap.len() && heap[i].0 <= keys(j)) { if i < heap.len() && j < nk && heap[i].0 == keys(j) { j += 1; } out.push(heap[i]); i += 1; }
            else { out.push((keys(j), j as u32)); j += 1; }
        }
        out
    }
    /// Snapshot of this table in slot layout, sorted by primary key: [version u64][0 u64][row] per committed row,
    /// zero-padded to whole chunks; then the page fence; then the unique-key table ([tag u32][slot u32], 2x capacity).
    /// Each region starts at a 4096-byte boundary. Returns (rows, min pending seq, max version, [rows, fence, uniq] offsets, ucap, fence entries, kmin, kmax).
    pub fn snapshot_table(&self, w: &mut BufWriter<File>) -> (u64, u64, u64, [u64; 3], u64, u64, i64, i64) {
        fn align(w: &mut BufWriter<File>) -> u64 { let pos = w.stream_position().unwrap(); let pad = (SNAP_ALIGN as u64 - pos % SNAP_ALIGN as u64) % SNAP_ALIGN as u64; w.write_all(&vec![0u8; pad as usize]).unwrap(); pos + pad }
        let (mut n, mut min_pending, mut max_v) = (0u64, u64::MAX, 0u64);
        let mut fence: Vec<i64> = Vec::new(); let mut ukeys: Vec<(u64, u32)> = Vec::new(); let (mut kmin, mut kmax) = (i64::MAX, i64::MIN);
        let u = self.schema.unique;
        let rows_off = align(w);
        let mut cur = usize::MAX; let mut guard: Option<RwLockReadGuard<Chunk>> = None;
        for (_, slot) in self.ordered_slots() {
            let ck = slot as usize / CHUNK_ROWS;
            if ck != cur { self.prefetch(ck + 1); guard = Some(self.chunks[ck].read().unwrap()); cur = ck; }
            if let Some(d) = guard.as_ref().unwrap().data.as_ref() {
                let off = (slot as usize % CHUNK_ROWS) * self.stride; let (v, p) = Self::hdr_at(d, off);
                if p != 0 && p < TXN_MARK { min_pending = min_pending.min(p); }
                if v > 0 { let row = &d[off + 16..off + self.stride]; w.write_all(&v.to_le_bytes()).unwrap(); w.write_all(&0u64.to_le_bytes()).unwrap(); w.write_all(row).unwrap();
                           let pk = self.pk_of(row); let page = (n as usize * self.stride) / SNAP_ALIGN; while fence.len() <= page { fence.push(pk); } kmin = kmin.min(pk); kmax = kmax.max(pk);
                           if let Some(u) = u { if let Some(k) = col_key(&self.schema, row, u) { ukeys.push((hash_bytes(k), n as u32)); } } n += 1; max_v = max_v.max(v); }
            }
        }
        drop(guard);
        let pad_rows = (CHUNK_ROWS - n as usize % CHUNK_ROWS) % CHUNK_ROWS; w.write_all(&vec![0u8; pad_rows * self.stride]).unwrap();
        let fence_off = align(w);
        for k in &fence { w.write_all(&k.to_le_bytes()).unwrap(); }
        let uniq_off = align(w);
        let ucap = if u.is_some() && n > 0 { (2 * n as usize).next_power_of_two().max(16) } else { 0 };
        if ucap > 0 {
            let mut tbl = vec![0u64; ucap]; let mask = ucap - 1;
            for (h, slot) in ukeys { let tag = ((h >> 32) as u32) | 1; let mut i = (h as usize) & mask; while tbl[i] != 0 { i = (i + 1) & mask; } tbl[i] = ((tag as u64) << 32) | slot as u64; }
            w.write_all(unsafe { std::slice::from_raw_parts(tbl.as_ptr() as *const u8, ucap * 8) }).unwrap();
        }
        align(w);
        (n, min_pending, max_v, [rows_off, fence_off, uniq_off], ucap as u64, fence.len() as u64, kmin, kmax)
    }
    /// Restart: point chunks [0, n) at the private mapping of the snapshot file and install the mapped indexes.
    pub fn attach_base(&self, map: Arc<MmapMut>, base: *mut u8, n: usize, offs: [u64; 3], ucap: usize, fence: Vec<i64>, kmin: i64, kmax: i64) {
        if n == 0 { return; }
        let nchunks = (n + CHUNK_ROWS - 1) / CHUNK_ROWS;
        for ck in 0..nchunks { let mut c = self.chunks[ck].write().unwrap(); c.data = Some(ChunkBuf::Mapped(unsafe { base.add(offs[0] as usize + ck * CHUNK_ROWS * self.stride) }, CHUNK_ROWS * self.stride)); }
        let _ = self.base.set(MappedBase { n, rows: unsafe { base.add(offs[0] as usize) }, rows_off: offs[0] as usize, stride: self.stride, pk_off: self.schema.offsets[self.schema.pk], fence, uniq: unsafe { base.add(offs[2] as usize) } as *const u64, ucap, kmin, kmax });
        let _ = self.map.set(map);
        self.next_slot.store(n as u32, Ordering::Relaxed);
    }
    /// Fill every non-unique index from the rows (used after attaching a mapped base; touches every row).
    pub fn backfill_indexes(&self) {
        let ixs: Vec<Arc<SecIndex>> = self.indexes.read().unwrap().clone();
        if ixs.is_empty() { return; }
        for ck in 0..self.chunk_count() {
            self.prefetch(ck + 1); let c = self.chunks[ck].read().unwrap();
            if let Some(d) = c.data.as_ref() { for i in 0..CHUNK_ROWS { let o = i * self.stride; if u64::from_le_bytes(d[o..o + 8].try_into().unwrap()) > 0 { let row = &d[o + 16..o + self.stride]; for ix in &ixs { if let Some(k) = read_key(&self.schema, row, ix.col) { ix.add(k, (ck * CHUNK_ROWS + i) as u32); } } } } }
        }
    }
    pub fn chunk_count(&self) -> usize { (self.rows_allocated() / CHUNK_ROWS + 1).min(MAX_CHUNKS) }
    #[inline] pub fn pk_of(&self, row: &[u8]) -> i64 { read_i64(row, self.schema.offsets[self.schema.pk]) }
    #[inline] pub fn unique_key<'a>(&self, row: &'a [u8]) -> Option<&'a [u8]> { self.schema.unique.and_then(|u| col_key(&self.schema, row, u)) }

    pub fn insert(&self, row: &[u8]) -> Result<u64, Abort> {
        debug_assert_eq!(row.len(), self.schema.row_size);
        let pk = self.pk_of(row);
        let ukey: Option<Vec<u8>> = self.unique_key(row).map(|k| k.to_vec());
        let slot = self.alloc_slot();
        let (ck, off) = (slot as usize / CHUNK_ROWS, (slot as usize % CHUNK_ROWS) * self.stride);
        let seq;
        {
            let mut pkm = self.pk[Self::pk_shard(pk)].write().unwrap();
            if pkm.contains_key(&pk) || self.mapped_live(pk) { self.free_slot(slot); return Err(Abort::Duplicate); }
            let h = ukey.as_ref().map(|k| hash_bytes(k)).unwrap_or(0);
            let mut secm = ukey.as_ref().map(|_| self.sec[Self::sec_shard_h(h)].write().unwrap());
            if let (Some(m), Some(k)) = (&secm, &ukey) { if self.uniq_contains_locked(m, h, k) { self.free_slot(slot); return Err(Abort::Duplicate); } }
            let mut c = self.chunks[ck].write().unwrap();
            seq = self.log.append(OP_INSERT, self.id, row);
            pkm.insert(pk, slot);
            if let Some(m) = &mut secm { Self::uniq_insert_locked(m, h, slot); }
            let d = Self::alloc(&mut c, self.stride);
            d[off..off + 8].copy_from_slice(&0u64.to_le_bytes()); d[off + 8..off + 16].copy_from_slice(&seq.to_le_bytes()); d[off + 16..off + self.stride].copy_from_slice(row);
        }
        if self.durable { self.log.wait_durable(seq); }
        { let mut c = self.chunks[ck].write().unwrap();
          let d = Self::alloc(&mut c, self.stride);
          d[off..off + 8].copy_from_slice(&seq.to_le_bytes()); d[off + 8..off + 16].copy_from_slice(&0u64.to_le_bytes()); }
        self.idx_add(row, slot);
        Ok(seq)
    }

    #[inline] fn with_slot<T>(&self, slot: u32, f: impl FnOnce(u64, &[u8]) -> T) -> Option<T> {
        let c = self.chunks[slot as usize / CHUNK_ROWS].read().unwrap();
        let d = c.data.as_ref()?;
        let off = (slot as usize % CHUNK_ROWS) * self.stride;
        let version = u64::from_le_bytes(d[off..off + 8].try_into().unwrap());
        if version > 0 { Some(f(version, &d[off + 16..off + self.stride])) } else { None }
    }
    pub fn with_pk<T>(&self, pk: i64, f: impl FnOnce(&[u8]) -> T) -> Option<T> {
        let slot = self.pk_slot(pk)?;
        self.with_slot(slot, |_, r| if self.pk_of(r) == pk { Some(f(r)) } else { None }).flatten()
    }
    pub fn with_unique<T>(&self, key: &[u8], f: impl FnOnce(&[u8]) -> T) -> Option<T> {
        let slot = self.uniq_lookup(key)?;
        let u = self.schema.unique?;
        self.with_slot(slot, |_, r| if col_key(&self.schema, r, u) == Some(key) { Some(f(r)) } else { None }).flatten()
    }
    pub fn get_by_pk(&self, pk: i64) -> Option<Vec<u8>> { self.with_pk(pk, |r| r.to_vec()) }

    pub fn update_by_pk(&self, pk: i64, f: impl FnOnce(&[u8]) -> Result<Vec<u8>, Abort>) -> Result<u64, Abort> {
        let slot = match self.pk_slot(pk) { Some(s) => s, None => return Err(Abort::NotFound) };
        let (ver, row) = self.with_slot(slot, |v, r| (v, r.to_vec())).ok_or(Abort::NotFound)?;
        if self.pk_of(&row) != pk { return Err(Abort::NotFound); }
        let new = f(&row)?;
        if new.len() != self.schema.row_size || self.pk_of(&new) != pk { return Err(Abort::Type("row shape or primary key changed".into())); }
        let old_key = self.unique_key(&row).map(|k| k.to_vec()); let new_key = self.unique_key(&new).map(|k| k.to_vec());
        let key_changed = old_key != new_key;
        let (ck, off) = (slot as usize / CHUNK_ROWS, (slot as usize % CHUNK_ROWS) * self.stride);
        let seq;
        {
            let nh = new_key.as_ref().map(|k| hash_bytes(k));
            let mut secm = if key_changed && nh.is_some() { Some(self.sec[Self::sec_shard_h(nh.unwrap())].write().unwrap()) } else { None };
            if let (Some(m), Some(k)) = (&secm, &new_key) { if self.uniq_contains_locked(m, nh.unwrap(), k) { return Err(Abort::Duplicate); } }
            let mut c = self.chunks[ck].write().unwrap();
            let d = Self::alloc(&mut c, self.stride);
            let v = u64::from_le_bytes(d[off..off + 8].try_into().unwrap()); let p = u64::from_le_bytes(d[off + 8..off + 16].try_into().unwrap());
            if p != 0 || v != ver { return Err(Abort::Conflict); }
            seq = self.log.append(OP_UPDATE, self.id, &new);
            d[off + 8..off + 16].copy_from_slice(&seq.to_le_bytes());
            if let Some(m) = &mut secm { Self::uniq_insert_locked(m, nh.unwrap(), slot); }
        }
        if self.durable { self.log.wait_durable(seq); }
        { let mut c = self.chunks[ck].write().unwrap(); let d = Self::alloc(&mut c, self.stride);
          d[off..off + 8].copy_from_slice(&seq.to_le_bytes()); d[off + 8..off + 16].copy_from_slice(&0u64.to_le_bytes()); d[off + 16..off + self.stride].copy_from_slice(&new); }
        if key_changed { if let Some(k) = old_key { self.uniq_remove(&k, slot); } }
        self.idx_replace(&row, &new, slot);
        Ok(seq)
    }

    /// Full scan of committed rows; the callback returns false to stop early.
    #[inline] pub fn scan<F: FnMut(&[u8]) -> bool>(&self, mut f: F) {
        if self.base_is_some() { self.prefetch(0); }
        for ck in 0..self.chunk_count() {
            if self.base_is_some() { self.prefetch(ck + 1); }
            let c = self.chunks[ck].read().unwrap();
            if let Some(d) = c.data.as_ref() {
                for i in 0..CHUNK_ROWS { let off = i * self.stride; if u64::from_le_bytes(d[off..off + 8].try_into().unwrap()) > 0 { if !f(&d[off + 16..off + self.stride]) { return; } } }
            }
        }
    }
    /// Ordered PK range scan, inclusive bounds, non-negative keys.
    #[inline] pub fn range<F: FnMut(&[u8]) -> bool>(&self, lo: i64, hi: i64, mut f: F) {
        if hi < lo { return; }
        let mut cur = usize::MAX; let mut guard: Option<RwLockReadGuard<Chunk>> = None;
        let base = self.base.get();
        for k in lo.max(0) / PK_SHARD_KEYS..=hi.max(0) / PK_SHARD_KEYS {
            let (a, b) = ((k * PK_SHARD_KEYS).max(lo), ((k + 1) * PK_SHARD_KEYS - 1).min(hi));
            let m = self.pk[(k as usize) % PK_SHARDS].read().unwrap();
            let mut heap = m.range(a..=b).peekable();
            let (mut mi, mend) = match base { Some(bs) => (bs.lower_bound(a), bs.lower_bound(b.saturating_add(1))), None => (0, 0) };
            loop {
                let hk = heap.peek().map(|(k, _)| **k);
                let mk = if mi < mend { Some(base.unwrap().key_at(mi)) } else { None };
                let (slot, key) = match (mk, hk) {
                    (None, None) => break,
                    (Some(mk), Some(hk)) if hk <= mk => { if hk == mk { mi += 1; } (*heap.next().unwrap().1, hk) }  // same key in both: the mapped row died and was re-inserted
                    (Some(mk), _) => { mi += 1; ((mi - 1) as u32, mk) }
                    (None, Some(hk)) => (*heap.next().unwrap().1, hk),
                };
                let ck = slot as usize / CHUNK_ROWS;
                if ck != cur { guard = Some(self.chunks[ck].read().unwrap()); cur = ck; }
                if let Some(d) = guard.as_ref().unwrap().data.as_ref() {
                    let off = (slot as usize % CHUNK_ROWS) * self.stride; let row = &d[off + 16..off + self.stride];
                    if u64::from_le_bytes(d[off..off + 8].try_into().unwrap()) > 0 && self.pk_of(row) == key { if !f(row) { return; } }
                }
            }
        }
    }
}

pub struct Database {
    pub dir: PathBuf, pub log: Arc<GLog>, pub durable: bool,
    tables: RwLock<Vec<Arc<Table>>>, names: RwLock<HashMap<String, usize>>,
    committer: Mutex<Option<std::thread::JoinHandle<()>>>,
}
#[derive(Debug, Default)] pub struct SnapStats { pub rows: u64, pub bytes: u64, pub low_water: u64, pub secs: f64, pub deleted_segments: usize }
#[derive(Debug, Default)] pub struct RecStats { pub snap_rows: u64, pub snap_secs: f64, pub catalog: usize, pub replayed: u64, pub skipped: u64, pub replay_secs: f64, pub segments: usize, pub low_water: u64 }
impl Database {
    /// Open (or create) a database directory. Nothing is replayed here; see `recover`. The committer starts on `start`.
    pub fn open(dir: &Path, durable: bool) -> Database {
        fs::create_dir_all(dir).unwrap();
        Database { dir: dir.to_path_buf(), log: Arc::new(GLog::new(dir)), durable, tables: RwLock::new(vec![]), names: RwLock::new(HashMap::new()), committer: Mutex::new(None) }
    }
    pub fn start(&self) { let l = self.log.clone(); *self.committer.lock().unwrap() = Some(std::thread::spawn(move || l.committer())); }
    pub fn create_table(&self, schema: Schema) -> Result<Arc<Table>, String> {
        let mut names = self.names.write().unwrap(); let mut tables = self.tables.write().unwrap();
        let key = schema.name.to_lowercase();
        if names.contains_key(&key) { return Err(format!("table {} already exists", schema.name)); }
        let t = Arc::new(Table::new(tables.len() as u16, schema, self.log.clone(), self.durable));
        names.insert(key, tables.len()); tables.push(t.clone());
        Ok(t)
    }
    pub fn table(&self, name: &str) -> Option<Arc<Table>> { let i = *self.names.read().unwrap().get(&name.to_lowercase())?; self.tables.read().unwrap().get(i).cloned() }
    pub fn table_by_id(&self, id: u16) -> Option<Arc<Table>> { self.tables.read().unwrap().get(id as usize).cloned() }

    /// Fuzzy snapshot of every table that existed at the snapshot point, plus the catalog (DDL + ACL statements).
    /// Restart replays log records with seq >= low_water on top (idempotent).
    pub fn snapshot(&self) -> SnapStats {
        let t = Instant::now();
        let (s0, catalog) = self.log.snapshot_point();
        let ntables = catalog.iter().filter(|c| c.trim_start().to_uppercase().starts_with("CREATE TABLE")).count();
        let tables: Vec<Arc<Table>> = { let ts = self.tables.read().unwrap(); ts.iter().take(ntables).cloned().collect() };
        let tmp = self.dir.join("snap.tmp");
        let mut w = BufWriter::with_capacity(1 << 20, File::create(&tmp).unwrap());
        w.write_all(&SNAP_MAGIC.to_le_bytes()).unwrap(); w.write_all(&0u64.to_le_bytes()).unwrap();
        w.write_all(&(catalog.len() as u32).to_le_bytes()).unwrap();
        for c in &catalog { w.write_all(&(c.len() as u32).to_le_bytes()).unwrap(); w.write_all(c.as_bytes()).unwrap(); }
        w.write_all(&(tables.len() as u16).to_le_bytes()).unwrap();
        // table directory: 64 bytes per table, filled in after each table's regions are written
        let dir_pos = w.stream_position().unwrap(); w.write_all(&vec![0u8; SNAP_HDR * tables.len()]).unwrap();
        let mut min_pending = u64::MAX; let mut rows = 0u64;
        for (ti, tb) in tables.iter().enumerate() {
            let (n, mp, max_v, offs, ucap, fence_n, kmin, kmax) = tb.snapshot_table(&mut w);
            rows += n; min_pending = min_pending.min(mp);
            let here = w.stream_position().unwrap(); w.seek(SeekFrom::Start(dir_pos + (SNAP_HDR * ti) as u64)).unwrap();
            let mut h = Vec::with_capacity(SNAP_HDR);
            h.extend_from_slice(&tb.id.to_le_bytes()); h.extend_from_slice(&0u16.to_le_bytes()); h.extend_from_slice(&(tb.schema.row_size as u32).to_le_bytes());
            h.extend_from_slice(&n.to_le_bytes()); h.extend_from_slice(&(tb.stride as u32).to_le_bytes()); h.extend_from_slice(&0u32.to_le_bytes());
            h.extend_from_slice(&ucap.to_le_bytes()); h.extend_from_slice(&max_v.to_le_bytes()); for o in offs { h.extend_from_slice(&o.to_le_bytes()); }
            h.extend_from_slice(&fence_n.to_le_bytes()); h.extend_from_slice(&kmin.to_le_bytes()); h.extend_from_slice(&kmax.to_le_bytes()); h.resize(SNAP_HDR, 0);
            w.write_all(&h).unwrap(); w.seek(SeekFrom::Start(here)).unwrap();
        }
        let low_water = (s0 + 1).min(min_pending);
        w.flush().unwrap();
        let mut f = w.into_inner().unwrap();
        f.seek(SeekFrom::Start(8)).unwrap(); f.write_all(&low_water.to_le_bytes()).unwrap();
        f.sync_all().unwrap();
        let bytes = f.metadata().unwrap().len(); drop(f);
        fs::rename(&tmp, self.dir.join("snap")).unwrap();
        let segs = self.log.st.lock().unwrap().segments.clone();
        let mut deleted: HashSet<u64> = HashSet::new();
        for i in 0..segs.len().saturating_sub(1) { if segs[i + 1] - 1 < low_water { fs::remove_file(self.log.seg_path(segs[i])).ok(); deleted.insert(segs[i]); } }
        { let mut s = self.log.st.lock().unwrap(); s.segments.retain(|f| !deleted.contains(f)); }
        SnapStats { rows, bytes, low_water, secs: t.elapsed().as_secs_f64(), deleted_segments: deleted.len() }
    }

    /// Restart: catalog statements go through `meta` (which recreates tables and ACL without logging), rows are
    /// loaded in parallel, then log segments are replayed from low_water. Call `start()` afterwards.
    pub fn recover(self: &Arc<Self>, threads: usize, meta: &mut dyn FnMut(&str) -> Result<(), String>) -> Result<RecStats, String> {
        let t = Instant::now();
        let mut st = RecStats { low_water: 1, ..Default::default() };
        let mut max_seq = 0u64; let mut catalog: Vec<String> = vec![];
        if let Ok(mut f) = File::open(self.dir.join("snap")) {
            let mut h = [0u8; 16]; f.read_exact(&mut h).map_err(|e| e.to_string())?;
            if u64::from_le_bytes(h[..8].try_into().unwrap()) != SNAP_MAGIC { return Err("bad snapshot magic".into()); }
            st.low_water = u64::from_le_bytes(h[8..16].try_into().unwrap());
            let mut b4 = [0u8; 4]; f.read_exact(&mut b4).map_err(|e| e.to_string())?; let ncat = u32::from_le_bytes(b4) as usize;
            for _ in 0..ncat { f.read_exact(&mut b4).map_err(|e| e.to_string())?; let l = u32::from_le_bytes(b4) as usize; let mut tb = vec![0u8; l]; f.read_exact(&mut tb).map_err(|e| e.to_string())?; let text = String::from_utf8_lossy(&tb).into_owned(); meta(&text)?; catalog.push(text); }
            let mut b2 = [0u8; 2]; f.read_exact(&mut b2).map_err(|e| e.to_string())?; let ntables = u16::from_le_bytes(b2) as usize;
            let mut dir = vec![0u8; SNAP_HDR * ntables]; f.read_exact(&mut dir).map_err(|e| e.to_string())?;
            // the rows stay in the file: map it privately (copy-on-write), point the chunks and the mapped indexes at it
            let mut mm = unsafe { MmapOptions::new().map_copy(&f) }.map_err(|e| e.to_string())?; let base = mm.as_mut_ptr();
            let mm = Arc::new(mm);  // kernel-default readahead (MADV_RANDOM measured worse: E25); scans prefetch explicitly
            let _ = threads;
            for ti in 0..ntables {
                let h = &dir[SNAP_HDR * ti..SNAP_HDR * (ti + 1)];
                let rd = |o: usize| u64::from_le_bytes(h[o..o + 8].try_into().unwrap());
                let (id, row_size, n, stride, ucap, max_v) = (u16::from_le_bytes(h[..2].try_into().unwrap()), u32::from_le_bytes(h[4..8].try_into().unwrap()) as usize, rd(8) as usize, u32::from_le_bytes(h[16..20].try_into().unwrap()) as usize, rd(24) as usize, rd(32));
                let (fence_n, kmin, kmax) = (rd(64) as usize, rd(72) as i64, rd(80) as i64);
                let tb = self.table_by_id(id).ok_or(format!("snapshot refers to unknown table {id}"))?;
                if tb.schema.row_size != row_size || tb.stride != stride { return Err(format!("snapshot row size mismatch for table {}", tb.schema.name)); }
                let mut fence = vec![0i64; fence_n];
                if fence_n > 0 { f.seek(SeekFrom::Start(rd(48))).map_err(|e| e.to_string())?; f.read_exact(unsafe { std::slice::from_raw_parts_mut(fence.as_mut_ptr() as *mut u8, fence_n * 8) }).map_err(|e| e.to_string())?; }
                tb.attach_base(mm.clone(), base, n, [rd(40), rd(48), rd(56)], ucap, fence, kmin, kmax);
                tb.backfill_indexes();
                max_seq = max_seq.max(max_v); st.snap_rows += n as u64;
            }
        }
        st.snap_secs = t.elapsed().as_secs_f64();
        let t = Instant::now();
        let segs = GLog::list_segments(&self.dir); st.segments = segs.len();
        for first in &segs {
            let data = fs::read(self.log.seg_path(*first)).unwrap();
            let (recs, _) = parse_glog(&data);
            for (seq, op, table, payload) in recs {
                if seq < st.low_water { st.skipped += 1; continue; }
                max_seq = max_seq.max(seq); st.replayed += 1;
                match op {
                    OP_META => { let text = String::from_utf8_lossy(&payload).into_owned(); meta(&text)?; catalog.push(text); }
                    OP_INSERT | OP_UPDATE => { self.table_by_id(table).ok_or(format!("log refers to unknown table {table}"))?.replay_record(seq, &payload); }
                    OP_DELETE => { self.table_by_id(table).ok_or(format!("log refers to unknown table {table}"))?.replay_delete(seq, &payload); }
                    OP_TXN => { let mut p = 0; while p + 7 <= payload.len() { let tid = u16::from_le_bytes(payload[p..p + 2].try_into().unwrap()); let op2 = payload[p + 2]; let len = u32::from_le_bytes(payload[p + 3..p + 7].try_into().unwrap()) as usize; p += 7; let tb = self.table_by_id(tid).ok_or(format!("log refers to unknown table {tid}"))?; if op2 == OP_DELETE { tb.replay_delete(seq, &payload[p..p + len]); } else { tb.replay_record(seq, &payload[p..p + len]); } p += len; } }
                    _ => return Err(format!("unknown log op {op}")),
                }
            }
        }
        st.replay_secs = t.elapsed().as_secs_f64(); st.catalog = catalog.len();
        { let mut s = self.log.st.lock().unwrap(); s.seq = max_seq; s.durable = max_seq; s.segments = segs; s.catalog = catalog; }
        Ok(st)
    }
}
impl Drop for Database { fn drop(&mut self) { self.log.stop(); if let Some(h) = self.committer.lock().unwrap().take() { h.join().ok(); } } }

// ---------- multi-statement transactions (optimistic, first committer wins) ----------
pub const TXN_MARK: u64 = 1 << 63;
static TXN_IDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

pub enum TxnWrite {
    Insert { table: Arc<Table>, row: Vec<u8>, slot: u32 },
    Update { table: Arc<Table>, slot: u32, base: u64, row: Vec<u8>, old_key: Option<Vec<u8>> },
    Delete { table: Arc<Table>, slot: u32, base: u64, row: Vec<u8> },
}
impl TxnWrite {
    fn row(&self) -> &Vec<u8> { match self { TxnWrite::Insert { row, .. } | TxnWrite::Update { row, .. } | TxnWrite::Delete { row, .. } => row } }
    fn table(&self) -> &Arc<Table> { match self { TxnWrite::Insert { table, .. } | TxnWrite::Update { table, .. } | TxnWrite::Delete { table, .. } => table } }
    fn slot(&self) -> u32 { match self { TxnWrite::Insert { slot, .. } | TxnWrite::Update { slot, .. } | TxnWrite::Delete { slot, .. } => *slot } }
    fn op(&self) -> u8 { match self { TxnWrite::Insert { .. } => OP_INSERT, TxnWrite::Update { .. } => OP_UPDATE, TxnWrite::Delete { .. } => OP_DELETE } }
}
pub struct Txn { pub id: u64, pub writes: Vec<TxnWrite>, pub reads: Vec<(Arc<Table>, u32, u64)>, by_key: HashMap<(u16, i64), usize> }
impl Txn { pub fn new() -> Txn { Txn { id: TXN_IDS.fetch_add(1, Ordering::Relaxed), writes: vec![], reads: vec![], by_key: HashMap::new() } } }
impl Default for Txn { fn default() -> Self { Self::new() } }

impl Table {
    #[inline] fn hdr_at(d: &[u8], off: usize) -> (u64, u64) { (u64::from_le_bytes(d[off..off + 8].try_into().unwrap()), u64::from_le_bytes(d[off + 8..off + 16].try_into().unwrap())) }
    /// Read inside a transaction: own writes first, then committed state (recorded for validation).
    pub fn txn_get(self: &Arc<Self>, txn: &mut Txn, pk: i64) -> Option<Vec<u8>> {
        if let Some(&i) = txn.by_key.get(&(self.id, pk)) { return match &txn.writes[i] { TxnWrite::Delete { .. } => None, w => Some(w.row().clone()) }; }
        let slot = self.pk_slot(pk)?;
        let (v, row) = self.with_slot(slot, |v, r| (v, r.to_vec()))?;
        if self.pk_of(&row) != pk { return None; }
        txn.reads.push((self.clone(), slot, v));
        Some(row)
    }
    pub fn txn_delete(self: &Arc<Self>, txn: &mut Txn, pk: i64) -> Result<(), Abort> {
        if let Some(&i) = txn.by_key.get(&(self.id, pk)) {
            match &txn.writes[i] {
                TxnWrite::Delete { .. } => return Err(Abort::NotFound),
                TxnWrite::Insert { .. } => { txn.writes[i] = TxnWrite::Insert { table: self.clone(), row: vec![], slot: u32::MAX }; txn.by_key.remove(&(self.id, pk)); return Ok(()); } // tombstoned own insert (skipped at commit)
                TxnWrite::Update { slot, base, .. } => { let (slot, base) = (*slot, *base); let old = self.with_slot(slot, |_, r| r.to_vec()).ok_or(Abort::NotFound)?; txn.writes[i] = TxnWrite::Delete { table: self.clone(), slot, base, row: old }; return Ok(()); }
            }
        }
        let slot = match self.pk_slot(pk) { Some(s) => s, None => return Err(Abort::NotFound) };
        let (ver, row) = self.with_slot(slot, |v, r| (v, r.to_vec())).ok_or(Abort::NotFound)?;
        if self.pk_of(&row) != pk { return Err(Abort::NotFound); }
        txn.by_key.insert((self.id, pk), txn.writes.len());
        txn.writes.push(TxnWrite::Delete { table: self.clone(), slot, base: ver, row });
        Ok(())
    }
    pub fn txn_insert(self: &Arc<Self>, txn: &mut Txn, row: &[u8]) -> Result<(), Abort> {
        let pk = self.pk_of(row);
        if let Some(&i) = txn.by_key.get(&(self.id, pk)) {
            if let TxnWrite::Delete { slot, base, row: old, .. } = &txn.writes[i] { let (slot, base, old_key) = (*slot, *base, self.unique_key(old).map(|k| k.to_vec())); txn.writes[i] = TxnWrite::Update { table: self.clone(), slot, base, row: row.to_vec(), old_key }; return Ok(()); }
            return Err(Abort::Duplicate);
        }
        if self.pk_slot(pk).is_some() { return Err(Abort::Duplicate); }
        txn.by_key.insert((self.id, pk), txn.writes.len());
        txn.writes.push(TxnWrite::Insert { table: self.clone(), row: row.to_vec(), slot: 0 });
        Ok(())
    }
    pub fn txn_update(self: &Arc<Self>, txn: &mut Txn, pk: i64, f: impl FnOnce(&[u8]) -> Result<Vec<u8>, Abort>) -> Result<(), Abort> {
        if let Some(&i) = txn.by_key.get(&(self.id, pk)) {
            if matches!(txn.writes[i], TxnWrite::Delete { .. }) { return Err(Abort::NotFound); }
            let new = f(txn.writes[i].row())?;
            if new.len() != self.schema.row_size || self.pk_of(&new) != pk { return Err(Abort::Type("row shape or primary key changed".into())); }
            match &mut txn.writes[i] { TxnWrite::Insert { row, .. } | TxnWrite::Update { row, .. } => *row = new, TxnWrite::Delete { .. } => unreachable!() }
            return Ok(());
        }
        let slot = match self.pk_slot(pk) { Some(s) => s, None => return Err(Abort::NotFound) };
        let (ver, row) = self.with_slot(slot, |v, r| (v, r.to_vec())).ok_or(Abort::NotFound)?;
        if self.pk_of(&row) != pk { return Err(Abort::NotFound); }
        let new = f(&row)?;
        if new.len() != self.schema.row_size || self.pk_of(&new) != pk { return Err(Abort::Type("row shape or primary key changed".into())); }
        let old_key = self.unique_key(&row).map(|k| k.to_vec());
        txn.by_key.insert((self.id, pk), txn.writes.len());
        txn.writes.push(TxnWrite::Update { table: self.clone(), slot, base: ver, row: new, old_key });
        Ok(())
    }
    // phase-1 claims
    fn claim_insert(&self, row: &[u8], slot_out: &mut u32, mark: u64) -> Result<(), Abort> {
        let pk = self.pk_of(row); let ukey = self.unique_key(row).map(|k| k.to_vec());
        let slot = self.alloc_slot();
        let (ck, off) = (slot as usize / CHUNK_ROWS, (slot as usize % CHUNK_ROWS) * self.stride);
        let mut pkm = self.pk[Self::pk_shard(pk)].write().unwrap();
        if pkm.contains_key(&pk) || self.mapped_live(pk) { self.free_slot(slot); return Err(Abort::Duplicate); }
        let h = ukey.as_ref().map(|k| hash_bytes(k)).unwrap_or(0);
        let mut secm = ukey.as_ref().map(|_| self.sec[Self::sec_shard_h(h)].write().unwrap());
        if let (Some(m), Some(k)) = (&secm, &ukey) { if self.uniq_contains_locked(m, h, k) { self.free_slot(slot); return Err(Abort::Duplicate); } }
        let mut c = self.chunks[ck].write().unwrap();
        pkm.insert(pk, slot);
        let d = Self::alloc(&mut c, self.stride);
        d[off..off + 8].copy_from_slice(&0u64.to_le_bytes()); d[off + 8..off + 16].copy_from_slice(&mark.to_le_bytes()); d[off + 16..off + self.stride].copy_from_slice(row);
        if let Some(m) = &mut secm { Self::uniq_insert_locked(m, h, slot); }
        *slot_out = slot;
        Ok(())
    }
    fn unclaim_insert(&self, row: &[u8], slot: u32) {
        let pk = self.pk_of(row);
        self.pk[Self::pk_shard(pk)].write().unwrap().remove(&pk);
        if let Some(k) = self.unique_key(row) { self.uniq_remove(k, slot); }
        let (ck, off) = (slot as usize / CHUNK_ROWS, (slot as usize % CHUNK_ROWS) * self.stride);
        { let mut c = self.chunks[ck].write().unwrap(); let d = Self::alloc(&mut c, self.stride); d[off..off + 16].copy_from_slice(&[0u8; 16]); }
        self.free_slot(slot);
    }
    fn claim_update(&self, slot: u32, base: u64, new: &[u8], old_key: Option<&[u8]>, mark: u64) -> Result<(), Abort> {
        let new_key = self.unique_key(new);
        let key_changed = new_key != old_key;
        let (ck, off) = (slot as usize / CHUNK_ROWS, (slot as usize % CHUNK_ROWS) * self.stride);
        let nh = new_key.map(hash_bytes);
        let mut secm = if key_changed && nh.is_some() { Some(self.sec[Self::sec_shard_h(nh.unwrap())].write().unwrap()) } else { None };
        if let (Some(m), Some(k)) = (&secm, new_key) { if self.uniq_contains_locked(m, nh.unwrap(), k) { return Err(Abort::Duplicate); } }
        let mut c = self.chunks[ck].write().unwrap(); let d = Self::alloc(&mut c, self.stride);
        let (v, p) = Self::hdr_at(d, off);
        if p != 0 || v != base { return Err(Abort::Conflict); }
        d[off + 8..off + 16].copy_from_slice(&mark.to_le_bytes());
        if let Some(m) = &mut secm { Self::uniq_insert_locked(m, nh.unwrap(), slot); }
        Ok(())
    }
    fn unclaim_update(&self, slot: u32, new: &[u8], old_key: Option<&[u8]>) {
        let (ck, off) = (slot as usize / CHUNK_ROWS, (slot as usize % CHUNK_ROWS) * self.stride);
        { let mut c = self.chunks[ck].write().unwrap(); let d = Self::alloc(&mut c, self.stride); d[off + 8..off + 16].copy_from_slice(&0u64.to_le_bytes()); }
        let new_key = self.unique_key(new);
        if new_key != old_key { if let Some(k) = new_key { self.uniq_remove(k, slot); } }
    }
    fn claim_delete(&self, slot: u32, base: u64, mark: u64) -> Result<(), Abort> {
        let (ck, off) = (slot as usize / CHUNK_ROWS, (slot as usize % CHUNK_ROWS) * self.stride);
        let mut c = self.chunks[ck].write().unwrap(); let d = Self::alloc(&mut c, self.stride);
        let (v, p) = Self::hdr_at(d, off); if p != 0 || v != base { return Err(Abort::Conflict); }
        d[off + 8..off + 16].copy_from_slice(&mark.to_le_bytes()); Ok(())
    }
    fn unclaim_delete(&self, slot: u32) { let (ck, off) = (slot as usize / CHUNK_ROWS, (slot as usize % CHUNK_ROWS) * self.stride); let mut c = self.chunks[ck].write().unwrap(); let d = Self::alloc(&mut c, self.stride); d[off + 8..off + 16].copy_from_slice(&0u64.to_le_bytes()); }
    pub fn with_chunk<T>(&self, ck: usize, f: impl FnOnce(&[u8]) -> T) -> Option<T> { let c = self.chunks[ck].read().unwrap(); c.data.as_ref().map(|d| f(d)) }
    pub fn chunk_lock(&self, ck: usize) -> std::sync::RwLockWriteGuard<'_, Chunk> { self.chunks[ck].write().unwrap() }
    pub fn alloc_in<'a>(&self, c: &'a mut Chunk) -> &'a mut [u8] { Self::alloc(c, self.stride) }
}

impl Database {
    /// Commit: claim every write (undo all on failure), lock all touched chunks in a canonical order, validate reads,
    /// append ONE atomic log record, mark pending with the commit sequence, wait for durability, publish.
    pub fn commit(&self, mut txn: Txn) -> Result<u64, Abort> {
        txn.writes.retain(|w| !matches!(w, TxnWrite::Insert { slot: u32::MAX, .. }));
        if txn.writes.is_empty() { return Ok(0); }
        let mark = TXN_MARK | txn.id;
        let mut claimed = 0; let mut err = None;
        for i in 0..txn.writes.len() {
            let r = match &mut txn.writes[i] {
                TxnWrite::Insert { table, row, slot } => table.claim_insert(row, slot, mark),
                TxnWrite::Update { table, slot, base, row, old_key } => table.claim_update(*slot, *base, row, old_key.as_deref(), mark),
                TxnWrite::Delete { table, slot, base, .. } => table.claim_delete(*slot, *base, mark),
            };
            if let Err(e) = r { err = Some(e); break; }
            claimed += 1;
        }
        let undo = |txn: &Txn, n: usize| { for w in txn.writes[..n].iter().rev() { match w { TxnWrite::Insert { table, row, slot } => table.unclaim_insert(row, *slot), TxnWrite::Update { table, slot, row, old_key, .. } => table.unclaim_update(*slot, row, old_key.as_deref()), TxnWrite::Delete { table, slot, .. } => table.unclaim_delete(*slot) } } };
        if let Some(e) = err { undo(&txn, claimed); return Err(e); }
        // phase 2
        let mut locks: Vec<(u16, usize)> = txn.writes.iter().map(|w| (w.table().id, w.slot() as usize / CHUNK_ROWS)).collect();
        locks.extend(txn.reads.iter().map(|(t, s, _)| (t.id, *s as usize / CHUNK_ROWS)));
        locks.sort_unstable(); locks.dedup();
        let tabs: Vec<Arc<Table>> = locks.iter().map(|(tid, _)| self.table_by_id(*tid).unwrap()).collect();
        let seq;
        {
            let mut guards: Vec<std::sync::RwLockWriteGuard<'_, Chunk>> = locks.iter().enumerate().map(|(i, (_, ck))| tabs[i].chunk_lock(*ck)).collect();
            let idx = |tid: u16, ck: usize| locks.binary_search(&(tid, ck)).unwrap();
            // validate reads
            for (t, slot, v) in &txn.reads {
                let i = idx(t.id, *slot as usize / CHUNK_ROWS);
                let d = t.alloc_in(&mut guards[i]); let off = (*slot as usize % CHUNK_ROWS) * t.stride;
                let (cv, cp) = Table::hdr_at(d, off);
                if cv != *v || (cp != 0 && cp != mark) { drop(guards); undo(&txn, claimed); return Err(Abort::Conflict); }
            }
            let mut payload = Vec::with_capacity(txn.writes.iter().map(|w| w.row().len() + 7).sum());
            for w in &txn.writes { let key; let bytes: &[u8] = if let TxnWrite::Delete { table, row, .. } = w { key = table.pk_of(row).to_le_bytes(); &key } else { w.row() }; payload.extend_from_slice(&w.table().id.to_le_bytes()); payload.push(w.op()); payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes()); payload.extend_from_slice(bytes); }
            seq = self.log.append(OP_TXN, u16::MAX, &payload);
            for w in &txn.writes {
                let (t, slot) = (w.table(), w.slot());
                let i = idx(t.id, slot as usize / CHUNK_ROWS); let d = t.alloc_in(&mut guards[i]); let off = (slot as usize % CHUNK_ROWS) * t.stride;
                d[off + 8..off + 16].copy_from_slice(&seq.to_le_bytes());
            }
        }
        if self.durable { self.log.wait_durable(seq); }
        // phase 3: publish
        for w in &txn.writes {
            let (t, slot) = (w.table(), w.slot());
            let (ck, off) = (slot as usize / CHUNK_ROWS, (slot as usize % CHUNK_ROWS) * t.stride);
            if let TxnWrite::Delete { row, .. } = w {
                { let mut c = t.chunk_lock(ck); let d = t.alloc_in(&mut c); d[off..off + 16].copy_from_slice(&[0u8; 16]); }
                t.unpublish(slot, row); t.free_slot(slot); continue;
            }
            let old: Option<Vec<u8>> = { let mut c = t.chunk_lock(ck); let d = t.alloc_in(&mut c);
                d[off..off + 8].copy_from_slice(&seq.to_le_bytes()); d[off + 8..off + 16].copy_from_slice(&0u64.to_le_bytes());
                if let TxnWrite::Update { row, .. } = w { let old = d[off + 16..off + t.stride].to_vec(); d[off + 16..off + t.stride].copy_from_slice(row); Some(old) } else { None } };
            match (w, old) { (TxnWrite::Insert { row, .. }, _) => t.idx_add(row, slot), (TxnWrite::Update { row, .. }, Some(old)) => t.idx_replace(&old, row, slot), _ => {} }
        }
        for w in &txn.writes { if let TxnWrite::Update { table, row, old_key: Some(k), slot, .. } = w { if table.unique_key(row) != Some(&k[..]) { table.uniq_remove(k, *slot); } } }
        Ok(seq)
    }
}
