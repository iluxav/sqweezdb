pub struct Rng(pub u64);
impl Rng { pub fn next(&mut self) -> u64 { let mut x = self.0; x ^= x << 13; x ^= x >> 7; x ^= x << 17; self.0 = x; x } }

pub fn crc32(data: &[u8]) -> u32 {
    // CRC-32C (Castagnoli), table-driven
    static TABLE: std::sync::OnceLock<[u32; 256]> = std::sync::OnceLock::new();
    let t = TABLE.get_or_init(|| { let mut t = [0u32; 256]; for i in 0..256u32 { let mut c = i; for _ in 0..8 { c = if c & 1 != 0 { 0x82F63B78 ^ (c >> 1) } else { c >> 1 }; } t[i as usize] = c; } t });
    let mut c = !0u32;
    for &b in data { c = t[((c ^ b as u32) & 0xFF) as usize] ^ (c >> 8); }
    !c
}

#[derive(Default)] pub struct IdHasher(u64);
impl std::hash::Hasher for IdHasher { fn finish(&self) -> u64 { self.0 } fn write(&mut self, b: &[u8]) { for x in b { self.0 = (self.0 ^ *x as u64).wrapping_mul(0x9E3779B97F4A7C15); } } fn write_i64(&mut self, i: i64) { self.0 = (i as u64).wrapping_mul(0x9E3779B97F4A7C15) >> 17; } fn write_u64(&mut self, i: u64) { self.0 = i.wrapping_mul(0x9E3779B97F4A7C15) >> 17; } }
pub type FastMapU64<V> = std::collections::HashMap<u64, V, std::hash::BuildHasherDefault<IdHasher>>;
/// 64-bit hash of a byte string (FNV-1a with a final mix).
pub fn hash_bytes(b: &[u8]) -> u64 { let mut h: u64 = 0xcbf29ce484222325; for x in b { h ^= *x as u64; h = h.wrapping_mul(0x100000001b3); } h ^= h >> 33; h = h.wrapping_mul(0xff51afd7ed558ccd); h ^= h >> 33; h = h.wrapping_mul(0xc4ceb9fe1a85ec53); h ^ (h >> 33) }
pub type FastMap<V> = std::collections::HashMap<i64, V, std::hash::BuildHasherDefault<IdHasher>>;

/// Hasher for 128-bit keys (UUID primary keys): fold the halves and mix, one multiply; keys are already random or
/// time-ordered, so a cryptographic hash would only cost time.
#[derive(Default)] pub struct WideHasher(u64);
impl std::hash::Hasher for WideHasher {
    fn finish(&self) -> u64 { self.0 }
    fn write(&mut self, b: &[u8]) { for x in b { self.0 = (self.0 ^ *x as u64).wrapping_mul(0x9E3779B97F4A7C15); } }
    fn write_i128(&mut self, k: i128) { let x = (k as u64) ^ ((k >> 64) as u64).rotate_left(29); let x = (x ^ (x >> 32)).wrapping_mul(0xD6E8FEB86659FD93); self.0 = x ^ (x >> 32); }
}
pub type WideMap<V> = std::collections::HashMap<i128, V, std::hash::BuildHasherDefault<WideHasher>>;
pub type WideSet = std::collections::HashSet<i128, std::hash::BuildHasherDefault<WideHasher>>;
/// A transaction's (table, key) -> write index: a small, hot map; the default SipHash dominated its cost.
#[derive(Default)] pub struct TxnKeyHasher(u64);
impl std::hash::Hasher for TxnKeyHasher {
    fn finish(&self) -> u64 { let x = self.0; let x = (x ^ (x >> 31)).wrapping_mul(0x9E3779B97F4A7C15); x ^ (x >> 29) }
    fn write(&mut self, b: &[u8]) { for x in b { self.0 = (self.0 ^ *x as u64).wrapping_mul(0x100000001b3); } }
    fn write_u16(&mut self, v: u16) { self.0 ^= (v as u64) << 48; }
    fn write_i128(&mut self, k: i128) { self.0 ^= (k as u64) ^ ((k >> 64) as u64).rotate_left(17); }
}
pub type TxnKeyMap = std::collections::HashMap<(u16, i128), usize, std::hash::BuildHasherDefault<TxnKeyHasher>>;
