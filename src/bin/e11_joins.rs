//! Experiment 11 (H13): the join round. Same 1M users + 5M orders in SQLite (tuned) and in typed contiguous rows.
use sqweezdb::util::Rng;
use rusqlite::{Connection, params};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

const USERS: usize = 1_000_000;
const ORDERS: usize = 5_000_000;
#[repr(C)] #[derive(Clone, Copy)] struct User { id: u64, score: f64, country: u32, age: u32, name: [u8; 32], payload: [u8; 64] }
#[repr(C)] #[derive(Clone, Copy)] struct Order { id: u64, user_id: u64, amount: f64, status: u32, _pad: u32, payload: [u8; 32] }

fn gen_data() -> (Vec<User>, Vec<Order>) {
    let mut rng = Rng(7);
    let users = (1..=USERS as u64).map(|i| User { id: i, score: (rng.next() % 100000) as f64 / 100.0, country: (i % 20) as u32, age: (18 + i % 60) as u32, name: [b'n'; 32], payload: [7; 64] }).collect();
    let orders = (1..=ORDERS as u64).map(|i| Order { id: i, user_id: rng.next() % USERS as u64 + 1, amount: (rng.next() % 50000) as f64 / 100.0, status: (i % 4) as u32, _pad: 0, payload: [3; 32] }).collect();
    (users, orders)
}

fn median(label: &str, iters: usize, mut f: impl FnMut() -> f64) -> f64 {
    let mut ts = Vec::new(); let mut v = 0.0;
    for _ in 0..iters { let t = Instant::now(); v = f(); ts.push(t.elapsed()); }
    ts.sort(); let m = ts[iters / 2].as_secs_f64() * 1e3;
    println!("{:<66} median={:>10.3}ms   (check {:.1})", label, m, v); m
}
fn percall(label: &str, n: usize, mut f: impl FnMut() -> f64) {
    let t = Instant::now(); let mut v = 0.0; for _ in 0..n { v += f(); } let el = t.elapsed();
    println!("{:<66} per-call={:>8.2}us  calls/s={:>9.0}   (check {:.1})", label, el.as_secs_f64() * 1e6 / n as f64, n as f64 / el.as_secs_f64(), v);
}

fn sqlite_side(users: &[User], orders: &[Order]) {
    let p = PathBuf::from("data/nocow/join.db");
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
    let c = Connection::open(&p).unwrap();
    c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA cache_size=-262144; PRAGMA mmap_size=1073741824;
        CREATE TABLE users(id INTEGER PRIMARY KEY, score REAL NOT NULL, country INTEGER NOT NULL, age INTEGER NOT NULL, name TEXT NOT NULL, payload BLOB NOT NULL);
        CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, amount REAL NOT NULL, status INTEGER NOT NULL, payload BLOB NOT NULL);").unwrap();
    let t = Instant::now();
    let tx = c.unchecked_transaction().unwrap();
    { let mut st = tx.prepare("INSERT INTO users VALUES(?,?,?,?,?,?)").unwrap(); for u in users { st.execute(params![u.id as i64, u.score, u.country, u.age, "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn", &u.payload[..]]).unwrap(); } }
    { let mut st = tx.prepare("INSERT INTO orders VALUES(?,?,?,?,?)").unwrap(); for o in orders { st.execute(params![o.id as i64, o.user_id as i64, o.amount, o.status, &o.payload[..]]).unwrap(); } }
    tx.commit().unwrap();
    let t2 = Instant::now();
    c.execute_batch("CREATE INDEX orders_user ON orders(user_id); ANALYZE; PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    println!("[sqlite] build {:.1}s, index on orders(user_id) {:.1}s, file {:.0} MB", t2.duration_since(t).as_secs_f64(), t2.elapsed().as_secs_f64(), fs::metadata(&p).unwrap().len() as f64 / 1e6);
    let _: f64 = c.query_row("SELECT sum(amount) FROM orders", [], |r| r.get(0)).unwrap();
    let mut rng = Rng(99);
    { let mut st = c.prepare("SELECT count(*), sum(amount) FROM orders WHERE user_id = ?").unwrap();
      percall("[sqlite] Q5 orders of one user via index (~5 rows): count,sum", 200_000, || { let u = rng.next() % USERS as u64 + 1; st.query_row(params![u as i64], |r| r.get::<_, Option<f64>>(1)).unwrap().unwrap_or(0.0) }); }
    { let mut st = c.prepare("SELECT u.id, sum(o.amount) FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id >= ? AND u.id < ? GROUP BY u.id").unwrap();
      percall("[sqlite] Q6 join 100 users -> ~500 orders, group by user", 2000, || { let s = rng.next() % (USERS as u64 - 100) + 1; st.query_map(params![s as i64, (s + 100) as i64], |r| r.get::<_, f64>(1)).unwrap().map(|x| x.unwrap()).sum() }); }
    median("[sqlite] Q7 join 5M orders x users WHERE country=7: count,sum", 5, || c.query_row("SELECT count(*), sum(o.amount) FROM orders o JOIN users u ON u.id = o.user_id WHERE u.country = 7", [], |r| r.get::<_, f64>(1)).unwrap());
    median("[sqlite] Q10 join 5M orders x users GROUP BY country: sum(amount)", 3, || { let mut st = c.prepare_cached("SELECT u.country, sum(o.amount) FROM orders o JOIN users u ON u.id = o.user_id GROUP BY u.country").unwrap(); st.query_map([], |r| r.get::<_, f64>(1)).unwrap().map(|x| x.unwrap()).sum() });
    for suf in ["", "-wal", "-shm"] { fs::remove_file(format!("{}{}", p.display(), suf)).ok(); }
}

// tiny open-addressing u64 set
struct U64Set { slots: Vec<u64>, mask: usize }
impl U64Set {
    fn new(cap: usize) -> U64Set { let n = (cap * 2).next_power_of_two(); U64Set { slots: vec![0; n], mask: n - 1 } }
    #[inline] fn h(&self, k: u64) -> usize { (k.wrapping_mul(0x9E3779B97F4A7C15) >> 20) as usize & self.mask }
    fn insert(&mut self, k: u64) { let mut i = self.h(k); loop { if self.slots[i] == 0 { self.slots[i] = k; return; } if self.slots[i] == k { return; } i = (i + 1) & self.mask; } }
    #[inline] fn contains(&self, k: u64) -> bool { let mut i = self.h(k); loop { let s = self.slots[i]; if s == k { return true; } if s == 0 { return false; } i = (i + 1) & self.mask; } }
}

fn par_sum<T: Sync>(items: &[T], threads: usize, f: impl Fn(&[T]) -> (u64, f64) + Sync) -> (u64, f64) {
    let per = (items.len() + threads - 1) / threads;
    std::thread::scope(|s| { let hs: Vec<_> = items.chunks(per).map(|c| s.spawn(|| f(c))).collect(); hs.into_iter().map(|h| h.join().unwrap()).fold((0, 0.0), |a, b| (a.0 + b.0, a.1 + b.1)) })
}

fn ours(users: &[User], orders: &[Order]) {
    // index on orders(user_id): CSR (dense keys) and sorted pairs (generic)
    let t = Instant::now();
    let mut counts = vec![0u32; USERS + 2];
    for o in orders { counts[o.user_id as usize + 1] += 1; }
    for i in 1..counts.len() { counts[i] += counts[i - 1]; }
    let offsets = counts; let mut fill = offsets.clone(); let mut slots = vec![0u32; ORDERS];
    for (i, o) in orders.iter().enumerate() { let u = o.user_id as usize; slots[fill[u] as usize] = i as u32; fill[u] += 1; }
    println!("[ours] build CSR index on orders(user_id): {:.3}s ({:.0} MB)", t.elapsed().as_secs_f64(), (offsets.len() * 4 + slots.len() * 4) as f64 / 1e6);
    let t = Instant::now();
    let mut pairs: Vec<(u64, u32)> = orders.iter().enumerate().map(|(i, o)| (o.user_id, i as u32)).collect();
    pairs.sort_unstable();
    println!("[ours] build sorted-pairs index on orders(user_id): {:.3}s ({:.0} MB)", t.elapsed().as_secs_f64(), (pairs.len() * 16) as f64 / 1e6);

    let mut rng = Rng(99);
    percall("[ours] Q5 orders of one user via CSR index: count,sum", 200_000, || { let u = rng.next() as usize % USERS + 1; slots[offsets[u] as usize..offsets[u + 1] as usize].iter().map(|&s| orders[s as usize].amount).sum() });
    percall("[ours] Q5 orders of one user via sorted-pairs index (binary search)", 200_000, || { let u = rng.next() % USERS as u64 + 1; let lo = pairs.partition_point(|p| p.0 < u); let hi = pairs.partition_point(|p| p.0 <= u); pairs[lo..hi].iter().map(|p| orders[p.1 as usize].amount).sum() });
    percall("[ours] Q6 join 100 users -> ~500 orders via CSR, group by user", 2000, || { let s = rng.next() as usize % (USERS - 100) + 1; let mut tot = 0.0; for u in s..s + 100 { let sum: f64 = slots[offsets[u] as usize..offsets[u + 1] as usize].iter().map(|&x| orders[x as usize].amount).sum(); tot += sum; } tot });

    median("[ours] Q7 bitmap semi-join (filter users -> 128KB bitmap -> scan 5M orders), 1 thread", 5, || {
        let mut bits = vec![0u64; USERS / 64 + 2];
        for u in users { if u.country == 7 { bits[u.id as usize >> 6] |= 1 << (u.id & 63); } }
        let mut n = 0u64; let mut s = 0.0; for o in orders { if bits[o.user_id as usize >> 6] >> (o.user_id & 63) & 1 == 1 { n += 1; s += o.amount; } } let _ = n; s });
    median("[ours] Q7 bitmap semi-join, 12 threads", 5, || {
        let mut bits = vec![0u64; USERS / 64 + 2];
        for u in users { if u.country == 7 { bits[u.id as usize >> 6] |= 1 << (u.id & 63); } }
        let bits = &bits;
        par_sum(orders, 12, |c| { let mut n = 0; let mut s = 0.0; for o in c { if bits[o.user_id as usize >> 6] >> (o.user_id & 63) & 1 == 1 { n += 1; s += o.amount; } } (n, s) }).1 });
    median("[ours] Q7 hash-set semi-join (50k keys open addressing), 1 thread", 5, || {
        let mut set = U64Set::new(60_000); for u in users { if u.country == 7 { set.insert(u.id); } }
        let mut s = 0.0; for o in orders { if set.contains(o.user_id) { s += o.amount; } } s });
    median("[ours] Q7 direct probe users[user_id].country (random access), 1 thread", 5, || { let mut s = 0.0; for o in orders { if users[o.user_id as usize - 1].country == 7 { s += o.amount; } } s });

    median("[ours] Q10 join GROUP BY country via 1MB country projection, 1 thread", 5, || {
        let mut country = vec![0u8; USERS + 1]; for u in users { country[u.id as usize] = u.country as u8; }
        let mut sums = [0f64; 20]; for o in orders { sums[country[o.user_id as usize] as usize] += o.amount; } sums.iter().sum() });
    median("[ours] Q10 join GROUP BY country via projection, 12 threads", 5, || {
        let mut country = vec![0u8; USERS + 1]; for u in users { country[u.id as usize] = u.country as u8; }
        let country = &country;
        let per = (orders.len() + 11) / 12;
        std::thread::scope(|s| { let hs: Vec<_> = orders.chunks(per).map(|c| s.spawn(move || { let mut sums = [0f64; 20]; for o in c { sums[country[o.user_id as usize] as usize] += o.amount; } sums })).collect();
            hs.into_iter().map(|h| h.join().unwrap()).fold([0f64; 20], |mut a, b| { for i in 0..20 { a[i] += b[i]; } a }).iter().sum() }) });
}

fn main() {
    println!("=== E11 / H13: joins (1M users, 5M orders) ===");
    let (users, orders) = gen_data();
    sqlite_side(&users, &orders);
    ours(&users, &orders);
}
