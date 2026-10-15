//! Experiment 02: physics of durable multi-writer throughput.
//! (a) group commit: N writers share ONE fsync per batch.  (b) can the NVMe run fsyncs in parallel on separate files?
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::sync::{Arc, Barrier, Condvar, Mutex};
use std::time::Instant;

struct State { buf: Vec<u8>, pending: u64, durable: u64, stop: bool, batches: u64, batched_txns: u64 }
struct GroupLog { st: Mutex<State>, cv_batch: Condvar, cv_durable: Condvar }

fn committer(log: Arc<GroupLog>, mut f: File) {
    let mut local = Vec::with_capacity(1 << 20);
    loop {
        let seq_end;
        {
            let mut s = log.st.lock().unwrap();
            while s.buf.is_empty() && !s.stop { s = log.cv_batch.wait(s).unwrap(); }
            if s.buf.is_empty() && s.stop { return; }
            std::mem::swap(&mut local, &mut s.buf);
            seq_end = s.pending;
            s.batches += 1; s.batched_txns += (seq_end - s.durable) as u64;
        }
        f.write_all(&local).unwrap();
        f.sync_data().unwrap();
        local.clear();
        let mut s = log.st.lock().unwrap();
        s.durable = seq_end;
        log.cv_durable.notify_all();
    }
}

fn group_commit(path: &std::path::Path, writers: usize, per_writer: usize) {
    let f = OpenOptions::new().create(true).write(true).truncate(true).open(path).unwrap();
    let log = Arc::new(GroupLog { st: Mutex::new(State { buf: vec![], pending: 0, durable: 0, stop: false, batches: 0, batched_txns: 0 }), cv_batch: Condvar::new(), cv_durable: Condvar::new() });
    let c = { let l = log.clone(); std::thread::spawn(move || committer(l, f)) };
    let barrier = Arc::new(Barrier::new(writers + 1));
    let payload = [0x5Au8; 128];
    let hs: Vec<_> = (0..writers).map(|_| { let l = log.clone(); let b = barrier.clone(); std::thread::spawn(move || {
        b.wait();
        for _ in 0..per_writer {
            let my;
            { let mut s = l.st.lock().unwrap(); s.buf.extend_from_slice(&payload); s.pending += 1; my = s.pending; l.cv_batch.notify_one();
              while s.durable < my { s = l.cv_durable.wait(s).unwrap(); } }
        }
    })}).collect();
    barrier.wait();
    let t = Instant::now();
    for h in hs { h.join().unwrap(); }
    let el = t.elapsed();
    { let mut s = log.st.lock().unwrap(); s.stop = true; log.cv_batch.notify_one(); }
    c.join().unwrap();
    let s = log.st.lock().unwrap();
    let n = writers * per_writer;
    println!("{:<40} writers={:<4} durable txn/s={:>8.0}  fsyncs={:<6} avg batch={:.1}  ({:.2}s)",
        "group commit, 128B txns, 1 shared log", writers, n as f64 / el.as_secs_f64(), s.batches, s.batched_txns as f64 / s.batches as f64, el.as_secs_f64());
    fs::remove_file(path).ok();
}

fn parallel_fsync(root: &std::path::Path, threads: usize, per: usize) {
    let barrier = Arc::new(Barrier::new(threads + 1));
    let hs: Vec<_> = (0..threads).map(|k| { let b = barrier.clone(); let p = root.join(format!("pf{k}.bin")); std::thread::spawn(move || {
        let mut f = OpenOptions::new().create(true).write(true).truncate(true).open(&p).unwrap();
        let buf = [0x33u8; 4096];
        b.wait();
        for _ in 0..per { f.write_all(&buf).unwrap(); f.sync_data().unwrap(); }
        fs::remove_file(p).ok();
    })}).collect();
    barrier.wait();
    let t = Instant::now();
    for h in hs { h.join().unwrap(); }
    let el = t.elapsed();
    println!("{:<40} threads={:<4} aggregate fsync/s={:>8.0}  per-thread={:.0}/s", "parallel fdatasync, separate files", threads, (threads * per) as f64 / el.as_secs_f64(), per as f64 / el.as_secs_f64());
}

fn main() {
    let root = std::path::PathBuf::from("data/nocow");
    println!("=== E02 durable write physics ===");
    for w in [1, 4, 16, 64, 256] { group_commit(&root.join("gc.log"), w, 2000); }
    for t in [1, 2, 4, 8, 16] { parallel_fsync(&root, t, 1000); }
}
