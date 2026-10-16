// E41: the Node API shape (objects, Dates, BigInt, durable promises, transaction retry, push subscriptions, workers)
// measured against better-sqlite3 and node:sqlite (DatabaseSync, built into Node). 1M rows, same statements.
'use strict';
const { open } = require('./index.js');
const { DatabaseSync } = require('node:sqlite');
const Better = require('better-sqlite3');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const fs = require('fs'); const path = require('path');
const ROOT = path.join(__dirname, '../../data/nocow'); const N = 1_000_000;
const now = () => Number(process.hrtime.bigint()) / 1e3;
const pr = (label, n, us) => console.log(`${label.padEnd(78)} ${(us / n).toFixed(2).padStart(9)} us/op ${Math.round(n / us * 1e6).toString().padStart(10)} ops/s`);
const pct = (a, p) => { const s = [...a].sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.floor(s.length * p))]; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

if (!isMainThread) {
  // worker: increment one shared counter row in transactions; conflicts with the other workers are retried by the API
  const db = open(workerData.dir);
  const bump = db.transaction((tx) => { const r = tx.get('SELECT v FROM counter WHERE id = 1'); tx.run('UPDATE counter SET v = ? WHERE id = 1', [r.v + 1]); });
  const t = now(); for (let i = 0; i < workerData.n; i++) bump();
  parentPort.postMessage({ retries: db.retries, us: now() - t }); db.close();
  return;
}

(async () => {
  console.log('=== E41: Node API shape — ours vs better-sqlite3 vs node:sqlite (1M rows) ===');
  const sz = (f) => (fs.statSync(f).size / 1048576).toFixed(1) + ' MB';
  console.log(`addon size: ours ${sz(path.join(__dirname, 'sqweezdb.node'))}, better-sqlite3 ${sz(require.resolve('better-sqlite3/build/Release/better_sqlite3.node'))}`);
  const dir = path.join(ROOT, 'e41'); fs.rmSync(dir, { recursive: true, force: true });
  const db = open(dir);
  db.exec('CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, name VARCHAR(32))');
  const ins = db.prepare('INSERT INTO t VALUES (?,?,?,?)');
  const load = db.transaction((tx, from, to) => { for (let i = from; i < to; i++) ins.run(i, i % 1000, i % 20, 'u' + i); });
  let t = now(); for (let i = 1; i <= N; i += 1000) load(i, i + 1000);
  pr('ours    load 1M rows, transaction() of 1000', N, now() - t);
  let x = 0; const get = db.prepare('SELECT * FROM t WHERE id = ?'); const rng = db.prepare('SELECT id, score FROM t WHERE id >= ? AND id <= ?'); const wide = db.prepare('SELECT * FROM t WHERE id >= ? AND id <= ?');
  for (const mode of ['native', 'packed']) {
    db.rowMode = mode; const tag = mode === 'native' ? 'objects built in Rust' : 'packed frame, objects built in JS';
    t = now(); for (let i = 0; i < 200_000; i++) x += db.get('SELECT score FROM t WHERE id = ?', (i * 7919) % N + 1).score;
    pr(`ours    db.get(sql, id) -> object, 1 column (${tag})`, 200_000, now() - t);
    t = now(); for (let i = 0; i < 200_000; i++) x += get.get((i * 7919) % N + 1).score;
    pr(`ours    stmt.get(id) -> object, 4 columns (${tag})`, 200_000, now() - t);
    t = now(); for (let i = 0; i < 1000; i++) { const r = rng.all(i * 1000 + 1, i * 1000 + 1000); if (r.length !== 1000) throw new Error(r.length); }
    pr(`ours    stmt.all(range) -> 1000 objects, 2 columns (${tag})`, 1000, now() - t);
    t = now(); for (let i = 0; i < 1000; i++) { const r = wide.all(i * 1000 + 1, i * 1000 + 1000); if (r.length !== 1000) throw new Error(r.length); }
    pr(`ours    stmt.all(range) -> 1000 objects, 4 columns incl. text (${tag})`, 1000, now() - t);
  }
  t = now(); for (let i = 5_000_000; i < 5_050_000; i++) db.run('INSERT INTO t VALUES (?,?,?,?)', [i, 1.0, i % 20, 'u' + i]);
  pr('ours    db.run(insert), autocommit, returns when visible', 50_000, now() - t);
  t = now(); for (let i = 6_000_000; i < 6_002_000; i++) await db.run('INSERT INTO t VALUES (?,?,?,?)', [i, 1.0, i % 20, 'u' + i]).durable;
  pr('ours    await db.run(insert).durable (fsync per insert)', 2000, now() - t);
  t = now(); const pend = []; for (let i = 6_100_000; i < 6_110_000; i++) pend.push(db.run('INSERT INTO t VALUES (?,?,?,?)', [i, 1.0, i % 20, 'u' + i]).durable); await Promise.all(pend);
  pr('ours    10k inserts, then await all their durable promises (group commit)', 10_000, now() - t);
  const upd = db.prepare('UPDATE t SET score = ? WHERE id = ?');
  t = now(); for (let i = 0; i < 50_000; i++) upd.run(i, (i * 7919) % N + 1);
  pr('ours    stmt.run(update by id), autocommit', 50_000, now() - t);
  // BigInt and Date columns
  db.exec('CREATE TABLE big(id INTEGER PRIMARY KEY, v INT)'); db.exec('CREATE TABLE ev(id INTEGER PRIMARY KEY, at TIMESTAMP)');
  const base = 2n ** 60n; const insBig = db.prepare('INSERT INTO big VALUES (?,?)'); const insEv = db.prepare('INSERT INTO ev VALUES (?,?)');
  const fill = db.transaction((tx) => { for (let i = 1; i <= 10_000; i++) { insBig.run(i, base + BigInt(i)); insEv.run(i, new Date(1_700_000_000_000 + i * 1000)); } }); fill();
  const gb = db.prepare('SELECT v FROM big WHERE id = ?'); t = now(); let ok = 0; for (let i = 0; i < 100_000; i++) { const v = gb.get(i % 10_000 + 1).v; if (typeof v === 'bigint' && v === base + BigInt(i % 10_000 + 1)) ok++; }
  pr(`ours    stmt.get -> BigInt column (${ok === 100_000 ? 'all exact' : 'MISMATCH ' + ok})`, 100_000, now() - t);
  const ge = db.prepare('SELECT at FROM ev WHERE id = ?'); t = now(); ok = 0; for (let i = 0; i < 100_000; i++) { const d = ge.get(i % 10_000 + 1).at; if (d instanceof Date && d.getTime() === 1_700_000_000_000 + (i % 10_000 + 1) * 1000) ok++; }
  pr(`ours    stmt.get -> Date column (${ok === 100_000 ? 'all exact' : 'MISMATCH ' + ok})`, 100_000, now() - t);
  const since = db.all('SELECT count(*) AS n FROM ev WHERE at >= ?', [new Date(1_700_000_000_000 + 9_001 * 1000)])[0].n;
  console.log(`ours    WHERE at >= Date -> ${since} rows (expect 1000); text literal ${db.get("SELECT count(*) AS n FROM ev WHERE at < '2023-11-14T22:13:20Z'").n} rows (expect 0, boundary)`);

  // subscriptions: push through a threadsafe function (E39 polled through the thread pool: p50 87 us)
  db.exec('CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INT, amount DOUBLE, ts INT)');
  const io = db.prepare('INSERT INTO orders VALUES (?,?,?,?)');
  for (let trial = 0; trial < 3; trial++) {
    const lags = []; const sub = db.subscribe('SELECT * FROM orders WHERE user_id = ?', [42]).on('change', (ev) => lags.push(now() - ev.row.ts));
    await sleep(20); const b0 = trial * 100_000;
    for (let i = 0; i < 1000; i++) { for (let k = 0; k < 9; k++) io.run(1_000_000 + b0 + i * 10 + k, 1000 + k, 1.0, 0); io.run(b0 + i + 1, 42, 9.5, Math.round(now())); await new Promise((r) => setImmediate(r)); }
    await sleep(100); sub.close();
    console.log(`ours    subscribe() trial ${trial + 1}: ${lags.length} of 1000 events for user 42 (9,000 others filtered); insert -> callback p50 ${pct(lags, 0.5).toFixed(0)} us, p99 ${pct(lags, 0.99).toFixed(0)} us`);
  }
  let live = 0, lastLive = 0; const w = db.watch('SELECT user_id, sum(amount) AS total FROM orders GROUP BY user_id', [], (rows, info) => { if (!info.initial) { live++; lastLive = now(); } });
  const t0 = now(); for (let i = 0; i < 1000; i++) io.run(2_000_000 + i, 42, 1.0, 0); const tEnd = now(); await sleep(200); w.close();
  console.log(`ours    watch(GROUP BY): 1000 inserts in ${((tEnd - t0) / 1000).toFixed(1)} ms -> ${live} result updates (5 ms debounce), last update ${((lastLive - tEnd) / 1000).toFixed(1)} ms after the last insert; final total ${db.get('SELECT sum(amount) AS s FROM orders WHERE user_id = 42').s}`);

  // worker threads: one engine per process, transaction() retries conflicts
  db.exec('CREATE TABLE counter(id INTEGER PRIMARY KEY, v INT)'); db.run('INSERT INTO counter VALUES (1, 0)');
  for (const workers of [1, 4]) {
    db.run('UPDATE counter SET v = 0 WHERE id = 1'); const per = 2000;
    t = now(); const res = await Promise.all(Array.from({ length: workers }, () => new Promise((res, rej) => { const wk = new Worker(__filename, { workerData: { dir, n: per } }); wk.once('message', res); wk.once('error', rej); })));
    const el = now() - t; const v = db.get('SELECT v FROM counter WHERE id = 1').v; const retries = res.reduce((a, r) => a + r.retries, 0);
    console.log(`ours    ${workers} worker thread(s) x ${per} read-then-update transactions on one row: final ${v} (expect ${workers * per}), ${retries} retried, ${(el / 1000).toFixed(0)} ms, ${Math.round(workers * per / el * 1e6)} txn/s`);
  }
  db.close(); fs.rmSync(dir, { recursive: true, force: true });

  // ---- better-sqlite3 ----
  const bp = path.join(ROOT, 'e41b.db'); for (const s of ['', '-wal', '-shm']) fs.rmSync(bp + s, { force: true });
  const b = new Better(bp); b.pragma('journal_mode = WAL'); b.pragma('synchronous = NORMAL');
  b.exec('CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL, country INTEGER, name TEXT)');
  const bins = b.prepare('INSERT INTO t VALUES (?,?,?,?)'); const bload = b.transaction((from, to) => { for (let i = from; i < to; i++) bins.run(i, i % 1000, i % 20, 'u' + i); });
  t = now(); for (let i = 1; i <= N; i += 1000) bload(i, i + 1000); pr('better  load 1M rows, transaction() of 1000', N, now() - t);
  t = now(); for (let i = 0; i < 200_000; i++) x += b.prepare('SELECT score FROM t WHERE id = ?').get((i * 7919) % N + 1).score; pr('better  db.prepare(sql).get(id) -> object (statement cache is the app\'s job)', 200_000, now() - t);
  const bget = b.prepare('SELECT * FROM t WHERE id = ?'); t = now(); for (let i = 0; i < 200_000; i++) x += bget.get((i * 7919) % N + 1).score; pr('better  stmt.get(id) -> object, 4 columns', 200_000, now() - t);
  const brng = b.prepare('SELECT id, score FROM t WHERE id >= ? AND id <= ?'); t = now(); for (let i = 0; i < 1000; i++) { const r = brng.all(i * 1000 + 1, i * 1000 + 1000); if (r.length !== 1000) throw new Error(r.length); } pr('better  stmt.all(range) -> 1000 objects, 2 columns', 1000, now() - t);
  const bwide = b.prepare('SELECT * FROM t WHERE id >= ? AND id <= ?'); t = now(); for (let i = 0; i < 1000; i++) { const r = bwide.all(i * 1000 + 1, i * 1000 + 1000); if (r.length !== 1000) throw new Error(r.length); } pr('better  stmt.all(range) -> 1000 objects, 4 columns incl. text', 1000, now() - t);
  t = now(); for (let i = 5_000_000; i < 5_050_000; i++) bins.run(i, 1.0, i % 20, 'u' + i); pr('better  stmt.run(insert), autocommit, WAL synchronous=NORMAL', 50_000, now() - t);
  b.pragma('synchronous = FULL'); t = now(); for (let i = 6_000_000; i < 6_002_000; i++) bins.run(i, 1.0, i % 20, 'u' + i); pr('better  stmt.run(insert), autocommit, synchronous=FULL (fsync per insert)', 2000, now() - t); b.pragma('synchronous = NORMAL');
  const bupd = b.prepare('UPDATE t SET score = ? WHERE id = ?'); t = now(); for (let i = 0; i < 50_000; i++) bupd.run(i, (i * 7919) % N + 1); pr('better  stmt.run(update by id), autocommit', 50_000, now() - t);
  b.exec('CREATE TABLE big(id INTEGER PRIMARY KEY, v INTEGER)'); const bib = b.prepare('INSERT INTO big VALUES (?,?)'); b.transaction(() => { for (let i = 1; i <= 10_000; i++) bib.run(i, base + BigInt(i)); })();
  const bgb = b.prepare('SELECT v FROM big WHERE id = ?').safeIntegers(true); t = now(); ok = 0; for (let i = 0; i < 100_000; i++) { const v = bgb.get(i % 10_000 + 1).v; if (typeof v === 'bigint' && v === base + BigInt(i % 10_000 + 1)) ok++; } pr(`better  stmt.get -> BigInt column via safeIntegers (${ok === 100_000 ? 'all exact' : 'MISMATCH ' + ok})`, 100_000, now() - t);
  b.exec('CREATE TABLE ev(id INTEGER PRIMARY KEY, at INTEGER)'); const bie = b.prepare('INSERT INTO ev VALUES (?,?)'); b.transaction(() => { for (let i = 1; i <= 10_000; i++) bie.run(i, 1_700_000_000_000 + i * 1000); })();
  const bge = b.prepare('SELECT at FROM ev WHERE id = ?'); t = now(); ok = 0; for (let i = 0; i < 100_000; i++) { const d = new Date(bge.get(i % 10_000 + 1).at); if (d.getTime() === 1_700_000_000_000 + (i % 10_000 + 1) * 1000) ok++; } pr(`better  stmt.get -> integer ms + new Date() in the app (${ok === 100_000 ? 'all exact' : 'MISMATCH'})`, 100_000, now() - t);
  console.log('better  subscriptions / live queries: not available (no change notification API; polling or triggers)');
  b.close(); for (const s of ['', '-wal', '-shm']) fs.rmSync(bp + s, { force: true });

  // ---- node:sqlite ----
  const np = path.join(ROOT, 'e41n.db'); for (const s of ['', '-wal', '-shm']) fs.rmSync(np + s, { force: true });
  const c = new DatabaseSync(np); c.exec('PRAGMA journal_mode=WAL'); c.exec('PRAGMA synchronous=NORMAL');
  c.exec('CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL, country INTEGER, name TEXT)');
  const cins = c.prepare('INSERT INTO t VALUES (?,?,?,?)');
  t = now(); for (let i = 1; i <= N; i++) { if (i % 1000 === 1) c.exec('BEGIN'); cins.run(i, i % 1000, i % 20, 'u' + i); if (i % 1000 === 0) c.exec('COMMIT'); } pr('node:sqlite load 1M rows, BEGIN/COMMIT every 1000', N, now() - t);
  const cget = c.prepare('SELECT * FROM t WHERE id = ?'); t = now(); for (let i = 0; i < 200_000; i++) x += cget.get((i * 7919) % N + 1).score; pr('node:sqlite stmt.get(id) -> object, 4 columns', 200_000, now() - t);
  const crng = c.prepare('SELECT id, score FROM t WHERE id >= ? AND id <= ?'); t = now(); for (let i = 0; i < 1000; i++) { const r = crng.all(i * 1000 + 1, i * 1000 + 1000); if (r.length !== 1000) throw new Error(r.length); } pr('node:sqlite stmt.all(range) -> 1000 objects, 2 columns', 1000, now() - t);
  const cwide = c.prepare('SELECT * FROM t WHERE id >= ? AND id <= ?'); t = now(); for (let i = 0; i < 1000; i++) { const r = cwide.all(i * 1000 + 1, i * 1000 + 1000); if (r.length !== 1000) throw new Error(r.length); } pr('node:sqlite stmt.all(range) -> 1000 objects, 4 columns incl. text', 1000, now() - t);
  t = now(); for (let i = 5_000_000; i < 5_050_000; i++) cins.run(i, 1.0, i % 20, 'u' + i); pr('node:sqlite stmt.run(insert), autocommit, WAL synchronous=NORMAL', 50_000, now() - t);
  c.exec('PRAGMA synchronous=FULL'); t = now(); for (let i = 6_000_000; i < 6_002_000; i++) cins.run(i, 1.0, i % 20, 'u' + i); pr('node:sqlite stmt.run(insert), autocommit, synchronous=FULL (fsync per insert)', 2000, now() - t);
  c.close(); for (const s of ['', '-wal', '-shm']) fs.rmSync(np + s, { force: true });
  console.log(`(checksum ${x.toFixed(0)})`);
})().catch((e) => { console.error('FAILED', e); process.exit(1); });
