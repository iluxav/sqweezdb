// Bench: the same workloads through our Node addon and through node:sqlite (DatabaseSync, built into Node 24).
const { DatabaseSync } = require('node:sqlite');
const fs = require('fs'); const path = require('path');
const dblab = require('./dblab.node');
const N = 1_000_000; const ROOT = path.join(__dirname, '../../data/nocow');
const now = () => Number(process.hrtime.bigint()) / 1e3; // microseconds
console.log('=== Node bindings: ours (N-API addon) vs node:sqlite (DatabaseSync); 1M rows ===');
(async () => {
  const d = path.join(ROOT, 'nodebench'); fs.rmSync(d, { recursive: true, force: true });
  const db = new dblab.Database(d); const s = db.session();
  s.exec('CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, country INT, name VARCHAR(32))');
  const ins = db.prepare('INSERT INTO t VALUES (?,?,?,?)');
  let t = now();
  for (let i = 1; i <= N; i++) { if (i % 1000 === 1) s.exec('BEGIN'); s.execute(ins, [i, i % 1000, i % 20, 'u' + i]); if (i % 1000 === 0) s.exec('COMMIT'); }
  console.log(`ours    load 1M rows, 1000-row txns          ${((now() - t) / N).toFixed(2)} us/row`);
  const get = db.prepare('SELECT score FROM t WHERE id = ?');
  t = now(); let x = 0; for (let i = 0; i < 200_000; i++) x += s.execute(get, [(i * 7919) % N + 1])[0][0];
  console.log(`ours    point read by id, sync                ${((now() - t) / 200_000).toFixed(2)} us`);
  t = now(); let done = 0; const batch = async (g) => { const ss = db.session(); for (let i = 0; i < 100_000; i++) { await ss.executeAsync(get, [((i * 7919) + g * 1234567) % N + 1]); done++; } };
  await Promise.all([0,1,2,3,4,5,6,7].map(batch));
  console.log(`ours    point reads, 8 async streams          ${(0.8 / ((now() - t) / 1e6)).toFixed(2)}M/s (${done} reads; node:sqlite has no async path)`);
  t = now(); for (let i = 5_000_000; i < 5_050_000; i++) s.execute(ins, [i, 1.0, i % 20, 'u' + i]);
  console.log(`ours    single-row inserts, autocommit        ${((now() - t) / 50_000).toFixed(2)} us`);
  const rng = db.prepare('SELECT id, score FROM t WHERE id >= ? AND id <= ?');
  t = now(); for (let i = 0; i < 1000; i++) { const r = s.execute(rng, [i * 1000 + 1, i * 1000 + 1000]); if (r.length !== 1000) throw new Error(r.length); }
  console.log(`ours    fetch 1000 rows by id range            ${((now() - t) / 1000).toFixed(1)} us`);
  fs.rmSync(d, { recursive: true, force: true });
  // ---- node:sqlite ----
  const p = path.join(ROOT, 'nodebench.db'); for (const suf of ['', '-wal', '-shm']) fs.rmSync(p + suf, { force: true });
  const c = new DatabaseSync(p); c.exec('PRAGMA journal_mode=WAL'); c.exec('PRAGMA synchronous=NORMAL');
  c.exec('CREATE TABLE t(id INTEGER PRIMARY KEY, score REAL, country INTEGER, name TEXT)');
  const sins = c.prepare('INSERT INTO t VALUES (?,?,?,?)');
  t = now();
  for (let i = 1; i <= N; i++) { if (i % 1000 === 1) c.exec('BEGIN'); sins.run(i, i % 1000, i % 20, 'u' + i); if (i % 1000 === 0) c.exec('COMMIT'); }
  console.log(`sqlite  load 1M rows, 1000-row txns          ${((now() - t) / N).toFixed(2)} us/row`);
  const sget = c.prepare('SELECT score FROM t WHERE id = ?');
  t = now(); for (let i = 0; i < 200_000; i++) x += sget.get((i * 7919) % N + 1).score;
  console.log(`sqlite  point read by id, sync                ${((now() - t) / 200_000).toFixed(2)} us`);
  t = now(); for (let i = 5_000_000; i < 5_050_000; i++) sins.run(i, 1.0, i % 20, 'u' + i);
  console.log(`sqlite  single-row inserts, autocommit        ${((now() - t) / 50_000).toFixed(2)} us`);
  const srng = c.prepare('SELECT id, score FROM t WHERE id >= ? AND id <= ?');
  t = now(); for (let i = 0; i < 1000; i++) { const r = srng.all(i * 1000 + 1, i * 1000 + 1000); if (r.length !== 1000) throw new Error(r.length); }
  console.log(`sqlite  fetch 1000 rows by id range            ${((now() - t) / 1000).toFixed(1)} us`);
  c.close(); for (const suf of ['', '-wal', '-shm']) fs.rmSync(p + suf, { force: true });
})();
