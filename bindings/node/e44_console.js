// E44: the web console served from inside a Node app (listen.http). What it costs the app at idle and under console
// load (event-loop lag, the app's own query speed), how fast the console answers from a separate process, how fast a
// write made by the app shows up on a console live stream, and whether close() frees the port and the directory.
'use strict';
const { open } = require('./index.js');
const http = require('http'); const net = require('net'); const fs = require('fs'); const path = require('path');
const { spawn } = require('child_process');
const ROOT = path.join(__dirname, '../../data/nocow'); const DIR = path.join(ROOT, 'e44'); const ADDR = '127.0.0.1:7792'; const PORT = 7792; const TOKEN = 'e44-token';
const now = () => Number(process.hrtime.bigint()) / 1e3;
const pct = (a, p) => { const s = [...a].sort((x, y) => x - y); return s[Math.min(s.length - 1, Math.floor(s.length * p))]; };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const status = () => { const s = fs.readFileSync('/proc/self/status', 'utf8'); const g = (k) => Number((s.match(new RegExp(k + ':\\s+(\\d+)')) || [0, 0])[1]); return { rssMb: g('VmRSS') / 1024, threads: g('Threads') }; };

if (process.argv[2] === 'client') {
  // a separate process hammering the console: point reads over one keep-alive connection for `ms` milliseconds
  const agent = new http.Agent({ keepAlive: true, maxSockets: 1 }); const ms = Number(process.argv[3]); const lat = []; const t0 = Date.now();
  const one = () => new Promise((res, rej) => { const body = JSON.stringify({ sql: 'SELECT score FROM t WHERE id = ?', params: [1 + Math.floor(Math.random() * 100000)] }); const t = now();
    const r = http.request({ host: '127.0.0.1', port: PORT, path: '/api/sql', method: 'POST', agent, headers: { Authorization: 'Bearer ' + TOKEN, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) } }, (resp) => { let d = ''; resp.on('data', (c) => (d += c)); resp.on('end', () => { if (resp.statusCode !== 200) return rej(new Error(d)); lat.push(now() - t); res(); }); }); r.on('error', rej); r.end(body); });
  (async () => { while (Date.now() - t0 < ms) await one(); process.stdout.write(JSON.stringify({ n: lat.length, p50: pct(lat, 0.5), p99: pct(lat, 0.99) })); agent.destroy(); })().catch((e) => { console.error(e); process.exit(1); });
  return;
}

function loopLag(ms) { // event-loop lag in the app: how late a 1 ms timer fires, sampled for `ms`
  return new Promise((res) => { const lags = []; let last = now(); const t0 = Date.now(); const iv = setInterval(() => { const t = now(); lags.push(t - last - 1000); last = t; if (Date.now() - t0 >= ms) { clearInterval(iv); res({ p50: pct(lags, 0.5), p99: pct(lags, 0.99), max: Math.max(...lags) }); } }, 1); });
}
function appReads(db, ms) { // the app's own work: synchronous point reads in slices, yielding to the loop between slices
  return new Promise((res) => { const st = db.prepare('SELECT score FROM t WHERE id = ?'); let n = 0; const t0 = now(); const slice = () => { for (let i = 0; i < 2000; i++) { st.get(1 + ((n * 7919) % 100000)); n++; } if (now() - t0 < ms * 1000) setImmediate(slice); else res(n / ((now() - t0) / 1e6)); }; slice(); });
}
function runClient(ms) { return new Promise((res, rej) => { const c = spawn(process.execPath, [__filename, 'client', String(ms)], { stdio: ['ignore', 'pipe', 'inherit'] }); let out = ''; c.stdout.on('data', (d) => (out += d)); c.on('exit', (code) => code === 0 ? res(JSON.parse(out)) : rej(new Error('client failed'))); }); }
function portFree(port) { return new Promise((res) => { const s = net.createServer(); s.once('error', () => res(false)); s.listen(port, '127.0.0.1', () => s.close(() => res(true))); }); }
function sse(pathq) { // open a console event stream; resolves with an emitter-like object
  return new Promise((res, rej) => { const r = http.get({ host: '127.0.0.1', port: PORT, path: pathq + '&token=' + TOKEN }, (resp) => { if (resp.statusCode !== 200) return rej(new Error('status ' + resp.statusCode)); let buf = ''; const waiters = []; const events = [];
    resp.on('data', (c) => { buf += c; let i; while ((i = buf.indexOf('\n\n')) >= 0) { const chunk = buf.slice(0, i); buf = buf.slice(i + 2); const line = chunk.split('\n').find((l) => l.startsWith('data: ')); if (!line) continue; const ev = { at: now(), data: JSON.parse(line.slice(6)) }; if (waiters.length) waiters.shift()(ev); else events.push(ev); } });
    resp.on('end', () => { for (const w of waiters) w(null); });
    res({ next: () => (events.length ? Promise.resolve(events.shift()) : new Promise((r2) => waiters.push(r2))), ended: new Promise((r3) => resp.on('end', r3)), req: r }); }); r.on('error', rej); });
}

(async () => {
  console.log('=== E44: the web console inside a Node app (listen.http) ===');
  fs.rmSync(DIR, { recursive: true, force: true });
  // ---- idle cost: the same database without and with the console
  let db = open(DIR); db.exec('CREATE TABLE t(id INTEGER PRIMARY KEY, score DOUBLE, name VARCHAR(16))');
  const ins = db.prepare('INSERT INTO t VALUES (?,?,?)'); db.transaction(() => { for (let i = 1; i <= 100000; i++) ins.run(i, i * 1.5, 'u' + i); })();
  await sleep(300); const a = status(); db.close(); await sleep(300);
  db = open(DIR, { listen: { http: ADDR, token: TOKEN } }); await sleep(300); const b = status();
  console.log(`idle, 100k rows: without the console ${a.threads} threads ${a.rssMb.toFixed(1)} MB; with it ${b.threads} threads ${b.rssMb.toFixed(1)} MB`);
  // ---- the app under console load
  const lag0 = await loopLag(2000); const r0 = await appReads(db, 2);
  const clientP = runClient(4000); await sleep(300); const lag1 = await loopLag(2000); const r1 = await appReads(db, 1.5); const cl = await clientP;
  console.log(`event-loop lag (1 ms timer): no console traffic p50 ${lag0.p50.toFixed(0)} us p99 ${lag0.p99.toFixed(0)} us | console hammered from another process p50 ${lag1.p50.toFixed(0)} us p99 ${lag1.p99.toFixed(0)} us`);
  console.log(`the app's own point reads: ${(r0 / 1e6).toFixed(2)}M/s alone, ${(r1 / 1e6).toFixed(2)}M/s while the console serves ${cl.n} requests`);
  console.log(`console point read from another process (HTTP+JSON, keep-alive, Node client): p50 ${cl.p50.toFixed(0)} us, p99 ${cl.p99.toFixed(0)} us`);
  // ---- a write by the app -> the console's live stream
  const live = await sse('/api/live?sql=' + encodeURIComponent('SELECT count(*) AS n FROM t'));
  const first = await live.next(); if (!first.data.initial) throw new Error('no initial result');
  const lags = [];
  for (let i = 0; i < 100; i++) { const t = now(); db.run('INSERT INTO t VALUES (?,?,?)', [200000 + i, 1, 'x']); const ev = await live.next(); lags.push(ev.at - t); await sleep(10); }
  console.log(`app's db.run -> console live stream (count(*) over ${(100100).toLocaleString()} rows): p50 ${(pct(lags, 0.5) / 1000).toFixed(1)} ms, p99 ${(pct(lags, 0.99) / 1000).toFixed(1)} ms (5 ms debounce)`);
  const tail = await sse('/api/changes?sql=' + encodeURIComponent('SELECT * FROM t WHERE id > 0'));
  // ---- close with an open live stream, an open change tail and a keep-alive connection
  const keep = new http.Agent({ keepAlive: true }); await new Promise((res) => http.get({ host: '127.0.0.1', port: PORT, path: '/api/status', agent: keep, headers: { Authorization: 'Bearer ' + TOKEN } }, (r) => { r.resume(); r.on('end', res); }));
  let t = now(); db.close(); const closeMs = (now() - t) / 1000;
  t = now(); await Promise.race([Promise.all([live.ended, tail.ended]), sleep(5000)]); const endMs = (now() - t) / 1000;
  const free = await portFree(PORT);
  t = now(); let reopened; try { reopened = open(DIR, { durable: true, listen: { http: ADDR, token: TOKEN } }); } catch (e) { reopened = e; }
  const reopenMs = (now() - t) / 1000;
  console.log(`close() with a live stream, a change tail and a keep-alive connection open: ${closeMs.toFixed(1)} ms; both streams ended ${endMs.toFixed(0)} ms later; port free: ${free}; reopen of the same directory with new options and the same port: ${reopened instanceof Error ? 'FAILED ' + reopened.message : 'ok, durable=' + reopened.durable + ', ' + reopened.get('SELECT count(*) AS n FROM t').n + ' rows'} in ${reopenMs.toFixed(1)} ms`);
  keep.destroy(); if (!(reopened instanceof Error)) reopened.close();
  // ---- the console off: nothing listens
  db = open(DIR); console.log(`opened without listen.http: port ${PORT} free: ${await portFree(PORT)}`); db.close();
  fs.rmSync(DIR, { recursive: true, force: true });
})().catch((e) => { console.error('FAILED', e); process.exit(1); });
