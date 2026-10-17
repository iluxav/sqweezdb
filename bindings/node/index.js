'use strict';
// sqweezdb for Node: the API developers see. Synchronous like better-sqlite3 and node:sqlite, because a read answers in
// well under a microsecond; promises only where there is real waiting (durability, subscriptions, long scans).
const native = require('./sqweezdb.node');

class SqweezError extends Error {
  constructor(code, message) { super(message); this.name = 'SqweezError'; this.code = code; }
}
function wrap(e) {
  if (e instanceof SqweezError) return e;
  const m = /^([A-Z_]+)\|([\s\S]*)$/.exec((e && e.message) || '');
  if (!m) return e;
  const err = new SqweezError(m[1], m[2]); if (e.stack) err.stack = err.stack.split('\n')[0] + '\n' + e.stack.split('\n').slice(1).join('\n'); return err;
}
const norm = (p) => (p === undefined ? [] : Array.isArray(p) ? p : [p]);
// a short randomized pause between transaction retries (microseconds), without yielding to the event loop
const PAUSE = new Int32Array(new SharedArrayBuffer(4));
const pause = (us) => { if (us >= 1) Atomics.wait(PAUSE, 0, 0, us / 1000); };
const flat = (args) => (args.length === 1 && Array.isArray(args[0]) ? args[0] : args);

// Decoder for the packed result frame the addon returns (see `pack` in src/lib.rs). One per statement: the row
// constructor is generated from the column names so every row has the same shape.
function decoder(names) {
  const nc = names.length;
  const mk = new Function('v', 'return {' + names.map((n, i) => JSON.stringify(n) + ':v[' + i + ']').join(',') + '}');
  return function decode(buf, first) {
    const n = buf.readUInt32LE(0); let off = 8; const out = first ? null : new Array(n); const v = new Array(nc);
    for (let r = 0; r < n; r++) {
      for (let c = 0; c < nc; c++) {
        const tag = buf[off++];
        if (tag === 1) { v[c] = buf.readDoubleLE(off); off += 8; }
        else if (tag === 3) { const len = buf.readUInt32LE(off); off += 4; v[c] = buf.toString('utf8', off, off + len); off += len; }
        else if (tag === 0) v[c] = null;
        else if (tag === 6) { v[c] = new Date(buf.readDoubleLE(off)); off += 8; }
        else if (tag === 5) v[c] = buf[off++] === 1;
        else if (tag === 2) { v[c] = buf.readBigInt64LE(off); off += 8; }
        else { const len = buf.readUInt32LE(off); off += 4; v[c] = Buffer.from(buf.subarray(off, off + len)); off += len; }
      }
      if (first) return mk(v); out[r] = mk(v);
    }
    return first ? null : out;
  };
}
class Entry {
  constructor(stmt) { this.stmt = stmt; this._dec = null; this._kind = -1; this._rows = 0; }
  get dec() { return this._dec || (this._dec = decoder(this.stmt.columns())); }
  get kind() { if (this._kind < 0) { this._kind = this.stmt.kind(); this._rows = this.stmt.rowsPerRun(); } return this._kind; }
  get rows() { this.kind; return this._rows; }
}

class RunResult {
  constructor(db, changes, seq, committed, lastInsertId) { this._db = db; this.changes = changes; this.seq = seq; this._committed = committed; if (lastInsertId !== undefined) this.lastInsertId = lastInsertId; }
  /** Resolves once this write's commit is fsynced (immediately if the database is durable or the write is inside an open transaction). */
  get durable() { return this._committed && !this._db.durable ? this._db._db.waitDurable(this.seq) : Promise.resolve(); }
}

class Statement {
  constructor(db, entry, sql) { this._db = db; this._e = entry; this.sql = sql; }
  get columns() { return this._e.stmt.columns(); }
  get(...args) { return this._db._get(this._e, flat(args)); }
  all(...args) { return this._db._all(this._e, flat(args)); }
  run(...args) { return this._db._run(this._e, flat(args)); }
  allAsync(...args) { return this._db._sess.allAsync(this._e.stmt, flat(args)).catch((e) => { throw wrap(e); }); }
}

class Subscription {
  constructor(start) {
    this._queue = []; this._waiters = []; this._closed = false; this._pulling = false;
    this._handlers = { change: [], live: [], overflow: [], close: [] };
    this._sub = start((ev) => this._push(ev));
  }
  _push(ev) {
    const hs = this._handlers[ev.type] || [];
    for (const h of hs) h(ev);
    if (this._waiters.length) this._waiters.shift()(ev); else if (hs.length === 0 || this._pulling) this._queue.push(ev);
    if (ev.type === 'overflow') this.close();
  }
  on(type, fn) { (this._handlers[type] || (this._handlers[type] = [])).push(fn); return this; }
  next() {
    this._pulling = true;
    if (this._queue.length) return Promise.resolve(this._queue.shift());
    if (this._closed) return Promise.resolve(null);
    return new Promise((r) => this._waiters.push(r));
  }
  unref() { this._sub.unref(); return this; }
  close() {
    if (this._closed) return; this._closed = true; this._sub.close();
    for (const w of this._waiters) w(null); this._waiters = [];
    for (const h of this._handlers.close) h();
  }
  [Symbol.asyncIterator]() {
    const self = this;
    return {
      async next() { const v = await self.next(); return v === null ? { done: true, value: undefined } : { done: false, value: v }; },
      async return() { self.close(); return { done: true, value: undefined }; },
    };
  }
}

class Database {
  constructor(handle, sess, info) {
    this._db = handle; this._sess = sess; this._stmts = info.stmts || new Map();
    this.path = info.path; this.durable = info.durable; this.user = info.user || null; this.identity = info.identity || null; this.retries = 0;
    /** How rows become objects: 'auto' (default: one row built in the addon, many rows as a packed frame decoded in JS), 'native' or 'packed'. */
    this.rowMode = info.rowMode || 'auto';
  }
  _prep(sql) {
    let e = this._stmts.get(sql);
    if (!e) { try { e = new Entry(this._db.prepare(sql)); } catch (err) { throw wrap(err); } if (this._stmts.size >= 2000) this._stmts.clear(); this._stmts.set(sql, e); }
    return e;
  }
  _get(e, p) { try { return this.rowMode === 'packed' ? e.dec(this._sess.packed(e.stmt, p), true) : this._sess.get(e.stmt, p); } catch (err) { throw wrap(err); } }
  _all(e, p) { try { return this.rowMode === 'native' ? this._sess.all(e.stmt, p) : e.dec(this._sess.packed(e.stmt, p), false); } catch (err) { throw wrap(err); } }
  _run(e, p) {
    let v;
    try { v = this._sess.run(e.stmt, p); }
    catch (err) { const w = wrap(err); if (w.code === 'NOT_FOUND') return new RunResult(this, 0, 0, false); throw w; }
    if (v < 0) { const committed = !this._sess.inTxn(); return new RunResult(this, -v - 1, committed ? this._sess.lastSeq() : 0, committed); }
    if (e.kind === 4) return new RunResult(this, e.rows, v, v > 0, this._sess.lastInsertId());
    return new RunResult(this, 1, v, v > 0);
  }
  /** Prepare once, run many times. */
  prepare(sql) { try { return new Statement(this, new Entry(this._db.prepare(sql)), sql); } catch (e) { throw wrap(e); } }
  /** Any statement, rows back as objects (statements are cached by text). */
  exec(sql, params) { try { return this._sess.exec(sql, norm(params)); } catch (e) { throw wrap(e); } }
  /** First row as an object, or null. */
  get(sql, params) { return this._get(this._prep(sql), norm(params)); }
  /** All rows as objects. */
  all(sql, params) { return this._all(this._prep(sql), norm(params)); }
  /** A write: returns { changes, seq, durable } where `durable` is a promise for the fsync. */
  run(sql, params) { return this._run(this._prep(sql), norm(params)); }
  /** Resolves once everything this database handle has committed so far is on disk. */
  flush() { const seq = this._sess.lastSeq(); return seq > 0 && !this.durable ? this._db.waitDurable(seq) : Promise.resolve(); }
  /**
   * Wrap `fn` in a transaction: `const place = db.transaction((tx, a, b) => {...}); place(1, 2)`.
   * Write conflicts with other writers are retried with a short randomized pause (default 200 times); a nested call joins the outer transaction.
   */
  transaction(fn, opts) {
    const db = this; const retries = (opts && opts.retries) || 200;
    const begin = db._prep('BEGIN'), commit = db._prep('COMMIT'), rollback = db._prep('ROLLBACK');
    return function (...args) {
      if (db._sess.inTxn()) return fn(db, ...args);
      for (let attempt = 0; ; attempt++) {
        db._run(begin, []);
        try { const out = fn(db, ...args); db._run(commit, []); return out; }
        catch (e) {
          try { db._sess.run(rollback.stmt, []); } catch (_) { /* already out of the transaction */ }
          const w = wrap(e);
          if (w.code === 'CONFLICT' && attempt < retries) { db.retries++; pause(Math.random() * Math.min(2000, 5 * 2 ** Math.min(attempt, 9))); continue; }
          throw w;
        }
      }
    };
  }
  /** Changes a single-table SELECT would see, as they commit: `for await (const ev of db.subscribe(...))` or `.on('change', fn)`. */
  subscribe(sql, params) { const p = norm(params); return new Subscription((cb) => { try { return this._sess.subscribe(sql, p, cb); } catch (e) { throw wrap(e); } }); }
  /** Keep any SELECT's result current: `fn(rows, info)` runs now and after every change (`info.added`, `info.removed`). */
  watch(sql, params, fn) {
    const p = norm(params); let sub;
    try { sub = this._sess.watch(sql, p, (ev) => { if (ev.type === 'live') fn(ev.rows, { added: ev.added, removed: ev.removed, initial: false }); else if (ev.type === 'overflow') fn(null, { overflow: true }); }); }
    catch (e) { throw wrap(e); }
    const rows = sub.initial(); fn(rows, { added: rows, removed: [], initial: true });
    return { close: () => sub.close(), unref: () => sub.unref() };
  }
  /**
   * The same database as someone else; their grants and row policies apply to every call, including subscriptions.
   * - `as('alice')`: a user created in the database (CREATE USER alice IN ROLE ... WITH tenant = 1).
   * - `as({ role: 'customer', tenant: 42 })`: an identity your app vouches for after its own login. `role` (or `roles`,
   *   an array) names existing roles; every other key is a session attribute that policies read as SESSION.<key>.
   *   Nothing is stored in the database. Cheap enough to call once per request.
   */
  as(who) {
    let sess, user = null, identity = null;
    if (typeof who === 'string') user = who;
    else if (who && typeof who === 'object' && !Array.isArray(who)) {
      const { role, roles, ...attrs } = who;
      const list = roles !== undefined ? roles : role !== undefined ? [role] : [];
      if (!Array.isArray(list) || list.some((r) => typeof r !== 'string')) throw new SqweezError('TYPE', "as(): 'role' is a role name and 'roles' an array of role names");
      identity = Object.freeze({ roles: Object.freeze([...list]), ...attrs });
    } else throw new SqweezError('TYPE', "as() takes a user name, or an identity such as { role: 'customer', tenant: 42 }");
    try {
      sess = this._db.session();
      if (user !== null) sess.login(user);
      else { const keys = Object.keys(identity).filter((k) => k !== 'roles'); sess.assume([...identity.roles], keys, keys.map((k) => identity[k])); }
    } catch (e) { throw wrap(e); }
    return new Database(this._db, sess, { path: this.path, durable: this.durable, user, identity, stmts: this._stmts, rowMode: this.rowMode });
  }
  /** Rows in all tables (from the engine's own count). */
  get rowCount() { return this._db.rows(); }
  close() { this._db.close(); }
}

/**
 * Open or create the database directory. Options are the engine's configuration keys:
 * durable (false | true | 'cloud'), create, backup { endpoint, bucket, prefix, region, accessKey, secretKey,
 * passphrase, interval, compress, compactAfter, compactEvery, bootstrap }, follow { dir | bucket, poll },
 * snapshot { logBytes, idleAfter }, listen { socket, tcp, http, token }, live { debounce, queue }, log { threadsIdleAfter },
 * configFile (a TOML file with the same keys, read first and overridden by these options), plus rowMode for this
 * binding. Nothing is read from the data directory itself. Never wipes a directory.
 */
function open(path, opts) {
  const { rowMode, ...engine } = opts || {};
  let handle, sess;
  try { handle = native.open(path, JSON.stringify(engine)); sess = handle.session(); } catch (e) { throw wrap(e); }
  return new Database(handle, sess, { path, durable: handle.isDurable(), rowMode });
}

module.exports = { open, Database, Statement, Subscription, RunResult, SqweezError };
