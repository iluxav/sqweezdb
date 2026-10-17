// Experiment 49: what an app-supplied identity costs per request (db.as({ role, ...attributes })).
// 100k orders across 1000 tenants, a row policy on tenant_id. Point reads by id, each from the reader's own tenant.
const { open } = require('./index.js');
const fs = require('fs');
const dir = process.argv[2] || '/tmp/sqweezdb-e49'; fs.rmSync(dir, { recursive: true, force: true });
const db = open(dir);
const N = 100_000, TENANTS = 1000, OPS = 200_000, ROUNDS = 5;
db.exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, tenant_id INT NOT NULL, amount DOUBLE)');
db.transaction(() => { for (let i = 1; i <= N; i++) db.run('INSERT INTO orders VALUES (?, ?, ?)', [i, i % TENANTS, i / 100]); })();
db.exec('CREATE ROLE customer'); db.exec('GRANT SELECT ON orders TO customer');
db.exec('CREATE POLICY own ON orders FOR SELECT TO customer USING (tenant_id = SESSION.tenant)');
db.exec('CREATE USER alice IN ROLE customer WITH tenant = 7');
const Q = 'SELECT id, tenant_id, amount FROM orders WHERE id = ?';
const ids = Array.from({ length: OPS }, (_, k) => 1 + ((k * 7919) % N));
let wrong = 0;
const check = (r, t) => { if (!r || r.tenant_id !== t) wrong++; };
function bench(label, fn) {
  const ts = [];
  for (let round = 0; round < ROUNDS; round++) { const t0 = process.hrtime.bigint(); for (let k = 0; k < OPS; k++) fn(k); ts.push(Number(process.hrtime.bigint() - t0) / OPS / 1000); }
  ts.sort((a, b) => a - b);
  console.log(`${label.padEnd(72)} ${ts[ROUNDS >> 1].toFixed(3)} us/op  (min ${ts[0].toFixed(3)}, max ${ts[ROUNDS - 1].toFixed(3)})`);
}
const t7 = db.as({ role: 'customer', tenant: 7 }), alice = db.as('alice');
const own7 = ids.map((id) => id - (id % TENANTS) + 7).map((id) => (id > N ? id - TENANTS : id));
console.log(`=== E49: app-supplied identities (Node, ${N} rows, ${TENANTS} tenants, ${OPS} ops x ${ROUNDS} rounds, medians) ===`);
bench('app handle, point read (no policy)', (k) => { db.get(Q, ids[k]); });
bench('identity handle reused, point read with the policy', (k) => { check(t7.get(Q, own7[k]), 7); });
bench('database user handle reused (as("alice")), same read', (k) => { check(alice.get(Q, own7[k]), 7); });
bench('new identity per request: db.as({ role, tenant }) + point read', (k) => { const t = ids[k] % TENANTS; check(db.as({ role: 'customer', tenant: t }).get(Q, ids[k]), t); });
bench('new database-user handle per request: db.as("alice") + point read', (k) => { check(db.as('alice').get(Q, own7[k]), 7); });
bench('db.as({ role, tenant }) alone', (k) => { db.as({ role: 'customer', tenant: k % TENANTS }); });
let hidden = 0; for (let k = 0; k < 20000; k++) { if (t7.get(Q, ids[k]) && ids[k] % TENANTS !== 7) hidden++; }
console.log(`checks: ${wrong} rows from the wrong tenant; ${hidden} rows of other tenants visible to tenant 7 (of 20000 tries)`);
db.close();
