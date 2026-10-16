# sqweezdb

An embedded SQL database in the SQLite mould, written in Rust, for the things SQLite cannot do:
many writers at once, enforced column types, row-level access control, live subscriptions, online
schema changes, and continuous backup to any S3-compatible bucket. Same deployment model: a library
in your process and a directory on disk. No server to run, unless you want one.

> **Status: experimental.** Every feature below exists and has been measured, but the file format
> is not frozen, the APIs will change, the SQL dialect is narrow, and nothing has been through a
> production incident. Treat it as a very fast prototype with a lab notebook attached.

## What you get

- **Many writers.** Transactions commit through one append-only log with group commit. On the lab
  machine, 256 concurrent writers sustain about 200,000 durable commits per second; SQLite serialises
  writers and tops out near 2,000 with a sync per commit.
- **Reads that scale.** Readers never take a lock. A point read costs about 0.4 µs in-process; 4
  threads read 21 million rows per second while writers run.
- **Enforced types.** `INT`, `DOUBLE`, `BOOL`, `TEXT(n)`, `BYTES(n)`, `TIMESTAMP`. A text value in an
  integer column is rejected, not stored.
- **Access control in the engine.** Roles, users, per-table grants and row policies compiled into
  every query and every subscription. A tenant cannot see another tenant's rows, or their changes.
- **Online schema changes.** `ALTER TABLE` adds, drops, renames or retypes a column while readers and
  writers keep running. Prepared statements re-plan themselves.
- **Foreign keys always on**, with `RESTRICT`, `CASCADE` and `SET NULL`.
- **Reactivity.** Subscribe to the changes any single-table `SELECT` would see, or watch any query and
  receive its new result when it changes. Events reach a Node callback about 15 µs after the commit.
- **Backup and replication from the log.** Point-in-time restore, follower replicas with 0.2 ms lag,
  and continuous backup to any S3-compatible bucket: compressed with zstd, encrypted under your
  passphrase, self-compacting, restorable to any point, with followers fed from the bucket alone.
- **Restart in a millisecond** regardless of size: the snapshot *is* the table image and is mapped,
  not loaded.
- **Thousands of databases in one process.** A tenant costs about 90 µs to create and around 100 KB
  resident; idle tenants are evicted and reopened on demand.
- **Bindings** for Node, Python, Go and C, plus the Rust crate itself.

## Quick start

### Node

```js
import { open } from 'sqweezdb'

const db = open('./data')   // creates or reopens; one engine per path per process

db.exec(`CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INT NOT NULL,
                             amount DOUBLE, placed_at TIMESTAMP)`)

const r = db.run('INSERT INTO orders VALUES (?,?,?,?)', [1, 42, 9.5, new Date()])
await r.durable                       // optional: resolves after the fsync

db.get('SELECT * FROM orders WHERE id = ?', [1])
// { id: 1, user_id: 42, amount: 9.5, placed_at: 2026-10-15T19:30:00.000Z }

const place = db.transaction((tx, userId, amount) => {
  const next = tx.get('SELECT max(id) AS m FROM orders').m + 1
  tx.run('INSERT INTO orders VALUES (?,?,?,?)', [next, userId, amount, new Date()])
  return next
})
place(42, 12)                         // write conflicts are retried inside

for await (const ev of db.subscribe('SELECT * FROM orders WHERE user_id = ?', [42])) {
  // ev = { type: 'change', op: 'insert', table: 'orders', key: 7, seq, row: {...} }
}

db.watch('SELECT user_id, sum(amount) AS total FROM orders GROUP BY user_id',
         [], (rows, info) => render(rows))

const alice = db.as('alice')          // same calls, alice's grants and row policies applied
```

Calls are synchronous, like better-sqlite3 and `node:sqlite`, because a read answers in about a
microsecond. Promises appear only where there is real waiting: `run().durable`, subscriptions, and
`stmt.allAsync()` for long scans. Rows are objects named after the columns; `INT` becomes a number or
a `BigInt` past 2^53, `BYTES` a `Buffer`, `TIMESTAMP` a `Date`. Errors are a `SqweezError` with a
`code`: `CONFLICT`, `DUPLICATE`, `FOREIGN_KEY`, `DENIED`, `NOT_FOUND`, `TYPE`, `SYNTAX`, `SCHEMA`.
The full surface is in `bindings/node/index.d.ts`.

The package is not on npm yet. To build it: see [Building](#building-from-source).

### Python

```python
import sqweezdb

db = sqweezdb.Database("./data")          # Database(path, durable=False)
s = db.session()
s.exec("CREATE TABLE t(id INTEGER PRIMARY KEY, name VARCHAR(32), at TIMESTAMP)")
ins = db.prepare("INSERT INTO t VALUES (?,?,?)")
s.execute(ins, (1, "alice", 1_700_000_000_000_000))   # TIMESTAMP as microseconds since the epoch
s.exec("SELECT * FROM t WHERE id = ?", (1,))          # [(1, 'alice', 1700000000000000)]

sub = db.session().subscribe("SELECT * FROM t WHERE id > ?", (0,))
ev = sub.next(1000)                                   # [op, table, key, columns...] or None on timeout
```

The interpreter lock is released while the engine works and while a subscription waits, so a
consumer thread can block on `next()` without stalling the rest of the program.

### Go

```go
import "github.com/iluxav/sqweezdb/bindings/go/sqweezdb"

db, err := sqweezdb.Open("./data", false)
s := db.Session()
s.ExecText("CREATE TABLE orders(id INTEGER PRIMARY KEY, user_id INT, amount DOUBLE)")
ins, _ := db.Prepare("INSERT INTO orders VALUES (?,?,?)")
s.Exec(ins, int64(1), int64(42), 9.5)
rows, _ := s.ExecText("SELECT * FROM orders WHERE user_id = ?", int64(42))   // [][]any

sub, _ := s.Subscribe("SELECT * FROM orders WHERE user_id = ?", int64(42))
ev, _ := sub.Next(200)   // []any{op, table, key, columns...}, or nil on timeout
```

`bindings/go/sse/main.go` is a complete example in the PocketBase shape: the Go program embeds the
database and serves `GET /events?user=N` as Server-Sent Events from a filtered subscription. The
binding uses cgo and links `libsqweezdb.so` from `target/release`.

### Rust

```rust
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::Value;

let sql = Sql::create(std::path::Path::new("./data"), false);   // Sql::open(path, durable, threads) to reopen
let mut s = Session::default();
sql.exec_s(&mut s, "CREATE TABLE t(id INTEGER PRIMARY KEY, name VARCHAR(32))", &[])?;
let ins = sql.prepare("INSERT INTO t VALUES (?,?)")?;
sql.execute_s(&mut s, &ins, &[Value::Int(1), Value::Text("alice".into())])?;
let rows = sql.exec_s(&mut s, "SELECT * FROM t WHERE id = ?", &[Value::Int(1)])?;
```

Other processes reach the owning process through `sqweezdb::net::Server` (a Unix socket, a
shared-memory ring for 0.6 µs reads, or TCP with a shared token) and `sqweezdb::net::Client`.
Backup to a bucket is `sqweezdb::cloud::{CloudConfig, start_uploader, download, start_puller}`;
followers are `Sql::follow`; many databases in one process are `sqweezdb::tenants::TenantHost`.
These are Rust-only today.

### C

`libsqweezdb.so` exports a small C ABI (`sqweezdb_open`, `sqweezdb_prepare`, `sqweezdb_exec`,
`sqweezdb_rows_all`, `sqweezdb_subscribe`, ...). The Go binding in `bindings/go/sqweezdb/sqweezdb.go`
doubles as its reference; the declarations are in its cgo preamble.

## SQL

Supported today:

- Tables with an `INTEGER PRIMARY KEY`, `NOT NULL`, `DEFAULT`, one `UNIQUE` column, `CREATE INDEX`
  on `INT` and `TIMESTAMP` columns, `FOREIGN KEY ... REFERENCES ... ON DELETE RESTRICT | CASCADE | SET NULL`.
- `SELECT` with `WHERE` (`=`, `<`, `>`, `<=`, `>=`, `AND`, `IN`, `NOT IN`, `IS NULL`, `BETWEEN`),
  `GROUP BY` one column with `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `ORDER BY` one column with `LIMIT`,
  equality `JOIN` across any number of tables, scalar, `IN`, `EXISTS` and correlated subqueries, aliases.
- `INSERT`, `UPDATE ... WHERE id = ?` (by primary key), `DELETE ... WHERE ...`,
  `BEGIN` / `COMMIT` / `ROLLBACK` with optimistic conflict detection.
- `ALTER TABLE ... ADD | DROP | RENAME COLUMN`, `ALTER COLUMN ... TYPE`, `SET | DROP NOT NULL`, all online.
- `CREATE ROLE`, `CREATE USER ... IN ROLE ... [WITH attr = value]`, `GRANT ... ON table TO role`,
  `CREATE POLICY name ON table FOR cmd TO role USING (col = SESSION.attr)`.
- `TIMESTAMP` literals as text: `'2026-10-15'`, `'2026-10-15 19:30:00'`, `'2026-10-15T19:30:00.123Z'`.

Not yet: `OR`, `LIKE`, `DISTINCT`, `HAVING`, arithmetic or functions inside expressions,
variable-length text (every text column has a maximum), multi-column indexes, indexes on text
columns, `UPDATE` by anything other than the primary key, `DROP TABLE`, `RIGHT` and `FULL` joins,
views, triggers. Reads inside an open transaction see committed data, not the transaction's own
writes. A table holds at most 16.7 million rows.

## How it works, briefly

One process owns a database directory. Every commit is appended to a checksummed log by a writer
thread; a second thread fsyncs, so writers batch behind one sync (group commit). Tables live in
memory as fixed-width typed rows, so a row access is tens of nanoseconds and readers need no locks.
A snapshot is written in the table's own in-memory layout and mapped privately at restart, which is
why restart takes a millisecond whatever the size. Everything that reads the log, follower replicas,
point-in-time restore, the cloud uploader and the change feed behind subscriptions, is the same
mechanism applied to the same bytes.

**Durability has two modes.** Opened with `durable = false`, a write returns when it is visible and
the fsync follows within about half a millisecond; the Node binding exposes that moment as the
`durable` promise, and a crash can lose only the last few hundred microseconds of acknowledged
writes. Opened with `durable = true`, every commit waits for its fsync, and many concurrent writers
still share one. A third mode waits for the record to be in the bucket before acknowledging.

Other processes do not open the files. They connect to the owner over a Unix socket, upgrade to a
shared-memory ring for 0.6 µs reads, or connect over TCP with a shared token. Network access is one
more listener on the same binary, never the only door.

## Numbers

All measured on one machine (Ryzen 5 9600X, NVMe, btrfs) against the system SQLite 3.53 and
Turso 0.8.3, with raw outputs under `results/`. They describe this machine, not yours.

| | SQLite | sqweezdb |
|---|---|---|
| Durable commits per second, 256 writers | about 2,000 | about 200,000 |
| Point read by primary key, in-process | 0.89 µs | 0.37 µs |
| Reads per second on 4 threads while writes run | 4.4M on 8 threads | 21M |
| GROUP BY over 1M rows, 20 groups | 121 ms | 2.5 ms |
| Restart with 1M rows | 0.5 ms | 1 ms |
| Insert then delete 3M rows one at a time | 614 s | 4.1 s |
| 1M rows in a backup bucket | 43 MB (Litestream) | 6 MB |
| Node: point read as an object | 1.38 µs (better-sqlite3) | 0.98 µs |
| Node: 1000 rows as objects | 111 µs (better-sqlite3) | 66 µs |
| Node: autocommit insert | 8.7 µs (better-sqlite3) | 1.07 µs |

Where SQLite still wins, measured on the same machine: bulk loading one row per call through the
language bindings (our per-call marshaling costs more), a cold restart by about half a millisecond,
metadata-only `ALTER TABLE` (SQLite rewrites nothing; we copy the table online), and lookups that
need only the key under a hard memory cap.

## Building from source

Requirements: Rust 1.98 or newer, a system `libsqlite3` with headers (the reference benchmarks link
it), and for the bindings Node 20+, Python 3.11+ and Go 1.24+.

```sh
cargo build --release                      # engine, C ABI (target/release/libsqweezdb.so), experiments

cd bindings/node && cargo build --release && cp target/release/libsqweezdb_node.so sqweezdb.node
cd bindings/python && cargo build --release && cp target/release/libsqweezdb_py.so sqweezdb.so
cd bindings/go && go build ./...           # links ../../target/release/libsqweezdb.so
```

The Node benchmark against better-sqlite3 needs `npm install --no-save better-sqlite3` in
`bindings/node`; it is then `node e41_api.js`.

## Repository layout

- `src/table.rs` storage, log, snapshots, recovery · `src/sql.rs` parser, planner, access control,
  change feed · `src/live.rs` subscriptions and live queries · `src/cloud.rs` S3 backup, compression,
  encryption · `src/tenants.rs` many databases per process · `src/net.rs` socket, ring and TCP
  server · `src/capi.rs` C ABI.
- `src/bin/e*.rs` the experiments, one per question asked during development; their raw outputs are
  in `results/`.
- `bindings/node`, `bindings/python`, `bindings/go` the language bindings.
- `data/` experiment working directories and `tools/` reference binaries (Litestream, rclone) are not
  tracked.

## Method

This project was built as a lab: every design decision was made by writing a benchmark, running it
against SQLite (and later Turso) on the same machine, and keeping whichever side the numbers favoured.
The experiments in `src/bin/` are that history, in order. The scoreboard of measured rounds stands at
39 for sqweezdb, 4 for SQLite, 5 ties.

## Name

Squeeze: megabytes of memory where SQLite needs hundreds, six megabytes in the bucket where a file
copy needs forty, thousands of writers through one log.

## License

Not chosen yet. All rights reserved until a license is added.
