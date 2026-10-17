<div align="center">

# sqweezdb

**An embedded SQL database for applications that outgrow SQLite.**<br>
Concurrent writers, strict types, row-level security, live queries and continuous backup,<br>
in a library inside your process and a directory on disk.

[![CI](https://github.com/iluxav/sqweezdb/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/iluxav/sqweezdb/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/iluxav/sqweezdb?sort=semver)](https://github.com/iluxav/sqweezdb/releases/latest)
[![Rust 1.85+](https://img.shields.io/badge/rust-1.85%2B-orange?logo=rust)](#building-from-source)
[![Platforms](https://img.shields.io/badge/platforms-Linux%20%7C%20macOS-blue)](#installation)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Status: experimental](https://img.shields.io/badge/status-experimental-yellow)](#project-status)

[Features](#features) · [Installation](#installation) · [Quick start](#quick-start) · [Guides](#guides) · [Configuration](#configuration) · [SQL](#sql-reference) · [Performance](#performance) · [Building](#building-from-source)

</div>

---

sqweezdb keeps SQLite's deployment model: no server to run, one directory per database, opened by the
library in your process. It is written in Rust and adds what SQLite leaves to the application: many
writers at once, enforced column types, access control inside the engine, subscriptions to changes,
schema changes without downtime, and backup to any S3-compatible bucket.

> [!WARNING]
> **Project status: experimental.** Every feature below exists and is covered by tests and benchmarks,
> but the file format is not frozen, the APIs may change between releases, and the SQL dialect is
> deliberately narrow. Do not use it for data you cannot afford to lose yet.

## Features

| Capability | What you get |
|---|---|
| **Concurrent writers** | Commits share one append-only log with group commit. 256 writers sustain about 200,000 durable commits per second, where SQLite serializes writers at about 2,000. |
| **Lock-free reads** | Readers never take a lock. A point read costs about 0.4 µs in-process, and 4 threads read 21 million rows per second while writers run. |
| **Strict types** | `INT`, `DOUBLE`, `BOOL`, `TEXT(n)`, `BYTES(n)`, `TIMESTAMP`, `UUID`. A value of the wrong type is rejected, never stored. |
| **Keys and ids** | Automatic integer ids that are never reused, UUID primary keys (time-ordered v7 or random v4), `DEFAULT now()`, and `INSERT … RETURNING`. |
| **Row-level security** | Roles, grants and row policies enforced on every query, write and subscription. Your app passes who is asking on each request. |
| **Live queries** | Subscribe to the changes a query would see, or watch a query and receive its new result whenever it changes. |
| **Online schema changes** | `ALTER TABLE` adds, drops, renames or retypes a column while reads and writes continue. |
| **Foreign keys** | Always enforced, with `RESTRICT`, `CASCADE` and `SET NULL`. |
| **Backup and replication** | Continuous backup to S3-compatible storage, compressed and encrypted; point-in-time restore; read-only followers. |
| **Fast restart** | The snapshot is the table image and is memory-mapped, not loaded. After a snapshot, a million-row table reopens in under a millisecond; each secondary index adds about 30 ms per million rows. |
| **Tooling** | A `sqweezdb` command with a server, a SQL shell and a web console, plus bindings for Node, Python, Go, C and Rust. |

## Installation

### Command-line tool

Prebuilt for Linux and macOS on x86_64 and arm64:

```sh
curl -fsSL https://raw.githubusercontent.com/iluxav/sqweezdb/main/install.sh | sh
```

The installer verifies the download against the release's `SHA256SUMS` and installs to `~/.local/bin`,
or `/usr/local/bin` when run as root. On Linux it uses the glibc build, which needs glibc 2.17 or newer,
and switches to the static musl build on Alpine.

| Variable | Effect |
|---|---|
| `SQWEEZDB_VERSION=v0.1.0` | Install a specific release instead of the latest. |
| `SQWEEZDB_INSTALL_DIR=/opt/bin` | Install somewhere else. |
| `SQWEEZDB_LIBC=musl` | Force the static musl build on Linux. |

To remove it, run `sh install.sh --uninstall`. From a checkout, `make install` and `make uninstall`
do the same with a local build.

### Libraries

The packages are not published to npm, PyPI or crates.io yet. Build the binding you need from source,
as described in [Building from source](#building-from-source).

| Language | Binding | Location |
|---|---|---|
| Node.js 20+ | Native addon (N-API), synchronous calls with promises where work waits | `bindings/node` |
| Python 3.11+ | Extension module (PyO3), releases the GIL while the engine works | `bindings/python` |
| Go 1.24+ | cgo package over the C ABI | `bindings/go/sqweezdb` |
| C | Shared library `libsqweezdb.so` | `src/capi.rs` |
| Rust | The `sqweezdb` crate | `src/` |

## Quick start

### Node.js

```js
import { open } from 'sqweezdb'

const db = open('./data/shop')   // creates the database, or reopens it

db.exec(`CREATE TABLE orders (
  id         SERIAL PRIMARY KEY,
  public_id  UUID UNIQUE DEFAULT uuid(),
  user_id    INT NOT NULL,
  amount     DOUBLE,
  placed_at  TIMESTAMP DEFAULT now()
)`)

const r = db.run('INSERT INTO orders (user_id, amount) VALUES (?, ?)', [42, 9.5])
r.lastInsertId       // 1
await r.durable      // optional: resolves once the write is on disk

db.get('INSERT INTO orders (user_id, amount) VALUES (?, ?) RETURNING id, public_id', [42, 12])
// { id: 2, public_id: '01a145c7-a2bb-7001-862a-a205073c0824' }

db.get('SELECT * FROM orders WHERE id = ?', [1])
// { id: 1, public_id: '01a145c7-…', user_id: 42, amount: 9.5, placed_at: 2026-10-15T19:30:00.000Z }

// transactions retry automatically on write conflicts
const place = db.transaction((tx, userId, amount) =>
  tx.run('INSERT INTO orders (user_id, amount) VALUES (?, ?)', [userId, amount]).lastInsertId)
place(42, 12)

// a query result that updates itself
db.watch('SELECT user_id, sum(amount) AS total FROM orders GROUP BY user_id', [],
  (rows) => render(rows))
```

Calls are synchronous, like better-sqlite3 and `node:sqlite`, because a read answers in about a
microsecond. Promises appear only where something waits: `run().durable`, subscriptions, and
`stmt.allAsync()` for long scans. Rows are plain objects. `INT` becomes a number, or a `BigInt` beyond
2^53; `BYTES` becomes a `Buffer` and `TIMESTAMP` a `Date`. The full API is typed in
[`bindings/node/index.d.ts`](bindings/node/index.d.ts).

<details>
<summary><b>Python</b></summary>

```python
import sqweezdb

db = sqweezdb.Database("./data/shop")                 # Database(path, durable=False, **options)
s = db.session()
s.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name VARCHAR(32), at TIMESTAMP)")
ins = db.prepare("INSERT INTO t VALUES (?, ?, ?)")
s.execute(ins, (1, "alice", 1_700_000_000_000_000))   # TIMESTAMP as microseconds since the epoch
s.exec("SELECT * FROM t WHERE id = ?", (1,))          # [(1, 'alice', 1700000000000000)]

sub = db.session().subscribe("SELECT * FROM t WHERE id > ?", (0,))
ev = sub.next(1000)                                   # [op, table, key, columns...] or None on timeout
```

The interpreter lock is released while the engine works and while a subscription waits, so a consumer
thread can block on `next()` without stalling the rest of the program.

</details>

<details>
<summary><b>Go</b></summary>

```go
import "github.com/iluxav/sqweezdb/bindings/go/sqweezdb"

db, err := sqweezdb.Open("./data/shop", false)
s := db.Session()
s.ExecText("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INT, amount DOUBLE)")
ins, _ := db.Prepare("INSERT INTO orders VALUES (?, ?, ?)")
s.Exec(ins, int64(1), int64(42), 9.5)
rows, _ := s.ExecText("SELECT * FROM orders WHERE user_id = ?", int64(42))   // [][]any

sub, _ := s.Subscribe("SELECT * FROM orders WHERE user_id = ?", int64(42))
ev, _ := sub.Next(200)   // []any{op, table, key, columns...}, or nil on timeout
```

[`bindings/go/sse/main.go`](bindings/go/sse/main.go) is a complete example: a Go server that embeds
the database and streams a filtered subscription to browsers as Server-Sent Events.

</details>

<details>
<summary><b>Rust</b></summary>

```rust
use sqweezdb::config::Config;
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::Value;

let sql = Sql::open_with(std::path::Path::new("./data/shop"), Config::default())?;
let mut s = Session::default();
sql.exec_s(&mut s, "CREATE TABLE t (id INTEGER PRIMARY KEY, name VARCHAR(32))", &[])?;
let ins = sql.prepare("INSERT INTO t VALUES (?, ?)")?;
sql.execute_s(&mut s, &ins, &[Value::Int(1), Value::Text("alice".into())])?;
let rows = sql.exec_s(&mut s, "SELECT * FROM t WHERE id = ?", &[Value::Int(1)])?;
```

Other processes reach the owning process through `sqweezdb::net::Client`, over a Unix socket, a
shared-memory ring or TCP. `sqweezdb::tenants::TenantHost` runs thousands of databases in one process,
at about 90 µs to create one and about 100 KB each; it is Rust-only for now.

</details>

<details>
<summary><b>C</b></summary>

`libsqweezdb.so` exports a small C ABI: `sqweezdb_open_with`, `sqweezdb_prepare`, `sqweezdb_exec`,
`sqweezdb_rows_all`, `sqweezdb_subscribe`, `sqweezdb_assume` and more. The declarations live in the
cgo preamble of [`bindings/go/sqweezdb/sqweezdb.go`](bindings/go/sqweezdb/sqweezdb.go), which also
serves as the reference for using them.

</details>

## Guides

### Databases are directories

A database is a directory, and its name is the database's name. `open('./data/orders')` and
`open('./data/users')` are two databases side by side. Copy, move, back up or delete the directory as
a whole.

```
data/orders/
  sqweezdb.meta   marks the directory as a database and records its format version
  snap            the table image, memory-mapped at startup
  log.000…        the change log, in segments; old segments are removed after a snapshot
  .lock           held while the database is open
```

Opening a path follows four rules, checked before anything is written:

- A directory with `sqweezdb.meta` opens, unless it was written by a newer, incompatible format.
- A missing or empty directory becomes a new database, unless `create: false` is set. Hidden files and
  `.toml` files don't count, so a configuration file can live there.
- A directory with database files but no marker, as written by 0.1.0, is adopted and gets a marker.
- Anything else is refused with the error code `PATH`, such as a regular file or a non-empty directory
  that isn't a database.

One process owns a database at a time. A second process, or a second handle in the same process, is
refused instead of corrupting the log. Other processes connect to the owner; see
[Configuration](#configuration) for `listen`.

### Primary keys: INT or UUID

```sql
CREATE TABLE accounts (id UUID PRIMARY KEY DEFAULT uuid(), name VARCHAR(64));
CREATE TABLE orders   (id UUID PRIMARY KEY DEFAULT uuid(),
                       account_id UUID NOT NULL REFERENCES accounts(id), amount DOUBLE);

INSERT INTO accounts (name) VALUES ('alice') RETURNING id;   -- '01a145fe-2334-7000-884d-285c8eaa5c58'
```

An integer key can be automatic with `SERIAL`, `AUTOINCREMENT` or `GENERATED AS IDENTITY`. Ids are never
reused, even after the highest row is deleted and the database restarts. A UUID key is read and written
as text, and `lastInsertId` returns that text. `uuid()` makes version 7 UUIDs, which can't be guessed and
sort by creation time; they do reveal when a row was created. `gen_random_uuid()` makes version 4 UUIDs,
which reveal nothing.

Without a column list, `INSERT` needs a value for every column; write `DEFAULT` for a generated one.
With a column list, the columns left out take their defaults.

A UUID key stores 16 bytes instead of 8 and costs more per operation. With 1 million rows per table:

| Operation | `INT` key | `UUID` key |
|---|---|---|
| Insert | 0.40 µs | 0.58 µs |
| Insert with a foreign key check | 0.67 µs | 1.07 µs |
| Point read by key | 0.27 µs | 0.50 µs |
| Join one row by key | 0.93 µs | 1.18 µs |
| Join all 1M orders to their users | 16 ms | 74 ms |
| Delete by key | 0.50 µs | 0.72 µs |
| Restart from snapshot | 90 ms | 87 ms |

Use `INT` when ids stay inside your system and speed matters most. Use `UUID` when ids are shown to
users, appear in URLs, or are created by clients. Either way, an access check protects a row; an id
that is hard to guess does not.

### Transactions and durability

`BEGIN`, `COMMIT` and `ROLLBACK` use optimistic concurrency: transactions run without locks, and a
commit that conflicts with a concurrent one fails with `CONFLICT`. `db.transaction(fn)` in Node retries
those automatically. Reads inside an open transaction see committed data, not the transaction's own
writes.

| `durable` | A commit returns when | A crash can lose |
|---|---|---|
| `false` (default) | it is visible to readers; the fsync follows within about half a millisecond | the last few hundred microseconds of acknowledged writes |
| `true` | it is fsynced; concurrent writers share one fsync | nothing acknowledged |
| `'cloud'` | its log record is in the backup bucket | nothing acknowledged |

With `durable: false`, Node's `run().durable` promise resolves at the moment a specific write is on disk.

### Live queries and subscriptions

```js
// every change a query would see, as it commits
for await (const ev of db.subscribe('SELECT * FROM orders WHERE user_id = ?', [42])) {
  // { type: 'change', op: 'insert', table: 'orders', key: 7, seq, row: { … } }
}

// the full result again whenever it changes, with what was added and removed
const w = db.watch('SELECT * FROM orders WHERE user_id = ?', [42], (rows, { added, removed }) => update(rows))
w.close()
```

`subscribe` takes a single-table query. `watch` takes any query and re-runs it when a table it reads
changes, debounced by `live.debounce`. Events reach a Node callback about 15 µs after the commit.
Subscriptions respect access control: each one sees only the rows its session may see. A subscriber
that falls behind by more than `live.queue` events receives an overflow notice and is dropped, so a
slow consumer never slows the writers.

### Access control

The rules live in the database: roles, what each role may do to each table, and which rows it may
touch. Who is asking comes from your application, which has already checked the login.

```sql
CREATE ROLE customer;
GRANT SELECT, INSERT ON orders TO customer;
CREATE POLICY own_orders ON orders FOR ALL TO customer USING (tenant_id = SESSION.tenant);
```

```js
app.get('/orders', (req, res) => {
  const tenantDb = db.as({ role: 'customer', tenant: req.user.tenantId })   // after your login check
  res.json(tenantDb.all('SELECT * FROM orders'))                           // only this tenant's rows
})
```

```python
s = db.session(); s.assume("customer", tenant=42)
```

```go
s := db.Session(); s.Assume([]string{"customer"}, map[string]any{"tenant": 42})
```

`role`, or `roles` as a list, names existing roles. Every other key is a session attribute that
policies read as `SESSION.<key>`: a number, a string, a boolean, or in Node a `BigInt` or a `Date`.
Nothing about the user is stored in the database.

- Reads, writes, subscriptions and live queries are all filtered. A write outside the policy fails with
  `DENIED`, and asking for another tenant's row by its id returns nothing.
- Grants from all of a session's roles add up, and every policy of those roles applies together.
- A missing attribute is an error, not an empty result. So is an unknown role, or an attribute that is
  `undefined`.
- An identity can never change the schema, roles, grants or policies. Only the application's own handle can.
- Creating an identity costs about 1.4 µs in Node, and a policy adds about 0.1 µs to a point read, so
  one identity per request is fine.

Users can also be stored in the database with `CREATE USER alice IN ROLE customer WITH tenant = 7`, and
used with `db.as('alice')`. A client of the TCP listener selects such a user by name after presenting
the listener's token. Per-user passwords are not built yet, and a network client can never supply its
own attributes.

### Backup, restore and replication

Everything is built on the change log, so backups never pause the database.

- **Continuous backup** to any S3-compatible bucket with the `backup` option. Objects are compressed
  with zstd and, with a `passphrase`, encrypted with AES-256-GCM before upload. Small objects are
  compacted in the bucket automatically. 1 million rows take about 6 MB in the bucket.
- **Bootstrap.** With `backup.bootstrap: true`, opening an empty directory restores the latest state
  from the bucket first; a 1M-row bucket was restored and answering queries in 0.28 s.
- **Followers.** The `follow` option opens a read-only replica of another directory on the same host,
  or of a bucket. With a 20 ms `poll`, new rows reached the follower about 38 ms after the primary
  committed them.
- **Point-in-time restore** to any log position is available from the Rust API
  (`Sql::backup` and `Sql::restore`).

### Web console and command line

```sh
sqweezdb serve ./data/shop --ui 127.0.0.1:7780 --tcp 0.0.0.0:7781   # own a database, serve it
sqweezdb shell ./data/shop                                          # SQL in the terminal
sqweezdb shell db.internal:7781 --token …                           # the same against a running server
```

`serve` owns a directory and starts what you ask for: a Unix socket, TCP, backup, the snapshot policy,
and with `--ui` a web console. Every console request needs the token; without `--token`, one is
generated and printed once. `Ctrl-C` stops cleanly, writing a final snapshot so the next start is
instant.

The console has a table browser, a form to create and alter tables, a SQL editor with a **Live** toggle
that keeps results current, a feed of changes, the status of the log, snapshots and backup, and the
roles, grants and policies. It is one HTML page compiled into the binary, with no build step. Bind it
to loopback unless you intend otherwise: it acts as the application and sees everything.

An embedded database can serve the same console from inside your application:

```js
const db = open('./data/shop', { listen: { http: '127.0.0.1:7780', token: process.env.DB_CONSOLE_TOKEN } })
```

It runs on the database's own threads, not on the event loop, and costs about 0.1 MB when idle.
`db.close()` stops it and frees the port.

## Configuration

Every binding accepts the same options. Values passed in code override a TOML file named by
`configFile`, which overrides the defaults. The data directory itself is never read for configuration,
so a directory others can write to cannot redirect your backups or open a listener. Unknown keys are
errors, so a typo never silently disables anything.

```js
const db = open('./data/orders', {
  configFile: '/etc/sqweezdb/orders.toml',
  durable: true,
  backup: {
    endpoint: 'https://s3.eu-central-1.amazonaws.com', bucket: 'acme-prod', prefix: 'orders/',
    region: 'eu-central-1', accessKey: process.env.S3_KEY, secretKey: process.env.S3_SECRET,
    passphrase: process.env.DB_PASSPHRASE,
  },
  listen: { socket: '/run/sqweezdb/orders.sock', http: '127.0.0.1:7780', token: process.env.DB_TOKEN },
})
```

Python takes the same keys as keyword arguments, `sqweezdb.Database(path, backup={...})`. Go takes
`sqweezdb.OpenWith(path, sqweezdb.Options{...})`, and C takes a JSON string in `sqweezdb_open_with`.

| Key | Default | Description |
|---|---|---|
| `configFile` | none | A TOML file with these same keys. Code only; a file cannot name another. |
| `durable` | `false` | `false`, `true` or `'cloud'`. See [durability](#transactions-and-durability). |
| `create` | `true` | Create the database if the directory holds none. Never wipes one. |
| `recoverThreads` | `4` | Threads used to replay the log at startup. |
| `backup.endpoint`, `bucket`, `accessKey`, `secretKey` | required | The S3-compatible bucket to back up to. |
| `backup.prefix` / `region` | `''` / `us-east-1` | Where in the bucket, and the bucket's region. |
| `backup.passphrase` | none | Encrypt with AES-256-GCM before upload. Without it, data is compressed only. |
| `backup.interval` | `1s` | How often new log records are uploaded. |
| `backup.compress` | `true` | Compress uploads with zstd. |
| `backup.compactAfter` / `compactEvery` | `1m` / `30s` | When small objects are merged in the bucket. |
| `backup.bootstrap` | `false` | Restore from the bucket when opening an empty directory. |
| `follow.dir` or `follow.bucket` | none | Open as a read-only replica of a directory or a bucket. |
| `follow.poll` | `100ms` | How often a follower checks for new records. |
| `snapshot.logBytes` | `64MB` | Snapshot once the log since the last snapshot exceeds this size, or that snapshot's own size if larger. |
| `snapshot.idleAfter` | `30s` | Snapshot after this long without writes. |
| `listen.socket` / `tcp` / `http` | none | A Unix socket, a TCP listener, and the web console. |
| `listen.token` | none | The shared secret for TCP and the console; required when either is on. The `sqweezdb` command generates one if you don't pass `--token`. |
| `live.debounce` | `5ms` | How long a live query waits to batch changes before re-running. |
| `live.queue` | `65536` | Events a subscriber may fall behind before it is dropped. |
| `log.threadsIdleAfter` | off | Let log threads exit when idle; for many idle databases in one process. |

Durations are written as `'5ms'`, `'1s'`, `'2m'`, `'1h'`, `'7d'` or a number of milliseconds. Sizes are
`'256MB'`, `'1GB'` or a number of bytes. `0` turns a snapshot policy off.

The same settings as a file, for operators. Strings may reference environment variables as `${NAME}`;
an unset variable is an error.

```toml
# /etc/sqweezdb/orders.toml
durable = true

[backup]
endpoint   = "https://s3.eu-central-1.amazonaws.com"
bucket     = "acme-prod"
prefix     = "orders/"
accessKey  = "${S3_KEY}"
secretKey  = "${S3_SECRET}"
passphrase = "${DB_PASSPHRASE}"
bootstrap  = true

[listen]
tcp   = "0.0.0.0:7781"
token = "${DB_TOKEN}"
```

## SQL reference

**Tables and constraints**

- `CREATE TABLE` with an `INT` or `UUID` primary key, `NOT NULL`, `DEFAULT` (a constant, `uuid()`,
  `gen_random_uuid()` or `now()`), one `UNIQUE` column, and
  `FOREIGN KEY … REFERENCES … ON DELETE RESTRICT | CASCADE | SET NULL`.
- `CREATE INDEX` on `INT`, `TIMESTAMP` and `UUID` columns.
- `ALTER TABLE … ADD | DROP | RENAME COLUMN`, `ALTER COLUMN … TYPE`, `SET | DROP NOT NULL`, all online.

**Queries**

- `SELECT` with `WHERE` (`=`, `<>`, `<`, `>`, `<=`, `>=`, `AND`, `IN`, `NOT IN`, `IS NULL`, `BETWEEN`),
  `GROUP BY` one column with `COUNT`, `SUM`, `AVG`, `MIN` and `MAX`, `ORDER BY` one column with `LIMIT`,
  and aliases.
- Equality `JOIN` across any number of tables, and scalar, `IN`, `EXISTS` and correlated subqueries.

**Writes**

- `INSERT … VALUES` with several rows (all or none), `DEFAULT` as a value, and `RETURNING`.
- `UPDATE … WHERE id = ?` by primary key, and `DELETE … WHERE …`.
- `BEGIN`, `COMMIT`, `ROLLBACK`.

**Access control**

- `CREATE ROLE`, `CREATE USER … IN ROLE … [WITH attr = value]`, `GRANT … ON table TO role`, and
  `CREATE POLICY name ON table FOR cmd TO role USING (col = SESSION.attr [AND …])`.

**Values**

- `TIMESTAMP` literals as text: `'2026-10-15'`, `'2026-10-15 19:30:00'`, `'2026-10-15T19:30:00.123Z'`.
- `UUID` literals as text, with or without hyphens.

**Not supported yet:** `OR`, `LIKE`, `DISTINCT`, `HAVING`, arithmetic and functions inside expressions,
variable-length text (every text column has a maximum), multi-column indexes, indexes on text columns,
`UPDATE` by anything other than the primary key, `INSERT … SELECT`, `DROP TABLE`, `RIGHT` and `FULL`
joins, views and triggers. A correlated `IN (subquery)` can't be keyed on a UUID column; use `EXISTS` or
a join. A table holds at most 16.7 million rows.

> [!NOTE]
> These are accepted today but not enforced, and will be refused until they are: a second `UNIQUE`
> column, `CHECK` constraints, and `TEXT` or `BYTES` wider than 65,535 bytes.

### Error codes

Every binding reports errors with a stable code. In Node it is `err.code` on a `SqweezError`.

| Code | Meaning |
|---|---|
| `SYNTAX` | The statement can't be parsed or uses something unsupported. |
| `SCHEMA` | No such table, column or role. |
| `TYPE` | A value doesn't fit its column: the wrong type, too long, or NULL in a `NOT NULL` column. |
| `DUPLICATE` | A primary key or `UNIQUE` value already exists. |
| `FOREIGN_KEY` | A referenced row is missing, or a delete is blocked by children. |
| `DENIED` | The session lacks a grant, or a row falls outside its policy. |
| `CONFLICT` | A concurrent transaction changed the same rows; retry. |
| `NOT_FOUND` | An `UPDATE` by primary key found no row. |
| `TRANSACTION` | `BEGIN` inside a transaction, or `COMMIT` outside one. |
| `ARGUMENTS` | Too few parameters for the statement. |
| `PATH` | The path is a file, or a directory that isn't a database. |
| `CONFIG` | An invalid or unknown option. |
| `ERROR` | Anything else. |

## Performance

Measured on one machine (AMD Ryzen 5 9600X, NVMe, btrfs) against the system SQLite 3.53. The numbers
describe that machine, not yours. Raw outputs are in [`results/`](results/), and every benchmark is a
program in [`src/bin/`](src/bin/) you can run yourself.

| Workload | SQLite | sqweezdb |
|---|---|---|
| Durable commits per second, 256 writers | about 2,000 | about 200,000 |
| Point read by primary key, in-process | 0.89 µs | 0.37 µs |
| Reads per second while writes run | 4.4M on 8 threads | 21M on 4 threads |
| `GROUP BY` over 1M rows, 20 groups | 121 ms | 2.5 ms |
| Restart with 1M rows | 0.5 ms | 1 ms |
| Insert then delete 3M rows, one at a time | 614 s | 4.1 s |
| 1M rows in a backup bucket | 43 MB (Litestream) | 6 MB |
| Node: point read as an object | 1.38 µs (better-sqlite3) | 0.98 µs |
| Node: 1,000 rows as objects | 111 µs (better-sqlite3) | 66 µs |
| Node: autocommit insert | 8.7 µs (better-sqlite3) | 1.07 µs |

SQLite is still faster at bulk loading one row per call through a language binding, at a cold restart
by about half a millisecond, at `ALTER TABLE` changes that only touch metadata, and at key-only lookups
under a hard memory cap.

## How it works

One process owns a database directory. Every commit is appended to a checksummed log by a writer
thread, and a second thread fsyncs, so concurrent writers share one sync. Tables live in memory as
fixed-width typed rows, so readers need no locks and a row access takes tens of nanoseconds. A snapshot
is written in the tables' own in-memory layout and memory-mapped at startup, so a table reopens in under
a millisecond at any size. Secondary indexes are rebuilt when the database opens, and anything written
since the last snapshot is replayed from the log at about 350 MB/s. The snapshot policy keeps that
replay short: by default it snapshots after 64 MB of log, or the last snapshot's size if larger, and
after 30 seconds without writes. Followers, point-in-time restore, cloud backup and subscriptions are
all readers of that same log.

Other processes never open the files. They connect to the owner over a Unix socket, upgrade to a
shared-memory ring for reads of about 0.6 µs, or connect over TCP with a token.

## Building from source

Requirements: Rust 1.85 or newer, and for the bindings Node 20+, Python 3.11+ or Go 1.24+.

```sh
make                 # the sqweezdb command, in target/dist/sqweezdb
make test            # the test suite
make install         # build and install the command to ~/.local/bin (PREFIX=… to change)
make node            # bindings/node/sqweezdb.node
make python          # bindings/python/sqweezdb.so
make go              # the C library, then the Go package
make dist            # a release tarball and its checksum
```

`cargo build --release` builds the engine and the C library, `target/release/libsqweezdb.so`. The web
console is the default `ui` feature; build with `--no-default-features` for the smallest library.

## Contributing

- `src/table.rs`: storage, log, snapshots and recovery
- `src/sql.rs`: parser, planner, executor, access control and the change feed
- `src/live.rs`: subscriptions and live queries
- `src/cloud.rs`: S3 backup, compression and encryption
- `src/net.rs`: the socket, shared-memory ring and TCP server
- `src/config.rs`: options, configuration files and validation
- `src/ui.rs`, `src/ui/index.html`: the web console
- `src/main.rs`: the `sqweezdb` command
- `src/capi.rs`: the C ABI
- `src/tenants.rs`: many databases in one process
- `bindings/`: Node, Python and Go

Design decisions are made with benchmarks. The programs in `src/bin/` race sqweezdb against SQLite and
Turso on the same machine, one question each; their outputs are in `results/`. They need the `lab`
feature and a system `libsqlite3` with headers: `cargo lab e40_tcp` runs one, and `make lab` builds
them all.

Releases are tags that match the version in `Cargo.toml`, such as `v0.1.0`. The release workflow builds
Linux binaries for glibc and musl on x86_64 and arm64 plus macOS binaries, and publishes them with
`SHA256SUMS` and `install.sh`.

## Project status

sqweezdb is experimental and pre-1.0. Expect breaking changes to the file format and the APIs before
1.0, and keep backups of anything that matters.

## License

sqweezdb is released under the [MIT License](LICENSE).
