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
- **Enforced types.** `INT`, `DOUBLE`, `BOOL`, `TEXT(n)`, `BYTES(n)`, `TIMESTAMP`, `UUID`. A text value in an
  integer column is rejected, not stored.
- **Automatic ids and UUIDs.** `id SERIAL PRIMARY KEY` (or `AUTOINCREMENT`, `GENERATED AS IDENTITY`) hands out
  1, 2, 3, … and never reuses a number, even after the highest row is deleted and the database restarts.
  `DEFAULT uuid()` gives time-ordered UUIDs (version 7), `gen_random_uuid()` random ones, `DEFAULT now()`
  the insert time. A UUID can be the primary key: `id UUID PRIMARY KEY DEFAULT uuid()`.
  `INSERT … RETURNING` and `lastInsertId` give the new values back.
- **Access control in the engine.** Roles, users, per-table grants and row policies compiled into
  every query and every subscription. A tenant cannot see another tenant's rows, or their changes.
- **Online schema changes.** `ALTER TABLE` adds, drops, renames or retypes a column while readers and
  writers keep running. The statement returns when the change is in place, or with an error and nothing
  changed (a value that does not convert to the new type, for example). Prepared statements re-plan themselves.
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

## Install

The `sqweezdb` command (server, shell, web console), prebuilt for Linux and macOS on x86_64 and arm64:

```sh
curl -fsSL https://raw.githubusercontent.com/iluxav/sqweezdb/main/install.sh | sh
```

It installs to `~/.local/bin` (or `/usr/local/bin` as root) after checking the download against the
release's `SHA256SUMS`. `SQWEEZDB_VERSION=v0.1.0` pins a release, `SQWEEZDB_INSTALL_DIR` picks the
directory, `sh install.sh --uninstall` removes it. On Linux it takes the glibc build (glibc 2.17 or
newer, as fast as a native build) and switches to the static musl build on Alpine; `SQWEEZDB_LIBC=musl`
forces it.

From a checkout:

```sh
make install            # builds target/dist/sqweezdb and installs it to ~/.local/bin (PREFIX=... to change)
make uninstall
```

The language bindings are built from source for now (see [Building](#building-from-source)).

## Quick start

### Node

```js
import { open } from 'sqweezdb'

const db = open('./data')   // creates or reopens; one engine per path per process; see Configuration for options

db.exec(`CREATE TABLE orders(id SERIAL PRIMARY KEY, public_id UUID UNIQUE DEFAULT uuid(),
                             user_id INT NOT NULL, amount DOUBLE, placed_at TIMESTAMP DEFAULT now())`)

const r = db.run('INSERT INTO orders (user_id, amount) VALUES (?, ?)', [42, 9.5])
r.lastInsertId                        // 1: the automatic id
await r.durable                       // optional: resolves after the fsync
db.get('INSERT INTO orders (user_id, amount) VALUES (?, ?) RETURNING id, public_id', [42, 12])
// { id: 2, public_id: '01a145c7-a2bb-7001-862a-a205073c0824' }

db.get('SELECT * FROM orders WHERE id = ?', [1])
// { id: 1, public_id: '01a145c7-…', user_id: 42, amount: 9.5, placed_at: 2026-10-15T19:30:00.000Z }

const place = db.transaction((tx, userId, amount) =>
  tx.run('INSERT INTO orders (user_id, amount) VALUES (?, ?)', [userId, amount]).lastInsertId)
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

The web console can run inside your Node app, served by the library itself on its own port. It is off
unless you ask for it:

```js
const db = open('./data', { listen: { http: '127.0.0.1:7780', token: process.env.DB_CONSOLE_TOKEN } })
// open http://127.0.0.1:7780/ in a browser and paste the token
```

It runs on the database's own threads, not on the event loop. Measured in E44: one extra thread and
0.1 MB at idle, no added event-loop lag and no slowdown of the app's own queries while another
process hammered the console. `db.close()` closes the console and its open streams and frees the port.

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
use sqweezdb::config::Config;
use sqweezdb::sql::{Session, Sql};
use sqweezdb::table::Value;

let sql = Sql::open_with(std::path::Path::new("./data"), Config::default())?;   // creates or reopens, starts the configured services
let mut s = Session::default();
sql.exec_s(&mut s, "CREATE TABLE t(id INTEGER PRIMARY KEY, name VARCHAR(32))", &[])?;
let ins = sql.prepare("INSERT INTO t VALUES (?,?)")?;
sql.execute_s(&mut s, &ins, &[Value::Int(1), Value::Text("alice".into())])?;
let rows = sql.exec_s(&mut s, "SELECT * FROM t WHERE id = ?", &[Value::Int(1)])?;
```

`Config` is the same set of keys the bindings take (below); `Config::load(dir, json)` merges a
`sqweezdb.toml` with options. Other processes reach the owner through `sqweezdb::net::Client` (a
Unix socket, a shared-memory ring for 0.6 µs reads, or TCP with a shared token). Many databases in
one process are `sqweezdb::tenants::TenantHost`, Rust-only today. `Sql::recreate` wipes a directory
first and exists for tests and experiments.

### C

`libsqweezdb.so` exports a small C ABI (`sqweezdb_open`, `sqweezdb_prepare`, `sqweezdb_exec`,
`sqweezdb_rows_all`, `sqweezdb_subscribe`, ...). The Go binding in `bindings/go/sqweezdb/sqweezdb.go`
doubles as its reference; the declarations are in its cgo preamble.

## The data directory

A database is a directory, and the directory's name is the database's name: `open('./data/orders')`
and `open('./data/users')` are two databases side by side. Everything inside belongs to that one
database, so copy, move, back up or delete it as a whole:

```
data/orders/
  sqweezdb.meta   marks the directory as a database and records its file-format version
  snap            the table image the next start maps into memory
  log.000…        the change log, in segments; old segments are deleted after a snapshot
  .lock           held while the database is open; a second owner is refused
```

It is several files on purpose: the log is appended and retired segment by segment, and a snapshot
is written beside the old one and swapped in with a rename, so space comes back without a VACUUM and
a crash mid-snapshot damages nothing.

Opening a path follows four rules, checked before anything is written:

- a directory with `sqweezdb.meta` opens, unless the marker comes from a newer, incompatible format;
- a missing or empty directory becomes a new database (with `create: false` it is refused); hidden
  files and `.toml` files do not count, so a configuration file can live there;
- a directory holding database files but no marker (made by 0.1.0) is adopted and gets its marker;
- anything else is refused with the error code `PATH`: a regular file, or a non-empty directory that
  is not a database. `open('.')` in a project root fails instead of scattering log files there.

## The `sqweezdb` command and the web console

```sh
sqweezdb serve ./data --ui 127.0.0.1:7780 --tcp 0.0.0.0:7781 --config /etc/sqweezdb/orders.toml
sqweezdb shell ./data                       # SQL in the terminal, in-process
sqweezdb shell db.internal:7781 --token …   # the same over TCP against a running server
```

`serve` owns a directory, starts whatever the configuration asks for (socket, TCP, backup, snapshot
policy) and, with `--ui`, a web console: tables and schema, a **+** button and an edit button on each
table that open a form to create or change a table (it shows the SQL it will run, and locks what the
engine cannot change after creation: the primary key, UNIQUE and foreign keys), a SQL console with a **Live** toggle that
turns any query into a result that updates as rows change, a change tail for any filter, the status
of the log, the snapshot policy and the bucket, users, roles, grants and row policies, and a snapshot
button. It is one HTML page compiled into the binary and a small JSON API, no framework and no build
step. Every API call needs the token; without `--token` one is generated and printed once. Bind the
console to loopback unless you mean otherwise; it runs as the application session and sees everything.

`Ctrl-C` stops cleanly: a final snapshot is written, so the next start maps it and answers at once.
A directory is held with a lock while open; a second process (or a second handle in the same process)
is refused instead of corrupting the log.

Measured in E43 on the lab machine: the command is 15 MB stripped and idles at 6 MB resident with the
console and TCP listening; a point read through the console's HTTP and JSON costs 7.9 µs against
5.4 µs over the binary frame protocol; a live query shows an insert about 8 ms later (the 5 ms
debounce plus delivery); stopping a 1M-row database took 0.26 s including its snapshot, and the
restart answered 54 ms after launch.

The same console can be served by an embedded database, from inside your application: set
`listen.http` (and `listen.token`) in the options of any binding. Nothing listens unless you do. In a
Node app it cost one idle thread and 0.1 MB and did not slow the app (E44). For the smallest library,
build without the `ui` cargo feature.

## Configuration

Every binding opens with the same keys. Options passed from code win over a TOML file the
application names with `configFile`, which wins over the defaults. The data directory itself is
never read for configuration: a directory someone else can write to must not be able to redirect
your backups, open a listener or read your environment. Strings in the file may reference
environment variables as `${NAME}`; an unset variable is an error, and nothing is read from the
environment unless the named file asks for it. Durations are `'5ms'`, `'1s'`, `'2m'`, `'1h'`, `'7d'`
or milliseconds; sizes are `'256MB'`, `'1GB'` or bytes; `0` turns a policy off. An unknown key is an
error, so typos do not silently disable anything.

```js
const db = open('./data', {
  configFile: '/etc/sqweezdb/orders.toml',   // optional: the same keys from a file you choose; these options win
  durable: false,            // false: writes return when visible, `durable` promises cover the fsync
                             // true: every commit waits for its fsync (concurrent writers share one)
                             // 'cloud': every commit waits until the bucket has it
  create: true,              // create the directory if it holds no database; never wipes one

  backup: {                  // continuous backup to any S3-compatible bucket
    endpoint: 'https://s3.eu-central-1.amazonaws.com', bucket: 'acme-prod', prefix: 'orders/',
    region: 'eu-central-1', accessKey: process.env.S3_KEY, secretKey: process.env.S3_SECRET,
    passphrase: process.env.DB_PASSPHRASE,   // AES-256-GCM; omit for plaintext (still zstd-compressed)
    interval: '1s', compress: true, compactAfter: '1m', compactEvery: '30s',
    bootstrap: true,         // empty directory + non-empty bucket: restore the latest state, then open
  },

  follow: { dir: '/srv/primary' },          // or { bucket: {...} }: open as a read-only replica
  snapshot: { logBytes: '64MB', idleAfter: '30s' },   // when the engine snapshots on its own (defaults)
  listen: { socket: '/run/sqweezdb/orders.sock', tcp: '0.0.0.0:7781', http: '127.0.0.1:7780', token: process.env.DB_TOKEN },
  live: { debounce: '5ms', queue: 65536 },  // live-query debounce, per-subscriber queue before overflow
  log: { threadsIdleAfter: '60s' },         // log threads leave when idle and return on the next write
})
```

The same document as a file, for operators, named from code with `configFile`:

```toml
# /etc/sqweezdb/orders.toml
durable = true

[backup]
endpoint  = "https://s3.eu-central-1.amazonaws.com"
bucket    = "acme-prod"
prefix    = "orders/"
accessKey = "${S3_KEY}"
secretKey = "${S3_SECRET}"
passphrase = "${DB_PASSPHRASE}"
bootstrap = true

[listen]
tcp   = "0.0.0.0:7781"
token = "${DB_TOKEN}"
```

Python takes the keys as keyword arguments (`sqweezdb.Database(path, durable=False, backup={...})`),
Go as `sqweezdb.OpenWith(path, sqweezdb.Options{...})`, C as a JSON string to `sqweezdb_open_with`.

What the policies do, measured in E42 on the lab machine:

- **Snapshots** write the table image the restart maps. The policy fires once the log since the
  last snapshot exceeds the larger of `logBytes` and that snapshot's own size, so a large table is
  not rewritten for every few megabytes of log, and after `idleAfter` without writes. Log replays at
  about 350 MB/s here, so the 64 MB floor bounds a restart at roughly 0.2 s plus one snapshot's
  worth; a quiet database restarts in a millisecond. The policy firing during a 1M-row insert burst
  cost a worst stall of 0.4 ms, and the idle snapshot shrank the log from 77 MB to nothing.
- **Backup** runs beside the writer: with uploads every half second, inserts cost 0.88 µs with or
  without it, and the bucket was 0.4 s behind the last write.
- **Bootstrap** of a new node from a 1M-row bucket (10.7 MB, encrypted) took 0.28 s from `open()` to
  the first answered query; the node then continued as the primary on the same prefix. An empty bucket
  opens an empty database.
- **Follow** turns a directory into a read-only replica of a primary on the same host or of a bucket;
  1,000 new rows were visible on the follower 38 ms after the primary committed them (20 ms poll).

## SQL

Supported today:

- Tables with an `INT` primary key (optionally automatic: `SERIAL`, `AUTOINCREMENT`, `GENERATED AS IDENTITY`)
  or a `UUID` primary key (usually `DEFAULT uuid()`), `NOT NULL`,
  `DEFAULT` (a constant, `uuid()`, `gen_random_uuid()`, `now()`), one `UNIQUE` column, `CREATE INDEX`
  on `INT`, `TIMESTAMP` and `UUID` columns, `FOREIGN KEY ... REFERENCES ... ON DELETE RESTRICT | CASCADE | SET NULL`
  (the column's type matches the parent's key: `INT` for an `INT` key, `UUID` for a `UUID` key).
- `SELECT` with `WHERE` (`=`, `<`, `>`, `<=`, `>=`, `AND`, `IN`, `NOT IN`, `IS NULL`, `BETWEEN`),
  `GROUP BY` one column with `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `ORDER BY` one column with `LIMIT`,
  equality `JOIN` across any number of tables, scalar, `IN`, `EXISTS` and correlated subqueries, aliases.
- `INSERT ... VALUES (...), (...)` (several rows, all or none), `DEFAULT` as a value, `NULL` for a generated key,
  `RETURNING` with column names or `*`. With a column list, the columns left out take their defaults
  (`INSERT INTO accounts (name) VALUES ('ilya')`); without one, every column needs a value, `DEFAULT` included
  (`INSERT INTO accounts VALUES (DEFAULT, 'ilya')`), as in SQLite and Postgres;
  `UPDATE ... WHERE id = ?` (by primary key), `DELETE ... WHERE ...`,
  `BEGIN` / `COMMIT` / `ROLLBACK` with optimistic conflict detection.
- `ALTER TABLE ... ADD | DROP | RENAME COLUMN`, `ALTER COLUMN ... TYPE`, `SET | DROP NOT NULL`, all online.
- `CREATE ROLE`, `CREATE USER ... IN ROLE ... [WITH attr = value]`, `GRANT ... ON table TO role`,
  `CREATE POLICY name ON table FOR cmd TO role USING (col = SESSION.attr)`.
- `TIMESTAMP` literals as text: `'2026-10-15'`, `'2026-10-15 19:30:00'`, `'2026-10-15T19:30:00.123Z'`.

Not yet: `OR`, `LIKE`, `DISTINCT`, `HAVING`, arithmetic or functions inside expressions,
variable-length text (every text column has a maximum), multi-column indexes, indexes on text
columns, `UPDATE` by anything other than the primary key, `INSERT ... SELECT`, `DROP TABLE`, `RIGHT`
and `FULL` joins, views, triggers. A correlated subquery joins on `INT`/`TIMESTAMP` or `UUID` columns,
and a correlated `IN (subquery)` not on `UUID` ones (use `EXISTS` or a join). Reads inside an open
transaction see committed data, not the transaction's own writes. A table holds at most 16.7 million
rows. Accepted but not enforced yet (to be refused until they are): a second `UNIQUE` column, `CHECK`,
and `TEXT`/`BYTES` wider than 65,535 bytes.

### INT or UUID keys

```sql
CREATE TABLE accounts (id UUID PRIMARY KEY DEFAULT uuid(), name VARCHAR(64));
CREATE TABLE orders (id UUID PRIMARY KEY DEFAULT uuid(),
                     account_id UUID NOT NULL REFERENCES accounts(id), amount DOUBLE);

INSERT INTO accounts (name) VALUES ('alice') RETURNING id;   -- '01a145fe-2334-7000-884d-285c8eaa5c58'
SELECT a.name, count(*) FROM orders o JOIN accounts a ON a.id = o.account_id GROUP BY a.name;
```

A UUID key is written and read as text. `lastInsertId` is that text, where an `INT` key gives a number.
`uuid()` makes version 7 UUIDs: random enough that nobody can guess the next one, and ordered by
creation time, so new rows go to the end of the key order like an increasing number does. They do
reveal when a row was created; `gen_random_uuid()` makes version 4 UUIDs, which reveal nothing.

A UUID key costs more than an `INT` key: 16 bytes instead of 8 in every row, index and foreign key,
and more time per operation. Measured in E48, with 1 million rows per table:

| | `INT` key | `UUID` key |
|---|---|---|
| Insert | 0.40 µs | 0.58 µs |
| Insert with a foreign key check | 0.67 µs | 1.07 µs |
| Point read by key | 0.27 µs | 0.50 µs |
| Join one row by key | 0.93 µs | 1.18 µs |
| Join all 1M orders to their users | 16 ms | 74 ms |
| Delete by key | 0.50 µs | 0.72 µs |
| Restart from snapshot | 90 ms | 87 ms |

Use `INT` when ids stay inside your system and speed matters most. Use `UUID` when ids are shown to
users, appear in URLs, or are created by clients before they reach the database. Either way, checking
who may read a row protects it; an id that is hard to guess does not.

## How it works, briefly

One process owns a database directory. Every commit is appended to a checksummed log by a writer
thread; a second thread fsyncs, so writers batch behind one sync (group commit). Tables live in
memory as fixed-width typed rows, so a row access is tens of nanoseconds and readers need no locks.
A snapshot is written in the table's own in-memory layout and mapped privately at restart, which is
why restart takes a millisecond whatever the size. Everything that reads the log, follower replicas,
point-in-time restore, the cloud uploader and the change feed behind subscriptions, is the same
mechanism applied to the same bytes.

**Durability has three modes.** Opened with `durable = false`, a write returns when it is visible and
the fsync follows within about half a millisecond; the Node binding exposes that moment as the
`durable` promise, and a crash can lose only the last few hundred microseconds of acknowledged
writes. Opened with `durable = true`, every commit waits for its fsync, and many concurrent writers
still share one. With `durable = 'cloud'`, a commit is acknowledged once its record is in the bucket.

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

Requirements: Rust 1.85 or newer (edition 2024), and for the bindings Node 20+, Python 3.11+ and Go 1.24+.

```sh
make                                       # the sqweezdb command: target/dist/sqweezdb
make test                                  # unit tests
make dist                                  # dist/sqweezdb-<target>.tar.gz + .sha256, as a release ships it
cargo build --release                      # engine and C ABI (target/release/libsqweezdb.so)

cd bindings/node && cargo build --release && cp target/release/libsqweezdb_node.so sqweezdb.node
cd bindings/python && cargo build --release && cp target/release/libsqweezdb_py.so sqweezdb.so
cd bindings/go && go build ./...           # links ../../target/release/libsqweezdb.so
```

The experiments that race SQLite and Turso need the `lab` feature and a system `libsqlite3` with
headers: `cargo lab e40_tcp` (an alias for `cargo run --release --features lab --bin`), or `make lab`
to build them all. The Node benchmark against better-sqlite3 needs `npm install --no-save
better-sqlite3` in `bindings/node`; it is then `node e41_api.js`.

**Releases.** Push a tag that matches the version in `Cargo.toml` (`v0.1.0` for `version = "0.1.0"`).
The release workflow builds the four Linux binaries (glibc 2.17 and static musl, x86_64 and arm64)
with `cargo zigbuild` and the two macOS binaries natively, checks the tag against the version, and
publishes a GitHub release with the tarballs, `SHA256SUMS` and `install.sh`. Running the workflow by
hand builds the artifacts without publishing.

## Repository layout

- `src/table.rs` storage, log, snapshots, recovery · `src/sql.rs` parser, planner, access control,
  change feed · `src/live.rs` subscriptions and live queries · `src/cloud.rs` S3 backup, compression,
  encryption · `src/tenants.rs` many databases per process · `src/net.rs` socket, ring and TCP
  server · `src/capi.rs` C ABI.
- `src/config.rs` the configuration keys, file loading and validation · `src/ui.rs` and `src/ui/index.html` the web console · `src/main.rs` the `sqweezdb` command.
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
