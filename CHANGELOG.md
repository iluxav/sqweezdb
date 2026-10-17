# Changelog

## 0.2.0 — 2026-10-17

### Added
- **UUID primary keys.** `id UUID PRIMARY KEY DEFAULT uuid()`, with foreign keys, joins, indexes, the change feed
  and live queries on UUID columns. `lastInsertId` is the UUID text for such tables.
- **Automatic ids and generated values.** `SERIAL`, `AUTOINCREMENT` and `GENERATED AS IDENTITY` hand out ids
  that are never reused; `DEFAULT uuid()`, `gen_random_uuid()` and `now()`; `INSERT … RETURNING`;
  `INSERT` with several rows, all or none.
- **Identities supplied by the application** for row-level security: `db.as({ role, ...attributes })` in Node,
  `session.assume(...)` in Python, `Session.Assume(...)` in Go, `sqweezdb_assume` in C. Nothing about the user
  is stored in the database.
- **Database directories are marked** with `sqweezdb.meta`; opening a file or a foreign directory is refused
  with the error code `PATH`. Directories made by 0.1.0 are adopted.
- **A redesigned web console**: a graphite theme, a sign-in screen, SQL highlighting and line numbers,
  query history, typed result columns with click-to-copy cells, live results that highlight changed rows,
  a readable change feed, a table designer, and an Access tab.
- **Backup health** in the console: whether uploads work, how far the bucket is behind, the last error,
  and a connection test; when backup is off, a generator for the settings file and the restart command.
- **The MIT license.**

### Fixed
- The backup uploader stopped for good on the first failed upload, without telling anyone. It now records
  the error, retries with backoff and catches up when the bucket answers.
- At start, an unreachable bucket was read as an empty one. The uploader now retries until it can read it.
- A malformed backup `endpoint` crashed the S3 client; it is now refused when the database opens.
- Durable commits with many concurrent writers were about a third as fast as they should be: 256 writers
  now make 130,000 to 165,000 durable commits per second on the benchmark machine, up from about 65,000.
- `GROUP BY` and `ORDER BY` on text, boolean and UUID columns returned wrong groups and unsorted rows.
- Correlated subqueries on UUID columns returned one value for every parent row; other non-integer
  correlations are refused instead of answered wrongly.
- A failing `ALTER TABLE` reported success; it now returns the error and changes nothing.
- The change feed could end a poll early during an `ALTER`; a live subscription could miss changes
  committed while it started; a failed `CREATE TABLE` could leave a half-registered table.
- A short `VALUES` row now names the columns and shows the corrected statement.

## 0.1.0 — 2026-10-16

First release: the embedded engine with concurrent writers, strict types, row policies, live queries,
online schema changes, foreign keys, S3 backup with compression and encryption, followers, the `sqweezdb`
command with its web console, and bindings for Node, Python, Go and C.
