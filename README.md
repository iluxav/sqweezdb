# dblab

Lab for an embedded database in Rust that keeps SQLite's deployment model and removes its limits
(multiple writers, enforced types, access control) without losing speed.

Rule: every design decision is backed by a measurement in `RESULTS.md`. Experiments live in `src/bin/`.

```
cargo run --release --bin e00_baseline        # physics + SQLite baseline (system libsqlite3)
cargo run --release --bin e01_read_scaling    # why SQLite reads stop scaling
cargo run --release --bin e01b_sqlite_knobs 1 # knob diagnosis (arg: MEMSTATUS 0/1, optional "only")
cargo run --release --bin e02_group_commit    # durable write physics
cargo run --release --bin e03_cpu_ceiling     # in-process ceilings
cargo run --release --features bundled --bin e00_baseline   # same, with rusqlite's bundled SQLite
```

## Layout

- `src/` — the engine (`table.rs` storage and log, `sql.rs` planner and access control, `live.rs` reactivity, `cloud.rs` S3 backup, `tenants.rs`, `net.rs` socket/TCP server, `capi.rs` C ABI).
- `src/bin/e*.rs` — one binary per experiment; outputs in `results/`, conclusions in `RESULTS.md`.
- `bindings/go`, `bindings/python`, `bindings/node` — language bindings over the C ABI / PyO3 / N-API. Build artifacts (`*.so`, `*.node`, Go binaries) are not tracked: `cargo build --release` in the binding's directory, then copy `target/release/libnodedblab.so` to `dblab.node` (Node) or `libpydblab.so` to `pydblab.so` (Python).
- `data/` — experiment working directories (ignored). `data/nocow` should have the btrfs no-COW attribute (`chattr +C`).
- `tools/` — downloaded third-party binaries used as references (ignored): Litestream 0.5.17 as `tools/litestream`, rclone 1.75.2 as `tools/rclone` (serves the local S3 bucket: `tools/rclone serve s3 --addr 127.0.0.1:9000 --auth-key minioadmin,minioadmin --dir-cache-time 1s data/nocow/s3store`).
- Node comparisons need `better-sqlite3` installed locally: `cd bindings/node && npm install --no-save better-sqlite3`.
