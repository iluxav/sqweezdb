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
