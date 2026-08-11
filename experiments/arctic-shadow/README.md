# TRAINS + Arctic Shadow Experiment

This standalone crate checks an Arctic-backed string store against a real
Valkey engine while TRAINS provides the replicated total order. It is outside
the production workspace because Arctic 0.1.3 uses Rust edition 2024 while the
production workspace retains its Rust 1.78 minimum.

The experiment is correctness-only. The current proxy serializes store access,
so these results must not be presented as evidence of Arctic's concurrent
throughput.

The single-region service contract, composition invariants, refinement map,
durability assumptions, and pre-performance decision gate are recorded in
`../../docs/ADR-002-trains-arctic-assurance-boundary.md`.

Run from this directory with a current Rust toolchain and `valkey-server` or
`redis-server` on `PATH`:

```sh
cargo test --release -- --nocapture
```

Initial command scope: `SET`, `GET`, `DEL`, `EXISTS`, and `DBSIZE`. Keys must be
NUL-free; the test uses fixed-width ASCII keys.

The suite contains seven gates:

- direct Valkey/Arctic point-command and snapshot equivalence;
- duplicate delivered-operation suppression across the mirrored store;
- a healthy three-node TRAINS ring with two issuers, 1,000 keys, phased updates
  and deletes, and per-origin barriers before final comparison;
- a passive rejoiner that replaces stale Valkey state plus an empty Arctic
  shadow from a pinned-TLS snapshot, then follows continued writes through
  incremental delivered-log tails;
- a full lifecycle gate that crashes a live mirrored member, commits writes in
  the reduced view, restarts it on its stale Valkey engine with an empty Arctic
  shadow, promotes/re-admits it, and verifies writes from both the survivor and
  recovered member converge after the full view is restored;
- deterministic composition simulation over six seeds and rotating victims,
  including delayed/lost recovery polls, stale restart, duplicate delivery,
  overlapping tail retries, and exact manifest checking; and
- a 4,096-command seeded differential manifest that checks binary values and
  the supported command subset against Valkey, Arctic, and an independent
  expected-state model.

The correctness gates are complete. ADR-002 records a scoped decision to
prototype an ordered-writer/shared-reader interface. The current proxy still
serializes store access, so the local benchmark below is not an end-to-end
throughput claim.

## Local Concurrency Benchmark

`ConcurrentArcticStore` exposes shared point operations without the proxy's
store mutex. Its concurrency test uses eight threads and checks exact state
after shared reads, writes, removals, and hot-key updates.

Run the exploratory data-plane benchmark with:

```sh
ARCTIC_BENCH_KEYS=10000 \
ARCTIC_BENCH_OPS_PER_THREAD=100000 \
ARCTIC_BENCH_THREADS=1,2,4,8 \
ARCTIC_BENCH_REPETITIONS=3 \
cargo run --release --bin data-plane-bench
```

The benchmark compares shared Arctic, `Mutex<BTreeMap>`, and an unmodified
Valkey process over loopback RESP. It measures local materialization paths, not
TRAINS replication throughput. Results and caveats are in
`../../bench/results/arctic-data-plane-2026-07-25.md`.

## Ordered writer / shared reader prototype

`OrderedArcticStore` now owns all mutations and snapshot installation, while
clonable `SharedArcticReader` handles concurrent `GET`/`EXISTS`. Snapshot import
builds a fresh generation before swapping it into view, so readers never see a
partially restored keyspace. `DBSIZE` and multi-key `EXISTS` intentionally
remain coordinated on the writer side.

The original seven gates plus three concurrency tests pass through this
interface. The benchmark now also includes `ordered-mutex-btree`, which pays
the same writer queue and acknowledgement cost as `ordered-arctic`:

```sh
ARCTIC_BENCH_KEYS=10000 \
ARCTIC_BENCH_OPS_PER_THREAD=100000 \
ARCTIC_BENCH_THREADS=1,2,4,8 \
ARCTIC_BENCH_REPETITIONS=3 \
ARCTIC_BENCH_OUTPUT=../../bench/results/arctic-ordered-data-plane.json \
cargo run --release --bin data-plane-bench
```

The 2026-08-11 result records a bounded GO for interface performance work, not
a production switch. See
`../../bench/results/arctic-ordered-data-plane-2026-08-11.md`.
