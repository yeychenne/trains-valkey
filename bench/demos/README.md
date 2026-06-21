# bench/demos — workload demos for trains-valkey

Two demo apps that exercise the trains-valkey proxy under realistic
coordination-fast-path patterns. Picked from the survey at
`bench/reports/valkey-demo-apps-survey-2026-05-27.md` because they match
the paper's motivating story ("locks, queues, gate flags") and stay
within the v1 RESP subset trains-valkey supports.

| Directory | Demo | What it tests |
|---|---|---|
| `distributed-lock/` | `SET key uuid NX EX` + `INCR ops` + ownership-checked `DEL` | Mutual exclusion across a proxy kill: every successful acquire has exactly one matching release. Counter equals acquired count. No orphaned locks. |
| `leaderboard/` | `ZINCRBY leaderboard <delta> <player>` | Sorted-set convergence: every survivor's `ZRANGE` reconciles to the sum of acked deltas, byte-for-byte. |

## How each demo is shaped

Both Python scripts follow the same two-phase `--mode load | verify-local`
contract as `crates/trains-valkey/src/bin/chaos.rs`, so they slot into the
existing EC2 chaos harness (`scripts/redis-chaos/`) without harness changes:

- `--mode load` writes the workload, captures every successful operation
  in an `acked.json`, and exits. Run on the coordinator (or a single
  driver host).
- `--mode verify-local` reads `acked.json`, queries the local engine on
  this host, and writes a `PartialReport` to JSON. Run on every survivor.
- The aggregator step (per-engine partials → final REPORT.md) is the same
  human step the existing chaos run uses.

## Why Python and not a new Rust crate

The lock and leaderboard demos are workload generators, not protocol
participants. They drive RESP from outside the trains-valkey process — same
role as the bench coordinator's `faults.py`. Python ships them in < 1 h
with zero workspace changes, and the RESP client is ~80 LOC with no
external deps. If they prove valuable in the long term, promote them to
`crates/trains-valkey-demos/` as proper Rust binaries (suggested name
matches the `trains-valkey-chaos` convention).

## Local smoke

See `bench/results/demo-apps-2026-05-27/REPORT.md` for the reproducible
local-smoke baseline: 8 k lock cycles/s and 30 k ZINCRBY/s against a
vanilla Valkey on loopback, both sub-ms p99.

## EC2 chaos extension

Deferred to the next session. Plan is in the 2026-05-27 EOD handover.
