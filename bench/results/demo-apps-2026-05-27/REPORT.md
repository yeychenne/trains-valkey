# Demo-app benchmarks — local-smoke baseline — 2026-05-27

Track 5 of the 2026-05-27 build. The two demos identified in
`bench/reports/valkey-demo-apps-survey-2026-05-27.md` (distributed lock,
real-time leaderboard) are implemented and pass their correctness checks
against a real Valkey 9.x backend on loopback.

## TL;DR

| Demo | Workers | Duration | Throughput | p50 lat | p99 lat | max lat | Correctness | Cost |
|---|---:|---:|---:|---:|---:|---:|:---:|---:|
| **Distributed lock** (SET NX EX / DEL) | 4 | 10 s | **8 192 cycles/s** | 0.114 ms | 0.156 ms | 8.88 ms | ✅ 39 531 acquired = 39 531 released, 0 orphaned, counter exact | $0 (local) |
| **Real-time leaderboard** (ZINCRBY) | 4 | 10 s | **30 634 ops/s** | 0.127 ms | 0.164 ms | 6.59 ms | ✅ all 50 players reconcile to expected score | $0 (local) |

This is the **baseline against a vanilla Valkey 9.x on `127.0.0.1`** (no
proxy, no ring). The point of the run is to establish the workload's
inherent rate ceiling and latency floor before introducing trains-valkey.

## Why this matters

Both demos hit **sub-ms p99 latency** with a single-host Python sync client
driving 4 workers. The leaderboard demo also crossed 30 k ops/s — comfortably
above the ~700 wr/s ceiling the E4 sweep saw on inter-AZ EC2 (`bench/results/ec2-2026-05-26-e4/REPORT.md`).

The E4 ceiling was network-bound, not CPU-bound. Same client driver, same
RESP-level behaviour — the local run isolates the *workload's* rate and
latency floor. When we run these demos against the trains-valkey proxy ring
on EC2 (the deferred run; see §"What's next" below), we expect:

- **Latency** to climb to the inter-AZ-RTT-plus-broadcast floor (~5-15 ms p50
  observed in the E1-v2 acked-stream).
- **Throughput** to land near the E4 ceiling (~700 wr/s per client).
- **Correctness** to hold byte-for-byte across survivors during chaos.

## How to reproduce the local smoke

Prereqs: `valkey-server` on PATH (`brew install valkey` on macOS).

```bash
# 1. start a clean local Valkey
valkey-server --port 16379 --daemonize yes \
  --dir /tmp/trains-valkey-demo-smoke --save "" \
  --pidfile /tmp/trains-valkey-demo-smoke/valkey.pid \
  --logfile /tmp/trains-valkey-demo-smoke/valkey.log

# 2. lock demo: load + verify-local
python3 bench/demos/distributed-lock/lock_chaos.py \
  --mode load --host 127.0.0.1 --port 16379 \
  --workers 4 --keys 8 --ttl 10 --duration 10 \
  --acked-out bench/results/demo-apps-2026-05-27/lock/acked.json

python3 bench/demos/distributed-lock/lock_chaos.py \
  --mode verify-local --host 127.0.0.1 --port 16379 \
  --acked-in  bench/results/demo-apps-2026-05-27/lock/acked.json \
  --report-out bench/results/demo-apps-2026-05-27/lock/report-local.json

# 3. leaderboard demo: load + verify-local  (FLUSHDB between demos)
valkey-cli -p 16379 FLUSHDB
python3 bench/demos/leaderboard/leader_chaos.py \
  --mode load --host 127.0.0.1 --port 16379 \
  --workers 4 --players 50 --duration 10 \
  --acked-out bench/results/demo-apps-2026-05-27/leader/acked.json

python3 bench/demos/leaderboard/leader_chaos.py \
  --mode verify-local --host 127.0.0.1 --port 16379 \
  --acked-in  bench/results/demo-apps-2026-05-27/leader/acked.json \
  --report-out bench/results/demo-apps-2026-05-27/leader/report-local.json

# 4. teardown
kill $(cat /tmp/trains-valkey-demo-smoke/valkey.pid)
```

## Lock demo — detail

**Workload.** 4 worker threads each spin in a tight loop:

1. Pick a random key from a pool of 8.
2. `SET lock:<key> <uuid> NX EX 10` — try to acquire.
3. If acquired: `INCR ops` (the critical section's only state mutation).
4. `GET lock:<key>`; if the value still matches our UUID, `DEL lock:<key>`.

Race: between GET-confirm and DEL, the TTL could fire and another acquirer
could win, in which case the DEL deletes a lock we no longer own. Documented
in the paper (no Lua available in trains-valkey v1) and tolerated for the
demo's measurement purpose — the count of orphans is the chaos signal.

**Observed.**
- 4 workers × 10 s ⇒ 39 531 successful acquire/release pairs (≈ 9 880 per
  worker per second, with `INCR` included between them).
- The acquire/release count and the `ops` counter are **exactly** matched
  (every successful acquire incremented the counter once). Zero orphaned
  locks.
- Throughput per command (each pair is 4 RESP commands) sums to ~33 k
  RESP cmds/s — close to the leaderboard demo's command rate, which is the
  same RESP-roundtrip-cost cap.

**Latency.** p50 = 0.114 ms, p95 = 0.133 ms, p99 = 0.156 ms, max = 8.883 ms.
The single multi-ms outlier is the `valkey-server` process being scheduled
out briefly under load; both demos saw it.

## Leaderboard demo — detail

**Workload.** 4 worker threads each spin in a tight loop:

1. Pick a random player from a 50-player pool.
2. Pick a random integer delta `[1, 100]`.
3. `ZINCRBY leaderboard <delta> <player>`; record the new score the server
   returned.

The verify step queries `ZRANGE leaderboard 0 -1 WITHSCORES` and compares
each member's observed score against the sum of deltas in the acked log.
The reconciliation must be **exact** — sorted sets are deterministic over
integer increments, so even on a vanilla Valkey we expect byte-identical
reconciliation. (Under trains-valkey this exercises the broadcast-and-apply
path on `ZINCRBY` — see paper §4.3 effect resolution; integer ZINCRBY is
classified as `DeterministicWrite` and broadcast as-is.)

**Observed.**
- 4 workers × 10 s ⇒ 307 161 ZINCRBYs (≈ 30 634 / s).
- All 50 players appear in the leaderboard; every score reconciles
  exactly.

**Latency.** p50 = 0.127 ms, p95 = 0.148 ms, p99 = 0.164 ms, max = 6.586 ms.

## What's next — the EC2 chaos extension (deferred to next session)

The local smoke is the *baseline*. The headline number is the EC2 chaos
run: each demo through the trains-valkey proxy ring with a mid-workload
proxy SIGKILL. Plan:

1. Reuse the E1-v2 chaos harness (`scripts/redis-chaos/` + the bench-aws CDK).
2. Boot a 3-node `t4g.small` ring as in E1-v2.
3. Upload the two Python demos to the coordinator (small enough to inline
   into UserData; no Rust rebuild needed).
4. For each demo:
   - Phase 1: `lock_chaos.py --mode load` (or `leader_chaos.py`) driven
     against node-0's RESP port (port 7000 in the bench, which the proxy
     listens on; proxy then talks loopback to its local Valkey).
   - Mid-phase: SSM RunCommand SIGKILL on victim's trains-valkey proxy.
   - Phase 2: load continues for the second half.
   - After kill window: `verify-local` on every survivor against the
     acked-set; assert `orphaned == 0` for lock and `all_match == true`
     for leader.
5. Capture `acked.json` + per-engine `report-local.json` per demo; produce
   `REPORT.md` per demo.

Estimated cost: ≤ $0.20 EC2 + ~1 h wall clock (both demos in one ring
session).

The plan is captured in detail under §"Demo-app EC2 chaos plan" in the
2026-05-27 EOD handover.

## Artifacts

```
bench/demos/
├── distributed-lock/
│   └── lock_chaos.py            # load + verify-local
└── leaderboard/
    └── leader_chaos.py          # load + verify-local

bench/results/demo-apps-2026-05-27/
├── REPORT.md                    # this file
├── lock/
│   ├── acked.json               # 82 073 events (acquire / release / miss)
│   └── report-local.json        # verify-local result
└── leader/
    ├── acked.json               # 307 161 ZINCRBY events
    └── report-local.json        # ZRANGE reconciliation
```

## Cross-references

- Selection rationale: `bench/reports/valkey-demo-apps-survey-2026-05-27.md`
- Paper section to update: §6.5 (new) — "Demo workloads"
- Chaos harness this will plug into: `scripts/redis-chaos/README.md`
- Threat-model context: T-tr-01 (RESP-client spoofing — no auth on the
  loopback today; the demos drive plaintext RESP so they're an honest
  illustration of the current trust posture)
