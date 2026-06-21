# trains-valkey

A RESP-level write-interception proxy that gives Redis loss-free
failover via state-machine replication over a uniform total-order
broadcast ring. *Sentinel without acked-write loss, against an
unmodified Valkey or Redis 8.*

Built on top of [trains-rust](https://github.com/yeychenne/trains-rust)
— the TRAINS protocol kernel + TLS ring transport + view-change
recovery, formally verified in TLA+ / Apalache / Ivy.

## What this gives you

| Property | Redis Sentinel | trains-valkey |
|---|:---:|:---:|
| Async-replication acked-write loss on single primary crash | ⚠ documented data-loss window | ✅ zero loss on every survivor |
| Multi-victim sequential kills (quorum exhaustion) | ❌ unrecoverable after N-1 kills | ✅ view change re-forms the ring |
| Restarted node re-integrates + restores N-redundancy | ⚠ resyncs as a replica, no quorum role | ✅ passive catch-up (v2) → re-admit to a full acking member (v3) |
| Sub-ms p99 latency at moderate write rates | ✅ | ✅ |
| Drops in front of an unmodified engine (no Redis fork) | n/a | ✅ Valkey 9.x or Redis 8 |

Verified on EC2 with a 3-node `t4g.small` ring:

| Experiment | Setup | Result |
|---|---|---|
| **E1-v2** (during-window kill) | 3-node trains-valkey ring, SIGKILL the victim's proxy mid-load | **2 000 / 2 000 acked writes preserved** on every survivor; byte-identical `DBSIZE`; ~45 s wall-clock |
| **E5 t1-rejoin** (kill → restart → rejoin) | SIGKILL node 2 mid-load, restart it; it rejoins via state transfer while writes continue | **Rejoined node converges 2 000 / 2 000**, matching survivors, zero acked-write loss (v2). [REPORT](bench/results/ec2-2026-06-16-e5-rejoin/REPORT.md) |
| **E4 clean rate-threshold sweep** | Sentinel comparison at 50 / 500 / 1 000 / 2 000 wr/s | Sentinel: **0 loss at every rate** with fresh cluster per rate. trains-valkey: same property, *plus* survives multi-victim scenarios Sentinel doesn't. |
| **Demos (local-smoke)** | distributed-lock + leaderboard apps | Lock: 8 192 cycles/s, p99 0.156 ms, 0 orphans. Leaderboard: 30 634 ops/s, p99 0.164 ms, exact reconciliation. |

The decisive comparison: **trains-valkey preserves acked writes through
both single-kill and multi-victim scenarios; Sentinel survives only the
single-kill case** (and only when the chaos client is failover-aware
and the rate is below the replication-lag threshold).

Full numbers: [paper](bench/reports/paper-trains-replicated-redis-draft-2026-05-26.md)
(v1.0, 2026-05-27 PM). Threat model and remediation plan are under
[`bench/reports/`](bench/reports/).

## What's here

| Path | Contents |
|---|---|
| `crates/trains-valkey/` | The proxy itself: RESP listener + classifier + origin-side effect resolver (`SPOP → SREM`, `INCRBYFLOAT → SET`, `HINCRBYFLOAT → HSET`) + apply-side dedup + crash masking via [`trains-recovery`](https://github.com/yeychenne/trains-rust) |
| `bench/coordinator/e4_chaos.py` | Sentinel + direct-Redis chaos client (pipelined RESP, rate cap, latency capture) |
| `bench/demos/` | `distributed-lock/` + `leaderboard/` — two demo workloads matching the paper's "Redis as a coordination fast-path" framing |
| `bench/diagrams/` | 4 draw.io architecture views: EC2 bench infra, protocol stack, RESP data flow, view-change sequence |
| `bench/reports/` | Paper draft (v1.0), blog post, threat model (STRIDE per element), security remediation plan |
| `bench/results/` | EC2 chaos run artefacts — `ec2-2026-05-26-e1v2/` (the decisive run), `ec2-2026-05-27-e4-clean/` (the rate-threshold sweep), `demo-apps-2026-05-27/` (local-smoke baseline) |
| `scripts/bench-aws/` | CDK app for the EC2 bench (network + compute stacks), `day2-runbook.sh` turnkey deploy/demos/e4-clean/teardown, `cdk_lints.py` pre-deploy gate |
| `scripts/redis-chaos/` | RESP-level chaos pieces: `keygen.sh`, `node-bootstrap.sh`, `launch-node.sh` |

## Quickstart

### Run the local smoke (no AWS)

```bash
# Spin up a local Valkey
valkey-server --port 16379 --daemonize yes --dir /tmp/d --save "" \
  --pidfile /tmp/d/valkey.pid --logfile /tmp/d/valkey.log

# Drive the lock demo
python3 bench/demos/distributed-lock/lock_chaos.py --mode load \
  --port 16379 --workers 4 --keys 8 --ttl 10 --duration 5 \
  --acked-out /tmp/lock.json

# Verify
python3 bench/demos/distributed-lock/lock_chaos.py --mode verify-local \
  --port 16379 --acked-in /tmp/lock.json --report-out /tmp/lock-rep.json
cat /tmp/lock-rep.json
# Expect: counter_match=true, locks_still_held=0

kill $(cat /tmp/d/valkey.pid)
```

### Run the EC2 chaos sweep (turnkey)

```bash
AWS_PROFILE=<operator-tier> ./scripts/bench-aws/day2-runbook.sh prereqs
                                                       ... deploy
                                                       ... demos       (v1.1 scope — needs proxy-ring bootstrap)
                                                       ... e4-clean    (Sentinel sweep — v1.0 result)
                                                       ... teardown
```

Total: ~4 h babysat, < $0.50 spend. Per the chaos runbook §7, this is **not** an unattended one-shot.

## Build

```bash
cargo test --workspace --no-fail-fast
```

`crates/trains-valkey` depends on `trains-core`, `trains-net`, and
`trains-recovery` from the
[trains-rust](https://github.com/yeychenne/trains-rust) repo. The
workspace `Cargo.toml` pins them by git rev for now; we'll switch to
crates.io once trains-rust publishes.

## Security posture

A STRIDE-per-element threat model lives at
[`bench/reports/threat-model-trains-valkey-2026-05-27.md`](bench/reports/threat-model-trains-valkey-2026-05-27.md).
20 threats (T-tr-01..T-tr-20). 5 ship-this-week items (R-01..R-05) are
fully or partially landed (regression tests, listener backpressure, S3
bucket hardening, argv-redaction documentation). The bounded write-dedup
(R-10) shipped 2026-06-12 (PR-RED-1): per-origin watermark + recent-set
replaces the previously unbounded `applied_ops` set, in both the replica
and the proxy driver, and snapshots no longer scale with op count.
Client-boundary mTLS (R-06) shipped 2026-06-12 (PR-RED-3): pass
`--client-identity` + `--allowed-client-spki` to require a pinned client
certificate on the RESP port (reusing the ring's SPKI verifier); the
boundary defaults to plaintext with a loud startup warning, and
`--no-client-tls` silences it. Valkey-on-UDS + `requirepass` (R-07)
shipped 2026-06-12 (PR-RED-4): `--backend unix:///path.sock` +
`--backend-password-file` connect to an engine bound to a UNIX domain
socket only (no TCP); set `VALKEY_UDS` in the EC2 bootstrap to deploy it.
3 items remain v1.1 follow-ups (signed binaries via `rsign2`, append-only
audit log, view-change frame authorisation).

Full plan: [`bench/reports/remediation-plan-trains-valkey-2026-05-27.md`](bench/reports/remediation-plan-trains-valkey-2026-05-27.md).

## License

MIT — see [`LICENSE`](LICENSE).

## History

`trains-valkey` was split out of the original
[`trains-rust`](https://github.com/yeychenne/trains-rust)
monorepo on 2026-05-27. The first stable release tag (`v1.0.0`) aligns
with the paper's banner "Draft v1.0 — 2026-05-27 PM".

The protocol kernel + formal-methods work lives in
[trains-rust](https://github.com/yeychenne/trains-rust). All discussion
of the TRAINS protocol's correctness, TLA+ specification, Apalache
inductive proofs, and Ivy specification is in that repo.
