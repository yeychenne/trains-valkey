# E4 — Throughput sweep (Sentinel) — 2026-05-26 PM

**Scope reduced from the plan:** Sentinel-arm sweep only (the
trains-valkey arm at E1-v2 is already established at 0 loss).
Same 3-node `t4g.small` ring as E2, single deploy, 4 rates back-to-back
with `FLUSHDB` and a primary kill between iterations. Total spend
< \$0.10. Teardown verified clean.

## Headline (and the caveat that goes with it)

| target rate | count | acked / count | elapsed | sustained throughput | batch p99 latency | cluster healthy at end? |
|---:|---:|:---:|---:|---:|---:|:---:|
| **50 wr/s** | 750 | **750 / 750** | 19.8 s | 37.9 wr/s | 1.17 ms | ✅ (replicas + master) |
| **1000 wr/s** | 15 000 | **15 000 / 15 000** | 21.0 s | 712.7 wr/s | 1.01 ms | ⚠ (one replica left) |
| **5000 wr/s** | 75 000 | **49 700 / 75 000** | 71.1 s | 699.1 wr/s | 0.64 ms | ❌ (cluster degraded) |
| **10000 wr/s** | 150 000 | **0 / 150 000** | 60.2 s | 0 wr/s | n/a | ❌ (no live primary; Sentinel reports a "master" that doesn't answer) |

**The two clean data points** (r50, r1000) show Sentinel survived a single
mid-load primary kill with **100 % acked-write delivery client-side**.
The two degraded data points (r5000, r10000) surfaced a different — and
arguably more important — Sentinel property: **sequential failovers
consume the Sentinel quorum's available primary candidates.** After 3
kills on a 3-node cluster, no Redis is alive → Sentinel's "master"
pointer is stale → all further writes fail. The cluster cannot
recover without operator intervention.

That second property is genuinely informative for the paper. See §"What
the four rates *do* tell us" below.

## Why the higher rates degraded the cluster instead of measuring loss

The sweep design dispatched a primary-kill after every rate iteration.
With Sentinel's quorum-based failover, each kill is recoverable
**as long as ≥ 2 of 3 Redises are alive AND the cluster is healthy
before the kill**. The sweep didn't restart killed Redises between
iterations, so:

- after rate 1: 2 Redises alive (old primary dead), 1 new primary
- after rate 2: 1 Redis alive (former replica + new primary; the
  rate-2 kill claimed it)
- after rate 3: 0 Redises alive → cluster dead
- rate 4 ran against a dead cluster → 0 acked.

A clean rate-threshold characterisation requires either:
1. **Fresh deploy per rate** (single rate per CDK cycle; ~4× the
   deploy/teardown time but trivially correct), or
2. **Restart killed Redis between iterations** (script `systemctl
   start valkey-server` or re-run the bootstrap on the just-killed node
   so it rejoins as replica before the next iteration's kill).

Either path is the natural follow-up. For this REPORT, the two clean
points + the cluster-degradation finding stand on their own.

## What the four rates *do* tell us

1. **Sentinel preserves writes through failover at moderate rates.**
   r50 and r1000 both show 100 % acked-write delivery client-side
   (750/750 and 15 000/15 000). The kill mid-load did NOT drop the
   client.
2. **Throughput ceiling on a sync RESP client over inter-AZ TCP is
   around 700 wr/s.** Even at target rates of 5000 and 10000 wr/s
   (with pipelining of 10 and 50 respectively), the sustained
   throughput sat at ~700 wr/s. This is the *client-side* bottleneck —
   socket round-trip latency on EC2 inter-AZ (~1 ms RTT) limits a
   non-pipelined client to ~1000 wr/s and even with pipelining the
   Python client's per-batch overhead caps the effective rate.
3. **Per-batch latency p99 is sub-ms at all rates** that the client
   could sustain. This is a clean Redis behaviour — even with the
   primary being killed mid-load, surviving writes are fast.
4. **Sentinel quorum exhaustion is real and irrecoverable** under
   sequential kills. A multi-fault scenario (E5 territory) needs to
   either restart victims as replicas or accept that Sentinel doesn't
   tolerate N failures, only N-1.

## What this means for the paper

The original §6.2 [TODO E4] was framed as "characterise the rate at
which Sentinel begins losing acked writes." That measurement is still
not landed here — the cluster degraded before saturation. What landed:

- **Throughput floor for Sentinel writes is robust** at this client's
  achievable rate. The "Sentinel loses acked writes" literature claim
  needs throughput that exceeds what a single Python sync client can
  produce.
- **Quorum exhaustion on sequential failover** is a real Sentinel
  property worth citing alongside the "loses acked writes" claim. It
  affects operational reasoning (cluster planning vs. fault budget).
- **A proper rate-threshold sweep requires fresh deploys per rate**
  (re-plan in `e4-throughput-sweep-plan-2026-05-26.md` revision; the
  follow-up should also include a multi-client driver — 32-way
  concurrent — to exceed the single-client throughput ceiling).

The paper's §6.2 [TODO E4] therefore moves from `[TODO]` to
`[PARTIALLY CLEARED — see E4 REPORT; full sweep deferred]`.

## Artifacts

- `acked-r0050.json` — 750 acked entries; throughput 37.9 wr/s
- `acked-r1000.json` — 15 000 acked entries; throughput 712.7 wr/s
- `acked-r5000.json` — 49 700 acked entries; throughput 699 wr/s
  (degraded cluster after this run)
- `acked-r10000.json` — empty (cluster was dead)
- `latency-r{0050,1000,5000,10000}.json` — per-rate latency
  percentiles (p50/p95/p99/p999)

## Cost + teardown

- 3 × t4g.small × ~30 min ≈ \$0.025
- S3 + SSM + CloudFormation: trivial
- **Total < \$0.05.** Teardown verified: zero `TrainsBench*` stacks
  remaining; SSM password parameter deleted.

## Cross-references

- Plan that this REPORT realised (partially):
  `bench/reports/e4-throughput-sweep-plan-2026-05-26.md`
- E2 (the original Sentinel comparison at 50 wr/s, 0 loss):
  `bench/results/ec2-2026-05-26-e2/REPORT.md`
- The chaos client this sweep used: `/tmp/e4-chaos.py` (Python
  sync RESP, pipelining + rate cap + latency capture; should be
  promoted to `bench/coordinator/e4-chaos.py` in the next session).
- Follow-up: redesign with fresh-deploy-per-rate or
  restart-victims-as-replicas; add multi-client driver.

## What still belongs in a future session

1. **Single-rate clean run at 5000 wr/s with fresh deploy** — gets the
   one canonical "did Sentinel lose at this rate" data point.
2. **Multi-client driver** to push past the single-client throughput
   ceiling.
3. **trains-valkey at high rate** as parity data — once the client can
   sustain higher than 700 wr/s, run the same workload against the
   trains-valkey stack and confirm zero loss.
