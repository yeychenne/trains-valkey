# t1-multi-victim — 5-node EC2 chaos run, 2026-06-25

**Result: ✅ PASS.** Two sequential SIGKILLs on a 5-node `trains-valkey`
ring (kill node 1 at T+15s, kill node 2 at T+45s).  The remaining 3
survivors converged with **zero acked-write loss** and byte-identical
state.

This is the post-launch verification-roadmap item **#12** — credibility
on small clusters — closed.  TLC + Apalache cover safety at N=3/4 in
the spec layer; this is the matching live-hardware evidence at the
upper end of "small."

## Setup

- **Infra:** CDK `TrainsBenchNetwork` + `TrainsBenchCompute`, **5×
  `t4g.small`** (ARM Graviton, AL2023), `eu-west-3`, account
  `<account-id>`.  IAM via instance role (no operator creds on the
  box).
- **AZ placement:** `eu-west-3a` + `eu-west-3b` returned
  `InsufficientInstanceCapacity` for t4g.small on the run day; all
  five nodes ended up in **`eu-west-3c`**.  Documented as an
  intermittent capacity finding in the CDK comment; not pinned in
  config to preserve future multi-AZ runs.
- **Binary:** `trains-valkey` rebuilt with `TRAINS_RING_SIZE=5`
  (compile-time constant in `trains-rust`) and cross-compiled
  `aarch64-unknown-linux-gnu` via `cargo zigbuild`.
- **Schedule:** `bench/coordinator/schedules/t1-multi-victim.json`
  (`ring_size: 5`, events: `kill-proxy at 15 → target 1`,
  `kill-proxy at 45 → target 2`).
- **Workload:** 2 000-write chaos load from node 0 over RESP
  (`trains-valkey-chaos --mode load --count 2000 --hold-secs 30
  --abandon-secs 5`).
- **Verify:** `--mode verify-local` on every surviving node against
  the chaos-client's acked-set JSON.

## Result

| Metric                | Value | Notes |
|-----------------------|------:|-------|
| Acked writes (chaos client) | **1 999** / 2 000 | the missing one was explicitly *abandoned* by the client after 5 s with no `+OK` (the `--abandon-secs 5` honest fingerprint), not lost |
| Acked-write loss on survivors | **0** | every survivor holds all 1 999 acked writes |
| Survivors converged | ✅ | byte-identical `DBSIZE` across the three live engines |
| Killed nodes (1, 2)   | proxies down, engines stale (expected) — not counted as survivors |

Raw e5-run.sh summary line:
```
t1-multi-victim PASS [chaos] abandoned 1 write(s): no +OK within Some(5s)
                              (not counted as acked) [chaos] total acked 1999 writes
```

## What this validates

- The view-change machinery handles **two sequential exclusions** on
  the same ring without operator intervention.  After the first kill,
  the ring re-forms around 4 survivors; after the second, around 3.
- **Zero acked-write loss** holds through the multi-victim scenario —
  the property Redis Sentinel cannot deliver after `N-1` kills (it
  exhausts failover candidates; see the comparison column in the
  trains-valkey README).
- The TRAINS protocol's formal claim "an acknowledged write survives
  partition, double-kill, and rejoin" lands on real EC2 hardware at
  the upper end of the "small cluster" envelope.

## Operator notes (2026-06-25 deploy)

Three operator-side findings worth landing back into the bench infra:

1. `cdk.json` had a reserved `aws:region` context key that aborts a
   fresh `cdk deploy`.  Fix: remove it.
2. A `cdk.context.json` (AZ lookup) was required for reproducible
   deploys — committing one prevents the next operator hitting the
   capacity-discovery dance.
3. The trains-rust binary's ring size is a **compile-time constant**
   (`TRAINS_RING_SIZE`) — running the same scenarios at different
   N requires separate binaries.  This is why `t1-partition` and
   `t1-rejoin` (ring_size=3) skipped on this 5-node deploy.

Items 1+2 are CDK-stack improvements to land separately; item 3 is
a design note worth surfacing in `e5-run.sh` and `scripts/redis-chaos/
launch-node.sh`.

## Spend + teardown

- AWS spend: ~$0.30 (5× t4g.small, ~3 h wall, eu-west-3 pricing).
- Teardown: `cdk destroy --all --force` verified clean — no instances
  remaining (`aws ec2 describe-instances ... --filters tag:Project=TRAINS
  state=running` returned empty).
