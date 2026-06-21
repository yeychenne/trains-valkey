# E5 adversarial matrix — live EC2 results (2026-06-15)

Full Tier-1/2 matrix run via `scripts/bench-aws/e5-run.sh` (fresh ring per
scenario), 3 × t4g.small, eu-west-3, `<aws-profile>`. Companion to the E1-v2
masked-crash result (`../ec2-2026-06-15-e1v2-rerun/`).

## Results — 4 PASS, 1 real FAIL

| Scenario | Result | acked | survivors converged | note |
|---|---|---:|---|---|
| `t1-partition` | ✅ PASS | 1000 | yes (1000=1000=1000) | one-way drop 1→2, 20 s |
| `t1-rejoin` | ❌ **FAIL** | 2000 | survivors yes; **rejoined node stuck at 1000** | see below |
| `t2-asymmetric-partition` | ✅ PASS | 1000 | yes | one-way drop 0→2 |
| `t2-clock-skew` | ✅ PASS | 2000 | yes (2000=2000=2000) | +10 s on node 1 — logical clock unaffected |
| `t2-burst-partition` | ✅ PASS | 1000 | yes | partition + 150 ms netem latency |

In every scenario **zero acked writes were lost** (`missing_keys: []` on every
verified node). The partition scenarios correctly *refused to ack* the ~1365
writes they couldn't safely total-order during the disruption (516 abandoned +
the rest non-OK) rather than acking-then-losing them — `total acked 1000` =
exactly phase 1, with phase 2 correctly rejected mid-partition.

## The t1-rejoin FAIL — state transfer is NOT wired into the live proxy

Node 2 was SIGKILLed mid-load and restarted by the sequencer's `restart-proxy`.
It **rejoined** (its proxy came back) but caught up only **1000 of 2000** writes
(the ones it had before the kill; its valkey was never flushed). The survivors
`{0,1}` had all 2000 (zero acked-write loss holds).

Root cause (code-confirmed): `proxy.rs` wires the view-change for **excluding** a
crashed node (`vc_inbox`, Gather/Install) but **never calls
`import_snapshot`/`export_snapshot`**. State transfer exists at the SMR layer
(`replica.rs`, `store.rs`) and is unit-tested *in-process* (`tests/state_transfer.rs`),
but a restarting live node has **no mechanism to request a survivor's snapshot
and catch up**. Same shape as the trains-rust B2 finding (machinery built +
tested, not wired into the live binary).

**This is not acked-write loss** — it's that a *rejoined* node does not reconverge.
Follow-up: wire state transfer into the proxy rejoin path (on restart, fetch +
import a survivor snapshot before resuming delivery). Tracked as a chip / PR.

## Orchestrator notes

Two `e5-run.sh` bugs were found and fixed mid-run (committed): the health-check
`_hc` key must be flushed from **all** engines (flushing only node 0 left a
1-key DBSIZE skew that failed convergence), and `relaunch.sh` must be staged as a
real file via S3 (the in-SSM `printf` mangled `PEER_ADDRS`' quotes, so the first
rejoin restart silently failed).

Cost: a few cents; clean teardown (the access-logs bucket needs emptying before
`cdk destroy` — see teardown.sh).
