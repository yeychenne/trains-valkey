# E5 Tier-1.1 (partition) — zero acked-write loss under a network partition (2026-06-15)

First adversarial E5 scenario driven by `e5_sequencer.py` on live EC2 (3 ×
t4g.small, eu-west-3, `<aws-profile>`). Companion to the E1-v2 masked-crash result
(`../ec2-2026-06-15-e1v2-rerun/`).

## Scenario

`bench/coordinator/schedules/t1-partition.json`: one-way packet drop **node 1 →
node 2** (`iptables -A OUTPUT -d <node2> -j DROP`) injected at T+10 s, healed at
T+30 s, while a 2000-write load (`--abandon-secs 5`) runs on node 0. The
sequencer dispatched the inject + heal over SSM:

```
running schedule t1-partition against 3-node ring
[T+ 10.0s] node 1: partition inject
[T+ 30.0s] node 1: partition heal
schedule complete
```

(iptables rule confirmed present on node 1 during the window, gone after heal.)

## Result — zero acked-write loss

```
[chaos] abandoned 500 write(s): no +OK within 5s (not counted as acked)
[chaos] total acked 635 writes
```

| node | acked_total | missing_keys | DBSIZE |
|---|---:|---:|---:|
| 0 | 635 | 0 | 635 |
| 1 | 635 | 0 | 635 |
| 2 | 635 | 0 | 635 |

- **Every acked write survived** on all three nodes; **0 lost**.
- **All three converged byte-identically** (635 = 635 = 635) after the heal —
  including node 2, which was partitioned out and caught back up.
- The system **correctly refused to ack** the ~1365 writes it could not safely
  total-order during the partition (500 timed out + abandoned, the rest got
  non-OK replies) rather than acking-then-losing them. This is the contrast with
  Sentinel: TRAINS never returns a `+OK` it can't honour, so "acked" means
  "durable" even across a partition.

## Status of the rest of the matrix

This run validated the **adversarial sequencer path end-to-end on live EC2**. The
other Tier-1/2 schedules were *not* validly run in this session — see the
operational notes in `ops-e5-campaign-plan-2026-06-13.md` (each scenario needs a
**fresh ring** — a fault leaves the ring degraded, so running back-to-back on one
ring yields a meaningless 0-acked result; `t1-rejoin`'s `restart-proxy` needs the
node's launch env; `t1-multi-victim` needs a 5-node deploy). Completing the matrix
is mechanical with a per-scenario relaunch runner.

Cost: a few cents; clean teardown.
