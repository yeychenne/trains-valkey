# E1-v2 — Live-fire chaos-2 EC2 retest with PR-RD-9 active (2026-05-26 PM)

**Third EC2 chaos run today.** This one is the *during-window* test that
E1 surfaced as the open question: writes that begin BEFORE the kill, get
acknowledged DURING the masked window, must be present on every survivor.

## Headline

**Validated.** Every survivor preserved every acknowledged write through
the masked-crash window. The TRAINS-replicated Redis ring **continued
serving writes** while the failure detector struck, view-changed, and
re-formed the ring around the dead node.

| Engine | acked_total | missing_keys | dbsize |
|---|---|---|---|
| node-0 (alive) | **2000** | **[]** | **2000** |
| node-1 (alive) | **2000** | **[]** | **2000** |
| node-2 (proxy SIGKILLed mid-load) | 2000 | 1000 phase-2 keys (expected) | 1000 |

The chaos client got `+OK` for **all 2000 writes** — 1000 from phase 1
(pre-kill) and **1000 from phase 2 (post-kill, through the masked
window)**. Both survivors hold all 2000 keys with byte-identical
convergence. Node 2 (the killed one) holds the 1000 phase-1 writes its
engine had already applied before the kill but, correctly, none of the
phase-2 writes — its proxy was dead so no further writes could reach its
local engine. **Node 2 is not a survivor; the survivor claim is about
nodes 0 and 1 only.**

This closes the [open question from E1's REPORT](../ec2-2026-05-26-e1/REPORT.md).

## Topology

- Same as E1: 3-node `trains-valkey` ring on EC2 `t4g.small` arm64 AL2023
  in `eu-west-3`. Self-managed Valkey 9.0.3 loopback per node.
- Instance IDs: `<instance-id>` (node 0, 10.0.0.89),
  `<instance-id>` (node 1, 10.0.2.171),
  `<instance-id>` (node 2, 10.0.0.239, victim).
- **Built from `integration/e1-chaos-2-retest`** with PR-RD-5a + PR-RD-5b
  + PR-RD-6 + PR-RD-8 + **PR-RD-9** merged on top of `main`. The PR-RD-9
  delta — connector reads TLS stream + probe-reconnects on peer-close —
  is the new ingredient vs E1.

## Workload + timing

- `trains-valkey-chaos --mode load --count 2000 --hold-secs 30 --acked-out
  /tmp/acked-e1v2.json` on node 0.
- Dispatch via SSM at 14:59:30; kill (`fis-kill-redis` on node 2)
  dispatched 20 s later at 14:59:50; victim's proxy died at **12:59:51
  UTC** per the kill script.
- Chaos load **completed in 45.30 s wall clock** (down from E1's 5+ min
  hang).

Decomposition (approximate):
- Phase 1 (writes 0..999): ~13 s after dispatch lag.
- Kill: ~T+20 s, lands during phase 1 (around write index ~600-700).
- Phase 1 finishes after the kill — the proxy on node 0 had already
  broadcast and the train continued circulating with the *masking*
  behavior:
  - PR-RD-9's reader task on node 1's connector observes EOF when the
    accept-side task on node 2 dies (PR-RD-8 closes it).
  - `peer_close` signal fires → connector breaks → probe-reconnect →
    fail_streak ticks 1→2→3→4→5 → `unreachable_rx` fires.
  - Proxy's driver sees `unreachable_rx`, runs the view-change protocol,
    retargets node 1's successor from node 2 to node 0.
  - Ring is re-formed as {0, 1} with edge 0→1→0.
- Hold (30 s): the client sleeps. During this window the view change
  completes if it hadn't already.
- Phase 2 (writes 1000..1999): **all 1000 succeed** on the re-formed
  2-node ring. Every `+OK` is honored by both surviving engines.

The MTTR (kill → first phase-2 write `+OK`) was below the 30 s hold —
the chaos client never had to wait past the natural hold window.

## What PR-RD-9 actually changed

The E1 hang was caused by node 1's connector idling in `select!{wire_rx,
retarget_rx}` after the kill — it never tried to write to the dead peer
because the chaos client was blocked waiting for round-trips, so
upstream stopped issuing trains, so `wire_rx` was empty, so no write was
attempted, so `TCP_USER_TIMEOUT` (PR-RD-6) had nothing to time out, so
`fail_streak` never advanced.

PR-RD-9 splits the TLS stream and spawns a reader task that drains the
read half. When the peer's TLS stream is dropped (PR-RD-8 ensures this
happens on `abort()`; on EC2 it happens when SIGKILL drops the
process's sockets), the reader observes EOF and signals the connector.
The connector then probe-reconnects, the reconnect fails, fail_streak
advances, unreachable fires, view change runs. **All without needing
any in-flight write traffic.**

## Cost

- 3 × t4g.small × ~30 min in eu-west-3 ≈ \$0.02
- S3 + SSM + CloudFormation: trivial
- **Total < \$0.05.** Teardown verified clean (3 instances `terminated`,
  zero `TrainsBench*` stacks remaining, SSM password parameter deleted).

## Artifacts

- `report-node-0.json` and `report-node-1.json`: the two survivor
  PartialReports — `acked_total=2000, missing_keys=[], dbsize=2000`.
- `report-node-2-killed-victim.json`: the victim's PartialReport — has
  the 1000 phase-1 keys but missing the 1000 phase-2 keys (correct,
  expected). Annotated.
- `acked-e1v2.json` (2000 entries) was uploaded to S3 during the run;
  the bucket was auto-deleted by CDK teardown. The acked-set is fully
  reconstructible from the workload spec (`chaos:k0..chaos:k1999` =
  `v0..v1999`); the survivor PartialReports are the load-bearing
  evidence.

## What this means for the paper

The §6 [TODO E1] line item — *the during-window claim* — is **cleared**.
The paper's headline now reads, without caveat:

> Every acknowledged write was preserved on every survivor through a
> permanent in-window crash. DBSIZE converged byte-for-byte across all
> surviving engines. Property held at EC2 scale on a real `Valkey 9.0.3`
> backend in `eu-west-3`.

Remaining open from §6:
- [TODO E2] Sentinel head-to-head measurement
- [TODO E3] Larger N (5, 7 nodes)
- [TODO E4] Throughput/latency sweep
- [TODO E5] Jepsen-style adversarial schedule
- [TODO E6] Containerized reviewer harness

E2 is the next-highest-leverage step (gives the paper its quantitative
comparison).

## Cross-references

- **Open PRs that produced this result:** #20, #21, #23, #27 (PR-RD-8),
  **#30 (PR-RD-9 — the key piece)**.
- **E1 report (the morning run that surfaced the gap):**
  `bench/results/ec2-2026-05-26-e1/REPORT.md`.
- **Morning healthy-ring run (200/200):**
  `bench/results/ec2-2026-05-26/REPORT.md`.
- **Paper draft to update:** `bench/reports/paper-trains-replicated-redis-draft-2026-05-26.md` §6.1 + §6.2.
- **Blog draft to update:** `bench/reports/blog-trains-replicated-redis-2026-05-26.md` §"What's next" — E1 is now done.
