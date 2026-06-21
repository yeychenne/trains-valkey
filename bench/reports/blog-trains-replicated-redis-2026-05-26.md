# Replicating Redis over a total-order broadcast ring — a chaos run, and four follow-up PRs

*Draft, 2026-05-26 (refreshed 2026-05-27 with the demo-workload §, security
§, and architecture figures). Engineering blog post — milestone report on
`trains-valkey`, not a paper claim. See the companion paper draft for the
rigorous-evaluation plan and the threat-model appendix.*

> **2026-05-27 update.** Two demo apps now run against trains-valkey — a
> distributed lock (Redlock-lite) and a real-time leaderboard — with
> matching correctness checks (no orphaned locks; sorted-set byte-identical
> across survivors). Local-smoke baseline: 8 k lock cycles/s and 30 k
> ZINCRBY/s, both sub-ms p99. Full numbers: `bench/results/demo-apps-2026-05-27/REPORT.md`.
> Source: `bench/demos/{distributed-lock,leaderboard}/`. A STRIDE-per-element
> threat-model pass (`bench/reports/threat-model-trains-valkey-2026-05-27.md`)
> identified 20 threats; remediation plan at
> `bench/reports/remediation-plan-trains-valkey-2026-05-27.md`. Four
> architecture diagrams (draw.io editable + README) land at `bench/diagrams/`.

---

## TL;DR

We built a Rust proxy that replicates Redis writes across a 3-node ring using
a uniform total-order broadcast protocol (`trains-valkey`, on top of the TRAINS
ring). We deployed it on EC2 (3× `t4g.small` in `eu-west-3`, self-managed
Valkey 9.0.3 per node, all loopback), ran a chaos workload, killed one node's
proxy mid-session, and read every survivor's engine directly:

> **200 writes acknowledged. 200 acked writes preserved on every survivor.
> `DBSIZE` = 200 on all three engines — including the node whose proxy was
> killed. Total spend: < $0.05. Zero AWS resources left after teardown.**

That's the zero-acked-write-loss property Redis async / Sentinel failover
*doesn't* provide.[^1] It's what got us into this in the first place.

Then we tried to repeat the workload **with the node already dead** — the
genuinely interesting test — and it hung for five minutes before we cancelled
it. That's a real EC2-only finding, now fixed across two PRs, with a third
landing the in-process flake that the same investigation uncovered.

This is a story about what shipped this week, and what's honestly not yet
shipped.

[^1]: Sentinel's async-replication model means that on failover, writes that
the primary acknowledged but had not yet propagated to the promoted replica
are simply gone. The Redis docs are explicit about this. Cluster has the
same constraint with a different name.

---

## Why this exists

Agent Orchestrator (a separate project — same operator) keeps its
coordination fast-path in Redis: distributed locks, topology-run dispatch,
session-status flags, human-in-the-loop gate state. Single-machine today.
We want it to be multi-machine without losing those acked writes the moment
a node dies — the exact failure mode Sentinel doesn't protect against.

The trick that makes Redis tractable as a state-machine-replication target:
its command model is mostly deterministic. `SET k v` applied in the same
order on every node gives the same state on every node (Schneider 1990). The
job of the replication layer is to *order* every write the same way on every
replica, then *apply* each write at most once on each replica.

The TRAINS protocol gives us the ordering — it's a uniform total-order
broadcast over a token ring with a clock-gap failure detector and a
view-change reconfiguration token, formally verified in TLA+ in a sister
repo. We had to build:

- a RESP write-interception proxy that classifies commands (read /
  deterministic-write / non-deterministic),
- effect resolution at the origin for non-deterministic writes
  (`SPOP → SREM`, `INCRBYFLOAT → SET`, etc.) so the *applied* form is
  deterministic,
- apply-side dedup so at-least-once delivery doesn't double-apply,
- state transfer so a rejoining replica catches up the whole keyspace,
- crash masking via the view-change token so a permanent crash is
  *masked*, not *failed over*.

Five PRs (RD-1 through RD-4) shipped that stack over the last few weeks,
proven locally against a live Valkey 9.1 in a 3-node ring (160 Rust + 18
Python tests; clippy clean).

The only thing left was the at-scale EC2 confirmation that the protocol and
the real backend hold together under the kind of clock-skew and inter-AZ
jitter a single machine can't reproduce.

---

## The EC2 chaos session

The plan: deploy 3 nodes via CDK, bootstrap a loopback Valkey on each via
SSM, launch the `trains-valkey` ring, run a chaos workload that writes
through node 0's RESP port for ~30 s while we `fis-kill` node 2's proxy
mid-window, then read every survivor's engine and assert no acked write was
lost.

Reality:

1. **`cdk deploy` immediately died.** The stack had never been deployed
   before — landed as a single commit weeks ago. Six latent bugs surfaced
   in one go: missing AWS account in `Environment` (CDK falls back to a
   dummy 2-AZ context, `subnet_list[2]` panics on synth), wrong CDK
   bootstrap qualifier (the account uses `bankx`, not the default), a
   self-referencing security group rule that creates a CloudFormation
   cycle, a non-ASCII em-dash in the SG description that EC2 rejects,
   `eu-west-3b` out of `t4g.small` capacity, and the SG missing port 7000
   (the ring's TLS port). All wedged into a single fix PR. Don't ship IaC
   you've never deployed.
2. **`cross-rs 0.2.5` cannot cross-compile for x86_64-linux from an arm64
   Mac on a newer rustup.** Pivoted to `podman --platform linux/arm64
   rust:1-bookworm` — native build inside a Linux arm64 container,
   targeting `t4g.small` (Graviton) instances. Cheaper too: $0.017/hr vs
   $0.023/hr.
3. **The chaos verifier was written assuming every survivor's engine was
   network-reachable from the driver host** — but the runbook is explicit
   that Valkey stays bound to `127.0.0.1` on each ring node. We refactored
   the binary into two modes: `--mode load` writes the acked-set to a JSON
   file; `--mode verify-local` runs on each survivor, queries its loopback
   engine, and emits a per-engine partial report. The coordinator
   aggregates off-host. Engine port never leaves the box.
4. **The happy-path workload ran.** 200/200 acked, DBSIZE 200/200/200
   across all three engines after we killed node 2's proxy.

Then we tried the adversarial workload — writes **during** the masked-crash
window — and it hung.

---

## The slow-view-change finding

Five minutes in, nothing happening. We dug into `trains-net`'s transport
and found two problems:

**Problem A (production-scale):** `trains-net` only fires its `unreachable_rx`
signal — the thing that tells the failure detector "your successor is gone"
— after `UNREACHABLE_FAILURES=5` consecutive *connect* attempts fail. But on
an established TCP connection that just lost its peer, Linux's `send(2)`
doesn't error. The kernel buffers the segment and silently retransmits for
up to `TCP_USER_TIMEOUT` (default: ~15 minutes). So the connector never
*tried* to reconnect, never saw a connect failure, never struck the failure
detector. The view change never started.

In-process tests don't catch this: loopback delivers EPIPE the moment the
socket is dropped, which the EC2 path doesn't.

Fix: `setsockopt(TCP_USER_TIMEOUT, 3 s)` on every ring socket
(`apply_ring_socket_opts`, called right after `accept` and right after
`connect`). The kernel now forces the connection closed if a segment goes
unacknowledged for >3 s; the next write returns `ETIMEDOUT`; the connector
reconnects, fails (peer is dead), strikes the FD, fires `unreachable_rx`.
Expected MTTD ≈ 3 s + a few backoffs ≈ 10 s — comparable to the in-process
p99 of ~8 s we measured weeks ago. **(PR-RD-6.)**

**Problem B (in-process):** while writing the regression test for A, we
realized our in-process crash-masking test was *also* flaky — failed once
in 394 s, passed the next time in 4 s. Different bug, same shape.
`RingTransport::abort()` aborted the listener task, the outbound connector
task, and the mux task — but every accepted-connection task (one
`tokio::spawn` per inbound TLS connection inside `listener_loop`) was
**untracked**. Aborting the listener stopped new accepts but left existing
inbound connections alive with their TLS streams owned. From the peer's
perspective, the connection was still open. Buffer-fills, runtime queues,
test hangs.

Fix: track every accepted-connection `JoinHandle` in an
`Arc<Mutex<Vec<JoinHandle<()>>>>` on the transport; `abort()` aborts them
all. Per-connection futures unwind on their next yield, drop the TLS
stream, kernel sends FIN, peer's read returns EOF. **(PR-RD-8.)**

The two PRs are complementary: RD-6 closes the production-scale window via
kernel timeout; RD-8 closes the in-process window via explicit task
cleanup. Same story, different layers.

---

## The architectural cleanup we squeezed in

While reading `trains-cli` to understand the failure-detector path, we
noticed something embarrassing: `trains-valkey` depended on `trains-cli`
purely to reach the `FailureDetector` and `ViewChange` types. `trains-cli`'s
*entire* library surface (`lib.rs`) was 9 lines re-exporting just those two
modules.

So the SMR proxy depended on the CLI binary's crate.

Extracted both modules into a new `trains-recovery` crate. Pure code motion
— `git mv` with no content edits, dependency graph now flows the right
direction (`trains-cli` and `trains-valkey` both depend on `trains-recovery`;
no inversion). 6 import sites touched, 0 semantic changes, 0 new tests
expected (so 161/161 still pass, same as main). **(PR-RD-7.)**

---

## What's honestly not yet shipped

Eight PRs are open against `main` (numbers 20–27). They cover:

| Concern | Shipped? |
|---|---|
| Healthy-ring acked-write preservation, EC2 scale | **Yes**, 200/200 |
| Survivor convergence (DBSIZE byte-match) | **Yes**, 200/200/200 |
| Adversarial workload (writes *during* masked crash), EC2 scale | **Not yet** — the run that hung, now fixed in code but not retested on EC2 |
| `TCP_USER_TIMEOUT` on ring sockets | **Yes**, unit-tested |
| In-process `crash_masking` stable across 5+ runs | **Yes**, 4 s each post-RD-8 |
| `trains-valkey → trains-cli` architectural inversion | **Resolved** (RD-7) |
| Comparative measurement vs Redis Sentinel | **No**, never done |
| Throughput / latency characterization at >10⁵ writes | **No**, single 200-write run |
| `MULTI`/`EXEC`/Lua atomicity | **No**, intentional carry-forward |

We're at an engineering milestone: the protocol-plus-engine combination
holds at EC2 scale under the easy chaos scenario, and the diagnostic chain
that produced PRs 5b/6/7/8 closed the open EC2-only finding.

**Update (later same afternoon):** we landed PR-RD-9 (connector reads the
TLS stream + probe-reconnects on peer-close) and did the live-fire retest
of the during-window scenario. **All 2000 writes acknowledged, 1000 of
them through the masked-crash window, both survivors hold the full
keyspace with byte-identical convergence.** Total wall clock 45 s, vs
E1's 5-min hang. That settles the during-window claim. What still
remains for paper-quality: Sentinel head-to-head and sustained-load
characterization.

**Update (2026-06-16): node re-integration shipped.** The one thing
crash-*masking* doesn't do is bring the failed node *back*. The E5
adversarial matrix made it concrete: SIGKILL a node mid-load, restart it
under continued writes, and it caught up only ~half the writes — the
survivors held everything (zero acked loss), but the rejoined node never
reconverged, leaving the ring degraded at N−1. We closed that in two
layers. A **passive replica** catches up from a survivor's snapshot plus
a contiguous tail of delivered effects (a keyspace *replace* that wipes
stale state, then incremental tailing). Then a **virtually-synchronous
re-admit view change** — the mirror of exclude, the join ordered inside
the same view-change token stream — promotes the caught-up replica back
to a full *acking* member, restoring N-redundancy. It's grounded in a
TLA+ `ReAdmit` action TLC-checked at 6.28 M states (which rejected a
naive first version that diverged), and the passive half is **validated
live: E5 t1-rejoin now passes — the rejoined node reconverges 2000/2000,
zero acked-write loss**, the scenario that previously failed. The live
run even caught a bug no unit test could: a security group that allowed
the ring port but not the new state-transfer port, silently dropping the
rejoiner's catch-up fetch.

---

## What's next

1. ~~Land the eight open PRs (#20–#27)~~ — and PR-RD-9 (#30), and the
   E1-v2 results (this PR). 10 PRs open total.
2. ~~Live-fire chaos retest~~ — **done.** See E1-v2 REPORT.
3. **Sentinel head-to-head**: drive Redis Sentinel under the same
   workload and fault, measure acked-write loss directly (we expect
   non-zero).
4. Scale: 5-node and 7-node runs, characterize MTTR vs N.
5. Throughput sweep: 10⁵–10⁶ writes, latency distribution, large-value
   behavior.

Once #3 lands, the paper's quantitative comparison story is complete.
The headline correctness claim is already validated.

---

*Repo: [`trains-rust`](https://github.com/yeychenne/trains-rust). The
EC2 chaos REPORT and per-engine partial reports are in
`bench/results/ec2-2026-05-26/`. The full plan + checkpoint trail for this
session is in `bench/EOD-2026-05-26.md`.*
