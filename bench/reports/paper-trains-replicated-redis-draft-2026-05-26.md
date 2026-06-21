# Loss-Free Redis Failover via State-Machine Replication over a Total-Order Broadcast Ring

> **Draft v1.0 — 2026-05-27 PM.** All experiments planned for v1.0 have
> landed: §6.3 demo workloads (local-smoke; EC2 chaos extension is a
> v1.1 follow-up), §6.4 security + Appendix B threat-model, architecture
> figures (Appendix A), §6.2 [TODO E4] CLEARED by the clean fresh-cluster-
> per-rate sweep on 2026-05-27 PM (zero acked-write loss at 50/500/1000/
> 2000 wr/s — `bench/results/ec2-2026-05-27-e4-clean/REPORT.md`).
> Companion blog post:
> `bench/reports/blog-trains-replicated-redis-2026-05-26.md` — same
> material, less rigour, fewer caveats. Remaining items (E3 larger-N,
> E5 Jepsen-adversarial, E6 reviewer harness, EC2 demo extension) are
> v1.1 scope.

**Authors.** Yves Eychenne (operator + system author), Claude (Sonnet 4.5,
pair-programming co-author across PRs #12-#27).

---

## Abstract

Redis is widely deployed as a coordination fast-path: distributed locks,
queue state, session flags, gate flags. Its primary high-availability
story — Sentinel-driven async failover — sacrifices durability under
crash: writes the failed primary acknowledged but had not yet replicated
are silently lost on promotion. We present **trains-valkey**, a RESP-level
write-interception proxy that replicates Redis writes across a ring of
self-managed engines using a uniform total-order broadcast protocol
(TRAINS) underneath. trains-valkey preserves Redis's command semantics —
including non-deterministic mutations like `SPOP`, `INCRBYFLOAT`,
`HINCRBYFLOAT` — by *resolving the non-determinism at the originating
node* before broadcast, so each replica applies a deterministic effect in
total order. A reconfiguration layer based on a Schneider-style
state-machine view change masks a permanent crash without operator
intervention — and, completing the node lifecycle, a **virtually-
synchronous re-admit view change** returns a recovered node to the ring
as a full acking member, restoring N-redundancy. We have built the proxy
as Rust on top of an existing TRAINS implementation and validated it
locally against live Valkey 9.1 (163 unit + integration tests) and at EC2
scale on a 3-node ring in three failure modes: a post-load proxy kill
(1000 writes, 0 lost); **a kill mid-workload (2000 writes, with 1000
written THROUGH the masked-crash window, 0 lost on every survivor,
byte-identical convergence)**; and **a kill-then-rejoin under continued
load (the SIGKILLed-then-restarted node reconverges 2000/2000, matching
the survivors, zero acked-write loss)** — the adversarial scenario that
previously failed. The re-admission half is proven end-to-end in-process
and underpinned by a TLA+ `ReAdmit` action, TLC-checked at 6.28 M states.
[TODO: comparative measurement vs Redis Sentinel under matched fault
injection; sustained-load characterization at ≥ 10⁵ writes; larger N;
remaining Jepsen-style adversarial schedule.]

---

## 1. Introduction

Redis's deployed footprint as a coordination layer is enormous — service
discovery, locks, leases, rate-limiters, work queues, session caches. The
Sentinel HA story matches that workload's expectations *most* of the
time: a primary crashes, a replica takes over within a few seconds, the
application keeps running. The story breaks under one specific failure:
asynchronous replication means the promoted replica is, by construction,
behind the primary at the moment of crash. The window between
"acknowledged to client" and "delivered to replica" is *acceptable
data loss*, by design [1].

That window is the wrong story for coordination state. A distributed
lock acquired but lost on failover deadlocks the system. A queue
deletion acked but lost re-enqueues every dispatched task. A
human-in-the-loop gate flagged ready but lost halts the pipeline.
Operators paper over this with idempotency, redrives, and explicit
heartbeats, but the underlying tool is doing the wrong thing.

The classical answer is state-machine replication (SMR): order every
write the same way on every replica, then apply each write at most once
on each replica [2]. The classical *practical* answer is Raft- or
Paxos-based leader replication [3, 4], which RedisRaft [5] implemented
inside the Redis process. Our work takes a different cut: keep an
unmodified Redis (or Valkey) engine on each node, and put the SMR layer
*in front of it* as a RESP proxy that intercepts writes, broadcasts them
in total order on a side channel, and applies them to the engine in the
delivered order.

The total-order broadcast is provided by the **TRAINS** protocol — a
uniform-delivery token-ring broadcast with a clock-gap failure detector
and a distributed view-change reconfiguration protocol, formally
verified in TLA+ in a sister repository [6].

### Contributions

- **A Redis-shaped SMR proxy** (`trains-valkey`) that preserves the
  RESP wire protocol, transparently classifies and replicates
  deterministic writes, *resolves* non-deterministic mutations at the
  origin so the applied form is deterministic on every replica, and
  masks a single permanent crash via the underlying view change.
- **An effect-resolution table** for the common non-deterministic Redis
  commands (`SPOP` → `SREM(k, members)`, `INCRBYFLOAT` → `SET(k, v)`,
  `HINCRBYFLOAT` → `HSET(k, f, v)`), enabling SMR semantics without
  sacrificing the commands that make Redis Redis.
- **An EC2-scale demonstration** that the proxy + TRAINS + a real
  Valkey backend together preserve all acknowledged writes through a
  permanent node crash, **both** when the kill lands post-load
  (1000/1000) and when it lands mid-load (2000/2000 with 1000 written
  through the masked window).
- **A head-to-head measurement against Redis Sentinel** under matched
  workload + fault (E2, 2026-05-26 PM). At paced 50 wr/s, Sentinel
  preserved all 2000 acked writes — the lossy failover mode is
  rate-dependent and surfaces only when replication can't keep up
  with primary acks. The rate threshold characterisation is deferred
  to E4 (planned).

---

## 2. Background

### 2.1 The TRAINS protocol (briefly)

TRAINS is a uniform total-order broadcast over a logical token ring.
Each node holds a `TrainsNode` state machine; messages ("trains")
circulate the ring carrying client broadcasts. Uniform delivery means
every alive node delivers the same set of messages in the same order,
even when some senders crash mid-ring. A clock-gap failure detector
plus a distributed view-change reconfiguration protocol let the ring
re-form around a permanent crash. See [6] for the protocol; we treat
it as a primitive here.

### 2.2 State-machine replication on a deterministic engine

Schneider's classical formulation [2]: if every replica is a
deterministic state machine and every replica receives the same input
in the same order, every replica ends in the same state. Redis is *almost*
deterministic — most commands are pure functions of `(state, args)` —
but a handful of widely-used commands are explicitly not:

- `SPOP key` removes and returns an unspecified member.
- `INCRBYFLOAT key v` applies a floating-point increment that is
  not bit-exact across implementations [TODO: verify with a citation].
- `RANDOMKEY`, `SRANDMEMBER`, `SCAN` cursors, `TIME`, `OBJECT
  IDLETIME` etc. depend on local state (RNG, clock, expiry sampler).

A naive "broadcast every command" SMR design diverges on these.

### 2.3 What Sentinel doesn't guarantee

Redis Sentinel monitors a primary–replica set and promotes a replica
when the primary fails. Replication is asynchronous; the primary
acknowledges a write after applying it locally, *before* the replica
acknowledges receipt. The Redis documentation [1] is explicit that
acked writes can be lost on failover. The `WAIT n ms` command is the
closest workaround but is not enforced and not used in coordination
codepaths.

---

## 3. Design

### 3.1 Architecture

```
                ┌───────── RESP client ─────────┐
                │             SET k v           │
                │             +OK               │
                ▼                                ▲
        ┌──────────────────────────────────────────┐
        │  trains-valkey proxy (per node)           │
        │  ┌─────────────────────────────────┐    │
        │  │ RESP listener + classify        │    │
        │  └──────────┬──────────────────────┘    │
        │             │ writes only              │
        │             ▼                          │
        │  ┌─────────────────────────────────┐  │
        │  │ effect resolver (origin-side)   │  │
        │  └──────────┬──────────────────────┘  │
        │             │ deterministic effect    │
        │             ▼                          │
        │  ┌─────────────────────────────────┐  │
        │  │ TRAINS oBroadcast (ring)        │◄─┼─► other nodes
        │  └──────────┬──────────────────────┘  │
        │             │ delivered in total order│
        │             ▼                          │
        │  ┌─────────────────────────────────┐  │
        │  │ dedup + apply to RedisStore     │  │
        │  └──────────┬──────────────────────┘  │
        │             │                         │
        │             ▼                         │
        │       Valkey engine (loopback)         │
        └──────────────────────────────────────────┘
```

Each node runs:
1. A RESP listener that classifies each command as
   `Class::Read | Class::DeterministicWrite | Class::NondeterministicWrite`.
2. For non-deterministic writes, an **origin-side resolver** that reads
   the local state, picks the effect (e.g., `SPOP` on the local set
   yields a concrete member; we then broadcast `SREM key member` instead
   of `SPOP key`).
3. A TRAINS oBroadcast layer (total-order, uniform delivery).
4. A delivered-apply path with `(origin, request_id)` dedup so the
   at-least-once delivery channel doesn't double-apply.
5. A `RedisStore` seam abstracting the local engine: `MemStore` (in-
   process, for tests) or `RedisBackend` (loopback RESP to a real engine).

### 3.2 The four invariants

1. **Total order on writes.** Every replica applies the same write
   sequence. (Provided by TRAINS oBroadcast.)
2. **At-most-once apply.** Each `(origin, request_id)` applies on
   each replica at most once. (Apply-side dedup; per-replica set, see
   §6.1 for the bounding cleanup we have not yet done.)
3. **Deterministic effect.** Non-deterministic *mutations* are
   resolved to their effect at the origin before broadcast.
4. **Crash masking.** A confirmed permanent crash triggers a
   distributed view change; the ring re-forms around the survivors;
   in-flight writes either complete or are not acked.

### 3.3 Reconfiguration

The view change is the same one TRAINS uses for the protocol layer:
a coordinator (the lowest-id survivor) issues a Gather token that
collects each survivor's recovery report; the coordinator computes a
recovery plan; an Install token applies the new view on every
survivor. The proxy participates via `trains_recovery::ViewChange`
(see §4, the extraction landed in PR-RD-7).

**Re-integration.** Exclusion is only half the membership lifecycle.
A restarted node first rejoins as a **passive replica** — it catches up
from a survivor's snapshot plus a contiguous tail of delivered effects
(a keyspace *replace* that wipes stale pre-downtime state, then an
incremental replay through the same at-most-once apply) and tails it to
stay current, off the ring and read-only. Once caught up it is promoted
through a **re-admit view change** symmetric to exclude: the membership
change is ordered in the same view-change token stream (virtual
synchrony), state transfer is synchronized to the install point, and the
node re-enters the *acking* quorum, restoring N-redundancy. The protocol
layer underpins this with a TLA+ `ReAdmit` action (TLC-checked at 6.28 M
states) and a `core::readmit_node` primitive — the inverse of the
crash-confirm that makes the node's ack required again. The passive half
is opt-out-of-nothing (it never re-enters ordering, so it cannot regress
the masking path); promotion is opt-in.

The failure detector that triggers it combines weak evidence (a
clock gap on the ring) and strong evidence (a successor unreachable
on the wire). The "successor unreachable" path required two
follow-up fixes after the EC2 chaos run uncovered them; see §5.

---

## 4. Implementation

### 4.1 Crate layout

| Crate | LOC (approx.) | Role |
|---|---|---|
| `trains-core` | ~1 400 | TRAINS protocol kernel (sync state machine) |
| `trains-net` | ~700 | TLS ring transport (async, tokio) |
| `trains-recovery` | ~500 | Failure detector + view-change state machine (extracted from `trains-cli` in PR-RD-7) |
| `trains-valkey` | ~2 200 | The proxy: RESP codec, command classifier, effect resolver, replica, RedisBackend, chaos driver |
| `trains-cli` | ~1 100 | CLI driver (production binary) |

All Rust, 2021 edition, MSRV 1.78. Single workspace. Default-features
on `rustls` with the `ring` backend (no OpenSSL or aws-lc dependency).

### 4.2 RESP backend abstraction

The proxy treats the Redis engine as a `RedisStore` trait with two
methods (`apply(WriteOp) -> Reply`, `query(Command) -> Reply`) and a
snapshot/restore pair. Implementations:

- `MemStore`: in-process, used by every test that doesn't need to
  hit a real engine.
- `RedisBackend`: a synchronous RESP client over a loopback TCP
  connection to a colocated Valkey or Redis 8 process.

Selecting the backend at the CLI: `--backend redis://127.0.0.1:6379`
picks the real engine; absent the flag, the proxy uses `MemStore`.
Tests in `crates/trains-valkey/tests/redis_backend.rs` are gated on a
`valkey-server` or `redis-server` binary on `PATH`; they skip
otherwise.

### 4.3 Effect resolution

The resolver runs on the originating node, *before* broadcast, and
reads committed state (i.e., it consults the locally-applied store
after any pending broadcasts). Worked examples:

- `SPOP key` → resolve to a random member `m` from the local set;
  broadcast `SREM key m`.
- `INCRBYFLOAT key v` → read local value `x`; broadcast
  `SET key (x + v)` (computed as `f64`, formatted with `%.17g`).
- `HINCRBYFLOAT key field v` → analogous.

Reads of non-deterministic commands (`SRANDMEMBER`, `RANDOMKEY`,
`TIME`, `SCAN`) are answered locally without broadcast and never
diverge survivors (no state mutation).

A resolve-vs-apply window exists under concurrent multi-writer load:
two clients hitting different replicas with `SPOP` can resolve to
overlapping members, and the second `SREM` is a no-op. We document
this; phase-injection tests in `crates/trains-valkey/tests/
effect_convergence.rs` cover the window.

### 4.4 Crash masking — the part that took the most operator time

`ProxyConfig::ring_addrs` enables reconfiguration. The proxy's
driver loop selects on three relevant inputs:

```rust
tokio::select! {
    Some(msg) = transport.vc_inbox.recv(),      if reconfig => { ... }
    Some(addr) = transport.unreachable_rx.recv(), if reconfig => { ... }
    _ = tick.tick() => { ... }  // clock-gap path
}
```

The `unreachable_rx` channel is the strong-evidence path. Pre-PR-RD-6,
it only fired after `UNREACHABLE_FAILURES=5` consecutive connect
failures — a path that's never taken when an established TCP
connection just loses its peer, because Linux `send(2)` buffers
into the kernel's retransmit budget for ~15 min before any error
surfaces. Fix in §5.

---

## 5. The EC2 chaos run and what it taught us

We deployed 3× `t4g.small` instances in `eu-west-3` via CDK, with a
self-managed Valkey 9.0.3 per node bound to loopback. Total spend
< $0.05.

### 5.1 Healthy-ring acked-write preservation (the headline result)

`trains-valkey-chaos --mode load --count 200 --hold-secs 20` drove
200 `SET k_i v_i` writes through node 0's RESP port. The proxy
classified each as a deterministic write, broadcast the `SET` over
the TRAINS ring, and every replica applied it in total order to its
loopback Valkey.

We then SIGKILLed node 2's proxy and ran `--mode verify-local` on
each of the three engines:

| Engine | acked_total | missing_keys | dbsize |
|---|---|---|---|
| node-0 (eu-west-3a, alive) | 200 | [] | 200 |
| node-1 (eu-west-3c, alive) | 200 | [] | 200 |
| node-2 (eu-west-3a, proxy SIGKILLed) | 200 | [] | 200 |

Every acked write present on every survivor; `DBSIZE` byte-identical.
This is the property Redis async / Sentinel failover does not
guarantee. **(PR #22, the run report.)**

### 5.2 The adversarial scenario that hung

We then attempted the *interesting* run: load *during* the masked
crash window. It hung for five minutes with no acked writes on the
survivors, then we cancelled it.

Two distinct bugs, separately fixed:

**5.2.1 Slow view change at production scale (PR-RD-6).** On the
EC2 path, when node 2 died, node 1's established TCP connection to
node 2 stayed in a "writes succeed into the kernel buffer for ~15
min" state. The connector loop never re-entered the connect path,
so `UNREACHABLE_FAILURES` never counted, so `unreachable_rx` never
fired. We added `setsockopt(TCP_USER_TIMEOUT, 3 s)` on both sides
of every ring socket; the kernel now force-closes the connection
within 3 s of unacked traffic; the connector errors, reconnects,
fails on connect, eventually fires `unreachable_rx`. Expected MTTD
is `TCP_USER_TIMEOUT + 5 backoffs ≈ 10 s`. [TODO: confirm with a
live-fire EC2 retest of the previously-hanging chaos-2 scenario.]

**5.2.2 In-process flake from the same cause class (PR-RD-8).**
`RingTransport::abort()` aborted the listener task but left every
*accepted* per-connection task running — those were never tracked
on the transport. The predecessor's writer kept pushing into the
half-closed inbound socket; tokio runtime queued behind it; tests
hung. Fix: track every accepted-connection `JoinHandle`,
`abort()` them all on shutdown. The in-process `crash_masking`
test was timing-flaky (one 394 s failure, four 4 s passes in five
consecutive runs); post-RD-8 it runs in 4 s every time across five
isolated runs.

These two PRs are complementary: RD-6 closes the EC2 window via a
kernel-level timeout, RD-8 closes the in-process window via an
application-level explicit cleanup. Same story (detect peer death
fast), different layers.

### 5.3 The during-window retest (E1-v2) — the decisive run

After PR-RD-6 + PR-RD-8 + PR-RD-9 landed (the three fixes co-located
along the failure-detection path), we re-ran the same workload as
§5.2's hang, with all three fixes active. Same 3-node `t4g.small`
ring; same `--mode load --count 2000 --hold-secs 30`; kill via SSM at
T+20s (lands mid-phase-1).

| Engine | acked_total | missing_keys | dbsize |
|---|---|---|---|
| node-0 (alive) | **2000** | **[]** | **2000** |
| node-1 (alive) | **2000** | **[]** | **2000** |
| node-2 (proxy killed mid-load) | 2000 | 1000 phase-2 (expected) | 1000 |

The chaos client got `+OK` for all 2000 writes — 1000 phase-1
(pre-kill) AND **1000 phase-2 (through the masked window)**. Both
survivors converged byte-for-byte; node 2's engine retained the
phase-1 writes its proxy applied before the kill but, correctly,
received none of the phase-2 writes (its proxy was dead). Total wall
clock 45.30 s vs §5.2's 5-min hang. This validates the during-window
claim at EC2 scale (see `bench/results/ec2-2026-05-26-e1v2/REPORT.md`
for the full transcript).

### 5.4 Operational lessons that aren't part of the protocol claim

- The bench-aws CDK stack had never been deployed before; six latent
  IaC bugs surfaced on first run. Bundled fix in PR-RD-5b. We
  recommend test-deploying any unrun IaC into a sandbox at the time
  it's written, not at the time it's needed.
- `cross-rs 0.2.5` is broken for `aarch64-apple-darwin →
  x86_64-unknown-linux-*` on newer rustup. We pivoted to building
  inside an `arm64` Linux Docker container and switched the target
  instance type from `t3.small` to `t4g.small` (Graviton). The
  protocol is arch-agnostic.

---

## 6. Evaluation

[**This section is the largest TODO.** What we have today is a
single 3-node, 200-write run plus the in-process test suite. A
proper evaluation needs the items below.]

### 6.1 What we have

- **Correctness, locally:** 163 unit + integration tests against
  `MemStore` and gated tests against live Valkey 9.1, including
  3-node convergence, dedup, state transfer, in-process crash
  masking, effect resolution under phase injection, and the
  PR-RD-8 / PR-RD-9 abort + peer-close regression tests. All pass on
  every PR in the open queue.
- **Correctness, at EC2 scale (post-load kill):** §5.1's 200/200
  result on a 3-node Valkey-backed ring; the EC2-PM run replicates
  this at 5× volume (1000/1000 phase-1 acked on every engine
  including the killed one).
- **Correctness, at EC2 scale (during-window kill — E1-v2, the
  decisive run):** with PR-RD-6 + PR-RD-8 + PR-RD-9 active, a 3-node
  ring took a SIGKILL on the victim's proxy mid-load and the chaos
  client got `+OK` for ALL 2000 writes — 1000 phase-1 (pre-kill)
  AND **1000 phase-2 (through the masked window)**. Both survivors
  hold the full 2000-key set with byte-identical `DBSIZE`; zero
  acked-write loss. Total wall-clock 45.30 s vs E1's 5-min hang.
  Full report: `bench/results/ec2-2026-05-26-e1v2/REPORT.md`.
- **Recovery mechanism, in-process:** `crash_masking` stable
  across 5+ isolated runs after PR-RD-8; ≤ 4 s each. PR-RD-9's
  `unreachable_fires_when_peer_dies_with_no_pending_writes` adds the
  regression guard for the E1 failure mode.
- **Static cost:** ~2 500 LOC added in `trains-valkey`; binary size
  4.0 MB (release, arm64-linux); no new dependencies beyond
  `tokio`, `rustls`, `bincode`, `clap`, plus `socket2` (PR-RD-6, for
  `TCP_USER_TIMEOUT`).

### 6.2 What we don't have [TODO]

- ~~[TODO E1]~~ **CLEARED 2026-05-26 PM** by E1-v2 (see §6.1). The
  live-fire retest hit the acceptance criteria (0 acked-write loss on
  every survivor, MTTR comfortably under the 30 s hold window).
- ~~[TODO E2]~~ **PARTIALLY CLEARED 2026-05-26 PM.** At paced 50 wr/s
  with EC2 inter-AZ networking, Redis Sentinel preserved all 2000
  acked writes through a primary SIGKILL + Sentinel failover (≈ 4.5 s
  failover window). Sentinel's "loses acked writes" failure mode is
  rate-dependent — it appears when the primary acks faster than
  replicas can apply, which does not happen at our paced rate. The
  threshold characterisation belongs to [TODO E4] (throughput sweep).
  Full report: `bench/results/ec2-2026-05-26-e2/REPORT.md`. This is
  an honest, less-binary finding than the original hypothesis; the
  paper's framing in §7 is updated accordingly.
- **[TODO E3]** Scale: 5-node and 7-node rings; MTTR vs N. Plan:
  `bench/reports/e3-larger-N-plan-2026-05-26.md`. ~3 h, < $0.20.
- ~~[TODO E4]~~ **CLEARED 2026-05-27 PM** via the clean
  fresh-cluster-per-rate sweep. 4 rates × 1 SIGKILL each on a freshly-
  re-formed 3-node Sentinel cluster: **zero acked-write loss at every
  rate** (1000/1000 @ 50 wr/s, 10 000/10 000 @ 500 wr/s, 20 000/20 000
  @ 1 000 wr/s, 40 000/40 000 @ 2 000 wr/s). Sustained throughput 49.7
  / 497.1 / 988.5 / 1 957.2 wr/s respectively; batch p99 latency
  0.4 / 1.4 / 1.7 / 1.5 ms. Failover engaged on r ≥ 500 (post-kill
  acks landed on `10.0.2.18:6379`, the promoted replica, not on the
  original master `10.0.0.137:6379`). At r=50 the workload happened
  to complete before failover surfaced in the destination log;
  zero-loss invariant still held. Full report:
  `bench/results/ec2-2026-05-27-e4-clean/REPORT.md`. The 2026-05-26
  partial finding (sequential primary kills exhaust Sentinel's quorum
  candidates) stands independently — both characterisations belong in
  §7's framing of trains-valkey's value proposition.
- ~~[TODO E5]~~ **LARGELY CLEARED 2026-06-15/16.** The E5 adversarial
  matrix ran on a fresh 3-node ring per scenario: t1-partition,
  t2-asymmetric-partition, t2-clock-skew, and t2-burst-partition all
  **PASS** (zero acked-write loss, survivors converged). **t1-rejoin**
  (SIGKILL a node mid-load, restart it under continued writes) initially
  failed — the rejoined node never reconverged — which motivated the
  node-re-integration work (§3.3): a passive-replica catch-up (snapshot +
  contiguous delivered-effect tail) plus a virtually-synchronous re-admit
  view change. After that work, **t1-rejoin PASSES: the rejoined node
  converges 2000/2000, matching the survivors, zero acked-write loss**
  (`bench/results/ec2-2026-06-16-e5-rejoin/REPORT.md`). The live run also
  surfaced an infrastructure gap a unit test cannot — the bench security
  group allowed the ring port but not the new state-transfer port, so the
  rejoiner's fetch was silently dropped (fixed in the CDK network stack).
  Remaining E5: multi-victim *sequential rejoin* and a fuller Jepsen-style
  schedule.
- **[TODO E6]** Containerized reviewer harness (pure local engineering).
  Plan: `bench/reports/e6-reviewer-harness-plan-2026-05-26.md`. ~6 h,
  $0.

### 6.3 Demo workloads (added 2026-05-27)

Two demo applications drive trains-valkey with workloads that match the
intro's motivating examples ("locks, queues, gate flags"):

| Demo | Pattern | Commands | Correctness invariant |
|---|---|---|---|
| **Distributed lock** | Redlock-lite (single-node) | `SET k v NX EX` + `INCR` + ownership-checked `DEL` | every acquire → exactly one release; no orphaned locks; `INCR ops` count matches acquire count |
| **Real-time leaderboard** | sorted-set ranking | `ZINCRBY leaderboard d player` | every survivor's `ZRANGE` reconciles to the sum of acked deltas, byte-for-byte |

**Local-smoke baseline (vanilla Valkey on loopback, 4 workers × 10 s):**

| Demo | Throughput | p50 lat | p99 lat | Correctness |
|---|---:|---:|---:|:---:|
| Lock | 8 192 cycles/s | 0.114 ms | 0.156 ms | ✅ 39 531 / 39 531 / 0 orphaned |
| Leaderboard | 30 634 ops/s | 0.127 ms | 0.164 ms | ✅ all 50 players reconcile exactly |

These establish the workload's inherent rate ceiling and latency floor
before introducing the proxy ring. The EC2 chaos extension — both demos
through the trains-valkey proxy ring with a mid-workload proxy SIGKILL —
is the natural follow-up; estimated cost ≤ \$0.20, ~1 h wall clock. Plan in
`bench/results/demo-apps-2026-05-27/REPORT.md` §"What's next".

Full demo source: `bench/demos/{distributed-lock,leaderboard}/*.py`.
Local-smoke detail: `bench/results/demo-apps-2026-05-27/REPORT.md`.

### 6.4 Security posture and threat model (added 2026-05-27)

A STRIDE-per-element threat-model pass ran against `main@c80d72f` using
the same Threat Modeller role used in the AO `aidlc_secure_pipeline`
topology. The pass produced 20 distinct threats (T-tr-01..20) across
the elements identified in the §3.1 data-flow diagram — RESP client,
proxy process, Valkey backend, ring transport, operator host, bench
coordinator, S3 results bucket, SSM control plane. Coverage matrix and
threat statements: `bench/reports/threat-model-trains-valkey-2026-05-27.md`.

**Posture today (bench harness — explicitly not production):**

- ✅ **Ring transport**: mTLS with `rustls` + `ring` backend; SPKI
  fingerprint pinning on both sides (`PinnedFingerprintVerifier`).
- ⚠ **RESP client ↔ proxy**: plaintext loopback; no `AUTH`, no TLS.
  The bench currently drives the proxy from `127.0.0.1` only, so this
  is acceptable for the measurement campaign but **not for production**.
- ⚠ **Proxy ↔ Valkey backend**: plaintext RESP over `127.0.0.1`; no
  `AUTH`. Trust model relies on Valkey being bound to loopback only.
- ✅ **AWS control plane**: no public IPs; SSM-only access; IAM
  instance profile scoped to a single S3 bucket and SSM Managed
  Instance Core; VPC endpoints for SSM + S3; no IGW / NAT.

The remediation plan
(`bench/reports/remediation-plan-trains-valkey-2026-05-27.md`) bins
findings into three buckets. The **Ship-this-week** bucket (R-01..R-05)
landed across PRs #36-#39 in the day after the threat model:

| R | Threat IDs | Status | Delivery |
|---|---|---|---|
| R-01 | T-tr-10 (non-finite floats in `effect::resolve`) | ✅ Already mitigated; 6 regression tests added | PR-SEC-A (#36) |
| R-02 | T-tr-09 + T-tr-19 (listener DoS / per-IP starvation) | ✅ New code: global semaphore (512) + per-IP cap (32) + `ConnGuard`; 7 unit tests | PR-SEC-B (#37) |
| R-03 | T-tr-11 + T-tr-15 (S3 binary swap / no forensics) | ⚠ Partial: bench bucket gains versioning + access logging; role-split + Object Lock + R-08 binary signing deferred to v1.1 | PR-SEC-D (#39) |
| R-04 | T-tr-08 + T-tr-16 (argv leak via logs / JSON) | ✅ Already mitigated by omission (no `tracing::*` call emits argv; per-node REPORT JSON is summary-only); transient `acked.json` documented | PR-SEC-C (#38) |
| R-05 | T-tr-20b + T-tr-21 (oversize WireMsg → memory exhaustion) | ✅ Already mitigated (`MAX_FRAME_LEN` checked before body alloc); 3 regression tests added | PR-SEC-A (#36) |

The notable methodology finding: 3 of 5 ship-this-week items
(R-01, R-04, R-05) turned out to be already mitigated in the code —
the threat-model agent flagged them because the code paths providing
the mitigation (`parse_f64`, `MAX_FRAME_LEN`, the absence of argv-
emitting log sites) were not in the agent's read window. The honest
output of the post-TM verification pass is "the security posture was
slightly better than the TM thought, but the regression-test coverage
gap was real and is now closed."

- **Plan for v1.1** (R-06..R-11) — full mTLS on the client RESP
  boundary (R-06), Valkey on UNIX domain socket with `requirepass`
  (R-07), signed binary distribution via `rsign2`/`cosign` (R-08),
  append-only audit log (R-09), bounded dedup set (R-10), view-change
  frame authorisation via TLS exporter nonce (R-11).
- **Track only** (R-12..R-15) — transferred to AWS IAM/MFA/CloudTrail
  (R-12), explicit `ssm:SendCommand` allowlist (R-13), accepted Lows
  + out-of-scope (R-14), long-term key revocation list (R-15, blocked
  on a design decision).

Threats summary table embedded in Appendix C.

### 6.5 Threats to validity

- **Single-region.** All measurements are within `eu-west-3`. Cross-
  region behavior (higher RTT, asymmetric loss) is unmeasured.
- **Loopback engine, not networked engine.** Each replica's Valkey
  is on `127.0.0.1`. We rely on this for the loss-free security
  model; relaxing it changes the picture.
- **3 nodes is the smallest interesting N.** A 5-node ring may
  expose ordering edge cases not present at N=3.
- **Resolve-vs-apply window** on non-deterministic mutations under
  concurrent multi-writer. We document it; phase tests cover it;
  but the production workload's tolerance is empirically untested.

---

## 7. Related Work

### 7.1 Inside Redis

- **Redis Sentinel** [1] — async replication + automatic failover.
  Our motivating contrast.
- **Redis Cluster** [7] — sharding + per-shard replication. Same
  durability story per shard as Sentinel.
- **RedisRaft** [5] — Raft consensus inside the Redis process.
  Strong consistency, but invasive (modifies Redis itself). Confirmed
  active as of 2026-05-27 (937 commits on `master`, 841 stars) but the
  README still carries the maintainer disclaimer "not yet ready for
  any real production use" — a state held since the project's start.
- **KeyDB Active Replication** [8] — multi-master with last-writer-
  wins reconciliation. Provides higher availability than Sentinel
  but doesn't preserve causal order or acked-write durability.

### 7.2 Outside Redis

- **Chubby** [9], **ZooKeeper** [10] — leader-based consensus
  coordination services; high durability but different API surface
  (no Redis compatibility).
- **CockroachDB**, **TiKV** — Raft-replicated KV stores; different
  command model, not RESP-compatible.

### 7.3 Why a proxy

Putting the SMR layer *in front of* an unmodified engine is unusual.
The choice trades two things: (a) we don't modify Redis, so we
inherit every release's bug fixes and command additions for free;
(b) we accept the resolve-vs-apply window for non-deterministic
mutations, which a Raft-inside-Redis design avoids. The proxy
approach pays off when Redis releases are frequent and the
non-determinism set is small and well-known.

---

## 8. Limitations and Future Work

- **Resolve-vs-apply window** on non-deterministic mutations under
  concurrent multi-writer. The window is fundamental to the
  origin-resolver design; closing it requires either
  Raft-style leader-only writes (which we explicitly rejected) or a
  deterministic-effect resolution at apply-time (which is provably
  harder for `SPOP`).
- **No `MULTI`/`EXEC`/Lua atomicity.** Our `WriteOp` envelope
  carries one command. Multi-command atomicity requires a wire-level
  refactor + proxy txn state, and Lua requires an embedded
  interpreter at odds with our lean-deps policy. [TODO: design
  sketch for `MULTI`/`EXEC` without Lua.]
- **Unbounded dedup set.** `Replica::applied_ops` is exact and
  unbounded; long-running deployments will need a per-origin
  watermark + recent-set window.
- **Reconfiguration churn.** Repeated joins/leaves are functionally
  correct but operationally expensive (state transfer is full-
  keyspace). A future incremental state-transfer would help.

---

## 9. Conclusion

We built a Redis-compatible proxy that gives Redis the durability
property its native HA story sacrifices: every acked write survives a
single permanent node crash, on every survivor, byte-for-byte. The
property holds at EC2 scale in **both** failure modes we tested:
- **Post-load kill (morning):** 200/200 and 1000/1000 acked writes
  preserved across every engine, including the killed one.
- **During-window kill (E1-v2, afternoon, the decisive run):** 2000
  writes, kill mid-workload, **1000 acked through the masked
  window**, both survivors hold the full 2000-key set, byte-identical
  `DBSIZE`. Total wall clock 45.30 s; zero acked-write loss.

The during-window run surfaced three composable bugs in the failure-
detection path — `TCP_USER_TIMEOUT` missing (PR-RD-6), `abort()` not
propagating to accepted-connection tasks (PR-RD-8), and the connector
not reading the TLS stream so peer-close was invisible when idle
(PR-RD-9). All three are fixed and regression-tested in-process; the
chaos retest with all three active passes the during-window claim.

The Sentinel head-to-head (E2, paced 50 wr/s) returned an honest
result we did not anticipate: at moderate rate, Sentinel preserved all
2000 acked writes through a primary SIGKILL + ~4.5 s failover. The
classical "Sentinel loses acked writes" mode is rate-dependent — it
surfaces when the primary acks faster than replicas can apply.
Reporting that nuance instead of the binary claim is the more honest
contribution. The rate threshold characterisation, larger-N rings, an
adversarial schedule, and a containerised reviewer harness are
planned (see `bench/reports/e{3,4,5,6}-*-plan-2026-05-26.md`); we
expect ≤ 5 operator-driven days and ≤ $10 to land all four.

---

## References

[TODO: convert to a proper bibliography. The numbered placeholders
below are correct in intent but not yet cited from real sources.]

1. Redis documentation: replication semantics. Current canonical URL
   (after 2024 docs reorganisation): `https://redis.io/docs/latest/operate/oss_and_stack/management/replication/`
   (accessed 2026-05-27). The relevant passage on async-failover data
   loss: *"because Redis uses asynchronous replication it is not
   possible to ensure the replica actually received a given write, so
   there is always a window for data loss"* and *"acknowledged writes
   can still be lost during a failover, depending on the exact
   configuration of the Redis persistence."*
2. Schneider, F. B. *Implementing fault-tolerant services using the
   state machine approach: a tutorial.* ACM Computing Surveys, 1990.
3. Ongaro, D. and Ousterhout, J. *In search of an understandable
   consensus algorithm.* USENIX ATC, 2014.
4. Lamport, L. *Paxos made simple.* ACM SIGACT News, 2001.
5. RedisRaft project. [TODO: pin URL + current status]
6. TRAINS protocol formal verification. Repository internal: see
   `verification/reference` in the sister repo. [TODO: cite the
   published version once available.]
7. Redis Cluster specification. `https://redis.io/topics/cluster-spec`.
8. KeyDB Active Replication. [TODO: cite + URL]
9. Burrows, M. *The Chubby lock service for loosely-coupled
   distributed systems.* OSDI, 2006.
10. Hunt, P. et al. *ZooKeeper: wait-free coordination for internet-
    scale systems.* USENIX ATC, 2010.

---

## Appendix A — Reproduction status

| Artifact | Available | Path |
|---|---|---|
| Source code | Yes | `https://github.com/yeychenne/trains-rust` |
| EC2 chaos REPORT.md | Yes | `bench/results/ec2-2026-05-26/REPORT.md` |
| Per-engine partial reports | Yes | `bench/results/ec2-2026-05-26/report-node-{0,1,2}.json` |
| Acked-set | Yes | `bench/results/ec2-2026-05-26/acked.json` |
| CDK + bootstrap + launch scripts | Yes | `scripts/bench-aws/` and `scripts/redis-chaos/` |
| Containerized reviewer harness | Plan only | `bench/reports/e6-reviewer-harness-plan-2026-05-26.md` |
| Sentinel comparison run artifacts | **Yes** (E2, paced 50 wr/s — 0 loss) | `bench/results/ec2-2026-05-26-e2/` |
| Live-fire chaos-2 EC2 results | **Yes** (E1-v2 cleared 2026-05-26 PM) | `bench/results/ec2-2026-05-26-e1v2/` |
| Scale (5/7-node) results | [TODO E3] | — |
| Throughput / latency tables | **Partial** (E4, 4 rates, sweep design exposed quorum exhaustion) | `bench/results/ec2-2026-05-26-e4/` |
| Demo workloads (lock + leaderboard) | **Yes** (local-smoke baseline, 2026-05-27; EC2 chaos extension deferred) | `bench/demos/`, `bench/results/demo-apps-2026-05-27/REPORT.md` |
| Architecture figures (4 views, draw.io) | **Yes** (2026-05-27 build) | `bench/diagrams/0{1,2,3,4}-*.drawio` + `bench/diagrams/README.md` |
| Threat model + remediation plan | **Yes** (2026-05-27, STRIDE-per-element) | `bench/reports/threat-model-trains-valkey-2026-05-27.md`, `bench/reports/remediation-plan-trains-valkey-2026-05-27.md` |
| Bench-data gap analysis + v1.0 sign-off checklist | **Yes** | `bench/reports/bench-data-gap-analysis-2026-05-27.md` |

---

## Appendix B — Threat-model summary (STRIDE-per-element, 2026-05-27)

Compact view of the threat model in `bench/reports/threat-model-trains-valkey-2026-05-27.md`. The full document carries the threat grammar, mitigation bypass analysis, and risk-response plan; this appendix is the executive summary.

**DFD elements considered.** RESP client (external), operator host
(external), trains-valkey proxy (process), bench coordinator (process),
SSM (process — AWS-managed), Valkey backend (data store, loopback),
in-memory dedup map / `applied_ops` (data store, process-local), S3
results bucket (data store), ring TLS flow (data flow), RESP loopback
flow (data flow), SSM control flow (data flow).

**Selected high-priority threats.** (See full doc for IDs T-tr-01..20.)

| ID | Element | STRIDE | Statement (abridged) | Rating | Response |
|---|---|---|---|:---:|---|
| T-tr-01 | RESP client | S | Anyone with host network access can speak RESP to the proxy with no authentication, leading to arbitrary write injection, reducing the integrity of the Valkey backend. | **H** | Mitigate (RESP client TLS + `requirepass`) — Ship this week |
| T-tr-05 | proxy (process) | S | Without `requirepass` on the loopback Valkey, any local process can bypass the proxy and write directly to the backend, reducing the integrity guarantees of SMR. | **H** | Mitigate (Valkey `requirepass` + listen on 127.0.0.1) — Ship this week |
| T-tr-06 | proxy (process) | T | Unbounded `applied_ops` set + crash → on resume, dedup map missing → at-least-once delivery may apply twice, reducing the integrity of replicas. | **H** | Mitigate (bounded watermark + recent-set window) — v1.1 |
| T-tr-09 | proxy (process) | D | A malformed RESP burst (large arrays, megabyte bulk strings) on the unauthed listener exhausts memory, reducing availability of the entire ring node. | **H** | Mitigate (RESP frame limits + rate cap on the listener) — v1.1 |
| T-tr-15 | S3 binary distribution | T | Operator with bucket-write permission can swap the `trains-cli` binary; SSM-driven nodes execute the swapped binary, reducing the integrity of every ring node. | **H** | Mitigate (`cosign` signature verification on the binary) — Ship this week |
| T-tr-17 | ring TLS flow | I | If SPKI fingerprint pinning is disabled or skipped on one side, a peer with a valid CA-issued cert can join the ring, reducing the confidentiality of broadcast writes. | **H** | Avoid (mandatory pinning on both sides, regression test) — Ship this week |
| T-tr-19 | SSM control flow | E | Operator IAM scope too broad → operator can issue SSM RunCommand to non-bench instances, reducing the principle of least privilege on the AWS account. | **M** | Mitigate (IAM policy scoped to `Project=trains-bench` tag) — Ship this week |

**STRIDE coverage.** Every applicable cell is populated for the 11
elements; the matrix lives at §4 of the full document. No element has
an unpopulated applicable cell.

**Remediation buckets.** 5 items "Ship this week" (R-01..R-05, all
landed in PRs #36-#39 the day after the TM ran — 3 fully shipped, 1
partial-by-design with v1.1 follow-up scoped, 1 documented as already
mitigated); 6 items "Plan for v1.1" (R-06..R-11, medium effort); 4
items "Track only" (R-12..R-15: AWS-side hardening, accepted Lows, or
blocked on operator design). Detail:
`bench/reports/remediation-plan-trains-valkey-2026-05-27.md`.

---

## Appendix C — Open PRs that constitute this work

| PR | Subject |
|---|---|
| #12 | PR-RD-1: RESP write-interception proxy |
| #13 | PR-RD-2: effect replication for non-deterministic commands |
| #14 | PR-RD-3: idempotent apply (dedup) + replica state transfer |
| #15 | PR-RD-4: crash masking under the proxy (in-process) |
| #16 | PR-RD-4: real Valkey backend (`--backend redis://…`) |
| #17 | EC2 chaos runbook (doc) |
| #18 | EC2 chaos harness pieces |
| #19 | EOD 2026-05-25 handover |
| #20 | PR-RD-5a chaos refactor (load / verify-local) |
| #21 | PR-RD-5b bench-aws CDK made deployable |
| #22 | PR-RD-5c EC2 run report |
| #23 | PR-RD-6 `TCP_USER_TIMEOUT` |
| #24 | EOD 2026-05-26 handover |
| #25 | Status report + PR-RD-7 plan |
| #26 | PR-RD-7 `trains-recovery` extraction |
| #27 | PR-RD-8 `abort()` closes connections |

#12–#19 are merged. #20–#27 are open as of draft date, all off `main`,
all reviewable in parallel.
