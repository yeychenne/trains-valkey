# ADR-002: TRAINS + Arctic Assurance Boundary

Status: correctness gates 1-5 complete; scoped performance exploration approved

Date: 2026-07-25

## Decision

Continue the open-source TRAINS + Arctic experiment through correctness gates
1-5, then make an explicit investment decision before changing the production
data path or claiming performance benefits.

Use these architectural terms:

- **TRAINS ordering and replication plane**: assigns and delivers the write
  order, masks failures, changes membership, and controls readmission.
- **Arctic materialized-state plane**: applies delivered deterministic effects
  and serves local point and ordered-index operations.
- **Operational control plane**: configures identities, membership, placement,
  recovery sources, and future shard management.

TRAINS is not merely a control plane because it participates in every write.

## Scope

This experiment is a single-region, three-node replicated key-value system. It
uses unmodified Valkey as a compatibility oracle and Arctic as an independently
developed open-source materialization engine.

The current command contract is `SET`, `GET`, `DEL`, `EXISTS`, and `DBSIZE`.
Keys are NUL-free. Transactions, Lua, the full Valkey command surface,
multi-region replication, clock-based snapshots, sharding, and cross-shard
atomicity are out of scope.

## Service Contract

### Writes and acknowledgements

1. A supported mutation is converted to a deterministic `WriteOp` before it is
   broadcast.
2. Every installed-view member applies delivered effects in TRAINS order.
3. `(origin, request_id)` deduplication makes overlapping delivery and catch-up
   idempotent.
4. The originating proxy replies only after that operation is delivered and
   applied locally.
5. Under the TRAINS failure assumptions, an acknowledged write remains in the
   ordered history available to surviving members of the installed view.

This does not promise acknowledgement durability after simultaneous loss of all
replica protocol state or all storage copies.

### Reads

Reads are currently served immediately from the contacted local store.

- A read from the same node after its write acknowledgement observes that
  write, subject to normal command semantics.
- Reads after an explicit quiescence/barrier may be compared across replicas.
- A read sent to an arbitrary different node is **not currently claimed to be
  linearizable** with a just-completed write.

A read-index, ordered read barrier, lease, or MVCC completeness watermark would
be required before claiming cross-node linearizable reads.

### Crash, recovery, and readmission

1. A confirmed crash installs a reduced view before survivors continue under
   the new membership.
2. A recovering member stays passive and rejects writes.
3. A full snapshot replaces stale state; it is never merged with stale state.
4. The snapshot carries delivered index `X`; catch-up applies the contiguous
   delivered-effect tail after `X` through the same deduplication path used by
   live delivery.
5. Promotion/readmission occurs only after a final survivor snapshot and tail
   establish the recovered member's base state and installed view.
6. After readmission, writes originating at both a survivor and the recovered
   member must converge on the restored full view.

## Failure And Durability Assumptions

The implemented recovery protocol requires at least one reachable survivor
that retains:

- a complete materialized store;
- the installed view and deduplication metadata; and
- either the requested contiguous in-memory tail or the ability to create a
  fresh full snapshot.

The delivered-effect log is bounded runtime-retained state, not a durable
Journal. When `have` predates its low-water mark, recovery falls back to a fresh
snapshot. If all survivor protocol state is lost together, this experiment has
no external Journal or checkpoint from which to reconstruct the ordered
history. Valkey durability is separately determined by its persistence
configuration; the experimental Arctic state is reconstructed rather than
persisted.

Network byte duplication and reordering are hidden by the authenticated TLS/TCP
transport. At the composition boundary, the relevant faults are connection
loss, delayed or abandoned state-transfer polls, overlapping retries, duplicate
delivered operations, and process crash/restart.

## Composition Invariants

| ID | Invariant |
|---|---|
| C1 | Every accepted mutation has a deterministic replicated effect. |
| C2 | Every active replica applies effects in the TRAINS delivery order. |
| C3 | An `(origin, request_id)` effect changes a store at most once. |
| C4 | A client acknowledgement follows local ordered delivery and apply. |
| C5 | Snapshot at `X` plus contiguous tail after `X` has no gap or overlap effect. |
| C6 | A passive or stale member cannot originate replicated writes. |
| C7 | Readmission restores full-view bidirectional write convergence. |

## Refinement Map

| Abstract event or property | Implementation event | Evidence |
|---|---|---|
| Broadcast deterministic operation | `WriteOp` creation and `Input::LocalBroadcast` | command/effect unit tests and TRAINS kernel assurance |
| Uniform ordered delivery | `Output::Deliver` | TRAINS TLA+, Apalache, Ivy, Kani, property, differential, and trace checks |
| Deterministic state transition | `RedisStore::apply` on Valkey and Arctic | inline mirror reply equality and seeded differential manifests |
| At-most-once materialized effect | `WriteDedup::first_seen` before apply | duplicate delivery tests and overlapping transfer retries |
| Acknowledgement point | origin entry removed from `pending` after apply | proxy lifecycle and acknowledged-operation manifest |
| Consistent recovery cut | `ReplicaSnapshot.delivered_index == X` | snapshot round trips and full-replacement tests |
| Gap-free continuation | `DeliveredLog` tail after `X` | tail-window tests and deterministic fault simulation |
| Passive recovery | rejoin listener rejects writes | passive-rejoin integration test |
| Safe membership growth | final snapshot, `adopt_view`, then re-admit view change | live crash/restart/promotion lifecycle test |

The kernel proofs establish the abstract ordering and reconfiguration
properties. The refinement map and implementation tests establish evidence that
the proxy and storage composition preserve those properties. Until this map is
mechanically checked, the result is a layered assurance argument rather than a
single end-to-end formal proof.

## Validation Strategy

The seven-way TRAINS assurance method is the first layer, not the last:

1. formal specification and model checking of core protocols;
2. implementation-level refinement and trace mapping;
3. deterministic simulation of failure and retry schedules;
4. real-process fault injection through the public RESP boundary;
5. differential semantics against Valkey;
6. exact acknowledged-operation manifests and final-state comparison; and
7. performance and resource evaluation only after correctness gates pass.

This mirrors the validation shape reported for Aurora DSQL: formal models,
deterministic simulation, deployed fault injection, and differential testing,
while remaining intentionally smaller and single-region.

## Pre-Performance Decision Gate

Proceed to performance engineering only if:

- C1-C7 have direct executable evidence;
- deterministic victim rotation passes repeatably under fixed seeds;
- every acknowledged manifest operation is accounted for;
- Valkey and Arctic match for the declared command and key domain;
- the one-survivor durability assumption is prominent in the paper and API
  documentation; and
- cross-node linearizable reads are either implemented or explicitly excluded
  from the claimed service contract.

If these conditions hold, the next investment is to remove or partition the
`Mutex<S>` serialization point and measure whether Arctic creates useful local
read/index scalability without weakening the ordering and recovery invariants.

## Investment Decision

Decision on 2026-07-25: **GO for a scoped performance exploration**, in a
separate phase.

Evidence at the decision point:

- all seven Arctic experiment gates pass;
- six deterministic seeds each rotate through all three crash victims;
- the simulator checks stale restart, a lost poll, delayed snapshot install,
  overlapping tail retry, duplicate effects, and resumed writes;
- 4,096 seeded commands, including binary values, are checked against Valkey,
  Arctic, and an independent expected-state model;
- the real-process lifecycle ends in the exact 56-key acknowledged-operation
  manifest on every Valkey and Arctic replica;
- the production workspace passes 103 tests, with only its two pre-existing
  opt-in soak/timing tests ignored; and
- both the production and experimental workspaces pass Clippy with warnings
  denied.

This decision approves measuring the local data-plane opportunity. It does not
approve claims of cross-node linearizable reads, durable recovery after loss of
all survivor state, full Valkey compatibility, multi-region operation, or
improved throughput before those measurements exist.

## Performance Exploration Result

The first local microbenchmark completed on 2026-07-25. Across three
backend-order-rotated repetitions, Arctic was approximately tied with
`Mutex<BTreeMap>` at one thread. At eight threads its median throughput was
12.67x higher for reads, 12.14x higher for 90/10 mixed traffic, and 12.10x
higher for writes. Median sampled p99 latency remained between 0.458 us and
1.000 us, versus 46.458 us to 72.709 us for the mutex baseline.

Decision: **GO for a narrow ordered-writer/shared-reader interface prototype**.
Do not switch the production data path yet. Snapshot replacement needs an
explicit exclusive fence, and all C1-C7 lifecycle evidence must pass through
the new interface before measuring full proxy or replicated-ring throughput.

See `bench/results/arctic-data-plane-2026-07-25.md` and its raw JSON companion.

## Ordered Interface Result

The ordered-writer/shared-reader prototype completed on 2026-08-11.
`OrderedArcticStore` is the non-clonable mutation owner;
`SharedArcticReader` is clonable for local point reads. Snapshot import builds
a replacement generation off to the side and swaps it into view atomically.
Concurrent `DBSIZE` remains excluded because the separate cardinality counter
has a mutation visibility window; multi-key `EXISTS` also stays on the ordered
side so it cannot combine observations from different mutation points.

All original C1-C7 lifecycle evidence passes through the new adapter. Three
additional concurrency tests cover raw shared operations, concurrent ordered
writes with shared reads, and atomic snapshot generation replacement.

The architecture-matched benchmark compares Arctic with a `Mutex<BTreeMap>`
using the same single-writer queue and acknowledgement. At eight threads,
ordered Arctic delivered 1.80x median read-only throughput and 2.09x median
90/10 throughput. Write-only was 0.89x, with both implementations dominated by
the common queue. Median sampled p99 was materially lower for Arctic in the
read-heavy cases.

Decision on 2026-08-11: **GO for bounded performance engineering and
proxy-integration design**, not a production data-path switch. The next gate
requires at least 1.5x the ordered-mutex throughput at eight threads for both
read-only and 90/10, no more than a 15% write-only regression, CPU/RSS data,
longer runs, and all correctness evidence still passing. Only then should the
interface enter the local proxy and replicated ring for end-to-end measurement.

See `bench/results/arctic-ordered-data-plane-2026-08-11.md` and its raw JSON.

## References

- Aurora DSQL paper: <https://arxiv.org/pdf/2607.13276>
- Arctic: <https://docs.rs/arctic-map/latest/arctic/>
- TRAINS + Arctic experiment: `experiments/arctic-shadow/README.md`
