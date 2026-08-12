# TRAINS + Arctic: Final Evidence and Investment Decision

Date: 2026-08-12

## Executive decision

The TRAINS + Arctic exploration has reached a solid stopping point for its
original research question. We should now write the final report rather than
continue Arctic performance engineering.

The central result is methodological, not merely a benchmark result. The work
demonstrates that a layered assurance process can carry an idea from an
abstract verified protocol, through an independently developed open-source
data structure, into a real replicated RESP endpoint, and then support a
grounded investment decision. The process produced two different decisions at
two different boundaries:

- **GO after correctness and primitive-level evidence:** Arctic preserved the
  composition invariants and exposed approximately `12x` eight-thread
  headroom over `Mutex<BTreeMap>` in the isolated data-plane benchmark.
- **NO-GO after endpoint evidence:** in the complete local three-node proxy,
  Arctic reached `1.22x` the mutex proxy for eight-client reads and `0.99x` for
  90/10 traffic, missing both predeclared `1.5x` gates.

This is a successful use of the method. It prevented us from promoting an
impressive microbenchmark into an unsupported system-level performance claim,
and it prevented unnecessary AWS expenditure. No EC2-A2 resources were
launched and none of the authorized 60 USD was spent.

Arctic should remain in the repository as a feature-gated composition case
study and regression target. Further Arctic optimization is interesting only
if the objective changes from validating the assurance method to building a
higher-throughput product.

## What has been established

The verified foundation is the TRAINS protocol kernel. It is checked through
seven complementary methods: TLA+/TLC, Apalache, Kani/CBMC, property testing,
differential testing, trace validation, and Ivy parameterized verification.
Those checks establish abstract ordering and reconfiguration properties.

The proxy and Arctic composition should not be described as one end-to-end
formal proof. Its assurance is layered. The refinement map connects abstract
events such as broadcast, delivery, apply, acknowledgement, snapshot cut, and
readmission to concrete implementation events. Deterministic simulation,
real-process fault injection, differential comparison with Valkey, exact
acknowledged-operation manifests, and final-state comparison then test that
connection.

The composition has direct executable evidence for C1-C7:

| Invariant family | Evidence now available |
|---|---|
| Deterministic mutation and ordered apply, C1-C2 | command/effect tests, TRAINS delivery, three-node convergence, and Valkey differential manifests |
| At-most-once effects and acknowledgement discipline, C3-C4 | duplicate-delivery tests, request deduplication, exact acknowledged-operation manifests, and read-after-ack tests |
| Snapshot plus tail recovery, C5 | replacement snapshots, contiguous delivered-log catch-up, retry schedules, and final-state equality |
| Passive safety and full readmission, C6-C7 | live crash, reduced view, stale restart, promotion, recovered-member write, survivor write, and restored three-node convergence |

The integration moved beyond an in-memory model. A feature-enabled binary runs
`OrderedArcticStore` in a real TLS TRAINS ring. Point `GET` and single-key
`EXISTS` use the shared Arctic reader, while mutations, aggregate reads,
snapshots, and readmission remain ordered. Existing tests pass with the feature
disabled, and the feature-on suite covers concurrent reads, writes, replacement
snapshots, and exact final values.

Finally, the performance decision used one deterministic RESP driver for the
feature-off mutex proxy, the Arctic proxy, and unmodified Valkey. The completed
qualification contains 126 validated cases, seven rotated rounds, exact command
schedules, reply validation, final-value checks, CPU/RSS telemetry, immutable
binary hashes, and no analyzer errors. Correctness and write-regression gates
passed. The two intended read-scaling gates failed.

## Why the negative performance result is valuable

The isolated benchmark was not wrong. It answered whether Arctic removes local
map-lock contention, and the answer was yes. The endpoint qualification asked
whether that advantage dominates the full system, and the answer was no for the
current architecture.

This boundary is scientifically useful. It localizes the next bottleneck above
the materialized map: RESP processing, task and socket scheduling, request
round trips, TRAINS circulation and delivery, and acknowledgement. The 90/10
result is especially decisive. Once writes enter the workload, both stores pay
almost the same ordered path, and Arctic's read advantage disappears. Comparable
CPU and RSS results also show that the adapter did not hide a large resource
regression.

For the paper, this makes the assurance method more credible. A method that
always concludes “continue” looks like advocacy. Here, predeclared gates
allowed a promising component to advance, required it to face a stronger
boundary, and then stopped investment when the system evidence did not justify
the next cost. The result is reproducible, falsifiable, and honest about what
each level can and cannot establish.

## Is another test required?

No additional Arctic test is required to answer the original question or to
begin the final report. Another run of the same local matrix or an EC2-A2 run
would be unlikely to change the investment decision: `0.99x` mixed throughput
is far from the `1.5x` threshold, and seven interleaved medians already reduce
the influence of individual noisy rounds. Repeating the architecture in AWS
would improve environmental replication, but it would not address the shared
path that the local evidence identifies.

Additional tests become necessary only if the report expands its claims:

| Proposed claim | Additional evidence required |
|---|---|
| Arctic improves full endpoint throughput materially | profile and change the shared endpoint path, define a new gate, then rerun locally before AWS |
| Reads are linearizable from arbitrary nodes | a cross-node read protocol and histories checked against a linearizability oracle |
| State survives loss or restart of every member | durable external journal/checkpoint recovery and an all-node restart test |
| Broad Valkey compatibility | a larger command-semantics differential suite, including atomic and complex data-type behavior |
| Multi-region operation | a different protocol/deployment scope with WAN fault, clock, latency, and regional-failure evidence |

These are legitimate future projects, but they are outside the current claim
and do not block publication of the present result. Performance profiling may
be useful product work; it is not missing proof for the methodological thesis.

## Recommendation for the final report

Freeze the Arctic branch as a completed case study, keep its feature off by
default, and retain its correctness tests in CI where practical. Do not begin
EC2-A2 or optimize Arctic itself now.

The final report should present the sequence of questions and decisions:

1. specify the service boundary and C1-C7;
2. establish the abstract TRAINS properties with the seven kernel methods;
3. map those properties to the proxy and Arctic implementation;
4. challenge the composition through deterministic and real-process faults;
5. compare semantics with Valkey and reconcile acknowledged operations;
6. expose and measure Arctic's concurrency at the primitive boundary;
7. test the complete endpoint against criteria declared before the run; and
8. record the NO-GO and the known claim limitations.

One synthesis task remains before declaring the paper complete: build a compact
claim-to-evidence table that links every final claim to its invariant, method,
test, artifact, and limitation. This is documentation and audit work, not a new
experiment. Subject to that synthesis and ordinary editorial review, the
project is ready for the final report.

Primary evidence:

- `docs/ADR-002-trains-arctic-assurance-boundary.md`
- `bench/results/arctic-data-plane-2026-07-25.md`
- `bench/results/arctic-proxy-integration-2026-08-11.md`
- `bench/results/arctic-proxy-local/7c03a87b5b0fac2624d7b0d28ec1412488e44299/RUN-NOTES.md`
- `bench/results/arctic-proxy-local/7c03a87b5b0fac2624d7b0d28ec1412488e44299/REPORT.md`
