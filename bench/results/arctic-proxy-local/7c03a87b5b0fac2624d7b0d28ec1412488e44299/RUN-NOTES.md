# Arctic proxy local qualification - run notes and decision

Date: 2026-08-12

Measured commit: `7c03a87b5b0fac2624d7b0d28ec1412488e44299`

## Executive decision

**NO-GO for EC2-A2 and further performance spending on the current proxy
architecture.** The correctness composition remains valid and worth retaining,
but the local endpoint result does not meet the predeclared performance gate.

- eight-client read-only: Arctic 101,562 ops/s versus mutex 83,110 ops/s,
  `1.22x`, below the required `1.5x`;
- eight-client 90/10: Arctic 22,442 ops/s versus mutex 22,641 ops/s, `0.99x`,
  below the required `1.5x`;
- eight-client write-only: Arctic 867 ops/s versus mutex 837 ops/s, `1.04x`;
- eight-client write-only p99: Arctic 19.95 ms versus mutex 22.58 ms, `0.88x`;
  and
- every reply, operation count, latency sample count, final-value sample, and
  evidence artifact passed validation.

This is a performance NO-GO, not a correctness rejection of TRAINS + Arctic.
The feature remains off by default and is useful as executable composition
evidence. AWS was not started and none of the 60 USD authorization was spent.

## Question tested

Does Arctic's primitive-level concurrent-read advantage survive the full local
RESP endpoint when TRAINS still owns mutation ordering and acknowledgement?

The symmetric decision comparison used:

1. a real three-node TLS TRAINS ring with feature-off `MemStore` behind the
   proxy's ordered mutex;
2. the same ring and client path with feature-on `OrderedArcticStore` and
   `SharedArcticReader`; and
3. unmodified Valkey over loopback RESP as an endpoint reference.

Valkey is not the decision denominator because it does not pay the same
replication, delivery, and acknowledgement work.

## Harness development observations

The benchmark was built as a committed Rust driver rather than around a local
`memtier_benchmark` installation. Each client owns one persistent RESP
connection with one outstanding request. Client-owned key shards make every
concurrent read independently checkable. Each target sees the same fixed seed,
key set, command schedule, operation count, and rotating target order.

The benchmark-only proxy process launches all three real ring nodes in one
process and reports node zero's RESP address. CPU and RSS therefore describe
the complete local three-node target process, not just the contacted node.

Two pre-evidence micro-runs found and fixed harness defects:

- Serde initially encoded the mixed workload as `read90-write10`, while the
  analyzer expected `read-90-write-10`. The analyzer correctly rejected that
  run as incomplete.
- Toolchain capture assumed `rustc` was on `PATH` even when Cargo was supplied
  by absolute path. The runner now derives the adjacent compiler when needed.

A further review fixed malformed-readiness cleanup, made evidence directories
immutable, isolated feature-on and feature-off Cargo targets, and added binary
hashes, exact commands, seed, environment, CPU/RSS telemetry, and final-value
validation. A cached 18-case micro-run then passed end to end.

## Smoke result

The official smoke at parent commit `c912775` used 1,000 keys, 2,000 operations
per client, one and eight clients, all three workloads, and one round. All 18
cases passed. It proved orchestration and evidence completeness, not the
performance claim.

Its eight-client Arctic/mutex ratios were `0.99x` read-only, `1.02x` mixed, and
`0.99x` write-only. Those short cases already suggested parity, but one round
was intentionally insufficient for a decision.

## Aborted uniform-count pilot

The first qualification attempt used the original uniform 100,000 operations
per client for every workload. It completed all read-only and 90/10 cases. The
long mixed cases repeatedly placed both proxies in the same regime:

- one client: about 1.1k-1.3k ops/s and 9-12 ms sampled p99;
- eight clients: commonly 13k-16k ops/s and 5-8 ms sampled p99; and
- direct Valkey remained tens or hundreds of thousands of ops/s.

The first 100,000-operation write-only case then ran for several minutes. At
the observed sustained write rate, completing 42 such write cases would have
added hours without improving the comparison. The run was interrupted through
the runner's cleanup path. No target listener survived, no `raw.json` or report
was finalized, and the partial directory was moved outside the repository so
it cannot be mistaken for qualification evidence.

The pilot was useful methodological evidence: operation count changed duration,
not the observed proxy regime. The committed bounded matrix therefore retained
seven interleaved rounds but used:

- 100,000 operations/client for read-only;
- 20,000 operations/client for 90/10; and
- 5,000 operations/client for write-only.

At a sample interval of 16, one write-only round still produces 313 latency
samples at one client and 2,504 at eight clients. The driver was also changed
to append each completed case and its telemetry before starting the next one.
Only a complete 126-case matrix can produce a final report.

## Final qualification

The complete run lasted from 06:09:10Z to 07:03:19Z and produced:

- 126 independently validated case files;
- 14,826 process telemetry samples;
- seven rotated rounds for each target/workload/client combination;
- immutable binary hashes and environment identity;
- complete raw JSON, checkpoint JSONL, summary, and generated report; and
- no analyzer errors.

Median endpoint results:

| Workload | Clients | Mutex ops/s | Arctic ops/s | A/M | Mutex p99 | Arctic p99 |
|---|---:|---:|---:|---:|---:|---:|
| read-only | 1 | 20,740 | 19,300 | 0.93x | 0.732 ms | 0.805 ms |
| read-only | 8 | 83,110 | 101,562 | 1.22x | 0.469 ms | 0.295 ms |
| 90/10 | 1 | 1,929 | 1,992 | 1.03x | 4.617 ms | 4.667 ms |
| 90/10 | 8 | 22,641 | 22,442 | 0.99x | 3.085 ms | 3.686 ms |
| write-only | 1 | 174 | 174 | 1.00x | 11.540 ms | 11.617 ms |
| write-only | 8 | 837 | 867 | 1.04x | 22.582 ms | 19.949 ms |

## Resource interpretation

At eight clients, median target CPU and peak RSS were:

| Workload | Mutex CPU | Arctic CPU | Mutex RSS | Arctic RSS |
|---|---:|---:|---:|---:|
| read-only | 24.68 s | 21.57 s | 14.1 MiB | 13.3 MiB |
| 90/10 | 12.19 s | 11.61 s | 32.8 MiB | 31.1 MiB |
| write-only | 58.77 s | 56.71 s | 27.3 MiB | 27.6 MiB |

Arctic did not introduce a material CPU or RSS regression. Its lower read-only
CPU and p99 indicate that the fast path is real, but the gain becomes only
`1.22x` at the endpoint. Mixed and write-only resource use is nearly symmetric,
matching the expectation that shared TRAINS ordering and acknowledgement work
dominates those cases.

## Variability observed during the run

Some later rounds slowed multiple targets in adjacent rotated slots. For
example, eight-client mixed and write-only cases showed broad drops that also
affected direct Valkey. The raw min/max ranges and telemetry retain those
episodes. They are treated as host-level noise rather than silently removed.

Medians were predeclared before the run, target order rotated by repetition,
and no outlier was discarded. The central conclusion does not depend on one
slow round: the normal rounds also place Arctic and mutex close together.

## Engineering interpretation

The experiment successfully exposed Arctic concurrency through RESP without
weakening C1-C7. It did not show that replacing the materialized map alone is
enough to scale the complete endpoint.

The evidence points to shared work above the map:

- RESP parsing, socket scheduling, and one-request-at-a-time client round trips;
- the common driver and pending-request path;
- TRAINS write circulation, ordered delivery, and origin acknowledgement; and
- three-node ring work inside one benchmark target process.

Read-only receives a modest Arctic benefit because it can avoid the ordered
store mutex. Once 10% of operations are writes, that benefit disappears into
the common ordered path. Sustained writes are orders of magnitude slower than
direct Valkey for both stores and have nearly identical backend ratios.

This does not disprove Arctic's primitive benchmark or EC2-A1 result. Those
measure lower boundaries where map concurrency is exposed directly. The local
qualification answers the next composition question: at the current full
endpoint boundary, that advantage is not yet the limiting factor.

## Investment decision and next work

Do not run EC2-A2 now. Repeating the same proxy architecture on AWS would spend
money to confirm a local gate that already failed by a wide margin.

Retain the feature-gated Arctic adapter and its correctness tests as valuable
open-source assurance evidence. Before reconsidering performance investment:

1. profile the read-only endpoint to partition client, RESP, Tokio scheduling,
   ring-task, and store time;
2. profile the ordered write path around enqueue, circulation, delivery,
   apply, and acknowledgement;
3. decide whether the paper's useful claim is bounded correctness composition
   rather than an immediate endpoint throughput win;
4. make any batching or queue change symmetrically for Arctic and mutex; and
5. define a new local gate before authorizing cloud spend.

Cross-node linearizable reads, all-node restart durability, full Valkey
compatibility, and multi-region operation remain outside the claim.
