# Arctic Data-Plane Microbenchmark - 2026-07-25

## Question

Does Arctic provide enough local concurrent point-operation headroom to justify
designing a store interface that no longer serializes reads behind
`Arc<Mutex<S>>`?

This is not a TRAINS throughput benchmark. It compares local materialization
paths after ordering has already been decided.

## Method

- aarch64 development machine, 10 available hardware threads;
- release build;
- 10,000 preloaded ASCII keys and 32-byte values;
- 100,000 operations per worker;
- 1, 2, 4, and 8 worker threads;
- read-only, 90% read/10% write, and write-only workloads;
- three repetitions with rotated backend execution order;
- latency sampled every 16 operations;
- Arctic through a shared `ConcurrentArcticStore`;
- `Mutex<BTreeMap>` through one shared standard-library mutex; and
- unmodified Valkey through one loopback RESP connection per worker, with RDB
  and AOF persistence disabled.

The in-process Arctic/`Mutex<BTreeMap>` comparison isolates synchronization and
map costs. Valkey includes RESP encoding, kernel networking, and a process
boundary, so it is a reference endpoint measurement rather than a direct data
structure comparison.

Raw repetitions: `arctic-data-plane-2026-07-25.json`.

## Median Throughput

Throughput is millions of operations per second. `A/M` is Arctic divided by
`Mutex<BTreeMap>`.

| Workload | Threads | Arctic | Mutex BTree | Valkey RESP | A/M |
|---|---:|---:|---:|---:|---:|
| Read only | 1 | 9.59 | 9.76 | 0.069 | 0.98x |
| Read only | 2 | 19.61 | 5.12 | 0.112 | 3.83x |
| Read only | 4 | 32.81 | 3.61 | 0.168 | 9.10x |
| Read only | 8 | 39.48 | 3.12 | 0.232 | 12.67x |
| 90% read / 10% write | 1 | 9.60 | 9.27 | 0.068 | 1.03x |
| 90% read / 10% write | 2 | 16.72 | 4.62 | 0.110 | 3.62x |
| 90% read / 10% write | 4 | 24.86 | 2.90 | 0.163 | 8.58x |
| 90% read / 10% write | 8 | 30.06 | 2.48 | 0.180 | 12.14x |
| Write only | 1 | 5.57 | 6.18 | 0.063 | 0.90x |
| Write only | 2 | 5.97 | 3.49 | 0.107 | 1.71x |
| Write only | 4 | 12.94 | 1.68 | 0.153 | 7.70x |
| Write only | 8 | 14.86 | 1.23 | 0.179 | 12.10x |

## Eight-Thread Tail Latency

Median p99 latency from the sampled operations:

| Workload | Arctic | Mutex BTree | Valkey RESP |
|---|---:|---:|---:|
| Read only | 0.458 us | 46.458 us | 78.000 us |
| 90% read / 10% write | 0.917 us | 49.500 us | 148.084 us |
| Write only | 1.000 us | 72.709 us | 119.375 us |

## Interpretation

Single-thread results are deliberately unexciting: Arctic is approximately
tied on reads and mixed traffic and is 10% slower for writes. Its value appears
under contention. At eight threads it is about 12x faster than the mutex
baseline in all three workloads, while retaining sub-microsecond median p99 for
reads and mixed traffic and a 1 us write p99.

The scaling shape justifies further interface work. It does not establish that
the complete TRAINS + Arctic system will achieve these numbers: total ordered
delivery still serializes logical write order, snapshot import requires an
exclusive lifecycle transition, and the proxy, RESP parsing, replication, and
network are absent here.

## Decision

**GO** for a narrow prototype that separates:

- one ordered apply owner;
- shared lock-free point-read handles;
- an explicit barrier/fence for snapshot export and replacement; and
- the existing deterministic effect, deduplication, and readmission contracts.

Do not switch the production store or make distributed throughput claims yet.
The prototype must rerun C1-C7 and the crash/readmission manifest before an
end-to-end benchmark is meaningful.

## Remaining Measurements

- CPU time and RSS by backend;
- longer runs with confidence intervals;
- skewed/hot-key and range-scan workloads;
- snapshot export/import interference;
- ordered single-writer plus concurrent-reader workload; and
- full proxy and replicated-ring throughput after the interface prototype.
