# Arctic EC2-A1 ARM64 result

Decision: **GO for local proxy integration**.

## Reproducibility

- commit: `a3a8d0e39394d90551011b0d5d65718af45d0fa9`
- host: `c7g.2xlarge`, `Amazon Linux 2023.12.20260803`
- engine: `Valkey server v=9.0.5 sha=00000000:0 malloc=jemalloc-5.3.0 bits=64 build=3be9513dab266553`
- seven interleaved ArcSwap/RwLock rounds; medians below
- 100,000 keys, 32-byte values, 1,000,000 operations per thread
- process CPU and Linux high-water RSS retained in raw JSON

## Eight-thread architecture-matched result

| Workload | ArcSwap Arctic Mops/s | RwLock Arctic | Ordered mutex | ArcSwap/mutex | ArcSwap p99 us | Mutex p99 us | MAD |
|---|---:|---:|---:|---:|---:|---:|---:|
| read-only | 21.980 | 6.970 | 1.216 | 18.07x | 0.588 | 32.415 | 1.8% |
| read-90-write-10 | 3.398 | 3.184 | 1.160 | 2.93x | 19.906 | 47.967 | 1.0% |
| write-only | 0.334 | 0.333 | 0.347 | 0.96x | 73.518 | 24.561 | 0.4% |

## Decision gates

- PASS: 8-thread read-only ArcSwap is at least 1.5x ordered mutex.
- PASS: 8-thread 90/10 ArcSwap is at least 1.5x ordered mutex.
- PASS: 8-thread write-only ArcSwap is at least 0.85x ordered mutex.
- PASS: both generation strategies completed the full correctness suite on the measured host.

## CPU, memory, and latency debt

| Workload | ArcSwap CPU s | Ordered mutex CPU s |
|---|---:|---:|
| read-only | 2.78 | 36.21 |
| read-90-write-10 | 8.16 | 29.98 |
| write-only | 45.98 | 45.25 |

Median process high-water RSS was 42.5 MiB for ArcSwap and 41.9 MiB for RwLock. Each process ran the same complete workload matrix, so this is a process-level comparison rather than per-case allocation attribution.

The write-throughput gate passes, but eight-thread write-only p99 is 73.5 us for Arctic versus 24.6 us for the ordered mutex. The RwLock Arctic baseline has the same shape, so ArcSwap is not the source; queued Arctic mutation latency remains an explicit proxy-integration gate.

## Valkey endpoint reference

Valkey includes RESP parsing, loopback networking, and a process boundary. It is retained as an endpoint reference, not as an architecture-matched map comparison.

| Workload | Threads | Mops/s | p99 us | Client CPU ms | Engine CPU ms |
|---|---:|---:|---:|---:|---:|
| read-90-write-10 | 1 | 0.060 | 18.056 | 2030.0 | 2590.0 |
| read-90-write-10 | 8 | 0.154 | 67.223 | 12900.0 | 12940.0 |
| read-only | 1 | 0.060 | 17.548 | 2010.0 | 2580.0 |
| read-only | 8 | 0.156 | 65.350 | 12830.0 | 12800.0 |
| write-only | 1 | 0.059 | 17.649 | 2020.0 | 2680.0 |
| write-only | 8 | 0.150 | 69.475 | 13010.0 | 13370.0 |

## Claim boundary

This gate evaluates concurrent local materialization behind one ordered mutation owner. Cross-node linearizable reads, durability after all nodes restart, full Valkey compatibility, and multi-region operation remain outside the claim. A GO authorizes feature-flagged local proxy integration; a separate proxy gate is required before replicated-ring measurement. It is not a production data-path decision.
