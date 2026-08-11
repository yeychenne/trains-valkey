# Ordered Arctic data-plane exploration - 2026-08-11

## Question

Can TRAINS keep a single ordered mutation owner while Arctic serves concurrent
local point reads, without weakening the existing crash/rejoin invariants, and
does that interface retain an advantage over the current mutex-shaped design?

This is a local data-plane experiment. It is not a replicated-ring throughput
result and does not extend the service claim to cross-node linearizable reads,
all-node restart durability, full Valkey compatibility, or multi-region use.

## Interface under test

`OrderedArcticStore` is non-clonable and requires `&mut self` for mutations and
snapshot installation. `SharedArcticReader` is clonable and provides local
`GET`/`EXISTS`. A snapshot is decoded into a fresh map and then installed by an
atomic generation change; pinned readers finish on either the old complete
generation or the new complete generation, never a partially loaded map.

`DBSIZE` and multi-key `EXISTS` remain on the ordered side. Updating Arctic and
a separate cardinality counter creates a visibility window, so concurrent
`DBSIZE` is not claimed; multi-key reads require one coordinated observation.

## Correctness result

PASS. The three concurrency tests and all seven original shadow gates pass
through the new adapter, including:

- concurrent ordered writes and shared point reads;
- atomic snapshot generation replacement;
- six deterministic crash schedules with rotating victims;
- the 4,096-command Valkey/Arctic/independent-model manifest; and
- live member crash, reduced-view writes, stale restart, promotion/re-admission,
  and exact final convergence.

Clippy passes for all release targets with warnings denied.

## Benchmark method

- architecture: aarch64, 10 available hardware threads;
- 10,000 preloaded keys, 32-byte values;
- 100,000 operations per client thread;
- 1, 2, 4, and 8 client threads;
- read-only, 90/10 read/write, and write-only workloads;
- three repetitions with backend order rotated;
- medians reported below; latency sampled every 16 operations;
- Valkey persistence disabled to measure its in-memory endpoint path.

Five boundaries were measured. The decision comparison is between
`ordered-arctic` and `ordered-mutex-btree`: both use the same bounded writer
queue and per-write acknowledgement. Raw Arctic and direct `Mutex<BTreeMap>`
are primitive-level reference ceilings. Valkey includes RESP, loopback network,
and a process boundary and is therefore an endpoint reference, not an
apples-to-apples map comparison.

Raw data: `arctic-ordered-data-plane-2026-08-11.json` (180 samples).

## Median throughput

Millions of operations per second:

| Workload | Threads | Ordered Arctic | Ordered mutex | Ratio | Raw Arctic | Valkey endpoint |
|---|---:|---:|---:|---:|---:|---:|
| read-only | 1 | 9.48 | 9.36 | 1.01x | 9.54 | 0.069 |
| read-only | 2 | 10.69 | 5.18 | 2.06x | 15.59 | 0.107 |
| read-only | 4 | 4.72 | 2.37 | 2.00x | 18.13 | 0.116 |
| read-only | 8 | 3.41 | 1.89 | 1.80x | 37.91 | 0.114 |
| 90/10 | 1 | 1.76 | 1.79 | 0.98x | 8.38 | 0.066 |
| 90/10 | 2 | 2.27 | 1.97 | 1.15x | 13.11 | 0.098 |
| 90/10 | 4 | 3.04 | 1.67 | 1.82x | 17.81 | 0.119 |
| 90/10 | 8 | 2.86 | 1.37 | 2.09x | 28.10 | 0.118 |
| write-only | 1 | 0.233 | 0.237 | 0.98x | 3.23 | 0.049 |
| write-only | 2 | 0.166 | 0.152 | 1.09x | 5.01 | 0.034 |
| write-only | 4 | 0.300 | 0.291 | 1.03x | 7.74 | 0.087 |
| write-only | 8 | 0.463 | 0.518 | 0.89x | 11.99 | 0.107 |

At eight threads the median sampled p99 was 8.917 us versus 66.500 us for
read-only, and 38.125 us versus 63.500 us for 90/10. Write-only p99 was
41.625 us versus 37.750 us; the shared queue dominates that workload.

## Interpretation

The composition idea survives the correctness gates and retains useful local
read scalability under the architecture-matched comparison. The write-only
result is close to the ordered mutex baseline, as expected when both backends
pay the same single-writer queue and acknowledgement cost.

The gap between raw Arctic and ordered Arctic also exposes the next bottleneck:
every point read currently takes a shared `RwLock` and clones an `Arc` to pin
the generation. That mechanism preserves atomic snapshot replacement but
creates refcount/lock contention at four and eight threads. It is interface
overhead, not evidence that Arctic itself stops scaling.

Three repetitions were noisy, especially in queued write-only cases. These
figures justify another bounded experiment, not a publication-grade throughput
claim.

## Investment decision

**GO for bounded performance engineering and proxy-integration design.** Do
not switch the production data path yet.

The next gate must:

1. replace per-read generation locking with a lower-contention atomic generation
   handle while preserving the atomic replacement test;
2. compare any queue/ack batching symmetrically against the ordered mutex;
3. measure CPU and RSS in longer runs;
4. preserve every current correctness gate; and
5. demonstrate at least 1.5x ordered-mutex throughput at eight threads for both
   read-only and 90/10, with no more than a 15% write-only regression.

Only after that gate should the experiment enter the local proxy and replicated
ring for end-to-end measurement.
