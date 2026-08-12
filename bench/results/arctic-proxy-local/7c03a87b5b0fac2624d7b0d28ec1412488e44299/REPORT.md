# Arctic proxy local qualification report

**Decision: NO-GO.**

All targets use the same deterministic persistent-connection RESP driver. The decision comparison is Arctic proxy versus mutex proxy; Valkey is an endpoint reference.

| Workload | Clients | Mutex ops/s | Arctic ops/s | A/M | Mutex p99 us | Arctic p99 us | p99 ratio | Valkey ops/s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| read-only | 1 | 20740 | 19300 | 0.93x | 731.916 | 804.583 | 1.10x | 69141 |
| read-only | 8 | 83110 | 101562 | 1.22x | 469.416 | 295.167 | 0.63x | 138449 |
| read-90-write-10 | 1 | 1929 | 1992 | 1.03x | 4617.292 | 4667.292 | 1.01x | 68851 |
| read-90-write-10 | 8 | 22641 | 22442 | 0.99x | 3084.958 | 3686.333 | 1.19x | 255433 |
| write-only | 1 | 174 | 174 | 1.00x | 11540.041 | 11616.750 | 1.01x | 62690 |
| write-only | 8 | 837 | 867 | 1.04x | 22582.125 | 19949.166 | 0.88x | 111612 |

## Gates

- PASS: `correctness_and_evidence`
- FAIL: `read_only_throughput_1_5x`
- FAIL: `mixed_throughput_1_5x`
- PASS: `write_p99_below_3x`
- PASS: `write_throughput_within_15pct`

Cross-node linearizable reads, all-node restart durability, full Valkey compatibility, and multi-region operation remain outside scope.
