# TRAINS + Arctic AWS pause point - 2026-08-11

## Current decision

EC2-A1 is complete: GO for a feature-flagged local proxy integration test, not
for a production data-path switch. ArcSwap cleared the three established
throughput gates and both generation variants passed all ten correctness tests
on ARM64 AWS. The result preserves the existing claim boundary.

The open performance issue is queued write latency. At eight threads, Arctic
write-only p99 was 73.5 us versus 24.6 us for the architecture-matched ordered
mutex, despite 0.96x throughput. Do not hide this in an aggregate GO result.

## Continuation status

The 0-12 hour local proxy adapter milestone is complete. The binary now selects
the adapter explicitly with `--backend arctic` when compiled with the
off-by-default `arctic-proxy` feature. The feature-off suite passes 104 tests;
the feature-on suite passes 112, including a real three-node RESP/TLS Arctic
ring test. Clippy passes with warnings denied in both configurations.

The correctness portion of the 12-24 hour gate is also complete: same-node
read-after-ack, cross-node convergence after quiescence, fast reads while the
ordered mutex is held, writer/reader contention, and snapshot replacement under
active RESP reads all pass. The symmetric local throughput/p99 comparison is
still pending and remains the blocker for EC2-A2.

## Next 48 hours

### 0-12 hours: local proxy adapter

Status: **complete**.

- Add a feature-flagged `OrderedArcticStore` adapter behind the local RESP
  proxy; keep the existing backend as the default.
- Route only supported `GET` and single-key `EXISTS` through shared readers.
- Keep mutations, `DBSIZE`, multi-key `EXISTS`, snapshots, and readmission on
  the ordered owner.
- Preserve C1-C7 and the current ten-test suite unchanged.

### 12-24 hours: end-to-end local gate

Status: **correctness complete; performance comparison pending**.

- Add concurrent RESP tests for same-node read-after-ack, snapshot replacement,
  and writer/read contention.
- Benchmark feature-off mutex, feature-on Arctic, and unmodified Valkey through
  the same RESP client path.
- Reject the AWS phase if any correctness test fails, read-heavy throughput is
  below 1.5x the feature-off proxy, or write-only p99 exceeds 3x the feature-off
  proxy without a documented queue explanation.

### 24-42 hours: EC2-A2 proxy run

- Reuse one `c7g.2xlarge` in `eu-west-3c`, seven interleaved rounds, one- and
  eight-client loads, CPU/RSS telemetry, a six-hour host shutdown, and automatic
  stack teardown.
- Measure the real local RESP proxy path; do not include replicated-ring or
  cross-node-read claims in this run.
- Keep a 60 USD authorization ceiling. Expected spend remains below 3 USD with
  the current instance and time limits.

### 42-48 hours: decision and paper evidence

- Publish raw JSON, environment identity, binary hashes, test logs, and a
  generated report.
- Decide GO/NO-GO for a three-node replicated-ring benchmark.
- A GO still excludes cross-node linearizable reads, all-node restart
  durability, full Valkey compatibility, and multi-region operation.

## Re-entry command

The feature-flagged local proxy adapter is committed at `a7d139e`. Continue with
the symmetric local performance gate in
`TOMORROW-2026-08-12-ARCTIC-PROXY-GATE.md`. Do not begin EC2-A2 until its local
correctness and throughput gates are green.

No AWS resources were launched for this continuation.
