# Arctic feature-flagged proxy integration - 2026-08-11

## Decision

**PASS for the correctness half of the local proxy gate.** Do not start EC2-A2
until the symmetric local RESP performance comparison also passes.

## Integrated path

- `arctic-proxy` is off by default.
- A feature-enabled binary accepts `--backend arctic`.
- Valid `GET` and single-key `EXISTS` use `SharedArcticReader` without locking
  the ordered store.
- Mutations still pass through TRAINS delivery and ordered `apply` before the
  origin acknowledges them.
- `DBSIZE`, multi-key `EXISTS`, malformed reads, snapshot import, and readmission
  remain on `OrderedArcticStore`.
- Invalid Arctic keys in state-transfer snapshots return `SnapshotError`
  instead of terminating the process.

## Verification

| Gate | Result |
|---|---:|
| Feature off, all workspace targets | 104 passed, 2 pre-existing ignored |
| Feature on, all workspace targets | 112 passed, 2 pre-existing ignored |
| Clippy feature off, warnings denied | PASS |
| Clippy feature on, warnings denied | PASS |
| Direct Arctic adapter tests | 7 passed |
| Real RESP/TLS Arctic ring test | PASS |

The feature-on integration test creates three Arctic-backed proxy nodes on the
real TLS TRAINS ring. Through RESP it verifies:

- same-node `GET` observes a locally acknowledged `SET`;
- all three nodes converge after quiescence;
- `GET` completes while the ordered store mutex is deliberately held elsewhere;
- multi-key `EXISTS` and `DBSIZE` retain correct ordered results;
- four clients complete 400 point reads while 100 writes pass through TRAINS;
- every node reaches the exact final value; and
- a replacement snapshot installs while RESP point reads remain active, with
  the replacement state visible afterward.

## Remaining local gate

Use one RESP workload generator for the feature-off mutex proxy, feature-on
Arctic proxy, and unmodified Valkey. Measure one and eight clients, read-only,
90/10, and write-only traffic over at least seven interleaved rounds, including
CPU, RSS, throughput, and sampled p99.

Proceed to EC2-A2 only if correctness remains green, eight-client read-only and
90/10 throughput are at least 1.5x the feature-off proxy, and write-only p99 is
below 3x the feature-off proxy or has a documented queue-level explanation.

Cross-node linearizable reads, all-node restart durability, full Valkey
compatibility, and multi-region operation remain outside the claim.
