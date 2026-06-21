# redis-chaos — EC2 fis-kill harness pieces for the TRAINS-replicated Redis ring

The node-level building blocks for the PR-RD-4 at-scale chaos run. They make the
run turnkey but **provision nothing** — the operator drives the live AWS steps
(deploy / SSM / teardown). Full procedure + cost + security:
[`bench/reports/trains-valkey-ec2-chaos-runbook-2026-05-25.md`](../../bench/reports/trains-valkey-ec2-chaos-runbook-2026-05-25.md).

## Pieces (which runbook gap each fills)

| Script / binary | Gap | Role |
|---|---|---|
| `../bench-aws/build-linux.sh` | G1 | Cross-builds `trains-valkey` + `trains-valkey-chaos` (musl; `TARGET=…-gnu` fallback) alongside the `trains-cli` variants. |
| `node-bootstrap.sh` | G2 | Install + start a **loopback** Valkey (password, no persistence) on an AL2023 node. |
| `keygen.sh` | G3 | Generate N identities (via `trains keygen`) + the `--peer-fp` fingerprint list. |
| `launch-node.sh` | G4 | Start the `trains-valkey` proxy with `--backend redis://…` + `--peer-addr` (reconfiguration on). |
| `trains-valkey-chaos` (crate bin) | G5 | Write a monotonic `SET` stream, hold for the fault, then verify **no acked-write loss** + survivor convergence by reading the engines directly. |
| `bench/coordinator/faults.py` (`fis-kill-redis`) | G6 | SIGKILLs the `trains-valkey` proxy via SSM (parameterized off the trains-cli kill). |

## Shape of a run (see the runbook for the live AWS commands)

1. `build-linux.sh` → upload `trains-valkey`, `trains-valkey-chaos` (+ a static
   `valkey-server` if AL2023 lacks the package) to the bench S3 bucket.
2. `keygen.sh 3` → distribute `id<i>.json` + `fingerprints.txt`.
3. Per node via SSM: `node-bootstrap.sh` then `launch-node.sh`.
4. `trains-valkey-chaos --resp <node0-resp> --engines <survivor-ips> --count N
   --hold-secs H` — during the hold, the coordinator injects
   `fis-kill-redis` on the victim (`faults.py`); the driver asserts no acked
   write was lost and survivors converged.
5. **Always** `../bench-aws/teardown.sh`.

The driver (G5) and the `fis-kill-redis` fault (G6) are unit/integration-tested
locally (`crates/trains-valkey/tests/redis_backend.rs::ring::chaos_driver_*`,
`bench/coordinator/tests/test_faults.py`). The shell scripts are loopback/SSM
glue, verified at run time on the instances.
