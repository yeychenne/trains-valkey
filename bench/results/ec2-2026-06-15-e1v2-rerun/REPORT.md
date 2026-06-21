# E1-v2 re-run — zero acked-write loss through a masked crash (2026-06-15)

First live EC2 run under the `<aws-profile>` credential model, with the PR-RED-6
chaos-driver fix (`--abandon-secs`). Reproduces the paper's headline E1-v2 result
on a fresh deploy.

## Setup

| | |
|---|---|
| Infra | 3 × `t4g.small` (ARM64, AL2023), eu-west-3, CDK (`bankx` qualifier), `Project=TRAINS` |
| Credentials | `[<aws-profile>]` (IAM user `yves-devops`) — instances use their **own** IMDSv2 role, key never on the boxes |
| Engine | valkey 9.0.4 (AL2023 default repo), loopback `127.0.0.1:6379`, `requirepass` |
| Proxy | `trains-valkey` (RING_SIZE=3), RESP `127.0.0.1:6380`, mTLS ring on `:7000`, reconfiguration enabled |
| Binaries | built with `cargo-zigbuild --target aarch64-unknown-linux-gnu.2.34` (`cross` is broken on the Apple-Silicon build host) |
| Workload | `trains-valkey-chaos --mode load --count 2000 --hold-secs 30 --abandon-secs 5` on node 0 |

## Procedure

1. Ring formed; replication confirmed (`SET` on node 0 readable on all 3).
2. Load phase 1: 1000 writes acked. Load entered the 30 s hold.
3. **SIGKILL the proxy on node 2** during the hold (masked-crash window).
4. Load phase 2: 1000 writes **through the masked window**.
5. `verify-local` on each survivor (acked set distributed via S3 — option-B split).

## Result — zero acked-write loss

```
[chaos] phase 1 acked 1000 writes; holding 30s for fault injection
[chaos] writing 1000..2000 through the masked window
[chaos] total acked 2000 writes
```

| Survivor | acked_total | missing_keys | DBSIZE |
|---|---:|---:|---:|
| node 0 | **2000** | **0** | 2001 |
| node 1 | **2000** | **0** | 2001 |

- **2000 / 2000 acked writes preserved**, 1000 of them written *through* the masked
  node-2 crash. **0 lost** on either survivor.
- **Survivors converged byte-identically** (DBSIZE 2001 = 2001; the +1 is the
  `e5key` from the pre-load replication check).
- Crash masking fired on real EC2: the predecessor logged "successor unreachable
  → view change → retarget → reissue" within ~8 s of the kill (observed on the
  earlier 2026-06-15 run); the masked `{0,1}` ring kept serving writes.

## Cost + teardown

3 × t4g.small for ~30 min — a few cents (well under \$1). `cdk destroy` removed
both stacks; no stacks / instances / buckets remain.

## Notes

- **PR-RED-6 was load-bearing here.** Before it, the Rust load driver hung forever
  on the single write in-flight at crash time (no per-write timeout), so the clean
  number couldn't be captured. `--abandon-secs 5` abandons that un-acked write
  (correctly — it was never `+OK`'d) and reconnects, so the load drives straight
  through the masked window.
- This is the E5/E1-v2 evidence the OPS-E5 plan calls for. The remaining E5 work
  is the *adversarial* Tier-1/2 schedules (partition, multi-victim ×5-node,
  rejoin, clock-skew) via `e5_sequencer.py` — this run is the single-victim
  masked-crash baseline on the new infra.
