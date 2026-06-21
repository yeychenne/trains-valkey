# E5 t1-rejoin — live EC2 run, 2026-06-16

**Result: ✅ PASS.** The v2 passive rejoin (PR-RJ-2b/2c/3a/3b/3c + the
`--rejoin` CLI/bench wiring) converges a SIGKILLed-then-restarted node on real
hardware — the exact assertion that **FAILED on 2026-06-15** (rejoined node
1000/2000) now passes **2000/2000**.

## Setup

- **Infra:** CDK `TrainsBenchNetwork` + `TrainsBenchCompute`, 3× `t4g.small`
  (ARM Graviton, AL2023), eu-west-3, account <account-id>, tag `Project=TRAINS`.
  Instances use their own IMDSv2 instance role (no operator creds on the box).
- **Binary:** `trains-valkey` rebuilt this day via `cargo zigbuild --target
  aarch64-unknown-linux-gnu.2.34` with the merged `--snapshot-listen` /
  `--rejoin-from` flags (verified present in the shipped ELF).
- **Scenario:** `bench/coordinator/schedules/t1-rejoin.json` — kill node 2's
  proxy at T+15s, restart it at T+75s while a 2000-write chaos load runs;
  survivors serve state transfer on :7001, the restarted node comes up as a
  passive replica (`REJOIN_FROM=<survivor>:7001`).

## Result

| node | role | acked | missing_keys | DBSIZE |
|------|------|-------|--------------|--------|
| 0 | survivor (issuer) | 2000 | `[]` | 2000 |
| 1 | survivor (issuer) | 2000 | `[]` | 2000 |
| **2** | **killed → passive rejoiner** | **2000** | **`[]`** | **2000** |

`>>> t1-rejoin: PASS (zero acked-write loss, survivors converged)` — and the
**rejoined node matches the survivors**, the bar that was failing.

Node 2's proxy log confirms the path:
```
[2] state-transfer server: serving on 0.0.0.0:7001
[2] PASSIVE REJOIN: catching up from 1 survivor(s), poll 200ms
```
Its valkey `DBSIZE` = **2000** (was stuck at 1000 — its stale pre-kill keyspace
— in the first attempt). The snapshot import (FLUSHDB+RESTORE) wiped the stale
state; incremental tail polling tracked the writes that continued after rejoin.

## The finding the live run caught (in-process tests could not)

**First attempt FAILED** (node 2 at DBSIZE 1000). Root cause: the CDK security
group predated the state-transfer server — it allowed the ring port **7000** but
**not 7001**, so the passive rejoiner's `fetch_state` to a survivor was silently
dropped and it never caught up. The in-process integration tests
(`tests/proxy_tls.rs`) use ephemeral localhost ports with no security group, so
they could not surface this — exactly the gap a live run exists to find.

**Fix:** opened 7001 intra-SG. Permanent fix committed to the CDK network stack
(`scripts/bench-aws/trains_bench/stacks/network.py` — 7001 added to the
intra-SG ingress list). Re-ran → PASS.

## Cost / teardown

3× t4g.small for ~1h ≈ a few cents (no LLM spend on the TRAINS bench).
Infrastructure torn down after the run (`cdk destroy`); the interim
`<aws-profile>` profile was used locally only.

## Provenance

- v2 design: `trains-rust/docs/ADR-001-rejoin-virtual-synchrony-2026-06-15.md`
- v3 spec (ReAdmit, TLC-verified): trains-rust PR #13
- Prior live FAIL: `bench/results/ec2-2026-06-15-e5-matrix/REPORT.md`
- Run summary: `bench/results/ec2-2026-06-16-e5-matrix/summary.txt`
  (line 1 = FAIL pre-SG-fix, line 2 = PASS).
