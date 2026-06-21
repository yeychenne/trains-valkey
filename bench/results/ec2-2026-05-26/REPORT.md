# EC2 chaos run — 2026-05-26

**Topology:** `trains-valkey` 3-node ring on AWS EC2 `t4g.small` (arm64, AL2023),
eu-west-3 (Paris). Self-managed Valkey 9.0.3 per node, loopback. Region account
<account-id>; instance ids `<instance-id>` / `<instance-id>` /
`<instance-id>`. Total spend: < $0.05.

This is the **operator-gated at-scale confirmatory run** from
`bench/reports/trains-valkey-ec2-chaos-runbook-2026-05-25.md` — the last open
item from RD-1..4 (see `bench/EOD-2026-05-25.md`).

## Headline result

**200/200 acked writes preserved on every survivor.** The chaos workload
(`trains-valkey-chaos --mode load --count 200 --hold-secs 20`) drove `SET kN vN`
through node 0's RESP port, the proxy replicated each write across the
3-node ring, and `--mode verify-local` on each survivor confirmed:

| Engine | acked_total | missing_keys | dbsize |
|---|---|---|---|
| node-0 (10.0.0.211, eu-west-3a, alive) | 200 | [] | 200 |
| node-1 (10.0.2.175, eu-west-3c, alive) | 200 | [] | 200 |
| node-2 (10.0.0.99, eu-west-3a, proxy SIGKILLed at 10:11:35) | 200 | [] | 200 |

This is the **zero-acked-write-loss** property TRAINS replication was built to
provide and Redis async/Sentinel failover does *not* guarantee. The dead-proxy
node still has the full keyspace because the writes were applied to its
co-located Valkey *before* the proxy was killed; the survivors converged on the
identical keyspace (DBSIZE 200/200/200).

## What got tested (and what didn't)

### Confirmed at EC2 scale

- **Real-backend masking on a healthy ring**: Valkey 9.0.3 + the `RedisBackend`
  TLS proxy holds 200 writes across 3 nodes with no loss; DBSIZE matches
  byte-for-byte across the cluster.
- **`option-B` chaos pipeline**: `--mode load` writes to a proxy and emits an
  acked-set JSON, distributable via S3; `--mode verify-local` runs on each
  survivor against `127.0.0.1:6379` (engine stays loopback-only) and emits a
  per-engine `PartialReport`. No engine port was ever exposed off-host.
- **All the new EC2 plumbing**: CDK stack (network + 3× t4g.small +
  S3 + SG); the bootstrap (dnf valkey) + ring launch (`launch-node.sh`) +
  fis-kill-redis paths via SSM; arm64-Linux build under podman (no `cross`).

### Surfaced finding — not blocking, but worth fixing

**View-change recovery is slow at EC2 scale when the victim's *successor*
predecessor has nothing to send.** Re-running the chaos workload *after*
node 2's proxy had been killed (`acked-masked.json` workload) hung for >5
minutes without completing. Diagnosis:

- The transport's `unreachable_rx` channel only fires after
  `UNREACHABLE_FAILURES=5` *failed connect attempts* on the connector loop,
  which only runs when there's a message to forward (or pending after a
  broken send).
- On Linux, TCP `send` to a half-closed peer doesn't error immediately — the
  kernel retransmits silently until `TCP_USER_TIMEOUT` (or default
  retransmit budget, which can be 15+ min). So `wire_rx.recv()` keeps
  yielding messages and the connector keeps trying to *write* (not
  *connect*) — which means the `fail_streak` counter never increments and
  the unreachable signal is never sent.
- In-process tests (`crash_masking.rs`) don't exercise this: both peers are
  loopback sockets where the kernel reflects EPIPE immediately.

The healthy-ring chaos run was the documented headline; the failed
masked-crash run is a real EC2-only signal. Mitigation candidates (deferred,
not in scope of this run):

1. Set `TCP_USER_TIMEOUT` on the ring sockets (~3 s).
2. Add an application-level keepalive ping with explicit ack timeout that
   feeds the failure detector directly.
3. Surface a coordinator-driven `ConfirmCrash` hook so the operator can
   force view change when external evidence shows a node is gone.

A follow-up PR can pick the cheapest of these (option 1 is one socket
option call). Not relevant to the headline correctness story.

## Artifacts

- `acked.json` (200 entries) — every `(key, value)` pair the proxy ack'd
  with `+OK` during the chaos workload.
- `report-node-{0,1,2}.json` — per-engine `PartialReport` from
  `--mode verify-local`. All show `dbsize=200, missing_keys=[]`.

## Repro

Codepath now in main: build via `podman run --platform linux/arm64 …
rust:1-bookworm cargo build --release -p trains-valkey`; bootstrap via
`scripts/redis-chaos/{keygen,node-bootstrap,launch-node}.sh`; load + verify via
`trains-valkey-chaos --mode {load,verify-local}`. The CDK deltas (compute.py
INSTANCE_TYPE + AMI + AZ filter, app.py account env, cdk.json `bankx`
qualifier, network.py self-ref SG via `connections.allow_from`) are all in
`scripts/bench-aws/`. No Terraform anywhere.

## Cost + teardown

- 3× t4g.small × ~30 min @ $0.0168/hr ≈ $0.025
- S3 + SSM + CloudFormation: ~$0.01
- **Total: < $0.05.** Teardown step ran at the end of this session
  (`scripts/bench-aws/teardown.sh`); confirmed zero `TrainsBench*` stacks
  remain via `aws cloudformation list-stacks --region eu-west-3`.
