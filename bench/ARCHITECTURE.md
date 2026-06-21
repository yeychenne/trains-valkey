# TRAINS-bench — Architecture

## Overview

```
                    ┌─────────────────────────────────────────────┐
                    │                AWS / us-east-1               │
                    │                                              │
                    │   VPC 10.50.0.0/16 (no internet gateway)    │
                    │                                              │
   operator host ──▶│   ┌──────────────┐                          │
   (Mac Mini)       │   │  coordinator │  ── SSM RunCommand ─▶    │
                    │   │  t4g.micro   │                          │
                    │   │  AZ a        │                          │
                    │   └──────┬───────┘                          │
                    │          │                                  │
                    │          ▼ (download src, cross-compile)    │
                    │   ┌──────────────┐                          │
                    │   │  S3 bucket   │ ─── binary + identities ─┐
                    │   │  ─── results ◀── per-node JSON ◀────────┤
                    │   └──────────────┘                          │
                    │                                             │
                    │   ring nodes (3 × t4g.micro):               │
                    │   ┌────────┐   ┌────────┐   ┌────────┐      │
                    │   │ node-0 │──▶│ node-1 │──▶│ node-2 │──┐   │
                    │   │ AZ a   │   │ AZ a   │   │ AZ a   │  │   │
                    │   └────────┘   └────────┘   └────────┘  │   │
                    │       ▲────────────────────────────────-┘   │
                    │       (ring topology, QUIC + TLS)           │
                    │                                             │
                    │   Cluster Placement Group (single-AZ mode)  │
                    │   Spread Placement Group  (3-AZ mode)       │
                    └─────────────────────────────────────────────┘
```

## AWS resources

| Resource | Type | Purpose | Count |
|---|---|---|---|
| VPC | `aws_ec2.Vpc` | Dedicated `10.50.0.0/16` — no NAT, no IGW (SSM only) | 1 |
| Subnet | `aws_ec2.Subnet` | One /24 per AZ used | 1 (single) or 3 (3-AZ) |
| Security Group | `aws_ec2.SecurityGroup` | Allow ALL ports within SG; deny inbound from outside | 1 |
| Placement Group | `aws_ec2.PlacementGroup` | Cluster (single-AZ) or Spread (3-AZ) | 1 |
| EC2 ring nodes | `aws_ec2.Instance` (t4g.micro, AL2023 arm64) | TRAINS ring peers | `ringSize` (default 3) |
| EC2 coordinator | `aws_ec2.Instance` (same type) | Cross-compile, distribute, trigger, aggregate | 1 |
| S3 results bucket | `aws_s3.Bucket` | Versioned, lifecycle 90d, tagged Project=trains-bench | 1 |
| IAM instance profile | `aws_iam.Role` + `InstanceProfile` | SSM Managed Instance Core + S3 read/write on results bucket only | 1 |
| VPC endpoints | `aws_ec2.VpcEndpoint` (Interface) | `ssm`, `ec2messages`, `ssmmessages`, `s3` (Gateway) | 4 |

**No public IPs.** All instance access is via SSM Session Manager
(operator → coordinator) and SSM RunCommand (coordinator → ring nodes).
The instances reach S3 and SSM through VPC endpoints, never the
internet.

## Bench-control protocol

The operator never SSHs into any instance. The flow is:

```
1. operator (host):
   cdk deploy -c ringSize=3 -c azSpread=single
   └─ stack outputs include CoordinatorInstanceId, ResultsBucket

2. operator (host):
   aws ssm start-session --target <CoordinatorInstanceId>
   └─ optional — only to tail coordinator logs

3. operator (host) or coordinator (auto on first boot via UserData):
   python3 /opt/bench/coordinator.py run \
     --duration 30s --message-count 1000 --payload-size 1024
   └─ this is the bench TRIGGER; happens AFTER cdk deploy succeeds

4. coordinator (in-AWS):
   a. Discover ring peers by EC2 tag (boto3 describe_instances).
   b. Build trains-cli on coordinator (cross-compile already done at boot).
   c. Upload binary + per-node identity JSON to S3.
   d. For each ring node: SSM RunCommand "AWS-RunShellScript"
        → download binary from S3
        → start `trains-cli node --id N --listen ... --successor ... --identity ...`
        → background process; PID written to /tmp/trains-cli.pid
   e. Wait for all nodes to log "ring established" (poll SSM command status).
   f. SSM RunCommand the issuer node:
        → driver script pipes 1000 timestamped broadcasts to trains-cli stdin
        → trains-cli's stderr (with structured tracing) is captured
        → all logs upload to S3
   g. After duration: SSM RunCommand all nodes:
        → kill -SIGINT $(cat /tmp/trains-cli.pid)
        → parse trains-cli stderr for "Deliver" events
        → emit per-message latency JSON to S3
        → run iperf3 between adjacent peers (5s baseline)
   h. Coordinator downloads all per-node JSON from S3, aggregates,
      writes bench/results/run-<timestamp>.{json,csv,md}.

5. operator (host):
   aws s3 cp s3://<ResultsBucket>/run-<timestamp>.md .
   open run-<timestamp>.md

6. operator (host):
   cdk destroy
   └─ verify teardown clean via describe-instances tag filter
```

## Cross-compile flow

Bench ring nodes are `t4g.micro` (arm64). They have **no rustup**.
Cross-compile happens on the **coordinator** instance during UserData
bootstrap (one-shot, ~3 min):

```bash
# UserData on coordinator:
dnf install -y gcc git
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    --default-toolchain stable --profile minimal
source $HOME/.cargo/env
# Target == host (arm64 → arm64), so no cross-compile toolchain needed.
git clone https://github.com/yeychenne/trains-rust.git /opt/trains-src
cd /opt/trains-src
cargo build --release --bin trains
aws s3 cp target/release/trains s3://${RESULTS_BUCKET}/bin/trains
```

**Why on the coordinator and not in CI?** The bench can be re-run
against any TRAINS git commit by editing the UserData (or the
coordinator's `coordinator.py --rebuild` flag). CI-built artefacts
would couple bench iteration to a separate pipeline.

Both ring nodes and coordinator are arm64, so "cross-compile" is a
misnomer — it's a native arm64 build of trains-cli on the coordinator,
then `aws s3 cp` to ring nodes via SSM. The binary is statically linked
(via `s2n-quic` + `rustls` + `ring`) so no glibc-version pinning is
needed.

## Bench-control IAM

The coordinator's instance profile needs:

| Permission | Resource | Why |
|---|---|---|
| `ec2:DescribeInstances` | `*` (filter on tag) | Discover ring peers |
| `ssm:SendCommand` | ring node instances (by tag) + `AWS-RunShellScript` document | Trigger bench on each peer |
| `ssm:GetCommandInvocation` | `*` | Poll command status |
| `s3:PutObject` / `GetObject` / `ListBucket` | `<results-bucket>/*` | Upload binary, download per-node JSON, write final report |
| `ce:GetCostAndUsage` | `*` | Pre-flight budget check (read-only) |

Ring nodes' instance profile needs:

| Permission | Resource | Why |
|---|---|---|
| `ssmmanagedinstancecore` | (managed policy) | SSM agent registration |
| `s3:GetObject` | `<results-bucket>/bin/*` | Download trains-cli binary |
| `s3:PutObject` | `<results-bucket>/results/<instance-id>/*` | Upload per-node logs |

Operator's IAM identity (whoever runs `cdk deploy`) needs the standard
CDK deploy permissions plus `ssm:StartSession` to optionally tail
coordinator logs. The operator NEVER needs `ec2:RunInstances` after
deploy — bench trigger is via SSM, not direct EC2.

## Failure modes

| Failure | Detection | Mitigation |
|---|---|---|
| Ring node SSM agent not registered | `ssm:DescribeInstanceInformation` empty after 2 min | Coordinator aborts run; deployment-failed.md emitted |
| Cross-compile fails on coordinator | UserData exit non-zero | Cloud-init logs in `/var/log/cloud-init-output.log`; coordinator marks ready=false |
| Binary fails on ring node | SSM Command status `Failed` with stderr | Bench aborts; coordinator pulls cloud-init + SSM stderr to S3 for forensics |
| TLS handshake fails (fingerprint mismatch) | trains-cli's first 10 s of stderr | Bench aborts; identities were generated by coordinator and pinned via env var, so this should not happen — investigate via `tracing` logs in S3 |
| iperf3 baseline fails | iperf3 exit non-zero | Recorded as `baseline_ok=false` in the run report; TRAINS results still aggregated |
| `cdk destroy` leaves orphaned resources | Post-teardown `describe-instances` tag query | Operator manually deletes; this is the safety net behind AC-6 |

## Trade-offs and decisions

| Decision | Alternative considered | Why this |
|---|---|---|
| QUIC (s2n-quic) transport from trains-net | Custom TCP + bincode | Use the upstream's actual transport — bench reflects what gets deployed |
| Coordinator + ring as separate instances | Coordinator co-located on ring node 0 | Clean separation: a coordinator failure doesn't kill the ring; SSM commands have a clear "from" |
| SSM RunCommand for bench trigger | SSH | No SSH keys to manage; no public IPs; SSM is audited automatically via CloudTrail |
| S3 results bucket per deploy | Single long-lived bucket | Lifecycle 90d on the per-deploy bucket avoids accidental retention; bucket name has a stack suffix |
| iperf3 baseline always | TRAINS-only metrics | Without a TCP-level reference, a "slow" TRAINS number is unattributable to network vs protocol |
| No CloudWatch agent | Detailed CW metrics | Adds ~$0.30/instance/run; the bench is short (30s); per-node JSON in S3 is sufficient |
