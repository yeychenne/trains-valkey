#!/usr/bin/env python3
"""TRAINS AWS ring benchmark coordinator.

Orchestrates a multi-node ring benchmark on EC2 instances using SSM.

Usage:
    python3 coordinator.py --ring-sizes 3 10 15 --duration 30 --payload-size 64

Phases for each ring size N:
  1. Select first N instances from the CDK stack outputs.
  2. Generate N identities locally (requires local `trains` binary).
  3. Upload identities to S3.
  4. Via SSM: download binary + identity, start `trains node`.
  5. Wait for ring stabilization (30 s).
  6. Via SSM on node 0: inject broadcasts for `--duration` seconds.
  7. Via SSM: stop nodes, collect DELIVER counts, upload results to S3.
  8. Download results, compute throughput, emit JSONL.

Prerequisites:
  - `./deploy.sh` already ran (CDK stacks up, binaries in S3).
  - Local `trains` binary available (for keygen).
  - AWS credentials configured (via profile or env).
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import boto3

SCRIPT_DIR = Path(__file__).parent
REPO = SCRIPT_DIR.parent.parent
CDK_OUTPUTS = SCRIPT_DIR / "cdk-outputs.json"
DIST_DIR = SCRIPT_DIR / "dist"

STARTUP_WAIT_S = 40   # seconds to wait for the ring to stabilize
DRAIN_WAIT_S = 5      # seconds to let the ring drain after broadcast flood
REGION = "eu-west-3"


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_cdk_outputs() -> dict:
    with open(CDK_OUTPUTS) as f:
        raw = json.load(f)
    flat: dict = {}
    for stack_vals in raw.values():
        flat.update(stack_vals)
    return flat


def local_trains_binary() -> Path:
    # Try repo target first, then PATH.
    candidates = [
        REPO / "target" / "release" / "trains",
        REPO / "target" / "x86_64-apple-darwin" / "release" / "trains",
        Path("trains"),
    ]
    for c in candidates:
        if c.exists() and os.access(c, os.X_OK):
            return c
    # Try which
    result = subprocess.run(["which", "trains"], capture_output=True, text=True)
    if result.returncode == 0:
        return Path(result.stdout.strip())
    raise RuntimeError(
        "Local `trains` binary not found. "
        "Run `cargo build --release -p trains-cli` first."
    )


def keygen(trains_bin: Path, node_id: int, ip: str, out_path: Path) -> str:
    """Generate identity for node_id; return hex fingerprint."""
    result = subprocess.run(
        [str(trains_bin), "keygen", "--out", str(out_path),
         "--sni", ip, "--sni", "localhost"],
        capture_output=True, text=True, check=True,
    )
    for line in result.stdout.splitlines():
        if line.startswith("fingerprint:"):
            return line.split(":")[1].strip()
    raise RuntimeError(f"keygen did not print fingerprint:\n{result.stdout}")


def ssm_run(ssm, instance_id: str, commands: list[str], timeout: int = 120) -> str:
    """Run commands on instance via SSM; return stdout."""
    resp = ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": commands, "executionTimeout": [str(timeout)]},
    )
    cmd_id = resp["Command"]["CommandId"]
    deadline = time.time() + timeout + 30
    while time.time() < deadline:
        time.sleep(4)
        inv = ssm.get_command_invocation(
            CommandId=cmd_id, InstanceId=instance_id
        )
        status = inv["Status"]
        if status in ("Success", "Failed", "Cancelled", "TimedOut"):
            if status != "Success":
                raise RuntimeError(
                    f"SSM command {status} on {instance_id}:\n"
                    f"stdout: {inv.get('StandardOutputContent','')}\n"
                    f"stderr: {inv.get('StandardErrorContent','')}"
                )
            return inv.get("StandardOutputContent", "")
    raise TimeoutError(f"SSM command timed out on {instance_id}")


def ssm_run_background(ssm, instance_id: str, commands: list[str]) -> str:
    """Fire SSM command without waiting (for background node processes)."""
    resp = ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={
            "commands": commands,
            "executionTimeout": ["600"],
        },
    )
    return resp["Command"]["CommandId"]


# ── Benchmark pass ────────────────────────────────────────────────────────────

def run_ring_bench(
    ring_size: int,
    instance_ids: list[str],
    private_ips: list[str],
    bucket: str,
    duration: int,
    payload_size: int,
    trains_bin: Path,
    ssm,
    s3,
    timestamp: str,
) -> dict:
    """Run a single ring-size benchmark; return result dict."""
    n = ring_size
    ids = instance_ids[:n]
    ips = private_ips[:n]
    print(f"\n{'='*60}")
    print(f"  Ring size: {n}  |  Duration: {duration}s  |  Payload: {payload_size}B")
    print(f"{'='*60}")

    # ── 1. Generate identities locally ──────────────────────────────────
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        fingerprints: list[str] = []
        identity_paths: list[Path] = []

        print(f"  Generating {n} identities...")
        for i in range(n):
            out = tmp / f"node{i}.json"
            fp = keygen(trains_bin, i, ips[i], out)
            fingerprints.append(fp)
            identity_paths.append(out)
        all_fps = ",".join(fingerprints)

        # ── 2. Upload identities to S3 ───────────────────────────────────
        prefix = f"bench-{timestamp}/ring{n}"
        print(f"  Uploading identities to s3://{bucket}/{prefix}/identities/")
        for i, path in enumerate(identity_paths):
            s3.upload_file(
                str(path),
                bucket,
                f"{prefix}/identities/node{i}.json",
            )

    # ── 3. Start nodes via SSM ───────────────────────────────────────────
    print(f"  Starting {n} nodes via SSM...")
    cmd_ids: list[str] = []
    for i in range(n):
        successor_ip = ips[(i + 1) % n]
        issue_flag = "--issue-initial" if i == 0 else ""
        binary_key = f"binaries/trains-{n}"

        cmds = [
            f"aws s3 cp s3://{bucket}/{binary_key} /opt/trains/trains --region {REGION}",
            "chmod +x /opt/trains/trains",
            f"aws s3 cp s3://{bucket}/{prefix}/identities/node{i}.json "
            f"/opt/trains/identity.json --region {REGION}",
            "mkdir -p /opt/trains/state",
            "rm -f /opt/trains/node.in",
            "mkfifo /opt/trains/node.in",
            "exec 3<>/opt/trains/node.in",
            f"export RUST_LOG=warn",
            f"stdbuf -oL -eL /opt/trains/trains node "
            f"  --id {i} "
            f"  --listen 0.0.0.0:9000 "
            f"  --successor {successor_ip}:9000 "
            f"  --identity /opt/trains/identity.json "
            f"  --peer-fp '{all_fps}' "
            f"  {issue_flag} "
            f"  </opt/trains/node.in >/opt/trains/node.out 2>/opt/trains/node.err &",
            f"echo $! > /opt/trains/node.pid",
            f"echo node{i} started",
        ]
        cid = ssm_run_background(ssm, ids[i], cmds)
        cmd_ids.append(cid)

    # ── 4. Wait for ring stabilization ──────────────────────────────────
    print(f"  Waiting {STARTUP_WAIT_S}s for ring to stabilize...")
    time.sleep(STARTUP_WAIT_S)

    # ── 5. Inject broadcasts on node 0 for `duration` seconds ───────────
    payload = "A" * payload_size
    print(f"  Injecting {payload_size}B broadcasts on node 0 for {duration}s...")
    inject_cmds = [
        f"START=$SECONDS",
        f"COUNT=0",
        f"while [ $((SECONDS - START)) -lt {duration} ]; do",
        f"  echo '{payload}' >> /opt/trains/node.in",
        f"  COUNT=$((COUNT + 1))",
        f"done",
        f"echo \"injected $COUNT broadcasts\"",
    ]
    inject_out = ssm_run(ssm, ids[0], inject_cmds, timeout=duration + 60)
    print(f"  {inject_out.strip()}")

    # ── 6. Drain and stop ─────────────────────────────────────────────────
    print(f"  Draining ring ({DRAIN_WAIT_S}s)...")
    time.sleep(DRAIN_WAIT_S)

    # Kill nodes and collect DELIVER counts.
    delivered_total = 0
    for i in range(n):
        stop_cmds = [
            "PID=$(cat /opt/trains/node.pid 2>/dev/null || echo '')",
            "[ -n \"$PID\" ] && kill $PID 2>/dev/null || true",
            # Count DELIVER lines (each = one delivered broadcast).
            "DELIVERED=$(grep -c '^DELIVER' /opt/trains/node.out 2>/dev/null || echo 0)",
            "echo \"node_delivers=$DELIVERED\"",
            # Upload per-node log to S3 for audit.
            f"aws s3 cp /opt/trains/node.out "
            f"  s3://{bucket}/{prefix}/logs/node{i}.out --region {REGION} 2>/dev/null || true",
        ]
        try:
            out = ssm_run(ssm, ids[i], stop_cmds, timeout=60)
            for line in out.splitlines():
                if line.startswith("node_delivers="):
                    delivered_total += int(line.split("=")[1].strip())
                    break
        except Exception as exc:
            print(f"  Warning: could not collect results from node {i}: {exc}")

    # Average delivered per node (all nodes deliver the same set in TRAINS).
    avg_delivered = delivered_total // n if n else 0
    throughput = avg_delivered / (duration + DRAIN_WAIT_S)

    result = {
        "ring_size": n,
        "duration_s": duration,
        "payload_bytes": payload_size,
        "avg_delivered_per_node": avg_delivered,
        "throughput_msg_s": round(throughput, 1),
        "throughput_kbps": round(throughput * payload_size / 1024, 1),
        "timestamp": timestamp,
    }

    # ── 7. Upload result to S3 ────────────────────────────────────────────
    result_json = json.dumps(result) + "\n"
    s3.put_object(
        Bucket=bucket,
        Key=f"results/ring{n}-{timestamp}.jsonl",
        Body=result_json.encode(),
    )
    print(
        f"  Result: {avg_delivered} delivered | "
        f"{throughput:.1f} msg/s | "
        f"{result['throughput_kbps']:.1f} KiB/s"
    )
    return result


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="TRAINS AWS ring benchmark coordinator")
    parser.add_argument("--ring-sizes", nargs="+", type=int, default=[3, 10, 15])
    parser.add_argument("--duration", type=int, default=30,
                        help="seconds to inject broadcasts per ring size")
    parser.add_argument("--payload-size", type=int, default=64,
                        help="broadcast payload size in bytes")
    parser.add_argument(
        "--profile",
        default=os.environ.get("AWS_PROFILE"),
        help="AWS profile to use. Defaults to $AWS_PROFILE (the day2-runbook / "
             "OPS-E5 pattern is 'AWS_PROFILE=trains-run ...'); if unset, boto3's "
             "default credential chain is used. The old 'workshop-profile' default "
             "is retired (2026-05-25).",
    )
    args = parser.parse_args()

    outputs = load_cdk_outputs()
    bucket = outputs["BenchBucketName"]
    max_nodes = int(outputs.get("MaxNodes", 15))

    instance_ids: list[str] = []
    private_ips: list[str] = []
    for i in range(max_nodes):
        key_id = f"InstanceId{i:02d}"
        key_ip = f"PrivateIp{i:02d}"
        if key_id in outputs and key_ip in outputs:
            instance_ids.append(outputs[key_id])
            private_ips.append(outputs[key_ip])

    max_requested = max(args.ring_sizes)
    if max_requested > len(instance_ids):
        print(
            f"ERROR: ring size {max_requested} requested but only "
            f"{len(instance_ids)} instances available.",
            file=sys.stderr,
        )
        sys.exit(1)

    trains_bin = local_trains_binary()
    timestamp = time.strftime("%Y%m%d-%H%M%S")

    session = boto3.Session(profile_name=args.profile, region_name=REGION)
    ssm = session.client("ssm")
    s3 = session.client("s3")

    results: list[dict] = []
    for ring_size in args.ring_sizes:
        result = run_ring_bench(
            ring_size=ring_size,
            instance_ids=instance_ids,
            private_ips=private_ips,
            bucket=bucket,
            duration=args.duration,
            payload_size=args.payload_size,
            trains_bin=trains_bin,
            ssm=ssm,
            s3=s3,
            timestamp=timestamp,
        )
        results.append(result)

    # ── Summary table ─────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  TRAINS AWS Benchmark Results — {timestamp}")
    print(f"  Region: {REGION}  |  Payload: {args.payload_size}B  |  Duration: {args.duration}s/ring")
    print(f"{'='*60}")
    print(f"  {'Nodes':>6}  {'Delivered':>10}  {'msg/s':>10}  {'KiB/s':>10}")
    print(f"  {'-'*46}")
    for r in results:
        print(
            f"  {r['ring_size']:>6}  "
            f"{r['avg_delivered_per_node']:>10}  "
            f"{r['throughput_msg_s']:>10.1f}  "
            f"{r['throughput_kbps']:>10.1f}"
        )
    print(f"{'='*60}")

    # Write local JSONL summary.
    summary_path = SCRIPT_DIR / f"results-{timestamp}.jsonl"
    with open(summary_path, "w") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")
    print(f"\nResults written to {summary_path}")
    print(f"Full logs at s3://{bucket}/results/")


if __name__ == "__main__":
    main()
