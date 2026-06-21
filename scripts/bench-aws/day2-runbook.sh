#!/usr/bin/env bash
# Day-2 runbook (2026-05-27 build follow-up).
# Operator-driven turnkey wrapper around the two open EC2 experiments:
#
#   E4-clean — fresh-deploy-per-rate Sentinel rate-threshold sweep.
#              Closes the one remaining v1.0 paper checklist item.
#   demos    — distributed-lock + leaderboard demos driven through the
#              trains-valkey proxy ring with a mid-workload SIGKILL.
#              Adds a new §6.5 contribution to the paper.
#
# Per the chaos runbook (bench/reports/trains-valkey-ec2-chaos-runbook-2026-05-25.md §7),
# **this is NOT an unattended one-shot**. Run it in a focused babysat
# session so nothing is left provisioned on a failure.
#
# Usage:
#   AWS_PROFILE=<profile> ./scripts/bench-aws/day2-runbook.sh prereqs
#   AWS_PROFILE=<profile> ./scripts/bench-aws/day2-runbook.sh deploy
#   AWS_PROFILE=<profile> ./scripts/bench-aws/day2-runbook.sh demos
#   AWS_PROFILE=<profile> ./scripts/bench-aws/day2-runbook.sh e4-clean
#   AWS_PROFILE=<profile> ./scripts/bench-aws/day2-runbook.sh teardown
#
# Recommended order: prereqs → deploy → demos → e4-clean → teardown.
# Total wall clock ~3-4 h. Spend ~$0.40 EC2 + trivial S3/SSM.
#
# Each subcommand is idempotent and prints clearly what it did. If you
# need to abort, jump straight to `teardown` — the bench bucket has
# `auto_delete_objects=True`, so cdk destroy cleans S3 too.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS_DIR="$REPO/bench/results"
TODAY="2026-05-27"

# ── Prereq checks ───────────────────────────────────────────────────────────

cmd_prereqs() {
  echo "=== Day-2 prerequisite check ==="

  local missing=0

  command -v aws  >/dev/null || { echo "  MISSING: aws cli"; missing=1; }
  command -v cdk  >/dev/null || { echo "  MISSING: aws-cdk (npm i -g aws-cdk)"; missing=1; }
  command -v jq   >/dev/null || { echo "  MISSING: jq"; missing=1; }
  command -v cross >/dev/null || echo "  WARN: cross not on PATH (build-linux.sh will fall back)"
  command -v podman >/dev/null || command -v docker >/dev/null || \
    { echo "  MISSING: podman or docker (needed by 'cross' to cross-compile)"; missing=1; }

  echo
  echo "AWS identity:"
  aws sts get-caller-identity --query 'Arn' --output text \
    || { echo "  AWS_PROFILE=${AWS_PROFILE:-unset} cannot call STS — abort"; missing=1; }

  echo
  echo "Required IAM scope (informational — verify in your account):"
  echo "  - cloudformation:*  (cdk deploy / destroy)"
  echo "  - ec2:*             (VPC, instances, security groups)"
  echo "  - s3:*              (bench bucket + access-log bucket; PR-SEC-D additions)"
  echo "  - iam:*Role*        (BenchInstanceRole)"
  echo "  - ssm:*             (Session Manager + RunCommand)"
  echo "  - logs:*            (CloudWatch for SSM)"
  echo "  The BedrockAccess IAM user does NOT have these by default."
  echo "  Use an operator-tier role/profile (e.g. AdministratorAccess for the bench)."

  echo
  echo "Local repo:"
  echo "  HEAD: $(git -C "$REPO" rev-parse --short HEAD) on $(git -C "$REPO" branch --show-current)"
  echo "  Uncommitted files:"
  git -C "$REPO" status -s | head -10 | sed 's/^/    /'

  if [ "$missing" -ne 0 ]; then
    echo
    echo "ABORT: prerequisites missing"
    exit 1
  fi

  echo
  echo "Prereqs OK. Recommended next step: $0 deploy"
}

# ── Deploy CDK + cross-build + upload binaries ──────────────────────────────

cmd_deploy() {
  echo "=== Deploy: build linux binaries + cdk deploy + upload binaries ==="
  echo
  echo "Note: PR-SEC-D adds VersioningConfiguration + LoggingConfiguration to"
  echo "the bench bucket. The first 'cdk deploy' after merging PR-SEC-D will"
  echo "show those additions in 'cdk diff'."
  echo
  "$SCRIPT_DIR/deploy.sh"
  echo
  echo "Deploy complete. CDK outputs:"
  jq '.' "$SCRIPT_DIR/cdk-outputs.json"
  echo
  echo "Recommended next step: $0 demos  (or e4-clean if you want the v1.0 closer first)"
}

# ── Demo-app EC2 chaos: lock + leader through the proxy ring with mid-kill ──

cmd_demos() {
  echo "=== Demo-app EC2 chaos (track 5 of 2026-05-27 build, deferred from local) ==="
  echo

  local OUT="$RESULTS_DIR/demo-apps-$TODAY-ec2"
  mkdir -p "$OUT/lock" "$OUT/leader"

  local BUCKET INST_IDS
  BUCKET=$(jq -r '.. | objects | select(.BenchBucketName?) | .BenchBucketName' "$SCRIPT_DIR/cdk-outputs.json" | head -1)
  INST_IDS=( $(jq -r '.. | objects | to_entries[] | select(.key | startswith("InstanceId")) | .value' "$SCRIPT_DIR/cdk-outputs.json") )

  if [ -z "$BUCKET" ] || [ ${#INST_IDS[@]} -ne 3 ]; then
    echo "ABORT: expected 1 bucket + 3 instance IDs in cdk-outputs.json"
    echo "       Bucket: '$BUCKET', InstanceIds: ${#INST_IDS[@]}"
    exit 1
  fi

  echo "Bucket:       $BUCKET"
  echo "Instances:    ${INST_IDS[*]}"
  echo "Output dir:   $OUT"
  echo

  # 1. Upload both demos to S3 so each instance can fetch them.
  echo "Step 1/5  upload demo Python sources to S3"
  aws s3 cp "$REPO/bench/demos/distributed-lock/lock_chaos.py"   "s3://$BUCKET/demos/lock_chaos.py"   --quiet
  aws s3 cp "$REPO/bench/demos/leaderboard/leader_chaos.py"      "s3://$BUCKET/demos/leader_chaos.py" --quiet

  # 2. Run each demo: load phase on coordinator (instance 0), mid-load SIGKILL
  #    on victim (instance 2), then verify-local on every survivor.
  local NODE_0_IP NODE_0_PRIVATE_IP
  NODE_0_PRIVATE_IP=$(jq -r '.. | objects | to_entries[] | select(.key == "PrivateIp00") | .value' "$SCRIPT_DIR/cdk-outputs.json")
  NODE_0_RESP_PORT=7000  # the trains-valkey proxy listens on 7000; engine on 6379

  for demo in lock leader; do
    echo
    echo "Step 2/5  ($demo) phase-1 load against $NODE_0_PRIVATE_IP:$NODE_0_RESP_PORT"
    aws ssm send-command \
      --instance-ids "${INST_IDS[0]}" \
      --document-name "AWS-RunShellScript" \
      --comment "day2-runbook: $demo demo load phase 1" \
      --parameters "commands=[
        'aws s3 cp s3://$BUCKET/demos/${demo}_chaos.py /opt/trains/${demo}_chaos.py --quiet',
        'cd /opt/trains && nohup python3 ${demo}_chaos.py --mode load --host $NODE_0_PRIVATE_IP --port $NODE_0_RESP_PORT --workers 4 --duration 30 --acked-out /opt/trains/${demo}-acked.json > /opt/trains/${demo}-load.log 2>&1 &'
      ]" \
      --output text --query 'Command.CommandId'

    echo "Step 3/5  ($demo) sleep 10s, then SIGKILL the proxy on victim (instance 2)"
    sleep 10
    aws ssm send-command \
      --instance-ids "${INST_IDS[2]}" \
      --document-name "AWS-RunShellScript" \
      --comment "day2-runbook: $demo demo SIGKILL victim" \
      --parameters 'commands=["sudo pkill -9 -f trains-cli || true"]' \
      --output text --query 'Command.CommandId'

    echo "Step 4/5  ($demo) wait for load to finish (30s + 10s margin)"
    sleep 30

    echo "Step 5/5  ($demo) collect acked.json from coordinator, verify on each survivor"
    aws ssm send-command \
      --instance-ids "${INST_IDS[0]}" \
      --document-name "AWS-RunShellScript" \
      --parameters "commands=['aws s3 cp /opt/trains/${demo}-acked.json s3://$BUCKET/results/${demo}-acked.json --quiet']" \
      --output text --query 'Command.CommandId'
    sleep 5
    aws s3 cp "s3://$BUCKET/results/${demo}-acked.json" "$OUT/$demo/acked.json" --quiet

    # Survivors are instances 0 and 1 (2 was killed). Verify on each.
    for i in 0 1; do
      echo "          verify-local on instance ${INST_IDS[$i]}"
      aws ssm send-command \
        --instance-ids "${INST_IDS[$i]}" \
        --document-name "AWS-RunShellScript" \
        --parameters "commands=[
          'aws s3 cp s3://$BUCKET/results/${demo}-acked.json /opt/trains/${demo}-acked.json --quiet',
          'cd /opt/trains && python3 ${demo}_chaos.py --mode verify-local --host 127.0.0.1 --port 6379 --acked-in ${demo}-acked.json --report-out /opt/trains/${demo}-report-node-${i}.json',
          'aws s3 cp /opt/trains/${demo}-report-node-${i}.json s3://$BUCKET/results/${demo}-report-node-${i}.json --quiet'
        ]" \
        --output text --query 'Command.CommandId'
      sleep 5
      aws s3 cp "s3://$BUCKET/results/${demo}-report-node-${i}.json" "$OUT/$demo/report-node-${i}.json" --quiet
    done

    echo
    echo "Demo '$demo' done. Reports:"
    for i in 0 1; do
      echo "  $OUT/$demo/report-node-${i}.json"
      jq '.' "$OUT/$demo/report-node-${i}.json"
    done
  done

  echo
  echo "Both demos done. Next: $0 e4-clean  (or $0 teardown to stop spend)"
}

# ── E4 clean rate-threshold sweep (fresh-deploy-per-rate) ───────────────────

cmd_e4_clean() {
  echo "=== E4 clean rate-threshold sweep (fresh-deploy-per-rate variant) ==="
  echo
  echo "Approach: 4 rates back-to-back, each preceded by a Sentinel-cluster"
  echo "  reset (restart-victim-as-replica is cheaper than a fresh cdk deploy)."
  echo "  Rates: 50, 500, 1000, 2000 wr/s. ~30 s of writes per rate."
  echo "  Plan: bench/reports/e4-throughput-sweep-plan-2026-05-26.md"
  echo "  Driver: bench/coordinator/e4_chaos.py (promoted from /tmp on Day-3)"
  echo
  echo "This step is the v1.0 closer for the paper. After it lands, drop the"
  echo "'v0.9 RC' banner in paper-trains-replicated-redis-draft-2026-05-26.md."
  echo

  local OUT="$RESULTS_DIR/ec2-$TODAY-e4-clean"
  mkdir -p "$OUT"
  echo "Output dir: $OUT"
  echo

  local BUCKET INST_IDS NODE_0_PRIVATE_IP
  BUCKET=$(jq -r '.. | objects | select(.BenchBucketName?) | .BenchBucketName' "$SCRIPT_DIR/cdk-outputs.json" | head -1)
  INST_IDS=( $(jq -r '.. | objects | to_entries[] | select(.key | startswith("InstanceId")) | .value' "$SCRIPT_DIR/cdk-outputs.json") )
  NODE_0_PRIVATE_IP=$(jq -r '.. | objects | to_entries[] | select(.key == "PrivateIp00") | .value' "$SCRIPT_DIR/cdk-outputs.json")
  if [ -z "$BUCKET" ] || [ ${#INST_IDS[@]} -ne 3 ]; then
    echo "ABORT: expected 1 bucket + 3 instance IDs in cdk-outputs.json"
    exit 1
  fi

  # 1. Upload the driver to S3 once.
  echo "Step 0/N  upload e4_chaos.py to S3"
  aws s3 cp "$REPO/bench/coordinator/e4_chaos.py" "s3://$BUCKET/coordinator/e4_chaos.py" --quiet

  # 2. Per-rate loop: reset → chaos client → mid-load primary kill → collect.
  local rate
  for rate in 50 500 1000 2000; do
    echo
    echo "──── rate $rate wr/s ────"
    local count=$(( rate * 30 ))   # 30 s of sustained writes per rate

    echo "Step 1  reset Sentinel cluster (restart valkey-server on all 3 nodes)"
    for i in 0 1 2; do
      aws ssm send-command \
        --instance-ids "${INST_IDS[$i]}" \
        --document-name "AWS-RunShellScript" \
        --comment "e4-clean r${rate}: reset valkey on node $i" \
        --parameters 'commands=["sudo systemctl restart valkey-server || true", "sleep 2", "sudo systemctl restart valkey-sentinel || true"]' \
        --output text --query 'Command.CommandId' >/dev/null
    done
    echo "          waiting 10s for Sentinel quorum to stabilise"
    sleep 10

    echo "Step 2  start chaos client on instance 0 (rate=$rate, count=$count, pipeline=auto)"
    aws ssm send-command \
      --instance-ids "${INST_IDS[0]}" \
      --document-name "AWS-RunShellScript" \
      --comment "e4-clean r${rate}: chaos load" \
      --parameters "commands=[
        'aws s3 cp s3://$BUCKET/coordinator/e4_chaos.py /opt/trains/e4_chaos.py --quiet',
        'nohup python3 /opt/trains/e4_chaos.py --target sentinel://$NODE_0_PRIVATE_IP:26379 --master-name mymaster --rate $rate --count $count --pipeline 10 --acked-out /opt/trains/e4-r${rate}-acked.json --latency-out /opt/trains/e4-r${rate}-latency.json > /opt/trains/e4-r${rate}.log 2>&1 &'
      ]" \
      --output text --query 'Command.CommandId' >/dev/null

    echo "Step 3  T+5s: SIGKILL the Sentinel primary on instance 2"
    sleep 5
    aws ssm send-command \
      --instance-ids "${INST_IDS[2]}" \
      --document-name "AWS-RunShellScript" \
      --comment "e4-clean r${rate}: kill primary" \
      --parameters 'commands=["sudo pkill -9 -f valkey-server || true"]' \
      --output text --query 'Command.CommandId' >/dev/null

    echo "Step 4  wait for the 30s workload to finish + 10s margin"
    sleep 35

    echo "Step 5  collect acked + latency JSON"
    aws ssm send-command \
      --instance-ids "${INST_IDS[0]}" \
      --document-name "AWS-RunShellScript" \
      --parameters "commands=[
        'aws s3 cp /opt/trains/e4-r${rate}-acked.json s3://$BUCKET/results/e4-r${rate}-acked.json --quiet',
        'aws s3 cp /opt/trains/e4-r${rate}-latency.json s3://$BUCKET/results/e4-r${rate}-latency.json --quiet'
      ]" \
      --output text --query 'Command.CommandId' >/dev/null
    sleep 5
    aws s3 cp "s3://$BUCKET/results/e4-r${rate}-acked.json"   "$OUT/acked-r${rate}.json"   --quiet || echo "WARN: r${rate} acked.json missing (cluster may be dead)"
    aws s3 cp "s3://$BUCKET/results/e4-r${rate}-latency.json" "$OUT/latency-r${rate}.json" --quiet || echo "WARN: r${rate} latency.json missing"

    # Loss computation: count - acked, expressed as a % of count.
    local acked
    acked=$(jq 'length' "$OUT/acked-r${rate}.json" 2>/dev/null || echo 0)
    local loss=$(( count - acked ))
    local pct=$(awk "BEGIN { printf \"%.2f\", 100 * $loss / $count }")
    echo "          rate=$rate  count=$count  acked=$acked  loss=$loss  loss_pct=$pct%"
    echo "$rate,$count,$acked,$loss,$pct" >> "$OUT/summary.csv"
  done

  echo
  echo "Summary (also in $OUT/summary.csv):"
  printf "%-8s %-8s %-8s %-8s %-8s\n" "rate" "count" "acked" "loss" "loss_pct"
  while IFS=, read -r r c a l p; do
    printf "%-8s %-8s %-8s %-8s %-8s\n" "$r" "$c" "$a" "$l" "$p%"
  done < "$OUT/summary.csv"
  echo
  echo "Write up: bench/results/ec2-$TODAY-e4-clean/REPORT.md"
  echo "Recommended next step: $0 teardown"
}

# ── Teardown ────────────────────────────────────────────────────────────────

cmd_teardown() {
  echo "=== Teardown: cdk destroy --all + verify ==="
  echo
  echo "This will delete every stack with the TrainsBench prefix AND empty"
  echo "the bench + bench-logs buckets (auto_delete_objects=True on both)."
  echo
  "$SCRIPT_DIR/teardown.sh"
  echo
  echo "Verifying nothing remains:"
  aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE \
    --query "StackSummaries[?starts_with(StackName, 'TrainsBench')].StackName" \
    --output text
  echo
  echo "If the output above is empty, you're done. Total spend this session: < \$0.50 expected."
}

# ── Dispatch ────────────────────────────────────────────────────────────────

case "${1:-help}" in
  prereqs)   cmd_prereqs ;;
  deploy)    cmd_deploy ;;
  demos)     cmd_demos ;;
  e4-clean)  cmd_e4_clean ;;
  teardown)  cmd_teardown ;;
  help|*)
    echo "Day-2 runbook — turnkey wrapper for the 2026-05-27 follow-up experiments."
    echo
    echo "Usage:"
    echo "  AWS_PROFILE=<profile> $0 prereqs    — verify tools, AWS access, IAM scope"
    echo "  AWS_PROFILE=<profile> $0 deploy     — cross-build + cdk deploy + upload bins"
    echo "  AWS_PROFILE=<profile> $0 demos      — run lock + leader demos through proxy ring with mid-load kill"
    echo "  AWS_PROFILE=<profile> $0 e4-clean   — E4 rate-threshold sweep (fresh-deploy-per-rate)"
    echo "  AWS_PROFILE=<profile> $0 teardown   — cdk destroy + verify nothing remains"
    echo
    echo "Recommended order: prereqs → deploy → demos → e4-clean → teardown."
    echo "Total wall-clock ~3-4 h; spend < \$0.50."
    echo
    echo "If anything goes sideways, jump to '$0 teardown' to stop spend."
    ;;
esac
