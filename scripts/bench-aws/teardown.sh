#!/usr/bin/env bash
# Destroy all TRAINS benchmark AWS resources (CDK stacks + S3 bucket).
#
# IMPORTANT: This deletes the results S3 bucket (auto_delete_objects=True).
#            Download results from S3 before running if you need them.
#
# Usage:
#   AWS_PROFILE=trains-deploy ./scripts/bench-aws/teardown.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

cd "$SCRIPT_DIR"
source .venv/bin/activate 2>/dev/null || {
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -q -r requirements.txt
}

echo "=== Destroying TRAINS benchmark stacks ==="
echo "WARNING: This will delete all EC2 instances and the S3 results bucket."
read -r -p "Continue? [y/N] " confirm
[[ "$confirm" =~ ^[Yy] ]] || { echo "Aborted."; exit 1; }

# Empty the bench buckets FIRST. The access-logs bucket fills with S3 server
# access logs during a run; `auto_delete_objects` can miss late-arriving logs,
# leaving a non-empty bucket that fails `cdk destroy` with DELETE_FAILED
# (observed 2026-06-15). Emptying both buckets up front makes destroy reliable.
ACCT=$(aws sts get-caller-identity --query Account --output text)
REGION="${CDK_DEFAULT_REGION:-eu-west-3}"
BENCH_BUCKETS=("trains-bench-${ACCT}-${REGION}" "trains-bench-logs-${ACCT}-${REGION}")
empty_buckets(){ for b in "${BENCH_BUCKETS[@]}"; do
    aws s3 ls "s3://$b" >/dev/null 2>&1 && { echo "  emptying s3://$b"; aws s3 rm "s3://$b" --recursive --quiet 2>/dev/null || true; }
done; }

empty_buckets
if ! cdk destroy --all --force; then
    # Access logs can re-arrive between the empty and the bucket delete, racing
    # destroy to DELETE_FAILED. Empty again + force-delete the stacks directly.
    echo "cdk destroy failed; emptying buckets again and force-deleting stacks..."
    empty_buckets
    for s in TrainsBenchCompute TrainsBenchNetwork; do
        aws cloudformation delete-stack --region "$REGION" --stack-name "$s" 2>/dev/null || true
        aws cloudformation wait stack-delete-complete --region "$REGION" --stack-name "$s" 2>/dev/null || true
    done
fi
echo "Teardown complete."
