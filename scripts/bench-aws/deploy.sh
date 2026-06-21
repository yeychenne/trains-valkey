#!/usr/bin/env bash
# Deploy CDK infrastructure + upload binaries to S3.
#
# Run this once before bench-sweep.sh.
#
# Usage:
#   AWS_PROFILE=trains-deploy ./scripts/bench-aws/deploy.sh
#   (trains-deploy assumes TrainsBenchDeployRole — see
#    docs/operations/aws-credential-runbook-trains-bench-2026-06-13.md)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "=== Step 0: CDK lints (pre-deploy gate) ==="
# Catches the failure classes from PR-RD-5b (em-dash in IAM/SG descriptions,
# missing required ports, etc.) BEFORE the 5-10 min cdk deploy rolls back.
# Source: scripts/bench-aws/cdk_lints.py.
if ! python3 "$SCRIPT_DIR/cdk_lints.py"; then
    echo "" >&2
    echo "ABORT: CDK lints reported findings (above). Fix them, then re-run." >&2
    echo "       To skip (NOT recommended): TRAINS_SKIP_CDK_LINTS=1 $0" >&2
    if [ "${TRAINS_SKIP_CDK_LINTS:-0}" != "1" ]; then
        exit 1
    fi
    echo "  WARNING: TRAINS_SKIP_CDK_LINTS=1 — proceeding despite findings." >&2
fi

echo ""
echo "=== Step 1: Build Linux binaries ==="
"$SCRIPT_DIR/build-linux.sh"

echo ""
echo "=== Step 2: Bootstrap CDK (if first time in eu-west-3) ==="
cd "$SCRIPT_DIR"
if [ ! -d .venv ]; then
    python3 -m venv .venv
fi
source .venv/bin/activate
pip install -q -r requirements.txt

# CDK CLI floor: aws-cdk-lib (pip, ≥2.259) emits cloud-assembly schema 54,
# which needs cdk CLI ≥ 2.1126.0. Older CLIs fail to read the assembly.
CDK_VER="$(cdk --version 2>/dev/null | awk '{print $1}')"
if [ "$(printf '%s\n2.1126.0\n' "$CDK_VER" | sort -V | head -1)" != "2.1126.0" ]; then
    echo "WARNING: cdk CLI $CDK_VER < 2.1126.0 — may fail to read the synthesized" >&2
    echo "         assembly. Upgrade: npm i -g aws-cdk@latest" >&2
fi

# Bootstrap is a ONE-TIME ADMIN step done via the credential runbook with the
# SCOPED execution policy + permission boundary — NOT a plain bootstrap here.
# Re-running `cdk bootstrap` with defaults would replace the scoped exec role
# with an AdministratorAccess one, undoing the least-privilege posture
# (docs/operations/aws-credential-runbook-trains-bench-2026-06-13.md §0.2).
# deploy.sh assumes the account is already bootstrapped that way; set
# TRAINS_BOOTSTRAP=1 only for a throwaway sandbox where admin-scoped bootstrap
# is acceptable.
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
if [ "${TRAINS_BOOTSTRAP:-0}" = "1" ]; then
    echo "TRAINS_BOOTSTRAP=1 — plain (admin-scoped) bootstrap; sandbox only." >&2
    cdk bootstrap "aws://${ACCOUNT}/eu-west-3" 2>&1 | tail -10
else
    echo "Skipping cdk bootstrap (assumed done with scoped exec policy + boundary"
    echo "per the credential runbook §0.2). Set TRAINS_BOOTSTRAP=1 to force a"
    echo "plain bootstrap in a throwaway sandbox."
fi

echo ""
echo "=== Step 3: Deploy CDK stacks ==="
cdk deploy --all --require-approval never --outputs-file cdk-outputs.json
echo "CDK outputs written to $SCRIPT_DIR/cdk-outputs.json"

echo ""
echo "=== Step 4: Upload binaries to S3 ==="
BUCKET=$(python3 - <<'EOF'
import json, sys
with open("cdk-outputs.json") as f:
    outputs = json.load(f)
for stack, vals in outputs.items():
    if "BenchBucketName" in vals:
        print(vals["BenchBucketName"])
        sys.exit(0)
EOF
)

echo "Bucket: $BUCKET"
for variant in 3 10 15; do
    echo "  Uploading trains-${variant}..."
    aws s3 cp "$SCRIPT_DIR/dist/trains-${variant}" \
        "s3://$BUCKET/binaries/trains-${variant}" \
        --content-type application/octet-stream
done
echo ""
echo "Deploy complete. Run bench-sweep.sh to start the benchmark."
echo "Bucket: $BUCKET"
