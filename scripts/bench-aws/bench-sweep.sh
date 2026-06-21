#!/usr/bin/env bash
# Run the full TRAINS AWS benchmark sweep (3, 10, 15 nodes).
#
# Prerequisites:
#   1. ./deploy.sh completed (CDK stacks up, binaries in S3)
#   2. AWS credentials set (AWS_PROFILE or env)
#
# Usage:
#   AWS_PROFILE=trains-run ./scripts/bench-aws/bench-sweep.sh [--duration 30] [--payload-size 64]

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ ! -f "$SCRIPT_DIR/cdk-outputs.json" ]; then
    echo "ERROR: cdk-outputs.json not found. Run deploy.sh first." >&2
    exit 1
fi

if [ ! -d "$SCRIPT_DIR/.venv" ]; then
    python3 -m venv "$SCRIPT_DIR/.venv"
    source "$SCRIPT_DIR/.venv/bin/activate"
    pip install -q -r "$SCRIPT_DIR/requirements.txt"
else
    source "$SCRIPT_DIR/.venv/bin/activate"
fi

echo "=== TRAINS AWS Benchmark Sweep ==="
echo "Ring sizes: 3, 10, 15 nodes across 3 AZs (eu-west-3)"
echo ""

python3 "$SCRIPT_DIR/coordinator.py" \
    --ring-sizes 3 10 15 \
    "$@"
