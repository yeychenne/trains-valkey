#!/usr/bin/env bash
# Validate the assurance index and decisive endpoint evidence without rerunning it.

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/../../.." && pwd)"
MANIFEST="$SCRIPT_DIR/manifest.json"
RESULT="$REPO/bench/results/arctic-proxy-local/7c03a87b5b0fac2624d7b0d28ec1412488e44299"

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

jq -e '.schema_version == 1 and .status == "complete"' "$MANIFEST" >/dev/null

while IFS= read -r path; do
    test -e "$REPO/$path" || {
        echo "missing indexed artifact: $path" >&2
        exit 1
    }
done < <(jq -r '.. | objects | .artifacts? // empty | .[]' "$MANIFEST")

test "$(find "$RESULT/rounds" -type f -name '*.json' | wc -l | tr -d ' ')" = 126
test "$(wc -l < "$RESULT/cases.jsonl" | tr -d ' ')" = 126
jq -e '
    .status == "NO-GO" and
    (.errors | length) == 0 and
    .gates.correctness_and_evidence == true and
    .gates.read_only_throughput_1_5x == false and
    .gates.mixed_throughput_1_5x == false and
    .gates.write_p99_below_3x == true and
    .gates.write_throughput_within_15pct == true
' "$RESULT/summary.json" >/dev/null

for required in REPORT.md RUN-NOTES.md binary-sha256.txt cases.jsonl \
    correctness.log environment.json manifest.json process-telemetry.jsonl \
    raw.json run.log summary.json; do
    test -s "$RESULT/$required" || {
        echo "missing or empty endpoint artifact: $required" >&2
        exit 1
    }
done

echo "PASS: assurance index and 126-case endpoint decision evidence are complete."
