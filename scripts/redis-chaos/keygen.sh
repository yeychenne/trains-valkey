#!/usr/bin/env bash
# PR-RD-4 (G3): generate N node identities + the comma-joined fingerprint list.
#
# Uses the `trains` binary (trains-cli) — the proxy loads the same NodeIdentity
# JSON format. Distribute id<i>.json to node i and fingerprints.txt (the
# --peer-fp value) to all nodes, out of band (e.g. via the bench S3 bucket / SSM).
#
# Usage: keygen.sh N [outdir]   (TRAINS_BIN overrides the binary path)
set -euo pipefail

N="${1:?usage: keygen.sh N [outdir]}"
OUT="${2:-./identities}"
TRAINS="${TRAINS_BIN:-trains}"

mkdir -p "$OUT"
fps=()
for i in $(seq 0 $((N - 1))); do
    fp=$("$TRAINS" keygen --out "$OUT/id$i.json" | awk '/fingerprint:/{print $2}')
    fps+=("$fp")
    echo "node $i -> $OUT/id$i.json  fp=$fp"
done

# Comma-joined list for --peer-fp.
(IFS=,; echo "${fps[*]}") > "$OUT/fingerprints.txt"
echo "peer fingerprints -> $OUT/fingerprints.txt"
