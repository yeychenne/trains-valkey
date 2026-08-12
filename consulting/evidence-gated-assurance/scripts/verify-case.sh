#!/usr/bin/env bash
# Check case structure, evidence references, and completion consistency.

set -Eeuo pipefail

CASE_DIR="${1:-}"
test -n "$CASE_DIR" || { echo "usage: $0 <case-directory>" >&2; exit 2; }
test -d "$CASE_DIR" || { echo "case directory not found: $CASE_DIR" >&2; exit 1; }
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

CASE_DIR="$(cd "$CASE_DIR" && pwd)"
MANIFEST="$CASE_DIR/case.json"
test -s "$MANIFEST" || { echo "missing case.json" >&2; exit 1; }

jq -e '
    .schema_version == 1 and
    (.case_id | type == "string" and test("^[a-z0-9][a-z0-9-]*$")) and
    (.status | IN("draft", "active", "blocked", "complete", "superseded")) and
    (.claims | type == "array") and
    (.invariants | type == "array") and
    (.methods | type == "array") and
    (.gates | type == "array") and
    (.runs | type == "array") and
    (.decisions | type == "array") and
    (.artifacts | type == "array")
' "$MANIFEST" >/dev/null || { echo "invalid case manifest structure" >&2; exit 1; }

for collection in claims invariants methods gates runs decisions artifacts; do
    jq -e --arg collection "$collection" '
        [.[$collection][].id] as $ids | ($ids | length) == ($ids | unique | length)
    ' "$MANIFEST" >/dev/null || {
        echo "duplicate ID in $collection" >&2
        exit 1
    }
done

jq -e '
    [.artifacts[].id] as $ids |
    [
      .claims[]?.evidence_ids[]?,
      .gates[]?.evidence_ids[]?,
      .runs[]?.artifact_ids[]?,
      .decisions[]?.evidence_ids[]?
    ] | all(. as $id | ($ids | index($id)) != null)
' "$MANIFEST" >/dev/null || {
    echo "a claim, gate, run, or decision references an unknown artifact ID" >&2
    exit 1
}

jq -e '
    [.claims[].id] as $ids |
    [.invariants[]?.claim_ids[]?, .methods[]?.claim_ids[]?] |
    all(. as $id | ($ids | index($id)) != null)
' "$MANIFEST" >/dev/null || {
    echo "an invariant or method references an unknown claim ID" >&2
    exit 1
}

hash_file() {
    if command -v shasum >/dev/null; then
        shasum -a 256 "$1" | awk '{print $1}'
    else
        sha256sum "$1" | awk '{print $1}'
    fi
}

while IFS=$'\t' read -r id path required checksum; do
    case "$path" in
        /* | ../* | */../* | */..)
            echo "unsafe artifact path for $id: $path" >&2
            exit 1
            ;;
    esac

    if [ "$required" = true ]; then
        test -s "$CASE_DIR/$path" || {
            echo "missing or empty required artifact $id: $path" >&2
            exit 1
        }
    elif [ ! -e "$CASE_DIR/$path" ]; then
        continue
    fi

    if [ -n "$checksum" ]; then
        test -f "$CASE_DIR/$path" || {
            echo "checksummed artifact is not a file $id: $path" >&2
            exit 1
        }
        actual="$(hash_file "$CASE_DIR/$path")"
        test "$actual" = "$checksum" || {
            echo "SHA-256 mismatch for $id: $path" >&2
            exit 1
        }
    fi
done < <(jq -r '.artifacts[] | [.id, .path, (.required|tostring), .sha256] | @tsv' "$MANIFEST")

if [ "$(jq -r '.status' "$MANIFEST")" = complete ]; then
    jq -e '
        .decision_question != "" and
        .target.name != "" and
        .target.revision != "" and
        .final_disposition != "pending" and
        (.claims | length) > 0 and
        (.gates | length) > 0 and
        (.decisions | length) > 0 and
        ([.claims[].status] | all(. != "proposed")) and
        ([.claims[] | select(.status | IN("supported", "refuted", "conditional")) |
          (.evidence_ids | length > 0)] | all) and
        ([.gates[].status] | all(. != "pending")) and
        ([.gates[] | select(.status | IN("pass", "fail", "inconclusive")) |
          (.evidence_ids | length > 0)] | all) and
        ([.decisions[] | (.evidence_ids | length > 0)] | all)
    ' "$MANIFEST" >/dev/null || {
        echo "completed case retains unanswered claims/gates or lacks a final decision" >&2
        exit 1
    }
fi

echo "PASS: case manifest and evidence references are consistent."
