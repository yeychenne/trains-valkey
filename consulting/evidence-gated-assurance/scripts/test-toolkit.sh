#!/usr/bin/env bash
# Exercise case scaffolding, successful verification, and expected rejection.

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/assurance-toolkit.XXXXXX")"
trap 'rm -rf "$TMP_ROOT"' EXIT

PASS_CASE="$TMP_ROOT/pass-case"
FAIL_CASE="$TMP_ROOT/fail-case"
REF_CASE="$TMP_ROOT/ref-case"

"$SCRIPT_DIR/new-case.sh" smoke-case "$PASS_CASE" >/dev/null
"$SCRIPT_DIR/verify-case.sh" "$PASS_CASE" >/dev/null
grep -q 'Assurance Case: `smoke-case`' "$PASS_CASE/CASE-README.md"

printf '%s\n' 'validated result' > "$PASS_CASE/evidence/result.txt"
if command -v shasum >/dev/null; then
    HASH="$(shasum -a 256 "$PASS_CASE/evidence/result.txt" | awk '{print $1}')"
else
    HASH="$(sha256sum "$PASS_CASE/evidence/result.txt" | awk '{print $1}')"
fi

jq --arg hash "$HASH" '
    .status = "complete" |
    .decision_question = "Should the candidate be adopted?" |
    .target.name = "candidate" |
    .target.revision = "abc123" |
    .claims = [{
      "id":"CL-01", "statement":"The candidate preserves the contract.",
      "criticality":"high", "status":"supported",
      "evidence_ids":["ART-RESULT"], "limitations":[]
    }] |
    .invariants = [{
      "id":"INV-01", "statement":"Accepted operations match the oracle.",
      "claim_ids":["CL-01"], "observable":"exact manifest", "status":"covered"
    }] |
    .methods = [{
      "id":"M-DIFF", "name":"differential testing", "boundary":"component",
      "claim_ids":["CL-01"], "limitations":[]
    }] |
    .gates = [{
      "id":"G-01", "question":"Adopt?", "status":"pass",
      "criteria":["exact match"], "evidence_ids":["ART-RESULT"],
      "next_on_pass":"adopt", "next_on_fail":"stop"
    }] |
    .runs = [{
      "id":"RUN-20260812-01", "classification":"qualifying", "revision":"abc123",
      "started_utc":"2026-08-12T08:00:00Z",
      "ended_utc":"2026-08-12T08:01:00Z", "artifact_ids":["ART-RESULT"]
    }] |
    .decisions = [{
      "id":"D-FINAL", "date":"2026-08-12", "disposition":"go",
      "scope":"declared contract", "evidence_ids":["ART-RESULT"],
      "rationale":"The qualifying oracle matched."
    }] |
    .artifacts = [{
      "id":"ART-RESULT", "path":"evidence/result.txt", "type":"raw",
      "required":true, "immutable":true, "sha256":$hash
    }] |
    .final_disposition = "go"
' "$PASS_CASE/case.json" > "$PASS_CASE/case.next.json"
mv "$PASS_CASE/case.next.json" "$PASS_CASE/case.json"
"$SCRIPT_DIR/verify-case.sh" "$PASS_CASE" >/dev/null

"$SCRIPT_DIR/new-case.sh" rejection-case "$FAIL_CASE" >/dev/null
jq '.status = "complete"' "$FAIL_CASE/case.json" > "$FAIL_CASE/case.next.json"
mv "$FAIL_CASE/case.next.json" "$FAIL_CASE/case.json"
if "$SCRIPT_DIR/verify-case.sh" "$FAIL_CASE" >/dev/null 2>&1; then
    echo "incomplete completed case was incorrectly accepted" >&2
    exit 1
fi

"$SCRIPT_DIR/new-case.sh" reference-case "$REF_CASE" >/dev/null
jq '.invariants = [{
      "id":"INV-01", "statement":"Broken reference.",
      "claim_ids":["CL-MISSING"], "observable":"none", "status":"proposed"
    }]' "$REF_CASE/case.json" > "$REF_CASE/case.next.json"
mv "$REF_CASE/case.next.json" "$REF_CASE/case.json"
if "$SCRIPT_DIR/verify-case.sh" "$REF_CASE" >/dev/null 2>&1; then
    echo "unknown claim reference was incorrectly accepted" >&2
    exit 1
fi

printf '%s\n' 'tampered' >> "$PASS_CASE/evidence/result.txt"
if "$SCRIPT_DIR/verify-case.sh" "$PASS_CASE" >/dev/null 2>&1; then
    echo "tampered evidence was incorrectly accepted" >&2
    exit 1
fi

echo "PASS: scaffolding, completion, references, and integrity checks work."
