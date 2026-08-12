#!/usr/bin/env bash
# Create an assurance case from the technology-neutral templates.

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TOOLKIT="$(cd "$SCRIPT_DIR/.." && pwd)"
CASE_ID="${1:-}"
DEST="${2:-}"

if [ -z "$CASE_ID" ] || [ -z "$DEST" ]; then
    echo "usage: $0 <case-id> <destination>" >&2
    exit 2
fi

case "$CASE_ID" in
    *[!a-z0-9-]* | -* | *-)
        echo "case-id must use lowercase letters, digits, and internal hyphens" >&2
        exit 2
        ;;
esac

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }
test ! -e "$DEST" || { echo "refusing to overwrite: $DEST" >&2; exit 1; }

mkdir -p "$DEST/evidence" "$DEST/runs"
for template in "$TOOLKIT"/templates/*.md; do
    cp "$template" "$DEST/$(basename "$template")"
done

for file in "$DEST"/*.md; do
    awk -v case_id="$CASE_ID" '{gsub(/<case-id>/, case_id); print}' "$file" \
        > "$file.next"
    mv "$file.next" "$file"
done

NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
jq --arg case_id "$CASE_ID" --arg now "$NOW" \
    '.case_id = $case_id | .created_utc = $now | .updated_utc = $now' \
    "$TOOLKIT/templates/case-manifest.template.json" > "$DEST/case.json"

printf '%s\n' \
    "Created assurance case: $DEST" \
    "Case ID: $CASE_ID" \
    "Next: complete 00-ENGAGEMENT-CHARTER.md and case.json"
