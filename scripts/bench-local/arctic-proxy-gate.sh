#!/usr/bin/env bash
# Build and run the local three-target Arctic proxy endpoint gate.

set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"
MODE="${1:-}"
CARGO="${CARGO:-$(command -v cargo || true)}"
RUSTC="${RUSTC:-$(command -v rustc || true)}"
PYTHON="${PYTHON:-$(command -v python3 || true)}"
VALKEY_BIN="${PROXY_BENCH_VALKEY_BIN:-$(command -v valkey-server || command -v redis-server || true)}"
SHA="$(git -C "$REPO" rev-parse HEAD)"
DRIVER_PID=""

case "$MODE" in
    smoke)
        KEYS="${PROXY_BENCH_KEYS:-1000}"
        OPS="${PROXY_BENCH_OPS_PER_CLIENT:-2000}"
        REPS="${PROXY_BENCH_REPETITIONS:-1}"
        RESULTS_DIR="${RESULTS_DIR:-${TMPDIR:-/tmp}/trains-arctic-proxy-smoke/$SHA}"
        ;;
    qualification)
        KEYS="${PROXY_BENCH_KEYS:-10000}"
        OPS="${PROXY_BENCH_OPS_PER_CLIENT:-100000}"
        REPS="${PROXY_BENCH_REPETITIONS:-7}"
        RESULTS_DIR="${RESULTS_DIR:-$REPO/bench/results/arctic-proxy-local/$SHA}"
        ;;
    *)
        echo "usage: $0 {smoke|qualification}" >&2
        exit 2
        ;;
esac

MUTEX_TARGET="$REPO/target/bench-mutex"
ARCTIC_TARGET="$REPO/target/bench-arctic"
DRIVER_TARGET="$REPO/target/bench-driver"
MUTEX_BIN="$MUTEX_TARGET/release/proxy-bench-server"
ARCTIC_BIN="$ARCTIC_TARGET/release/proxy-bench-server"
DRIVER_BIN="$DRIVER_TARGET/release/proxy-endpoint-bench"

cleanup() {
    if [ -n "$DRIVER_PID" ] && kill -0 "$DRIVER_PID" 2>/dev/null; then
        pkill -TERM -P "$DRIVER_PID" 2>/dev/null || true
        kill -TERM "$DRIVER_PID" 2>/dev/null || true
        wait "$DRIVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

test -n "$CARGO" || { echo "cargo is required" >&2; exit 1; }
if [ -z "$RUSTC" ]; then
    RUSTC="$(dirname "$CARGO")/rustc"
fi
test -x "$RUSTC" || { echo "rustc is required" >&2; exit 1; }
test -n "$PYTHON" || { echo "python3 is required" >&2; exit 1; }
test -n "$VALKEY_BIN" || { echo "valkey-server or redis-server is required" >&2; exit 1; }
test -x "$(command -v jq)" || { echo "jq is required" >&2; exit 1; }

if [ "${ALLOW_DIRTY:-0}" != 1 ]; then
    test -z "$(git -C "$REPO" status --porcelain)" || {
        echo "refusing to benchmark a dirty worktree; commit first" >&2
        exit 1
    }
    BRANCH="$(git -C "$REPO" branch --show-current)"
    REMOTE_SHA="$(git -C "$REPO" ls-remote origin "refs/heads/$BRANCH" | awk '{print $1}')"
    test "$SHA" = "$REMOTE_SHA" || {
        echo "refusing to benchmark unpushed HEAD $SHA (origin: $REMOTE_SHA)" >&2
        exit 1
    }
fi

test ! -e "$RESULTS_DIR" || {
    echo "refusing to overwrite immutable evidence directory: $RESULTS_DIR" >&2
    exit 1
}
mkdir -p "$RESULTS_DIR/rounds"
STARTED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "=== Build isolated benchmark targets ==="
CARGO_TARGET_DIR="$MUTEX_TARGET" \
    "$CARGO" build --release --locked -p trains-valkey --bin proxy-bench-server \
    --manifest-path "$REPO/Cargo.toml"
CARGO_TARGET_DIR="$ARCTIC_TARGET" \
    "$CARGO" build --release --locked -p trains-valkey --bin proxy-bench-server \
    --features arctic-proxy --manifest-path "$REPO/Cargo.toml"
CARGO_TARGET_DIR="$DRIVER_TARGET" \
    "$CARGO" build --release --locked --bin proxy-endpoint-bench \
    --manifest-path "$REPO/experiments/arctic-shadow/Cargo.toml"

HASH_TOOL="shasum -a 256"
command -v shasum >/dev/null 2>&1 || HASH_TOOL="sha256sum"
{
    $HASH_TOOL "$MUTEX_BIN"
    $HASH_TOOL "$ARCTIC_BIN"
    $HASH_TOOL "$DRIVER_BIN"
} > "$RESULTS_DIR/binary-sha256.txt"

jq -n \
    --arg generated "$STARTED" \
    --arg uname "$(uname -a)" \
    --arg rustc "$("$CARGO" --version) / $("$RUSTC" --version)" \
    --arg valkey "$($VALKEY_BIN --version)" \
    --arg logical_cpu "$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.logicalcpu)" \
    '{generated_utc:$generated, uname:$uname, toolchain:$rustc,
      valkey:$valkey, logical_cpu:($logical_cpu|tonumber)}' \
    > "$RESULTS_DIR/environment.json"

jq -n \
    --arg mode "$MODE" \
    --arg commit "$SHA" \
    --arg branch "$(git -C "$REPO" branch --show-current)" \
    --arg started "$STARTED" \
    --argjson keys "$KEYS" \
    --argjson operations_per_client "$OPS" \
    --argjson repetitions "$REPS" \
    --arg seed "6075990630378709030" \
    '{mode:$mode, commit:$commit, branch:$branch, dirty:false,
      started_utc:$started, keys:$keys,
      operations_per_client:$operations_per_client, clients:[1,8],
      repetitions:$repetitions,
      workloads:["read-only","read-90-write-10","write-only"],
      targets:["mutex-proxy","arctic-proxy","valkey"], seed:$seed,
      commands:["GET key","SET key 32-byte-value"],
      build_commands:[
        "cargo build --release --locked -p trains-valkey --bin proxy-bench-server",
        "cargo build --release --locked -p trains-valkey --bin proxy-bench-server --features arctic-proxy",
        "cargo build --release --locked --bin proxy-endpoint-bench"
      ],
      target_rotation:"offset by repetition modulo three",
      client_model:"one persistent RESP connection and one outstanding request per client"}' \
    > "$RESULTS_DIR/manifest.json"

echo "=== Run $MODE gate ==="
env \
    PROXY_BENCH_KEYS="$KEYS" \
    PROXY_BENCH_OPS_PER_CLIENT="$OPS" \
    PROXY_BENCH_CLIENTS=1,8 \
    PROXY_BENCH_REPETITIONS="$REPS" \
    PROXY_BENCH_MUTEX_BIN="$MUTEX_BIN" \
    PROXY_BENCH_ARCTIC_BIN="$ARCTIC_BIN" \
    PROXY_BENCH_VALKEY_BIN="$VALKEY_BIN" \
    PROXY_BENCH_OUTPUT="$RESULTS_DIR/raw.json" \
    PROXY_BENCH_TELEMETRY_OUTPUT="$RESULTS_DIR/process-telemetry.jsonl" \
    "$DRIVER_BIN" > >(tee "$RESULTS_DIR/run.log") 2>&1 &
DRIVER_PID=$!
wait "$DRIVER_PID"
DRIVER_PID=""

"$PYTHON" "$SCRIPT_DIR/arctic-proxy-analyze.py" \
    "$RESULTS_DIR/raw.json" \
    "$RESULTS_DIR/process-telemetry.jsonl" \
    "$RESULTS_DIR/summary.json" \
    "$RESULTS_DIR/REPORT.md" \
    "$MODE"

ENDED="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
jq --arg ended "$ENDED" '. + {ended_utc:$ended}' \
    "$RESULTS_DIR/manifest.json" > "$RESULTS_DIR/manifest.tmp"
mv "$RESULTS_DIR/manifest.tmp" "$RESULTS_DIR/manifest.json"
printf 'PASS: every reply and %s final keys per case validated.\n' \
    "$((KEYS < 64 ? KEYS : 64))" > "$RESULTS_DIR/correctness.log"

for required in REPORT.md environment.json manifest.json summary.json raw.json \
    correctness.log process-telemetry.jsonl binary-sha256.txt run.log; do
    test -s "$RESULTS_DIR/$required" || {
        echo "missing evidence: $RESULTS_DIR/$required" >&2
        exit 1
    }
done
test "$(find "$RESULTS_DIR/rounds" -type f -name '*.json' | wc -l | tr -d ' ')" -gt 0

echo "Evidence: $RESULTS_DIR"
cat "$RESULTS_DIR/REPORT.md"
