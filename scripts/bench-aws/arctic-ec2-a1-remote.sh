#!/usr/bin/env bash
# Reproducible ARM64 benchmark payload. Invoked on EC2 through SSM.

set -Eeuo pipefail

: "${GIT_SHA:?GIT_SHA is required}"
: "${BUCKET:?BUCKET is required}"
AWS_REGION="${AWS_REGION:-eu-west-3}"
RESULTS="/opt/trains/results/arctic-ec2-a1/${GIT_SHA}"
SOURCE="/opt/trains/source"
EXPERIMENT="$SOURCE/experiments/arctic-shadow"
S3_PREFIX="s3://${BUCKET}/arctic-ec2-a1/${GIT_SHA}"

mkdir -p "$RESULTS"
exec > >(tee -a "$RESULTS/run.log") 2>&1

upload_results() {
    local exit_code=$?
    jq -n \
        --arg status "$([ "$exit_code" -eq 0 ] && echo complete || echo failed)" \
        --argjson exit_code "$exit_code" \
        --arg finished_at "$(date --iso-8601=seconds)" \
        '{status: $status, exit_code: $exit_code, finished_at: $finished_at}' \
        > "$RESULTS/status.json" || true
    aws s3 sync "$RESULTS" "$S3_PREFIX" --region "$AWS_REGION" || true
    return "$exit_code"
}
trap upload_results EXIT

echo "=== Wait for host initialization ==="
cloud-init status --wait
dnf install -y git gcc gcc-c++ make cmake perl jq sysstat tar gzip
if ! command -v valkey-server >/dev/null 2>&1 && ! command -v redis-server >/dev/null 2>&1; then
    dnf install -y valkey || dnf install -y redis6 || dnf install -y redis
fi
ENGINE_BIN="$(command -v valkey-server || command -v redis-server)"

echo "=== Install pinned Rust toolchain ==="
if [ ! -x /root/.cargo/bin/rustup ]; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --profile minimal --default-toolchain 1.95.0
fi
export PATH="/root/.cargo/bin:$PATH"
rustup toolchain install 1.95.0 --profile minimal
rustup default 1.95.0

echo "=== Check out immutable candidate ==="
rm -rf "$SOURCE"
git clone https://github.com/yeychenne/trains-valkey.git "$SOURCE"
git -C "$SOURCE" checkout --detach "$GIT_SHA"
test "$(git -C "$SOURCE" rev-parse HEAD)" = "$GIT_SHA"

echo "=== Capture host and source identity ==="
METADATA_TOKEN="$(curl -fsS -X PUT \
    -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' \
    http://169.254.169.254/latest/api/token)"
INSTANCE_TYPE="$(curl -fsS \
    -H "X-aws-ec2-metadata-token: $METADATA_TOKEN" \
    http://169.254.169.254/latest/meta-data/instance-type)"
jq -n \
    --arg git_sha "$GIT_SHA" \
    --arg started_at "$(date --iso-8601=seconds)" \
    --arg kernel "$(uname -a)" \
    --arg os "$(. /etc/os-release; echo "$PRETTY_NAME")" \
    --arg instance_type "$INSTANCE_TYPE" \
    --arg engine "$($ENGINE_BIN --version 2>&1 | head -1)" \
    --arg rustc "$(rustc --version --verbose)" \
    '{git_sha: $git_sha, started_at: $started_at, kernel: $kernel, os: $os,
      instance_type: $instance_type, engine: $engine, rustc: $rustc}' \
    > "$RESULTS/environment.json"
lscpu > "$RESULTS/lscpu.txt"
cp "$EXPERIMENT/Cargo.lock" "$RESULTS/Cargo.lock"

echo "=== Correctness gate: ArcSwap and committed-style RwLock ==="
(
    cd "$EXPERIMENT"
    cargo test --locked --release 2>&1 | tee "$RESULTS/tests-arc-swap.log"
    cargo test --locked --release --features rwlock-generation 2>&1 \
        | tee "$RESULTS/tests-rwlock.log"
)

echo "=== Build and fingerprint both candidates ==="
(
    cd "$EXPERIMENT"
    cargo build --locked --release --bin data-plane-bench
    install -m0755 target/release/data-plane-bench /opt/trains/data-plane-bench-arc-swap
    cargo build --locked --release --features rwlock-generation --bin data-plane-bench
    install -m0755 target/release/data-plane-bench /opt/trains/data-plane-bench-rwlock
)
sha256sum /opt/trains/data-plane-bench-* > "$RESULTS/binary-sha256.txt"

run_ordered() {
    local binary=$1
    local label=$2
    local output=$3
    ARCTIC_BENCH_LABEL="$label" \
    ARCTIC_BENCH_KEYS=100000 \
    ARCTIC_BENCH_OPS_PER_THREAD=1000000 \
    ARCTIC_BENCH_THREADS=1,8 \
    ARCTIC_BENCH_REPETITIONS=1 \
    ARCTIC_BENCH_BACKENDS=ordered-arctic,ordered-mutex-btree \
    ARCTIC_BENCH_OUTPUT="$output" \
        taskset -c 0-7 "$binary"
}

echo "=== Warm both binaries (not recorded) ==="
ARCTIC_BENCH_KEYS=10000 ARCTIC_BENCH_OPS_PER_THREAD=25000 \
ARCTIC_BENCH_THREADS=8 ARCTIC_BENCH_REPETITIONS=1 \
ARCTIC_BENCH_WORKLOADS=read-90-write-10 \
ARCTIC_BENCH_BACKENDS=ordered-arctic,ordered-mutex-btree \
    taskset -c 0-7 /opt/trains/data-plane-bench-arc-swap >/dev/null
ARCTIC_BENCH_KEYS=10000 ARCTIC_BENCH_OPS_PER_THREAD=25000 \
ARCTIC_BENCH_THREADS=8 ARCTIC_BENCH_REPETITIONS=1 \
ARCTIC_BENCH_WORKLOADS=read-90-write-10 \
ARCTIC_BENCH_BACKENDS=ordered-arctic,ordered-mutex-btree \
    taskset -c 0-7 /opt/trains/data-plane-bench-rwlock >/dev/null

echo "=== Seven interleaved A/B rounds ==="
for round in 1 2 3 4 5 6 7; do
    case $((round % 4)) in
        1|0) order=(arc-swap rwlock) ;;
        *)   order=(rwlock arc-swap) ;;
    esac
    for candidate in "${order[@]}"; do
        if [ "$candidate" = arc-swap ]; then
            binary=/opt/trains/data-plane-bench-arc-swap
        else
            binary=/opt/trains/data-plane-bench-rwlock
        fi
        run_ordered "$binary" "aws-${candidate}-round-${round}" \
            "$RESULTS/${candidate}-round-${round}.json"
    done
done

echo "=== Valkey endpoint reference ==="
ARCTIC_BENCH_LABEL=aws-valkey-reference \
ARCTIC_BENCH_KEYS=100000 \
ARCTIC_BENCH_OPS_PER_THREAD=250000 \
ARCTIC_BENCH_THREADS=1,8 \
ARCTIC_BENCH_REPETITIONS=7 \
ARCTIC_BENCH_BACKENDS=valkey \
ARCTIC_BENCH_OUTPUT="$RESULTS/valkey.json" \
    taskset -c 0-7 /opt/trains/data-plane-bench-arc-swap

echo "=== Complete ==="
