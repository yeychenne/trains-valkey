#!/usr/bin/env bash
# Cross-compile three TRAINS binaries for Linux x86_64 (musl static).
#
# Output:
#   dist/trains-3   RING_SIZE=3,  NUM_TRAINS=2
#   dist/trains-10  RING_SIZE=10, NUM_TRAINS=3
#   dist/trains-15  RING_SIZE=15, NUM_TRAINS=3
#
# Requirements:
#   cargo install cross   (Docker/Podman required for cross-rs)
#   OR: rustup target add x86_64-unknown-linux-musl + musl-cross linker
#
# Usage:
#   ./scripts/bench-aws/build-linux.sh

set -euo pipefail
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
DIST="$REPO/scripts/bench-aws/dist"
# Override for the gnu fallback: TARGET=x86_64-unknown-linux-gnu ./build-linux.sh
TARGET="${TARGET:-x86_64-unknown-linux-musl}"

mkdir -p "$DIST"
cd "$REPO"

# Detect build tool: prefer 'cross' (handles musl cross-compile without
# a local musl-gcc install), fall back to plain cargo.
if command -v cross &>/dev/null; then
    BUILD="cross build --release --target $TARGET"
    echo "Using cross (Docker-based cross-compilation)"
else
    echo "cross not found; falling back to cargo (requires musl-cross toolchain)."
    echo "Install with: cargo install cross"
    # Attempt native cargo — works if the host has the musl linker:
    #   brew install FiloSottile/musl-cross/musl-cross
    #   rustup target add x86_64-unknown-linux-musl
    BUILD="cargo build --release --target $TARGET"
fi

build_variant() {
    local ring_size=$1 num_trains=$2 suffix=$3
    echo ""
    echo "── Building trains-${suffix} (RING_SIZE=${ring_size}, NUM_TRAINS=${num_trains}) ──"
    TRAINS_RING_SIZE=$ring_size TRAINS_NUM_TRAINS=$num_trains \
        $BUILD -p trains-cli 2>&1 | tail -5
    cp "$REPO/target/$TARGET/release/trains" "$DIST/trains-${suffix}"
    echo "    → $DIST/trains-${suffix} ($(du -sh "$DIST/trains-${suffix}" | cut -f1))"
}

build_variant 3  2  3
build_variant 10 3  10
build_variant 15 3  15

# ── trains-valkey proxy + chaos driver (PR-RD-4) ──────────────────────────────
# Build the RESP write-interception proxy (RING_SIZE=3) and the chaos
# workload/verifier for the EC2 fis-kill run. If musl static linking of the
# proxy's extra deps (s2n-quic/rustls) fails under `cross`, re-run with
# TARGET=x86_64-unknown-linux-gnu (AL2023 glibc runs gnu binaries fine).
echo ""
echo "── Building trains-valkey + trains-valkey-chaos (RING_SIZE=3) ──"
TRAINS_RING_SIZE=3 TRAINS_NUM_TRAINS=2 \
    $BUILD -p trains-valkey 2>&1 | tail -5
cp "$REPO/target/$TARGET/release/trains-valkey"       "$DIST/trains-valkey"
cp "$REPO/target/$TARGET/release/trains-valkey-chaos" "$DIST/trains-valkey-chaos"
echo "    → $DIST/trains-valkey ($(du -sh "$DIST/trains-valkey" | cut -f1))"
echo "    → $DIST/trains-valkey-chaos ($(du -sh "$DIST/trains-valkey-chaos" | cut -f1))"

echo ""
echo "Build complete:"
ls -lh "$DIST/"
