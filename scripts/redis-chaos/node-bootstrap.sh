#!/usr/bin/env bash
# PR-RD-4 (G2): install + start a loopback Valkey on an AL2023 ring node.
#
# Run via SSM RunCommand on each node BEFORE launch-node.sh. The engine is
# loopback-only with a password — the trains-valkey proxy is its sole client;
# never expose this port (the ring SG must not open it). TRAINS + PR-RD-3 state
# transfer provide cross-node durability, so persistence is off.
#
# R-07 (hardened path): set VALKEY_UDS=/var/run/trains/valkey.sock to bind the
# engine to a UNIX domain socket ONLY (port 0 — no TCP at all), with 0700 perms
# and requirepass. The proxy then connects via `--backend unix://$VALKEY_UDS`
# (launch-node.sh does this automatically when VALKEY_UDS is set). This removes
# the "any host process can reach the engine over loopback TCP" exposure
# (T-tr-14/14b/22). Default (VALKEY_UDS unset) keeps the loopback-TCP behaviour.
set -euo pipefail

PW="${VALKEY_PASSWORD:?set VALKEY_PASSWORD (deliver via an SSM/Secrets-Manager secret, never commit it)}"
PORT="${VALKEY_PORT:-6379}"
# Defaults to loopback per runbook security model. Override (e.g. `0.0.0.0`)
# only when the chaos verifier must read survivor engines across nodes — and
# only then in combination with an SG that restricts 6379 to the ring SG.
BIND="${VALKEY_BIND:-127.0.0.1}"
# R-07: socket path when binding UDS-only. Empty ⇒ legacy loopback-TCP mode.
UDS="${VALKEY_UDS:-}"

if ! command -v valkey-server >/dev/null 2>&1 && ! command -v redis-server >/dev/null 2>&1; then
    # AL2023 may not ship valkey in the default repos; try dnf, else a static
    # binary staged at /opt/trains/valkey-server (uploaded with the trains binary).
    sudo dnf install -y valkey 2>/dev/null \
        || sudo dnf install -y redis6 2>/dev/null \
        || {
            [ -x /opt/trains/valkey-server ] || { echo "no valkey/redis available and no staged binary" >&2; exit 1; }
            sudo install -m0755 /opt/trains/valkey-server /usr/local/bin/valkey-server
        }
fi

ENGINE_BIN="$(command -v valkey-server || command -v redis-server)"
CLI_BIN="$(command -v valkey-cli || command -v redis-cli)"

if [ -n "$UDS" ]; then
    # R-07 hardened path: UDS-only, no TCP. The socket dir is owned by the
    # engine's runtime user; 0700 perms restrict it to that user.
    sudo mkdir -p "$(dirname "$UDS")"
    "$ENGINE_BIN" \
        --port 0 \
        --unixsocket "$UDS" \
        --unixsocketperm 700 \
        --requirepass "$PW" \
        --save "" \
        --appendonly no \
        --daemonize yes \
        --pidfile /tmp/valkey.pid \
        --logfile /tmp/valkey.log

    for _ in $(seq 1 50); do
        if "$CLI_BIN" -s "$UDS" -a "$PW" ping 2>/dev/null | grep -q PONG; then
            echo "engine ready on unix://${UDS} (UDS-only, no TCP)"
            exit 0
        fi
        sleep 0.2
    done
    echo "engine did not become ready on unix://${UDS}" >&2
    exit 1
fi

"$ENGINE_BIN" \
    --bind "$BIND" \
    --port "$PORT" \
    --protected-mode yes \
    --requirepass "$PW" \
    --save "" \
    --appendonly no \
    --daemonize yes \
    --pidfile /tmp/valkey.pid \
    --logfile /tmp/valkey.log

for _ in $(seq 1 50); do
    if "$CLI_BIN" -p "$PORT" -a "$PW" ping 2>/dev/null | grep -q PONG; then
        echo "engine ready on ${BIND}:${PORT}"
        exit 0
    fi
    sleep 0.2
done
echo "engine did not become ready on ${BIND}:${PORT}" >&2
exit 1
