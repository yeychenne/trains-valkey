#!/usr/bin/env bash
# PR-RD-4 (G4): launch one trains-valkey proxy node on a ring instance.
#
# Run via SSM RunCommand per node, AFTER node-bootstrap.sh started the local
# engine. Reconfiguration is ON (--peer-addr for every node) so a node crash is
# masked by the distributed view change. The proxy applies the delivered stream
# to the co-located engine (--backend redis://127.0.0.1:6379).
#
# Required env (set per node by the orchestrator):
#   NODE_ID         this node's id (0..N-1)
#   RING_LISTEN     TLS ring listen addr, e.g. 0.0.0.0:7000
#   RING_SUCCESSOR  successor's <private-ip>:7000
#   IDENTITY        path to this node's identity JSON (from keygen.sh)
#   PEER_FP         comma-joined SPKI fingerprints of all peers
#   PEER_ADDRS      space-joined "id=<ip>:7000" for every node (incl. self)
#   VALKEY_PASSWORD engine password (matches node-bootstrap.sh)
# Optional: RESP_LISTEN (default 127.0.0.1:6380), ENGINE (127.0.0.1:6379),
#           NUM_TRAINS (2), TRAINS_REDIS_BIN (/opt/trains/trains-valkey)
# R-07: if VALKEY_UDS is set (must match node-bootstrap.sh), the proxy connects
#       to the engine over that UNIX domain socket and reads the password from a
#       0600 file instead of passing it in argv (which `ps` would expose).
set -euo pipefail

ID="${NODE_ID:?}"
LISTEN="${RING_LISTEN:?}"
SUCC="${RING_SUCCESSOR:?}"
IDFILE="${IDENTITY:?}"
FPS="${PEER_FP:?}"
PEERADDRS="${PEER_ADDRS:?}"
PW="${VALKEY_PASSWORD:?}"
RESP="${RESP_LISTEN:-127.0.0.1:6380}"
ENGINE="${ENGINE:-127.0.0.1:6379}"
UDS="${VALKEY_UDS:-}"
NUM_TRAINS="${NUM_TRAINS:-2}"
BIN="${TRAINS_REDIS_BIN:-/opt/trains/trains-valkey}"
# State-transfer server (PR-RJ-3b): every node serves snapshot+tail so a peer can
# rejoin through it. Default port 7001 (ring 7000, RESP 6380, engine 6379).
SNAP="${SNAP_LISTEN:-0.0.0.0:7001}"
# Passive rejoin (PR-RJ-3c): when REJOIN_FROM is set (space-joined survivor
# <ip>:7001 addrs), this node comes up OFF the ring and catches up from them —
# the restart path of the E5 t1-rejoin scenario. Empty ⇒ normal ring node.
REJOIN_FROM="${REJOIN_FROM:-}"

ISSUE=""
[ "$ID" -lt "$NUM_TRAINS" ] && ISSUE="--issue-initial"

PEER_FLAGS=""
for pa in $PEERADDRS; do
    PEER_FLAGS="$PEER_FLAGS --peer-addr $pa"
done

REJOIN_FLAGS=""
for rf in $REJOIN_FROM; do
    REJOIN_FLAGS="$REJOIN_FLAGS --rejoin-from $rf"
done

# Select the backend flags. UDS mode keeps the password off the argv (a file
# the proxy reads); legacy TCP mode keeps the existing --backend-password.
if [ -n "$UDS" ]; then
    PWFILE="$(mktemp /tmp/trains-backend-pw.XXXXXX)"
    chmod 600 "$PWFILE"
    printf '%s' "$PW" > "$PWFILE"
    BACKEND_FLAGS="--backend unix://$UDS --backend-password-file $PWFILE"
else
    BACKEND_FLAGS="--backend redis://$ENGINE --backend-password $PW"
fi

# shellcheck disable=SC2086  # intentional word-splitting of ISSUE/PEER_FLAGS/BACKEND_FLAGS/REJOIN_FLAGS
nohup "$BIN" --id "$ID" \
    --listen "$LISTEN" --successor "$SUCC" --resp-listen "$RESP" \
    --identity "$IDFILE" --peer-fp "$FPS" \
    $BACKEND_FLAGS \
    --snapshot-listen "$SNAP" $REJOIN_FLAGS \
    --delivery-mode to $ISSUE $PEER_FLAGS \
    > /tmp/trains-valkey.out 2>&1 &

echo $! > /tmp/trains-valkey.pid
echo "launched trains-valkey node ${ID} (pid $(cat /tmp/trains-valkey.pid)); RESP ${RESP}, ring ${LISTEN} -> ${SUCC}"
