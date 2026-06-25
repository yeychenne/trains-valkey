#!/usr/bin/env bash
# e5-run.sh — drive the E5 adversarial matrix on a deployed bench, ONE FRESH RING
# per scenario. This is the orchestrator the OPS-E5 campaign plan calls for; it
# encodes the operational lessons from the 2026-06-15 live run.
#
# Per scenario it: relaunches a clean N-node ring (install fault tools + valkey +
# proxy + a per-node relaunch.sh), health-checks it, starts the chaos load with
# --abandon-secs, runs e5_sequencer (with the venv python — or the fault silently
# no-ops), then verify-locals every SURVIVING node and asserts zero acked-write
# loss + convergence. Results land in bench/results/ec2-<date>-e5-matrix/.
#
# Prereqs: a deployed bench (deploy.sh wrote scripts/bench-aws/cdk-outputs.json),
# the arm64 binaries built (target/aarch64-unknown-linux-gnu/release/), and a
# local `trains` (trains-cli) for keygen. AWS via $AWS_PROFILE (e.g. <aws-profile>
# or trains-run).
#
# IMPORTANT — ring size is a COMPILE-TIME CONSTANT.  `trains-core/build.rs`
# reads `TRAINS_RING_SIZE` from the build environment (default 3) and bakes
# it into the binary.  Schedules with `ring_size != TRAINS_RING_SIZE` will
# skip with a size-mismatch warning.  To run a scenario at N=5:
#
#   TRAINS_RING_SIZE=5 cargo zigbuild --release \
#     --target aarch64-unknown-linux-gnu -p trains-valkey
#   TRAINS_RING_SIZE=5 cargo build --release -p trains-cli   # for keygen
#
# To run scenarios at different N in one session, rebuild + redeploy or
# stage multiple labelled binaries.  Mixing ring-size-3 and ring-size-5
# scenarios against a single binary build is not supported.
#
# Usage:
#   AWS_PROFILE=<aws-profile> ./scripts/bench-aws/e5-run.sh                 # all schedules that fit the deploy
#   AWS_PROFILE=<aws-profile> ./scripts/bench-aws/e5-run.sh t1-partition t1-rejoin
set -uo pipefail

SD="$(cd "$(dirname "$0")" && pwd)"; REPO="$(cd "$SD/../.." && pwd)"
REGION="${AWS_REGION:-eu-west-3}"
PROFILE="${AWS_PROFILE:-<aws-profile>}"
PW="${VALKEY_PASSWORD:-trainsE5pw}"
COUNT="${E5_COUNT:-2000}"; HOLD="${E5_HOLD:-30}"; ABANDON="${E5_ABANDON:-5}"
PY="$SD/.venv/bin/python3"
SCHED_DIR="$REPO/bench/coordinator/schedules"
SEQ="$REPO/bench/coordinator/e5_sequencer.py"
OUTPUTS="$SD/cdk-outputs.json"
ARM="$REPO/target/aarch64-unknown-linux-gnu/release"
TRAINS_BIN="${TRAINS_BIN:-$REPO/../trains-rust/target/release/trains}"
RESULTS="$REPO/bench/results/ec2-$(date +%F)-e5-matrix"

die(){ echo "ERROR: $*" >&2; exit 1; }
[ -f "$OUTPUTS" ] || die "no cdk-outputs.json — run deploy.sh first"
[ -x "$PY" ] || die "no venv python at $PY (boto3) — create scripts/bench-aws/.venv"
[ -x "$ARM/trains-valkey" ] || die "no arm64 trains-valkey — cargo zigbuild first"
[ -x "$TRAINS_BIN" ] || die "no local trains binary for keygen at $TRAINS_BIN"
command -v jq >/dev/null || die "jq required"

BUCKET=$(jq -r '.TrainsBenchCompute.BenchBucketName' "$OUTPUTS")
MAXNODES=$(jq -r '.TrainsBenchCompute.MaxNodes' "$OUTPUTS")
IDS=(); IPS=()
for i in $(seq 0 $((MAXNODES-1))); do
  k=$(printf '%02d' "$i")
  IDS+=("$(jq -r ".TrainsBenchCompute.InstanceId$k" "$OUTPUTS")")
  IPS+=("$(jq -r ".TrainsBenchCompute.PrivateIp$k" "$OUTPUTS")")
done
echo "deploy: $MAXNODES nodes, bucket $BUCKET, profile $PROFILE, region $REGION"

# ── SSM helper (waits for the command, prints stdout) ─────────────────────────
ssm(){ local id=$1 cmd=$2 c
  c=$(aws ssm send-command --region "$REGION" --profile "$PROFILE" --instance-ids "$id" \
        --document-name AWS-RunShellScript --parameters commands="[\"$cmd\"]" \
        --query 'Command.CommandId' --output text) || return 1
  for _ in $(seq 1 40); do sleep 2
    case "$(aws ssm get-command-invocation --region "$REGION" --profile "$PROFILE" \
              --command-id "$c" --instance-id "$id" --query 'Status' --output text 2>/dev/null)" in
      Success|Failed|Cancelled|TimedOut) break;; esac
  done
  aws ssm get-command-invocation --region "$REGION" --profile "$PROFILE" \
    --command-id "$c" --instance-id "$id" --query 'StandardOutputContent' --output text 2>&1; }

# ── stage: keygen N + upload binaries/scripts/identities (once) ───────────────
stage(){ local n=$1 tmp; tmp=$(mktemp -d)
  TRAINS_BIN="$TRAINS_BIN" bash "$REPO/scripts/redis-chaos/keygen.sh" "$n" "$tmp/identities" >/dev/null
  FPS=$(cat "$tmp/identities/fingerprints.txt")
  aws s3 cp "$ARM/trains-valkey"       "s3://$BUCKET/e5/trains-valkey"       --region "$REGION" --profile "$PROFILE" --quiet
  aws s3 cp "$ARM/trains-valkey-chaos" "s3://$BUCKET/e5/trains-valkey-chaos" --region "$REGION" --profile "$PROFILE" --quiet
  aws s3 cp "$REPO/scripts/redis-chaos/node-bootstrap.sh" "s3://$BUCKET/e5/node-bootstrap.sh" --region "$REGION" --profile "$PROFILE" --quiet
  aws s3 cp "$REPO/scripts/redis-chaos/launch-node.sh"    "s3://$BUCKET/e5/launch-node.sh"    --region "$REGION" --profile "$PROFILE" --quiet
  for i in $(seq 0 $((n-1))); do aws s3 cp "$tmp/identities/id$i.json" "s3://$BUCKET/e5/id$i.json" --region "$REGION" --profile "$PROFILE" --quiet; done
  rm -rf "$tmp"
}

# ── launch a fresh n-node ring (kills any old proxy first) ────────────────────
launch_ring(){ local n=$1; local PA="" i
  for i in $(seq 0 $((n-1))); do PA="$PA $i=${IPS[$i]}:7000"; done; PA="${PA# }"
  local cids=() id
  for i in $(seq 0 $((n-1))); do
    local succ=${IPS[$(( (i+1)%n ))]}
    # Every node serves state transfer (PR-RJ-3b) on :7001 so a restarted peer
    # can rejoin through it.
    local launchenv="NODE_ID=$i RING_LISTEN=0.0.0.0:7000 RING_SUCCESSOR=$succ:7000 IDENTITY=/opt/trains/identity.json PEER_FP=$FPS PEER_ADDRS=\\\"$PA\\\" VALKEY_PASSWORD=$PW SNAP_LISTEN=0.0.0.0:7001 TRAINS_REDIS_BIN=/opt/trains/trains-valkey"
    # A restarted node rejoins PASSIVELY (PR-RJ-3c): catch up off-ring from a
    # survivor's :7001. Point it at the lowest-id node that isn't itself.
    local rejoin_ip; if [ "$i" -eq 0 ]; then rejoin_ip=${IPS[1]}; else rejoin_ip=${IPS[0]}; fi
    # Stage a per-node relaunch.sh as a real local file (proper quoting), upload
    # to S3 — the in-SSM printf approach mangled PEER_ADDRS' quotes. restart-proxy
    # runs this on the victim, which comes back as a passive replica.
    printf '#!/usr/bin/env bash\nNODE_ID=%s RING_LISTEN=0.0.0.0:7000 RING_SUCCESSOR=%s:7000 IDENTITY=/opt/trains/identity.json PEER_FP=%s PEER_ADDRS="%s" VALKEY_PASSWORD=%s SNAP_LISTEN=0.0.0.0:7001 REJOIN_FROM=%s:7001 TRAINS_REDIS_BIN=/opt/trains/trains-valkey bash /opt/trains/launch-node.sh\n' \
      "$i" "$succ" "$FPS" "$PA" "$PW" "$rejoin_ip" > "/tmp/relaunch$i.sh"
    aws s3 cp "/tmp/relaunch$i.sh" "s3://$BUCKET/e5/relaunch$i.sh" --region "$REGION" --profile "$PROFILE" --quiet
    cat > "/tmp/e5cmd$i.json" <<JSON
{"commands":[
 "set -e","mkdir -p /opt/trains","cd /opt/trains","pkill -9 -f /opt/trains/trains-valkey || true","sleep 1",
 "dnf install -y -q iptables iproute-tc >/dev/null 2>&1 || true",
 "for f in trains-valkey trains-valkey-chaos node-bootstrap.sh launch-node.sh; do aws s3 cp s3://$BUCKET/e5/\$f /opt/trains/\$f --region $REGION --quiet; done",
 "aws s3 cp s3://$BUCKET/e5/id$i.json /opt/trains/identity.json --region $REGION --quiet",
 "aws s3 cp s3://$BUCKET/e5/relaunch$i.sh /opt/trains/relaunch.sh --region $REGION --quiet",
 "chmod +x /opt/trains/trains-valkey /opt/trains/trains-valkey-chaos /opt/trains/node-bootstrap.sh /opt/trains/launch-node.sh /opt/trains/relaunch.sh",
 "valkey-cli -a $PW -p 6379 FLUSHALL 2>/dev/null || true",
 "VALKEY_PASSWORD=$PW bash /opt/trains/node-bootstrap.sh",
 "$launchenv bash /opt/trains/launch-node.sh",
 "echo NODE-$i-UP"
]}
JSON
    id=$(aws ssm send-command --region "$REGION" --profile "$PROFILE" --instance-ids "${IDS[$i]}" \
          --document-name AWS-RunShellScript --parameters file:///tmp/e5cmd$i.json --query 'Command.CommandId' --output text)
    cids+=("$id")
  done
  sleep 45
  local ok=0
  for i in $(seq 0 $((n-1))); do
    local o; o=$(aws ssm get-command-invocation --region "$REGION" --profile "$PROFILE" --command-id "${cids[$i]}" --instance-id "${IDS[$i]}" --query 'StandardOutputContent' --output text 2>&1)
    echo "$o" | grep -q "NODE-$i-UP" && ok=$((ok+1)) || echo "  node $i launch: $(echo "$o" | tail -1)"
  done
  # health check: SET on node0, GET on all
  ssm "${IDS[0]}" "valkey-cli -p 6380 SET _hc ok" >/dev/null; sleep 2
  for i in $(seq 0 $((n-1))); do
    [ "$(ssm "${IDS[$i]}" "valkey-cli -p 6380 GET _hc")" = "ok" ] || { echo "  RING UNHEALTHY at node $i"; return 1; }
  done
  # Clear the _hc key from EVERY engine (it replicated to all; flushing only
  # node 0 left a 1-key DBSIZE skew that failed the convergence check).
  for i in $(seq 0 $((n-1))); do ssm "${IDS[$i]}" "valkey-cli -a $PW -p 6379 FLUSHALL 2>/dev/null" >/dev/null; done
  echo "  ring of $n healthy"
}

# ── run one scenario on a fresh ring; echo PASS/FAIL, write a REPORT line ─────
run_scenario(){ local sched=$1 file="$SCHED_DIR/$1.json"
  [ -f "$file" ] || { echo ">>> $sched: SKIP (no schedule file)"; return; }
  local n; n=$(jq -r '.ring_size' "$file")
  if [ "$n" -gt "$MAXNODES" ]; then echo ">>> $sched: SKIP (needs $n nodes, deploy has $MAXNODES)"; return; fi
  echo "############ $sched (ring_size $n) ############"
  launch_ring "$n" || { echo ">>> $sched: FAIL (ring would not form)"; return; }

  # start load on node 0 (background), then drive the fault timeline
  ssm "${IDS[0]}" "rm -f /opt/trains/acked.json /opt/trains/load.log; nohup /opt/trains/trains-valkey-chaos --mode load --resp 127.0.0.1:6380 --count $COUNT --hold-secs $HOLD --abandon-secs $ABANDON --acked-out /opt/trains/acked.json > /opt/trains/load.log 2>&1 & echo go" >/dev/null
  local inst ipcsv; inst=$(IFS=,; echo "${IDS[*]:0:$n}"); ipcsv=$(IFS=,; echo "${IPS[*]:0:$n}")
  "$PY" "$SEQ" --schedule "$file" --instances "$inst" --ips "$ipcsv" --profile "$PROFILE" --region "$REGION"
  for _ in $(seq 1 45); do ssm "${IDS[0]}" "test -f /opt/trains/acked.json && echo D || echo w" | grep -q D && break; sleep 4; done
  local loadline; loadline=$(ssm "${IDS[0]}" "grep -E 'total acked|abandoned' /opt/trains/load.log" | tr '\n' ' ')
  echo "  load: $loadline"

  # A passive rejoiner (t1-rejoin) catches up by polling a survivor every ~200ms;
  # give it a few seconds after load ends to apply the final tail before verify.
  sleep 8

  # verify-local on every node whose proxy is still alive (a killed-and-not-
  # restarted victim is not a survivor and is expected to be stale)
  ssm "${IDS[0]}" "aws s3 cp /opt/trains/acked.json s3://$BUCKET/e5/acked.json --region $REGION --quiet" >/dev/null
  local pass=1 dbs=() i
  for i in $(seq 0 $((n-1))); do
    local alive; alive=$(ssm "${IDS[$i]}" "pgrep -fc /opt/trains/trains-valkey || echo 0")
    if ! echo "$alive" | grep -qE '[1-9]'; then echo "  node$i: (proxy down — victim, skipped)"; continue; fi
    local r; r=$(ssm "${IDS[$i]}" "aws s3 cp s3://$BUCKET/e5/acked.json /opt/trains/acked.json --region $REGION --quiet; /opt/trains/trains-valkey-chaos --mode verify-local --acked-in /opt/trains/acked.json --engine 127.0.0.1:6379 --password $PW --label node-$i --report-out /opt/trains/r.json >/dev/null 2>&1; cat /opt/trains/r.json")
    local at db; at=$(echo "$r" | grep -o '"acked_total": [0-9]*' | grep -o '[0-9]*$'); db=$(echo "$r" | grep -o '"dbsize": [0-9]*' | grep -o '[0-9]*$')
    echo "  node$i: acked=$at missing=$(echo "$r" | grep -o '"missing_keys": \[[^]]*\]') dbsize=$db"
    echo "$r" | grep -o '"missing_keys": \[[^]]*\]' | grep -q '\[\]' || pass=0
    dbs+=("$db")
  done
  # convergence: all surviving DBSIZE equal
  local u; u=$(printf '%s\n' "${dbs[@]}" | sort -u | wc -l | tr -d ' ')
  [ "$u" = 1 ] || { echo "  survivors did NOT converge (DBSIZE: ${dbs[*]})"; pass=0; }
  mkdir -p "$RESULTS"
  if [ "$pass" = 1 ]; then echo ">>> $sched: PASS (zero acked-write loss, survivors converged)"; echo "$sched PASS $loadline" >> "$RESULTS/summary.txt"
  else echo ">>> $sched: FAIL"; echo "$sched FAIL $loadline" >> "$RESULTS/summary.txt"; fi
}

# ── main ──────────────────────────────────────────────────────────────────────
DEFAULT=(t1-partition t1-rejoin t2-asymmetric-partition t2-clock-skew t2-burst-partition t1-multi-victim)
SCENARIOS=("$@"); [ ${#SCENARIOS[@]} -eq 0 ] && SCENARIOS=("${DEFAULT[@]}")

# stage for the largest ring any requested scenario needs (so identities exist)
NEED=$MAXNODES
echo "staging binaries + $NEED identities..."; stage "$NEED"

for s in "${SCENARIOS[@]}"; do run_scenario "$s"; done
echo ""; echo "=== E5 matrix summary ==="; cat "$RESULTS/summary.txt" 2>/dev/null || echo "(none)"
echo "Remember: AWS_PROFILE=<aws-profile> ./scripts/bench-aws/teardown.sh"
