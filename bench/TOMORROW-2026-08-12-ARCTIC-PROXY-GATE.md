# Arctic proxy local gate - restart plan for 2026-08-12

## Start state

- Branch: `experiment/arctic-aws-validation`.
- Integrated adapter baseline: `a7d139e` (`experiment: wire Arctic proxy adapter`).
- The adapter, real three-node RESP/TLS integration test, feature-on/off test
  suites, Clippy, and optimized feature-on binary build are green.
- AWS has not been started. EC2-A2 remains blocked on this local gate.
- Local host checked at EOD: Darwin ARM64 with Valkey, `redis-benchmark`, `jq`,
  AWS CLI, Node/npx, and Python 3 available. `memtier_benchmark` is absent and
  is not required by this plan.

Read first:

1. `bench/EOD-2026-08-11-ARCTIC-AWS.md`
2. `bench/results/arctic-proxy-integration-2026-08-11.md`
3. `docs/ADR-002-trains-arctic-assurance-boundary.md`

## Objective

Measure the real RESP endpoint path with one workload implementation against:

1. feature-off `MemStore` (`Mutex<BTreeMap>` materialization) behind the TRAINS
   proxy;
2. feature-on `OrderedArcticStore` behind the same proxy and ring setup; and
3. unmodified Valkey over loopback RESP as an endpoint reference.

The decision comparison is Arctic proxy versus mutex proxy. Direct Valkey has a
different process and replication boundary, so it must not be used as the GO
denominator or described as an apples-to-apples map comparison.

## First implementation block

Add a committed Rust RESP workload driver and local orchestrator. Reuse the
request schedule and JSON conventions from
`experiments/arctic-shadow/src/bin/data-plane-bench.rs`, but drive every target
through persistent RESP connections. Do not depend on `memtier_benchmark`.

Build the proxy variants into separate target directories so Cargo feature
selection cannot overwrite one artifact with the other:

```sh
CARGO_TARGET_DIR=target/bench-mutex \
  cargo build --release --locked -p trains-valkey --bin trains-valkey

CARGO_TARGET_DIR=target/bench-arctic \
  cargo build --release --locked -p trains-valkey --bin trains-valkey \
  --features arctic-proxy
```

The orchestrator must:

- start and stop a fresh three-node local TRAINS ring for each proxy target;
- start a fresh persistence-disabled Valkey process for the endpoint reference;
- preload 10,000 fixed-width ASCII keys with 32-byte values outside timing;
- use one persistent RESP connection per client and one outstanding request per
  connection;
- issue deterministic read-only, 90/10 read/write, and write-only schedules;
- validate every reply and exact final sampled values before accepting a round;
- rotate target order between rounds;
- sample latency every 16 operations; and
- collect target CPU time and peak RSS as well as client throughput and p99.

Keep the command set to exact-arity `GET` and `SET`. This prevents unsupported
Valkey compatibility from becoming an accidental variable.

## Two-stage run

Smoke first, with one short round and both client counts. Its purpose is only to
catch orchestration, reply-validation, and cleanup failures:

```text
keys=1,000; operations/client=2,000; clients=1,8; repetitions=1
```

Run the qualifying gate only after the smoke artifacts validate:

```text
keys=10,000; operations/client=100,000; clients=1,8; repetitions=7
workloads=read-only,read-90-write-10,write-only
latency sample interval=16 operations
```

Use medians over the seven interleaved rounds. Also retain every paired round;
an aggregate must not hide a bimodal or steadily degrading result.

## Evidence contract

Write immutable evidence below:

```text
bench/results/arctic-proxy-local/<git-sha>/
  REPORT.md
  environment.json
  manifest.json
  summary.json
  rounds/*.json
  correctness.log
  process-telemetry.jsonl
  binary-sha256.txt
  run.log
```

`manifest.json` records the commit, dirty-state check, exact commands, random
seed, workload dimensions, backend order, Valkey version, Rust version, host
identity, and start/end timestamps. The runner must fail on a dirty worktree,
an unpushed commit, a bad reply, a missing artifact, or a process that survives
cleanup.

## Decision gate

EC2-A2 is **NO-GO** if any correctness check fails or required evidence is
missing.

For the eight-client median, Arctic proxy must reach:

- at least 1.5x mutex-proxy throughput for read-only;
- at least 1.5x mutex-proxy throughput for 90/10; and
- write-only p99 below 3x mutex-proxy p99, unless the report contains a
  queue-level explanation supported by telemetry.

Record write-only throughput even though it is not the primary gate. Stop and
investigate before AWS if its regression exceeds 15%, if CPU saturation differs
materially between paired targets, or if RSS grows across rounds.

## AWS hold point

Do not run `arctic-ec2-a1.sh`; EC2-A1 is already complete. Do not deploy EC2-A2
until the local report has an explicit GO and the branch is committed, pushed,
and clean. The later AWS run retains the 60 USD authorization ceiling, one
`c7g.2xlarge`, six-hour shutdown guard, evidence collection, and automatic
teardown.

The claim remains local concurrent reads with ordered replicated mutations.
Cross-node linearizable reads, all-node restart durability, full Valkey
compatibility, and multi-region operation remain outside scope.
