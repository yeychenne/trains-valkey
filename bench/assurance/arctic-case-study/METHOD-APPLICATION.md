# Applying the Layered Assurance Method: TRAINS + Arctic

## Purpose

This playbook records the complete method used to decide whether an
independently developed concurrent map could become the local data plane under
TRAINS ordering. It is both a reproduction guide for this case and a reusable
sequence for evaluating another storage component.

The method is evidence-gated. A positive result at one boundary authorizes the
next experiment; it does not establish the claim at a wider boundary.

## Step 1: State the question and claim boundary

Question: can TRAINS retain ownership of distributed ordering, deduplication,
acknowledgement, recovery, and membership while Arctic provides concurrent
local materialized state?

Declared command domain: `SET`, `GET`, `DEL`, `EXISTS`, and `DBSIZE`, with
NUL-free keys. Declared deployment domain: one region with at least one
surviving recovery source.

Explicit non-claims: arbitrary cross-node linearizable reads, durability after
all members lose runtime state, full Valkey compatibility, and multi-region
operation.

Artifacts:

- `docs/ADR-002-trains-arctic-assurance-boundary.md`
- `bench/results/arctic-data-plane-2026-07-25.md`

Rule: do not design tests or interpret results until the service boundary and
non-claims are written down.

## Step 2: Identify the verified foundation

TRAINS supplies the abstract protocol properties. Its kernel is checked in the
sister `trains-rust` repository through:

1. TLA+/TLC specification and bounded model checking;
2. Apalache symbolic checking;
3. Kani/CBMC implementation model checking;
4. property-based testing;
5. differential testing;
6. implementation trace validation; and
7. Ivy parameterized verification at unbounded N.

The measured composition pins `trains-core`, `trains-net`, and
`trains-recovery` to `trains-rust` commit
`da8c173878ed291a0935389de133313ef75fd132`, recorded in `Cargo.lock`.

Application rule: inherit only the properties established at the kernel
boundary. Do not claim that importing a verified component automatically
verifies the proxy, adapter, storage engine, or recovery orchestration.

## Step 3: Define composition invariants and a refinement map

The case introduced C1-C7 before performance work:

- C1 deterministic replicated effect for every accepted mutation;
- C2 ordered apply on every active replica;
- C3 at-most-once materialized effect per `(origin, request_id)`;
- C4 acknowledgement only after local ordered delivery and apply;
- C5 gap-free snapshot plus tail reconstruction;
- C6 no replicated writes from a passive or stale member; and
- C7 bidirectional convergence after readmission.

The refinement map connects abstract events to concrete locations: `WriteOp`
creation, `Input::LocalBroadcast`, `Output::Deliver`, store `apply`, dedup,
pending-request removal, snapshot index, delivered-log tail, passive rejection,
and final view adoption.

Artifact: `docs/ADR-002-trains-arctic-assurance-boundary.md`.

Exit condition: every invariant has an implementation event and a planned
observable. Unmapped properties remain assumptions, not claims.

## Step 4: Establish semantic equivalence at the narrow boundary

Before introducing distributed faults, compare Arctic with Valkey and with an
independent expected-state model.

Evidence:

- direct point-command and snapshot equivalence;
- duplicate delivered-operation suppression; and
- a seeded 4,096-command differential manifest, including binary values.

Run from `experiments/arctic-shadow`:

```sh
cargo test --release -- --nocapture --test-threads=1
```

Key tests:

- `experiments/arctic-shadow/tests/shadow_valkey.rs`
- `experiments/arctic-shadow/tests/differential_manifest.rs`

Exit condition: exact agreement for the declared command and key domain. A
silent fallback or unexplained mismatch is a stop.

## Step 5: Challenge composition under deterministic faults

Use fixed seeds so every failure is replayable. Rotate all three victims and
exercise stale restart, a lost recovery poll, delayed snapshot installation,
overlapping tail retries, duplicate delivery, and resumed writes.

Evidence:

- six deterministic seeds;
- all three members used as victims; and
- exact expected manifests after each completed schedule.

Test: `experiments/arctic-shadow/tests/deterministic_faults.rs`.

Exit condition: C1-C7 observables remain exact under every schedule. Passing
random chaos without a replayable trace is not equivalent evidence.

## Step 6: Cross the real-process lifecycle boundary

Run the public RESP and real TLS TRAINS paths. The lifecycle progresses through
healthy convergence, live-member crash, reduced-view writes, stale restart,
snapshot and tail recovery, promotion, readmission, and writes originating on
both sides of the restored full view.

The final oracle is an exact 56-key acknowledged-operation manifest on every
Valkey and Arctic replica.

Test: `live_crash_rejoin_promote_restores_mirrored_full_view` in
`experiments/arctic-shadow/tests/shadow_valkey.rs`.

Result artifact: `bench/EOD-2026-07-25.md`.

Decision: correctness gates passed. This authorized a scoped performance
exploration; it did not authorize a production switch.

## Step 7: Measure the isolated mechanism

Ask whether Arctic removes map-lock contention before paying to integrate it.
Compare Arctic, `Mutex<BTreeMap>`, and Valkey under identical point-operation
schedules at one, two, four, and eight workers.

```sh
cd experiments/arctic-shadow
ARCTIC_BENCH_KEYS=10000 \
ARCTIC_BENCH_OPS_PER_THREAD=100000 \
ARCTIC_BENCH_THREADS=1,2,4,8 \
ARCTIC_BENCH_REPETITIONS=3 \
cargo run --release --bin data-plane-bench
```

Result: approximately tied at one thread and approximately `12x` the mutex
baseline at eight threads. This established headroom at the map boundary.

Artifacts:

- `bench/results/arctic-data-plane-2026-07-25.md`
- `bench/results/arctic-data-plane-2026-07-25.json`

Decision: GO for an ordered-writer/shared-reader prototype.

## Step 8: Refine the interface without weakening invariants

Create one ordered mutation owner and clonable shared point readers. Keep
mutations, aggregate reads, snapshot export/replacement, and readmission behind
the ordered owner. Publish replacement snapshots atomically so readers never
observe a partially restored generation.

Evidence:

- all original seven shadow gates still pass;
- concurrent writer/readers make progress;
- snapshot replacement is atomic to readers; and
- the architecture-matched ordered mutex baseline pays the same queue and
  acknowledgement cost.

Artifacts:

- `bench/results/arctic-ordered-data-plane-2026-08-11.md`
- `bench/results/arctic-ordered-data-plane-2026-08-11.json`
- commit `0e5637adffbd861153255be407d346ccd38904c6`

Decision: GO for bounded platform validation.

## Step 9: Validate the mechanism on the target platform

EC2-A1 tested the architecture-matched implementation on ARM64 before proxy
integration. It used a pinned commit, current Rust toolchain, one six-hour
guarded host, seven interleaved rounds, binary hashes, CPU/RSS telemetry, and
automatic teardown.

```sh
AWS_PROFILE=<profile> MAX_BUDGET_USD=60 \
./scripts/bench-aws/arctic-ec2-a1.sh all
```

Result: `18.07x` read-only and `2.93x` 90/10 throughput versus ordered mutex at
eight threads; write throughput was `0.96x`. The write p99 debt remained an
explicit integration risk.

Artifacts:

- `bench/results/ec2-arctic-a1/a3a8d0e39394d90551011b0d5d65718af45d0fa9/`
- commit `a3a8d0e39394d90551011b0d5d65718af45d0fa9`

Decision: GO for feature-gated local proxy integration.

## Step 10: Integrate behind an off-by-default feature

Wire `OrderedArcticStore` into the binary without changing the default path.
Route only valid `GET` and single-key `EXISTS` through the shared reader.
Retain ordered routing for mutations, `DBSIZE`, multi-key `EXISTS`, snapshots,
and readmission.

Verification commands:

```sh
cargo test --workspace --all-targets --locked
cargo test --workspace --all-targets --features arctic-proxy --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy --workspace --all-targets --features arctic-proxy --locked -- -D warnings
```

The real RESP/TLS ring test checks read-after-ack, convergence, reads while the
ordered mutex is held, mixed readers/writers, aggregate routing, and replacement
snapshot visibility.

Artifacts:

- `crates/trains-valkey/src/arctic.rs`
- `crates/trains-valkey/tests/arctic_proxy.rs`
- `bench/results/arctic-proxy-integration-2026-08-11.md`
- commit `a7d139e688e5a6a80d046391596d98ee87277a69`

Decision: correctness half PASS. Performance remained a separate gate.

## Step 11: Predeclare the complete-endpoint decision gate

Use one deterministic persistent-connection RESP driver for the feature-off
mutex proxy, Arctic proxy, and unmodified Valkey. Rotate target order and
measure throughput, sampled p99, CPU, and RSS.

Predeclared GO conditions:

- every reply, operation count, final-value sample, and artifact validates;
- eight-client read-only Arctic/mutex throughput is at least `1.5x`;
- eight-client 90/10 Arctic/mutex throughput is at least `1.5x`;
- write-only p99 remains below `3x`; and
- write throughput remains within 15% of the mutex proxy.

Artifacts:

- `bench/TOMORROW-2026-08-12-ARCTIC-PROXY-GATE.md`
- `scripts/bench-local/arctic-proxy-gate.sh`
- `scripts/bench-local/arctic-proxy-analyze.py`
- commit `c91277571f704f0fea9b86d902786a872203e4c5`

Rule: define thresholds and evidence completeness before inspecting a
qualifying run.

## Step 12: Smoke, qualify, retain failed attempts, and decide

Run orchestration smoke first:

```sh
./scripts/bench-local/arctic-proxy-gate.sh smoke
```

The first uniform-count qualification exposed excessive duration in the
write-only matrix. It was interrupted through the cleanup path, retained
outside qualification evidence, and used to bound operation counts without
changing workloads, repetitions, clients, thresholds, or decision metrics.

The committed bounded qualification used 100,000 read-only, 20,000 mixed, and
5,000 write-only operations per client, with seven rounds:

```sh
./scripts/bench-local/arctic-proxy-gate.sh qualification
```

Result: all 126 cases and evidence checks passed. Arctic/mutex was `1.22x` for
eight-client reads and `0.99x` for 90/10. Write throughput was `1.04x` and p99
was `0.88x`.

Artifacts:

- `bench/results/arctic-proxy-local/7c03a87b5b0fac2624d7b0d28ec1412488e44299/`
- `bench/EOD-2026-08-11-ARCTIC-AWS.md`
- commit `f91f5aee0d4b3824c3efb73bb1cb567d43c47a9e`

Decision: correctness and write-regression gates PASS; both read-scaling gates
FAIL. NO-GO for EC2-A2 and further Arctic performance spending on the current
proxy architecture.

## Step 13: Convert the result into a reusable decision record

Preserve the positive and negative results together. The primitive GO explains
why integration was rational. The endpoint NO-GO explains why stopping was
rational. Neither result invalidates the other because they answer questions at
different boundaries.

Final interpretation:

- `bench/reports/two-page-arctic-decision-2026-08-12.md`
- `bench/assurance/arctic-case-study/EVIDENCE-MAP.md`
- `bench/assurance/arctic-case-study/manifest.json`

The next paper task is editorial synthesis, not another Arctic experiment.
Additional tests are required only for a new claim or a changed architecture.
