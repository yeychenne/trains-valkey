# TRAINS + Arctic Claim-to-Evidence Map

## Composition invariants

| Claim | Foundation | Composition method | Executable evidence | Result | Boundary |
|---|---|---|---|---|---|
| C1: accepted mutations have deterministic replicated effects | TRAINS broadcast/order assurance | command refinement and differential semantics | point-command equivalence; 4,096-command manifest; real ring convergence | PASS | five-command subset, NUL-free keys |
| C2: active replicas apply in delivery order | TRAINS uniform ordered delivery | map `Output::Deliver` to ordered store `apply` | healthy three-node ring; deterministic schedules; lifecycle manifest | PASS | active view members |
| C3: each effect changes a store at most once | protocol identity plus proxy dedup | observe `WriteDedup::first_seen` before apply | duplicate delivery and overlapping tail retry tests | PASS | retained dedup window assumptions apply |
| C4: acknowledgements follow local ordered apply | TRAINS origin/delivery trace | map pending-request removal to successful apply | read-after-ack integration; exact acknowledged-operation manifests | PASS | acknowledged mutations only |
| C5: snapshot at X plus tail after X has no gap or overlap effect | ordered delivery index | snapshot-index and delivered-tail refinement | passive catch-up; delayed install; lost poll; overlapping retry; atomic replacement | PASS | at least one survivor with snapshot/tail source |
| C6: passive or stale members cannot originate writes | TRAINS view state | public RESP rejection and lifecycle observation | passive rejoin and stale-restart tests | PASS | modeled process and membership faults |
| C7: readmission restores bidirectional convergence | TRAINS reconfiguration assurance | final snapshot, view adoption, promotion/readmission | recovered-member write plus survivor write; exact 56-key state on all three nodes | PASS | single-region three-node case |

## Assurance methods applied around the verified kernel

| Layer | Question answered | Principal artifacts | Outcome |
|---|---|---|---|
| Formal protocol foundation | Does the abstract ordering/reconfiguration kernel satisfy its properties? | `trains-rust` TLA+/TLC, Apalache, Kani/CBMC, PropTest, differential, trace, and Ivy artifacts | inherited kernel foundation |
| Refinement and trace mapping | Where do abstract events occur in the proxy and adapter? | `docs/ADR-002-trains-arctic-assurance-boundary.md` | explicit map; not mechanically checked end to end |
| Deterministic failure simulation | Do retry and failure schedules preserve C1-C7 observables? | `experiments/arctic-shadow/tests/deterministic_faults.rs` | PASS across six seeds and rotating victims |
| Real-process fault injection | Does the public RESP/TLS lifecycle preserve acknowledged state? | `experiments/arctic-shadow/tests/shadow_valkey.rs` | PASS through crash, reduced view, restart, promotion, and readmission |
| Differential semantics | Does Arctic match Valkey and an independent model? | `shadow_valkey.rs`; `differential_manifest.rs` | PASS for declared domain |
| Exact manifests and final state | Can every acknowledged effect be reconciled? | lifecycle and endpoint case manifests | PASS; exact final state |
| Performance after correctness | Does local map headroom survive progressively wider boundaries? | local, ordered, EC2-A1, and endpoint reports | primitive GO; complete-endpoint NO-GO |

## Decision trace

| Date | Boundary | Evidence | Decision | What it authorized |
|---|---|---|---|---|
| 2026-07-25 | semantic, deterministic, and lifecycle composition | seven shadow gates; C1-C7 executable evidence | GO | isolated performance exploration |
| 2026-07-25 | local concurrent map | approximately `12x` at eight threads | GO | ordered-writer/shared-reader prototype |
| 2026-08-11 | ordered Arctic generation on ARM64 | `18.07x` read-only, `2.93x` mixed, `0.96x` writes | GO | feature-gated proxy integration |
| 2026-08-11 | real RESP/TLS Arctic ring correctness | feature-off/on suites and integration lifecycle | PASS | symmetric endpoint qualification |
| 2026-08-12 | complete local three-node endpoint | 126 cases; `1.22x` reads, `0.99x` mixed | NO-GO | retain case study; stop EC2-A2 spend |

## Artifact inventory

| Category | Canonical artifact |
|---|---|
| Scope, fault model, C1-C7, refinement map | `docs/ADR-002-trains-arctic-assurance-boundary.md` |
| Initial implementation and gate record | `bench/EOD-2026-07-25.md` |
| Shadow experiment source and run instructions | `experiments/arctic-shadow/README.md` |
| Primitive benchmark report/raw data | `bench/results/arctic-data-plane-2026-07-25.md`; `.json` |
| Ordered interface report/raw data | `bench/results/arctic-ordered-data-plane-2026-08-11.md`; `.json` |
| EC2-A1 immutable evidence | `bench/results/ec2-arctic-a1/a3a8d0e39394d90551011b0d5d65718af45d0fa9/` |
| Feature-gated proxy correctness | `bench/results/arctic-proxy-integration-2026-08-11.md` |
| Endpoint gate protocol | `bench/TOMORROW-2026-08-12-ARCTIC-PROXY-GATE.md` |
| Endpoint harness | `scripts/bench-local/arctic-proxy-gate.sh`; `arctic-proxy-analyze.py` |
| Endpoint immutable evidence | `bench/results/arctic-proxy-local/7c03a87b5b0fac2624d7b0d28ec1412488e44299/` |
| Full run reasoning | endpoint evidence `RUN-NOTES.md` |
| Final generated endpoint report | endpoint evidence `REPORT.md` |
| Investment decision memo | `bench/reports/two-page-arctic-decision-2026-08-12.md` |

## Claim exclusions and triggers for new evidence

| Excluded claim | Why current evidence is insufficient | Triggered work |
|---|---|---|
| arbitrary cross-node linearizable reads | reads are local materialized-state observations | add a read protocol and linearizability histories |
| durability after all-node restart | recovery requires one surviving snapshot/tail source | add durable journal/checkpoint recovery and all-node restart tests |
| full Valkey compatibility | only five commands and NUL-free keys are covered | broaden command and atomicity differential suites |
| multi-region operation | no WAN or regional-failure model is exercised | define a new protocol/deployment model and regional fault campaign |
| material endpoint speedup | current endpoint missed both `1.5x` gates | profile and change shared paths, then predeclare a new local gate |

