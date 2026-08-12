# EOD: TRAINS + Arctic and Assurance Consulting Toolkit - 2026-08-12

## Executive status

The TRAINS + Arctic investigation is complete for its declared research
question. The result is a **correctness success and endpoint-performance
NO-GO** on the current proxy architecture.

The work also produced the larger asset intended by the investigation: a
technology-neutral, evidence-gated consulting method for assessing proprietary
or open-source code and making grounded GO/NO-GO investment decisions.

No additional Arctic experiment is required for the present claim. Do not run
EC2-A2. No AWS resource was launched during the final qualification phase, and
none of the authorized 60 USD was spent.

## Arctic decision

The method generated rational decisions at progressively wider boundaries:

| Boundary | Result | Decision |
|---|---|---|
| C1-C7 composition correctness | semantic, deterministic-fault, real-process lifecycle, Valkey differential, and exact-manifest gates passed | GO for scoped performance exploration |
| Isolated concurrent data plane | Arctic approximately `12x` `Mutex<BTreeMap>` at eight threads | GO for ordered-writer/shared-reader refinement |
| ARM64 architecture-matched data plane | `18.07x` reads, `2.93x` mixed, and `0.96x` writes | GO for feature-gated proxy integration |
| Real RESP/TLS proxy correctness | feature-off/on suites and Arctic lifecycle integration passed | PASS to full endpoint qualification |
| Complete local three-node endpoint | 126 valid cases; Arctic/mutex `1.22x` reads and `0.99x` mixed | NO-GO for EC2-A2 |

The endpoint run also passed correctness/evidence, write-throughput, write-p99,
CPU, and RSS checks. It failed both predeclared `1.5x` read-scaling gates. The
map-level concurrency benefit is real, but it does not dominate RESP,
scheduling, TRAINS circulation/delivery, and acknowledgement costs in the
current complete endpoint.

Arctic remains feature-gated and off by default. It is valuable as executable
composition evidence and a worked example of the assurance method, not as a
current production-performance recommendation.

## Assurance distinction

The TRAINS protocol kernel is checked through seven independent formal and
executable methods: TLA+/TLC, Apalache, Kani/CBMC, property testing,
differential testing, trace validation, and Ivy parameterized verification.

The complete TRAINS + Arctic composition is a **layered assurance argument**.
It combines a refinement map, deterministic simulation, real-process fault
injection, differential semantics, exact acknowledged-operation manifests,
final-state comparison, and progressively wider performance gates. It must not
be described as one mechanically checked end-to-end formal proof.

## Artifacts completed today

### Decisive Arctic evidence

- `bench/results/arctic-proxy-local/7c03a87b5b0fac2624d7b0d28ec1412488e44299/`
- `REPORT.md`: generated qualification result and gate decision.
- `RUN-NOTES.md`: full reasoning, smoke, interrupted pilot, bounded rerun,
  variability, interpretation, and investment decision.
- 126 per-case JSON files and 126 checkpoint records.
- 14,826 process telemetry samples.
- raw results, environment, binary hashes, commands, logs, and summary.

### Arctic assurance-case package

Path: `bench/assurance/arctic-case-study/`

- `METHOD-APPLICATION.md`: thirteen stages from boundary definition through
  final decision.
- `EVIDENCE-MAP.md`: C1-C7 and claim-to-method/test/artifact traceability.
- `manifest.json`: machine-readable revisions, gates, results, and exclusions.
- `verify.sh`: validates the index and decisive 126-case evidence.
- `README.md`: case entry point and formal-assurance terminology.

### Final decision documents

- `bench/reports/two-page-arctic-decision-2026-08-12.md`
- `docs/ADR-002-trains-arctic-assurance-boundary.md`
- `bench/TOMORROW-2026-08-12-ARCTIC-PROXY-GATE.md`
- `bench/EOD-2026-08-11-ARCTIC-AWS.md`

### Technology-neutral consulting toolkit

Path: `consulting/evidence-gated-assurance/`

- `PLAYBOOK.md`: complete evidence-gated engagement workflow.
- `METHOD-CATALOG.md`: formal, executable, fault, operational, security, and
  performance method selection.
- `INTAKE-QUESTIONNAIRE.md`: first-workshop decision and scope questions.
- `DELIVERY-MODEL.md`: rapid, standard, and critical-system engagement tiers.
- `TWO-PAGE-USAGE-GUIDE.md`: client-facing artifact and usage explanation.
- nine per-case templates covering charter through retrospective.
- JSON case-manifest schema and template.
- `new-case.sh`: creates a new assessment workspace.
- `verify-case.sh`: checks references, required evidence, completion rules, and
  optional SHA-256 integrity.
- `test-toolkit.sh`: exercises successful scaffolding/completion and rejects
  incomplete cases, broken references, and tampered evidence.
- `examples/arctic-case.md`: bridge from the generic method to this case.

## Verification state

The assurance package verifier passes:

```sh
bench/assurance/arctic-case-study/verify.sh
```

Expected:

```text
PASS: assurance index and 126-case endpoint decision evidence are complete.
```

The consulting toolkit self-test passes:

```sh
consulting/evidence-gated-assurance/scripts/test-toolkit.sh
```

Expected:

```text
PASS: scaffolding, completion, references, and integrity checks work.
```

The final changes are documentation, indexing, schema, and shell tooling. The
full feature-off/on Rust tests and Clippy gates passed before the qualifying
run; no code-path change was made afterward.

## Git handoff

Repository: `https://github.com/yeychenne/trains-valkey`

Branch: `experiment/arctic-aws-validation`

Pre-EOD pushed HEAD: `9b1759b7db5eb0425f0e71900cf6f27772fbf877`

Key commit sequence:

| Commit | Purpose |
|---|---|
| `a7d139e` | feature-gated Arctic proxy adapter |
| `c912775` | real proxy endpoint harness |
| `7c03a87` | bounded 126-case qualification protocol |
| `f91f5ae` | immutable qualification evidence and NO-GO |
| `d1d7f1d` | final Arctic investment memo |
| `066a9ef` | reusable Arctic assurance-case package |
| `58f25ce` | technology-neutral consulting playbook and tooling |
| `9b1759b` | two-page artifact usage guide |

The branch was clean and synchronized with `origin` before this EOD document.

## Claim boundary retained

Current evidence supports the declared five-command, NUL-free-key,
single-region composition with at least one surviving snapshot/tail recovery
source. The following remain explicit non-claims:

- arbitrary cross-node linearizable reads;
- durability after every member loses runtime recovery state;
- full Valkey command compatibility;
- multi-region operation; and
- a material full-endpoint Arctic throughput improvement.

Any of these claims requires a changed design and new evidence plan. They do
not block the current final report.

## Recommended next work

### Priority 1: final report synthesis

Use the existing `EVIDENCE-MAP.md` to produce the paper's compact final
claim-to-evidence table. For every report claim, record:

- claim and boundary;
- C1-C7 or other invariant;
- kernel or composition method;
- executable test/run;
- canonical artifact;
- result; and
- limitation or assumption.

This is editorial and audit work, not another experiment. The final report
should emphasize that the method correctly authorized intermediate exploration
and then stopped investment when the wider endpoint gate failed.

### Priority 2: demonstrate consulting repeatability

Choose a second open-source component with a meaningfully different risk
profile. Good selection criteria:

- a real adoption/replacement decision rather than a demonstration-only task;
- an independent reference or externally observable contract;
- one material concurrency, recovery, compatibility, security, or durability
  risk;
- a locally reproducible build and bounded first gate; and
- enough difference from Arctic to test method generality.

Create the case with:

```sh
consulting/evidence-gated-assurance/scripts/new-case.sh \
  <case-id> <case-directory>
```

Do not begin by choosing formal tools. Begin with the intake questionnaire,
decision, claim boundary, and cheapest material falsifier.

### Optional product work

Profiling RESP, Tokio scheduling, TRAINS circulation/delivery, and
acknowledgement could support a future higher-throughput product. It is not
required evidence for the current methodological result and should be funded
only as a new objective with new gates.

## Tomorrow's re-entry order

1. Read this EOD.
2. Read `bench/reports/two-page-arctic-decision-2026-08-12.md`.
3. Read `bench/assurance/arctic-case-study/EVIDENCE-MAP.md`.
4. Read `consulting/evidence-gated-assurance/TWO-PAGE-USAGE-GUIDE.md`.
5. Choose final-report synthesis or second-case selection as the next explicit
   objective.
6. Do not rerun Arctic or start AWS unless the claim or architecture changes.
