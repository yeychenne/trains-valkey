# Evidence-Gated Assurance Playbook

## Operating principles

**Decision first.** Name the decision owner, deadline, cost at risk, and
possible dispositions before investigating code.

**Claims before tests.** A large test suite is not an assurance argument until
its results are connected to explicit claims and assumptions.

**Boundaries matter.** A property shown for an algorithm, library, process, or
single node does not automatically hold for the integrated service.

**Cheapest strong falsifier first.** Start with the least expensive method that
could disprove the next claim. Spend on wider, slower, or cloud-based evidence
only after narrower gates pass.

**Independent oracles where possible.** Compare against a reference
implementation, independent model, exact manifest, or externally observable
contract instead of reusing the implementation's own logic as its oracle.

**Negative evidence is a result.** Preserve failed runs, scope reductions, and
NO-GO decisions with the same care as successful measurements.

## Phase 0: Intake and decision framing

Record:

- the business or engineering decision;
- target repository, revision, license, and dependency provenance;
- decision owner and technical evidence owner;
- deadline, budget ceiling, and environments available;
- cost of a false GO and cost of a false NO-GO; and
- acceptable decisions: GO, conditional GO, NO-GO, or insufficient evidence.

Deliverable: `00-ENGAGEMENT-CHARTER.md` and initialized `case.json`.

Gate G0: the engagement has one concrete decision. If the request is only
"review this code," narrow it before proceeding.

## Phase 1: Claim boundary and exclusions

Write each proposed claim as a falsifiable statement with a subject, property,
conditions, and observable. Record explicit non-claims and assumptions.

Example:

> For every mutation acknowledged by an active node in a three-member,
> single-region deployment, every surviving active member eventually contains
> the same effect exactly once.

Avoid adjectives such as safe, robust, scalable, or production-ready without
measurable definitions.

Deliverable: `01-CLAIM-REGISTER.md`.

Gate G1: each important claim is bounded by workload, fault, topology,
environment, and time where relevant.

## Phase 2: System, trust, and fault model

Inventory components and ownership boundaries:

- verified or trusted foundations;
- code under assessment;
- independently developed dependencies;
- operating system, network, storage, identity, and deployment services;
- human/operator actions; and
- external oracles.

For each boundary, list normal events, adversarial inputs, crashes, retries,
delays, duplication, reordering, partial failure, restart, and state loss. Mark
faults as included, excluded, or assumed impossible.

Deliverable: the system and fault sections of `00-ENGAGEMENT-CHARTER.md`.

Gate G2: every claim names the faults under which it is expected to hold.

## Phase 3: Invariants and observables

Convert claims into the smallest useful set of invariants. Each invariant must
have:

- an identifier;
- a precise statement;
- scope and assumptions;
- a violating trace or counterexample shape;
- an observable or oracle; and
- severity if violated.

Prefer invariants that can be checked at more than one boundary. For example,
"ack follows apply" can be inspected in a model trace, instrumented in process
events, and reconciled through an acknowledged-operation manifest.

Deliverable: `02-INVARIANT-REGISTER.md`.

Gate G3: no critical claim depends only on an unobservable internal intention.

## Phase 4: Refinement and trace map

Map abstract events and properties to implementation events:

- source file/function or interface;
- state before and after;
- emitted event or trace field;
- error and retry paths;
- test hook or production observable; and
- evidence expected.

This phase often finds the highest-value defects: an abstract acknowledgement
may occur before durable apply; a snapshot may omit a generation number; a
passive member may still accept writes.

Deliverable: `03-REFINEMENT-MAP.md`.

Gate G4: every critical invariant has at least one implementation event and one
independent observable. Unmapped claims are labeled assumptions or blocked.

## Phase 5: Select methods and predeclare gates

Use `METHOD-CATALOG.md` to select methods based on claim risk and available
oracles. Do not select tools for prestige. A deterministic state-machine test
may provide stronger relevant evidence than model checking an unrelated
abstraction.

For each evidence item record:

- claim and invariant covered;
- method and boundary;
- exact target revision and environment;
- test schedule, seed, workload, and oracle;
- expected artifacts;
- pass/fail threshold; and
- action after PASS, FAIL, or inconclusive evidence.

Deliverables: `04-EVIDENCE-PLAN.md` and `05-DECISION-GATES.md`.

Gate G5: decision criteria exist before the qualifying run starts.

## Phase 6: Narrow semantic evidence

Start below the full system where failures are cheap to diagnose:

- unit and contract tests;
- type, static, or bounded implementation checks;
- property and metamorphic tests;
- differential comparison with a reference;
- independent expected-state models; and
- serialization and compatibility fixtures.

Use fixed seeds and retain minimized counterexamples. Validate the harness with
an intentional fault when practical.

Gate G6: the component satisfies the declared semantics at its narrowest useful
boundary. Otherwise stop, remediate, or narrow the claim.

## Phase 7: Deterministic composition and fault schedules

Exercise the interaction between components without yet paying the full
deployment cost. Systematically vary actors, victims, retries, delays, and
recovery points. Record the complete schedule and reconcile exact final state.

Useful patterns:

- rotate every member through each fault role;
- duplicate, lose, delay, and replay boundary messages;
- restart from stale and empty state;
- overlap recovery with live operations;
- vary operation order with deterministic seeds; and
- check both safety throughout and convergence at completion.

Gate G7: all scheduled faults preserve the in-scope invariants, or every
counterexample has a disposition.

## Phase 8: Real-process and public-boundary evidence

Repeat the most important scenarios through public protocols and real process
lifecycle operations. Include configuration, identity, transport, persistence,
and deployment behavior that simulations omit.

Prefer exact external oracles:

- acknowledged-operation manifests;
- final-state hashes or full snapshots;
- linearizability histories;
- audit-log reconciliation;
- process exit and restart records; and
- externally captured traces.

Gate G8: the claim survives at the boundary where clients and operators
actually experience the system.

## Phase 9: Performance and resource investment gate

Performance follows correctness unless performance itself is the only claim.
Measure progressively:

1. primitive or algorithm boundary;
2. architecture-matched component boundary;
3. integrated local endpoint;
4. target platform; and
5. distributed or production-like deployment only when prior gates justify it.

Use symmetric drivers, identical schedules, target-order rotation, sufficient
repetitions, predeclared summary statistics, CPU/memory telemetry, and an
explicit reference denominator. Never present a primitive benchmark as full
service throughput.

Gate G9: compare the result with the predeclared investment threshold. A local
failure normally blocks cloud or scale spending until the architecture changes.

## Phase 10: Decision and claim ledger

For each claim set one status:

- supported within boundary;
- refuted;
- conditional;
- out of scope; or
- insufficient evidence.

Issue one engagement disposition: GO, conditional GO, NO-GO, or insufficient
evidence. State what the decision authorizes and what it does not authorize.

Deliverable: `07-FINAL-DECISION-MEMO.md`, updated `case.json`, and an immutable
evidence bundle.

Gate G10: a reader can follow every material conclusion back to a testable
claim, invariant, method, run, and artifact.

## Phase 11: Preserve, transfer, and reuse

Retain:

- measured revision and evidence revision;
- exact commands, toolchains, lock files, and environment identity;
- raw outputs and derived reports;
- seeds and fault schedules;
- binary or image hashes where relevant;
- interrupted or rejected runs, clearly separated from qualifying evidence;
- decision chronology; and
- excluded claims with triggers for future work.

Run `scripts/verify-case.sh` before delivery. Transfer the executive memo,
technical evidence map, machine-readable manifest, and reproduction procedure
together.

## Efficiency rules

1. Investigate the highest-severity, least-expensive falsifier first.
2. Stop widening when a required gate fails.
3. Reuse one deterministic schedule and oracle across implementations.
4. Separate diagnostic runs from qualifying evidence.
5. Automate provenance before running expensive tests.
6. Keep raw data immutable; regenerate summaries from raw inputs.
7. Distinguish product optimization from evidence needed for the current
   decision.
8. Add a method only when it covers an unaddressed risk or provides independent
   corroboration.

## Completion test

An engagement is ready to close when:

- the decision question has been answered within its boundary;
- critical claims have explicit statuses;
- evidence references resolve and integrity checks pass;
- limitations are prominent rather than buried;
- another test would address a new claim, changed architecture, or material
  residual uncertainty; and
- the decision owner can explain why the next investment is authorized or
  rejected.
