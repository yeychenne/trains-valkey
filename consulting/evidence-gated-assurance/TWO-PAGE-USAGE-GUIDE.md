# Evidence-Gated Assurance Artifacts: What They Are and How to Use Them

## Purpose and consulting value

The Evidence-Gated Software Assurance package is a reusable way to evaluate
whether code, an open-source dependency, or a system integration supports a
specific engineering or investment decision. It converts a broad request such
as "review this database component" into a traceable chain:

> decision -> bounded claims -> invariants -> implementation mapping ->
> selected methods -> qualifying evidence -> predeclared gates -> GO/NO-GO

It can be applied to a library adoption, database replacement, protocol,
critical upgrade, AI service, security control, or distributed integration.
Depth changes with risk, but the evidence discipline remains the same.

The consulting asset is the ability to select complementary methods, apply
them at the right boundary, preserve auditable evidence, and stop investment
when a wider test is no longer justified.

The package supports GO, conditional GO, NO-GO, and insufficient evidence. A
negative conclusion is valuable when it prevents an unsafe integration,
unsupported claim, or unnecessary expenditure.

## The artifact set

The artifacts are organized into four groups.

### 1. Method and engagement guidance

`PLAYBOOK.md` defines the workflow, stopping rules, and completion conditions.
It reaches performance or deployment testing only after narrower gates pass.

`METHOD-CATALOG.md` describes formal, static, property, differential, trace,
simulation, fault, history, reconciliation, security, and benchmark methods.
Methods add value when they expose the relevant risk through independent
assumptions or oracles.

`INTAKE-QUESTIONNAIRE.md` structures the first workshop: decision owner, target,
false-GO cost, properties, faults, exclusions, evidence, access, and budget.

`DELIVERY-MODEL.md` defines rapid, standard, and critical-system engagement
tiers. It also records roles, checkpoints, expected client deliverables, and
the quality bar for completion.

### 2. Per-case technical and decision records

The templates form one coherent assurance case:

| Artifact | Question answered |
|---|---|
| `00-ENGAGEMENT-CHARTER.md` | What decision, target, budget, faults, and exclusions govern the work? |
| `01-CLAIM-REGISTER.md` | What exactly is proposed, supported, refuted, conditional, or out of scope? |
| `02-INVARIANT-REGISTER.md` | What must always or eventually hold, and how would a violation be observed? |
| `03-REFINEMENT-MAP.md` | Where do abstract properties occur in concrete code, state transitions, and traces? |
| `04-EVIDENCE-PLAN.md` | Which method, command, seed, oracle, environment, and artifact will test each claim? |
| `05-DECISION-GATES.md` | Which thresholds authorize or stop the next investment? |
| `06-RUN-RECORD.md` | What happened in each diagnostic, qualifying, interrupted, or rejected run? |
| `07-FINAL-DECISION-MEMO.md` | What is the disposition, basis, residual risk, and next investment? |
| `08-CASE-RETROSPECTIVE.md` | Which methods were efficient, what did they cost, and what can be reused? |

Stable identifiers such as `CL-01`, `INV-01`, `G-01`, and `ART-...` connect
tests and artifacts directly to the final claims.

### 3. Machine-readable evidence integrity

`case.json` indexes provenance, scope, claims, invariants, methods, gates, runs,
decisions, and artifacts, including optional SHA-256 hashes.

`scripts/verify-case.sh` checks identifiers, references, paths, required files,
hashes, and completion consistency. It rejects incomplete or unsupported final
cases.

The verifier checks the integrity of the package. It does not decide whether a
test is scientifically sufficient; that remains part of method selection,
technical review, and the decision owner's judgment.

### 4. Worked example

`examples/arctic-case.md` connects the method to TRAINS + Arctic. Strong local
evidence justified integration; a later endpoint gate stopped AWS investment.
It demonstrates the process, not a conclusion to copy.

## How to use the package

### Step 1: Create the case workspace

From the repository root:

```sh
consulting/evidence-gated-assurance/scripts/new-case.sh \
  payment-ledger-upgrade-2026-09 /path/to/client-case
```

The command copies templates, creates evidence directories, initializes
`case.json`, and refuses to overwrite existing work.

### Step 2: Bound the decision before inspecting results

Complete the intake and charter with the decision owner. Pin the target
revision and dependency provenance. State included faults and explicit
non-claims. Then write falsifiable claims. Distinguish algorithm, library,
process, endpoint, deployment, and multi-region boundaries.

### Step 3: Derive invariants and map them to code

For each critical claim, define the smallest observable properties that must
hold. Describe a violating history. Map abstract events such as accept, commit,
apply, authorize, snapshot, retry, or recover to concrete functions, state
changes, errors, and traces.

Record unmapped obligations as assumptions, blockers, or exclusions.

### Step 4: Select the cheapest decisive evidence

Choose methods by risk. Begin with cheap, diagnosable evidence such as contract,
static, property, model, or differential tests. Progress to deterministic
faults, real processes, and paid deployment only when narrower gates pass.

Record target, command, seed, oracle, outputs, and failure action.

### Step 5: Freeze decision gates

Define pass, fail, and inconclusive criteria before a qualifying run. State what
a PASS authorizes and what a FAIL stops. If a threshold changes after results
are visible, preserve the old result and create a new gate version.

### Step 6: Execute and preserve every attempt

Create a run record for diagnostic, qualifying, interrupted, and rejected
attempts. Store raw outputs under `evidence/<run-id>/`; do not overwrite a
qualifying directory. Retain environment identity, exact commands, seeds,
toolchains, logs, hashes, and cleanup status.

Only runs satisfying frozen provenance and completeness rules influence a gate.

### Step 7: Verify and decide

Run:

```sh
consulting/evidence-gated-assurance/scripts/verify-case.sh /path/to/client-case
```

Reconcile every claim as supported, refuted, conditional, out of scope, or
insufficiently evidenced. Issue one disposition and state exactly what it
authorizes. The final memo should identify decisive evidence, counterevidence,
residual risks, future-test triggers, and the next funded action or stop.

### Step 8: Improve the consulting practice

Record effort, spend, first material counterexample, method yield, highest gate,
and spend avoided. A sanitized cross-case index reveals which methods find
important defects earliest and improves future estimates.

## Practical usage rules

- Reuse the structure and automation, not conclusions from a previous case.
- Keep client-confidential evidence inside the client case; retain only
  sanitized method metrics in the consulting portfolio.
- Use at least one independent oracle for each critical claim when practical.
- Do not describe layered empirical assurance as an end-to-end formal proof.
- Do not widen to cloud, scale, or production-like testing after a required
  local gate fails unless the architecture or claim has changed.
- Close the engagement when the decision is grounded. Additional engineering
  may be useful product work without being required evidence for that decision.

Used this way, the artifact set becomes a repeatable consulting operating
system: it makes assessments faster to start, harder to overclaim, easier to
audit, and progressively more efficient as experience accumulates across
codebases.
