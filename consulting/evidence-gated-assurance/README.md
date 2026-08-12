# Evidence-Gated Software Assurance Playbook

Version: 1.0

This toolkit turns code assessment into a traceable sequence of claims,
invariants, tests, evidence, and investment decisions. It is technology
neutral: the target may be proprietary code, an open-source dependency, a
protocol, a database component, an AI service, or an integration of several
systems.

The method is designed for consulting engagements where the useful outcome is
not always approval. A defensible NO-GO, scope reduction, or request for a
specific missing proof can be as valuable as a GO.

## Core idea

1. State the decision and claim boundary before choosing tools.
2. Turn important claims into observable invariants.
3. Map those invariants to concrete implementation events.
4. Apply the cheapest strong method that can falsify the next claim.
5. Widen the boundary only after the narrower gate passes.
6. Preserve exact provenance and failed attempts.
7. Decide against criteria declared before the qualifying evidence is read.

The result is a layered assurance argument. Call it an end-to-end formal proof
only when the refinement from specification through implementation has itself
been mechanically established.

## Package contents

| Path | Purpose |
|---|---|
| `PLAYBOOK.md` | End-to-end engagement workflow, gates, and stopping rules |
| `TWO-PAGE-USAGE-GUIDE.md` | Client-facing explanation of the artifacts and practical workflow |
| `METHOD-CATALOG.md` | Selectable formal, executable, operational, and performance methods |
| `DELIVERY-MODEL.md` | Engagement tiers, roles, cadence, and client deliverables |
| `INTAKE-QUESTIONNAIRE.md` | Questions used to bound a new client assessment quickly |
| `templates/` | Registers and reports copied into each new case |
| `schema/case-manifest.schema.json` | Machine-readable case contract |
| `scripts/new-case.sh` | Creates a new case workspace from the templates |
| `scripts/verify-case.sh` | Checks manifest references, evidence files, and completion rules |
| `examples/arctic-case.md` | Worked example and links to the source evidence |

## Start a case

From the repository root:

```sh
consulting/evidence-gated-assurance/scripts/new-case.sh \
  dependency-upgrade-2026-08 /tmp/dependency-upgrade-case
```

Then complete the charter and registers in phase order. Check the package at
any time with:

```sh
consulting/evidence-gated-assurance/scripts/verify-case.sh \
  /tmp/dependency-upgrade-case
```

The generated case is intentionally incomplete. Verification reports missing
or inconsistent evidence; it does not convert unanswered questions into a
pass.

Check the toolkit itself after modifying templates or scripts:

```sh
consulting/evidence-gated-assurance/scripts/test-toolkit.sh
```

## What is reusable consulting IP

The asset is the decision process and evidence architecture:

- claim and exclusion discipline;
- invariant and refinement templates;
- risk-based method selection;
- progressive boundary widening;
- deterministic, replayable test design;
- evidence provenance and integrity checks;
- predeclared gates and stop rules; and
- concise technical and executive decision formats.

Individual tools and target projects may be open source. The repeatable way
they are selected, combined, audited, and converted into decisions is the
consulting capability.
