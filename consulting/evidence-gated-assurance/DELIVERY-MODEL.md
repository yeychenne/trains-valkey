# Consulting Delivery Model

## Engagement tiers

The tiers tailor depth; they do not change evidence honesty.

| Tier | Typical duration | Appropriate use | Required core outputs |
|---|---:|---|---|
| Rapid decision review | 2-5 working days | dependency adoption, upgrade risk, architecture choice | charter, bounded claims, code/test assessment, highest-value falsifier, decision memo |
| Standard integration assurance | 2-4 weeks | critical open-source integration or subsystem replacement | full registers, refinement map, differential/property evidence, deterministic faults, public-boundary test, evidence package |
| Critical-system assurance | 6-12+ weeks | consistency, financial, safety, identity, or high-impact control planes | formal model where justified, independent methods, real fault campaign, operational/security evidence, reviewable assurance case |

Duration depends on code access, build reproducibility, oracle availability,
fault scope, and remediation cycles. The engagement charter records the actual
timebox and budget.

## Roles

| Role | Accountability |
|---|---|
| Decision owner | accepts the business/engineering disposition and residual risk |
| Evidence lead | owns claims, method selection, provenance, and final synthesis |
| Target maintainer | explains intended behavior and reviews refinement mappings |
| Independent oracle owner | supplies or reviews reference behavior where available |
| Run operator | executes qualifying procedures without changing declared gates |
| Reviewer | challenges assumptions, missing counterexamples, and overbroad wording |

One person may hold several roles, but the final memo should disclose where
independence is limited.

## Delivery cadence

1. **Intake checkpoint:** decision, scope, access, and stop conditions agreed.
2. **Claim workshop:** claims, invariants, exclusions, and fault model reviewed.
3. **Evidence-plan checkpoint:** methods and thresholds approved before runs.
4. **Narrow-gate review:** decide whether wider integration evidence is worth
   funding.
5. **Boundary-gate review:** inspect real-process and operational evidence.
6. **Decision review:** reconcile claim statuses and issue disposition.
7. **Transfer:** verify artifact package and hand over reproduction procedure.

## Standard client deliverables

- two-page executive decision memo;
- technical claim-to-evidence map;
- invariant and refinement registers;
- evidence plan and predeclared gates;
- reproducible test harness or exact runbook;
- immutable raw and derived evidence;
- machine-readable case manifest;
- limitations and future-test triggers; and
- prioritized remediation or investment recommendation.

## Quality bar

An engagement is not complete because tests are green. It is complete when:

- the requested decision is answered;
- each important claim has a status and boundary;
- evidence provenance is reproducible or explicitly limited;
- failed and interrupted attempts are distinguishable from qualifying runs;
- derived reports can be traced to raw inputs;
- the client knows what would invalidate the decision; and
- the next investment follows from evidence rather than momentum.

## Scaling across codebases

Maintain the same artifact identifiers and phase structure across cases. Reuse
templates and harness patterns, but do not reuse conclusions. Build a portfolio
index containing only non-confidential metadata:

- target category and integration shape;
- methods applied;
- time to first material counterexample;
- gates reached;
- final disposition;
- evidence gaps; and
- reusable harness components.

After several cases, this portfolio supports calibrated estimates: which
methods find defects earliest, what evidence clients can produce, where
integration claims usually fail, and when additional formal work changes a
decision.

