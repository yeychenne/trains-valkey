# Decision Gates

## Gate definition

| Gate ID | Decision question | Preconditions | PASS criteria | FAIL criteria | Inconclusive criteria | Evidence IDs | PASS action | FAIL action |
|---|---|---|---|---|---|---|---|---|
| G-01 | `<what investment is being authorized?>` | `<earlier gates>` | `<predeclared thresholds>` | `<stop conditions>` | `<missing/noisy evidence>` | `EV-01` | `<next phase>` | `<stop/remediate/narrow>` |

## Freeze record

| Field | Value |
|---|---|
| Gate version | `<version/hash>` |
| Frozen at | `<UTC timestamp>` |
| Frozen by | `<decision/evidence owner>` |
| Target revision | `<revision>` |
| Exceptions allowed | `<none or explicit process>` |

## Result record

Complete only after qualifying evidence exists.

| Gate ID | Result | Evidence IDs | Deviations | Decision owner | Date |
|---|---|---|---|---|---|
| G-01 | `PASS / FAIL / INCONCLUSIVE / NOT RUN` |  |  |  |  |

Changing a threshold after seeing results creates a new gate version. Preserve
the original result and rationale.

