# Refinement and Trace Map

Map each abstract event or property to code and an observable. One abstract
event may require several concrete steps.

| Map ID | Invariant | Abstract event/property | Concrete implementation event | Location/revision | Observable/trace | Error/retry behavior | Evidence ID |
|---|---|---|---|---|---|---|---|
| RM-01 | `INV-01` | `<abstract event>` | `<function/state transition>` | `<file:symbol @ revision>` | `<event/manifest/state>` | `<failure path>` | `EV-01` |

## Unmapped obligations

| ID | Claim/invariant | Missing mapping | Current treatment | Owner/date |
|---|---|---|---|---|
| UO-01 | `<ID>` | `<gap>` | `assumption / blocked / out-of-scope` | `<owner/date>` |

## Review questions

- Can acknowledgement occur before the mapped effect is complete?
- Can retry, duplication, cancellation, or timeout bypass the mapping?
- Can restart reconstruct the state needed by the invariant?
- Can a stale or unauthorized actor invoke the event?
- Does the trace prove the event or merely log intent?
- Does the test oracle reuse the same code as the implementation?

