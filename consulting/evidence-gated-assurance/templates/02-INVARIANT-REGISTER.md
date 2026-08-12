# Invariant Register

| ID | Invariant | Supports claims | Scope/assumptions | Violation shape | Observable/oracle | Severity |
|---|---|---|---|---|---|---|
| INV-01 | `<property that must always or eventually hold>` | `CL-01` | `<boundary>` | `<minimal counterexample>` | `<independent check>` | `critical` |

## Coverage review

| Claim ID | Required invariants | Direct evidence planned? | Independent corroboration? | Gap owner |
|---|---|---:|---:|---|
| CL-01 | `INV-01` | yes/no | yes/no | `<owner>` |

## Invariant design checklist

- [ ] State variables and actor identities are unambiguous.
- [ ] "Eventually" properties include fairness/time assumptions.
- [ ] Exactly-once claims separate duplicate suppression from durability.
- [ ] Acknowledgement points are explicit.
- [ ] Recovery invariants identify snapshot/log boundaries.
- [ ] Security invariants identify trust and authorization boundaries.
- [ ] A concrete violating history can be described.
