# Claim Register

Use stable IDs. A claim states subject, property, conditions, and observable.
Do not mark a claim supported until its evidence references are complete.

| ID | Claim | Criticality | Boundary/conditions | Status | Evidence IDs | Limitations |
|---|---|---|---|---|---|---|
| CL-01 | `<falsifiable statement>` | `critical/high/medium/low` | `<workload, topology, faults, time>` | `proposed` |  |  |

Allowed status:

- `proposed`
- `supported`
- `refuted`
- `conditional`
- `out-of-scope`
- `insufficient-evidence`

## Non-claims

| ID | Statement not claimed | Reason | Evidence needed to add it later |
|---|---|---|---|
| NC-01 | `<explicit exclusion>` | `<why excluded>` | `<new method/test>` |

## Claim wording audit

- [ ] Every critical adjective has an observable definition.
- [ ] Algorithm, component, process, endpoint, deployment, and regional
      boundaries are distinguished.
- [ ] Safety, liveness, durability, security, compatibility, and performance
      claims are separated.
- [ ] Assumptions are not presented as established properties.
- [ ] Each supported claim can cite at least one independent observable.

