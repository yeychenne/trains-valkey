# Evidence Plan

Freeze qualifying parameters and expected artifacts before execution.

| Evidence ID | Claims/invariants | Method | Boundary | Oracle | Procedure/command | Parameters/seeds | Expected artifacts | Qualifying? |
|---|---|---|---|---|---|---|---|---:|
| EV-01 | `CL-01 / INV-01` | `<catalog method>` | `<component/process/service>` | `<independent oracle>` | `<exact command or runbook>` | `<fixed inputs>` | `<paths/types>` | yes |

## Harness validation

| Check | Procedure | Expected result | Status |
|---|---|---|---|
| Intentional defect is detected | `<mutation/fault>` | `<specific rejection>` | pending |
| Missing artifact is rejected | `<remove/rename in diagnostic copy>` | verifier fails | pending |
| Wrong target revision is rejected | `<dirty/unpushed/digest test>` | runner refuses | pending |
| Interrupted run cannot qualify | `<interrupt diagnostic run>` | incomplete status | pending |

## Provenance requirements

- immutable source revision;
- dependency lock or image digest;
- exact command and environment;
- tool and compiler versions;
- seed, workload, schedule, and topology;
- raw output plus derived report;
- target binary/image hashes where material;
- start/end timestamps and operator; and
- explicit diagnostic/qualifying designation.

## Planned order

Order evidence by cheapest material falsifier. State which PASS authorizes the
next expense and which FAIL stops it.

