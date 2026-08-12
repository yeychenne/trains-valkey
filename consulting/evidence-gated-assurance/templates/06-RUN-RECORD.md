# Run Record

Create one copy per attempt, including interrupted or rejected attempts.

## Identity

| Field | Value |
|---|---|
| Run ID | `<RUN-YYYYMMDD-NN>` |
| Evidence IDs | `<EV-...>` |
| Classification | `diagnostic / qualifying / interrupted / rejected` |
| Source revision | `<immutable revision>` |
| Dirty state | `false/true with explanation` |
| Operator | `<name or automation identity>` |
| Started/ended UTC | `<timestamps>` |

## Environment and procedure

- Host/image: `<identity>`
- Toolchain: `<versions>`
- Dependencies: `<lock/digests>`
- Exact command: `<command or script revision>`
- Parameters and seeds: `<values>`
- Target order/topology: `<schedule>`
- Budget/termination guard: `<where relevant>`

## Observations during execution

Record harness defects, retries, interruptions, host noise, deviations, and
operator actions chronologically. Do not silently discard outliers or failed
attempts.

## Artifacts

| Artifact ID | Path | Type | Immutable? | SHA-256/digest | Notes |
|---|---|---|---:|---|---|
| ART-01 | `<relative path>` | `<raw/report/log/trace>` | yes | `<digest>` |  |

## Validation

- [ ] expected operation/sample count
- [ ] oracle/reconciliation passed
- [ ] required files nonempty
- [ ] analyzer errors empty
- [ ] process/resource cleanup complete
- [ ] derived report traceable to raw inputs

## Result and qualification

Result: `<summary>`

Qualifies for gate: `yes/no`

Reason: `<why this run may or may not influence the declared gate>`

