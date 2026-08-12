# Engagement Charter

## Decision

| Field | Value |
|---|---|
| Case ID | `<case-id>` |
| Decision question | `<one falsifiable decision question>` |
| Decision owner | `<name or role>` |
| Evidence lead | `<name or role>` |
| Deadline | `<date>` |
| Budget ceiling | `<amount or none>` |
| Allowed dispositions | `GO / CONDITIONAL GO / NO-GO / INSUFFICIENT EVIDENCE` |
| False-GO cost | `<impact>` |
| False-NO-GO cost | `<impact>` |

## Target and provenance

| Field | Value |
|---|---|
| Repository/package | `<URL or identifier>` |
| Measured revision | `<immutable revision>` |
| License | `<license and obligations>` |
| Build lock/provenance | `<lock file, image digest, SBOM, or limitation>` |
| Maintainer/source trust | `<assessment>` |

## System boundary

Describe included components, data flows, actors, trust boundaries, and the
public interface at which the final decision will apply.

`<system boundary>`

## Included faults and conditions

| ID | Fault/condition | Included behavior | Rationale |
|---|---|---|---|
| F-01 | `<example: process crash>` | `<what may fail and when>` | `<why material>` |

## Exclusions and assumptions

| ID | Exclusion/assumption | Consequence | Trigger for reconsideration |
|---|---|---|---|
| X-01 | `<excluded topology or failure>` | `<claim limitation>` | `<new claim or architecture>` |

## Access and environments

List source, build, test, cloud, production telemetry, credentials, data, and
subject-matter access. Record unavailable access as an evidence limitation.

## Stop rules

- Stop or narrow after a critical semantic mismatch.
- Stop wider/paid testing when a required local gate fails.
- Stop when evidence provenance cannot identify the measured target.
- Escalate when a new claim materially changes the agreed scope.

## Acceptance

| Role | Name | Date | Accepted scope/notes |
|---|---|---|---|
| Decision owner |  |  |  |
| Evidence lead |  |  |  |

