# TRAINS + Arctic Assurance Case Artifact Set

Status: complete

Final decision: retain the feature-gated correctness case study; stop Arctic
performance investment on the current proxy architecture and do not run
EC2-A2.

This directory indexes how the layered assurance method was applied to the
TRAINS + Arctic case. It does not duplicate raw evidence. Each result remains
in its immutable or canonical repository location.

## Artifacts in this set

| Artifact | Purpose |
|---|---|
| `METHOD-APPLICATION.md` | Reusable, step-by-step procedure and the decisions taken at each gate |
| `EVIDENCE-MAP.md` | Traceability from claims and C1-C7 to methods, tests, results, and limitations |
| `manifest.json` | Machine-readable index of commits, stages, decisions, and evidence paths |
| `verify.sh` | Checks the index and the decisive endpoint evidence without rerunning experiments |

## How to read the case

1. Start with `METHOD-APPLICATION.md` to understand the sequence and why each
   level was necessary.
2. Use `EVIDENCE-MAP.md` when auditing a claim or preparing the final report.
3. Use `manifest.json` to check paths or build later tooling around the case.
4. Read the final interpretation in
   `bench/reports/two-page-arctic-decision-2026-08-12.md`.
5. Read the complete endpoint narrative and raw-result index in
   `bench/results/arctic-proxy-local/7c03a87b5b0fac2624d7b0d28ec1412488e44299/RUN-NOTES.md`.

## Terminology

The TRAINS protocol kernel is checked by seven independent formal and
executable methods: TLA+/TLC, Apalache, Kani/CBMC, property testing,
differential testing, trace validation, and Ivy parameterized verification.
Those source artifacts live in the sister `trains-rust` repository. This case
pins its kernel dependencies to commit
`da8c173878ed291a0935389de133313ef75fd132`.

The TRAINS + Arctic system is supported by a layered composition assurance
argument. The refinement map and composition tests are not one mechanically
checked end-to-end proof. This artifact set preserves that boundary.
