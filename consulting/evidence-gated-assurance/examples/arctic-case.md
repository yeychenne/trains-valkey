# Worked Example: TRAINS + Arctic

## Decision question

Can an independently developed lock-free ordered map provide concurrent local
materialized state under TRAINS ordering without weakening the declared
replication and recovery invariants, and does the benefit justify full endpoint
performance investment?

## Why this example matters

The case produced different decisions at progressively wider boundaries:

| Boundary | Evidence | Decision |
|---|---|---|
| semantics and lifecycle | seven composition gates, C1-C7, Valkey differential oracle | GO for performance exploration |
| isolated local map | approximately `12x` mutex throughput at eight threads | GO for interface refinement |
| ordered-writer/shared-reader on ARM64 | `18.07x` reads, `2.93x` mixed, `0.96x` writes | GO for proxy integration |
| real RESP/TLS proxy correctness | feature-off/on suites and lifecycle integration | PASS to endpoint qualification |
| complete local three-node endpoint | 126 cases; `1.22x` reads and `0.99x` mixed | NO-GO for EC2-A2 |

The method prevented a valid primitive result from becoming an unsupported
system-level claim. It also prevented cloud spending after a predeclared local
gate failed.

## Source artifacts

The complete worked assurance package is under:

- `bench/assurance/arctic-case-study/README.md`
- `bench/assurance/arctic-case-study/METHOD-APPLICATION.md`
- `bench/assurance/arctic-case-study/EVIDENCE-MAP.md`
- `bench/assurance/arctic-case-study/manifest.json`
- `bench/assurance/arctic-case-study/verify.sh`

The final executive interpretation is:

- `bench/reports/two-page-arctic-decision-2026-08-12.md`

The decisive immutable endpoint evidence is:

- `bench/results/arctic-proxy-local/7c03a87b5b0fac2624d7b0d28ec1412488e44299/`

## Reusable lesson

A GO authorizes only the next named boundary. It does not accumulate into a
permanent endorsement. The ability to stop after stronger counterevidence is a
feature of the assurance process and a consulting deliverable in its own right.

