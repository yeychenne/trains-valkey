# Client Intake Questionnaire

Use this before or during the first workshop. Short, concrete answers are more
valuable than complete architecture prose.

## Decision and value

1. What decision must be made, by whom, and by what date?
2. What options are being compared, including "do nothing"?
3. What investment or exposure follows a GO?
4. What is the credible worst consequence of a false GO?
5. What is lost through a false NO-GO or delayed decision?
6. Which claims would make the decision easy if they were established?

## Target and change

1. Which exact repository, package, service, and revision are in scope?
2. Is the work adoption, replacement, upgrade, integration, remediation, or
   independent validation?
3. Which code is client-owned, third-party, generated, or externally hosted?
4. What licenses, export rules, data restrictions, or confidentiality terms
   apply?
5. Can dependencies and build inputs be pinned reproducibly?
6. Which maintainers or subject-matter experts are available?

## Required properties

1. What must never happen?
2. What must eventually happen, and within what time?
3. At what point does the system acknowledge or commit work?
4. What data loss, duplication, reordering, or stale behavior is acceptable?
5. Which compatibility surface is required?
6. Which security identities and trust boundaries matter?
7. What performance, latency, capacity, or cost threshold changes the decision?

## Boundary and faults

1. Is the claim about a function, library, process, endpoint, deployment, or
   multi-site service?
2. Which topologies, workloads, data shapes, and concurrency levels matter?
3. Which crashes, network faults, retries, timeouts, restarts, and correlated
   losses are in scope?
4. Which operator errors or adversarial actions matter?
5. What durability source remains after failure?
6. Which scenarios are explicitly out of scope for this decision?

## Existing evidence and oracles

1. What specifications, ADRs, tests, formal models, threat models, incidents,
   and benchmarks already exist?
2. Is there a mature reference implementation or previous version?
3. Can accepted operations and final state be independently reconciled?
4. Are production histories, traces, audit logs, or failure reports available?
5. Can known defects or intentional mutations validate the harness?
6. Which evidence has already influenced the client's view?

## Access and execution

1. Can the code build without network or privileged access?
2. What local, CI, cloud, staging, or production-like environments are
   available?
3. What credentials or approvals may block fault injection or deployment?
4. What budget and automatic termination controls apply?
5. Can raw evidence be retained, and for how long?
6. Who may execute qualifying runs and approve gate changes?

## Closing questions

1. What result would cause an immediate stop?
2. What narrow PASS would justify the next week or next dollar?
3. Which residual risks can the decision owner explicitly accept?
4. What evidence must be transferable to auditors, customers, or maintainers?
5. What should remain reusable after confidential details are removed?
