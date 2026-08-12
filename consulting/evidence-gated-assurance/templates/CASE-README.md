# Assurance Case: `<case-id>`

Status: draft

Decision question: `<one sentence>`

Decision owner: `<name or role>`

Target: `<repository/component and revision>`

## Working order

1. Complete `00-ENGAGEMENT-CHARTER.md`.
2. Register bounded claims in `01-CLAIM-REGISTER.md`.
3. Derive invariants in `02-INVARIANT-REGISTER.md`.
4. Map abstract and concrete events in `03-REFINEMENT-MAP.md`.
5. Select evidence in `04-EVIDENCE-PLAN.md`.
6. Freeze criteria in `05-DECISION-GATES.md` before qualifying runs.
7. Create one run record from `06-RUN-RECORD.md` for each qualifying attempt.
8. Reconcile the result in `07-FINAL-DECISION-MEMO.md` and `case.json`.

Put raw evidence under `evidence/<run-id>/`. Keep diagnostic or interrupted
runs, but mark them non-qualifying. Never overwrite a qualifying run.

Verify references and completion rules with the playbook's `verify-case.sh`.

