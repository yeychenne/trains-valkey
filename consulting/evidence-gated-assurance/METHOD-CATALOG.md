# Assurance Method Catalog

Select methods by the failure they can reveal and the decision they support.
No single method covers every boundary.

| Method | Best question | Typical output | Strength | Common limitation |
|---|---|---|---|---|
| Formal specification | Is the intended behavior precise and internally consistent? | state machine, temporal properties | exposes ambiguity before code | model may omit implementation behavior |
| Explicit-state model checking | Do bounded actors and states contain counterexamples? | traces and checked bounds | exhaustive inside the bound | state explosion; bounded topology/data |
| Symbolic/parameterized verification | Does a property hold beyond enumerated instances? | inductive invariant or proof obligation | stronger generality | abstraction and proof expertise required |
| Implementation model checking | Can selected code paths violate assertions or memory safety? | counterexample at code level | reaches implementation details | bounded inputs and environment models |
| Static analysis/type guarantees | Which defect classes are structurally prevented? | diagnostics and enforced constraints | cheap and repeatable | limited semantic/system coverage |
| Property-based testing | Do generated inputs violate algebraic or state properties? | seed and minimized counterexample | broad input exploration | only as strong as generators/properties |
| Metamorphic testing | Can behavior be checked without a full oracle? | relation violations | useful for opaque outputs | relation may miss shared defects |
| Differential testing | Does the candidate match a trusted or independent implementation? | mismatches and reproducer | strong semantic oracle | shared bugs and domain differences |
| Trace/refinement validation | Does concrete execution correspond to abstract events? | mapped execution trace | tests specification-to-code connection | sampled executions are not a proof |
| Deterministic simulation | Do replayable schedules preserve invariants? | seed, schedule, state manifest | diagnosable concurrency/fault evidence | simulation fidelity |
| Fault injection | Does the real process survive included failures? | process logs and external outcomes | crosses runtime/deployment boundary | incomplete fault coverage, nondeterminism |
| Linearizability/history checking | Are concurrent operations consistent with a legal sequential history? | accepted history or violation | precise concurrent-object evidence | history size and checker assumptions |
| Exact manifest reconciliation | Is every accepted effect present exactly as claimed? | operation and final-state manifest | simple independent accounting | requires stable identifiers and observability |
| Security threat modeling | What assets, trust boundaries, and abuse paths matter? | threats and mitigations | prioritizes adversarial risk | requires validation of mitigations |
| Fuzzing | Can malformed or unexpected input violate safety properties? | crashing/hanging input corpus | high-volume robustness testing | weak for long protocol histories alone |
| Soak and resource testing | Do time, load, or leaks break the claim? | latency, CPU, memory, error trends | captures duration/resource effects | expensive and hard to reproduce |
| Controlled benchmark | Does a change meet a declared performance threshold? | raw samples and statistical summary | supports investment decisions | invalid if boundaries or denominators differ |
| Production observation | Does behavior hold under real traffic and operations? | telemetry, traces, incidents | highest environmental realism | weak control and counterfactuals |

## Selection matrix

Choose at least one direct method for every critical risk. Add an independent
method when false confidence would be costly.

| Risk | Minimum useful method | Stronger corroboration |
|---|---|---|
| ambiguous protocol or lifecycle | formal state-machine specification | model checking and trace mapping |
| data corruption or semantic drift | property tests or exact model | differential oracle plus manifests |
| concurrency ordering | deterministic schedules | linearizability/history checking |
| retry, duplication, or recovery | deterministic simulation | real-process fault injection |
| security boundary | threat model and negative tests | independent review and deployed controls |
| dependency replacement | API/semantic differential suite | fault lifecycle and performance gates |
| durability | crash/restart tests | correlated-loss and external recovery campaign |
| scalability | architecture-matched benchmark | endpoint and target-platform qualification |
| operational safety | runbook exercise | production-like failure rehearsal |

## Independence ladder

Evidence becomes more persuasive when its oracle is independent of the code
under assessment:

1. implementation assertions;
2. tests written against the same internal representation;
3. separately implemented expected-state model;
4. mature reference implementation;
5. externally observed histories or manifests; and
6. mechanically checked specification/refinement.

Using multiple levels is useful only when they fail differently. Five wrappers
around the same implementation oracle do not provide five independent checks.

## Method recording rule

For every selected method, record:

- method identifier and version/toolchain;
- claim and invariant identifiers;
- target boundary;
- assumptions and completeness limits;
- exact command or procedure;
- oracle and expected counterexample;
- artifacts produced; and
- gate influenced.

