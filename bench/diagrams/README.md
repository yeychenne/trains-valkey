# Architecture diagrams — trains-valkey

draw.io editable sources for the four canonical architecture views referenced
from the paper (`bench/reports/paper-trains-replicated-redis-draft-2026-05-26.md`)
and the blog (`bench/reports/blog-trains-replicated-redis-2026-05-26.md`).

| File | What it shows | Replaces in paper |
|---|---|---|
| `01-ec2-bench-infrastructure.drawio` | The AWS layout: operator host, VPC, SSM + S3 endpoints, IAM, 3 ring nodes + coordinator, Security Group. | The ASCII art in `bench/ARCHITECTURE.md` §Overview |
| `02-protocol-layer-stack.drawio` | The 5-crate / 4-layer dependency stack: `trains-core` ← `trains-net` ← `trains-recovery` ← `trains-valkey` ← app, plus `trains-cli`. | Paper §4.1 (Crate layout) — currently a table only |
| `03-resp-data-flow.drawio` | The RESP write path: classify → resolve → broadcast → deliver in total order → apply, plus the four invariants box. | Paper §3.1 (Architecture) — currently ASCII art |
| `04-view-change-sequence.drawio` | Sequence-diagram-style view of a single permanent crash: T0 (SIGKILL) → T1 (TCP_USER_TIMEOUT) → T2 (connector reads close) → T3 (`unreachable_rx`) → T4–T7 (Gather/Compute/Install) → T8 (resume). | Paper §3.3 (Reconfiguration) + §5.2 — no existing figure |

## Exporting to SVG / PNG

The `.drawio` files are XML and open directly in:
- the **draw.io desktop app** (File → Export As → SVG / PNG)
- the **diagrams.net web app** (https://app.diagrams.net — local-only, no cloud upload by default)
- the **draw.io CLI** (`npx @hpcc-js/wasm` or `drawio-export-cli`) for batch export

Suggested export command once tooling is installed:

```bash
for f in bench/diagrams/*.drawio; do
  drawio --export --format svg --output "${f%.drawio}.svg" "$f"
done
```

Until SVGs are committed, the paper / blog Markdown references the `.drawio`
sources directly so the figures can be opened and regenerated.

## Editing conventions

- One diagram per file. Multi-page `.drawio` files are harder to embed.
- Colour palette (consistent across all four diagrams):
  - `#F8CECC` (red) — clients / external entities / dying components
  - `#DAE8FC` (blue) — proxy / ring nodes / process boxes
  - `#D5E8D4` (green) — data stores / successful state
  - `#FFE6CC` (orange) — coordinator role / control events
  - `#E1D5E7` (purple) — protocol / recovery layer
  - `#FFF2CC` (yellow) — origin-side resolution / kernel-layer events
  - `#F5F5F5` (grey) — annotation / legend boxes
- Edge styles:
  - solid red, thick = ring TLS (`trains-net` traffic)
  - solid green = successful reply
  - dashed = control plane (SSM, view-change tokens)
  - solid black = application-level data path
- Keep font sizes ≥ 10 pt for body text, ≥ 14 pt for layer / region titles.

## Status

| Diagram | Reviewer | Status |
|---|---|---|
| 01 EC2 bench | — | Draft, awaiting first read |
| 02 Protocol stack | — | Draft, awaiting first read |
| 03 RESP data flow | — | Draft, awaiting first read |
| 04 View-change sequence | — | Draft, awaiting first read |

Generated 2026-05-27 as part of the day's track-2 build. Pair with the
bench-data gap analysis at `bench/reports/bench-data-gap-analysis-2026-05-27.md`.
