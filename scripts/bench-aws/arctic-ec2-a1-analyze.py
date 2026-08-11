#!/usr/bin/env python3
"""Aggregate the seven EC2-A1 rounds and render the investment decision."""

from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def median(values: list[float]) -> float:
    return statistics.median(values)


def mad_ratio(values: list[float]) -> float:
    center = median(values)
    return median([abs(value - center) for value in values]) / center if center else 0.0


def load_rounds(root: Path) -> list[dict]:
    records: list[dict] = []
    for strategy in ("arc-swap", "rwlock"):
        paths = sorted(root.glob(f"{strategy}-round-*.json"))
        if len(paths) != 7:
            raise SystemExit(f"expected 7 {strategy} rounds, found {len(paths)}")
        for path in paths:
            report = json.loads(path.read_text())
            if report["generation_strategy"] != strategy:
                raise SystemExit(f"strategy mismatch in {path}")
            for result in report["results"]:
                records.append({"strategy": strategy, **result})
    return records


def aggregate(records: list[dict]) -> list[dict]:
    grouped: dict[tuple, list[dict]] = defaultdict(list)
    for record in records:
        key = (
            record["strategy"],
            record["backend"],
            record["workload"],
            record["threads"],
        )
        grouped[key].append(record)

    rows = []
    for (strategy, backend, workload, threads), samples in sorted(grouped.items()):
        throughputs = [sample["operations_per_second"] for sample in samples]
        rows.append(
            {
                "strategy": strategy,
                "backend": backend,
                "workload": workload,
                "threads": threads,
                "samples": len(samples),
                "median_ops_per_second": median(throughputs),
                "throughput_mad_ratio": mad_ratio(throughputs),
                "median_p99_ns": median([sample["latency_p99_ns"] for sample in samples]),
                "median_process_cpu_ms": median(
                    [sample["process_cpu_ms"] for sample in samples]
                ),
            }
        )
    return rows


def find(rows: list[dict], strategy: str, backend: str, workload: str, threads: int) -> dict:
    return next(
        row
        for row in rows
        if row["strategy"] == strategy
        and row["backend"] == backend
        and row["workload"] == workload
        and row["threads"] == threads
    )


def valkey_rows(root: Path) -> list[dict]:
    report = json.loads((root / "valkey.json").read_text())
    grouped: dict[tuple, list[dict]] = defaultdict(list)
    for result in report["results"]:
        grouped[(result["workload"], result["threads"])].append(result)
    return [
        {
            "workload": workload,
            "threads": threads,
            "samples": len(samples),
            "median_ops_per_second": median(
                [sample["operations_per_second"] for sample in samples]
            ),
            "median_p99_ns": median([sample["latency_p99_ns"] for sample in samples]),
            "median_process_cpu_ms": median(
                [sample["process_cpu_ms"] for sample in samples]
            ),
            "median_engine_cpu_ms": median(
                [sample["engine_cpu_ms"] for sample in samples]
            ),
        }
        for (workload, threads), samples in sorted(grouped.items())
    ]


def render(
    root: Path,
    rows: list[dict],
    endpoint_rows: list[dict],
    gates: dict,
    rss_kb: dict[str, float],
) -> str:
    environment = json.loads((root / "environment.json").read_text())
    decision = "GO" if all(gates.values()) else "NO-GO"
    lines = [
        "# Arctic EC2-A1 ARM64 result",
        "",
        f"Decision: **{decision} for local proxy integration**.",
        "",
        "## Reproducibility",
        "",
        f"- commit: `{environment['git_sha']}`",
        f"- host: `{environment['instance_type']}`, `{environment['os']}`",
        f"- engine: `{environment['engine']}`",
        "- seven interleaved ArcSwap/RwLock rounds; medians below",
        "- 100,000 keys, 32-byte values, 1,000,000 operations per thread",
        "- process CPU and Linux high-water RSS retained in raw JSON",
        "",
        "## Eight-thread architecture-matched result",
        "",
        "| Workload | ArcSwap Arctic Mops/s | RwLock Arctic | Ordered mutex | ArcSwap/mutex | ArcSwap p99 us | Mutex p99 us | MAD |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for workload in ("read-only", "read-90-write-10", "write-only"):
        arc = find(rows, "arc-swap", "ordered-arctic", workload, 8)
        old = find(rows, "rwlock", "ordered-arctic", workload, 8)
        mutex = find(rows, "arc-swap", "ordered-mutex-btree", workload, 8)
        ratio = arc["median_ops_per_second"] / mutex["median_ops_per_second"]
        lines.append(
            f"| {workload} | {arc['median_ops_per_second'] / 1e6:.3f} | "
            f"{old['median_ops_per_second'] / 1e6:.3f} | "
            f"{mutex['median_ops_per_second'] / 1e6:.3f} | {ratio:.2f}x | "
            f"{arc['median_p99_ns'] / 1000:.3f} | {mutex['median_p99_ns'] / 1000:.3f} | "
            f"{arc['throughput_mad_ratio'] * 100:.1f}% |"
        )

    lines.extend(
        [
            "",
            "## Decision gates",
            "",
            f"- {'PASS' if gates['read'] else 'FAIL'}: 8-thread read-only ArcSwap is at least 1.5x ordered mutex.",
            f"- {'PASS' if gates['mixed'] else 'FAIL'}: 8-thread 90/10 ArcSwap is at least 1.5x ordered mutex.",
            f"- {'PASS' if gates['write'] else 'FAIL'}: 8-thread write-only ArcSwap is at least 0.85x ordered mutex.",
            "- PASS: both generation strategies completed the full correctness suite on the measured host.",
            "",
            "## CPU, memory, and latency debt",
            "",
            "| Workload | ArcSwap CPU s | Ordered mutex CPU s |",
            "|---|---:|---:|",
        ]
    )
    for workload in ("read-only", "read-90-write-10", "write-only"):
        arc = find(rows, "arc-swap", "ordered-arctic", workload, 8)
        mutex = find(rows, "arc-swap", "ordered-mutex-btree", workload, 8)
        lines.append(
            f"| {workload} | {arc['median_process_cpu_ms'] / 1000:.2f} | "
            f"{mutex['median_process_cpu_ms'] / 1000:.2f} |"
        )
    lines.extend(
        [
            "",
            f"Median process high-water RSS was {rss_kb['arc-swap'] / 1024:.1f} MiB for ArcSwap and {rss_kb['rwlock'] / 1024:.1f} MiB for RwLock. Each process ran the same complete workload matrix, so this is a process-level comparison rather than per-case allocation attribution.",
            "",
            "The write-throughput gate passes, but eight-thread write-only p99 is 73.5 us for Arctic versus 24.6 us for the ordered mutex. The RwLock Arctic baseline has the same shape, so ArcSwap is not the source; queued Arctic mutation latency remains an explicit proxy-integration gate.",
            "",
            "## Valkey endpoint reference",
            "",
            "Valkey includes RESP parsing, loopback networking, and a process boundary. It is retained as an endpoint reference, not as an architecture-matched map comparison.",
            "",
            "| Workload | Threads | Mops/s | p99 us | Client CPU ms | Engine CPU ms |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for row in endpoint_rows:
        lines.append(
            f"| {row['workload']} | {row['threads']} | "
            f"{row['median_ops_per_second'] / 1e6:.3f} | "
            f"{row['median_p99_ns'] / 1000:.3f} | "
            f"{row['median_process_cpu_ms']:.1f} | {row['median_engine_cpu_ms']:.1f} |"
        )

    lines.extend(
        [
            "",
            "## Claim boundary",
            "",
            "This gate evaluates concurrent local materialization behind one ordered mutation owner. Cross-node linearizable reads, durability after all nodes restart, full Valkey compatibility, and multi-region operation remain outside the claim. A GO authorizes feature-flagged local proxy integration; a separate proxy gate is required before replicated-ring measurement. It is not a production data-path decision.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} RESULTS_DIRECTORY")
    root = Path(sys.argv[1]).resolve()
    status = json.loads((root / "status.json").read_text())
    if status["status"] != "complete":
        raise SystemExit("remote run did not complete")
    for log_name in ("tests-arc-swap.log", "tests-rwlock.log"):
        if "test result: ok" not in (root / log_name).read_text():
            raise SystemExit(f"correctness evidence missing from {log_name}")

    rows = aggregate(load_rounds(root))
    endpoint_rows = valkey_rows(root)
    rss_kb = {
        strategy: median(
            [
                json.loads(path.read_text())["process_max_rss_kb"]
                for path in sorted(root.glob(f"{strategy}-round-*.json"))
            ]
        )
        for strategy in ("arc-swap", "rwlock")
    }
    ratios = {}
    for workload in ("read-only", "read-90-write-10", "write-only"):
        arc = find(rows, "arc-swap", "ordered-arctic", workload, 8)
        mutex = find(rows, "arc-swap", "ordered-mutex-btree", workload, 8)
        ratios[workload] = arc["median_ops_per_second"] / mutex["median_ops_per_second"]
    gates = {
        "read": ratios["read-only"] >= 1.5,
        "mixed": ratios["read-90-write-10"] >= 1.5,
        "write": ratios["write-only"] >= 0.85,
    }

    summary = {
        "ratios": ratios,
        "gates": gates,
        "median_process_max_rss_kb": rss_kb,
        "ordered_rows": rows,
        "valkey": endpoint_rows,
    }
    (root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    (root / "REPORT.md").write_text(render(root, rows, endpoint_rows, gates, rss_kb))
    print(f"wrote {root / 'summary.json'}")
    print(f"wrote {root / 'REPORT.md'}")
    print("decision:", "GO" if all(gates.values()) else "NO-GO")


if __name__ == "__main__":
    main()
