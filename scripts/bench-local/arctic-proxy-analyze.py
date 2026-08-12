#!/usr/bin/env python3
"""Validate and summarize the local Arctic proxy endpoint benchmark."""

from __future__ import annotations

import json
import statistics
import sys
from collections import Counter
from pathlib import Path


TARGETS = ("mutex-proxy", "arctic-proxy", "valkey")
WORKLOADS = ("read-only", "read-90-write-10", "write-only")


def median(rows: list[dict], field: str) -> float:
    return float(statistics.median(row[field] for row in rows))


def main() -> int:
    if len(sys.argv) != 6:
        raise SystemExit(
            "usage: arctic-proxy-analyze.py "
            "<raw.json> <telemetry.jsonl> <summary.json> <REPORT.md> "
            "<smoke|qualification>"
        )
    raw_path, telemetry_path, summary_path, report_path = map(Path, sys.argv[1:5])
    mode = sys.argv[5]
    if mode not in {"smoke", "qualification"}:
        raise SystemExit(f"invalid mode: {mode}")

    raw = json.loads(raw_path.read_text())
    rows = raw["results"]
    config = raw["config"]
    expected_cases = (
        len(config["targets"])
        * len(config["workloads"])
        * len(config["client_counts"])
        * config["repetitions"]
    )
    errors: list[str] = []
    if len(rows) != expected_cases:
        errors.append(f"expected {expected_cases} cases, found {len(rows)}")
    ids = [row["case_id"] for row in rows]
    if len(set(ids)) != len(ids):
        errors.append("case ids are not unique")

    expected_latency_samples = {
        clients: clients
        * ((config["operations_per_client"] + config["latency_sample_every"] - 1)
           // config["latency_sample_every"])
        for clients in config["client_counts"]
    }
    expected_validated = min(config["keys"], 64)
    for row in rows:
        if row["total_operations"] <= 0 or row["operations_per_second"] <= 0:
            errors.append(f"{row['case_id']}: non-positive operation count or throughput")
        if row["latency_samples"] != expected_latency_samples[row["clients"]]:
            errors.append(f"{row['case_id']}: incomplete latency sample set")
        if row["validated_keys"] != expected_validated:
            errors.append(f"{row['case_id']}: final-value validation incomplete")
        if row["target_peak_rss_kb"] <= 0:
            errors.append(f"{row['case_id']}: target RSS telemetry missing")

    telemetry = [json.loads(line) for line in telemetry_path.read_text().splitlines() if line]
    telemetry_counts = Counter(sample["case_id"] for sample in telemetry)
    for case_id in ids:
        if telemetry_counts[case_id] == 0:
            errors.append(f"{case_id}: no process telemetry samples")

    medians: list[dict] = []
    for workload in config["workloads"]:
        for clients in config["client_counts"]:
            grouped: dict[str, dict] = {}
            for target in config["targets"]:
                selected = [
                    row
                    for row in rows
                    if row["target"] == target
                    and row["workload"] == workload
                    and row["clients"] == clients
                ]
                if len(selected) != config["repetitions"]:
                    errors.append(
                        f"{target}/{workload}/{clients}: expected "
                        f"{config['repetitions']} repetitions, found {len(selected)}"
                    )
                    continue
                grouped[target] = {
                    "throughput_ops_s": median(selected, "operations_per_second"),
                    "p99_us": median(selected, "latency_p99_us"),
                    "target_cpu_ms": median(selected, "target_cpu_ms"),
                    "target_peak_rss_kb": median(selected, "target_peak_rss_kb"),
                }
            if "mutex-proxy" in grouped and "arctic-proxy" in grouped:
                grouped["arctic_vs_mutex"] = {
                    "throughput_ratio": grouped["arctic-proxy"]["throughput_ops_s"]
                    / grouped["mutex-proxy"]["throughput_ops_s"],
                    "p99_ratio": grouped["arctic-proxy"]["p99_us"]
                    / grouped["mutex-proxy"]["p99_us"],
                }
            medians.append(
                {"workload": workload, "clients": clients, "targets": grouped}
            )

    eight_client = {
        item["workload"]: item
        for item in medians
        if item["clients"] == 8 and "arctic_vs_mutex" in item["targets"]
    }
    gates = {
        "correctness_and_evidence": not errors,
        "read_only_throughput_1_5x": eight_client.get("read-only", {})
        .get("targets", {})
        .get("arctic_vs_mutex", {})
        .get("throughput_ratio", 0.0)
        >= 1.5,
        "mixed_throughput_1_5x": eight_client.get("read-90-write-10", {})
        .get("targets", {})
        .get("arctic_vs_mutex", {})
        .get("throughput_ratio", 0.0)
        >= 1.5,
        "write_p99_below_3x": eight_client.get("write-only", {})
        .get("targets", {})
        .get("arctic_vs_mutex", {})
        .get("p99_ratio", float("inf"))
        < 3.0,
        "write_throughput_within_15pct": eight_client.get("write-only", {})
        .get("targets", {})
        .get("arctic_vs_mutex", {})
        .get("throughput_ratio", 0.0)
        >= 0.85,
    }
    status = "PASS" if not errors else "INVALID"
    if mode == "qualification":
        status = "GO" if all(gates.values()) else "NO-GO"

    summary = {
        "mode": mode,
        "status": status,
        "errors": errors,
        "gates": gates,
        "medians": medians,
    }
    summary_path.write_text(json.dumps(summary, indent=2) + "\n")

    rounds_dir = report_path.parent / "rounds"
    rounds_dir.mkdir(exist_ok=True)
    for row in rows:
        (rounds_dir / f"{row['case_id']}.json").write_text(
            json.dumps(row, indent=2) + "\n"
        )

    lines = [
        f"# Arctic proxy local {mode} report",
        "",
        f"**Decision: {status}.**",
        "",
        "All targets use the same deterministic persistent-connection RESP driver. "
        "The decision comparison is Arctic proxy versus mutex proxy; Valkey is an "
        "endpoint reference.",
        "",
        "| Workload | Clients | Mutex ops/s | Arctic ops/s | A/M | "
        "Mutex p99 us | Arctic p99 us | p99 ratio | Valkey ops/s |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in medians:
        targets = item["targets"]
        if not all(target in targets for target in TARGETS) or "arctic_vs_mutex" not in targets:
            continue
        mutex = targets["mutex-proxy"]
        arctic = targets["arctic-proxy"]
        valkey = targets["valkey"]
        ratio = targets["arctic_vs_mutex"]
        lines.append(
            f"| {item['workload']} | {item['clients']} | "
            f"{mutex['throughput_ops_s']:.0f} | {arctic['throughput_ops_s']:.0f} | "
            f"{ratio['throughput_ratio']:.2f}x | {mutex['p99_us']:.3f} | "
            f"{arctic['p99_us']:.3f} | {ratio['p99_ratio']:.2f}x | "
            f"{valkey['throughput_ops_s']:.0f} |"
        )
    lines.extend(["", "## Gates", ""])
    for name, passed in gates.items():
        lines.append(f"- {'PASS' if passed else 'FAIL'}: `{name}`")
    if errors:
        lines.extend(["", "## Evidence errors", ""])
        lines.extend(f"- {error}" for error in errors)
    lines.extend(
        [
            "",
            "Cross-node linearizable reads, all-node restart durability, full Valkey "
            "compatibility, and multi-region operation remain outside scope.",
            "",
        ]
    )
    report_path.write_text("\n".join(lines))
    print(f"{status}: wrote {summary_path} and {report_path}")
    return 1 if status == "INVALID" else 0


if __name__ == "__main__":
    raise SystemExit(main())
