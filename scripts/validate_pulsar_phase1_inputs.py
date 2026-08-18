#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from plan_backend_batch import build_batch_plan  # noqa: E402


DEFAULT_PHASE1_ROOT = (
    PROJECT_ROOT / "configs" / "campaigns" / "pulsar" / "phase1"
)
DEFAULT_GATE = (
    PROJECT_ROOT
    / "results"
    / "published"
    / "pulsar"
    / "phase1-gate"
    / "acceptance_report.json"
)
VARIANT_FIELDS = (
    "producer_ranks",
    "consumer_ranks",
    "partitions",
    "payload_size_bytes",
    "batching_max_messages",
    "batching_max_bytes",
    "batching_max_publish_delay_ms",
    "max_pending_messages",
    "max_pending_messages_across_partitions",
    "receiver_queue_size",
    "max_total_receiver_queue_size_across_partitions",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate all generated Pulsar Phase 1 inputs before submission."
    )
    parser.add_argument("--phase1-root", type=Path, default=DEFAULT_PHASE1_ROOT)
    parser.add_argument("--gate-report", type=Path, default=DEFAULT_GATE)
    parser.add_argument("--wall-time-sec", type=int, default=7200)
    parser.add_argument("--case-overhead-sec", type=int, default=60)
    parser.add_argument("--startup-overhead-sec", type=int, default=180)
    parser.add_argument("--stop-margin-sec", type=int, default=900)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.phase1_root.resolve()
    plan = _read_json(root / "phase1_campaign_plan.json")
    manifest_rows = _read_csv(root / "phase1_manifest.csv")
    batch_paths = sorted((root / "batches").glob("batch_*.csv"))
    failures: list[str] = []

    _require(plan.get("configuration_count") == 120, "plan count is not 120", failures)
    _require(len(manifest_rows) == 120, "manifest count is not 120", failures)
    _require(
        len({row.get("config_id") for row in manifest_rows}) == 120,
        "manifest config IDs are not unique",
        failures,
    )
    _require(
        len({row.get("case_id") for row in manifest_rows}) == 120,
        "manifest case IDs are not unique",
        failures,
    )
    _require(
        len({tuple(row.get(name) for name in VARIANT_FIELDS) for row in manifest_rows})
        == 120,
        "the 11-field workload variants are not unique",
        failures,
    )
    _require(
        [int(row.get("order", 0)) for row in manifest_rows] == list(range(1, 121)),
        "full-manifest order is not the sequence 1..120",
        failures,
    )
    expected_design = {
        "baseline": 1,
        "one_factor": 43,
        "rank_pair": 9,
        "seeded_mixed": 67,
    }
    _require(
        plan.get("design_counts") == expected_design,
        "design counts do not match 1/43/9/67",
        failures,
    )
    _require(len(batch_paths) == 4, "expected exactly four batch manifests", failures)
    _require(
        plan.get("batch_sizes") == [30, 30, 30, 30],
        "plan batch sizes are not 30/30/30/30",
        failures,
    )
    _require(
        plan.get("timing_sec") == {"warmup": 15, "measurement": 30, "drain": 60},
        "plan timing is not 15/30/60 seconds",
        failures,
    )
    _require(plan.get("ready_for_submission") is True, "plan is not gate-authorized", failures)

    gate = _read_json(args.gate_report)
    accepted_profile = _dict(gate.get("profile"))
    _require(
        gate.get("format") == "messaging-benchmark.pulsar-phase1-gate.v1",
        "gate report format is invalid",
        failures,
    )
    _require(
        gate.get("phase1_submission_authorized") is True,
        "gate report does not authorize Phase 1",
        failures,
    )
    _require(
        plan.get("profile_id") == accepted_profile.get("profile_id"),
        "plan profile ID differs from the accepted gate",
        failures,
    )
    _require(
        plan.get("profile_sha256") == accepted_profile.get("profile_sha256"),
        "plan profile checksum differs from the accepted gate",
        failures,
    )

    flattened_ids: list[str] = []
    batch_summaries: list[dict[str, Any]] = []
    for expected_block, batch_path in enumerate(batch_paths, start=1):
        rows = _read_csv(batch_path)
        _require(len(rows) == 30, f"{batch_path.name} does not have 30 cases", failures)
        _require(
            {int(row.get("block", 0)) for row in rows} == {expected_block},
            f"{batch_path.name} has an invalid block number",
            failures,
        )
        _require(
            [int(row.get("order", 0)) for row in rows] == list(range(1, 31)),
            f"{batch_path.name} order is not 1..30",
            failures,
        )
        flattened_ids.extend(str(row.get("config_id", "")) for row in rows)
        try:
            batch_plan = build_batch_plan(
                "pulsar",
                batch_path,
                PROJECT_ROOT,
                require_one_profile=True,
            )
        except (OSError, ValueError, KeyError) as exc:
            failures.append(f"{batch_path.name} cannot be planned: {exc}")
            continue
        required_time = (
            int(batch_plan["total_case_budget_sec"])
            + int(batch_plan["case_count"]) * args.case_overhead_sec
            + args.startup_overhead_sec
            + args.stop_margin_sec
        )
        _require(
            required_time <= args.wall_time_sec,
            f"{batch_path.name} conservative budget {required_time}s exceeds wall time",
            failures,
        )
        batch_summaries.append(
            {
                "batch": expected_block,
                "manifest": batch_path.name,
                "case_count": batch_plan["case_count"],
                "max_producer_ranks": batch_plan["max_producer_ranks"],
                "max_consumer_ranks": batch_plan["max_consumer_ranks"],
                "max_total_mpi_ranks": batch_plan["max_total_mpi_ranks"],
                "conservative_required_time_sec": required_time,
                "wall_time_sec": args.wall_time_sec,
            }
        )

    _require(
        flattened_ids == [str(row.get("config_id", "")) for row in manifest_rows],
        "concatenated batch order differs from the full manifest",
        failures,
    )
    _require(
        len(set(flattened_ids)) == 120,
        "batch manifests contain duplicate or omitted configurations",
        failures,
    )

    generated_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in [
            root / "phase1_campaign_plan.json",
            root / "phase1_manifest.csv",
            *batch_paths,
            *sorted((root / "generated_configs").glob("*.json")),
        ]
    )
    _require(
        not any(token in generated_text for token in ("/home/", "/user/", "/mnt/")),
        "generated inputs contain an absolute developer or HPC path",
        failures,
    )

    summary = {
        "format": "messaging-benchmark.pulsar-phase1-input-validation.v1",
        "valid": not failures,
        "configuration_count": len(manifest_rows),
        "unique_variant_count": len(
            {tuple(row.get(name) for name in VARIANT_FIELDS) for row in manifest_rows}
        ),
        "batch_count": len(batch_paths),
        "batch_sizes": [batch["case_count"] for batch in batch_summaries],
        "profile_id": plan.get("profile_id"),
        "profile_sha256": plan.get("profile_sha256"),
        "timing_sec": plan.get("timing_sec"),
        "gate_authorized": gate.get("phase1_submission_authorized") is True,
        "batches": batch_summaries,
        "failure_reasons": failures,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if not failures else 1


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object: {path}")
    return value


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _require(condition: bool, reason: str, failures: list[str]) -> None:
    if not condition:
        failures.append(reason)


if __name__ == "__main__":
    raise SystemExit(main())
