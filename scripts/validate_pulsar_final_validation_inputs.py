#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from plan_backend_batch import build_batch_plan  # noqa: E402
from src.benchmark.config_loader import load_benchmark_config  # noqa: E402


DEFAULT_ROOT = PROJECT_ROOT / "configs" / "campaigns" / "pulsar" / "final_validation"
DEFAULT_PLAN = DEFAULT_ROOT / "final_validation_plan.json"
DEFAULT_MANIFEST = DEFAULT_ROOT / "final_validation_manifest.csv"
DEFAULT_SHORTLIST = (
    PROJECT_ROOT
    / "results"
    / "published"
    / "pulsar"
    / "phase1"
    / "pulsar_phase1_shortlist.json"
)
DEFAULT_FINAL_SELECTION = (
    PROJECT_ROOT
    / "results"
    / "published"
    / "pulsar"
    / "complete"
    / "data"
    / "phase2-confirmation"
    / "pulsar_phase2_final_selection.json"
)
DEFAULT_PHASE1_CONFIG_ROOT = (
    PROJECT_ROOT / "configs" / "campaigns" / "pulsar" / "phase1" / "generated_configs"
)
DEFAULT_PROFILE = PROJECT_ROOT / "configs" / "backends" / "pulsar" / "profiles" / "BASELINE_H16_D32.json"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate deterministic Pulsar final workload-validation inputs."
    )
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--shortlist", type=Path, default=DEFAULT_SHORTLIST)
    parser.add_argument("--final-selection", type=Path, default=DEFAULT_FINAL_SELECTION)
    parser.add_argument("--phase1-config-root", type=Path, default=DEFAULT_PHASE1_CONFIG_ROOT)
    parser.add_argument("--profile", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--require-authorized", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = args.project_root.resolve()
    failures: list[str] = []

    def check(condition: bool, message: str) -> None:
        if not condition:
            failures.append(message)

    plan = _read_json(args.plan)
    shortlist = _read_json(args.shortlist)
    final_selection = _read_json(args.final_selection)
    profile = _read_json(args.profile)
    rows = _read_csv(args.manifest)
    expected_ids = [
        str(item)
        for item in shortlist.get(
            "historically_executed_final_validation_ids", []
        )
    ]

    check(
        plan.get("format") == "messaging-benchmark.pulsar-final-validation-plan.v1",
        "unexpected final-validation plan format",
    )
    check(
        plan.get("status") in {"ready_for_review", "authorized_for_submission"},
        "unsupported final-validation plan status",
    )
    if args.require_authorized:
        check(plan.get("status") == "authorized_for_submission", "plan is not authorized")
        check(plan.get("submission_authorized") is True, "submission_authorized is not true")
    check(len(rows) == 50, f"expected 50 rows, found {len(rows)}")
    check(len({row.get("case_id") for row in rows}) == 50, "case IDs are not unique")
    check(set(row.get("block") for row in rows) == {"1", "2", "3", "4", "5"}, "block set is incorrect")
    check(len(expected_ids) == 10 and len(set(expected_ids)) == 10, "historical workload set is invalid")
    check(set(row.get("config_id") for row in rows) == set(expected_ids), "configuration set differs from historical execution set")
    check(Counter(row.get("config_id") for row in rows) == Counter({item: 5 for item in expected_ids}), "each historical configuration must occur five times")
    check(Counter(row.get("block") for row in rows) == Counter({str(block): 10 for block in range(1, 6)}), "each block must contain ten cases")
    check({row.get("profile_id") for row in rows} == {"BASELINE_H16_D32"}, "manifest does not use one frozen profile")
    for block in range(1, 6):
        block_rows = [row for row in rows if row.get("block") == str(block)]
        check(
            sorted(int(row.get("order", "0")) for row in block_rows) == list(range(1, 11)),
            f"block {block} order is not a permutation of 1..10",
        )
        check(
            {row.get("config_id") for row in block_rows} == set(expected_ids),
            f"block {block} does not contain each configuration exactly once",
        )

    check(final_selection.get("status") == "profile_frozen", "Phase 2 profile is not frozen")
    check(final_selection.get("frozen_profile_id") == "BASELINE_H16_D32", "frozen profile changed")
    check(plan.get("profile_id") == "BASELINE_H16_D32", "plan profile ID changed")
    check(
        plan.get("workload_set_status")
        == "historically_executed_pre_correction_shortlist",
        "plan does not identify the historical pre-correction workload set",
    )
    check(profile.get("profile_id") == "BASELINE_H16_D32", "profile file ID changed")
    check(profile.get("sha256") == _canonical_sha256(_dict(profile.get("settings"))), "profile checksum mismatch")
    check(plan.get("profile_sha256") == profile.get("sha256"), "plan profile checksum mismatch")
    source = _dict(plan.get("source_evidence"))
    check(source.get("phase1_shortlist_sha256") == _sha256(args.shortlist), "shortlist checksum mismatch")
    check(source.get("phase2_final_selection_sha256") == _sha256(args.final_selection), "final-selection checksum mismatch")
    check(plan.get("manifest_sha256") == _sha256(args.manifest), "canonical manifest checksum mismatch")

    batch_rows: list[list[dict[str, str]]] = []
    for expected_batch, expected_blocks, expected_count in (
        (1, {"1", "2", "3"}, 30),
        (2, {"4", "5"}, 20),
    ):
        batch_path = args.plan.parent / "batches" / f"batch_{expected_batch:02d}.csv"
        selected = _read_csv(batch_path)
        batch_rows.append(selected)
        check(len(selected) == expected_count, f"batch {expected_batch} case count differs")
        check({row.get("block") for row in selected} == expected_blocks, f"batch {expected_batch} block set differs")
        try:
            batch_plan = build_batch_plan("pulsar", batch_path, project_root)
        except Exception as exc:
            failures.append(f"batch planner rejected batch {expected_batch}: {exc}")
        else:
            check(batch_plan.get("case_count") == expected_count, f"batch {expected_batch} planner count differs")
            check(batch_plan.get("profile_count") == 1, f"batch {expected_batch} has multiple profiles")
            check(batch_plan.get("node_count") == 4, f"batch {expected_batch} topology differs")
            check(batch_plan.get("total_case_budget_sec") == expected_count * 105, f"batch {expected_batch} timing budget differs")
    check(
        [row.get("case_id") for selected in batch_rows for row in selected]
        == [row.get("case_id") for row in rows],
        "batch manifests do not reproduce canonical manifest order",
    )

    profile_settings = _dict(profile.get("settings"))
    for row in rows:
        case_id = str(row.get("case_id"))
        config_path = Path(str(row.get("config_path", "")))
        if not config_path.is_absolute():
            config_path = project_root / config_path
        if not config_path.is_file():
            failures.append(f"missing generated config: {config_path}")
            continue
        config = load_benchmark_config(config_path)
        settings = config.backend_settings
        check(config.backend_id == "pulsar", f"{case_id}: wrong backend")
        check(config.case_id == case_id, f"{case_id}: case ID drift")
        check(settings.get("profile_id") == "BASELINE_H16_D32", f"{case_id}: profile ID drift")
        check(settings.get("profile_sha256") == profile.get("sha256"), f"{case_id}: profile checksum drift")
        for key in ("service_count", "service_mode", "managed_ledger", "standalone_properties", "runtime"):
            check(settings.get(key) == profile_settings.get(key), f"{case_id}: profile field {key} drift")
        check(config.warmup_sec == 15, f"{case_id}: warm-up drift")
        check(config.duration_sec == 30, f"{case_id}: measurement drift")
        check(config.drain_timeout_sec == 60, f"{case_id}: drain drift")
        check(config.latency_enabled is True, f"{case_id}: latency disabled")
        check(config.latency_sample_every == 10, f"{case_id}: latency sampling drift")
        source_config = load_benchmark_config(args.phase1_config_root / f"{row.get('config_id')}.json")
        check(_portable_workload(config) == _portable_workload(source_config), f"{case_id}: workload drift")
        check(_client_shape(config) == _client_shape(source_config), f"{case_id}: client or partition drift")

    for batch in _list(plan.get("batches")):
        count = int(batch.get("case_count", 0))
        expected_required = count * (105 + 60) + 180 + 900
        check(batch.get("conservative_required_time_sec") == expected_required, f"batch {batch.get('batch')} wall-time estimate differs")
        check(expected_required <= 7200, f"batch {batch.get('batch')} exceeds two-hour wall time")

    payload = {
        "format": "messaging-benchmark.pulsar-final-validation-input-validation.v1",
        "valid": not failures,
        "case_count": len(rows),
        "configuration_count": len(expected_ids),
        "block_count": len({row.get("block") for row in rows}),
        "batch_case_counts": [len(selected) for selected in batch_rows],
        "profile_id": plan.get("profile_id"),
        "submission_authorized": plan.get("submission_authorized") is True,
        "failures": list(dict.fromkeys(failures)),
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if not failures else 1


def _portable_workload(config: Any) -> dict[str, Any]:
    return {
        name: getattr(config, name)
        for name in (
            "scenario",
            "producer_ranks",
            "consumer_ranks",
            "payload_size_bytes",
            "payload_mode",
            "send_pattern",
            "virtual_devices_per_rank",
            "total_simulated_devices",
            "duration_sec",
            "warmup_sec",
            "drain_timeout_sec",
            "target_records_per_sec",
            "latency_enabled",
            "latency_sample_every",
            "latency_clock_samples",
            "latency_clock_max_uncertainty_us",
            "latency_clock_max_drift_us",
        )
    }


def _client_shape(config: Any) -> dict[str, Any]:
    settings = config.backend_settings
    return {
        "partitions": settings.get("partitions"),
        "subscription_type": settings.get("subscription_type"),
        "producer": settings.get("producer"),
        "consumer": settings.get("consumer"),
    }


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


if __name__ == "__main__":
    raise SystemExit(main())
