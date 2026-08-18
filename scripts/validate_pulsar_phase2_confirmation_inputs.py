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

from plan_backend_batch import build_batch_plan
from src.benchmark.config_loader import load_benchmark_config


DEFAULT_ROOT = (
    PROJECT_ROOT / "configs" / "campaigns" / "pulsar" / "phase2" / "confirmation"
)
DEFAULT_PLAN = DEFAULT_ROOT / "confirmation_plan.json"
DEFAULT_MANIFEST = DEFAULT_ROOT / "memory_confirmation_manifest.csv"
DEFAULT_PHASE1_CONFIG_ROOT = (
    PROJECT_ROOT / "configs" / "campaigns" / "pulsar" / "phase1" / "generated_configs"
)
DEFAULT_PROFILE_ROOT = PROJECT_ROOT / "configs" / "backends" / "pulsar" / "profiles"
DEFAULT_SELECTION = (
    PROJECT_ROOT
    / "results"
    / "rebuilt"
    / "pulsar"
    / "phase2-screening"
    / "pulsar_phase2_candidate_selection.json"
)
ANCHORS = {"cfg_120", "cfg_101", "cfg_093", "cfg_089", "cfg_001"}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Pulsar Phase 2 repeated-confirmation inputs."
    )
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--selection", type=Path, default=DEFAULT_SELECTION)
    parser.add_argument("--phase1-config-root", type=Path, default=DEFAULT_PHASE1_CONFIG_ROOT)
    parser.add_argument("--profile-root", type=Path, default=DEFAULT_PROFILE_ROOT)
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
    selection = _read_json(args.selection)
    with args.manifest.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    check(
        plan.get("format") == "messaging-benchmark.pulsar-phase2-confirmation-plan.v1",
        "unexpected confirmation plan format",
    )
    check(
        plan.get("status") in {"ready_for_review", "authorized_for_submission"},
        "confirmation plan has an unsupported review state",
    )
    if args.require_authorized:
        check(plan.get("status") == "authorized_for_submission", "confirmation plan is not authorized")
        check(plan.get("submission_authorized") is True, "submission_authorized is not true")
    check(len(rows) == 30, f"expected 30 rows, found {len(rows)}")
    check(len({row.get("case_id") for row in rows}) == 30, "case IDs are not unique")
    check({row.get("block") for row in rows} == {"2", "3"}, "blocks must be 2 and 3")
    check({row.get("anchor") for row in rows} == ANCHORS, "anchor set is incorrect")
    check(
        _sha256(args.selection) == _dict(plan.get("screening_selection")).get("sha256"),
        "screening selection checksum mismatch",
    )
    expected_profiles = [str(item) for item in selection.get("confirmation_profiles", [])]
    check(
        len(expected_profiles) == 3
        and expected_profiles[0] == "BASELINE_H16_D32"
        and len(set(expected_profiles)) == 3,
        "selection must contain the baseline and two candidates",
    )
    check(
        {row.get("profile_id") for row in rows} == set(expected_profiles),
        "manifest profile set differs from the screening selection",
    )
    check(
        Counter(row.get("block") for row in rows) == {"2": 15, "3": 15},
        "each confirmation block must contain 15 cases",
    )
    check(
        set(Counter((row.get("profile_id"), row.get("anchor")) for row in rows).values()) == {2},
        "each profile/anchor cell must occur once in each of two blocks",
    )
    for block in ("2", "3"):
        block_rows = [row for row in rows if row.get("block") == block]
        check(
            sorted(int(row.get("order", "0")) for row in block_rows) == list(range(1, 16)),
            f"block {block} order is not a permutation of 1..15",
        )

    profiles: dict[str, dict[str, Any]] = {}
    for profile_id in expected_profiles:
        path = args.profile_root / f"{profile_id}.json"
        if not path.is_file():
            failures.append(f"missing profile: {path}")
            continue
        profile = _read_json(path)
        profiles[profile_id] = profile
        check(profile.get("profile_id") == profile_id, f"{profile_id}: profile ID mismatch")
        check(
            profile.get("sha256") == _canonical_sha256(_dict(profile.get("settings"))),
            f"{profile_id}: profile checksum mismatch",
        )

    for row in rows:
        config_path = Path(str(row.get("config_path", "")))
        if not config_path.is_absolute():
            config_path = project_root / config_path
        if not config_path.is_file():
            failures.append(f"missing generated config: {config_path}")
            continue
        config = load_benchmark_config(config_path)
        profile_id = str(row.get("profile_id"))
        profile = profiles.get(profile_id)
        if profile is None:
            continue
        settings = config.backend_settings
        check(config.backend_id == "pulsar", f"{row.get('case_id')}: wrong backend")
        check(config.case_id == row.get("case_id"), f"{row.get('case_id')}: case ID drift")
        check(settings.get("profile_id") == profile_id, f"{row.get('case_id')}: profile ID drift")
        check(
            settings.get("profile_sha256") == profile.get("sha256") == row.get("profile_sha256"),
            f"{row.get('case_id')}: profile checksum drift",
        )
        for key in ("service_count", "service_mode", "managed_ledger", "standalone_properties", "runtime"):
            check(
                settings.get(key) == _dict(profile.get("settings")).get(key),
                f"{row.get('case_id')}: effective profile field {key} drift",
            )
        check(config.warmup_sec == 15, f"{row.get('case_id')}: warm-up drift")
        check(config.duration_sec == 30, f"{row.get('case_id')}: measurement drift")
        check(config.drain_timeout_sec == 60, f"{row.get('case_id')}: drain drift")
        check(config.latency_enabled is True, f"{row.get('case_id')}: latency disabled")
        check(config.latency_sample_every == 10, f"{row.get('case_id')}: latency sampling drift")
        source = load_benchmark_config(
            args.phase1_config_root / f"{row.get('anchor')}.json"
        )
        check(_portable_workload(config) == _portable_workload(source), f"{row.get('case_id')}: workload drift")
        check(_client_shape(config) == _client_shape(source), f"{row.get('case_id')}: client or partition drift")

    try:
        batch_plan = build_batch_plan(
            "pulsar", args.manifest, project_root, require_one_profile=False
        )
    except Exception as exc:
        failures.append(f"batch planner rejected confirmation: {exc}")
        batch_plan = {}
    if batch_plan:
        check(batch_plan.get("case_count") == 30, "batch planner case count differs")
        check(batch_plan.get("profile_count") == 3, "batch planner profile count differs")
        check(batch_plan.get("node_count") == 4, "batch planner did not request four nodes")
        check(batch_plan.get("total_case_budget_sec") == 3150, "case-time budget differs")

    conservative_required = 30 * (105 + 60) + 180 + 900
    check(conservative_required == 6030, "internal wall-time formula changed")
    check(
        _dict(plan.get("slurm")).get("conservative_required_time_sec") == conservative_required,
        "plan wall-time estimate differs",
    )
    payload = {
        "format": "messaging-benchmark.pulsar-phase2-confirmation-input-validation.v1",
        "valid": not failures,
        "case_count": len(rows),
        "profile_count": len(expected_profiles),
        "anchor_count": len({row.get("anchor") for row in rows}),
        "block_count": len({row.get("block") for row in rows}),
        "conservative_required_time_sec": conservative_required,
        "remaining_margin_sec": 7200 - conservative_required,
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


def _canonical_sha256(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
