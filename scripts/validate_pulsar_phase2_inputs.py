#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
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


DEFAULT_PLAN = (
    PROJECT_ROOT
    / "configs"
    / "campaigns"
    / "pulsar"
    / "phase2"
    / "phase2_campaign_plan.json"
)
DEFAULT_MANIFEST = DEFAULT_PLAN.parent / "memory_screening_manifest.csv"
DEFAULT_PHASE1_CONFIG_ROOT = (
    PROJECT_ROOT
    / "configs"
    / "campaigns"
    / "pulsar"
    / "phase1"
    / "generated_configs"
)
DEFAULT_PROFILE_ROOT = (
    PROJECT_ROOT / "configs" / "backends" / "pulsar" / "profiles"
)

PROFILE_GRID = {
    ("BASELINE_H16_D32", 16, 32),
    ("PHASE2_HEAP24_DIRECT32", 24, 32),
    ("PHASE2_HEAP32_DIRECT32", 32, 32),
    ("PHASE2_HEAP16_DIRECT48", 16, 48),
    ("PHASE2_HEAP24_DIRECT48", 24, 48),
    ("PHASE2_HEAP32_DIRECT48", 32, 48),
}
ANCHORS = {"cfg_120", "cfg_101", "cfg_093", "cfg_089", "cfg_001"}
MEMORY_RE = re.compile(
    r"^-Xms(?P<heap_min>[0-9]+)g -Xmx(?P<heap_max>[0-9]+)g "
    r"-XX:MaxDirectMemorySize=(?P<direct>[0-9]+)g$"
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Pulsar Phase 2 memory-screening inputs."
    )
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument(
        "--phase1-config-root", type=Path, default=DEFAULT_PHASE1_CONFIG_ROOT
    )
    parser.add_argument("--profile-root", type=Path, default=DEFAULT_PROFILE_ROOT)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument(
        "--require-authorized",
        action="store_true",
        help="Require the reviewed plan to authorize the one screening job.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = args.project_root.resolve()
    failures: list[str] = []

    def check(condition: bool, message: str) -> None:
        if not condition:
            failures.append(message)

    plan = _read_json(args.plan)
    with args.manifest.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    check(plan.get("format") == "messaging-benchmark.pulsar-phase2-plan.v2", "unexpected plan format")
    check(
        plan.get("status") in {"ready_for_review", "authorized_for_submission"},
        "plan has an unsupported review state",
    )
    if args.require_authorized:
        check(
            plan.get("status") == "authorized_for_submission",
            "plan has not been authorized for submission",
        )
        check(
            plan.get("submission_authorized") is True,
            "submission_authorized is not true",
        )
    check(len(rows) == 30, f"expected 30 manifest rows, found {len(rows)}")
    check(len({row.get("case_id") for row in rows}) == 30, "case IDs are not unique")
    check(len({row.get("config_path") for row in rows}) == 30, "config paths are not unique")
    check({row.get("anchor") for row in rows} == ANCHORS, "anchor set does not match the Phase 1 design")

    cell_counts = Counter(
        (row.get("profile_id"), row.get("anchor")) for row in rows
    )
    check(len(cell_counts) == 30, "profile/anchor cells are duplicated or missing")
    check(set(cell_counts.values()) == {1}, "each profile/anchor cell must appear once")
    profile_counts = Counter(row.get("profile_id") for row in rows)
    check(set(profile_counts.values()) == {5}, "each profile must contain all five anchors")
    anchor_counts = Counter(row.get("anchor") for row in rows)
    check(set(anchor_counts.values()) == {6}, "each anchor must contain all six profiles")
    orders = sorted(int(row.get("order", "0")) for row in rows)
    check(orders == list(range(1, 31)), "manifest order must be a permutation of 1..30")

    observed_grid: set[tuple[str, int, int]] = set()
    profiles: dict[str, dict[str, Any]] = {}
    baseline_settings: dict[str, Any] | None = None
    for profile_id, expected_heap, expected_direct in sorted(PROFILE_GRID):
        path = args.profile_root / f"{profile_id}.json"
        if not path.is_file():
            failures.append(f"missing profile: {path}")
            continue
        profile = _read_json(path)
        profiles[profile_id] = profile
        settings = _dict(profile.get("settings"))
        digest = canonical_sha256(settings)
        check(profile.get("profile_id") == profile_id, f"{profile_id}: profile ID mismatch")
        check(profile.get("sha256") == digest, f"{profile_id}: checksum mismatch")
        runtime = _dict(settings.get("runtime"))
        match = MEMORY_RE.fullmatch(str(runtime.get("jvm_memory", "")))
        if match is None:
            failures.append(f"{profile_id}: unsupported JVM memory string")
            continue
        heap_min = int(match.group("heap_min"))
        heap_max = int(match.group("heap_max"))
        direct = int(match.group("direct"))
        check(heap_min == heap_max, f"{profile_id}: initial and maximum heap differ")
        observed_grid.add((profile_id, heap_max, direct))
        check(heap_max == expected_heap, f"{profile_id}: unexpected heap")
        check(direct == expected_direct, f"{profile_id}: unexpected direct-memory limit")
        if profile_id == "BASELINE_H16_D32":
            baseline_settings = settings

    check(observed_grid == PROFILE_GRID, "profile memory grid is incomplete")
    if baseline_settings is not None:
        for profile_id, settings_profile in profiles.items():
            settings = _dict(settings_profile.get("settings"))
            for key in (
                "service_count",
                "service_mode",
                "managed_ledger",
                "standalone_properties",
            ):
                check(
                    settings.get(key) == baseline_settings.get(key),
                    f"{profile_id}: non-memory profile field {key} drifted",
                )
            runtime = _dict(settings.get("runtime"))
            baseline_runtime = _dict(baseline_settings.get("runtime"))
            for key, value in baseline_runtime.items():
                if key != "jvm_memory":
                    check(
                        runtime.get(key) == value,
                        f"{profile_id}: runtime field {key} drifted",
                    )

    for row in rows:
        config_path = Path(str(row.get("config_path", "")))
        if not config_path.is_absolute():
            config_path = project_root / config_path
        if not config_path.is_file():
            failures.append(f"missing generated config: {config_path}")
            continue
        config = load_benchmark_config(config_path)
        settings = config.backend_settings
        profile_id = str(row.get("profile_id"))
        profile = profiles.get(profile_id)
        if profile is None:
            failures.append(f"{row.get('case_id')}: unknown profile {profile_id}")
            continue
        check(config.backend_id == "pulsar", f"{row.get('case_id')}: wrong backend")
        check(config.case_id == row.get("case_id"), f"{row.get('case_id')}: case identity drift")
        check(settings.get("profile_id") == profile_id, f"{row.get('case_id')}: profile ID drift")
        check(settings.get("profile_sha256") == profile.get("sha256"), f"{row.get('case_id')}: profile checksum drift")
        for key in (
            "service_count",
            "service_mode",
            "managed_ledger",
            "standalone_properties",
            "runtime",
        ):
            check(
                settings.get(key) == _dict(profile.get("settings")).get(key),
                f"{row.get('case_id')}: effective profile field {key} drift",
            )
        check(config.warmup_sec == 15, f"{row.get('case_id')}: warm-up drift")
        check(config.duration_sec == 30, f"{row.get('case_id')}: measurement drift")
        check(config.drain_timeout_sec == 60, f"{row.get('case_id')}: drain drift")
        check(config.latency_enabled is True, f"{row.get('case_id')}: latency disabled")
        check(config.latency_sample_every == 10, f"{row.get('case_id')}: latency sampling drift")

        anchor_id = str(row.get("anchor"))
        source = load_benchmark_config(args.phase1_config_root / f"{anchor_id}.json")
        check(_portable_workload(config) == _portable_workload(source), f"{row.get('case_id')}: Phase 1 workload changed")
        check(_client_and_topic_shape(config) == _client_and_topic_shape(source), f"{row.get('case_id')}: Phase 1 client or partition settings changed")

    try:
        batch_plan = build_batch_plan(
            "pulsar",
            args.manifest,
            project_root,
            require_one_profile=False,
        )
    except Exception as exc:  # surfaced in the machine-readable report
        failures.append(f"batch planner rejected the manifest: {exc}")
        batch_plan = {}
    if batch_plan:
        check(batch_plan.get("case_count") == 30, "batch planner did not retain 30 cases")
        check(batch_plan.get("profile_count") == 6, "batch planner did not retain six profiles")
        check(batch_plan.get("node_count") == 4, "batch planner did not request four nodes")
        check(batch_plan.get("total_case_budget_sec") == 3150, "unexpected case-time budget")

    conservative_required_sec = 30 * (105 + 60) + 180 + 900
    check(conservative_required_sec == 6030, "internal wall-time formula changed")
    check(conservative_required_sec <= 7200, "screen does not fit the two-hour allocation")
    check(
        _dict(plan.get("slurm")).get("conservative_required_time_sec")
        == conservative_required_sec,
        "campaign plan wall-time estimate is inconsistent",
    )

    payload = {
        "format": "messaging-benchmark.pulsar-phase2-input-validation.v1",
        "valid": not failures,
        "case_count": len(rows),
        "profile_count": len(profile_counts),
        "anchor_count": len(anchor_counts),
        "profile_anchor_cell_count": len(cell_counts),
        "conservative_required_time_sec": conservative_required_sec,
        "wall_time_sec": 7200,
        "remaining_margin_sec": 7200 - conservative_required_sec,
        "profile_changes_require_explicit_opt_in": True,
        "submission_authorized": plan.get("submission_authorized") is True,
        "failures": failures,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if not failures else 1


def canonical_sha256(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


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


def _client_and_topic_shape(config: Any) -> dict[str, Any]:
    settings = config.backend_settings
    return {
        "partitions": settings.get("partitions"),
        "subscription_type": settings.get("subscription_type"),
        "producer": settings.get("producer"),
        "consumer": settings.get("consumer"),
    }


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
