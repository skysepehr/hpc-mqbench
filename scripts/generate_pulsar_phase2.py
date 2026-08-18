#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import random
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from models.benchmark_config import BenchmarkConfig, benchmark_config_input_dict
from src.benchmark.backends import get_backend
from src.benchmark.core.config_schema import normalize_case_config


DEFAULT_PHASE1_CONFIG_ROOT = (
    PROJECT_ROOT / "configs" / "campaigns" / "pulsar" / "phase1" / "generated_configs"
)
DEFAULT_SHORTLIST = (
    PROJECT_ROOT / "results" / "published" / "pulsar" / "phase1" / "pulsar_phase1_shortlist.json"
)
DEFAULT_SUMMARY = (
    PROJECT_ROOT / "results" / "published" / "pulsar" / "phase1" / "pulsar_phase1_summary.json"
)
DEFAULT_PHASE1_CASES = DEFAULT_SHORTLIST.with_name("pulsar_phase1_cases.json")
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "configs" / "campaigns" / "pulsar" / "phase2"
DEFAULT_PROFILE_ROOT = PROJECT_ROOT / "configs" / "backends" / "pulsar" / "profiles"

ANCHORS = (
    ("cfg_120", "historical Phase 1 byte-throughput leader"),
    ("cfg_101", "historical Phase 1 record-rate leader"),
    ("cfg_093", "historical Phase 1 p99-latency leader"),
    ("cfg_089", "historical pre-correction qualification-boundary reference"),
    ("cfg_001", "manifest baseline"),
)

# A 3 x 2 factorial isolates heap, direct-memory, and interaction effects.
MEMORY_PROFILES = (
    ("BASELINE_H16_D32", 16, 32),
    ("PHASE2_HEAP24_DIRECT32", 24, 32),
    ("PHASE2_HEAP32_DIRECT32", 32, 32),
    ("PHASE2_HEAP16_DIRECT48", 16, 48),
    ("PHASE2_HEAP24_DIRECT48", 24, 48),
    ("PHASE2_HEAP32_DIRECT48", 32, 48),
)

MANIFEST_FIELDS = (
    "block",
    "order",
    "stage",
    "case_id",
    "config_id",
    "config_path",
    "profile_id",
    "profile_sha256",
    "anchor",
    "anchor_purpose",
    "heap_gib",
    "direct_memory_gib",
    "producer_ranks",
    "consumer_ranks",
    "partitions",
    "payload_size_bytes",
    "warmup_sec",
    "duration_sec",
    "drain_timeout_sec",
    "latency_sample_every",
    "design_note",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate the first Pulsar Phase 2 memory-profile screen without "
            "submitting a Slurm job."
        )
    )
    parser.add_argument("--phase1-config-root", type=Path, default=DEFAULT_PHASE1_CONFIG_ROOT)
    parser.add_argument("--shortlist", type=Path, default=DEFAULT_SHORTLIST)
    parser.add_argument("--phase1-summary", type=Path, default=DEFAULT_SUMMARY)
    parser.add_argument("--phase1-cases", type=Path, default=DEFAULT_PHASE1_CASES)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--profile-root", type=Path, default=DEFAULT_PROFILE_ROOT)
    parser.add_argument("--seed", type=int, default=20260810)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    phase1_config_root = args.phase1_config_root.resolve()
    output_dir = args.output_dir.resolve()
    profile_root = args.profile_root.resolve()
    shortlist_path = args.shortlist.resolve()
    summary_path = args.phase1_summary.resolve()
    cases_path = args.phase1_cases.resolve()

    shortlist = _read_json(shortlist_path)
    summary = _read_json(summary_path)
    phase1_cases = _read_json(cases_path)
    _validate_phase1_evidence(shortlist, summary, phase1_cases)

    profile_root.mkdir(parents=True, exist_ok=True)
    profiles = _write_memory_profiles(profile_root)
    generated_dir = output_dir / "generated_configs"
    generated_dir.mkdir(parents=True, exist_ok=True)

    phase1_by_id = {
        str(item["config_id"]): item
        for item in _list(phase1_cases, "cases")
    }
    rows: list[dict[str, Any]] = []
    expected_names: set[str] = set()
    for profile_id, heap_gib, direct_gib in MEMORY_PROFILES:
        profile = profiles[profile_id]
        for anchor_id, anchor_purpose in ANCHORS:
            source = _read_json(phase1_config_root / f"{anchor_id}.json")
            case_id = (
                "pulsar-p2m-"
                f"{anchor_id.replace('_', '')}-h{heap_gib}-d{direct_gib}"
            )
            config = _build_case(
                source=source,
                profile=profile,
                case_id=case_id,
                anchor_id=anchor_id,
                anchor_purpose=anchor_purpose,
                phase1_row=_historical_anchor_row(
                    phase1_by_id[anchor_id], anchor_purpose
                ),
                seed=args.seed,
            )
            path = generated_dir / f"{case_id}.json"
            _write_case(path, config)
            expected_names.add(path.name)
            workload = _object(config, "workload")
            pulsar = _object(_object(config, "backend"), "pulsar")
            rows.append(
                {
                    "block": 1,
                    "order": 0,
                    "stage": "phase2-memory-screening",
                    "case_id": case_id,
                    "config_id": anchor_id,
                    "config_path": _project_relative(path),
                    "profile_id": profile_id,
                    "profile_sha256": profile["sha256"],
                    "anchor": anchor_id,
                    "anchor_purpose": anchor_purpose,
                    "heap_gib": heap_gib,
                    "direct_memory_gib": direct_gib,
                    "producer_ranks": workload["producer_ranks"],
                    "consumer_ranks": workload["consumer_ranks"],
                    "partitions": pulsar["partitions"],
                    "payload_size_bytes": workload["payload_size_bytes"],
                    "warmup_sec": workload["warmup_sec"],
                    "duration_sec": workload["duration_sec"],
                    "drain_timeout_sec": workload["drain_timeout_sec"],
                    "latency_sample_every": workload["latency_sample_every"],
                    "design_note": "3x2 heap/direct-memory factorial cell",
                }
            )

    random.Random(args.seed).shuffle(rows)
    for order, row in enumerate(rows, start=1):
        row["order"] = order
    manifest_path = output_dir / "memory_screening_manifest.csv"
    _write_manifest(manifest_path, rows)
    _reject_stale_generated_configs(generated_dir, expected_names)

    phase1_anchor_evidence = []
    for anchor_id, purpose in ANCHORS:
        source = phase1_by_id[anchor_id]
        phase1_anchor_evidence.append(
            {
                "config_id": anchor_id,
                "purpose": purpose,
                "phase1_qualified": bool(source["qualified"]),
                "phase1_balanced_mib_per_sec": source["balanced_mib_per_sec"],
                "phase1_balanced_records_per_sec": source[
                    "balanced_records_per_sec"
                ],
                "phase1_latency_p99_us": source["latency_p99_us"],
            }
        )

    case_budget_sec = 15 + 30 + 60
    conservative_required_sec = len(rows) * (case_budget_sec + 60) + 180 + 15 * 60
    plan = {
        "format": "messaging-benchmark.pulsar-phase2-plan.v2",
        "status": "ready_for_review",
        "ready_for_submission": True,
        "submission_authorized": False,
        "seed": args.seed,
        "campaign_id": "pulsar-phase2-memory-screening",
        "stage": "memory-profile-screening",
        "manifest": _project_relative(manifest_path),
        "configuration_count": len(rows),
        "profile_count": len(MEMORY_PROFILES),
        "anchor_count": len(ANCHORS),
        "observations_per_profile_anchor_cell": 1,
        "workload_set_status": "historically_executed_pre_correction_anchor_set",
        "design": "3 heap sizes x 2 direct-memory limits x 5 fixed workloads",
        "profiles": [
            {
                "profile_id": profile_id,
                "profile_sha256": profiles[profile_id]["sha256"],
                "heap_gib": heap_gib,
                "direct_memory_gib": direct_gib,
                "control": profile_id == "BASELINE_H16_D32",
            }
            for profile_id, heap_gib, direct_gib in MEMORY_PROFILES
        ],
        "anchors": phase1_anchor_evidence,
        "phase1_evidence": {
            "summary": _project_relative(summary_path),
            "summary_sha256": _sha256_file(summary_path),
            "shortlist": _project_relative(shortlist_path),
            "shortlist_sha256": _sha256_file(shortlist_path),
            "phase1_cases": _project_relative(cases_path),
            "phase1_cases_sha256": _sha256_file(cases_path),
            "completed_cases": summary["case_count"],
            "eligible_cases": summary["eligible_case_count"],
            "qualified_cases": summary["qualified_case_count"],
            "maximum_observed_heap_gib": 14.560546875,
            "maximum_observed_gc_time_rate_seconds_per_second": 0.2775944533333334,
            "maximum_observed_process_rss_gib": 34.742698669433594,
            "direct_memory_observation": (
                "Total Pulsar direct-memory usage was unavailable in Phase 1; "
                "the earlier 8-GiB pilot exhausted direct memory, while all "
                "120 Phase 1 cases completed with a 32-GiB limit."
            ),
            "selection_note": (
                "The five Phase 2 anchors are the immutable historical execution "
                "set. They differ from the retrospectively corrected Phase 1 "
                "shortlist, and newly shortlisted configurations were not run."
            ),
        },
        "fixed_during_screening": [
            "the complete Phase 1 workload and client settings within each anchor",
            "Pulsar 5.0.0-M1 and Pulsar Python client 3.13.0",
            "Java 21 and the default Pulsar garbage collector",
            "one standalone Pulsar service with one embedded BookKeeper bookie",
            "managed-ledger ensemble/write/ack quorum 1/1/1",
            "RAM-backed per-case storage and exclusive four-node placement",
            "qualification, correctness, latency, and eligibility rules",
            "15-second warm-up, 30-second measurement, and 60-second drain",
        ],
        "varied_during_screening": [
            "JVM initial and maximum heap: 16, 24, or 32 GiB",
            "JVM maximum direct memory: 32 or 48 GiB",
        ],
        "screening_interpretation": (
            "Each profile/anchor cell has one observation. This stage can reject "
            "invalid profiles and nominate candidates, but it cannot freeze a winner."
        ),
        "candidate_order": [
            "required reports, backend health, latency, and record accounting valid",
            "eligible-case count",
            "qualified count across cfg_120, cfg_101, cfg_093, and cfg_001",
            "geometric mean balanced-MiB/s ratio to BASELINE_H16_D32 on those anchors",
            "p99-latency ratio to BASELINE_H16_D32",
            "flush time, failed sends, GC, RSS, and tmpfs headroom",
            "qualification-boundary behavior on cfg_089 as supporting evidence",
        ],
        "next_gate": {
            "status": "blocked_pending_memory_screening_results",
            "confirmation_design": (
                "baseline plus two candidates, five anchors, two additional "
                "randomized blocks: at most 30 cases in one job"
            ),
            "winner_rule": (
                "No profile is frozen until each confirmed profile has three "
                "observations per anchor and passes correctness, qualification, "
                "throughput, latency, memory, GC, and tmpfs checks."
            ),
            "later_broker_tuning": (
                "Thread and cache settings remain fixed in this memory screen. "
                "They may be varied around the confirmed memory region only when "
                "the screening evidence identifies a relevant bottleneck."
            ),
        },
        "slurm": {
            "job_count": 1,
            "exclusive_nodes": 4,
            "wall_time_sec": 7200,
            "case_budget_sec": case_budget_sec,
            "per_case_overhead_sec": 60,
            "startup_overhead_sec": 180,
            "stop_margin_sec": 900,
            "conservative_required_time_sec": conservative_required_sec,
            "allow_profile_changes_between_cases": True,
            "fresh_service_and_storage_per_case": True,
        },
        "submission_command": (
            "BACKEND_BATCH_ALLOW_PROFILE_CHANGES=1 "
            "BACKEND_BATCH_RUN_ID=pulsar_phase2_memory_screening_$(date +%Y%m%d_%H%M%S) "
            "./scripts/submit_backend_batch.sh pulsar "
            "configs/campaigns/pulsar/phase2/memory_screening_manifest.csv"
        ),
        "note": (
            "The inputs are mechanically ready, but submission_authorized remains "
            "false until the profile table and randomized manifest are reviewed."
        ),
    }
    _write_json(output_dir / "phase2_campaign_plan.json", plan)
    print(f"[pulsar-phase2] Profiles: {len(MEMORY_PROFILES)}")
    print(f"[pulsar-phase2] Anchors: {len(ANCHORS)}")
    print(f"[pulsar-phase2] Cases: {len(rows)} in one randomized job")
    print(
        "[pulsar-phase2] Conservative wall-time budget: "
        f"{conservative_required_sec}/7200 sec"
    )
    print("[pulsar-phase2] No Slurm job was submitted")
    return 0


def _validate_phase1_evidence(
    shortlist: dict[str, Any],
    summary: dict[str, Any],
    phase1_cases: dict[str, Any],
) -> None:
    expected = {config_id for config_id, _ in ANCHORS}
    corrected = {
        str(item.get("config_id", ""))
        for item in _list(shortlist, "configurations")
    }
    observed = {
        str(item.get("config_id", ""))
        for item in _list(phase1_cases, "cases")
    }
    if shortlist.get("configuration_count") != 10 or len(corrected) != 10:
        raise ValueError("Retrospectively corrected Phase 1 shortlist is invalid")
    if set(shortlist.get("historically_executed_phase2_anchor_ids", [])) != expected:
        raise ValueError("Phase 1 artifact does not identify the historical Phase 2 anchors")
    if phase1_cases.get("case_count") != 120 or not expected <= observed:
        raise ValueError("Complete Phase 1 cases do not cover the historical anchors")
    cases = _list(phase1_cases, "cases")
    required_counts = {
        "case_count": len(cases),
        "completed_case_count": sum(
            str(item.get("status", "")) == "completed" for item in cases
        ),
        "eligible_case_count": sum(bool(item.get("eligible")) for item in cases),
        "qualified_case_count": sum(bool(item.get("qualified")) for item in cases),
        "latency_valid_case_count": sum(
            bool(item.get("latency_valid")) for item in cases
        ),
    }
    mismatches = {
        name: summary.get(name)
        for name, expected_value in required_counts.items()
        if summary.get(name) != expected_value
    }
    if mismatches:
        raise ValueError(f"Phase 1 summary is inconsistent with its case rows: {mismatches}")
    if required_counts["case_count"] != 120 or required_counts["completed_case_count"] != 120:
        raise ValueError("Phase 1 does not contain 120 completed case rows")
    if required_counts["eligible_case_count"] < 114:
        raise ValueError(
            "Phase 1 has fewer than 95% scientifically eligible observations"
        )
    if required_counts["latency_valid_case_count"] != required_counts["eligible_case_count"]:
        raise ValueError("Phase 1 latency-valid and eligible counts do not agree")


def _historical_anchor_row(
    phase1_row: dict[str, Any], purpose: str
) -> dict[str, Any]:
    row = dict(phase1_row)
    row["selection_categories"] = (
        f"historically executed pre-correction anchor: {purpose}"
    )
    return row


def _write_memory_profiles(profile_root: Path) -> dict[str, dict[str, Any]]:
    baseline = _read_json(profile_root / "BASELINE_H16_D32.json")
    if canonical_sha256(_object(baseline, "settings")) != baseline.get("sha256"):
        raise ValueError("BASELINE_H16_D32 profile checksum is invalid")
    profiles: dict[str, dict[str, Any]] = {}
    for profile_id, heap_gib, direct_gib in MEMORY_PROFILES:
        if profile_id == "BASELINE_H16_D32":
            profile = copy.deepcopy(baseline)
        else:
            profile = copy.deepcopy(baseline)
            profile["profile_id"] = profile_id
            profile["parent_profile_id"] = "BASELINE_H16_D32"
            profile["purpose"] = (
                "Phase 2 memory-screening profile with "
                f"{heap_gib}-GiB heap and {direct_gib}-GiB direct-memory limit"
            )
            runtime = _object(_object(profile, "settings"), "runtime")
            runtime["jvm_memory"] = (
                f"-Xms{heap_gib}g -Xmx{heap_gib}g "
                f"-XX:MaxDirectMemorySize={direct_gib}g"
            )
            profile["sha256"] = canonical_sha256(profile["settings"])
            _write_json(profile_root / f"{profile_id}.json", profile)
        profiles[profile_id] = profile
    return profiles


def _build_case(
    *,
    source: dict[str, Any],
    profile: dict[str, Any],
    case_id: str,
    anchor_id: str,
    anchor_purpose: str,
    phase1_row: dict[str, Any],
    seed: int,
    campaign_id: str = "pulsar-phase2-memory-screening",
    stage: str = "phase2-memory-screening",
    extra_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config = copy.deepcopy(source)
    settings = _object(_object(config, "backend"), "pulsar")
    profile_settings = _object(profile, "settings")
    for field in (
        "service_count",
        "service_mode",
        "managed_ledger",
        "standalone_properties",
        "runtime",
    ):
        settings[field] = copy.deepcopy(profile_settings[field])
    settings["profile_id"] = profile["profile_id"]
    settings["profile_sha256"] = profile["sha256"]
    slug = case_id.lower().replace("_", "-")
    settings["topic_name"] = f"persistent://public/default/{slug}"
    settings["subscription_name"] = f"messaging-benchmark-{slug}"
    config["qualification_policy_id"] = "qualification.application.v1"
    config["campaign"] = {
        "case_id": case_id,
        "campaign_id": campaign_id,
        "metadata": {
            "stage": stage,
            "anchor_config_id": anchor_id,
            "anchor_purpose": anchor_purpose,
            "phase1_selection_categories": phase1_row["selection_categories"],
            "profile_id": profile["profile_id"],
            "seed": seed,
            "screening_only": stage == "phase2-memory-screening",
            "measurement_contract_id": "measurement.messaging.reproducible.v1",
            "backlog_event": "pending producer delivery callbacks at flush start",
            "backlog_denominator": "measurement-period send attempts",
            **(extra_metadata or {}),
        },
    }
    return config


def _write_case(path: Path, config: dict[str, Any]) -> None:
    normalized = normalize_case_config(config)
    effective = BenchmarkConfig(**benchmark_config_input_dict(normalized))
    get_backend("pulsar").validate_config(effective)
    path.write_text(
        json.dumps(effective.to_versioned_dict(), indent=2) + "\n",
        encoding="utf-8",
    )


def canonical_sha256(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_manifest(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def _object(value: dict[str, Any], key: str) -> dict[str, Any]:
    child = value.get(key)
    if not isinstance(child, dict):
        raise ValueError(f"Expected object at {key}")
    return child


def _list(value: dict[str, Any], key: str) -> list[dict[str, Any]]:
    child = value.get(key)
    if not isinstance(child, list) or not all(isinstance(item, dict) for item in child):
        raise ValueError(f"Expected object list at {key}")
    return child


def _project_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path)


def _reject_stale_generated_configs(
    generated_dir: Path,
    expected_names: set[str],
) -> None:
    stale = sorted(
        path.name
        for path in generated_dir.glob("*.json")
        if path.name not in expected_names
    )
    if stale:
        raise RuntimeError(
            f"Stale Phase 2 generated config(s) in {generated_dir}: "
            + ", ".join(stale)
        )


if __name__ == "__main__":
    raise SystemExit(main())
