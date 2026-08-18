#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from generate_pulsar_phase2 import (  # noqa: E402
    ANCHORS,
    DEFAULT_PHASE1_CASES,
    MANIFEST_FIELDS,
    _build_case,
    _historical_anchor_row,
    _read_json,
    _reject_stale_generated_configs,
    _write_case,
)


DEFAULT_SELECTION = (
    PROJECT_ROOT
    / "results"
    / "rebuilt"
    / "pulsar"
    / "phase2-screening"
    / "pulsar_phase2_candidate_selection.json"
)
DEFAULT_PHASE1_CONFIG_ROOT = (
    PROJECT_ROOT / "configs" / "campaigns" / "pulsar" / "phase1" / "generated_configs"
)
DEFAULT_SHORTLIST = (
    PROJECT_ROOT
    / "results"
    / "published"
    / "pulsar"
    / "phase1"
    / "pulsar_phase1_shortlist.json"
)
DEFAULT_PROFILE_ROOT = PROJECT_ROOT / "configs" / "backends" / "pulsar" / "profiles"
DEFAULT_OUTPUT = PROJECT_ROOT / "configs" / "campaigns" / "pulsar" / "phase2" / "confirmation"
BASELINE_PROFILE = "BASELINE_H16_D32"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate two randomized Pulsar Phase 2 confirmation blocks from "
            "a complete, validated memory-screening selection."
        )
    )
    parser.add_argument("--selection", type=Path, default=DEFAULT_SELECTION)
    parser.add_argument("--phase1-config-root", type=Path, default=DEFAULT_PHASE1_CONFIG_ROOT)
    parser.add_argument("--shortlist", type=Path, default=DEFAULT_SHORTLIST)
    parser.add_argument("--phase1-cases", type=Path, default=DEFAULT_PHASE1_CASES)
    parser.add_argument("--profile-root", type=Path, default=DEFAULT_PROFILE_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--seed", type=int, default=20260811)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    selection = _read_json(args.selection)
    _validate_selection(selection)
    profile_ids = [str(item) for item in selection["confirmation_profiles"]]
    profiles = {
        profile_id: _validated_profile(args.profile_root / f"{profile_id}.json")
        for profile_id in profile_ids
    }
    shortlist = _read_json(args.shortlist)
    historical_ids = {anchor_id for anchor_id, _ in ANCHORS}
    if set(shortlist.get("historically_executed_phase2_anchor_ids", [])) != historical_ids:
        raise ValueError("Phase 1 artifact does not identify the historical Phase 2 anchors")
    phase1_cases = _read_json(args.phase1_cases)
    phase1_by_id = {
        str(row["config_id"]): row for row in phase1_cases["cases"]
    }
    missing_anchors = {anchor_id for anchor_id, _ in ANCHORS} - set(phase1_by_id)
    if missing_anchors:
        raise ValueError(
            "Complete Phase 1 cases are missing historical anchors: "
            + ", ".join(sorted(missing_anchors))
        )
    output_dir = args.output_dir.resolve()
    generated_dir = output_dir / "generated_configs"
    generated_dir.mkdir(parents=True, exist_ok=True)
    expected_names: set[str] = set()
    rows: list[dict[str, Any]] = []

    for block in (2, 3):
        block_rows: list[dict[str, Any]] = []
        for profile_id in profile_ids:
            profile = profiles[profile_id]
            heap_gib, direct_gib = _memory_sizes(profile)
            for anchor_id, anchor_purpose in ANCHORS:
                case_id = (
                    f"pulsar-p2c-b{block:02d}-{anchor_id.replace('_', '')}-"
                    f"h{heap_gib}-d{direct_gib}"
                )
                source = _read_json(args.phase1_config_root / f"{anchor_id}.json")
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
                    campaign_id="pulsar-phase2-memory-confirmation",
                    stage="phase2-memory-confirmation",
                    extra_metadata={
                        "confirmation_block": block,
                        "screening_selection_sha256": _sha256(args.selection),
                    },
                )
                path = generated_dir / f"{case_id}.json"
                _write_case(path, config)
                expected_names.add(path.name)
                workload = _dict(config.get("workload"))
                pulsar = _dict(_dict(config.get("backend")).get("pulsar"))
                block_rows.append(
                    {
                        "block": block,
                        "order": 0,
                        "stage": "phase2-memory-confirmation",
                        "case_id": case_id,
                        "config_id": anchor_id,
                        "config_path": _display_path(path),
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
                        "design_note": "repeated memory-profile confirmation",
                    }
                )
        random.Random(args.seed + block).shuffle(block_rows)
        for order, row in enumerate(block_rows, start=1):
            row["order"] = order
        rows.extend(block_rows)

    _reject_stale_generated_configs(generated_dir, expected_names)
    manifest_path = output_dir / "memory_confirmation_manifest.csv"
    _write_manifest(manifest_path, rows)
    case_budget_sec = 15 + 30 + 60
    conservative_required_sec = len(rows) * (case_budget_sec + 60) + 180 + 900
    plan = {
        "format": "messaging-benchmark.pulsar-phase2-confirmation-plan.v1",
        "status": "ready_for_review",
        "submission_authorized": False,
        "campaign_id": "pulsar-phase2-memory-confirmation",
        "seed": args.seed,
        "configuration_count": len(rows),
        "profile_count": len(profile_ids),
        "anchor_count": len(ANCHORS),
        "additional_blocks": [2, 3],
        "additional_observations_per_profile_anchor": 2,
        "total_observations_per_profile_anchor_after_confirmation": 3,
        "workload_set_status": "historically_executed_pre_correction_anchor_set",
        "profiles": [
            {
                "profile_id": profile_id,
                "profile_sha256": profiles[profile_id]["sha256"],
                "heap_gib": _memory_sizes(profiles[profile_id])[0],
                "direct_memory_gib": _memory_sizes(profiles[profile_id])[1],
                "baseline": profile_id == BASELINE_PROFILE,
            }
            for profile_id in profile_ids
        ],
        "anchors": [anchor_id for anchor_id, _ in ANCHORS],
        "phase1_cases": {
            "path": _display_path(args.phase1_cases),
            "sha256": _sha256(args.phase1_cases),
        },
        "retrospectively_corrected_phase1_shortlist": {
            "path": _display_path(args.shortlist),
            "sha256": _sha256(args.shortlist),
        },
        "manifest": _display_path(manifest_path),
        "screening_selection": {
            "path": _display_path(args.selection),
            "sha256": _sha256(args.selection),
            "source_evidence": selection.get("source_evidence", {}),
        },
        "final_freeze_rule": [
            "all 15 profile observations are complete, healthy, eligible, latency-valid, and correctness-valid",
            "sustainable-anchor qualified-repeat count is at least the baseline count",
            "geometric mean of median balanced-throughput ratios across cfg_120, cfg_101, cfg_093, and cfg_001 is at least 1.03",
            "geometric mean of median p99-latency ratios across the same anchors is at most 1.10",
            "no failed-send, incomplete-drain, duplicate, or out-of-order regression",
            "maximum observed tmpfs use is below 75 percent and no unresolved memory or GC exhaustion is present",
            "retain BASELINE_H16_D32 when no candidate passes every rule",
        ],
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
        "note": (
            "These inputs require a separate post-screening authorization. "
            "Generation does not submit a Slurm job. The anchors preserve the "
            "historical pre-correction execution set and are not the retrospectively "
            "corrected Phase 1 shortlist."
        ),
    }
    _write_json(output_dir / "confirmation_plan.json", plan)
    print(f"[pulsar-phase2-confirmation] profiles: {len(profile_ids)}")
    print(f"[pulsar-phase2-confirmation] cases: {len(rows)}")
    print(
        "[pulsar-phase2-confirmation] conservative wall-time budget: "
        f"{conservative_required_sec}/7200 sec"
    )
    print("[pulsar-phase2-confirmation] no Slurm job was submitted")
    return 0


def _validate_selection(selection: dict[str, Any]) -> None:
    if selection.get("format") != "messaging-benchmark.pulsar-phase2-candidate-selection.v1":
        raise ValueError("Unexpected Phase 2 candidate-selection format")
    if selection.get("status") != "ready_for_confirmation":
        raise ValueError("Screening selection is not ready for confirmation")
    profiles = selection.get("confirmation_profiles")
    if not isinstance(profiles, list) or len(profiles) != 3:
        raise ValueError("Confirmation requires the baseline and two candidates")
    if profiles[0] != BASELINE_PROFILE or len(set(profiles)) != 3:
        raise ValueError("Confirmation profile identities are invalid")


def _validated_profile(path: Path) -> dict[str, Any]:
    profile = _read_json(path)
    settings = _dict(profile.get("settings"))
    if profile.get("backend_id") != "pulsar":
        raise ValueError(f"Profile backend is not Pulsar: {path}")
    if profile.get("sha256") != _canonical_sha256(settings):
        raise ValueError(f"Profile checksum mismatch: {path}")
    _memory_sizes(profile)
    return profile


def _memory_sizes(profile: dict[str, Any]) -> tuple[int, int]:
    raw = str(_dict(_dict(profile.get("settings")).get("runtime")).get("jvm_memory", ""))
    parts = raw.replace("-Xms", "").replace("-Xmx", "").replace(
        "-XX:MaxDirectMemorySize=", ""
    ).split()
    if len(parts) != 3 or not all(part.endswith("g") for part in parts):
        raise ValueError(f"Unsupported JVM memory string: {raw}")
    heap_min, heap_max, direct = (int(part[:-1]) for part in parts)
    if heap_min != heap_max:
        raise ValueError(f"Initial and maximum heap differ: {raw}")
    return heap_max, direct


def _write_manifest(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _canonical_sha256(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _display_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
