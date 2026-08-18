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
    DEFAULT_PHASE1_CASES,
    MANIFEST_FIELDS,
    _build_case,
    _historical_anchor_row,
    _read_json,
    _reject_stale_generated_configs,
    _write_case,
)
from generate_pulsar_phase2_confirmation import (  # noqa: E402
    _validated_profile,
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
DEFAULT_PROFILE_ROOT = PROJECT_ROOT / "configs" / "backends" / "pulsar" / "profiles"
DEFAULT_OUTPUT = PROJECT_ROOT / "configs" / "campaigns" / "pulsar" / "final_validation"
DEFAULT_SEED = 20260812
FROZEN_PROFILE = "BASELINE_H16_D32"
BLOCKS = (1, 2, 3, 4, 5)
BATCH_BLOCKS = ((1, 2, 3), (4, 5))
HISTORICALLY_EXECUTED_CONFIGS = (
    "cfg_001",
    "cfg_007",
    "cfg_049",
    "cfg_055",
    "cfg_063",
    "cfg_080",
    "cfg_089",
    "cfg_093",
    "cfg_101",
    "cfg_120",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate five randomized Pulsar final-validation blocks from the "
            "historically executed workload set without submitting Slurm jobs."
        )
    )
    parser.add_argument("--phase1-config-root", type=Path, default=DEFAULT_PHASE1_CONFIG_ROOT)
    parser.add_argument("--shortlist", type=Path, default=DEFAULT_SHORTLIST)
    parser.add_argument("--phase1-cases", type=Path, default=DEFAULT_PHASE1_CASES)
    parser.add_argument("--final-selection", type=Path, default=DEFAULT_FINAL_SELECTION)
    parser.add_argument("--profile-root", type=Path, default=DEFAULT_PROFILE_ROOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    shortlist = _read_json(args.shortlist)
    phase1_cases = _read_json(args.phase1_cases)
    final_selection = _read_json(args.final_selection)
    _validate_source_evidence(shortlist, phase1_cases, final_selection)
    profile = _validated_profile(args.profile_root / f"{FROZEN_PROFILE}.json")
    phase1_by_id = {
        str(row["config_id"]): row for row in phase1_cases["cases"]
    }
    selected_rows = [
        _historical_anchor_row(
            phase1_by_id[config_id],
            "historical final-validation workload",
        )
        for config_id in HISTORICALLY_EXECUTED_CONFIGS
    ]

    output_dir = args.output_dir.resolve()
    generated_dir = output_dir / "generated_configs"
    batches_dir = output_dir / "batches"
    generated_dir.mkdir(parents=True, exist_ok=True)
    batches_dir.mkdir(parents=True, exist_ok=True)
    expected_names: set[str] = set()
    all_rows: list[dict[str, Any]] = []

    for block in BLOCKS:
        block_rows: list[dict[str, Any]] = []
        for selected in selected_rows:
            config_id = str(selected["config_id"])
            case_id = f"pulsar-pv-b{block:02d}-{config_id.replace('_', '')}"
            source = _read_json(args.phase1_config_root / f"{config_id}.json")
            categories = str(selected["selection_categories"])
            config = _build_case(
                source=source,
                profile=profile,
                case_id=case_id,
                anchor_id=config_id,
                anchor_purpose=categories,
                phase1_row=selected,
                seed=args.seed,
                campaign_id="pulsar-final-workload-validation",
                stage="final-workload-validation",
                extra_metadata={
                    "validation_block": block,
                    "repeat_index": block,
                    "retrospectively_corrected_phase1_shortlist_sha256": _sha256(
                        args.shortlist
                    ),
                    "phase2_final_selection_sha256": _sha256(args.final_selection),
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
                    "stage": "final-workload-validation",
                    "case_id": case_id,
                    "config_id": config_id,
                    "config_path": _display(path),
                    "profile_id": FROZEN_PROFILE,
                    "profile_sha256": profile["sha256"],
                    "anchor": config_id,
                    "anchor_purpose": categories,
                    "heap_gib": 16,
                    "direct_memory_gib": 32,
                    "producer_ranks": workload["producer_ranks"],
                    "consumer_ranks": workload["consumer_ranks"],
                    "partitions": pulsar["partitions"],
                    "payload_size_bytes": workload["payload_size_bytes"],
                    "warmup_sec": workload["warmup_sec"],
                    "duration_sec": workload["duration_sec"],
                    "drain_timeout_sec": workload["drain_timeout_sec"],
                    "latency_sample_every": workload["latency_sample_every"],
                    "design_note": "five-block historical workload-set validation",
                }
            )
        random.Random(args.seed + block).shuffle(block_rows)
        for order, row in enumerate(block_rows, start=1):
            row["order"] = order
        all_rows.extend(block_rows)

    _reject_stale_generated_configs(generated_dir, expected_names)
    manifest_path = output_dir / "final_validation_manifest.csv"
    _write_manifest(manifest_path, all_rows)
    batch_entries = []
    for batch_index, blocks in enumerate(BATCH_BLOCKS, start=1):
        rows = [row for row in all_rows if int(row["block"]) in blocks]
        batch_path = batches_dir / f"batch_{batch_index:02d}.csv"
        _write_manifest(batch_path, rows)
        required_sec = len(rows) * (105 + 60) + 180 + 900
        batch_entries.append(
            {
                "batch": batch_index,
                "blocks": list(blocks),
                "case_count": len(rows),
                "manifest": _display(batch_path),
                "manifest_sha256": _sha256(batch_path),
                "conservative_required_time_sec": required_sec,
                "remaining_wall_time_sec": 7200 - required_sec,
            }
        )

    plan = {
        "format": "messaging-benchmark.pulsar-final-validation-plan.v1",
        "status": "ready_for_review",
        "submission_authorized": False,
        "campaign_id": "pulsar-final-workload-validation",
        "seed": args.seed,
        "configuration_count": 10,
        "block_count": 5,
        "case_count": len(all_rows),
        "observations_per_configuration": 5,
        "profile_id": FROZEN_PROFILE,
        "profile_sha256": profile["sha256"],
        "configuration_ids": list(HISTORICALLY_EXECUTED_CONFIGS),
        "workload_set_status": "historically_executed_pre_correction_shortlist",
        "manifest": _display(manifest_path),
        "manifest_sha256": _sha256(manifest_path),
        "batches": batch_entries,
        "source_evidence": {
            "phase1_shortlist": _display(args.shortlist),
            "phase1_shortlist_sha256": _sha256(args.shortlist),
            "phase1_cases": _display(args.phase1_cases),
            "phase1_cases_sha256": _sha256(args.phase1_cases),
            "phase2_final_selection": _display(args.final_selection),
            "phase2_final_selection_sha256": _sha256(args.final_selection),
        },
        "eligibility_rule": (
            "completed healthy report, valid latency and clock calibration, "
            "balanced record accounting, and zero missing, duplicate, or "
            "out-of-order records"
        ),
        "qualification_rule": (
            "eligible and producer backlog at flush start <= 5% of messages "
            "enqueued, maximum producer flush <= 10 seconds, and failed sends <= 0.1%"
        ),
        "ranking_prerequisite": (
            "only eligible individual repeats contribute to aggregate performance metrics"
        ),
        "ranking_rule": [
            "qualified repeat count descending",
            "median balanced MiB/s descending",
            "balanced-throughput IQR ascending",
            "median producer backlog percentage ascending",
            "median flush duration ascending",
            "maximum failed-send percentage ascending",
            "configuration ID ascending",
        ],
        "secondary_metrics": [
            "median valid producer-to-consumer p99 latency",
        ],
        "slurm": {
            "job_count": 2,
            "exclusive_nodes_per_job": 4,
            "wall_time_sec": 7200,
            "case_budget_sec": 105,
            "per_case_overhead_sec": 60,
            "startup_overhead_sec": 180,
            "stop_margin_sec": 900,
            "fresh_service_and_storage_per_case": True,
            "single_frozen_profile": True,
        },
        "note": (
            "Generation does not submit Slurm jobs. The two batch manifests "
            "require an explicit authorization after validation. This preserves "
            "the historical pre-correction workload set; it does not claim that "
            "the retrospectively corrected Phase 1 shortlist was validated."
        ),
    }
    _write_json(output_dir / "final_validation_plan.json", plan)
    print(f"[pulsar-final-validation] configurations: {len(selected_rows)}")
    print(f"[pulsar-final-validation] blocks: {len(BLOCKS)}")
    print(f"[pulsar-final-validation] cases: {len(all_rows)}")
    print("[pulsar-final-validation] batches: 30 cases, 20 cases")
    print("[pulsar-final-validation] no Slurm job was submitted")
    return 0


def _validate_source_evidence(
    shortlist: dict[str, Any],
    phase1_cases: dict[str, Any],
    final_selection: dict[str, Any],
) -> None:
    rows = shortlist.get("configurations")
    if not isinstance(rows, list) or len(rows) != 10:
        raise ValueError("Published Phase 1 shortlist must contain 10 configurations")
    ids = [str(row.get("config_id")) for row in rows if isinstance(row, dict)]
    if len(ids) != 10 or len(set(ids)) != 10:
        raise ValueError("Published Phase 1 shortlist IDs are invalid")
    historical_ids = shortlist.get("historically_executed_final_validation_ids")
    if historical_ids != list(HISTORICALLY_EXECUTED_CONFIGS):
        raise ValueError(
            "Phase 1 artifact does not identify the historical final-validation set"
        )
    case_rows = phase1_cases.get("cases")
    if not isinstance(case_rows, list) or phase1_cases.get("case_count") != 120:
        raise ValueError("Complete Phase 1 case evidence is invalid")
    case_ids = {
        str(row.get("config_id")) for row in case_rows if isinstance(row, dict)
    }
    if not set(HISTORICALLY_EXECUTED_CONFIGS) <= case_ids:
        raise ValueError("Complete Phase 1 cases do not cover the historical workload set")
    if final_selection.get("status") != "profile_frozen":
        raise ValueError("Phase 2 final selection is not frozen")
    if final_selection.get("frozen_profile_id") != FROZEN_PROFILE:
        raise ValueError("Unexpected frozen Pulsar profile")


def _write_manifest(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _display(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path.resolve())


if __name__ == "__main__":
    raise SystemExit(main())
