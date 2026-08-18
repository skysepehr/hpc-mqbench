#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import random
from pathlib import Path
import sys
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from models.benchmark_config import BenchmarkConfig, benchmark_config_input_dict
from src.benchmark.backends import get_backend
from src.benchmark.core.config_schema import normalize_case_config


BLOCKS = 5
WORKLOADS = 10
DEFAULT_SEED = 20260812
FIELDS = (
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
            "Generate a fresh five-block Pulsar repeated-validation campaign "
            "from a newly analyzed Phase 1 shortlist."
        )
    )
    parser.add_argument("--phase1-config-root", type=Path, required=True)
    parser.add_argument("--shortlist", type=Path, required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--campaign-id", default="pulsar-v1-reproducible-validation")
    parser.add_argument("--stage", default="v1-repeated-validation")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--batch-size", type=int, default=30)
    parser.add_argument("--authorize", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be positive")
    shortlist = _read_json(args.shortlist)
    source_rows = shortlist.get("configurations")
    if not isinstance(source_rows, list) or len(source_rows) != WORKLOADS:
        raise ValueError("Phase 1 shortlist must contain exactly 10 configurations")
    rows_by_id = {
        str(row.get("config_id", "")): row
        for row in source_rows
        if isinstance(row, dict)
    }
    if len(rows_by_id) != WORKLOADS or not all(rows_by_id):
        raise ValueError("Phase 1 shortlist configuration IDs are invalid")
    profile = _read_json(args.profile)
    profile_id = str(profile.get("profile_id", ""))
    profile_sha = str(profile.get("sha256", ""))
    if not profile_id or not profile_sha:
        raise ValueError("Pulsar profile identity is incomplete")

    output = args.output_dir.resolve()
    generated = output / "generated_configs"
    batches = output / "batches"
    generated.mkdir(parents=True, exist_ok=True)
    batches.mkdir(parents=True, exist_ok=True)
    all_rows: list[dict[str, Any]] = []
    expected_names: set[str] = set()
    ordered_ids = list(rows_by_id)
    for block in range(1, BLOCKS + 1):
        block_ids = list(ordered_ids)
        random.Random(args.seed + block).shuffle(block_ids)
        for order, config_id in enumerate(block_ids, start=1):
            source_path = args.phase1_config_root / f"{config_id}.json"
            source = _read_json(source_path)
            selected = rows_by_id[config_id]
            case_id = f"pulsar-v1v-b{block:02d}-{config_id.replace('_', '')}"
            config = _case(
                source,
                profile,
                case_id=case_id,
                campaign_id=args.campaign_id,
                stage=args.stage,
                config_id=config_id,
                selection_categories=str(selected.get("selection_categories", "Phase 1 shortlist")),
                seed=args.seed,
                block=block,
                order=order,
            )
            path = generated / f"{case_id}.json"
            _write_case(path, config)
            expected_names.add(path.name)
            workload = _dict(config["workload"])
            pulsar = _dict(_dict(config["backend"])["pulsar"])
            all_rows.append(
                {
                    "block": block,
                    "order": order,
                    "stage": args.stage,
                    "case_id": case_id,
                    "config_id": config_id,
                    "config_path": _display(path),
                    "profile_id": profile_id,
                    "profile_sha256": profile_sha,
                    "anchor": config_id,
                    "anchor_purpose": str(selected.get("selection_categories", "Phase 1 shortlist")),
                    "producer_ranks": workload["producer_ranks"],
                    "consumer_ranks": workload["consumer_ranks"],
                    "partitions": pulsar["partitions"],
                    "payload_size_bytes": workload["payload_size_bytes"],
                    "warmup_sec": workload["warmup_sec"],
                    "duration_sec": workload["duration_sec"],
                    "drain_timeout_sec": workload["drain_timeout_sec"],
                    "latency_sample_every": workload["latency_sample_every"],
                    "design_note": "five-block validation from the new Phase 1 shortlist",
                }
            )
    for path in generated.glob("*.json"):
        if path.name not in expected_names:
            raise ValueError(f"stale generated configuration would be retained: {path}")
    manifest = output / "validation_manifest.csv"
    _write_csv(manifest, FIELDS, all_rows)
    for path in batches.glob("batch_*.csv"):
        path.unlink()
    batch_paths = []
    for index, offset in enumerate(range(0, len(all_rows), args.batch_size), start=1):
        path = batches / f"batch_{index:02d}.csv"
        _write_csv(path, FIELDS, all_rows[offset : offset + args.batch_size])
        batch_paths.append(path)
    plan = {
        "format": "messaging-benchmark.pulsar-reproducible-validation-plan.v1",
        "status": "authorized_for_submission" if args.authorize else "ready_for_review",
        "submission_authorized": bool(args.authorize),
        "campaign_id": args.campaign_id,
        "stage": args.stage,
        "seed": args.seed,
        "configuration_count": WORKLOADS,
        "block_count": BLOCKS,
        "case_count": len(all_rows),
        "observations_per_configuration": BLOCKS,
        "configuration_ids": ordered_ids,
        "profile_id": profile_id,
        "profile_sha256": profile_sha,
        "manifest": _display(manifest),
        "manifest_sha256": _sha256(manifest),
        "batches": [
            {"path": _display(path), "case_count": _csv_count(path), "sha256": _sha256(path)}
            for path in batch_paths
        ],
        "source_shortlist": _display(args.shortlist),
        "source_shortlist_sha256": _sha256(args.shortlist),
        "measurement_contract_id": "measurement.messaging.reproducible.v1",
        "qualification_policy_id": "qualification.application.v1",
        "ranking_rule": [
            "qualified-repeat count descending",
            "median balanced MiB/s descending",
            "balanced-throughput IQR ascending",
            "producer backlog ascending",
            "flush duration ascending",
            "failed-send percentage ascending",
            "configuration ID ascending",
        ],
    }
    _write_json(output / "validation_plan.json", plan)
    _write_checksums(output)
    print(f"[pulsar-reproducible-validation] cases: {len(all_rows)}")
    print(f"[pulsar-reproducible-validation] jobs: {len(batch_paths)}")
    return 0


def _case(
    source: dict[str, Any],
    profile: dict[str, Any],
    *,
    case_id: str,
    campaign_id: str,
    stage: str,
    config_id: str,
    selection_categories: str,
    seed: int,
    block: int,
    order: int,
) -> dict[str, Any]:
    config = copy.deepcopy(source)
    settings = _dict(_dict(config["backend"])["pulsar"])
    profile_settings = _dict(profile["settings"])
    for field in ("service_count", "service_mode", "managed_ledger", "standalone_properties", "runtime"):
        settings[field] = copy.deepcopy(profile_settings[field])
    settings["profile_id"] = profile["profile_id"]
    settings["profile_sha256"] = profile["sha256"]
    slug = case_id.lower().replace("_", "-")
    settings["topic_name"] = f"persistent://public/default/{slug}"
    settings["subscription_name"] = f"messaging-benchmark-{slug}"
    workload = _dict(config["workload"])
    workload.update(
        {
            "warmup_sec": 15,
            "duration_sec": 30,
            "drain_timeout_sec": 60,
            "latency_enabled": True,
            "latency_sample_every": 10,
            "latency_clock_samples": 100,
            "latency_clock_max_uncertainty_us": 250.0,
            "latency_clock_max_drift_us": 250.0,
        }
    )
    config["qualification_policy_id"] = "qualification.application.v1"
    config["campaign"] = {
        "case_id": case_id,
        "campaign_id": campaign_id,
        "metadata": {
            "stage": stage,
            "config_id": config_id,
            "selection_categories": selection_categories,
            "seed": seed,
            "block": block,
            "order": order,
            "measurement_contract_id": "measurement.messaging.reproducible.v1",
            "backlog_event": "pending producer delivery callbacks at flush start",
            "backlog_denominator": "measurement-period send attempts",
        },
    }
    return config


def _write_case(path: Path, config: dict[str, Any]) -> None:
    normalized = normalize_case_config(config)
    effective = BenchmarkConfig(**benchmark_config_input_dict(normalized))
    get_backend("pulsar").validate_config(effective)
    path.write_text(json.dumps(effective.to_versioned_dict(), indent=2) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, fields: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_checksums(root: Path) -> None:
    paths = sorted(path for path in root.rglob("*") if path.is_file() and path.name != "SHA256SUMS")
    (root / "SHA256SUMS").write_text(
        "".join(f"{_sha256(path)}  {path.relative_to(root).as_posix()}\n" for path in paths),
        encoding="utf-8",
    )


def _csv_count(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _display(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path.resolve())


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
