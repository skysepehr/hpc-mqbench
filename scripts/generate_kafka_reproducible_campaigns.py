#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import shutil
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from models.benchmark_config import BenchmarkConfig, benchmark_config_input_dict
from scripts.generate_simultaneous_budgeted_sweep import generate_sweep
from scripts.make_simultaneous_budgeted_batches import make_batches
from src.benchmark.broker_profile import (
    PROFILE_SETTING_EXTRA,
    BrokerProfile,
    load_broker_profile,
    write_broker_profile,
)
from src.benchmark.config_loader import load_config
from src.benchmark.core.config_schema import normalize_case_config
from src.benchmark.qualification import COMMON_QUALIFICATION_POLICY_ID


DEFAULT_ROOT = PROJECT_ROOT / "configs/campaigns/kafka/v1_reproducible"
BASELINE = PROJECT_ROOT / "configs/one_broker_mpi_simultaneous.json"
MEASUREMENT_CONTRACT_ID = "measurement.messaging.reproducible.v1"
SCREENING_SEED = 42
VALIDATION_SEED = 20260728
WARMUP_SEC = 15
MEASUREMENT_SEC = 30
DRAIN_TIMEOUT_SEC = 60
VALIDATION_BLOCKS = 5
VALIDATION_WORKLOAD_COUNT = 10
LATENCY_SAMPLE_EVERY = 10
VALIDATION_FIELDS = (
    "config_id",
    "workload_config_id",
    "config_path",
    "topic_name",
    "scenario",
    "block",
    "order",
    "warmup_sec",
    "measurement_sec",
    "drain_timeout_sec",
    "latency_sample_every",
    "qualification_policy_id",
    "measurement_contract_id",
    "broker_profile_id",
    "broker_profile_sha256",
    "selection_basis",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate isolated reproducible Kafka V1 campaigns"
    )
    parser.add_argument("--root", default=str(DEFAULT_ROOT))
    parser.add_argument(
        "--latency-sample-every",
        type=int,
        choices=(LATENCY_SAMPLE_EVERY,),
        default=LATENCY_SAMPLE_EVERY,
        help="fixed deterministic timestamp sampling interval (must be 10)",
    )
    parser.add_argument("--screening-batch-size", type=int, default=30)
    parser.add_argument("--validation-batch-size", type=int, default=30)
    parser.add_argument(
        "--validation-only",
        action="store_true",
        help=(
            "generate validation inputs from an existing Phase 1 campaign without "
            "rewriting any Phase 1 file"
        ),
    )
    parser.add_argument(
        "--validation-shortlist",
        type=Path,
        help=(
            "phase1 validation_shortlist.json produced by "
            "analyze_kafka_reproducible_campaign.py; omit until all 120 new "
            "Phase 1 reports have passed validation"
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.latency_sample_every <= 0:
        raise SystemExit("--latency-sample-every must be positive")
    if args.validation_only and args.validation_shortlist is None:
        raise SystemExit("--validation-only requires --validation-shortlist")

    root = Path(args.root)
    phase1_root = root / "phase1"
    validation_root = root / "validation"
    if args.validation_only:
        profile = load_broker_profile(
            root / "broker_profiles" / "V1_FIXED_B0.json"
        )
        screening_rows = _read_manifest_rows(phase1_root / "sweep_manifest.csv")
        screening_batches = sorted((phase1_root / "batches").glob("batch_*.csv"))
        if len(screening_rows) != 120 or len(screening_batches) != 4:
            raise ValueError(
                "validation-only generation requires the complete 120-case, "
                "four-batch Phase 1 campaign"
            )
    else:
        profile = _write_fixed_v1_profile(root)
        baseline_snapshot = _write_profile_pinned_baseline(root, profile)
        screening_rows = generate_sweep(
            baseline_path=baseline_snapshot,
            output_dir=phase1_root,
            max_configs=120,
            seed=SCREENING_SEED,
            campaign_id="kafka_v1_reproducible_phase1",
            topic_prefix="kafka-v1r-screen",
            warmup_sec=WARMUP_SEC,
            measurement_sec=MEASUREMENT_SEC,
            drain_timeout_sec=DRAIN_TIMEOUT_SEC,
            latency_enabled=True,
            latency_sample_every=args.latency_sample_every,
            qualification_policy_id=COMMON_QUALIFICATION_POLICY_ID,
            measurement_contract_id=MEASUREMENT_CONTRACT_ID,
        )
        screening_batch_root = phase1_root / "batches"
        _clear_batch_manifests(screening_batch_root)
        screening_batches = make_batches(
            manifest_path=phase1_root / "sweep_manifest.csv",
            output_dir=screening_batch_root,
            batch_size=args.screening_batch_size,
        )

    validation_rows: list[dict[str, Any]] = []
    validation_batches: list[Path] = []
    validation_selection: dict[str, Any] | None = None
    if args.validation_shortlist is not None:
        validation_selection = _load_validation_selection(
            args.validation_shortlist,
            phase1_root=phase1_root,
        )
    if validation_root.exists():
        shutil.rmtree(validation_root)
    if validation_selection is not None:
        validation_rows = _write_validation_campaign(
            phase1_root=phase1_root,
            validation_root=validation_root,
            latency_sample_every=args.latency_sample_every,
            selection=validation_selection,
        )
        validation_batch_root = validation_root / "batches"
        _clear_batch_manifests(validation_batch_root)
        validation_batches = make_batches(
            manifest_path=validation_root / "validation_manifest.csv",
            output_dir=validation_batch_root,
            batch_size=args.validation_batch_size,
        )

    plan = {
        "format": "kafka_reproducible_v1_campaign.v1",
        "measurement_contract_id": MEASUREMENT_CONTRACT_ID,
        "qualification_policy_id": COMMON_QUALIFICATION_POLICY_ID,
        "broker_profile": {
            "profile_id": profile.profile_id,
            "profile_sha256": profile.sha256,
            "path": _relative(profile.path),
            "settings": profile.settings,
        },
        "qualification": {
            "backlog_event": "pending producer delivery callbacks at flush start",
            "backlog_denominator": "measurement-period send attempts",
            "backlog_percent_max": 5.0,
            "flush_duration_sec_max": 10.0,
            "failed_send_percent_max": 0.1,
            "eligibility_is_separate": True,
        },
        "instrumentation": {
            "record_identity": "producer rank plus sequence number",
            "record_envelope_on_every_record": True,
            "latency_timestamp_sample_every": args.latency_sample_every,
            "clock_samples_before_and_after": 100,
            "clock_uncertainty_limit_us": 250.0,
            "clock_drift_limit_us": 250.0,
            "exact_correctness_accounting": True,
            "deterministic_sampling_rule": "sequence_number modulo 10 equals zero",
            "correctness_identity_scope": "every record",
        },
        "timing": {
            "warmup_sec": WARMUP_SEC,
            "measurement_sec": MEASUREMENT_SEC,
            "drain_timeout_sec": DRAIN_TIMEOUT_SEC,
            "producer_stop_at_measurement_end": True,
            "pending_callbacks_snapshotted_before_flush": True,
            "producer_flush_before_consumer_drain": True,
        },
        "case_isolation": {
            "restart_kafka_between_cases": True,
            "clean_ram_backed_broker_storage_between_cases": True,
        },
        "execution_plan": {
            "submission_model": (
                "four independent Phase 1 allocations with up to 30 sequential "
                "cases per exclusive four-node Slurm job"
            ),
            "submit_script": "scripts/submit_kafka_reproducible_campaign.sh",
            "wall_time": "02:00:00",
            "phase1_jobs": len(screening_batches),
            "planned_validation_jobs": (
                VALIDATION_BLOCKS * VALIDATION_WORKLOAD_COUNT
                + args.validation_batch_size
                - 1
            )
            // args.validation_batch_size,
            "kafka_restart_model": "fresh Kafka process and RAM storage per case",
        },
        "phase1": {
            "seed": SCREENING_SEED,
            "case_count": len(screening_rows),
            "batch_size": args.screening_batch_size,
            "job_count": len(screening_batches),
        },
        "validation": {
            "seed": VALIDATION_SEED,
            "blocks": VALIDATION_BLOCKS,
            "workloads_per_block": VALIDATION_WORKLOAD_COUNT,
            "case_count": len(validation_rows),
            "planned_case_count": VALIDATION_BLOCKS * VALIDATION_WORKLOAD_COUNT,
            "batch_size": args.validation_batch_size,
            "job_count": len(validation_batches),
            "planned_job_count": (
                VALIDATION_BLOCKS * VALIDATION_WORKLOAD_COUNT
                + args.validation_batch_size
                - 1
            )
            // args.validation_batch_size,
            "workload_ids": (
                [item["config_id"] for item in validation_selection["shortlist"]]
                if validation_selection is not None
                else []
            ),
            "selection_basis": "new reproducible 120-case Phase 1 results only",
            "selection_source": (
                _display_path(args.validation_shortlist)
                if args.validation_shortlist is not None
                else None
            ),
            "status": (
                "generated_from_new_phase1_results"
                if validation_selection is not None
                else "awaiting_new_phase1_results"
            ),
            "submission_gate": (
                "generated shortlist must be reviewed before HPC submission"
                if validation_selection is not None
                else "blocked until all 120 new Phase 1 cases are analyzed"
            ),
        },
    }
    _write_json(root / "campaign_plan.json", plan)
    _write_checksums(root)

    print(f"[kafka-v1-reproducible] root: {_relative(root)}")
    print(
        "[kafka-v1-reproducible] phase1: "
        f"{len(screening_rows)} cases in {len(screening_batches)} jobs"
    )
    print(
        "[kafka-v1-reproducible] validation: "
        f"{len(validation_rows)} cases in {len(validation_batches)} jobs"
    )
    return 0


def _clear_batch_manifests(batch_root: Path) -> None:
    if not batch_root.is_dir():
        return
    for path in batch_root.glob("batch_*.csv"):
        path.unlink()


def _read_manifest_rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        raise ValueError(f"missing existing Phase 1 manifest: {path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_validation_campaign(
    *,
    phase1_root: Path,
    validation_root: Path,
    latency_sample_every: int,
    selection: dict[str, Any],
) -> list[dict[str, Any]]:
    generated_root = validation_root / "generated_configs"
    generated_root.mkdir(parents=True, exist_ok=True)
    rng = random.Random(VALIDATION_SEED)
    rows: list[dict[str, Any]] = []

    selected = {
        str(item["config_id"]): item for item in selection["shortlist"]
    }
    for block in range(1, VALIDATION_BLOCKS + 1):
        workload_ids = list(selected)
        rng.shuffle(workload_ids)
        for order, workload_id in enumerate(workload_ids, start=1):
            source_path = phase1_root / "generated_configs" / f"{workload_id}.json"
            source = normalize_case_config(load_config(source_path))
            config_data = BenchmarkConfig(
                **benchmark_config_input_dict(source)
            ).to_input_dict()
            case_id = f"kafka-v1r-b{block:02d}-{workload_id.replace('_', '')}"
            config_data.update(
                {
                    "case_id": case_id,
                    "campaign_id": "kafka_v1_reproducible_validation",
                    "topic_name": case_id,
                    "latency_sample_every": latency_sample_every,
                    "qualification_policy_id": COMMON_QUALIFICATION_POLICY_ID,
                }
            )
            extra = dict(config_data.get("extra", {}))
            extra["campaign_metadata"] = {
                "measurement_contract_id": MEASUREMENT_CONTRACT_ID,
                "backlog_event": "pending delivery callbacks at flush start",
                "backlog_denominator": "measurement-period send attempts",
                "validation_seed": VALIDATION_SEED,
                "validation_block": block,
                "validation_order": order,
                "workload_config_id": workload_id,
                "selection_basis": (
                    "new reproducible Phase 1 categories: "
                    + ", ".join(selected[workload_id]["selection_categories"])
                ),
            }
            config_data["extra"] = extra
            config = BenchmarkConfig(**benchmark_config_input_dict(config_data))
            config_path = generated_root / f"{case_id}.json"
            _write_json(config_path, config.to_versioned_dict())
            rows.append(
                {
                    "config_id": case_id,
                    "workload_config_id": workload_id,
                    "config_path": _relative(config_path),
                    "topic_name": case_id,
                    "scenario": "simultaneous",
                    "block": block,
                    "order": order,
                    "warmup_sec": WARMUP_SEC,
                    "measurement_sec": MEASUREMENT_SEC,
                    "drain_timeout_sec": DRAIN_TIMEOUT_SEC,
                    "latency_sample_every": latency_sample_every,
                    "qualification_policy_id": COMMON_QUALIFICATION_POLICY_ID,
                    "measurement_contract_id": MEASUREMENT_CONTRACT_ID,
                    "broker_profile_id": config.broker_profile_id,
                    "broker_profile_sha256": config.broker_profile_sha256,
                    "selection_basis": ",".join(
                        selected[workload_id]["selection_categories"]
                    ),
                }
            )

    manifest_path = validation_root / "validation_manifest.csv"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=VALIDATION_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    return rows


def _load_validation_selection(
    path: Path,
    *,
    phase1_root: Path,
) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected a JSON object")
    if payload.get("source_stage") != "phase1":
        raise ValueError(f"{path}: shortlist source_stage must be phase1")
    if int(payload.get("source_case_count", 0)) != 120:
        raise ValueError(f"{path}: shortlist must come from all 120 Phase 1 cases")
    shortlist = payload.get("shortlist")
    if not isinstance(shortlist, list) or len(shortlist) != VALIDATION_WORKLOAD_COUNT:
        raise ValueError(
            f"{path}: shortlist must contain exactly {VALIDATION_WORKLOAD_COUNT} entries"
        )
    config_ids: list[str] = []
    for item in shortlist:
        if not isinstance(item, dict):
            raise ValueError(f"{path}: every shortlist entry must be an object")
        config_id = str(item.get("config_id", ""))
        categories = item.get("selection_categories")
        if not config_id or not isinstance(categories, list) or not categories:
            raise ValueError(f"{path}: shortlist entry lacks config_id or categories")
        if not (phase1_root / "generated_configs" / f"{config_id}.json").is_file():
            raise ValueError(f"{path}: unknown Phase 1 config_id {config_id}")
        config_ids.append(config_id)
    if len(config_ids) != len(set(config_ids)):
        raise ValueError(f"{path}: shortlist config IDs must be unique")
    return payload


def _write_fixed_v1_profile(root: Path) -> BrokerProfile:
    return write_broker_profile(
        root / "broker_profiles" / "V1_FIXED_B0.json",
        {
            "format": "kafka_broker_profile.v2",
            "profile_id": "V1_FIXED_B0",
            "scope": "reproducibly instrumented Kafka V1 campaigns",
            "settings": {
                "heap_opts": "-Xms8g -Xmx8g",
                "num_network_threads": 8,
                "num_io_threads": 16,
                "socket_send_buffer_bytes": 1_048_576,
                "socket_receive_buffer_bytes": 1_048_576,
                "socket_request_max_bytes": 104_857_600,
                "queued_max_requests": 1_000,
                "log_segment_bytes": 1_073_741_824,
            },
            "fixed_semantics": {
                "java_major_version": 17,
                "garbage_collector": "Java 17 server JVM default (G1)",
                "broker_count": 1,
                "replication_factor": 1,
                "acks": "1",
                "compression_type": "none",
                "log_storage": "RAM-backed /dev/shm",
                "network_interface": "ib0",
                "topic_partitions_varied_per_case": True,
            },
            "server_properties_contract": {
                "process.roles": "broker,controller",
                "controller.listener.names": "CONTROLLER",
                "listener.security.protocol.map": (
                    "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"
                ),
                "inter.broker.listener.name": "PLAINTEXT",
                "num.partitions": "3",
                "num.recovery.threads.per.data.dir": "1",
                "offsets.topic.replication.factor": "1",
                "transaction.state.log.replication.factor": "1",
                "transaction.state.log.min.isr": "1",
                "log.retention.hours": "1",
                "log.retention.check.interval.ms": "300000",
                "group.initial.rebalance.delay.ms": "0",
            },
        },
    )


def _write_profile_pinned_baseline(
    root: Path,
    profile: BrokerProfile,
) -> Path:
    baseline = json.loads(BASELINE.read_text(encoding="utf-8"))
    baseline["broker_profile_id"] = profile.profile_id
    baseline["broker_profile_sha256"] = profile.sha256
    extra = dict(baseline.get("extra", {}))
    extra["broker_profile_path"] = _relative(profile.path)
    for setting_name, extra_name in PROFILE_SETTING_EXTRA.items():
        extra[extra_name] = profile.settings[setting_name]
    baseline["extra"] = extra
    path = root / "baseline_snapshot.json"
    _write_json(path, baseline)
    return path


def _write_checksums(root: Path) -> None:
    paths = sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.name != "SHA256SUMS"
    )
    lines = [
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(root)}"
        for path in paths
    ]
    (root / "SHA256SUMS").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _relative(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        # External workflow roots are useful for isolated validation. The
        # batch/config loaders already accept absolute paths; repo-local
        # production workflows continue to receive portable relative paths.
        return str(resolved)


def _display_path(path: Path) -> str:
    try:
        return _relative(path)
    except ValueError:
        return path.name


if __name__ == "__main__":
    raise SystemExit(main())
