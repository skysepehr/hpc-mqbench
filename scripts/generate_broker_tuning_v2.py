#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import statistics
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from models.benchmark_config import BenchmarkConfig, benchmark_config_input_dict
from scripts.make_simultaneous_budgeted_batches import make_batches
from src.benchmark.broker_profile import (
    BrokerProfile,
    load_broker_profile,
    write_broker_profile,
)
from src.benchmark.config_loader import load_config
from src.benchmark.qualification import COMMON_QUALIFICATION_POLICY_ID


SEED = 20260728
MEASUREMENT_CONTRACT_ID = "measurement.messaging.reproducible.v1"
WARMUP_SEC = 15
MEASUREMENT_SEC = 30
DRAIN_TIMEOUT_SEC = 60
CAMPAIGN_BATCH_SIZE = 30
LATENCY_SAMPLE_EVERY = 10
PROFILE_IDS = ("B0", "B1", "B2", "B3", "B4", "B5")
INSTRUMENTATION_PROBE_CONFIG_ID = "cfg_074"
BROKER_EXTRA_KEYS = {
    "kafka_heap_opts",
    "kafka_num_network_threads",
    "kafka_num_io_threads",
    "kafka_socket_send_buffer_bytes",
    "kafka_socket_receive_buffer_bytes",
    "kafka_socket_request_max_bytes",
    "kafka_queued_max_requests",
    "kafka_log_segment_bytes",
}
MANIFEST_FIELDS = (
    "case_id",
    "config_id",
    "stage",
    "block",
    "order",
    "anchor_id",
    "anchor_role",
    "broker_profile_id",
    "latency_enabled",
    "latency_sample_every",
    "target_records_per_sec",
    "scenario",
    "config_path",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate isolated V2 broker-tuning and latency campaigns"
    )
    parser.add_argument(
        "--root",
        default="configs/tuning_v2",
        help="V2 configuration root (default: configs/tuning_v2)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "initialize",
        help="write profiles, six instrumentation pilots, and three calibration cases",
    )
    calibration = subparsers.add_parser(
        "prepare-rate-calibration",
        help="write three B0 latency-anchor cases with the selected sampling mode",
    )
    calibration.add_argument(
        "--latency-sample-every",
        type=int,
        choices=(LATENCY_SAMPLE_EVERY,),
        default=LATENCY_SAMPLE_EVERY,
    )

    screening = subparsers.add_parser(
        "prepare-screening",
        help="derive the latency-anchor rate and write the 30-case profile screen",
    )
    screening.add_argument("--calibration-results", required=True)
    screening.add_argument(
        "--phase1-selection",
        required=True,
        help=(
            "validation_shortlist.json generated from the completed new "
            "120-case reproducible Kafka Phase 1 campaign"
        ),
    )
    screening.add_argument(
        "--latency-sample-every",
        type=int,
        choices=(LATENCY_SAMPLE_EVERY,),
        default=LATENCY_SAMPLE_EVERY,
    )

    confirmation = subparsers.add_parser(
        "prepare-confirmation",
        help="write two additional repetitions for B0 and selected candidates",
    )
    confirmation.add_argument(
        "--profiles",
        required=True,
        help="comma-separated confirmed profiles; B0 is added automatically",
    )
    confirmation.add_argument("--target-records-per-sec", type=float, required=True)
    confirmation.add_argument(
        "--latency-sample-every",
        type=int,
        choices=(LATENCY_SAMPLE_EVERY,),
        default=LATENCY_SAMPLE_EVERY,
    )

    final = subparsers.add_parser(
        "prepare-final-validation",
        help="freeze a selected profile and write the 50-case final validation",
    )
    final.add_argument("--winner", choices=PROFILE_IDS, required=True)
    final.add_argument(
        "--confirmation-results",
        required=True,
        help="result root containing a valid confirmation case for the winner",
    )
    final.add_argument(
        "--latency-sample-every",
        type=int,
        choices=(LATENCY_SAMPLE_EVERY,),
        default=LATENCY_SAMPLE_EVERY,
    )
    final.add_argument(
        "--workload-shortlist",
        required=True,
        help=(
            "validation_shortlist.json generated from the completed new "
            "120-case reproducible Kafka Phase 1 campaign"
        ),
    )
    return parser.parse_args()


def project_root() -> Path:
    return PROJECT_ROOT


def relative_to_project(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(project_root()).as_posix()
    except ValueError:
        return str(resolved)


def _display_path(path: Path) -> str:
    try:
        return relative_to_project(path)
    except ValueError:
        return path.name


def profile_payload(
    profile_id: str,
    *,
    network_threads: int,
    io_threads: int,
    heap_gib: int,
    log_segment_bytes: int,
) -> dict[str, Any]:
    return {
        "format": "kafka_broker_profile.v2",
        "profile_id": profile_id,
        "scope": "single-broker GWDG broker-tuning campaign",
        "settings": {
            "heap_opts": (
                f"-Xms{heap_gib}g -Xmx{heap_gib}g -XX:+UseG1GC"
            ),
            "num_network_threads": network_threads,
            "num_io_threads": io_threads,
            "socket_send_buffer_bytes": 1_048_576,
            "socket_receive_buffer_bytes": 1_048_576,
            "socket_request_max_bytes": 104_857_600,
            "queued_max_requests": 1_000,
            "log_segment_bytes": log_segment_bytes,
        },
        "fixed_semantics": {
            "java_major_version": 17,
            "garbage_collector": "G1",
            "broker_count": 1,
            "replication_factor": 1,
            "acks": "1",
            "compression_type": "none",
            "replica_fetchers_varied": False,
            "log_storage": "RAM-backed /dev/shm",
            "network_interface": "ib0",
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
    }


def write_profiles(root: Path) -> dict[str, BrokerProfile]:
    definitions = {
        "B0": (8, 16, 8, 1_073_741_824),
        "B1": (16, 32, 16, 1_073_741_824),
        "B2": (32, 64, 32, 1_073_741_824),
        "B3": (48, 96, 32, 1_073_741_824),
        "B4": (48, 96, 64, 1_073_741_824),
        "B5": (48, 96, 64, 2_147_483_647),
    }
    profiles: dict[str, BrokerProfile] = {}
    for profile_id, values in definitions.items():
        profiles[profile_id] = write_broker_profile(
            root / "broker_profiles" / f"{profile_id}.json",
            profile_payload(
                profile_id,
                network_threads=values[0],
                io_threads=values[1],
                heap_gib=values[2],
                log_segment_bytes=values[3],
            ),
        )
    return profiles


def source_config(config_id: str) -> dict[str, Any]:
    path = (
        project_root()
        / "configs/sweeps/simultaneous_budgeted/generated_configs"
        / f"{config_id}.json"
    )
    raw = load_config(path)
    config = BenchmarkConfig(**benchmark_config_input_dict(raw))
    return config.to_input_dict()


def latency_anchor_source() -> dict[str, Any]:
    data = source_config("cfg_001")
    data.update(
        {
            "partitions": 120,
            "batch_size": 32_768,
            "linger_ms": 1,
            "payload_size_bytes": 4_096,
            "producer_ranks": 40,
            "consumer_ranks": 40,
        }
    )
    extra = dict(data["extra"])
    producer = dict(extra.get("kafka_producer_config", {}))
    consumer = dict(extra.get("kafka_consumer_config", {}))
    consumer["fetch.min.bytes"] = 1
    consumer["fetch.wait.max.ms"] = 1
    extra["kafka_producer_config"] = producer
    extra["kafka_consumer_config"] = consumer
    data["extra"] = extra
    return data


def anchor_source(anchor_id: str) -> dict[str, Any]:
    return latency_anchor_source() if anchor_id == "latency_anchor" else source_config(anchor_id)


def load_phase1_selection(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected a JSON object")
    if payload.get("source_stage") != "phase1":
        raise ValueError(f"{path}: source_stage must be phase1")
    if int(payload.get("source_case_count", 0)) != 120:
        raise ValueError(f"{path}: selection must represent all 120 Phase 1 cases")

    shortlist = payload.get("shortlist")
    if not isinstance(shortlist, list) or len(shortlist) != 10:
        raise ValueError(f"{path}: shortlist must contain exactly 10 workloads")
    shortlist_ids = [
        str(item.get("config_id", "")) if isinstance(item, dict) else ""
        for item in shortlist
    ]
    if not all(shortlist_ids) or len(shortlist_ids) != len(set(shortlist_ids)):
        raise ValueError(f"{path}: shortlist IDs must be non-empty and unique")

    anchors = payload.get("v2_profile_anchors")
    if not isinstance(anchors, list) or len(anchors) != 4:
        raise ValueError(f"{path}: exactly four V2 profile anchors are required")
    anchor_ids = [
        str(item.get("config_id", "")) if isinstance(item, dict) else ""
        for item in anchors
    ]
    if not all(anchor_ids) or len(anchor_ids) != len(set(anchor_ids)):
        raise ValueError(f"{path}: V2 anchor IDs must be non-empty and unique")
    sustainable = [
        item
        for item in anchors
        if str(item.get("role", "")).startswith("qualified_sustainable_")
        and item.get("qualification_status") == "qualified"
    ]
    if len(sustainable) != 2:
        raise ValueError(
            f"{path}: two qualified sustainable V2 anchors are required"
        )
    for config_id in set(shortlist_ids + anchor_ids):
        source_config(config_id)
    return payload


def screening_anchor_records(root: Path) -> list[dict[str, Any]]:
    path = root / "campaigns/profile_screening_metadata.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    anchors = payload.get("maximum_load_anchors")
    if not isinstance(anchors, list) or len(anchors) != 4:
        raise ValueError(
            f"{path}: profile screening metadata must contain four anchors"
        )
    return anchors


def build_case(
    *,
    root: Path,
    profile: BrokerProfile,
    anchor_id: str,
    case_id: str,
    stage: str,
    block: int,
    order: int,
    latency_enabled: bool,
    latency_sample_every: int,
    target_records_per_sec: float | None,
    anchor_role: str,
    measurement_sec: int = MEASUREMENT_SEC,
) -> tuple[Path, dict[str, Any]]:
    if latency_enabled and latency_sample_every != LATENCY_SAMPLE_EVERY:
        raise ValueError(
            "latency-enabled Kafka V2 cases require deterministic 1-in-10 sampling"
        )
    data = anchor_source(anchor_id)
    extra = {
        key: value
        for key, value in dict(data.get("extra", {})).items()
        if key not in BROKER_EXTRA_KEYS
    }
    extra.update(
        {
            "purpose": "focused Kafka broker-tuning and latency validation V2",
            "v2_stage": stage,
            "v2_case_id": case_id,
            "v2_anchor_id": anchor_id,
            "v2_anchor_role": anchor_role,
            "v2_block": block,
            "v2_order": order,
            "broker_profile_path": relative_to_project(profile.path),
            "prometheus_scrape_interval_sec": 1,
            "monitoring_range_step_sec": 1,
            "broker_process_monitor_interval_sec": 1,
            "campaign_metadata": {
                "measurement_contract_id": MEASUREMENT_CONTRACT_ID,
                "backlog_event": (
                    "pending delivery callbacks at flush start"
                ),
                "backlog_denominator": (
                    "measurement-period send attempts"
                ),
                "campaign_seed": SEED,
            },
        }
    )
    data.update(
        {
            "mode": "single",
            "scenario": "simultaneous",
            "topic_name": f"kbtv2-{case_id.replace('_', '-')}",
            "duration_sec": measurement_sec,
            "warmup_sec": WARMUP_SEC,
            "drain_timeout_sec": DRAIN_TIMEOUT_SEC,
            "target_records_per_sec": target_records_per_sec,
            "latency_enabled": latency_enabled,
            "latency_sample_every": latency_sample_every,
            "latency_clock_samples": 100,
            "latency_clock_max_uncertainty_us": 250.0,
            "latency_clock_max_drift_us": 250.0,
            "broker_profile_id": profile.profile_id,
            "broker_profile_sha256": profile.sha256,
            "qualification_policy_id": COMMON_QUALIFICATION_POLICY_ID,
            "campaign_id": "focused_broker_tuning_latency_v2",
            "case_id": case_id,
            "extra": extra,
        }
    )
    config = BenchmarkConfig(**benchmark_config_input_dict(data))

    config_path = root / "generated_configs" / stage / f"{case_id}.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        json.dumps(config.to_versioned_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    row = {
        "case_id": case_id,
        "config_id": case_id,
        "stage": stage,
        "block": block,
        "order": order,
        "anchor_id": anchor_id,
        "anchor_role": anchor_role,
        "broker_profile_id": profile.profile_id,
        "latency_enabled": str(latency_enabled).lower(),
        "latency_sample_every": latency_sample_every,
        "target_records_per_sec": (
            f"{target_records_per_sec:.6f}"
            if target_records_per_sec is not None
            else ""
        ),
        "scenario": "simultaneous",
        "config_path": relative_to_project(config_path),
    }
    return config_path, row


def write_manifest(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    batch_dir = path.parent / "batches" / path.stem
    batch_dir.mkdir(parents=True, exist_ok=True)
    for stale_path in batch_dir.glob("batch_*.csv"):
        stale_path.unlink()
    make_batches(
        manifest_path=path,
        output_dir=batch_dir,
        batch_size=CAMPAIGN_BATCH_SIZE,
    )


def initialize(root: Path) -> None:
    _clear_generated_stage(root, "instrumentation_pilot")
    _clear_generated_stage(root, "rate_calibration")
    stale_fallback = root / "campaigns/instrumentation_fallback.csv"
    stale_fallback.unlink(missing_ok=True)
    _clear_generated_stage(root, "instrumentation_fallback")
    stale_fallback_batches = root / "campaigns/batches/instrumentation_fallback"
    if stale_fallback_batches.is_dir():
        for path in stale_fallback_batches.glob("batch_*.csv"):
            path.unlink()
    profiles = write_profiles(root)
    rng = random.Random(SEED)

    pilot_rows: list[dict[str, Any]] = []
    for block in range(1, 4):
        variants = [("off", False), ("sample10", True)]
        rng.shuffle(variants)
        for order, (variant, latency_enabled) in enumerate(variants, start=1):
            case_id = (
                f"pilot_b{block:02d}_o{order:02d}_{variant}_"
                f"{INSTRUMENTATION_PROBE_CONFIG_ID.replace('_', '')}"
            )
            _, row = build_case(
                root=root,
                profile=profiles["B0"],
                anchor_id=INSTRUMENTATION_PROBE_CONFIG_ID,
                anchor_role="instrumentation_overhead_probe",
                case_id=case_id,
                stage="instrumentation_pilot",
                block=block,
                order=order,
                latency_enabled=latency_enabled,
                latency_sample_every=LATENCY_SAMPLE_EVERY,
                target_records_per_sec=None,
            )
            pilot_rows.append(row)
    write_manifest(root / "campaigns/instrumentation_pilot.csv", pilot_rows)

    prepare_rate_calibration(
        root,
        profiles=profiles,
        latency_sample_every=LATENCY_SAMPLE_EVERY,
    )

    blueprint = {
        "format": "kafka_broker_tuning_campaign.v2",
        "measurement_contract_id": MEASUREMENT_CONTRACT_ID,
        "qualification_policy_id": COMMON_QUALIFICATION_POLICY_ID,
        "seed": SEED,
        "profiles": list(PROFILE_IDS),
        "maximum_load_anchors": "generated from new reproducible V1 Phase 1 results",
        "maximum_load_anchor_count": 4,
        "latency_anchor": "latency_anchor",
        "final_shortlist": "generated from new reproducible V1 Phase 1 results",
        "final_shortlist_count": 10,
        "qualification": {
            "pending_backlog_percent_max": 5.0,
            "flush_sec_max": 10.0,
            "failed_send_percent_max": 0.1,
        },
        "timing": {
            "warmup_sec": WARMUP_SEC,
            "measurement_sec": MEASUREMENT_SEC,
            "rate_calibration_measurement_sec": MEASUREMENT_SEC,
            "profile_measurement_overrides_sec": {},
            "drain_timeout_sec": DRAIN_TIMEOUT_SEC,
            "producer_stop_at_measurement_end": True,
            "pending_callbacks_snapshotted_before_flush": True,
            "producer_flush_before_consumer_drain": True,
        },
        "case_isolation": {
            "restart_kafka_between_cases": True,
            "clean_ram_backed_broker_storage_between_cases": True,
        },
        "clock": {
            "samples_before_and_after": 100,
            "max_uncertainty_us": 250.0,
            "max_drift_us": 250.0,
        },
        "latency_sampling": {
            "timestamp_sample_every": LATENCY_SAMPLE_EVERY,
            "rule": "sequence_number modulo 10 equals zero",
            "record_identity_and_correctness_scope": "every record",
            "fixed_to_match_pulsar": True,
        },
        "case_budget": {
            "instrumentation_pilot": 6,
            "rate_calibration": 3,
            "profile_screening": 30,
            "profile_confirmation": "20 or 30",
            "final_validation": 50,
            "maximum_campaign_total": 119,
        },
        "execution_plan": {
            "submission_model": (
                "up to 30 sequential cases per exclusive four-node Slurm job"
            ),
            "batch_size_limit": CAMPAIGN_BATCH_SIZE,
            "kafka_restart_model": "fresh Kafka process and RAM storage per case",
            "instrumentation_pilot_jobs": 1,
            "rate_calibration_jobs": 1,
            "profile_screening_jobs": 1,
            "profile_confirmation_jobs": 1,
            "final_validation_jobs": 2,
            "maximum_total_jobs": 6,
        },
        "profile_selection_rules": {
            "required_measurements_and_correctness_valid": True,
            "sustainable_anchor_qualification_not_below_B0": True,
            "minimum_geometric_mean_throughput_ratio_to_B0": 1.03,
            "maximum_latency_anchor_p99_ratio_to_B0": 1.10,
            "failed_send_or_drain_regression_allowed": False,
            "maximum_tmpfs_used_percent": 75.0,
        },
    }
    (root / "campaign_blueprint.json").write_text(
        json.dumps(blueprint, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _clear_generated_stage(root: Path, stage: str) -> None:
    stage_root = root / "generated_configs" / stage
    if not stage_root.is_dir():
        return
    for path in stage_root.glob("*.json"):
        path.unlink()


def prepare_rate_calibration(
    root: Path,
    *,
    profiles: dict[str, BrokerProfile] | None = None,
    latency_sample_every: int,
) -> None:
    profiles = profiles or write_profiles(root)
    calibration_rows: list[dict[str, Any]] = []
    for block in range(1, 4):
        case_id = f"rate30_b{block:02d}_latency_anchor_B0"
        _, row = build_case(
            root=root,
            profile=profiles["B0"],
            anchor_id="latency_anchor",
            anchor_role="fixed_rate_latency_reference",
            case_id=case_id,
            stage="rate_calibration",
            block=block,
            order=1,
            latency_enabled=True,
            latency_sample_every=latency_sample_every,
            target_records_per_sec=None,
            measurement_sec=MEASUREMENT_SEC,
        )
        calibration_rows.append(row)
    write_manifest(root / "campaigns/rate_calibration.csv", calibration_rows)


def discover_calibration_rates(
    results_root: Path,
    *,
    expected_sample_every: int,
) -> list[float]:
    rates: list[float] = []
    seen_case_ids: set[str] = set()
    for path in results_root.rglob("final_report.json"):
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
            config = report.get("config", {})
            extra = config.get("extra", {})
            if extra.get("v2_stage") != "rate_calibration":
                continue
            case_id = str(extra.get("v2_case_id", ""))
            if not case_id or case_id in seen_case_ids:
                continue
            if int(config.get("latency_sample_every", 0)) != expected_sample_every:
                raise ValueError(
                    f"{path}: calibration sampling is "
                    f"1-in-{config.get('latency_sample_every')}, expected "
                    f"1-in-{expected_sample_every}"
                )
            if int(config.get("duration_sec", 0)) != MEASUREMENT_SEC:
                raise ValueError(
                    f"{path}: calibration measurement is "
                    f"{config.get('duration_sec')}s, expected "
                    f"{MEASUREMENT_SEC}s"
                )
            producers = report["aggregated_metrics"]["producers"]
            consumers = report["aggregated_metrics"]["consumers"]
            correctness = report["aggregated_metrics"]["record_correctness"]
            if report.get("case", {}).get("status") != "completed":
                raise ValueError(f"{path}: case is not completed")
            if any(
                int(correctness.get(field, -1)) != 0
                for field in (
                    "missing_after_drain_records",
                    "unexplained_surplus_records",
                    "invalid_envelope_count",
                    "duplicate_offset_count",
                    "out_of_order_offset_count",
                )
            ):
                raise ValueError(f"{path}: record correctness checks failed")
            if int(producers.get("flush_remaining_messages", -1)) != 0:
                raise ValueError(f"{path}: producer drain is incomplete")
            rate = min(
                float(producers["throughput_msgs_per_sec"]),
                float(consumers["throughput_msgs_per_sec"]),
            )
            if rate <= 0:
                raise ValueError(f"{path}: non-positive balanced records/s")
            if not bool(report.get("latency_validation", {}).get("valid")):
                raise ValueError(f"{path}: latency validation is not valid")
            rates.append(rate)
            seen_case_ids.add(case_id)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid rate-calibration report {path}: {exc}") from exc
    if len(rates) != 3:
        raise ValueError(
            f"expected exactly 3 unique valid rate-calibration reports, found {len(rates)}"
        )
    return rates


def prepare_screening(
    root: Path,
    *,
    calibration_results: Path,
    phase1_selection: Path,
    latency_sample_every: int,
) -> None:
    profiles = write_profiles(root)
    selection = load_phase1_selection(phase1_selection)
    maximum_load_anchors = selection["v2_profile_anchors"]
    all_anchors = [
        (str(item["config_id"]), str(item["role"]))
        for item in maximum_load_anchors
    ] + [("latency_anchor", "fixed_rate_latency_reference")]
    rates = discover_calibration_rates(
        calibration_results,
        expected_sample_every=latency_sample_every,
    )
    fixed_rate = 0.8 * statistics.median(rates)
    rng = random.Random(SEED)
    rows: list[dict[str, Any]] = []
    for block, (anchor_id, anchor_role) in enumerate(all_anchors, start=1):
        profile_order = list(PROFILE_IDS)
        rng.shuffle(profile_order)
        for order, profile_id in enumerate(profile_order, start=1):
            case_id = (
                f"screen_b{block:02d}_o{order:02d}_{anchor_id}_{profile_id}"
            )
            _, row = build_case(
                root=root,
                profile=profiles[profile_id],
                anchor_id=anchor_id,
                anchor_role=anchor_role,
                case_id=case_id,
                stage="profile_screening",
                block=block,
                order=order,
                latency_enabled=True,
                latency_sample_every=latency_sample_every,
                target_records_per_sec=(
                    fixed_rate if anchor_id == "latency_anchor" else None
                ),
                measurement_sec=MEASUREMENT_SEC,
            )
            rows.append(row)
    write_manifest(root / "campaigns/profile_screening.csv", rows)
    metadata = {
        "calibration_balanced_records_per_sec": rates,
        "median_max_load_balanced_records_per_sec": statistics.median(rates),
        "fixed_offered_rate_fraction": 0.8,
        "fixed_target_records_per_sec": fixed_rate,
        "latency_sample_every": latency_sample_every,
        "maximum_load_anchors": maximum_load_anchors,
        "phase1_selection_source": _display_path(phase1_selection),
        "phase1_selection_sha256": hashlib.sha256(
            phase1_selection.read_bytes()
        ).hexdigest(),
    }
    (root / "campaigns/profile_screening_metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def prepare_confirmation(
    root: Path,
    *,
    profile_ids: list[str],
    fixed_rate: float,
    latency_sample_every: int,
) -> None:
    profiles = write_profiles(root)
    maximum_load_anchors = screening_anchor_records(root)
    all_anchors = [
        (str(item["config_id"]), str(item["role"]))
        for item in maximum_load_anchors
    ] + [("latency_anchor", "fixed_rate_latency_reference")]
    selected = ["B0"] + [
        profile_id for profile_id in profile_ids if profile_id != "B0"
    ]
    if len(selected) not in {2, 3} or len(set(selected)) != len(selected):
        raise ValueError(
            "confirmation requires one or two unique candidate profiles plus B0"
        )
    unknown = sorted(set(selected) - set(PROFILE_IDS))
    if unknown:
        raise ValueError(f"unknown broker profiles: {', '.join(unknown)}")
    rng = random.Random(SEED + 1)
    rows: list[dict[str, Any]] = []
    block = 0
    for repetition in (2, 3):
        for anchor_id, anchor_role in all_anchors:
            block += 1
            profile_order = list(selected)
            rng.shuffle(profile_order)
            for order, profile_id in enumerate(profile_order, start=1):
                case_id = (
                    f"confirm_r{repetition}_b{block:02d}_o{order:02d}_"
                    f"{anchor_id}_{profile_id}"
                )
                _, row = build_case(
                    root=root,
                    profile=profiles[profile_id],
                    anchor_id=anchor_id,
                    anchor_role=anchor_role,
                    case_id=case_id,
                    stage="profile_confirmation",
                    block=block,
                    order=order,
                    latency_enabled=True,
                    latency_sample_every=latency_sample_every,
                    target_records_per_sec=(
                        fixed_rate if anchor_id == "latency_anchor" else None
                    ),
                    measurement_sec=MEASUREMENT_SEC,
                )
                rows.append(row)
    write_manifest(root / "campaigns/profile_confirmation.csv", rows)


def prepare_final_validation(
    root: Path,
    *,
    winner: str,
    confirmation_results: Path,
    workload_shortlist: Path,
    latency_sample_every: int,
) -> None:
    profiles = write_profiles(root)
    phase1_selection = load_phase1_selection(workload_shortlist)
    shortlist = phase1_selection["shortlist"]
    profile = profiles[winner]
    frozen_dir = root / "frozen"
    frozen_dir.mkdir(parents=True, exist_ok=True)
    frozen_path = frozen_dir / "frozen_broker_profile.json"
    runtime_evidence = find_freeze_runtime_evidence(
        confirmation_results,
        profile,
    )
    frozen_payload = {
        key: value
        for key, value in profile.payload.items()
        if key != "profile_sha256"
    }
    frozen_payload["source_profile_sha256"] = profile.sha256
    frozen_payload["frozen_runtime_evidence"] = runtime_evidence
    frozen_profile = write_broker_profile(frozen_path, frozen_payload)
    (frozen_dir / "frozen_broker_profile.sha256").write_text(
        frozen_profile.sha256 + "\n",
        encoding="utf-8",
    )
    selection = {
        "format": "kafka_broker_profile_selection.v2",
        "selected_profile_id": winner,
        "selected_profile_sha256": frozen_profile.sha256,
        "source_profile": relative_to_project(profile.path),
        "source_profile_sha256": profile.sha256,
        "source_confirmation_case_id": runtime_evidence["case_id"],
        "workload_shortlist_source": _display_path(workload_shortlist),
        "workload_shortlist_sha256": hashlib.sha256(
            workload_shortlist.read_bytes()
        ).hexdigest(),
        "note": (
            "The profile is the best eligible profile among B0-B5 for this "
            "single-broker GWDG campaign, not a universal Kafka optimum."
        ),
    }
    (frozen_dir / "selection.json").write_text(
        json.dumps(selection, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    rng = random.Random(SEED)
    rows: list[dict[str, Any]] = []
    for block in range(1, 6):
        config_order = list(shortlist)
        rng.shuffle(config_order)
        for order, selected in enumerate(config_order, start=1):
            anchor_id = str(selected["config_id"])
            categories = [str(item) for item in selected["selection_categories"]]
            case_id = (
                f"final_b{block:02d}_o{order:02d}_{anchor_id}_{winner}"
            )
            _, row = build_case(
                root=root,
                profile=frozen_profile,
                anchor_id=anchor_id,
                anchor_role="final_validation:" + ",".join(categories),
                case_id=case_id,
                stage="final_validation",
                block=block,
                order=order,
                latency_enabled=True,
                latency_sample_every=latency_sample_every,
                target_records_per_sec=None,
                measurement_sec=MEASUREMENT_SEC,
            )
            rows.append(row)
    write_manifest(root / "campaigns/final_validation.csv", rows)


def find_freeze_runtime_evidence(
    results_root: Path,
    profile: BrokerProfile,
) -> dict[str, Any]:
    candidates: list[tuple[Path, dict[str, Any]]] = []
    for report_path in sorted(results_root.rglob("final_report.json")):
        report = json.loads(report_path.read_text(encoding="utf-8"))
        config = report.get("config", {})
        extra = config.get("extra", {}) if isinstance(config, dict) else {}
        if (
            config.get("broker_profile_id") == profile.profile_id
            and extra.get("v2_stage") == "profile_confirmation"
            and report.get("case", {}).get("status") == "completed"
            and report.get("latency_validation", {}).get("valid") is True
        ):
            candidates.append((report_path, report))
    if not candidates:
        raise ValueError(
            f"no valid profile-confirmation report found for {profile.profile_id}"
        )

    for report_path, report in candidates:
        case_dir = (
            report_path.parent.parent
            if report_path.parent.name == "data"
            else report_path.parent
        )
        runtime_path = case_dir / "runtime/broker_runtime_manifest.json"
        snapshot_path = case_dir / "runtime/broker_profile_snapshot.json"
        config_paths = sorted(
            (case_dir / "runtime/brokers/configs").glob(
                "server-*.properties"
            )
        )
        if not runtime_path.is_file() or not snapshot_path.is_file() or not config_paths:
            continue
        runtime = json.loads(runtime_path.read_text(encoding="utf-8"))
        snapshot = load_broker_profile(snapshot_path)
        if (
            runtime.get("profile_id") != profile.profile_id
            or runtime.get("profile_sha256") != profile.sha256
            or snapshot.sha256 != profile.sha256
        ):
            continue
        runtime_hashes = {
            Path(str(record.get("path", ""))).name: str(record.get("sha256", ""))
            for record in runtime.get("server_properties", [])
            if isinstance(record, dict)
        }
        generated_properties = []
        hashes_valid = True
        for config_path in config_paths:
            digest = hashlib.sha256(config_path.read_bytes()).hexdigest()
            if runtime_hashes.get(config_path.name) != digest:
                hashes_valid = False
                break
            generated_properties.append(
                {
                    "filename": config_path.name,
                    "sha256": digest,
                    "content": config_path.read_text(encoding="utf-8"),
                }
            )
        if not hashes_valid:
            continue
        correctness = report.get("aggregated_metrics", {}).get(
            "record_correctness",
            {},
        )
        producers = report.get("aggregated_metrics", {}).get("producers", {})
        if any(
            int(correctness.get(field, -1)) != 0
            for field in (
                "missing_after_drain_records",
                "unexplained_surplus_records",
                "invalid_envelope_count",
                "duplicate_offset_count",
                "out_of_order_offset_count",
            )
        ) or int(producers.get("flush_remaining_messages", -1)) != 0:
            continue
        return {
            "case_id": report.get("case", {}).get("case_id")
            or report.get("config", {}).get("case_id"),
            "runtime_manifest_sha256": hashlib.sha256(
                runtime_path.read_bytes()
            ).hexdigest(),
            "java_version": runtime.get("java_version"),
            "kafka_version": runtime.get("kafka_version"),
            "jvm_command_contract": runtime.get("jvm_command_contract"),
            "generated_server_properties": generated_properties,
        }

    raise ValueError(
        f"no complete, hash-valid confirmation runtime found for {profile.profile_id}"
    )


def main() -> None:
    args = parse_args()
    root = Path(args.root)
    if not root.is_absolute():
        root = project_root() / root
    root.mkdir(parents=True, exist_ok=True)

    if args.command == "initialize":
        initialize(root)
    elif args.command == "prepare-rate-calibration":
        prepare_rate_calibration(
            root,
            latency_sample_every=args.latency_sample_every,
        )
    elif args.command == "prepare-screening":
        prepare_screening(
            root,
            calibration_results=Path(args.calibration_results),
            phase1_selection=Path(args.phase1_selection),
            latency_sample_every=args.latency_sample_every,
        )
    elif args.command == "prepare-confirmation":
        profile_ids = [
            value.strip()
            for value in args.profiles.split(",")
            if value.strip()
        ]
        prepare_confirmation(
            root,
            profile_ids=profile_ids,
            fixed_rate=args.target_records_per_sec,
            latency_sample_every=args.latency_sample_every,
        )
    elif args.command == "prepare-final-validation":
        prepare_final_validation(
            root,
            winner=args.winner,
            confirmation_results=Path(args.confirmation_results),
            workload_shortlist=Path(args.workload_shortlist),
            latency_sample_every=args.latency_sample_every,
        )
    else:
        raise AssertionError(f"unsupported command: {args.command}")
    write_campaign_checksums(root)


def write_campaign_checksums(root: Path) -> None:
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


if __name__ == "__main__":
    main()
