#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import csv
import json
import random
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from models.benchmark_config import BenchmarkConfig, benchmark_config_input_dict
from src.benchmark.core.config_schema import normalize_case_config


DEFAULT_TEMPLATE = (
    PROJECT_ROOT
    / "configs"
    / "campaigns"
    / "pulsar"
    / "pilots"
    / "corrected"
    / "baseline_h16_d32_case.json"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "configs" / "campaigns" / "pulsar"
PROFILE_ROOT = PROJECT_ROOT / "configs" / "backends" / "pulsar" / "profiles"
DEFAULT_PHASE1_GATE = (
    PROJECT_ROOT
    / "results"
    / "published"
    / "pulsar"
    / "phase1-gate"
    / "acceptance_report.json"
)
PULSAR_CLIENT_VERSION = "3.13.0"
MEASUREMENT_CONTRACT_ID = "measurement.messaging.reproducible.v1"
QUALIFICATION_POLICY_ID = "qualification.application.v1"

PRODUCER_RANKS = [16, 24, 32, 40, 48, 56, 64]
CONSUMER_RANKS = [16, 24, 32, 40, 48, 56, 64]
PARTITIONS = [60, 90, 120, 180, 240]
PAYLOAD_SIZES = [1_024, 2_048, 4_096, 8_192, 16_384]
BATCHING_MAX_MESSAGES = [250, 500, 1_000, 2_000, 4_000]
BATCHING_MAX_BYTES = [262_144, 524_288, 1_048_576, 2_097_152, 4_194_304]
BATCHING_DELAYS_MS = [1, 5, 10, 20, 40, 80]
PENDING_MESSAGES = [500, 1_000, 2_000]
PENDING_MESSAGES_ACROSS_PARTITIONS = [10_000, 20_000, 40_000]
RECEIVER_QUEUE_SIZES = [250, 500, 1_000, 2_000]
TOTAL_RECEIVER_QUEUE_SIZES = [30_000, 60_000, 120_000, 240_000]

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

MANIFEST_FIELDS = [
    "block",
    "order",
    "stage",
    "case_id",
    "config_id",
    "config_path",
    "profile_id",
    "anchor",
    "latency_enabled",
    "latency_sample_every",
    *VARIANT_FIELDS,
    "design_note",
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate gated Pulsar acceptance pilots and the 120-case Phase 1 "
            "screening design without submitting any job."
        )
    )
    parser.add_argument("--template", default=str(DEFAULT_TEMPLATE))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--seed", type=int, default=20260806)
    parser.add_argument("--phase1-size", type=int, default=120)
    parser.add_argument("--phase1-latency-sample-every", type=int, default=10)
    parser.add_argument("--phase1-warmup-sec", type=int, default=15)
    parser.add_argument("--phase1-duration-sec", type=int, default=30)
    parser.add_argument("--phase1-drain-timeout-sec", type=int, default=60)
    parser.add_argument("--phase1-batch-size", type=int, default=30)
    parser.add_argument(
        "--phase1-gate-report",
        default=str(DEFAULT_PHASE1_GATE),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.phase1_size <= 0:
        raise SystemExit("--phase1-size must be greater than 0")
    if args.phase1_latency_sample_every <= 0:
        raise SystemExit(
            "--phase1-latency-sample-every must be greater than 0"
        )
    if args.phase1_warmup_sec < 0:
        raise SystemExit("--phase1-warmup-sec must not be negative")
    if args.phase1_duration_sec <= 0:
        raise SystemExit("--phase1-duration-sec must be greater than 0")
    if args.phase1_drain_timeout_sec < 0:
        raise SystemExit("--phase1-drain-timeout-sec must not be negative")
    if args.phase1_batch_size <= 0:
        raise SystemExit("--phase1-batch-size must be greater than 0")

    template = _read_json(Path(args.template))
    output_root = Path(args.output_root)
    pilot_counts = generate_acceptance_pilots(
        template=template,
        output_root=output_root,
        seed=args.seed,
    )
    phase1_rows = generate_phase1(
        template=template,
        output_root=output_root,
        seed=args.seed,
        max_configs=args.phase1_size,
        latency_sample_every=args.phase1_latency_sample_every,
        warmup_sec=args.phase1_warmup_sec,
        duration_sec=args.phase1_duration_sec,
        drain_timeout_sec=args.phase1_drain_timeout_sec,
        batch_size=args.phase1_batch_size,
        gate_report=Path(args.phase1_gate_report),
    )
    _write_phase2_gate(output_root / "phase2", seed=args.seed)

    print(
        "[pulsar-generate] instrumentation pilot cases: "
        f"{pilot_counts['instrumentation']}"
    )
    print(
        "[pulsar-generate] optional memory-comparison cases: "
        f"{pilot_counts['memory']}"
    )
    print(f"[pulsar-generate] Phase 1 cases: {len(phase1_rows)}")
    phase1_plan = _read_json(output_root / "phase1" / "phase1_campaign_plan.json")
    status = "accepted" if phase1_plan["ready_for_submission"] else "gated"
    print(f"[pulsar-generate] submission status: {status}")
    return 0


def generate_acceptance_pilots(
    *,
    template: dict[str, Any],
    output_root: Path,
    seed: int,
) -> dict[str, int]:
    output_dir = output_root / "pilots" / "corrected"
    generated_dir = output_dir / "generated_configs"
    generated_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)

    instrumentation_rows: list[dict[str, Any]] = []
    for block in range(1, 4):
        treatments = [False, True]
        rng.shuffle(treatments)
        for order, latency_enabled in enumerate(treatments, start=1):
            suffix = "latency-on" if latency_enabled else "latency-off"
            case_id = f"pulsar-inst-b{block:02d}-{suffix}"
            config = _build_case(
                template,
                case_id=case_id,
                campaign_id="pulsar-instrumentation-overhead-pilot",
                profile_id="BASELINE_H16_D32",
                metadata={
                    "stage": "instrumentation-overhead",
                    "block": block,
                    "latency_enabled": latency_enabled,
                    "acceptance_rule": (
                        "Median balanced-throughput overhead must not exceed 3%"
                    ),
                },
            )
            workload = _workload(config)
            workload["latency_enabled"] = latency_enabled
            workload["latency_sample_every"] = 10
            path = generated_dir / f"{case_id}.json"
            _write_case(path, config)
            instrumentation_rows.append(
                _manifest_row(
                    config,
                    block=block,
                    order=order,
                    stage="instrumentation-overhead",
                    config_id=case_id,
                    config_path=path,
                    anchor="moderate",
                    design_note="paired latency instrumentation A/B",
                )
            )

    memory_rows: list[dict[str, Any]] = []
    anchors = {
        "moderate": {},
        "stress": {
            "producer_ranks": 64,
            "consumer_ranks": 64,
            "partitions": 240,
        },
    }
    for block in range(1, 4):
        treatments = [
            (profile_id, anchor, variant)
            for profile_id in ("P1", "BASELINE_H16_D32")
            for anchor, variant in anchors.items()
        ]
        rng.shuffle(treatments)
        for order, (profile_id, anchor, variant) in enumerate(
            treatments,
            start=1,
        ):
            profile_slug = "h8-d32" if profile_id == "P1" else "h16-d32"
            case_id = (
                f"pulsar-memory-b{block:02d}-{anchor}-{profile_slug}"
            )
            config = _build_case(
                template,
                case_id=case_id,
                campaign_id="pulsar-memory-acceptance-pilot",
                profile_id=profile_id,
                metadata={
                    "stage": "memory-acceptance",
                    "block": block,
                    "anchor": anchor,
                    "comparison": "8-GiB versus 16-GiB heap at 32-GiB direct memory",
                },
            )
            _apply_variant(config, variant)
            _workload(config)["latency_sample_every"] = 10
            path = generated_dir / f"{case_id}.json"
            _write_case(path, config)
            memory_rows.append(
                _manifest_row(
                    config,
                    block=block,
                    order=order,
                    stage="memory-acceptance",
                    config_id=case_id,
                    config_path=path,
                    anchor=anchor,
                    design_note="paired immutable memory-profile comparison",
                )
            )

    _write_manifest(
        output_dir / "instrumentation_overhead_manifest.csv",
        instrumentation_rows,
    )
    _write_manifest(
        output_dir / "memory_acceptance_manifest.csv",
        memory_rows,
    )
    _write_json(
        output_dir / "pilot_gate.json",
        {
            "format": "messaging-benchmark.pulsar-pilot-design.v1",
            "seed": seed,
            "authoritative_for_phase1_submission": False,
            "status": "superseded_by_published_phase1_gate",
            "published_phase1_gate": (
                "results/published/pulsar/phase1-gate/acceptance_report.json"
            ),
            "originally_planned_sequence": [
                "instrumentation_overhead_manifest.csv",
                "memory_acceptance_manifest.csv",
            ],
            "acceptance": {
                "all_cases_complete": True,
                "backend_health_valid": True,
                "missing_after_drain_records": 0,
                "invalid_envelopes": 0,
                "duplicate_records": 0,
                "negative_latency_samples": 0,
                "latency_validation_valid_when_enabled": True,
                "failed_send_percent_max": 0.1,
                "flush_duration_sec_max": 10.0,
                "instrumentation_median_throughput_overhead_percent_max": 3.0,
            },
            "note": (
                "These deterministic pilot inputs are retained for reproducibility. "
                "The memory comparison was not executed and is optional; Phase 1 "
                "authorization comes only from the published gate, which combines "
                "the completed instrumentation A/B and accepted 10-case batch run."
            ),
        },
    )
    _assert_no_stale_configs(
        generated_dir,
        {Path(row["config_path"]).name for row in instrumentation_rows + memory_rows},
    )
    return {
        "instrumentation": len(instrumentation_rows),
        "memory": len(memory_rows),
    }


def generate_phase1(
    *,
    template: dict[str, Any],
    output_root: Path,
    seed: int,
    max_configs: int,
    latency_sample_every: int,
    warmup_sec: int,
    duration_sec: int,
    drain_timeout_sec: int,
    batch_size: int,
    gate_report: Path,
) -> list[dict[str, Any]]:
    output_dir = output_root / "phase1"
    generated_dir = output_dir / "generated_configs"
    batches_dir = output_dir / "batches"
    generated_dir.mkdir(parents=True, exist_ok=True)
    batches_dir.mkdir(parents=True, exist_ok=True)

    base = _build_case(
        template,
        case_id="pulsar-cfg-001",
        campaign_id="pulsar-phase1-screening",
        profile_id="BASELINE_H16_D32",
        metadata={
            "stage": "phase1-screening",
            "design": "budgeted baseline, OFAT, rank-pair, and seeded mixed design",
        },
    )
    workload = _workload(base)
    workload.update(
        {
            "warmup_sec": warmup_sec,
            "duration_sec": duration_sec,
            "drain_timeout_sec": drain_timeout_sec,
            "latency_enabled": True,
            "latency_sample_every": latency_sample_every,
        }
    )

    candidates: list[tuple[dict[str, Any], str]] = [
        (copy.deepcopy(base), "baseline")
    ]
    baseline_values = _variant_values(base)
    one_factor_specs = [
        ("producer_ranks", PRODUCER_RANKS),
        ("consumer_ranks", CONSUMER_RANKS),
        ("partitions", PARTITIONS),
        ("payload_size_bytes", PAYLOAD_SIZES),
        ("batching_max_messages", BATCHING_MAX_MESSAGES),
        ("batching_max_bytes", BATCHING_MAX_BYTES),
        ("batching_max_publish_delay_ms", BATCHING_DELAYS_MS),
        ("max_pending_messages", PENDING_MESSAGES),
        (
            "max_pending_messages_across_partitions",
            PENDING_MESSAGES_ACROSS_PARTITIONS,
        ),
        ("receiver_queue_size", RECEIVER_QUEUE_SIZES),
        (
            "max_total_receiver_queue_size_across_partitions",
            TOTAL_RECEIVER_QUEUE_SIZES,
        ),
    ]
    for field_name, values in one_factor_specs:
        for value in values:
            if baseline_values[field_name] == value:
                continue
            config = copy.deepcopy(base)
            _apply_variant(config, {field_name: value})
            candidates.append(
                (config, f"one-factor:{field_name}={value}")
            )

    for producer_ranks, consumer_ranks in (
        (24, 24),
        (32, 32),
        (48, 48),
        (56, 56),
        (64, 64),
        (24, 48),
        (32, 56),
        (48, 32),
        (56, 24),
        (64, 40),
        (40, 64),
    ):
        config = copy.deepcopy(base)
        _apply_variant(
            config,
            {
                "producer_ranks": producer_ranks,
                "consumer_ranks": consumer_ranks,
            },
        )
        candidates.append(
            (config, f"rank-pair:{producer_ranks}x{consumer_ranks}")
        )

    rng = random.Random(seed)
    while len(candidates) < max_configs * 4:
        config = copy.deepcopy(base)
        _apply_variant(
            config,
            {
                "producer_ranks": rng.choice(PRODUCER_RANKS),
                "consumer_ranks": rng.choice(CONSUMER_RANKS),
                "partitions": rng.choice(PARTITIONS),
                "payload_size_bytes": rng.choice(PAYLOAD_SIZES),
                "batching_max_messages": rng.choice(BATCHING_MAX_MESSAGES),
                "batching_max_bytes": rng.choice(BATCHING_MAX_BYTES),
                "batching_max_publish_delay_ms": rng.choice(
                    BATCHING_DELAYS_MS
                ),
                "max_pending_messages": rng.choice(PENDING_MESSAGES),
                "max_pending_messages_across_partitions": rng.choice(
                    PENDING_MESSAGES_ACROSS_PARTITIONS
                ),
                "receiver_queue_size": rng.choice(RECEIVER_QUEUE_SIZES),
                "max_total_receiver_queue_size_across_partitions": rng.choice(
                    TOTAL_RECEIVER_QUEUE_SIZES
                ),
            },
        )
        candidates.append((config, "seeded-mixed"))

    config_rows: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for config, note in candidates:
        if len(config_rows) >= max_configs:
            break
        key = tuple(_variant_values(config)[name] for name in VARIANT_FIELDS)
        if key in seen:
            continue
        seen.add(key)
        index = len(config_rows) + 1
        config_id = f"cfg_{index:03d}"
        case_id = f"pulsar-{config_id.replace('_', '-')}"
        config = copy.deepcopy(config)
        _set_case_identity(
            config,
            case_id=case_id,
            campaign_id="pulsar-phase1-screening",
            metadata={
                "stage": "phase1-screening",
                "config_id": config_id,
                "design_note": note,
                "seed": seed,
            },
        )
        path = generated_dir / f"{config_id}.json"
        _write_case(path, config)
        config_rows.append(
            _manifest_row(
                config,
                block=1,
                order=index,
                stage="phase1-screening",
                config_id=config_id,
                config_path=path,
                anchor="",
                design_note=note,
            )
        )

    if len(config_rows) != max_configs:
        raise RuntimeError(
            f"Generated {len(config_rows)} unique Phase 1 cases, expected {max_configs}"
        )

    execution_rows = [dict(row) for row in config_rows]
    random.Random(seed + 1).shuffle(execution_rows)
    for order, row in enumerate(execution_rows, start=1):
        row["order"] = order
    _write_manifest(output_dir / "phase1_manifest.csv", execution_rows)

    written_batch_sizes: list[int] = []
    expected_batch_names: set[str] = set()
    for batch_index, offset in enumerate(
        range(0, len(execution_rows), batch_size),
        start=1,
    ):
        batch = [
            dict(row)
            for row in execution_rows[offset : offset + batch_size]
        ]
        for order, row in enumerate(batch, start=1):
            row["block"] = batch_index
            row["order"] = order
        batch_name = f"batch_{batch_index:02d}.csv"
        _write_manifest(
            batches_dir / batch_name,
            batch,
        )
        expected_batch_names.add(batch_name)
        written_batch_sizes.append(len(batch))
    for stale_batch in batches_dir.glob("batch_*.csv"):
        if stale_batch.name not in expected_batch_names:
            stale_batch.unlink()

    profile = _profile("BASELINE_H16_D32")
    phase1_ready, gate_reason = _phase1_gate_status(gate_report, profile)
    runtime = _object(_object(profile, "settings"), "runtime")

    _write_json(
        output_dir / "phase1_campaign_plan.json",
        {
            "format": "messaging-benchmark.pulsar-phase1-plan.v1",
            "seed": seed,
            "profile_id": "BASELINE_H16_D32",
            "profile_sha256": profile["sha256"],
            "runtime_provenance": {
                "pulsar_product_version": profile["product_version"],
                "pulsar_python_client_version": PULSAR_CLIENT_VERSION,
                "java_major": runtime["java_major"],
                "jvm_memory": runtime["jvm_memory"],
                "service_mode": _object(profile, "settings")["service_mode"],
                "storage": runtime["storage"],
            },
            "configuration_count": len(config_rows),
            "design_counts": {
                "baseline": 1,
                "one_factor": sum(
                    row["design_note"].startswith("one-factor:")
                    for row in config_rows
                ),
                "rank_pair": sum(
                    row["design_note"].startswith("rank-pair:")
                    for row in config_rows
                ),
                "seeded_mixed": sum(
                    row["design_note"] == "seeded-mixed"
                    for row in config_rows
                ),
            },
            "batch_sizes": written_batch_sizes,
            "batch_count": len(written_batch_sizes),
            "batch_size_limit": batch_size,
            "execution_order_seed": seed + 1,
            "timing_sec": {
                "warmup": warmup_sec,
                "measurement": duration_sec,
                "drain": drain_timeout_sec,
            },
            "latency_sample_every": latency_sample_every,
            "ready_for_submission": phase1_ready,
            "submission_gate": _project_relative(gate_report),
            "submission_gate_status": gate_reason,
            "note": (
                "The 30-second measurement window matches the Kafka V1 screening "
                "window. Pulsar adds an excluded warm-up and bounded coordinated "
                "drain for correctness and latency validation. Submit batches "
                "sequentially and validate each completed batch before the next."
            ),
        },
    )
    _assert_no_stale_configs(
        generated_dir,
        {Path(row["config_path"]).name for row in config_rows},
    )
    return config_rows


def _phase1_gate_status(
    gate_report: Path,
    profile: dict[str, Any],
) -> tuple[bool, str]:
    if not gate_report.is_file():
        return False, "acceptance report missing"
    try:
        report = _read_json(gate_report)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return False, f"acceptance report invalid: {exc}"
    accepted_profile = report.get("profile")
    if not isinstance(accepted_profile, dict):
        return False, "accepted profile identity missing"
    checks = (
        report.get("format") == "messaging-benchmark.pulsar-phase1-gate.v1",
        report.get("phase1_submission_authorized") is True,
        accepted_profile.get("profile_id") == profile.get("profile_id"),
        accepted_profile.get("profile_sha256") == profile.get("sha256"),
    )
    if not all(checks):
        return False, "acceptance report does not authorize this profile"
    return True, "accepted"


def _write_phase2_gate(output_dir: Path, *, seed: int) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    plan_path = output_dir / "phase2_campaign_plan.json"
    if plan_path.is_file():
        existing = _read_json(plan_path)
        if existing.get("status") != "blocked_pending_phase1":
            return
    _write_json(
        plan_path,
        {
            "format": "messaging-benchmark.pulsar-phase2-plan.v1",
            "seed": seed,
            "status": "blocked_pending_phase1",
            "configuration_count": 0,
            "prerequisites": [
                "accepted published Phase 1 submission gate",
                "120 complete Phase 1 screening cases",
                "Phase 1 qualification-first report",
                "data-driven workload shortlist",
            ],
            "fixed_during_phase2": [
                "workload definition within each selected anchor",
                "Pulsar product version",
                "single standalone service topology",
                "qualification and eligibility rules",
            ],
            "varied_during_phase2": [
                "explicit immutable Pulsar broker/JVM profiles only"
            ],
            "note": (
                "Phase 2 settings must be chosen from Phase 1 bottleneck evidence; "
                "no broker-tuning cases are generated speculatively."
            ),
        },
    )


def _build_case(
    template: dict[str, Any],
    *,
    case_id: str,
    campaign_id: str,
    profile_id: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    config = copy.deepcopy(template)
    _apply_profile(config, profile_id)
    _set_case_identity(
        config,
        case_id=case_id,
        campaign_id=campaign_id,
        metadata=metadata,
    )
    return config


def _set_case_identity(
    config: dict[str, Any],
    *,
    case_id: str,
    campaign_id: str,
    metadata: dict[str, Any],
) -> None:
    settings = _pulsar(config)
    slug = case_id.lower().replace("_", "-")
    settings["topic_name"] = f"persistent://public/default/{slug}"
    settings["subscription_name"] = f"messaging-benchmark-{slug}"
    common_metadata = {
        **copy.deepcopy(metadata),
        "measurement_contract_id": MEASUREMENT_CONTRACT_ID,
        "backlog_event": "pending producer delivery callbacks at flush start",
        "backlog_denominator": "measurement-period send attempts",
    }
    config["qualification_policy_id"] = QUALIFICATION_POLICY_ID
    config["campaign"] = {
        "case_id": case_id,
        "campaign_id": campaign_id,
        "metadata": common_metadata,
    }


def _apply_profile(config: dict[str, Any], profile_id: str) -> None:
    profile = _profile(profile_id)
    settings = _pulsar(config)
    profile_settings = profile["settings"]
    for name in (
        "service_count",
        "service_mode",
        "managed_ledger",
        "standalone_properties",
        "runtime",
    ):
        settings[name] = copy.deepcopy(profile_settings[name])
    settings["profile_id"] = profile_id
    settings["profile_sha256"] = profile["sha256"]


def _profile(profile_id: str) -> dict[str, Any]:
    profile = _read_json(PROFILE_ROOT / f"{profile_id}.json")
    if profile.get("profile_id") != profile_id:
        raise ValueError(f"Profile ID mismatch in {profile_id}.json")
    if not isinstance(profile.get("settings"), dict):
        raise ValueError(f"Profile {profile_id} has no settings object")
    return profile


def _variant_values(config: dict[str, Any]) -> dict[str, int]:
    workload = _workload(config)
    settings = _pulsar(config)
    producer = _object(settings, "producer")
    consumer = _object(settings, "consumer")
    return {
        "producer_ranks": int(workload["producer_ranks"]),
        "consumer_ranks": int(workload["consumer_ranks"]),
        "partitions": int(settings["partitions"]),
        "payload_size_bytes": int(workload["payload_size_bytes"]),
        "batching_max_messages": int(producer["batching_max_messages"]),
        "batching_max_bytes": int(producer["batching_max_bytes"]),
        "batching_max_publish_delay_ms": int(
            producer["batching_max_publish_delay_ms"]
        ),
        "max_pending_messages": int(producer["max_pending_messages"]),
        "max_pending_messages_across_partitions": int(
            producer["max_pending_messages_across_partitions"]
        ),
        "receiver_queue_size": int(consumer["receiver_queue_size"]),
        "max_total_receiver_queue_size_across_partitions": int(
            consumer["max_total_receiver_queue_size_across_partitions"]
        ),
    }


def _apply_variant(config: dict[str, Any], values: dict[str, int]) -> None:
    workload = _workload(config)
    settings = _pulsar(config)
    producer = _object(settings, "producer")
    consumer = _object(settings, "consumer")
    for name, value in values.items():
        if name in {"producer_ranks", "consumer_ranks", "payload_size_bytes"}:
            workload[name] = int(value)
        elif name == "partitions":
            settings[name] = int(value)
        elif name in {
            "batching_max_messages",
            "batching_max_bytes",
            "batching_max_publish_delay_ms",
            "max_pending_messages",
            "max_pending_messages_across_partitions",
        }:
            producer[name] = int(value)
        elif name in {
            "receiver_queue_size",
            "max_total_receiver_queue_size_across_partitions",
        }:
            consumer[name] = int(value)
        else:
            raise KeyError(name)


def _manifest_row(
    config: dict[str, Any],
    *,
    block: int,
    order: int,
    stage: str,
    config_id: str,
    config_path: Path,
    anchor: str,
    design_note: str,
) -> dict[str, Any]:
    values = _variant_values(config)
    workload = _workload(config)
    settings = _pulsar(config)
    return {
        "block": block,
        "order": order,
        "stage": stage,
        "case_id": config["campaign"]["case_id"],
        "config_id": config_id,
        "config_path": _project_relative(config_path),
        "profile_id": settings["profile_id"],
        "anchor": anchor,
        "latency_enabled": str(bool(workload["latency_enabled"])).lower(),
        "latency_sample_every": int(workload["latency_sample_every"]),
        **values,
        "design_note": design_note,
    }


def _write_manifest(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _write_case(path: Path, config: dict[str, Any]) -> None:
    normalized = normalize_case_config(config)
    effective = BenchmarkConfig(**benchmark_config_input_dict(normalized))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(effective.to_versioned_dict(), indent=2) + "\n",
        encoding="utf-8",
    )


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def _workload(config: dict[str, Any]) -> dict[str, Any]:
    return _object(config, "workload")


def _pulsar(config: dict[str, Any]) -> dict[str, Any]:
    return _object(_object(config, "backend"), "pulsar")


def _object(parent: dict[str, Any], name: str) -> dict[str, Any]:
    value = parent.get(name)
    if not isinstance(value, dict):
        raise ValueError(f"Expected object at {name}")
    return value


def _project_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path)


def _assert_no_stale_configs(
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
            f"Stale generated config(s) in {generated_dir}: " + ", ".join(stale)
        )


if __name__ == "__main__":
    raise SystemExit(main())
