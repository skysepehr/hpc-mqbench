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
from src.benchmark.qualification import COMMON_QUALIFICATION_POLICY_ID

DEFAULT_BASELINE = PROJECT_ROOT / "configs" / "one_broker_mpi_simultaneous.json"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "configs" / "sweeps" / "simultaneous_budgeted"


PRODUCER_RANKS = [16, 24, 32, 40, 48, 56, 64]
CONSUMER_RANKS = [16, 24, 32, 40, 48, 56, 64]
PARTITIONS = [60, 90, 120, 180, 240]
BATCH_SIZES = [262_144, 524_288, 1_048_576, 2_097_152, 4_194_304]
LINGER_MS = [0, 5, 10, 20, 40, 80]
PAYLOAD_SIZES = [1_024, 2_048, 4_096, 8_192, 16_384]
PRODUCER_QUEUE_MESSAGES = [500_000, 1_000_000, 2_000_000]
PRODUCER_QUEUE_KBYTES = [524_288, 1_048_576, 2_097_152]
CONSUMER_FETCH_MIN_BYTES = [262_144, 1_048_576, 2_097_152, 4_194_304, 8_388_608]
CONSUMER_FETCH_WAIT_MS = [10, 25, 50, 100]
CONSUMER_FETCH_MAX_BYTES = [4_194_304, 8_388_608, 16_777_216, 33_554_432]


MANIFEST_FIELDS = [
    "config_id",
    "config_path",
    "topic_name",
    "scenario",
    "partitions",
    "producer_ranks",
    "consumer_ranks",
    "batch_size",
    "linger_ms",
    "payload_size_bytes",
    "producer_queue_messages",
    "producer_queue_kbytes",
    "consumer_fetch_min_bytes",
    "consumer_fetch_wait_max_ms",
    "consumer_fetch_message_max_bytes",
    "warmup_sec",
    "measurement_sec",
    "drain_timeout_sec",
    "latency_enabled",
    "latency_sample_every",
    "qualification_policy_id",
    "measurement_contract_id",
    "broker_profile_id",
    "broker_profile_sha256",
    "notes",
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate a budgeted simultaneous-only Kafka/MPI sweep."
    )
    parser.add_argument("--baseline", default=str(DEFAULT_BASELINE))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--max-configs", type=int, default=120)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--campaign-id", default="simultaneous_budgeted")
    parser.add_argument("--topic-prefix", default="sweep-sim")
    parser.add_argument("--warmup-sec", type=int, default=0)
    parser.add_argument("--measurement-sec", type=int, default=30)
    parser.add_argument("--drain-timeout-sec", type=int, default=0)
    parser.add_argument(
        "--latency-enabled",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument("--latency-sample-every", type=int, default=1)
    parser.add_argument(
        "--qualification-policy-id",
        default="qualification.kafka.v1",
    )
    parser.add_argument("--measurement-contract-id", default="historical.kafka.v1")
    args = parser.parse_args(argv)

    if args.max_configs <= 0:
        raise SystemExit("--max-configs must be positive")

    baseline_path = Path(args.baseline)
    output_dir = Path(args.output_dir)
    rows = generate_sweep(
        baseline_path=baseline_path,
        output_dir=output_dir,
        max_configs=args.max_configs,
        seed=args.seed,
        campaign_id=args.campaign_id,
        topic_prefix=args.topic_prefix,
        warmup_sec=args.warmup_sec,
        measurement_sec=args.measurement_sec,
        drain_timeout_sec=args.drain_timeout_sec,
        latency_enabled=args.latency_enabled,
        latency_sample_every=args.latency_sample_every,
        qualification_policy_id=args.qualification_policy_id,
        measurement_contract_id=args.measurement_contract_id,
    )
    manifest_path = output_dir / "sweep_manifest.csv"
    print(f"[sweep-generate] configs: {len(rows)}")
    print(f"[sweep-generate] manifest: {_project_relative(manifest_path)}")
    return 0


def generate_sweep(
    baseline_path: Path,
    output_dir: Path,
    max_configs: int,
    seed: int,
    campaign_id: str = "simultaneous_budgeted",
    topic_prefix: str = "sweep-sim",
    warmup_sec: int = 0,
    measurement_sec: int = 30,
    drain_timeout_sec: int = 0,
    latency_enabled: bool = False,
    latency_sample_every: int = 1,
    qualification_policy_id: str = "qualification.kafka.v1",
    measurement_contract_id: str = "historical.kafka.v1",
) -> list[dict[str, Any]]:
    baseline_path = baseline_path.resolve()
    with baseline_path.open("r", encoding="utf-8") as handle:
        baseline = json.load(handle)
    if not isinstance(baseline, dict):
        raise ValueError(f"Baseline config must be a JSON object: {baseline_path}")

    generated_dir = output_dir / "generated_configs"
    batches_dir = output_dir / "batches"
    generated_dir.mkdir(parents=True, exist_ok=True)
    batches_dir.mkdir(parents=True, exist_ok=True)

    candidates: list[tuple[dict[str, Any], str]] = []
    base = _normalize_config(
        copy.deepcopy(baseline),
        warmup_sec=warmup_sec,
        measurement_sec=measurement_sec,
        drain_timeout_sec=drain_timeout_sec,
        latency_enabled=latency_enabled,
        latency_sample_every=latency_sample_every,
        qualification_policy_id=qualification_policy_id,
    )
    candidates.append((base, "baseline"))

    baseline_values = _variant_values(base)
    one_factor_specs = [
        ("producer_ranks", PRODUCER_RANKS),
        ("consumer_ranks", CONSUMER_RANKS),
        ("partitions", PARTITIONS),
        ("batch_size", BATCH_SIZES),
        ("linger_ms", LINGER_MS),
        ("payload_size_bytes", PAYLOAD_SIZES),
        ("producer_queue_messages", PRODUCER_QUEUE_MESSAGES),
        ("producer_queue_kbytes", PRODUCER_QUEUE_KBYTES),
        ("consumer_fetch_min_bytes", CONSUMER_FETCH_MIN_BYTES),
        ("consumer_fetch_wait_max_ms", CONSUMER_FETCH_WAIT_MS),
        ("consumer_fetch_message_max_bytes", CONSUMER_FETCH_MAX_BYTES),
    ]
    for field_name, values in one_factor_specs:
        for value in values:
            if baseline_values.get(field_name) == value:
                continue
            cfg = copy.deepcopy(base)
            _apply_variant(cfg, {field_name: value})
            candidates.append((cfg, f"one-factor:{field_name}={value}"))

    for producer_ranks, consumer_ranks in [
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
    ]:
        cfg = copy.deepcopy(base)
        _apply_variant(
            cfg,
            {
                "producer_ranks": producer_ranks,
                "consumer_ranks": consumer_ranks,
            },
        )
        candidates.append((cfg, f"rank-pair:{producer_ranks}x{consumer_ranks}"))

    rng = random.Random(seed)
    while len(candidates) < max_configs * 4:
        variant = {
            "producer_ranks": rng.choice(PRODUCER_RANKS),
            "consumer_ranks": rng.choice(CONSUMER_RANKS),
            "partitions": rng.choice(PARTITIONS),
            "batch_size": rng.choice(BATCH_SIZES),
            "linger_ms": rng.choice(LINGER_MS),
            "payload_size_bytes": rng.choice(PAYLOAD_SIZES),
            "producer_queue_messages": rng.choice(PRODUCER_QUEUE_MESSAGES),
            "producer_queue_kbytes": rng.choice(PRODUCER_QUEUE_KBYTES),
            "consumer_fetch_min_bytes": rng.choice(CONSUMER_FETCH_MIN_BYTES),
            "consumer_fetch_wait_max_ms": rng.choice(CONSUMER_FETCH_WAIT_MS),
            "consumer_fetch_message_max_bytes": rng.choice(CONSUMER_FETCH_MAX_BYTES),
        }
        cfg = copy.deepcopy(base)
        _apply_variant(cfg, variant)
        candidates.append((cfg, "seeded-mixed"))

    rows: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for cfg, note in candidates:
        if len(rows) >= max_configs:
            break
        key = _dedupe_key(cfg)
        if key in seen:
            continue
        seen.add(key)
        config_id = f"cfg_{len(rows) + 1:03d}"
        cfg = copy.deepcopy(cfg)
        cfg["topic_name"] = f"{topic_prefix}-{config_id.replace('_', '-')}"
        cfg["case_id"] = config_id
        cfg["campaign_id"] = campaign_id
        extra = cfg.setdefault("extra", {})
        if isinstance(extra, dict):
            extra["sweep_id"] = campaign_id
            extra["sweep_config_id"] = config_id
            extra["purpose"] = (
                "reproducible instrumented simultaneous screening sweep"
                if qualification_policy_id == COMMON_QUALIFICATION_POLICY_ID
                else "budgeted simultaneous-only screening sweep"
            )
            extra["measured_phase"] = "simultaneous"
            extra["measured_roles"] = ["producer", "consumer"]
            extra["campaign_metadata"] = {
                "measurement_contract_id": measurement_contract_id,
                "backlog_event": "pending delivery callbacks at flush start",
                "backlog_denominator": (
                    "measurement-period send attempts"
                    if qualification_policy_id == COMMON_QUALIFICATION_POLICY_ID
                    else "successfully enqueued records"
                ),
                "sweep_seed": seed,
            }
        config_path = generated_dir / f"{config_id}.json"
        _write_json(config_path, cfg)
        rows.append(_manifest_row(config_id, config_path, cfg, note))

    manifest_path = output_dir / "sweep_manifest.csv"
    with manifest_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    return rows


def _normalize_config(
    config: dict[str, Any],
    *,
    warmup_sec: int = 0,
    measurement_sec: int = 30,
    drain_timeout_sec: int = 0,
    latency_enabled: bool = False,
    latency_sample_every: int = 1,
    qualification_policy_id: str = "qualification.kafka.v1",
) -> dict[str, Any]:
    config["mode"] = "single"
    config["scenario"] = "simultaneous"
    config["broker_count"] = 1
    config["replication_factor"] = 1
    config["acks"] = "1"
    config["compression_type"] = "none"
    config["duration_sec"] = measurement_sec
    config["warmup_sec"] = warmup_sec
    config["drain_timeout_sec"] = drain_timeout_sec
    config["latency_enabled"] = latency_enabled
    config["latency_sample_every"] = latency_sample_every
    config["latency_clock_samples"] = 100
    config["latency_clock_max_uncertainty_us"] = 250.0
    config["latency_clock_max_drift_us"] = 250.0
    config["qualification_policy_id"] = qualification_policy_id
    config["producer_ranks"] = int(config.get("producer_ranks") or 40)
    config["consumer_ranks"] = int(config.get("consumer_ranks") or 40)
    config["partitions"] = int(config.get("partitions") or 120)
    config["batch_size"] = int(config.get("batch_size") or 1_048_576)
    config["linger_ms"] = int(config.get("linger_ms") or 20)
    config["payload_size_bytes"] = int(config.get("payload_size_bytes") or 4_096)
    config["payload_mode"] = str(config.get("payload_mode") or "fixed_size")
    config["send_pattern"] = str(config.get("send_pattern") or "steady")
    config["virtual_devices_per_rank"] = int(
        config.get("virtual_devices_per_rank") or 50_000
    )
    config.pop("case_id", None)
    config.pop("campaign_id", None)
    extra = config.setdefault("extra", {})
    if not isinstance(extra, dict):
        config["extra"] = {}
        extra = config["extra"]
    extra.setdefault("kafka_producer_config", {})
    extra.setdefault("kafka_consumer_config", {})
    return config


def _variant_values(config: dict[str, Any]) -> dict[str, Any]:
    producer_cfg = _dict(_dict(config.get("extra")).get("kafka_producer_config"))
    consumer_cfg = _dict(_dict(config.get("extra")).get("kafka_consumer_config"))
    return {
        "producer_ranks": int(config.get("producer_ranks", 0)),
        "consumer_ranks": int(config.get("consumer_ranks", 0)),
        "partitions": int(config.get("partitions", 0)),
        "batch_size": int(config.get("batch_size", 0)),
        "linger_ms": int(config.get("linger_ms", 0)),
        "payload_size_bytes": int(config.get("payload_size_bytes", 0)),
        "producer_queue_messages": int(
            producer_cfg.get("queue.buffering.max.messages", 0)
        ),
        "producer_queue_kbytes": int(producer_cfg.get("queue.buffering.max.kbytes", 0)),
        "consumer_fetch_min_bytes": int(consumer_cfg.get("fetch.min.bytes", 0)),
        "consumer_fetch_wait_max_ms": int(consumer_cfg.get("fetch.wait.max.ms", 0)),
        "consumer_fetch_message_max_bytes": int(
            consumer_cfg.get("fetch.message.max.bytes", 0)
        ),
    }


def _apply_variant(config: dict[str, Any], values: dict[str, Any]) -> None:
    top_level = {
        "producer_ranks",
        "consumer_ranks",
        "partitions",
        "batch_size",
        "linger_ms",
        "payload_size_bytes",
    }
    for key, value in values.items():
        if key in top_level:
            config[key] = int(value)
            continue
        extra = config.setdefault("extra", {})
        if not isinstance(extra, dict):
            config["extra"] = {}
            extra = config["extra"]
        producer_cfg = extra.setdefault("kafka_producer_config", {})
        consumer_cfg = extra.setdefault("kafka_consumer_config", {})
        if key == "producer_queue_messages":
            producer_cfg["queue.buffering.max.messages"] = int(value)
        elif key == "producer_queue_kbytes":
            producer_cfg["queue.buffering.max.kbytes"] = int(value)
        elif key == "consumer_fetch_min_bytes":
            consumer_cfg["fetch.min.bytes"] = int(value)
        elif key == "consumer_fetch_wait_max_ms":
            consumer_cfg["fetch.wait.max.ms"] = int(value)
        elif key == "consumer_fetch_message_max_bytes":
            consumer_cfg["fetch.message.max.bytes"] = int(value)
            consumer_cfg["receive.message.max.bytes"] = max(100_000_000, int(value) + 1_048_576)
        else:
            raise KeyError(key)


def _dedupe_key(config: dict[str, Any]) -> tuple[Any, ...]:
    values = _variant_values(config)
    return tuple(values[key] for key in sorted(values))


def _manifest_row(
    config_id: str,
    config_path: Path,
    config: dict[str, Any],
    note: str,
) -> dict[str, Any]:
    values = _variant_values(config)
    return {
        "config_id": config_id,
        "config_path": _project_relative(config_path),
        "topic_name": config["topic_name"],
        "scenario": config["scenario"],
        "partitions": values["partitions"],
        "producer_ranks": values["producer_ranks"],
        "consumer_ranks": values["consumer_ranks"],
        "batch_size": values["batch_size"],
        "linger_ms": values["linger_ms"],
        "payload_size_bytes": values["payload_size_bytes"],
        "producer_queue_messages": values["producer_queue_messages"],
        "producer_queue_kbytes": values["producer_queue_kbytes"],
        "consumer_fetch_min_bytes": values["consumer_fetch_min_bytes"],
        "consumer_fetch_wait_max_ms": values["consumer_fetch_wait_max_ms"],
        "consumer_fetch_message_max_bytes": values["consumer_fetch_message_max_bytes"],
        "warmup_sec": config["warmup_sec"],
        "measurement_sec": config["duration_sec"],
        "drain_timeout_sec": config["drain_timeout_sec"],
        "latency_enabled": str(config["latency_enabled"]).lower(),
        "latency_sample_every": config["latency_sample_every"],
        "qualification_policy_id": config["qualification_policy_id"],
        "measurement_contract_id": _dict(_dict(config.get("extra")).get("campaign_metadata")).get(
            "measurement_contract_id", ""
        ),
        "broker_profile_id": config.get("broker_profile_id") or "",
        "broker_profile_sha256": config.get("broker_profile_sha256") or "",
        "notes": note,
    }


def _project_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = normalize_case_config(payload)
    config = BenchmarkConfig(**benchmark_config_input_dict(normalized))
    path.write_text(
        json.dumps(config.to_versioned_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
