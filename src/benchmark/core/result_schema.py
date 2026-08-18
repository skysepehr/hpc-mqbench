from __future__ import annotations

from copy import deepcopy
import math
from typing import Any

from src.benchmark.backends.base import BackendAdapter
from src.benchmark.qualification import (
    backlog_denominator_for_policy,
    producer_operational_metrics,
    producer_operational_reasons,
)


RESULT_SCHEMA_VERSION = "messaging-benchmark.result.v1"
COMMON_NAMESPACE_FORBIDDEN_TOKENS = (
    "kafka",
    "librdkafka",
    "jmx",
    "pulsar",
    "mqtt",
)


def enrich_result_schema(
    benchmark_result: dict[str, Any],
    adapter: BackendAdapter,
) -> dict[str, Any]:
    """
    Add the portable result contract while preserving all legacy result keys.

    Existing Kafka reports and downstream scripts continue to read their
    original fields. New backends and cross-system tools use common_metrics and
    backend_metrics instead.
    """
    config = _dict(benchmark_result.get("config"))
    campaign_metadata = _dict(_dict(config.get("extra")).get("campaign_metadata"))
    aggregated = _dict(benchmark_result.get("aggregated_metrics"))
    producers = _dict(aggregated.get("producers"))
    consumers = _dict(aggregated.get("consumers"))
    correctness = _dict(aggregated.get("record_correctness"))
    latency = _dict(consumers.get("latency_histogram"))
    latency_validation = _dict(benchmark_result.get("latency_validation"))
    policy_id = str(
        config.get("qualification_policy_id") or _single_policy_id(adapter) or ""
    )
    producer_operations = producer_operational_metrics(
        producers,
        backlog_denominator=backlog_denominator_for_policy(policy_id),
    )
    producer_bytes_per_sec = _first_number(
        producers,
        "throughput_bytes_per_sec",
    )
    consumer_bytes_per_sec = _first_number(
        consumers,
        "throughput_bytes_per_sec",
    )
    producer_records_per_sec = _first_number(
        producers,
        "throughput_msgs_per_sec",
        "throughput_records_per_sec",
    )
    consumer_records_per_sec = _first_number(
        consumers,
        "throughput_msgs_per_sec",
        "throughput_records_per_sec",
    )

    benchmark_result["result_schema_version"] = RESULT_SCHEMA_VERSION
    benchmark_result["system_under_test"] = adapter.identity_from_dict(config)
    eligible, eligibility_failures = _eligibility(benchmark_result)
    if eligible is not None:
        eligibility = benchmark_result.get("eligibility")
        if not isinstance(eligibility, dict):
            eligibility = {}
            benchmark_result["eligibility"] = eligibility
        eligibility["eligible"] = eligible
        eligibility["failure_reasons"] = list(
            dict.fromkeys(
                [
                    *(
                        str(item)
                        for item in eligibility.get("failure_reasons", [])
                        if str(item).strip()
                    ),
                    *eligibility_failures,
                ]
            )
        )
    qualified = adapter.qualification_result(benchmark_result)
    if eligible is False:
        qualified = False

    benchmark_result["common_metrics"] = {
        "measurement_contract": {
            "id": campaign_metadata.get("measurement_contract_id"),
            "record_identity": "producer_rank_and_sequence",
            "send_timestamp_clock": "calibrated_wall_clock_nanoseconds",
            "receive_timestamp_clock": "calibrated_wall_clock_nanoseconds",
            "backlog_event": campaign_metadata.get("backlog_event"),
            "backlog_denominator_description": campaign_metadata.get(
                "backlog_denominator"
            ),
        },
        "consumer_drain": _portable_coordinated_drain(
            benchmark_result,
            producers,
            consumers,
            correctness,
        ),
        "records": {
            "offered": _first_number(
                producers,
                "messages_attempted",
                "offered_records",
            ),
            "published": _first_number(
                producers,
                "messages_delivered",
                "successful_messages",
            ),
            "consumed": _first_number(
                consumers,
                "messages_received",
            ),
            "late_drained": _first_number(
                correctness,
                "consumer_late_drained_records",
            ),
            "missing": _first_number(
                correctness,
                "missing_after_drain_records",
            ),
            "duplicate": _first_number(
                correctness,
                "duplicate_offset_count",
            ),
            "out_of_order": _first_number(
                correctness,
                "out_of_order_offset_count",
            ),
            "invalid_envelope": _first_number(
                correctness,
                "invalid_envelope_count",
            ),
            "surplus": _first_number(
                correctness,
                "unexplained_surplus_records",
            ),
        },
        "throughput": {
            "producer_bytes_per_sec": producer_bytes_per_sec,
            "consumer_bytes_per_sec": consumer_bytes_per_sec,
            "balanced_bytes_per_sec": _minimum_available(
                producer_bytes_per_sec,
                consumer_bytes_per_sec,
            ),
            "producer_mib_per_sec": _divide_or_none(
                producer_bytes_per_sec,
                1_048_576,
            ),
            "consumer_mib_per_sec": _divide_or_none(
                consumer_bytes_per_sec,
                1_048_576,
            ),
            "balanced_mib_per_sec": _divide_or_none(
                _minimum_available(
                    producer_bytes_per_sec,
                    consumer_bytes_per_sec,
                ),
                1_048_576,
            ),
            "producer_records_per_sec": producer_records_per_sec,
            "consumer_records_per_sec": consumer_records_per_sec,
            "balanced_records_per_sec": _minimum_available(
                producer_records_per_sec,
                consumer_records_per_sec,
            ),
        },
        "latency_end_to_end": {
            "enabled": bool(latency_validation.get("enabled")),
            "valid": bool(latency_validation.get("valid")),
            "count": _first_number(latency, "count"),
            "delivered_sample_count": _first_number(
                latency_validation,
                "delivered_sample_count",
            ),
            "sample_every": _first_number(latency_validation, "sample_every"),
            "mean_us": _latency_us(latency, "mean"),
            "p50_us": _latency_us(latency, "p50"),
            "p95_us": _latency_us(latency, "p95"),
            "p99_us": _latency_us(latency, "p99"),
            "p99_9_us": _latency_us(latency, "p99_9", "p999"),
            "max_us": _latency_us(latency, "max"),
            "negative_count": _first_number(
                latency_validation,
                "negative_latency_count",
            ),
            "clock_uncertainty_us_max": _clock_max_us(
                latency_validation,
                "max_uncertainty_ns",
            ),
            "clock_drift_us_max": _clock_max_us(
                latency_validation,
                "drift_ns",
            ),
        },
        "timing": {
            "warmup_sec": config.get("warmup_sec"),
            "measurement_sec": config.get("duration_sec"),
            "drain_timeout_sec": config.get("drain_timeout_sec"),
        },
        "producer_delivery": {
            "measurement_period_send_attempts": producer_operations[
                "messages_attempted"
            ],
            "successfully_enqueued_records": producer_operations[
                "messages_enqueued"
            ],
            "pending_delivery_callbacks_at_flush_start": producer_operations[
                "pending_messages_at_flush_start"
            ],
            "backlog_denominator": producer_operations[
                "backlog_denominator"
            ],
            "backlog_denominator_value": producer_operations[
                "backlog_denominator_value"
            ],
            "backlog_percent": _finite_or_none(
                producer_operations["producer_backlog_percent"]
            ),
            "flush_duration_sec": _finite_or_none(
                producer_operations["max_flush_duration_sec"]
            ),
            "failed_sends": producer_operations["messages_failed"],
            "failed_send_percent": _finite_or_none(
                producer_operations["failed_send_percent"]
            ),
        },
        "resources": _portable_resources(benchmark_result),
        "qualification": {
            "policy_id": policy_id or None,
            "eligible": eligible,
            "qualified": qualified,
            "thresholds": {
                "backlog_percent_max": 5.0,
                "flush_duration_sec_max": 10.0,
                "failed_send_percent_max": 0.1,
            },
            "failure_reasons": producer_operational_reasons(
                producer_operations
            ),
        },
    }
    backend_metrics = benchmark_result.setdefault("backend_metrics", {})
    backend_metrics[adapter.backend_id] = adapter.normalize_metrics(benchmark_result)
    validate_result_schema(benchmark_result)
    return benchmark_result


def validate_result_schema(benchmark_result: dict[str, Any]) -> None:
    if benchmark_result.get("result_schema_version") != RESULT_SCHEMA_VERSION:
        raise ValueError("Unsupported or missing result_schema_version")
    common = _dict(benchmark_result.get("common_metrics"))
    for path in _walk_paths(common):
        lowered = path.lower()
        if any(token in lowered for token in COMMON_NAMESPACE_FORBIDDEN_TOKENS):
            raise ValueError(
                "Backend-specific result field is not allowed in common_metrics: "
                + path
            )

    system = _dict(benchmark_result.get("system_under_test"))
    backend_id = str(system.get("backend_id", "")).strip()
    backend_metrics = _dict(benchmark_result.get("backend_metrics"))
    if not backend_id or backend_id not in backend_metrics:
        raise ValueError(
            "backend_metrics must contain the system_under_test backend namespace"
        )


def _portable_resources(result: dict[str, Any]) -> dict[str, Any]:
    inventory = _dict(result.get("system_inventory"))
    monitoring = _dict(result.get("monitoring"))
    nodes = inventory.get("nodes")
    portable_nodes: list[dict[str, Any]] = []
    if isinstance(nodes, list):
        for node in nodes:
            if not isinstance(node, dict):
                continue
            portable_nodes.append(
                {
                    key: deepcopy(node[key])
                    for key in (
                        "node",
                        "role",
                        "observed_hostname",
                        "service_address",
                        "cpu",
                        "memory",
                        "network",
                        "numa",
                    )
                    if key in node
                }
            )

    collected = monitoring.get("collected_metrics")
    portable_metrics: list[dict[str, Any]] = []
    if isinstance(collected, list):
        for metric in collected:
            if not isinstance(metric, dict):
                continue
            metric_id = str(metric.get("id", "")).lower()
            category = str(metric.get("category", "")).lower()
            if category not in {"system", "process", "network", "storage"}:
                continue
            if any(
                token in metric_id
                for token in COMMON_NAMESPACE_FORBIDDEN_TOKENS
            ):
                continue
            portable_metrics.append(deepcopy(metric))

    return {
        "system_inventory": {
            key: deepcopy(inventory[key])
            for key in (
                "format",
                "status",
                "enabled",
                "settings",
                "probe_summary",
                "network_tests",
            )
            if key in inventory
        }
        | {"nodes": portable_nodes},
        "node_monitoring": {
            "status": monitoring.get("status"),
            "time_window": deepcopy(monitoring.get("time_window")),
            "metrics": portable_metrics,
        },
    }


def _walk_paths(value: Any, prefix: str = "") -> list[str]:
    paths: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            paths.append(path)
            paths.extend(_walk_paths(item, path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            path = f"{prefix}[{index}]"
            paths.extend(_walk_paths(item, path))
    return paths


def _first_number(values: dict[str, Any], *keys: str) -> int | float | None:
    for key in keys:
        value = values.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return value
    return None


def _minimum_available(
    first: int | float | None,
    second: int | float | None,
) -> int | float | None:
    if first is None or second is None:
        return None
    return min(first, second)


def _divide_or_none(
    value: int | float | None,
    divisor: int | float,
) -> float | None:
    return float(value) / divisor if value is not None else None


def _finite_or_none(value: Any) -> int | float | None:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    return value if math.isfinite(float(value)) else None


def _eligibility(result: dict[str, Any]) -> tuple[bool | None, list[str]]:
    lifecycle_failures = common_measurement_lifecycle_failures(result)
    explicit = result.get("eligible")
    if isinstance(explicit, bool):
        failures = [
            *_existing_eligibility_failures(result),
            *lifecycle_failures,
        ]
        return explicit and not failures, list(dict.fromkeys(failures))
    eligibility = _dict(result.get("eligibility"))
    value = eligibility.get("eligible")
    if isinstance(value, bool):
        failures = [
            *_existing_eligibility_failures(result),
            *lifecycle_failures,
        ]
        return value and not failures, list(dict.fromkeys(failures))

    # Legacy results predate the runtime-health contract. Keep their historical
    # null eligibility rather than retrospectively inventing evidence.
    health = _dict(result.get("backend_health"))
    if not health:
        if lifecycle_failures:
            return False, lifecycle_failures
        return None, []

    failures: list[str] = []
    health_status = str(health.get("status", "invalid")).lower()
    if health_status != "healthy":
        failures.append(f"backend health status is {health_status}")

    case = _dict(result.get("case"))
    if str(case.get("status", "unknown")).lower() != "completed":
        failures.append(f"case status is {case.get('status', 'unknown')}")

    aggregated = _dict(result.get("aggregated_metrics"))
    producers = _dict(aggregated.get("producers"))
    consumers = _dict(aggregated.get("consumers"))
    correctness = _dict(aggregated.get("record_correctness"))
    if not producers or not consumers or not correctness:
        failures.append(
            "required producer, consumer, or correctness results are missing"
        )
    else:
        if _first_number(producers, "messages_attempted") in (None, 0):
            failures.append("producer attempted-record count is missing or zero")
        if _first_number(producers, "messages_delivered") in (None, 0):
            failures.append("producer delivered-record count is missing or zero")
        for key, label in (
            ("missing_after_drain_records", "records missing after drain"),
            ("invalid_envelope_count", "invalid record envelopes"),
            ("duplicate_offset_count", "duplicate records"),
            ("out_of_order_offset_count", "out-of-order records"),
            ("unexplained_surplus_records", "unexplained surplus records"),
        ):
            value = _first_number(correctness, key)
            if value is None:
                failures.append(f"{label} evidence is missing")
            elif value != 0:
                failures.append(f"{label}: {value}")

    config = _dict(result.get("config"))
    if bool(config.get("latency_enabled")):
        latency_validation = _dict(result.get("latency_validation"))
        if latency_validation.get("valid") is not True:
            reason = str(
                latency_validation.get("reason", "latency validation did not pass")
            )
            failures.append(reason)

    failures.extend(lifecycle_failures)
    return not failures, list(dict.fromkeys(failures))


def common_measurement_lifecycle_failures(
    result: dict[str, Any],
) -> list[str]:
    """Validate the coordinated drain required by the reproducible contract."""
    config = _dict(result.get("config"))
    metadata = _dict(_dict(config.get("extra")).get("campaign_metadata"))
    if (
        metadata.get("measurement_contract_id")
        != "measurement.messaging.reproducible.v1"
        or config.get("scenario") != "simultaneous"
    ):
        return []

    failures: list[str] = []
    drain = _dict(result.get("coordinated_drain"))
    aggregated = _dict(result.get("aggregated_metrics"))
    producers = _dict(aggregated.get("producers"))
    consumers = _dict(aggregated.get("consumers"))
    correctness = _dict(aggregated.get("record_correctness"))

    delivered = _first_number(producers, "messages_delivered")
    consumed = _first_number(
        correctness,
        "consumer_received_through_drain_records",
    )
    consumer_ranks = _first_number(consumers, "consumer_rank_count")
    coordinated_ranks = _first_number(
        consumers,
        "coordinated_drain_rank_count",
    )

    if drain.get("enabled") is not True:
        failures.append("coordinated producer-flush/consumer-drain evidence is missing")
        return failures
    if drain.get("producer_flush_boundary") != "all_producer_ranks_completed_flush":
        failures.append("coordinated drain has an invalid producer flush boundary")
    if drain.get("configured_post_flush_timeout_sec") != config.get(
        "drain_timeout_sec"
    ):
        failures.append("coordinated drain timeout does not match the case config")
    if drain.get("completion_reason") != "delivered_target_reached":
        failures.append("coordinated drain did not reach the delivered-record target")
    if drain.get("complete") is not True:
        failures.append("coordinated drain is not complete")
    if delivered is None or drain.get("delivered_target_records") != delivered:
        failures.append("coordinated drain target does not match producer deliveries")
    if consumed is None or drain.get("consumed_records_at_completion") != consumed:
        failures.append("coordinated drain completion count does not match consumers")
    if (
        not isinstance(consumer_ranks, (int, float))
        or consumer_ranks <= 0
        or coordinated_ranks != consumer_ranks
    ):
        failures.append("not every consumer rank recorded coordinated drain evidence")

    start = _first_number(consumers, "first_post_flush_drain_start_time_unix")
    end = _first_number(consumers, "last_post_flush_drain_end_time_unix")
    if start is None or end is None or start <= 0 or end < start:
        failures.append("coordinated drain timestamps are missing or invalid")
    return list(dict.fromkeys(failures))


def _portable_coordinated_drain(
    result: dict[str, Any],
    producers: dict[str, Any],
    consumers: dict[str, Any],
    correctness: dict[str, Any],
) -> dict[str, Any]:
    drain = _dict(result.get("coordinated_drain"))
    return {
        "enabled": drain.get("enabled"),
        "producer_flush_boundary": drain.get("producer_flush_boundary"),
        "delivered_target_records": drain.get("delivered_target_records"),
        "producer_delivered_records": _first_number(
            producers,
            "messages_delivered",
        ),
        "consumed_records_at_completion": drain.get(
            "consumed_records_at_completion"
        ),
        "consumer_received_through_drain_records": _first_number(
            correctness,
            "consumer_received_through_drain_records",
        ),
        "consumer_rank_count": _first_number(consumers, "consumer_rank_count"),
        "coordinated_drain_rank_count": _first_number(
            consumers,
            "coordinated_drain_rank_count",
        ),
        "pre_flush_drained_records": _first_number(
            consumers,
            "pre_flush_drained_messages",
        ),
        "post_flush_drained_records": _first_number(
            consumers,
            "post_flush_drained_messages",
        ),
        "post_flush_drain_start_time_unix": _first_number(
            consumers,
            "first_post_flush_drain_start_time_unix",
        ),
        "post_flush_drain_end_time_unix": _first_number(
            consumers,
            "last_post_flush_drain_end_time_unix",
        ),
        "configured_post_flush_timeout_sec": drain.get(
            "configured_post_flush_timeout_sec"
        ),
        "completion_reason": drain.get("completion_reason"),
        "complete": drain.get("complete"),
    }


def _existing_eligibility_failures(result: dict[str, Any]) -> list[str]:
    eligibility = _dict(result.get("eligibility"))
    failures = eligibility.get("failure_reasons", [])
    if not isinstance(failures, list):
        return []
    return [str(item) for item in failures if str(item).strip()]


def _latency_us(
    latency: dict[str, Any],
    *base_names: str,
) -> float | None:
    for base_name in base_names:
        microseconds = _first_number(latency, f"{base_name}_us")
        if microseconds is not None:
            return float(microseconds)
        nanoseconds = _first_number(latency, f"{base_name}_ns")
        if nanoseconds is not None:
            return float(nanoseconds) / 1_000
    return None


def _clock_max_us(
    latency_validation: dict[str, Any],
    key: str,
) -> float | None:
    calibrations = latency_validation.get("clock_calibrations")
    if not isinstance(calibrations, list):
        return None
    values = [
        float(record[key]) / 1_000
        for record in calibrations
        if isinstance(record, dict)
        and isinstance(record.get(key), (int, float))
        and not isinstance(record.get(key), bool)
    ]
    return max(values) if values else None


def _single_policy_id(adapter: BackendAdapter) -> str | None:
    policies = sorted(adapter.qualification_policy_ids)
    return policies[0] if len(policies) == 1 else None


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}
