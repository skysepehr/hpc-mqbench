from __future__ import annotations

import math
from typing import Any, Mapping


BACKLOG_LIMIT_PERCENT = 5.0
FLUSH_LIMIT_SEC = 10.0
FAILED_SEND_LIMIT_PERCENT = 0.1
COMMON_QUALIFICATION_POLICY_ID = "qualification.application.v1"
KAFKA_HISTORICAL_QUALIFICATION_POLICY_ID = "qualification.kafka.v1"
PULSAR_HISTORICAL_QUALIFICATION_POLICY_ID = "qualification.pulsar.v1"
BACKLOG_DENOMINATOR_ATTEMPTED = "messages_attempted"
BACKLOG_DENOMINATOR_ENQUEUED = "messages_enqueued"


def producer_operational_metrics(
    producers: Mapping[str, Any],
    *,
    backlog_denominator: str = BACKLOG_DENOMINATOR_ENQUEUED,
) -> dict[str, float | int | bool | str]:
    """Return application-level producer qualification metrics.

    Historical Kafka analyses divided pending callbacks by records successfully
    enqueued into the client. Pulsar derived analyses and the reusable
    measurement contract divide by every measurement-period send attempt.
    Keeping the denominator explicit prevents historical Kafka results from
    being silently reinterpreted.
    """
    messages_enqueued = _integer(producers.get("messages_enqueued"))
    pending_is_present = producers.get("pending_messages_at_flush_start") is not None
    pending_at_flush = _integer(
        producers.get("pending_messages_at_flush_start")
    )
    messages_attempted = _integer(producers.get("messages_attempted"))
    failed_is_present = producers.get("messages_failed") is not None
    messages_failed = _integer(producers.get("messages_failed"))
    flush_sec = _number(
        producers.get("max_flush_duration_sec"),
        default=math.inf,
    )
    if backlog_denominator == BACKLOG_DENOMINATOR_ATTEMPTED:
        backlog_denominator_value = messages_attempted
    elif backlog_denominator == BACKLOG_DENOMINATOR_ENQUEUED:
        backlog_denominator_value = messages_enqueued
    else:
        raise ValueError(
            "backlog_denominator must be 'messages_attempted' or "
            "'messages_enqueued'"
        )

    backlog_percent = (
        100.0 * pending_at_flush / backlog_denominator_value
        if backlog_denominator_value > 0
        and pending_is_present
        and pending_at_flush >= 0
        else math.inf
    )
    failed_send_percent = (
        100.0 * messages_failed / messages_attempted
        if messages_attempted > 0
        and failed_is_present
        and messages_failed >= 0
        else math.inf
    )
    thresholds_satisfied = (
        backlog_percent <= BACKLOG_LIMIT_PERCENT
        and flush_sec <= FLUSH_LIMIT_SEC
        and failed_send_percent <= FAILED_SEND_LIMIT_PERCENT
    )
    return {
        "messages_enqueued": messages_enqueued,
        "pending_messages_at_flush_start": pending_at_flush,
        "backlog_denominator": backlog_denominator,
        "backlog_denominator_value": backlog_denominator_value,
        "producer_backlog_percent": backlog_percent,
        "messages_attempted": messages_attempted,
        "messages_failed": messages_failed,
        "failed_send_percent": failed_send_percent,
        "max_flush_duration_sec": flush_sec,
        "thresholds_satisfied": thresholds_satisfied,
    }


def backlog_denominator_for_policy(policy_id: str) -> str:
    """Resolve the documented backlog denominator for a policy version."""
    if policy_id in {
        COMMON_QUALIFICATION_POLICY_ID,
        PULSAR_HISTORICAL_QUALIFICATION_POLICY_ID,
    }:
        return BACKLOG_DENOMINATOR_ATTEMPTED
    return BACKLOG_DENOMINATOR_ENQUEUED


def producer_operational_reasons(
    metrics: Mapping[str, float | int | bool],
) -> list[str]:
    reasons: list[str] = []
    backlog = float(metrics["producer_backlog_percent"])
    flush_sec = float(metrics["max_flush_duration_sec"])
    failed = float(metrics["failed_send_percent"])
    if backlog > BACKLOG_LIMIT_PERCENT:
        reasons.append(
            f"producer backlog {backlog:.6f}% > {BACKLOG_LIMIT_PERCENT:g}%"
        )
    if flush_sec > FLUSH_LIMIT_SEC:
        reasons.append(f"flush {flush_sec:.6f} s > {FLUSH_LIMIT_SEC:g} s")
    if failed > FAILED_SEND_LIMIT_PERCENT:
        reasons.append(
            f"failed sends {failed:.6f}% > {FAILED_SEND_LIMIT_PERCENT:g}%"
        )
    return reasons


def _integer(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _number(value: Any, *, default: float) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default
