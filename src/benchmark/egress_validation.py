from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_egress_prefill_result(output_dir: str | Path) -> dict[str, Any] | None:
    """
    Load the optional egress prefill artifact for one case directory.
    """
    output_path = Path(output_dir)
    for path in (
        output_path / "data" / "egress_prefill_result.json",
        output_path / "results" / "egress_prefill_result.json",
    ):
        if path.is_file():
            break
    else:
        return None

    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, dict):
        raise ValueError(f"Egress prefill result must be a JSON object: {path}")
    return data


def merge_egress_prefill_into_result(
    benchmark_result: dict[str, Any],
    prefill_artifact: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    Add an egress prefill/consume summary to a benchmark result when available.
    """
    merged = dict(benchmark_result)
    summary = build_egress_prefill_summary(benchmark_result, prefill_artifact)
    if summary is not None:
        merged["egress_prefill"] = summary
    return merged


def build_egress_prefill_summary(
    benchmark_result: dict[str, Any],
    prefill_artifact: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """
    Compare prefilled egress backlog with the messages consumed by the benchmark.
    """
    config = benchmark_result.get("config", {})
    scenario = str(config.get("scenario", ""))
    if prefill_artifact is None and scenario != "egress_only":
        return None

    if prefill_artifact is None:
        return {
            "enabled": False,
            "status": "missing_prefill_artifact",
            "reason": "No data/egress_prefill_result.json file was found.",
        }

    prefill = prefill_artifact.get("prefill", {})
    if not isinstance(prefill, dict):
        prefill = {}

    consumers = (
        benchmark_result.get("aggregated_metrics", {})
        .get("consumers", {})
    )
    if not isinstance(consumers, dict):
        consumers = {}

    requested = _as_int(prefill.get("messages_requested"))
    attempted = _as_int(prefill.get("messages_attempted"))
    delivered = _as_int(prefill.get("messages_delivered"))
    failed = _as_int(prefill.get("messages_failed"))
    consumed = _as_int(consumers.get("messages_received"))
    consumer_failed = _as_int(consumers.get("messages_failed"))
    remaining = max(delivered - consumed, 0)
    over_consumed = max(consumed - delivered, 0)
    drain_ratio = (consumed / delivered) if delivered > 0 else 0.0

    delivery_complete = requested > 0 and delivered == requested and failed == 0
    if not delivery_complete:
        status = "prefill_incomplete"
    elif consumed >= delivered:
        status = "drained"
    elif consumed > 0:
        status = "partial_consumption"
    else:
        status = "not_consumed"

    return {
        "enabled": True,
        "status": status,
        "topic": prefill.get("topic", config.get("topic_name")),
        "messages_requested": requested,
        "messages_attempted": attempted,
        "messages_delivered": delivered,
        "messages_failed": failed,
        "messages_consumed": consumed,
        "consumer_messages_failed": consumer_failed,
        "messages_remaining": remaining,
        "messages_over_consumed": over_consumed,
        "drain_ratio": drain_ratio,
        "prefill_duration_sec": _as_float(prefill.get("duration_sec")),
        "prefill_throughput_msgs_per_sec": _as_float(
            prefill.get("throughput_msgs_per_sec")
        ),
        "artifact": "data/egress_prefill_result.json",
    }


def validate_egress_prefill_drain(
    output_dir: str | Path,
    min_drain_ratio: float = 1.0,
) -> dict[str, Any]:
    """
    Fail if an egress benchmark did not drain enough of its prefilled backlog.
    """
    output_path = Path(output_dir)
    final_report_path = output_path / "final_report.json"
    if not final_report_path.is_file():
        raise RuntimeError(f"final_report.json not found: {final_report_path}")

    with final_report_path.open("r", encoding="utf-8") as handle:
        benchmark_result = json.load(handle)
    if not isinstance(benchmark_result, dict):
        raise RuntimeError(f"final_report.json is not an object: {final_report_path}")

    summary = benchmark_result.get("egress_prefill")
    if not isinstance(summary, dict):
        summary = build_egress_prefill_summary(
            benchmark_result,
            load_egress_prefill_result(output_path),
        )

    if not isinstance(summary, dict):
        raise RuntimeError("No egress prefill summary is available for this case")

    if summary.get("status") == "missing_prefill_artifact":
        raise RuntimeError(summary.get("reason", "Missing egress prefill artifact"))

    drain_ratio = _as_float(summary.get("drain_ratio"))
    delivered = _as_int(summary.get("messages_delivered"))
    consumed = _as_int(summary.get("messages_consumed"))
    remaining = _as_int(summary.get("messages_remaining"))

    if delivered <= 0:
        raise RuntimeError("Egress prefill delivered no messages")

    if drain_ratio < min_drain_ratio:
        raise RuntimeError(
            "Egress prefill drain validation failed: "
            f"delivered={delivered}, consumed={consumed}, remaining={remaining}, "
            f"drain_ratio={drain_ratio:.6f}, required={min_drain_ratio:.6f}"
        )

    return summary


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
