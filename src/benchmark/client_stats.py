from __future__ import annotations

import json
import threading
import time
from collections import Counter
from typing import Any


def librdkafka_stats_enabled(config_extra: dict[str, Any]) -> bool:
    value = config_extra.get("enable_librdkafka_stats", True)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off"}
    return bool(value)


def librdkafka_stats_interval_ms(config_extra: dict[str, Any]) -> int:
    raw_value = config_extra.get(
        "librdkafka_statistics_interval_ms",
        config_extra.get("statistics_interval_ms", 5000),
    )
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        value = 5000
    return max(1000, value)


class LibrdkafkaStatsTracker:
    """
    Compact reducer for confluent-kafka/librdkafka statistics callbacks.

    The raw callback payload is intentionally not stored because high-rank runs
    can generate a large amount of JSON. The summary keeps the queue, latency,
    byte counter, and consumer-group signals needed for benchmark diagnosis.
    """

    def __init__(self, *, role: str, rank: int) -> None:
        self.role = role
        self.rank = rank
        self._lock = threading.Lock()
        self._sample_count = 0
        self._callback_error_count = 0
        self._first_callback_unix = 0.0
        self._last_callback_unix = 0.0
        self._client_type = ""
        self._client_name = ""
        self._client_id = ""
        self._first_counters: dict[str, float] = {}
        self._last_counters: dict[str, float] = {}
        self._max_values: dict[str, float] = {}
        self._last_broker_states: Counter[str] = Counter()
        self._last_cgrp_state = ""

    def record(self, payload: str) -> None:
        now = time.time()
        with self._lock:
            if self._first_callback_unix <= 0:
                self._first_callback_unix = now
            self._last_callback_unix = now
            self._sample_count += 1

        try:
            stats = json.loads(payload)
        except (TypeError, json.JSONDecodeError):
            with self._lock:
                self._callback_error_count += 1
            return

        if not isinstance(stats, dict):
            with self._lock:
                self._callback_error_count += 1
            return

        with self._lock:
            self._observe(stats)

    def summary(self) -> dict[str, Any]:
        with self._lock:
            duration = max(0.0, self._last_callback_unix - self._first_callback_unix)
            summary = {
                "enabled": True,
                "role": self.role,
                "rank": self.rank,
                "sample_count": self._sample_count,
                "callback_error_count": self._callback_error_count,
                "first_callback_unix": self._first_callback_unix,
                "last_callback_unix": self._last_callback_unix,
                "callback_window_sec": duration,
                "client_type": self._client_type,
                "client_name": self._client_name,
                "client_id": self._client_id,
                "broker_state_counts": dict(sorted(self._last_broker_states.items())),
                "last_cgrp_state": self._last_cgrp_state,
            }
            summary.update(self._max_values)
            for key, value in self._last_counters.items():
                summary[f"{key}_last"] = value
                summary[f"{key}_delta_between_stats"] = _counter_delta(
                    self._first_counters.get(key, 0.0),
                    value,
                )
            return summary

    def _observe(self, stats: dict[str, Any]) -> None:
        self._client_type = str(stats.get("type", self._client_type) or self._client_type)
        self._client_name = str(stats.get("name", self._client_name) or self._client_name)
        self._client_id = str(stats.get("client_id", self._client_id) or self._client_id)

        for key in (
            "tx",
            "tx_bytes",
            "rx",
            "rx_bytes",
            "txmsgs",
            "txmsg_bytes",
            "rxmsgs",
            "rxmsg_bytes",
            "txerrs",
            "rxerrs",
        ):
            self._observe_counter(key, stats.get(key))

        for key in ("replyq", "msg_cnt", "msg_size", "msg_max", "msg_size_max"):
            self._observe_max(key, stats.get(key))

        broker_states: Counter[str] = Counter()
        for broker in _dict(stats.get("brokers")).values():
            broker = _dict(broker)
            state = str(broker.get("state", "unknown") or "unknown")
            broker_states[state] += 1
            for key in (
                "outbuf_cnt",
                "outbuf_msg_cnt",
                "waitresp_cnt",
                "waitresp_msg_cnt",
                "txerrs",
                "rxerrs",
                "connects",
                "disconnects",
            ):
                self._observe_max(f"broker_{key}", broker.get(key))
            rtt = _dict(broker.get("rtt"))
            for key in ("avg", "p50", "p75", "p90", "p95", "p99", "max"):
                self._observe_max(f"broker_rtt_{key}_us", rtt.get(key))

        if broker_states:
            self._last_broker_states = broker_states

        for topic in _dict(stats.get("topics")).values():
            topic = _dict(topic)
            for partition in _dict(topic.get("partitions")).values():
                partition = _dict(partition)
                for key in (
                    "msgq_cnt",
                    "msgq_bytes",
                    "xmit_msgq_cnt",
                    "xmit_msgq_bytes",
                    "fetchq_cnt",
                    "fetchq_size",
                    "consumer_lag",
                ):
                    self._observe_max(f"toppar_{key}", partition.get(key))

        cgrp = _dict(stats.get("cgrp"))
        if cgrp:
            self._last_cgrp_state = str(cgrp.get("state", self._last_cgrp_state) or "")
            for key in (
                "assignment_size",
                "rebalance_cnt",
                "rebalance_age",
                "join_state",
            ):
                self._observe_max(f"cgrp_{key}", cgrp.get(key))

    def _observe_counter(self, key: str, value: Any) -> None:
        parsed = _float_or_none(value)
        if parsed is None:
            return
        if key not in self._first_counters:
            self._first_counters[key] = parsed
        self._last_counters[key] = parsed

    def _observe_max(self, key: str, value: Any) -> None:
        parsed = _float_or_none(value)
        if parsed is None:
            return
        output_key = f"max_{key}"
        self._max_values[output_key] = max(self._max_values.get(output_key, 0.0), parsed)


def disabled_librdkafka_stats_summary(*, role: str, rank: int) -> dict[str, Any]:
    return {
        "enabled": False,
        "role": role,
        "rank": rank,
        "reason": "disabled by extra.enable_librdkafka_stats",
    }


def aggregate_librdkafka_stats(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    if not summaries:
        return {"enabled": False, "rank_count": 0, "sample_count": 0}

    enabled = [summary for summary in summaries if bool(summary.get("enabled"))]
    result: dict[str, Any] = {
        "enabled": bool(enabled),
        "rank_count": len(summaries),
        "stats_enabled_rank_count": len(enabled),
        "sample_count": sum(_int(summary.get("sample_count")) for summary in enabled),
        "callback_error_count": sum(_int(summary.get("callback_error_count")) for summary in enabled),
        "first_callback_unix": min(
            (_float(summary.get("first_callback_unix")) for summary in enabled if _float(summary.get("first_callback_unix")) > 0),
            default=0.0,
        ),
        "last_callback_unix": max(
            (_float(summary.get("last_callback_unix")) for summary in enabled),
            default=0.0,
        ),
    }
    if result["first_callback_unix"] and result["last_callback_unix"]:
        result["callback_window_sec"] = max(
            0.0,
            result["last_callback_unix"] - result["first_callback_unix"],
        )
    else:
        result["callback_window_sec"] = 0.0

    max_keys = set()
    counter_last_keys = set()
    counter_delta_keys = set()
    broker_states: Counter[str] = Counter()
    cgrp_states: Counter[str] = Counter()
    for summary in enabled:
        for key in summary:
            if key.startswith("max_"):
                max_keys.add(key)
            elif key.endswith("_last"):
                counter_last_keys.add(key)
            elif key.endswith("_delta_between_stats"):
                counter_delta_keys.add(key)
        broker_states.update(_dict(summary.get("broker_state_counts")))
        cgrp_state = str(summary.get("last_cgrp_state", ""))
        if cgrp_state:
            cgrp_states[cgrp_state] += 1

    for key in sorted(max_keys):
        result[key] = max((_float(summary.get(key)) for summary in enabled), default=0.0)
    for key in sorted(counter_last_keys):
        result[key] = sum(_float(summary.get(key)) for summary in enabled)
    for key in sorted(counter_delta_keys):
        result[key] = sum(_float(summary.get(key)) for summary in enabled)
    result["broker_state_counts"] = dict(sorted(broker_states.items()))
    result["consumer_group_state_counts"] = dict(sorted(cgrp_states.items()))
    return result


def _counter_delta(first: float, last: float) -> float:
    if last >= first:
        return last - first
    return last


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _float_or_none(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> float:
    parsed = _float_or_none(value)
    return parsed if parsed is not None else 0.0


def _int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0
