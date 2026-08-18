from __future__ import annotations

from copy import deepcopy
from typing import Any


CASE_SCHEMA_VERSION = "messaging-benchmark.case.v1"
LEGACY_KAFKA_SCHEMA_VERSION = "legacy.kafka.flat.v1"
DEFAULT_QUALIFICATION_POLICY = "qualification.kafka.v1"

TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "backend_id",
        "workload",
        "backend",
        "campaign",
        "qualification_policy_id",
    }
)

WORKLOAD_FIELDS = frozenset(
    {
        "mode",
        "scenario",
        "payload_size_bytes",
        "payload_mode",
        "send_pattern",
        "producer_ranks",
        "consumer_ranks",
        "virtual_devices_per_rank",
        "total_simulated_devices",
        "duration_sec",
        "warmup_sec",
        "drain_timeout_sec",
        "target_records_per_sec",
        "latency_enabled",
        "latency_sample_every",
        "latency_clock_samples",
        "latency_clock_max_uncertainty_us",
        "latency_clock_max_drift_us",
    }
)

# Public compatibility constant. Ownership of these fields now lives in the
# Kafka adapter; keeping the name avoids breaking downstream imports.
KAFKA_FIELDS = frozenset(
    {
        "broker_count",
        "partitions",
        "replication_factor",
        "topic_name",
        "acks",
        "compression_type",
        "batch_size",
        "linger_ms",
        "broker_profile_id",
        "broker_profile_sha256",
        "extra",
    }
)

CAMPAIGN_FIELDS = frozenset({"case_id", "campaign_id", "metadata"})


def normalize_case_config(raw_config: dict[str, Any]) -> dict[str, Any]:
    """
    Convert either supported input form to the flat runtime representation.

    Existing Kafka JSON files remain accepted unchanged. Versioned files keep
    portable workload settings separate from Kafka-specific settings and are
    flattened only at the compatibility boundary used by BenchmarkConfig.
    """
    if not isinstance(raw_config, dict):
        raise ValueError("Benchmark case input must be a dictionary")

    if "workload" not in raw_config and "backend" not in raw_config:
        normalized = deepcopy(raw_config)
        backend_id = str(normalized.get("backend_id", "kafka")).strip()
        if backend_id != "kafka":
            raise ValueError(
                "Flat legacy configurations are supported only for backend_id='kafka'"
            )
        normalized.setdefault("schema_version", LEGACY_KAFKA_SCHEMA_VERSION)
        normalized.setdefault("backend_id", "kafka")
        normalized.setdefault(
            "qualification_policy_id",
            DEFAULT_QUALIFICATION_POLICY,
        )
        return normalized

    _require_exact_fields("top-level case", raw_config, TOP_LEVEL_FIELDS)
    schema_version = str(raw_config.get("schema_version", "")).strip()
    if schema_version != CASE_SCHEMA_VERSION:
        raise ValueError(
            f"schema_version must be {CASE_SCHEMA_VERSION!r}, got {schema_version!r}"
        )

    backend_id = str(raw_config.get("backend_id", "")).strip()
    if not backend_id:
        raise ValueError("backend_id must not be empty")

    workload = _require_dict("workload", raw_config.get("workload"))
    backend = _require_dict("backend", raw_config.get("backend"))
    campaign = _require_dict("campaign", raw_config.get("campaign", {}))
    _require_exact_fields("workload", workload, WORKLOAD_FIELDS)
    _require_exact_fields("campaign", campaign, CAMPAIGN_FIELDS)

    if set(backend) != {backend_id}:
        names = ", ".join(sorted(backend)) or "none"
        raise ValueError(
            "backend must contain exactly one namespace matching backend_id "
            f"{backend_id!r}; found: {names}"
        )
    backend_config = _require_dict(
        f"backend.{backend_id}",
        backend.get(backend_id),
    )
    adapter = _get_backend(backend_id)
    accepted_fields = adapter.versioned_config_fields()
    if accepted_fields is not None:
        _require_exact_fields(
            f"backend.{backend_id}",
            backend_config,
            accepted_fields,
        )

    normalized = deepcopy(workload)
    normalized.update(adapter.normalize_versioned_config(backend_config))
    normalized.update(
        {
            key: deepcopy(value)
            for key, value in campaign.items()
            if key in {"case_id", "campaign_id"}
        }
    )
    campaign_metadata = campaign.get("metadata")
    if campaign_metadata is not None:
        metadata = _require_dict("campaign.metadata", campaign_metadata)
        extra = normalized.setdefault("extra", {})
        if not isinstance(extra, dict):
            raise ValueError("backend settings field 'extra' must be a dictionary")
        extra.setdefault("campaign_metadata", deepcopy(metadata))

    normalized["schema_version"] = schema_version
    normalized["backend_id"] = backend_id
    normalized["qualification_policy_id"] = str(
        raw_config.get("qualification_policy_id", "")
    ).strip()
    if not normalized["qualification_policy_id"]:
        raise ValueError("qualification_policy_id must not be empty")
    return normalized


def versioned_case_config(flat_config: dict[str, Any]) -> dict[str, Any]:
    """Build a portable case-schema document from a normalized flat config."""
    backend_id = str(flat_config.get("backend_id", "kafka")).strip()
    workload = {
        key: deepcopy(flat_config[key])
        for key in WORKLOAD_FIELDS
        if key in flat_config
    }
    adapter = _get_backend(backend_id)
    backend_config = adapter.versioned_config_from_runtime(flat_config)
    campaign = {
        key: deepcopy(flat_config[key])
        for key in ("case_id", "campaign_id")
        if flat_config.get(key) is not None
    }
    extra = backend_config.get("extra")
    if not isinstance(extra, dict):
        extra = flat_config.get("extra")
    if isinstance(extra, dict) and "campaign_metadata" in extra:
        campaign["metadata"] = deepcopy(extra["campaign_metadata"])
        if "extra" in backend_config:
            backend_config["extra"] = {
                key: deepcopy(value)
                for key, value in extra.items()
                if key != "campaign_metadata"
            }

    return {
        "schema_version": CASE_SCHEMA_VERSION,
        "backend_id": backend_id,
        "workload": workload,
        "backend": {backend_id: backend_config},
        "campaign": campaign,
        "qualification_policy_id": str(
            flat_config.get(
                "qualification_policy_id",
                DEFAULT_QUALIFICATION_POLICY,
            )
        ),
    }


def _require_dict(name: str, value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return value


def _require_exact_fields(
    name: str,
    value: dict[str, Any],
    allowed_fields: frozenset[str],
) -> None:
    unknown = sorted(set(value) - allowed_fields)
    if unknown:
        raise ValueError(f"Unsupported {name} field(s): {', '.join(unknown)}")


def _get_backend(backend_id: str) -> Any:
    # Lazy import prevents a config-schema/backend-registry import cycle while
    # keeping backend-specific fields out of the benchmark core.
    from src.benchmark.backends import get_backend

    return get_backend(backend_id)
