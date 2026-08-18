from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import Any, Literal, Optional

from src.benchmark.core.config_schema import (
    CASE_SCHEMA_VERSION,
    DEFAULT_QUALIFICATION_POLICY,
    versioned_case_config,
)


# Supported configuration value types.
RunMode = Literal["single", "campaign"]
ScenarioName = Literal[
    "ingress_ramp",
    "egress_only",
    "simultaneous",
    "consume_and_process",
]
AckMode = Literal["0", "1", "all"]
CompressionType = Literal["none", "lz4", "zstd", "gzip", "snappy"]
PayloadMode = Literal["compact", "standard", "enriched", "fixed_size"]
SendPattern = Literal["steady", "batched", "burst", "windowed"]

ALLOWED_RUN_MODES = {"single", "campaign"}
ALLOWED_SCENARIOS = {
    "ingress_ramp",
    "egress_only",
    "simultaneous",
    "consume_and_process",
}
ALLOWED_ACKS = {"0", "1", "all"}
ALLOWED_COMPRESSION_TYPES = {"none", "lz4", "zstd", "gzip", "snappy"}
ALLOWED_PAYLOAD_MODES = {"compact", "standard", "enriched", "fixed_size"}
ALLOWED_SEND_PATTERNS = {"steady", "batched", "burst", "windowed"}
ALLOWED_BROKER_COUNTS = {1}


@dataclass(slots=True)
class BenchmarkConfig:
    """
    Represents one exact benchmark configuration for one run.

    This object is used after loading JSON configuration and should contain
    only validated, normalized values that the rest of the program can trust.
    """

    # Versioned portable case identity. Legacy flat Kafka configs receive these
    # values in the compatibility loader before this model is constructed.
    schema_version: str = CASE_SCHEMA_VERSION
    backend_id: str = "kafka"
    qualification_policy_id: str = DEFAULT_QUALIFICATION_POLICY

    # General execution mode and scenario.
    mode: RunMode = "single"
    scenario: ScenarioName = "ingress_ramp"

    # Kafka cluster settings.
    broker_count: int = 1
    partitions: int = 24
    replication_factor: int = 1
    topic_name: str = "benchmark-topic"

    # Producer reliability and tuning settings.
    acks: AckMode = "all"
    compression_type: CompressionType = "none"
    batch_size: int = 65_536
    linger_ms: int = 5

    # Payload and sending behavior.
    payload_size_bytes: int = 1_024
    payload_mode: PayloadMode = "standard"
    send_pattern: SendPattern = "steady"

    # Parallelism.
    producer_ranks: int = 1
    consumer_ranks: int = 0

    # Virtual device scale.
    virtual_devices_per_rank: int = 10_000
    total_simulated_devices: Optional[int] = None

    # Timing.
    duration_sec: int = 300
    warmup_sec: int = 0
    drain_timeout_sec: int = 0

    # Optional fixed aggregate offered rate across all producer ranks.
    target_records_per_sec: Optional[float] = None

    # End-to-end latency instrumentation. These fields are opt-in so existing
    # V1 configurations retain their original runtime behavior.
    latency_enabled: bool = False
    latency_sample_every: int = 1
    latency_clock_samples: int = 100
    latency_clock_max_uncertainty_us: float = 250.0
    latency_clock_max_drift_us: float = 250.0

    # Immutable broker-profile identity used by V2 tuning campaigns.
    broker_profile_id: Optional[str] = None
    broker_profile_sha256: Optional[str] = None

    # Opaque product settings for non-Kafka adapters. Kafka settings remain
    # available through their legacy flat fields until V1/V2 compatibility no
    # longer requires those attributes.
    backend_settings: dict[str, Any] = field(default_factory=dict)

    # Optional metadata for campaign or reporting.
    case_id: Optional[str] = None
    campaign_id: Optional[str] = None
    extra: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """
        Validate and normalize the config immediately after creation.
        """
        self._normalize()
        self._validate()
        self._derive_missing_values()

    def _normalize(self) -> None:
        """
        Normalize simple text fields to avoid issues caused by whitespace.
        """
        self.schema_version = self._normalize_text_field(
            "schema_version",
            self.schema_version,
        )
        self.backend_id = self._normalize_text_field("backend_id", self.backend_id)
        self.qualification_policy_id = self._normalize_text_field(
            "qualification_policy_id",
            self.qualification_policy_id,
        )
        self.mode = self._normalize_text_field("mode", self.mode)
        self.scenario = self._normalize_text_field("scenario", self.scenario)
        self.topic_name = self._normalize_text_field("topic_name", self.topic_name)
        self.acks = self._normalize_text_field("acks", self.acks)
        self.compression_type = self._normalize_text_field(
            "compression_type",
            self.compression_type,
        )
        self.payload_mode = self._normalize_text_field("payload_mode", self.payload_mode)
        self.send_pattern = self._normalize_text_field("send_pattern", self.send_pattern)

    def _validate(self) -> None:
        """
        Validate the configuration. Raise ValueError if something is invalid.

        This keeps errors close to config loading instead of failing later
        deep in the benchmark logic.
        """
        if not self.schema_version:
            raise ValueError("schema_version must not be empty")
        if not self.backend_id:
            raise ValueError("backend_id must not be empty")
        if not self.qualification_policy_id:
            raise ValueError("qualification_policy_id must not be empty")
        self._validate_choice("mode", self.mode, ALLOWED_RUN_MODES)
        self._validate_choice("scenario", self.scenario, ALLOWED_SCENARIOS)
        self._validate_choice("payload_mode", self.payload_mode, ALLOWED_PAYLOAD_MODES)
        self._validate_choice("send_pattern", self.send_pattern, ALLOWED_SEND_PATTERNS)

        if self.backend_id == "kafka":
            self._validate_choice("acks", self.acks, ALLOWED_ACKS)
            self._validate_choice(
                "compression_type",
                self.compression_type,
                ALLOWED_COMPRESSION_TYPES,
            )
            if self.broker_count not in ALLOWED_BROKER_COUNTS:
                raise ValueError(
                    f"broker_count must be one of {sorted(ALLOWED_BROKER_COUNTS)}, "
                    f"got {self.broker_count}"
                )
            if self.partitions <= 0:
                raise ValueError("partitions must be greater than 0")
            if self.replication_factor <= 0:
                raise ValueError("replication_factor must be greater than 0")
            if self.replication_factor > self.broker_count:
                raise ValueError(
                    "replication_factor cannot be greater than broker_count "
                    f"({self.replication_factor} > {self.broker_count})"
                )
            if not self.topic_name:
                raise ValueError("topic_name must not be empty")
            if self.batch_size <= 0:
                raise ValueError("batch_size must be greater than 0")
            if self.linger_ms < 0:
                raise ValueError("linger_ms must be greater than or equal to 0")

        if self.payload_size_bytes <= 0:
            raise ValueError("payload_size_bytes must be greater than 0")

        if self.producer_ranks < 0:
            raise ValueError("producer_ranks must be greater than or equal to 0")

        if self.consumer_ranks < 0:
            raise ValueError("consumer_ranks must be greater than or equal to 0")

        if self.producer_ranks == 0 and self.consumer_ranks == 0:
            raise ValueError(
                "At least one producer or consumer rank must be greater than 0"
            )

        self._validate_scenario_rank_layout()

        if self.virtual_devices_per_rank <= 0:
            raise ValueError("virtual_devices_per_rank must be greater than 0")

        if self.total_simulated_devices is not None and self.total_simulated_devices <= 0:
            raise ValueError("total_simulated_devices must be greater than 0 when set")

        if self.duration_sec <= 0:
            raise ValueError("duration_sec must be greater than 0")

        if self.warmup_sec < 0:
            raise ValueError("warmup_sec must be greater than or equal to 0")

        if self.drain_timeout_sec < 0:
            raise ValueError("drain_timeout_sec must be greater than or equal to 0")

        if (
            self.target_records_per_sec is not None
            and self.target_records_per_sec <= 0
        ):
            raise ValueError("target_records_per_sec must be greater than 0 when set")

        if self.latency_sample_every <= 0:
            raise ValueError("latency_sample_every must be greater than 0")

        if self.latency_clock_samples <= 0:
            raise ValueError("latency_clock_samples must be greater than 0")

        if self.latency_clock_max_uncertainty_us <= 0:
            raise ValueError(
                "latency_clock_max_uncertainty_us must be greater than 0"
            )

        if self.latency_clock_max_drift_us <= 0:
            raise ValueError("latency_clock_max_drift_us must be greater than 0")

        if self.record_envelope_enabled:
            if self.payload_mode != "fixed_size":
                raise ValueError(
                    "warm-up/drain or latency instrumentation requires "
                    "payload_mode='fixed_size'"
                )
            if self.payload_size_bytes < 32:
                raise ValueError(
                    "warm-up/drain or latency instrumentation requires "
                    "payload_size_bytes >= 32"
                )

        profile_values = (self.broker_profile_id, self.broker_profile_sha256)
        if any(profile_values) and not all(profile_values):
            raise ValueError(
                "broker_profile_id and broker_profile_sha256 must be set together"
            )

        if self.broker_profile_sha256 is not None:
            digest = self.broker_profile_sha256.strip().lower()
            if len(digest) != 64 or any(
                character not in "0123456789abcdef" for character in digest
            ):
                raise ValueError(
                    "broker_profile_sha256 must be a 64-character hexadecimal digest"
                )
            self.broker_profile_sha256 = digest

    @staticmethod
    def _normalize_text_field(name: str, value: object) -> str:
        """
        Normalize a required text field and fail early for non-string values.
        """
        if not isinstance(value, str):
            raise ValueError(f"{name} must be a string, got {type(value).__name__}")
        return value.strip()

    @staticmethod
    def _validate_choice(name: str, value: str, allowed_values: set[str]) -> None:
        """
        Validate fields whose allowed values are documented as Literal types.
        """
        if value not in allowed_values:
            allowed = ", ".join(sorted(allowed_values))
            raise ValueError(f"{name} must be one of [{allowed}], got {value!r}")

    def _validate_scenario_rank_layout(self) -> None:
        """
        Ensure each scenario has the worker ranks it needs to produce results.
        """
        if self.scenario == "ingress_ramp" and self.producer_ranks == 0:
            raise ValueError("ingress_ramp requires at least one producer rank")

        if self.scenario == "egress_only" and self.consumer_ranks == 0:
            raise ValueError("egress_only requires at least one consumer rank")

        if self.scenario in {"simultaneous", "consume_and_process"}:
            if self.producer_ranks == 0 or self.consumer_ranks == 0:
                raise ValueError(
                    f"{self.scenario} requires at least one producer and one consumer rank"
                )

    def _derive_missing_values(self) -> None:
        """
        Derive fields that were not explicitly provided.

        If total_simulated_devices is not given, compute it from the active
        worker side and virtual devices per rank. Consumer-only egress cases
        still need a nonzero simulated device count for topic prefill.
        """
        if self.total_simulated_devices is None:
            rank_scale = max(self.producer_ranks, self.consumer_ranks, 1)
            self.total_simulated_devices = (
                rank_scale * self.virtual_devices_per_rank
            )

    @property
    def total_worker_ranks(self) -> int:
        """
        Total number of producer and consumer ranks, excluding controller rank 0.
        """
        return self.producer_ranks + self.consumer_ranks

    @property
    def record_envelope_enabled(self) -> bool:
        """
        Whether records need V2 phase/correctness metadata.

        Warm-up and drain need phase markers even when timestamp collection is
        disabled for the instrumentation-overhead control cases.
        """
        return (
            self.latency_enabled
            or self.warmup_sec > 0
            or self.drain_timeout_sec > 0
        )

    @property
    def total_mpi_ranks(self) -> int:
        """
        Total MPI ranks including controller rank 0.
        """
        return 1 + self.total_worker_ranks

    @property
    def benchmark_kafka_client_count(self) -> int:
        """
        Real Kafka producer/consumer client objects created by the benchmark.

        Virtual devices are logical IDs multiplexed through producer ranks, not
        one Kafka client connection per device.
        """
        return self.producer_ranks + self.consumer_ranks

    @property
    def benchmark_client_count(self) -> int:
        """Portable producer/consumer client count."""
        return self.producer_ranks + self.consumer_ranks

    @property
    def estimated_max_client_broker_connections(self) -> int:
        """
        Coarse upper bound for benchmark client-to-broker TCP connections.

        librdkafka may open connections lazily and not every client necessarily
        talks to every broker for the whole run, so this is intentionally an
        estimate for report interpretation rather than an exact socket count.
        """
        return self.benchmark_kafka_client_count * self.broker_count

    @property
    def estimated_max_client_backend_connections(self) -> int:
        """Portable alias for the current connection upper-bound estimate."""
        if self.backend_id == "kafka":
            service_count = self.broker_count
        else:
            service_count = int(self.backend_settings.get("service_count", 1))
        return self.benchmark_client_count * service_count

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the config back into a plain dictionary.

        This is useful for logging, reporting, and writing config snapshots.
        """
        data = {
            "schema_version": self.schema_version,
            "backend_id": self.backend_id,
            "qualification_policy_id": self.qualification_policy_id,
            "mode": self.mode,
            "scenario": self.scenario,
            "payload_size_bytes": self.payload_size_bytes,
            "payload_mode": self.payload_mode,
            "send_pattern": self.send_pattern,
            "producer_ranks": self.producer_ranks,
            "consumer_ranks": self.consumer_ranks,
            "virtual_devices_per_rank": self.virtual_devices_per_rank,
            "total_simulated_devices": self.total_simulated_devices,
            "benchmark_client_count": self.benchmark_client_count,
            "estimated_max_client_backend_connections": (
                self.estimated_max_client_backend_connections
            ),
            "duration_sec": self.duration_sec,
            "warmup_sec": self.warmup_sec,
            "drain_timeout_sec": self.drain_timeout_sec,
            "target_records_per_sec": self.target_records_per_sec,
            "latency_enabled": self.latency_enabled,
            "latency_sample_every": self.latency_sample_every,
            "latency_clock_samples": self.latency_clock_samples,
            "latency_clock_max_uncertainty_us": (
                self.latency_clock_max_uncertainty_us
            ),
            "latency_clock_max_drift_us": self.latency_clock_max_drift_us,
            "broker_profile_id": self.broker_profile_id,
            "broker_profile_sha256": self.broker_profile_sha256,
            "backend_settings": self.backend_settings,
            "case_id": self.case_id,
            "campaign_id": self.campaign_id,
            "extra": self.extra,
        }
        if self.backend_id == "kafka":
            data.update(
                {
                    "broker_count": self.broker_count,
                    "partitions": self.partitions,
                    "replication_factor": self.replication_factor,
                    "topic_name": self.topic_name,
                    "acks": self.acks,
                    "compression_type": self.compression_type,
                    "batch_size": self.batch_size,
                    "linger_ms": self.linger_ms,
                    "benchmark_kafka_client_count": (
                        self.benchmark_kafka_client_count
                    ),
                    "estimated_max_client_broker_connections": (
                        self.estimated_max_client_broker_connections
                    ),
                }
            )
        return data

    def to_input_dict(self) -> dict[str, Any]:
        """
        Convert the config into a JSON object that can be loaded as input again.

        Report snapshots include derived read-only fields such as estimated
        client counts. Runtime campaign configs should omit those fields because
        they are recomputed by BenchmarkConfig during loading.
        """
        data = self.to_dict()
        for key in READ_ONLY_REPORT_FIELDS:
            data.pop(key, None)
        return data

    def to_versioned_dict(self) -> dict[str, Any]:
        """Serialize this effective config using the portable case schema."""
        return versioned_case_config(self.to_input_dict())


READ_ONLY_REPORT_FIELDS = frozenset(
    {
        "benchmark_kafka_client_count",
        "benchmark_client_count",
        "estimated_max_client_broker_connections",
        "estimated_max_client_backend_connections",
    }
)


def benchmark_config_input_dict(raw_config: dict[str, Any]) -> dict[str, Any]:
    """
    Return only constructor-supported BenchmarkConfig fields from raw input.

    This accepts report snapshots that contain derived read-only fields while
    still rejecting genuinely unknown config keys. It prevents generated report
    metadata from leaking back into the strict BenchmarkConfig constructor.
    """
    if not isinstance(raw_config, dict):
        raise ValueError("BenchmarkConfig input must be a dictionary")

    allowed_fields = {item.name for item in fields(BenchmarkConfig)}
    unknown_fields = set(raw_config) - allowed_fields
    unsupported_fields = sorted(unknown_fields - READ_ONLY_REPORT_FIELDS)
    if unsupported_fields:
        joined = ", ".join(unsupported_fields)
        raise ValueError(f"Unsupported BenchmarkConfig field(s): {joined}")

    return {key: value for key, value in raw_config.items() if key in allowed_fields}
