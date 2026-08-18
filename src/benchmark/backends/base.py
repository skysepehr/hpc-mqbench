from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

from models.benchmark_config import BenchmarkConfig


@dataclass(frozen=True, slots=True)
class BackendResourcePlan:
    """Backend-owned service and portable MPI resource requirements."""

    service_nodes: int
    monitoring_nodes: int
    producer_ranks: int
    consumer_ranks: int

    @property
    def total_mpi_ranks(self) -> int:
        return 1 + self.producer_ranks + self.consumer_ranks


class BackendAdapter(ABC):
    """Contract between the benchmark core and one messaging product."""

    backend_id: str
    adapter_version: str
    qualification_policy_ids: frozenset[str]
    supports_full_report = False

    def versioned_config_fields(self) -> frozenset[str] | None:
        """Return accepted backend namespace fields, or None for open settings."""
        return None

    def normalize_versioned_config(
        self,
        backend_config: dict[str, Any],
    ) -> dict[str, Any]:
        """Map a versioned backend namespace to BenchmarkConfig constructor fields."""
        return {"backend_settings": dict(backend_config)}

    def versioned_config_from_runtime(
        self,
        flat_config: dict[str, Any],
    ) -> dict[str, Any]:
        """Serialize adapter settings back into its versioned namespace."""
        settings = flat_config.get("backend_settings", {})
        if not isinstance(settings, dict):
            raise ValueError("backend_settings must be a dictionary")
        return dict(settings)

    def resource_plan(self, config: BenchmarkConfig) -> BackendResourcePlan:
        """Return nodes and MPI ranks required for one case."""
        service_nodes = int(config.backend_settings.get("service_count", 1))
        if service_nodes <= 0:
            raise ValueError("backend service_count must be greater than 0")
        return BackendResourcePlan(
            service_nodes=service_nodes,
            monitoring_nodes=1,
            producer_ranks=config.producer_ranks,
            consumer_ranks=config.consumer_ranks,
        )

    def runtime_environment(self, config: BenchmarkConfig) -> dict[str, str]:
        """Return shell-safe scalar settings consumed by the Slurm runner."""
        plan = self.resource_plan(config)
        return {
            "BACKEND_ID": self.backend_id,
            "MODE": config.mode,
            "SCENARIO": config.scenario,
            "SERVICE_NODE_COUNT": str(plan.service_nodes),
            "PRODUCER_RANKS": str(config.producer_ranks),
            "CONSUMER_RANKS": str(config.consumer_ranks),
        }

    def uses_coordinated_consumer_drain(self, config: BenchmarkConfig) -> bool:
        """Return whether the core must coordinate producer flush and drain."""
        return False

    @abstractmethod
    def validate_config(self, config: BenchmarkConfig) -> None:
        """Reject settings that this adapter cannot execute."""

    @abstractmethod
    def effective_settings(self, config: BenchmarkConfig) -> dict[str, Any]:
        """Return a stable, backend-specific settings snapshot."""

    @abstractmethod
    def create_producer_worker(
        self,
        *,
        config: BenchmarkConfig,
        rank: int,
        bootstrap_servers: str,
        producer_index: int,
        clock_offset_ns: int,
    ) -> Any:
        """Construct one producer worker without exposing it to the core."""

    @abstractmethod
    def create_consumer_worker(
        self,
        *,
        config: BenchmarkConfig,
        rank: int,
        bootstrap_servers: str,
        case_id: str,
        clock_offset_ns: int,
    ) -> Any:
        """Construct one consumer worker without exposing it to the core."""

    @abstractmethod
    def lifecycle_script(self, action: str, project_root: Path) -> Path | None:
        """Resolve a product lifecycle action to its implementation script."""

    @abstractmethod
    def normalize_metrics(
        self,
        benchmark_result: dict[str, Any],
    ) -> dict[str, Any]:
        """Return backend-specific evidence for backend_metrics.<backend_id>."""

    def prepare_result(self, benchmark_result: dict[str, Any]) -> None:
        """Add compatibility fields needed by this backend's legacy reports."""

    def monitoring_endpoint_specs(
        self,
        config: BenchmarkConfig,
    ) -> dict[str, Any]:
        """Describe runtime files and defaults used to discover metric endpoints."""
        return {}

    def configuration_report_lines(
        self,
        config: dict[str, Any],
    ) -> list[str]:
        """Render the backend-neutral case and workload configuration summary."""
        backend_settings = config.get("backend_settings", {})
        return [
            f"- **Backend ID:** {self.backend_id}",
            f"- **Mode:** {config.get('mode', 'unknown')}",
            f"- **Scenario:** {config.get('scenario', 'unknown')}",
            f"- **Payload Size (bytes):** {config.get('payload_size_bytes', 'unknown')}",
            f"- **Payload Mode:** {config.get('payload_mode', 'unknown')}",
            f"- **Send Pattern:** {config.get('send_pattern', 'unknown')}",
            f"- **Producer Ranks:** {config.get('producer_ranks', 'unknown')}",
            f"- **Consumer Ranks:** {config.get('consumer_ranks', 'unknown')}",
            (
                "- **Virtual Devices per Rank:** "
                f"{config.get('virtual_devices_per_rank', 'unknown')}"
            ),
            (
                "- **Total Simulated Devices:** "
                f"{config.get('total_simulated_devices', 'unknown')}"
            ),
            (
                "- **Benchmark Clients:** "
                f"{config.get('benchmark_client_count', _client_count(config))}"
            ),
            f"- **Duration (sec):** {config.get('duration_sec', 'unknown')}",
            (
                "- **Backend Settings:** `"
                + json.dumps(backend_settings, sort_keys=True)
                + "`"
            ),
        ]

    def scale_report_lines(self, config: dict[str, Any]) -> list[str]:
        """Render portable worker/client scale semantics."""
        return [
            "| Quantity | Value | Meaning |",
            "|---|---:|---|",
            (
                "| Simulated logical devices | {devices} | Logical identities "
                "multiplexed through producer workers. |"
            ).format(devices=config.get("total_simulated_devices", "unknown")),
            (
                "| Producer clients | {producers} | One backend producer client "
                "per producer MPI rank. |"
            ).format(producers=config.get("producer_ranks", "unknown")),
            (
                "| Consumer clients | {consumers} | One backend consumer client "
                "per consumer MPI rank. |"
            ).format(consumers=config.get("consumer_ranks", "unknown")),
            (
                "| Total benchmark clients | {clients} | Producer plus consumer "
                "client objects created by the benchmark. |"
            ).format(
                clients=config.get(
                    "benchmark_client_count",
                    _client_count(config),
                )
            ),
            "",
            (
                "Simulated logical devices are record identities, not one "
                "backend connection per device."
            ),
        ]

    def primary_report_markdown(
        self,
        benchmark_result: dict[str, Any],
    ) -> str:
        """Return the backend-specific primary evidence section, if available."""
        return ""

    def monitoring_report_markdown(
        self,
        monitoring: dict[str, Any],
    ) -> str:
        """Return backend-specific monitoring evidence, if available."""
        return ""

    def report_notes(self) -> tuple[str, ...]:
        """Return backend-specific explanatory notes for the case report."""
        return ()

    def latency_report_notes(self) -> tuple[str, ...]:
        """Return backend-specific latency distinctions."""
        return ()

    def producer_enqueue_label(self) -> str:
        """Name the adapter-client counter for accepted producer records."""
        return "Messages Enqueued to Backend Client"

    def qualification_result(
        self,
        benchmark_result: dict[str, Any],
    ) -> bool | None:
        """Return this backend policy's result when the report contains it."""
        return None

    def identity(self, config: BenchmarkConfig) -> dict[str, Any]:
        profile_id, profile_sha256 = self.profile_identity(config)
        return {
            "backend_id": self.backend_id,
            "adapter_version": self.adapter_version,
            "product_version": _product_version(config.extra),
            "profile_id": profile_id,
            "profile_sha256": profile_sha256,
        }

    def profile_identity(
        self,
        config: BenchmarkConfig,
    ) -> tuple[str | None, str | None]:
        return config.broker_profile_id, config.broker_profile_sha256

    def identity_from_dict(self, config: dict[str, Any]) -> dict[str, Any]:
        extra = config.get("extra")
        return {
            "backend_id": self.backend_id,
            "adapter_version": self.adapter_version,
            "product_version": _product_version(extra),
            "profile_id": config.get("broker_profile_id"),
            "profile_sha256": config.get("broker_profile_sha256"),
        }


def _product_version(extra: Any) -> str:
    if not isinstance(extra, dict):
        return "not_recorded"
    value = extra.get("backend_product_version")
    return str(value).strip() if value else "not_recorded"


def _client_count(config: dict[str, Any]) -> int:
    return int(config.get("producer_ranks", 0) or 0) + int(
        config.get("consumer_ranks", 0) or 0
    )
