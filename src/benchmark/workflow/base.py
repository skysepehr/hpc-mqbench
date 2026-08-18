from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Protocol


COMMON_MEASUREMENT_CONTRACT: dict[str, Any] = {
    "contract_id": "measurement.messaging.reproducible.v1",
    "qualification_policy_id": "qualification.application.v1",
    "timing": {
        "warmup_sec": 15,
        "measurement_sec": 30,
        "drain_timeout_sec": 60,
    },
    "case_isolation": {
        "restart_backend_between_cases": True,
        "clean_ram_backed_storage_between_cases": True,
    },
    "producer_delivery": {
        "backlog_event": "pending producer delivery callbacks at flush start",
        "backlog_denominator": "measurement-period send attempts",
    },
    "consumer_drain": {
        "poll_during_producer_flush": True,
        "start_after_all_producer_flushes": True,
        "target": "callback-confirmed measurement-period deliveries",
        "stop_condition": "target consumed or drain timeout",
    },
    "latency": {
        "deterministic_sample_every": 10,
        "record_identity_on_every_record": True,
        "clock_samples_before_and_after": 100,
        "clock_uncertainty_limit_us": 250.0,
        "clock_drift_limit_us": 250.0,
    },
    "qualification_thresholds": {
        "backlog_percent_max": 5.0,
        "flush_duration_sec_max": 10.0,
        "failed_send_percent_max": 0.1,
    },
    "correctness": {
        "missing_after_drain": 0,
        "duplicate": 0,
        "out_of_order": 0,
        "invalid_envelope": 0,
        "unexplained_surplus": 0,
        "eligibility_is_separate_from_qualification": True,
    },
}


class WorkflowError(RuntimeError):
    """A deterministic workflow safety or acceptance check failed."""


@dataclass(frozen=True, slots=True)
class StageSpec:
    name: str
    phase: str
    description: str
    prerequisites: tuple[str, ...] = ()


@dataclass(slots=True)
class StageOutcome:
    status: str = "completed"
    artifacts: list[str] = field(default_factory=list)
    result_paths: list[str] = field(default_factory=list)
    job_ids: list[str] = field(default_factory=list)
    input_files: list[str] = field(default_factory=list)
    selected_configs: list[str] = field(default_factory=list)
    selected_profiles: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)
    message: str = ""

    def __post_init__(self) -> None:
        # Stage inputs are immutable evidence too. Keep them in both the
        # dedicated field and details so backend recovery can reconstruct a
        # submission even for workflow states written by older controllers.
        if self.input_files and "input_files" not in self.details:
            self.details["input_files"] = list(self.input_files)


class WorkflowRuntime(Protocol):
    backend_id: str
    dry_run: bool

    def path(self, *parts: str) -> str:
        """Return an absolute path inside this workflow's private directory."""

    def repository_path(self, *parts: str) -> str:
        """Return an absolute repository path."""

    def submission_environment(self) -> dict[str, str | None]:
        """Return the frozen scheduler environment for every submission."""

    def run_command(
        self,
        command: list[str],
        *,
        environment: dict[str, str | None] | None = None,
        cwd: str | None = None,
    ) -> str:
        """Run a checked local command and return its standard output."""

    def submit_manifest_batches(
        self,
        *,
        stage_name: str,
        manifest_paths: list[str],
        run_id: str,
        allow_profile_changes: bool = False,
        independent: bool = False,
        repair_id: str | None = None,
    ) -> StageOutcome:
        """Submit one backend-batch job per manifest, optionally independently."""

    def outcome_from_submission_ledger(
        self,
        *,
        ledger_path: str,
        expected_jobs: int,
        result_paths: list[str],
        input_files: list[str],
    ) -> StageOutcome:
        """Recover a complete backend-owned durable submission ledger."""

    def wait_for_stage_jobs(self, submission_stage: str) -> StageOutcome:
        """Wait for all recorded jobs in a submission stage."""

    def verify_results(
        self,
        *,
        manifest_path: str,
        results_roots: list[str],
        expected_case_count: int,
        allow_latency_disabled: bool = False,
        max_clock_invalid_cases: int = 0,
        max_ineligible_cases: int = 0,
        min_eligible_repeats_per_workload: int | None = None,
        repair_clock_invalid_cases: bool = True,
        repair_submission_stage: str | None = None,
        selected_results_group: str | None = None,
    ) -> StageOutcome:
        """Verify result integrity and the stage's eligible-evidence threshold."""

    def stage_details(self, stage_name: str) -> dict[str, Any]:
        """Return persisted details for an earlier stage."""

    def stage_record(self, stage_name: str) -> dict[str, Any]:
        """Return a copy of one complete persisted stage record."""

    def deferred(self, message: str, **details: Any) -> StageOutcome:
        """Return a dry-run-only deferred outcome."""


class BackendWorkflow(ABC):
    """Backend-owned campaign stages executed by the common state machine."""

    backend_id: str

    @abstractmethod
    def stages(self) -> tuple[StageSpec, ...]:
        """Return the deterministic stage graph in execution order."""

    @abstractmethod
    def required_files(self) -> tuple[str, ...]:
        """Return repository-relative implementation inputs checked in preflight."""

    @abstractmethod
    def execute(self, stage: StageSpec, runtime: WorkflowRuntime) -> StageOutcome:
        """Execute one backend-specific stage through common runtime services."""

    def phase_aliases(self) -> dict[str, tuple[str, ...]]:
        specs = self.stages()
        return {
            "all": tuple(item.name for item in specs),
            "phase1": tuple(item.name for item in specs if item.phase in {"setup", "phase1"}),
            "validation": tuple(
                item.name
                for item in specs
                if item.phase in {"setup", "phase1", "validation", "v1-finalize"}
            ),
            "v1": tuple(item.name for item in specs if not item.phase.startswith("v2")),
            "v2": tuple(item.name for item in specs if item.phase == "setup" or item.phase.startswith("v2")),
        }

    def resolve_phase(self, phase: str) -> tuple[str, ...]:
        aliases = self.phase_aliases()
        if phase in aliases:
            return aliases[phase]
        names = {item.name for item in self.stages()}
        if phase in names:
            return (phase,)
        choices = sorted({*aliases, *names})
        raise WorkflowError(
            f"Unknown phase/stage {phase!r} for {self.backend_id}; "
            f"choose one of: {', '.join(choices)}"
        )

    def retry_failed_batches(
        self,
        submission_stage: str,
        failed_job_ids: tuple[str, ...],
        runtime: WorkflowRuntime,
    ) -> StageOutcome:
        """Submit isolated repair manifests for failed Slurm allocations."""
        raise WorkflowError(
            f"{self.backend_id} does not implement failed-batch recovery"
        )

    def repair_invalid_cases(
        self,
        submission_stage: str,
        case_ids: tuple[str, ...],
        runtime: WorkflowRuntime,
    ) -> StageOutcome:
        """Submit an immutable repair manifest for completed but invalid cases."""
        raise WorkflowError(
            f"{self.backend_id} does not implement invalid-case recovery"
        )
