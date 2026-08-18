from __future__ import annotations

import csv
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import time
from typing import Any, Iterable

from src.benchmark.backends import get_backend
from src.benchmark.core.result_schema import common_measurement_lifecycle_failures
from src.benchmark.workflow.base import (
    COMMON_MEASUREMENT_CONTRACT,
    BackendWorkflow,
    StageOutcome,
    StageSpec,
    WorkflowError,
)


STATE_FORMAT = "messaging-benchmark.workflow-state.v2"
FINAL_REPORT_FORMAT = "messaging-benchmark.workflow-result.v2"
MACHINE_OUTPUT_CONTRACT = {
    "json_source_of_truth": True,
    "csv_primary_comparison_format": True,
    "required_outputs": [
        "per-case final_report.json",
        "campaign/stage CSV files",
        "validation summary CSV",
        "provenance",
        "manifests",
        "checksums",
        "workflow state",
    ],
    "presentation_pipeline": {
        "required_for_success": False,
        "executed_by_workflow": False,
        "artifact_types": ["figures", "tables", "Markdown", "HTML", "LaTeX", "PDF"],
    },
}
EXECUTION_SETTINGS_FORMAT = "messaging-benchmark.workflow-execution-settings.v1"
SLURM_SETTING_ENV = {
    "account": "SLURM_ACCOUNT",
    "partition": "SLURM_PARTITION",
    "qos": "SLURM_QOS",
    "time": "SLURM_TIME",
    "constraint": "SLURM_CONSTRAINT",
}
SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,119}$")
TERMINAL_SUCCESS = {"COMPLETED"}
TERMINAL_FAILURE = {
    "BOOT_FAIL",
    "CANCELLED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "REVOKED",
    "TIMEOUT",
}


class WorkflowEngine:
    """Backend-neutral, durable stage runner for one benchmark campaign."""

    def __init__(
        self,
        *,
        project_root: Path,
        output_root: Path,
        workflow_id: str,
        backend: BackendWorkflow,
        dry_run: bool,
        resume: bool,
        requested_phase: str,
        poll_seconds: int = 30,
        max_wait_seconds: int = 0,
        retry_failed_batches: bool = False,
        max_case_repair_attempts: int = 2,
        execution_setting_overrides: dict[str, str] | None = None,
        accepted_implementation_updates: Iterable[str] = (),
        implementation_update_reason: str = "",
    ) -> None:
        self.project_root = project_root.resolve()
        self.output_root = output_root.resolve()
        self.workflow_id = workflow_id
        self.backend = backend
        self.backend_id = backend.backend_id
        self.dry_run = dry_run
        self.resume = resume
        self.requested_phase = requested_phase
        self.poll_seconds = poll_seconds
        self.max_wait_seconds = max_wait_seconds
        self.retry_failed_batches = retry_failed_batches
        self.max_case_repair_attempts = max_case_repair_attempts
        self.execution_setting_overrides = dict(execution_setting_overrides or {})
        self.accepted_implementation_updates = tuple(
            str(item) for item in accepted_implementation_updates
        )
        self.implementation_update_reason = implementation_update_reason.strip()
        self._lock_handle: Any | None = None
        self._validate_identity()
        self.run_root = self.output_root / self.backend_id / self.workflow_id
        self.state_path = self.run_root / "workflow_state.json"
        self.lock_path = (
            self.output_root / self.backend_id / f".{self.workflow_id}.workflow.lock"
        )
        self._acquire_run_lock()
        try:
            self.state = self._open_state()
        except BaseException:
            self._release_run_lock()
            raise
        if self.resume:
            self.state["requested_phase"] = self.requested_phase
            self.state["resumed_at"] = _timestamp()
            self._write_state(self.state)

    def _validate_identity(self) -> None:
        if not SAFE_ID.fullmatch(self.workflow_id):
            raise WorkflowError(
                "workflow ID must use 1-120 letters, digits, '.', '_', or '-'"
            )
        if self.poll_seconds <= 0:
            raise WorkflowError("poll interval must be positive")
        if self.max_wait_seconds < 0:
            raise WorkflowError("maximum wait must not be negative")
        if self.max_case_repair_attempts < 0:
            raise WorkflowError("maximum case-repair attempts must not be negative")
        if self.accepted_implementation_updates and not self.resume:
            raise WorkflowError("implementation updates require resume mode")
        if self.accepted_implementation_updates and not self.implementation_update_reason:
            raise WorkflowError("implementation update reason is required")
        unknown_settings = sorted(
            set(self.execution_setting_overrides) - set(SLURM_SETTING_ENV)
        )
        if unknown_settings:
            raise WorkflowError(
                "unknown execution setting(s): " + ", ".join(unknown_settings)
            )
        for name, value in self.execution_setting_overrides.items():
            _validate_scheduler_value(name, value)
        if self.output_root in {Path("/"), self.project_root, self.project_root / "results"}:
            raise WorkflowError(f"unsafe workflow output root: {self.output_root}")
        for historical in (
            self.project_root / "results" / "sweeps",
            self.project_root / "results" / "tuning_v2",
            self.project_root / "results" / "published",
            self.project_root / "results" / "runs",
        ):
            if self.output_root == historical.resolve():
                raise WorkflowError(
                    f"workflow state may not be written into historical results: {historical}"
                )

    def _open_state(self) -> dict[str, Any]:
        if self.resume:
            if not self.state_path.is_file():
                raise WorkflowError(
                    f"cannot resume; workflow state is missing: {self.state_path}"
                )
            state = _read_json(self.state_path)
            self._validate_loaded_state(state)
            return state
        if self.run_root.exists():
            raise WorkflowError(
                f"workflow directory already exists; use --resume or a new ID: {self.run_root}"
            )
        for directory in (
            self.run_root,
            self.run_root / "inputs",
            self.run_root / "analysis",
            self.run_root / "artifacts",
            self.run_root / "logs",
            self.run_root / "slurm",
        ):
            directory.mkdir(parents=True, exist_ok=False)
        now = _timestamp()
        self.execution_settings = _new_execution_settings(
            self.execution_setting_overrides
        )
        state = {
            "format": STATE_FORMAT,
            "workflow_id": self.workflow_id,
            "backend_id": self.backend_id,
            "mode": "dry-run" if self.dry_run else "run",
            "status": "initialized",
            "requested_phase": self.requested_phase,
            "current_stage": None,
            "created_at": now,
            "updated_at": now,
            "project_root": _display(self.project_root, self.project_root),
            "workflow_root": _display(self.run_root, self.project_root),
            "common_measurement_contract": COMMON_MEASUREMENT_CONTRACT,
            "execution_settings": self.execution_settings,
            "machine_output_contract": MACHINE_OUTPUT_CONTRACT,
            "reporting": {
                "required_for_success": False,
                "executed_by_workflow": False,
                "status": "not_requested",
            },
            "completed_stages": [],
            "stages": {},
            "job_ids": [],
            "result_paths": [],
            "selected_configs": [],
            "selected_profiles": [],
            "input_checksums": {},
            "failures": [],
            "recovery_policy": {
                "automatic": self.retry_failed_batches,
                "max_case_repair_attempts": self.max_case_repair_attempts,
                "raw_results_are_immutable": True,
            },
        }
        self._write_state(state)
        return state

    def _validate_loaded_state(self, state: dict[str, Any]) -> None:
        checks = {
            "format": STATE_FORMAT,
            "workflow_id": self.workflow_id,
            "backend_id": self.backend_id,
            "mode": "dry-run" if self.dry_run else "run",
        }
        for field, expected in checks.items():
            if state.get(field) != expected:
                raise WorkflowError(
                    f"resume state {field} is {state.get(field)!r}, expected {expected!r}"
                )
        recorded_settings = state.get("execution_settings")
        if recorded_settings is None:
            # Compatibility for workflows created before scheduler capture was
            # introduced. The adoption is explicit in state and then frozen.
            self.execution_settings = _new_execution_settings(
                self.execution_setting_overrides
            )
            state["execution_settings"] = self.execution_settings
            state["execution_settings_adoption"] = {
                "timestamp": _timestamp(),
                "reason": "legacy workflow state predates execution-settings capture",
            }
        else:
            self.execution_settings = _validated_recorded_execution_settings(
                recorded_settings
            )
            drift = []
            recorded_slurm = _dict(self.execution_settings.get("slurm"))
            for name, requested in self.execution_setting_overrides.items():
                if recorded_slurm.get(name) != requested:
                    drift.append(
                        f"{name}: recorded={recorded_slurm.get(name)!r}, "
                        f"requested={requested!r}"
                    )
            if drift:
                raise WorkflowError(
                    "execution settings are immutable on resume: " + "; ".join(drift)
                )
        if self.accepted_implementation_updates:
            self._apply_implementation_updates(state)
        state["recovery_policy"] = {
            "automatic": self.retry_failed_batches,
            "max_case_repair_attempts": self.max_case_repair_attempts,
            "raw_results_are_immutable": True,
        }
        self._verify_recorded_checksums(state)

    def close(self) -> dict[str, Any]:
        """Release the workflow lock without executing any benchmark stage."""
        self._release_run_lock()
        return self.state

    def run(self) -> dict[str, Any]:
        try:
            self._global_preflight()
            selected = set(self.backend.resolve_phase(self.requested_phase))
            for spec in self.backend.stages():
                if spec.name not in selected:
                    continue
                self._run_stage(spec)
            self.state["current_stage"] = None
            self.state["status"] = (
                "dry_run_validated" if self.dry_run else "completed"
            )
            self._write_state(self.state)
            self._write_final_artifacts()
            return self.state
        except BaseException as exc:
            self._record_failure(exc)
            raise
        finally:
            self._release_run_lock()

    def _acquire_run_lock(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.seek(0)
            owner = handle.read().strip() or "owner metadata unavailable"
            handle.close()
            raise WorkflowError(
                f"workflow {self.workflow_id!r} is already active: {owner}"
            ) from exc
        metadata = {
            "workflow_id": self.workflow_id,
            "backend_id": self.backend_id,
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "mode": "dry-run" if self.dry_run else "run",
            "acquired_at": _timestamp(),
        }
        handle.seek(0)
        handle.truncate()
        json.dump(metadata, handle, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
        self._lock_handle = handle

    def _release_run_lock(self) -> None:
        handle = self._lock_handle
        if handle is None:
            return
        self._lock_handle = None
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()

    def _global_preflight(self) -> None:
        get_backend(self.backend_id)
        required = [self.project_root / item for item in self.backend.required_files()]
        missing = [str(path) for path in required if not path.is_file()]
        if missing:
            raise WorkflowError("required workflow files are missing: " + ", ".join(missing))
        required_commands = ["python3"]
        if not self.dry_run:
            required_commands.extend(
                [
                    "sbatch",
                    "squeue",
                    "sacct",
                    "scontrol",
                    "srun",
                    "mpirun",
                ]
            )
        absent = [name for name in required_commands if shutil.which(name) is None]
        if absent:
            raise WorkflowError(
                "required command(s) are unavailable: " + ", ".join(absent)
            )
        if self.run_root.is_symlink() or self.output_root.is_symlink():
            raise WorkflowError("workflow result directories must not be symbolic links")
        self._record_input_checksums(str(path) for path in required)
        self._verify_recorded_checksums(self.state)
        self._write_state(self.state)

    def _run_stage(self, spec: StageSpec) -> None:
        record = self.state["stages"].get(spec.name, {})
        accepted_statuses = {"completed"}
        if self.dry_run:
            accepted_statuses.update({"dry_run_completed", "deferred"})
        if record.get("status") in accepted_statuses:
            self._verify_stage_artifacts(spec.name, record)
            print(f"[workflow] skip valid completed stage: {spec.name}")
            return
        if record.get("status") == "running":
            record["recovery_note"] = (
                "stage was interrupted; durable ledgers and output checks are reused"
            )
        if not self.dry_run:
            incomplete = [
                name
                for name in spec.prerequisites
                if self.state["stages"].get(name, {}).get("status") != "completed"
            ]
            if incomplete:
                raise WorkflowError(
                    f"stage {spec.name} requires completed stage(s): "
                    + ", ".join(incomplete)
                )
        self._verify_recorded_checksums(self.state)
        started = _timestamp()
        self.state["current_stage"] = spec.name
        self.state["status"] = "running"
        self.state["stages"][spec.name] = {
            **record,
            "phase": spec.phase,
            "description": spec.description,
            "status": "running",
            "started_at": started,
            "attempt_count": int(record.get("attempt_count", 0)) + 1,
        }
        self._write_state(self.state)
        print(f"[workflow] stage: {spec.name} - {spec.description}")
        outcome = self.backend.execute(spec, self)
        if outcome.status not in {"completed", "deferred"}:
            raise WorkflowError(
                f"stage {spec.name} returned unsupported status {outcome.status!r}"
            )
        final_status = outcome.status
        if self.dry_run and final_status == "completed":
            final_status = "dry_run_completed"
        finished = _timestamp()
        stage_record = {
            **self.state["stages"][spec.name],
            "status": final_status,
            "completed_at": finished,
            "artifacts": _unique(outcome.artifacts),
            "result_paths": _unique(outcome.result_paths),
            "job_ids": _unique(outcome.job_ids),
            "input_files": _unique(outcome.input_files),
            "selected_configs": _unique(outcome.selected_configs),
            "selected_profiles": _unique(outcome.selected_profiles),
            "details": outcome.details,
            "message": outcome.message,
        }
        self.state["stages"][spec.name] = stage_record
        if final_status in {"completed", "dry_run_completed"}:
            self.state["completed_stages"] = _unique(
                [*self.state["completed_stages"], spec.name]
            )
        self.state["job_ids"] = _unique(
            [*self.state["job_ids"], *outcome.job_ids]
        )
        self.state["result_paths"] = _unique(
            [*self.state["result_paths"], *outcome.result_paths]
        )
        self.state["selected_configs"] = _unique(
            [*self.state["selected_configs"], *outcome.selected_configs]
        )
        self.state["selected_profiles"] = _unique(
            [*self.state["selected_profiles"], *outcome.selected_profiles]
        )
        self._record_input_checksums(outcome.input_files)
        self.state["current_stage"] = None
        self._write_state(self.state)

    def path(self, *parts: str) -> str:
        path = self.run_root.joinpath(*parts).resolve()
        _require_within(path, self.run_root)
        return str(path)

    def repository_path(self, *parts: str) -> str:
        path = self.project_root.joinpath(*parts).resolve()
        _require_within(path, self.project_root)
        return str(path)

    def submission_environment(self) -> dict[str, str | None]:
        slurm = _dict(self.execution_settings.get("slurm"))
        environment = {
            environment_name: (
                str(slurm[setting_name])
                if slurm.get(setting_name) is not None
                else None
            )
            for setting_name, environment_name in SLURM_SETTING_ENV.items()
        }
        # The facade loaded the local HPC environment before these values were
        # frozen. Lower-level submitters must not source it again and override
        # an explicit command-line selection.
        environment["SOURCE_HPC_ENV_FILE"] = "0"
        return environment

    def run_command(
        self,
        command: list[str],
        *,
        environment: dict[str, str | None] | None = None,
        cwd: str | None = None,
    ) -> str:
        if not command:
            raise WorkflowError("empty workflow command")
        env = os.environ.copy()
        if environment:
            for key, value in environment.items():
                normalized_key = str(key)
                if value is None:
                    env.pop(normalized_key, None)
                else:
                    env[normalized_key] = str(value)
        started = _timestamp()
        completed = subprocess.run(
            command,
            cwd=cwd or str(self.project_root),
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        entry = {
            "started_at": started,
            "completed_at": _timestamp(),
            "command": command,
            "cwd": cwd or str(self.project_root),
            "environment_overrides": environment or {},
            "returncode": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
        }
        self._append_command_log(entry)
        if completed.stdout:
            print(completed.stdout.rstrip())
        if completed.returncode != 0:
            if completed.stderr:
                print(completed.stderr.rstrip(), file=sys.stderr)
            raise WorkflowError(
                f"command failed with exit {completed.returncode}: "
                + " ".join(command)
            )
        return completed.stdout

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
        if not manifest_paths:
            raise WorkflowError(f"{stage_name}: no batch manifests")
        if repair_id is not None and not SAFE_ID.fullmatch(repair_id):
            raise WorkflowError(f"{stage_name}: invalid repair ID {repair_id!r}")
        ledger = Path(self.path("slurm", f"{stage_name}_jobs.csv"))
        existing = _read_csv(ledger) if ledger.is_file() else []
        raw_results_root = self.project_root / "results" / "runs" / self.backend_id / run_id
        if (
            not self.dry_run
            and repair_id is None
            and not existing
            and raw_results_root.exists()
        ):
            raise WorkflowError(
                f"{stage_name}: raw results directory already exists without this "
                f"workflow's submission ledger: {raw_results_root}"
            )
        expected_resolved = [str(Path(path).resolve()) for path in manifest_paths]
        if existing:
            recorded = [str(Path(row["manifest_path"]).resolve()) for row in existing]
            if recorded != expected_resolved[: len(recorded)]:
                raise WorkflowError(
                    f"{stage_name}: durable submission ledger differs from manifests"
                )
            if len(existing) > len(manifest_paths):
                raise WorkflowError(f"{stage_name}: submission ledger has extra jobs")
        rows = list(existing)
        previous_job = rows[-1]["job_id"] if rows else ""
        for sequence, manifest in enumerate(manifest_paths[len(rows) :], start=len(rows) + 1):
            manifest_path = Path(manifest).resolve()
            if not manifest_path.is_file():
                raise WorkflowError(f"batch manifest is missing: {manifest_path}")
            environment = {
                **self.submission_environment(),
                "DRY_RUN": "1" if self.dry_run else "0",
                "RUN_PREFLIGHT": "1" if sequence == 1 else "0",
                "BACKEND_BATCH_RUN_ID": run_id,
                "BACKEND_BATCH_ALLOW_PROFILE_CHANGES": (
                    "1" if allow_profile_changes else "0"
                ),
                "BACKEND_BATCH_REPAIR_ID": repair_id or "",
                "SLURM_DEPENDENCY": (
                    f"afterok:{previous_job}"
                    if (
                        not independent
                        and previous_job
                        and not previous_job.startswith("dry-run-")
                    )
                    else ""
                ),
                "SLURM_JOB_NAME": f"{self.backend_id}-{stage_name}-{sequence:02d}",
                "PROMETHEUS_SCRAPE_INTERVAL_SEC": "1",
                "MONITORING_RANGE_STEP_SEC": "1",
                "ENABLE_BROKER_PROCESS_MONITOR": "1",
                "BENCHMARK_REPORT_MODE": "machine",
                "SKIP_MONITORING_GRAPHS": "1",
            }
            intent = Path(
                self.path(
                    "slurm", f"{stage_name}_{sequence:02d}.submission-intent.json"
                )
            )
            if intent.exists():
                raise WorkflowError(
                    f"{stage_name}: unresolved submission intent for job {sequence}; "
                    "reconcile it with Slurm before resuming to avoid a duplicate job"
                )
            _write_json_atomic(
                intent,
                {
                    "format": "messaging-benchmark.submission-intent.v1",
                    "stage": stage_name,
                    "sequence": sequence,
                    "manifest_path": str(manifest_path),
                    "manifest_sha256": _sha256(manifest_path),
                    "dependency": environment["SLURM_DEPENDENCY"],
                    "created_at": _timestamp(),
                },
            )
            output = self.run_command(
                [
                    self.repository_path("scripts", "submit_backend_batch.sh"),
                    self.backend_id,
                    str(manifest_path),
                ],
                environment=environment,
            )
            if self.dry_run:
                job_id = f"dry-run-{stage_name}-{sequence}"
            else:
                matches = re.findall(r"Submitted batch job\s+(\d+)", output)
                if not matches:
                    raise WorkflowError(
                        f"{stage_name}: could not parse Slurm job ID for {manifest_path}"
                    )
                job_id = matches[-1]
            rows.append(
                {
                    "sequence": sequence,
                    "manifest_path": str(manifest_path),
                    "manifest_sha256": _sha256(manifest_path),
                    "job_id": job_id,
                    "dependency": environment["SLURM_DEPENDENCY"],
                    "submitted_at": _timestamp(),
                }
            )
            _write_csv(
                ledger,
                (
                    "sequence",
                    "manifest_path",
                    "manifest_sha256",
                    "job_id",
                    "dependency",
                    "submitted_at",
                ),
                rows,
            )
            intent.unlink()
            if not independent:
                previous_job = job_id
        if len(rows) != len(manifest_paths):
            raise WorkflowError(f"{stage_name}: incomplete durable submission ledger")
        return StageOutcome(
            artifacts=[str(ledger)],
            result_paths=[
                self.repository_path("results", "runs", self.backend_id, run_id)
            ],
            job_ids=[row["job_id"] for row in rows],
            input_files=manifest_paths,
            details={
                "job_count": len(rows),
                "run_id": run_id,
                "submission_mode": "independent" if independent else "sequential",
                "repair_id": repair_id,
            },
        )

    def outcome_from_submission_ledger(
        self,
        *,
        ledger_path: str,
        expected_jobs: int,
        result_paths: list[str],
        input_files: list[str],
    ) -> StageOutcome:
        ledger = Path(ledger_path)
        if not ledger.is_file():
            raise WorkflowError(f"submission ledger is missing: {ledger}")
        delimiter = "\t" if ledger.suffix == ".tsv" else ","
        with ledger.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle, delimiter=delimiter))
        if len(rows) != expected_jobs:
            raise WorkflowError(
                f"submission ledger contains {len(rows)} job(s), expected {expected_jobs}; "
                "refusing an ambiguous resubmission"
            )
        job_ids = [str(row.get("job_id", "")).strip() for row in rows]
        if not all(job_ids):
            raise WorkflowError("submission ledger contains an empty job ID")
        return StageOutcome(
            artifacts=[str(ledger)],
            result_paths=result_paths,
            job_ids=job_ids,
            input_files=input_files,
            details={"job_count": len(job_ids)},
        )

    def wait_for_stage_jobs(self, submission_stage: str) -> StageOutcome:
        record = self.state["stages"].get(submission_stage, {})
        job_ids = [str(item) for item in record.get("job_ids", [])]
        if not job_ids:
            if self.dry_run:
                return self.deferred(
                    f"no real jobs are polled during dry-run ({submission_stage})"
                )
            raise WorkflowError(f"{submission_stage}: no recorded Slurm job IDs")
        if self.dry_run:
            return StageOutcome(
                status="deferred",
                job_ids=job_ids,
                message="Slurm polling is intentionally skipped in dry-run mode",
            )
        started = time.monotonic()
        numeric_ids = [item for item in job_ids if item.isdigit()]
        if len(numeric_ids) != len(job_ids):
            raise WorkflowError(f"{submission_stage}: non-numeric Slurm job ID")
        while True:
            active = self._active_jobs(numeric_ids)
            if not active:
                break
            if self.max_wait_seconds and time.monotonic() - started > self.max_wait_seconds:
                raise WorkflowError(
                    f"timed out waiting for Slurm jobs: {', '.join(sorted(active))}"
                )
            print(
                f"[workflow] waiting for {submission_stage}: "
                + ", ".join(f"{job}={state}" for job, state in sorted(active.items()))
            )
            time.sleep(self.poll_seconds)
        statuses = self._terminal_job_states(numeric_ids)
        failures = {
            job_id: state
            for job_id, state in statuses.items()
            if state not in TERMINAL_SUCCESS
        }
        if failures:
            if not self.retry_failed_batches:
                raise WorkflowError(
                    "Slurm stage failed: "
                    + ", ".join(
                        f"{job}={state}" for job, state in sorted(failures.items())
                    )
                    + "; resume with --retry-failed-batches after reviewing the failure"
                )
            repair = self.backend.retry_failed_batches(
                submission_stage,
                tuple(sorted(failures)),
                self,
            )
            if repair.status != "completed":
                raise WorkflowError(
                    f"{submission_stage}: failed-batch recovery did not complete"
                )
            if not repair.job_ids:
                if repair.details.get("repair_not_required") is not True:
                    raise WorkflowError(
                        f"{submission_stage}: failed-batch recovery submitted no jobs"
                    )
                return StageOutcome(
                    job_ids=job_ids,
                    details={
                        "terminal_states": statuses,
                        "failed_original_jobs": failures,
                        "repair": repair.details,
                    },
                )
            repair_ids = [str(item) for item in repair.job_ids]
            repair_states = self._wait_for_job_ids(
                repair_ids,
                f"{submission_stage} repair",
            )
            combined = {**statuses, **repair_states}
            details = {
                "terminal_states": combined,
                "failed_original_jobs": failures,
                "repair": repair.details,
            }
            return StageOutcome(
                artifacts=repair.artifacts,
                result_paths=repair.result_paths,
                job_ids=[*job_ids, *repair_ids],
                input_files=repair.input_files,
                details=details,
            )
        return StageOutcome(job_ids=job_ids, details={"terminal_states": statuses})

    def _wait_for_job_ids(self, job_ids: list[str], label: str) -> dict[str, str]:
        if not job_ids or not all(item.isdigit() for item in job_ids):
            raise WorkflowError(f"{label}: invalid Slurm repair job ID")
        started = time.monotonic()
        while True:
            active = self._active_jobs(job_ids)
            if not active:
                break
            if self.max_wait_seconds and time.monotonic() - started > self.max_wait_seconds:
                raise WorkflowError(
                    f"timed out waiting for {label}: {', '.join(sorted(active))}"
                )
            print(
                f"[workflow] waiting for {label}: "
                + ", ".join(
                    f"{job}={state}" for job, state in sorted(active.items())
                )
            )
            time.sleep(self.poll_seconds)
        states = self._terminal_job_states(job_ids)
        failed = {job: state for job, state in states.items() if state not in TERMINAL_SUCCESS}
        if failed:
            raise WorkflowError(
                f"{label} failed: "
                + ", ".join(f"{job}={state}" for job, state in sorted(failed.items()))
            )
        return states

    def _active_jobs(self, job_ids: list[str]) -> dict[str, str]:
        completed = subprocess.run(
            [
                "squeue",
                "--noheader",
                "--jobs",
                ",".join(job_ids),
                "--format=%i|%T",
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if completed.returncode != 0:
            raise WorkflowError("squeue failed: " + completed.stderr.strip())
        output: dict[str, str] = {}
        for line in completed.stdout.splitlines():
            parts = line.strip().split("|", 1)
            if len(parts) == 2 and parts[0] in job_ids:
                output[parts[0]] = _normalize_slurm_state(parts[1])
        return output

    def _terminal_job_states(self, job_ids: list[str]) -> dict[str, str]:
        completed = subprocess.run(
            [
                "sacct",
                "--noheader",
                "--parsable2",
                "--jobs",
                ",".join(job_ids),
                "--format=JobIDRaw,State,ExitCode",
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if completed.returncode != 0:
            raise WorkflowError("sacct failed: " + completed.stderr.strip())
        states: dict[str, str] = {}
        exits: dict[str, str] = {}
        for line in completed.stdout.splitlines():
            parts = line.strip().split("|")
            if len(parts) < 3 or parts[0] not in job_ids:
                continue
            states[parts[0]] = _normalize_slurm_state(parts[1])
            exits[parts[0]] = parts[2]
        missing = sorted(set(job_ids) - set(states))
        if missing:
            raise WorkflowError(
                "sacct has no terminal record for job(s): " + ", ".join(missing)
            )
        for job_id in job_ids:
            if exits[job_id] != "0:0" and states[job_id] == "COMPLETED":
                states[job_id] = f"COMPLETED_EXIT_{exits[job_id]}"
        return states

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
        if self.dry_run:
            return self.deferred(
                "result verification requires completed HPC reports",
                expected_case_count=expected_case_count,
                manifest_path=manifest_path,
            )
        manifest = _read_csv(Path(manifest_path))
        if len(manifest) != expected_case_count:
            raise WorkflowError(
                f"manifest has {len(manifest)} rows, expected {expected_case_count}"
            )
        id_field = "case_id" if "case_id" in manifest[0] else "config_id"
        expected_ids = [str(row.get(id_field, "")).strip() for row in manifest]
        if not all(expected_ids) or len(set(expected_ids)) != len(expected_ids):
            raise WorkflowError("manifest case IDs are empty or duplicated")
        if max_clock_invalid_cases < 0:
            raise WorkflowError("maximum clock-invalid case count must not be negative")
        if max_ineligible_cases < 0:
            raise WorkflowError("maximum ineligible case count must not be negative")
        effective_max_ineligible_cases = max(
            max_ineligible_cases,
            max_clock_invalid_cases,
        )
        if (
            min_eligible_repeats_per_workload is not None
            and min_eligible_repeats_per_workload <= 0
        ):
            raise WorkflowError("minimum eligible repeats must be positive")
        group = selected_results_group or str(self.state.get("current_stage") or "verify")
        if not SAFE_ID.fullmatch(group):
            raise WorkflowError(f"invalid selected-results group: {group!r}")

        repair_events: list[dict[str, Any]] = []
        repair_artifacts: list[str] = []
        repair_inputs: list[str] = []
        repair_job_ids: list[str] = []
        while True:
            pending_repair = (
                self._pending_case_repair_record(repair_submission_stage)
                if repair_submission_stage
                else None
            )
            if pending_repair is not None:
                record_path, event = pending_repair
                job_ids = [str(item) for item in event.get("job_ids", [])]
                if not job_ids:
                    raise WorkflowError(
                        f"{repair_submission_stage}: pending repair has no job IDs"
                    )
                repair_artifacts.append(str(record_path))
                repair_job_ids.extend(job_ids)
                event["status"] = "waiting"
                event["wait_started_at"] = _timestamp()
                _write_json_atomic(record_path, event)
                try:
                    event["terminal_states"] = self._wait_for_job_ids(
                        job_ids,
                        f"{repair_submission_stage} invalid-case repair",
                    )
                    event["status"] = "completed"
                except WorkflowError as exc:
                    event["status"] = "failed"
                    event["failure"] = str(exc)
                event["finished_at"] = _timestamp()
                _write_json_atomic(record_path, event)
                continue

            candidates = self._discover_common_report_candidates(results_roots)
            selected, selection_rows = self._select_common_report_candidates(
                candidates,
                allow_latency_disabled=allow_latency_disabled,
            )
            missing = sorted(set(expected_ids) - set(selected))
            repair_case_ids = [
                case_id
                for case_id in expected_ids
                if case_id in selected
                and selection_rows[case_id]["requires_repair"] is True
                and (
                    repair_clock_invalid_cases
                    or selection_rows[case_id]["clock_only_invalid"] is not True
                )
            ]
            repair_case_ids = _unique([*missing, *repair_case_ids])
            existing_attempts = (
                self._case_repair_attempt_count(repair_submission_stage)
                if repair_submission_stage
                else 0
            )
            if not (
                repair_case_ids
                and repair_submission_stage
                and self.retry_failed_batches
                and existing_attempts < self.max_case_repair_attempts
            ):
                break
            repair = self.backend.repair_invalid_cases(
                repair_submission_stage,
                tuple(repair_case_ids),
                self,
            )
            if repair.status != "completed" or not repair.job_ids:
                raise WorkflowError(
                    f"{repair_submission_stage}: invalid-case repair submitted no jobs"
                )
            repair_artifacts.extend(repair.artifacts)
            repair_inputs.extend(repair.input_files)
            repair_job_ids.extend(str(item) for item in repair.job_ids)
            attempt = repair.details.get("repair_attempt", existing_attempts + 1)
            if not isinstance(attempt, int) or attempt <= 0:
                raise WorkflowError(
                    f"{repair_submission_stage}: repair attempt number is invalid"
                )
            event = {
                "format": "messaging-benchmark.case-repair-attempt.v1",
                "submission_stage": repair_submission_stage,
                "attempt": attempt,
                "case_ids": repair_case_ids,
                "job_ids": [str(item) for item in repair.job_ids],
                "artifacts": list(repair.artifacts),
                "input_files": list(repair.input_files),
                "submitted_at": _timestamp(),
                "status": "submitted",
            }
            record_path = Path(
                self.path(
                    "inputs",
                    "recovery",
                    repair_submission_stage,
                    f"attempt_{attempt:02d}",
                    "repair_attempt.json",
                )
            )
            _write_json_atomic(record_path, event)
            repair_artifacts.append(str(record_path))

        reports = {case_id: item[1] for case_id, item in selected.items()}
        report_paths = {case_id: item[0] for case_id, item in selected.items()}
        missing = sorted(set(expected_ids) - set(reports))
        extra = sorted(set(candidates) - set(expected_ids))
        failures: list[str] = []
        if missing:
            failures.append(
                f"missing {len(missing)} required report(s): "
                + ", ".join(missing[:12])
            )

        ineligible_case_ids: list[str] = []
        clock_invalid_case_ids: list[str] = []
        ineligible_reasons: dict[str, list[str]] = {}
        for case_id in expected_ids:
            if case_id not in reports:
                continue
            selection = selection_rows[case_id]
            structural_failures = [
                str(item) for item in selection["structural_failures"]
            ]
            if structural_failures:
                failures.extend(
                    f"{case_id}: {reason}" for reason in structural_failures
                )
            eligibility = _dict(reports[case_id].get("eligibility"))
            if eligibility.get("eligible") is not True:
                ineligible_case_ids.append(case_id)
                ineligible_reasons[case_id] = [
                    str(item) for item in selection["strict_failures"]
                ]
            if selection["clock_only_invalid"] is True:
                clock_invalid_case_ids.append(case_id)
        if len(ineligible_case_ids) > effective_max_ineligible_cases:
            failures.append(
                f"{len(ineligible_case_ids)} ineligible case(s) exceed the stage "
                f"limit of {effective_max_ineligible_cases}: "
                + ", ".join(ineligible_case_ids[:12])
            )
        if len(clock_invalid_case_ids) > max_clock_invalid_cases:
            failures.append(
                f"{len(clock_invalid_case_ids)} clock-invalid case(s) exceed the "
                f"stage limit of {max_clock_invalid_cases}: "
                + ", ".join(clock_invalid_case_ids[:12])
            )

        eligible_repeats_by_workload: dict[str, int] = {}
        if min_eligible_repeats_per_workload is not None:
            manifest_by_id = {
                str(row.get(id_field, "")).strip(): row for row in manifest
            }
            repeat_counts: dict[str, int] = {}
            for case_id in expected_ids:
                if case_id not in reports:
                    continue
                workload_id = _manifest_workload_id(manifest_by_id[case_id], id_field)
                if not workload_id:
                    failures.append(
                        f"{case_id}: repeated-validation workload identity is missing"
                    )
                    continue
                repeat_counts[workload_id] = repeat_counts.get(workload_id, 0) + 1
                eligibility = _dict(reports[case_id].get("eligibility"))
                if eligibility.get("eligible") is True:
                    eligible_repeats_by_workload[workload_id] = (
                        eligible_repeats_by_workload.get(workload_id, 0) + 1
                    )
                else:
                    eligible_repeats_by_workload.setdefault(workload_id, 0)
            for workload_id, repeats in sorted(repeat_counts.items()):
                eligible = eligible_repeats_by_workload.get(workload_id, 0)
                if eligible < min_eligible_repeats_per_workload:
                    failures.append(
                        f"{workload_id}: only {eligible}/{repeats} repeats are eligible; "
                        f"at least {min_eligible_repeats_per_workload} are required"
                    )
        selection_path = Path(
            self.path(
                "artifacts",
                f"{self.state['current_stage']}_result_selection.json",
            )
        )
        if repair_submission_stage:
            repair_records = self._case_repair_records(repair_submission_stage)
            repair_events = [event for _, event in repair_records]
            repair_artifacts.extend(str(path) for path, _ in repair_records)
        selection_payload = {
            "format": "messaging-benchmark.result-selection.v1",
            "backend_id": self.backend_id,
            "manifest": _display(Path(manifest_path), self.project_root),
            "selected_results_group": group,
            "selected_reports": [
                {
                    **selection_rows[case_id],
                    "case_id": case_id,
                    "source_report": _display(
                        report_paths[case_id], self.project_root
                    ),
                    "source_report_sha256": _sha256(report_paths[case_id]),
                }
                for case_id in expected_ids
                if case_id in report_paths
            ],
            "repair_events": repair_events,
            "ineligible_case_ids": ineligible_case_ids,
            "ineligible_reasons": ineligible_reasons,
            "missing_case_ids": missing,
            "valid": not failures,
            "created_at": _timestamp(),
        }
        _write_json(selection_path, selection_payload)
        if failures:
            raise WorkflowError(
                "result contract validation failed: " + "; ".join(failures[:20])
            )
        selected_root = self._materialize_selected_results(
            group,
            {case_id: report_paths[case_id] for case_id in expected_ids},
        )
        eligible_case_count = sum(
            _dict(reports[case_id].get("eligibility")).get("eligible") is True
            for case_id in expected_ids
        )
        verification = {
            "format": "messaging-benchmark.workflow-result-verification.v1",
            "backend_id": self.backend_id,
            "manifest": _display(Path(manifest_path), self.project_root),
            "manifest_sha256": _sha256(Path(manifest_path)),
            "expected_case_count": expected_case_count,
            "verified_case_count": len(expected_ids),
            "eligible_case_count": eligible_case_count,
            "ineligible_case_count": len(expected_ids) - eligible_case_count,
            "ineligible_case_ids": ineligible_case_ids,
            "ineligible_reasons": ineligible_reasons,
            "tolerated_clock_invalid_case_ids": clock_invalid_case_ids,
            "max_clock_invalid_cases": max_clock_invalid_cases,
            "max_ineligible_cases": effective_max_ineligible_cases,
            "repair_clock_invalid_cases": repair_clock_invalid_cases,
            "min_eligible_repeats_per_workload": min_eligible_repeats_per_workload,
            "eligible_repeats_by_workload": eligible_repeats_by_workload,
            "selected_results_root": str(selected_root),
            "result_selection": str(selection_path),
            "repair_events": repair_events,
            "extra_case_ids": extra,
            "valid": True,
            "verified_at": _timestamp(),
        }
        path = Path(self.path("artifacts", f"{self.state['current_stage']}_verification.json"))
        _write_json(path, verification)
        return StageOutcome(
            artifacts=_unique([str(path), str(selection_path), *repair_artifacts]),
            result_paths=_unique([*results_roots, str(selected_root)]),
            job_ids=_unique(repair_job_ids),
            input_files=_unique([manifest_path, *repair_inputs]),
            details=verification,
        )

    def _discover_common_report_candidates(
        self, roots: list[str]
    ) -> dict[str, list[tuple[Path, dict[str, Any]]]]:
        candidates: dict[str, list[tuple[Path, dict[str, Any]]]] = {}
        for raw_root in roots:
            root = Path(raw_root)
            if not root.exists():
                raise WorkflowError(f"results root does not exist: {root}")
            paths = [root] if root.is_file() else sorted(root.rglob("final_report.json"))
            for path in paths:
                report = _read_json(path)
                system = _dict(report.get("system_under_test"))
                config = _dict(report.get("config"))
                if str(system.get("backend_id") or config.get("backend_id")) != self.backend_id:
                    continue
                case = _dict(report.get("case"))
                case_id = str(case.get("case_id") or config.get("case_id") or "").strip()
                if case_id:
                    candidates.setdefault(case_id, []).append((path, report))
        return candidates

    def _select_common_report_candidates(
        self,
        candidates: dict[str, list[tuple[Path, dict[str, Any]]]],
        *,
        allow_latency_disabled: bool,
    ) -> tuple[
        dict[str, tuple[Path, dict[str, Any]]],
        dict[str, dict[str, Any]],
    ]:
        selected: dict[str, tuple[Path, dict[str, Any]]] = {}
        records: dict[str, dict[str, Any]] = {}
        for case_id, values in candidates.items():
            evaluated: list[
                tuple[Path, dict[str, Any], list[str], list[str], bool]
            ] = []
            for path, report in values:
                ineligible = _dict(report.get("eligibility")).get("eligible") is not True
                structural = _common_result_failures(
                    report,
                    self.backend_id,
                    allow_latency_disabled=allow_latency_disabled,
                    allow_ineligible_evidence=ineligible,
                )
                strict = _common_result_failures(
                    report,
                    self.backend_id,
                    allow_latency_disabled=allow_latency_disabled,
                )
                evaluated.append(
                    (
                        path,
                        report,
                        structural,
                        strict,
                        _clock_only_latency_ineligibility(report),
                    )
                )
            evaluated.sort(
                key=lambda item: (
                    int(_dict(item[1].get("case")).get("status") == "completed"),
                    int(not item[2]),
                    int(_dict(item[1].get("eligibility")).get("eligible") is True),
                    _repair_attempt_from_path(item[0]),
                    *_report_preference(item[0]),
                ),
                reverse=True,
            )
            preferred = evaluated[0]
            valid_completed = [
                item
                for item in evaluated
                if _dict(item[1].get("case")).get("status") == "completed"
                and not item[2]
                and _dict(item[1].get("eligibility")).get("eligible") is True
                and item[0].parent.name not in {"data", "reports"}
            ]
            valid_hashes = {
                _canonical_json_sha256(item[1]) for item in valid_completed
            }
            structural = list(preferred[2])
            if len(valid_hashes) > 1:
                structural.append(
                    "conflicting eligible completed reports require manual review"
                )
            selected[case_id] = (preferred[0], preferred[1])
            records[case_id] = {
                "selected_from_candidate_count": len(values),
                "selected_repair_attempt": _repair_attempt_from_path(preferred[0]),
                "eligible": _dict(preferred[1].get("eligibility")).get("eligible") is True,
                "clock_only_invalid": preferred[4],
                "structural_failures": structural,
                "strict_failures": list(preferred[3]),
                "requires_repair": bool(structural)
                or _dict(preferred[1].get("eligibility")).get("eligible") is not True,
            }
        return selected, records

    def _case_repair_attempt_count(self, submission_stage: str) -> int:
        root = Path(self.path("inputs", "recovery", submission_stage))
        if not root.exists():
            return 0
        return sum(path.is_dir() for path in root.glob("attempt_*"))

    def _case_repair_records(
        self,
        submission_stage: str,
    ) -> list[tuple[Path, dict[str, Any]]]:
        root = Path(self.path("inputs", "recovery", submission_stage))
        records: list[tuple[Path, dict[str, Any]]] = []
        if not root.exists():
            return records
        for path in sorted(root.glob("attempt_*/repair_attempt.json")):
            event = _read_json(path)
            if event.get("format") != "messaging-benchmark.case-repair-attempt.v1":
                raise WorkflowError(f"invalid case-repair attempt record: {path}")
            if event.get("submission_stage") != submission_stage:
                raise WorkflowError(f"case-repair stage drift: {path}")
            records.append((path, event))
        return records

    def _pending_case_repair_record(
        self,
        submission_stage: str,
    ) -> tuple[Path, dict[str, Any]] | None:
        pending = [
            (path, event)
            for path, event in self._case_repair_records(submission_stage)
            if event.get("status") in {"submitted", "waiting"}
        ]
        if len(pending) > 1:
            raise WorkflowError(
                f"{submission_stage}: multiple pending case-repair attempts"
            )
        return pending[0] if pending else None

    def _materialize_selected_results(
        self,
        group: str,
        report_paths: dict[str, Path],
    ) -> Path:
        root = Path(self.path("selected-results", group))
        root.mkdir(parents=True, exist_ok=True)
        for case_id, report_path in report_paths.items():
            if not SAFE_ID.fullmatch(case_id):
                raise WorkflowError(f"unsafe case ID in selected results: {case_id!r}")
            destination = root / case_id
            _require_within(destination.resolve(), root.resolve())
            if destination.is_symlink() or destination.is_file():
                destination.unlink()
            elif destination.exists():
                shutil.rmtree(destination)
            destination.mkdir()
            source_dir = report_path.parent.resolve()
            for source in source_dir.iterdir():
                target = destination / source.name
                target.symlink_to(source.resolve(), target_is_directory=source.is_dir())
            if not (destination / "final_report.json").is_file():
                (destination / "final_report.json").symlink_to(report_path.resolve())
        return root

    def _discover_common_reports(self, roots: list[str]) -> dict[str, dict[str, Any]]:
        candidates: dict[str, list[tuple[Path, dict[str, Any]]]] = {}
        for raw_root in roots:
            root = Path(raw_root)
            if not root.exists():
                raise WorkflowError(f"results root does not exist: {root}")
            paths = [root] if root.is_file() else sorted(root.rglob("final_report.json"))
            for path in paths:
                report = _read_json(path)
                system = _dict(report.get("system_under_test"))
                config = _dict(report.get("config"))
                if str(system.get("backend_id") or config.get("backend_id")) != self.backend_id:
                    continue
                case = _dict(report.get("case"))
                case_id = str(case.get("case_id") or config.get("case_id") or "").strip()
                if case_id:
                    candidates.setdefault(case_id, []).append((path, report))
        selected: dict[str, dict[str, Any]] = {}
        for case_id, values in candidates.items():
            values.sort(key=_result_report_preference, reverse=True)
            preferred = values[0]
            preferred_hash = _canonical_json_sha256(preferred[1])
            conflicts = [
                path
                for path, report in values[1:]
                if _dict(report.get("case")).get("status") == "completed"
                and _dict(preferred[1].get("case")).get("status") == "completed"
                if _canonical_json_sha256(report) != preferred_hash
                and path.parent.name not in {"data", "reports"}
            ]
            if conflicts:
                raise WorkflowError(
                    f"conflicting final reports for {case_id}: "
                    + ", ".join(str(path) for path in conflicts)
                )
            selected[case_id] = preferred[1]
        return selected

    def stage_details(self, stage_name: str) -> dict[str, Any]:
        return _dict(self.state["stages"].get(stage_name, {}).get("details"))

    def stage_record(self, stage_name: str) -> dict[str, Any]:
        return dict(_dict(self.state["stages"].get(stage_name)))

    def deferred(self, message: str, **details: Any) -> StageOutcome:
        if not self.dry_run:
            raise WorkflowError(message)
        return StageOutcome(status="deferred", details=details, message=message)

    def _record_input_checksums(self, paths: Iterable[str]) -> None:
        checksums = self.state["input_checksums"]
        for raw in paths:
            path = Path(raw).resolve()
            if not path.is_file():
                raise WorkflowError(f"recorded workflow input is missing: {path}")
            key = _display(path, self.project_root)
            digest = _sha256(path)
            previous = checksums.get(key)
            if previous is not None and previous != digest:
                raise WorkflowError(f"workflow input changed during execution: {key}")
            checksums[key] = digest

    def _apply_implementation_updates(self, state: dict[str, Any]) -> None:
        declared = {Path(item).as_posix() for item in self.backend.required_files()}
        changes: list[dict[str, Any]] = []
        checksums = _dict(state.get("input_checksums"))
        for raw in self.accepted_implementation_updates:
            candidate = Path(raw)
            if candidate.is_absolute() or ".." in candidate.parts:
                raise WorkflowError(
                    f"implementation update must be a repository-relative path: {raw}"
                )
            name = Path(candidate.as_posix()).as_posix()
            relative = Path(name)
            if name not in declared:
                raise WorkflowError(
                    f"implementation update is not declared by {self.backend_id}: {name}"
                )
            if (
                not relative.parts
                or relative.parts[0] not in {"scripts", "src", "models"}
                or relative.suffix not in {".py", ".sh"}
            ):
                raise WorkflowError(
                    f"only declared Python or shell implementation files may migrate: {name}"
                )
            path = (self.project_root / relative).resolve()
            _require_within(path, self.project_root)
            if not path.is_file():
                raise WorkflowError(f"implementation update is missing: {name}")
            current = _sha256(path)
            previous = checksums.get(name)
            if previous == current:
                raise WorkflowError(
                    f"implementation input is unchanged; migration is unnecessary: {name}"
                )
            changes.append(
                {
                    "path": name,
                    "change_type": "updated" if previous else "adopted_dependency",
                    "previous_sha256": previous,
                    "current_sha256": current,
                }
            )

        backup_root = self.run_root / "state_backups"
        backup_root.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup = backup_root / f"workflow_state.pre-implementation-update.{stamp}.json"
        sequence = 1
        while backup.exists():
            backup = backup_root / (
                f"workflow_state.pre-implementation-update.{stamp}.{sequence:02d}.json"
            )
            sequence += 1
        shutil.copy2(self.state_path, backup)
        for change in changes:
            checksums[change["path"]] = change["current_sha256"]
        state["input_checksums"] = checksums
        event = {
            "format": "messaging-benchmark.workflow-implementation-update.v1",
            "timestamp": _timestamp(),
            "reason": self.implementation_update_reason,
            "state_backup": _display(backup, self.project_root),
            "files": changes,
        }
        state.setdefault("implementation_updates", []).append(event)
        state["updated_at"] = _timestamp()
        _write_json_atomic(self.state_path, state)

    def _verify_stage_artifacts(
        self, stage_name: str, record: dict[str, Any]
    ) -> None:
        missing = [
            str(raw)
            for raw in record.get("artifacts", [])
            if not Path(str(raw)).is_file()
        ]
        if missing:
            raise WorkflowError(
                f"completed stage {stage_name} has missing artifact(s): "
                + ", ".join(missing[:12])
            )

    def _verify_recorded_checksums(self, state: dict[str, Any]) -> None:
        failures = []
        for name, expected in _dict(state.get("input_checksums")).items():
            path = Path(name)
            if not path.is_absolute():
                path = self.project_root / path
            if not path.is_file():
                failures.append(f"missing {name}")
            elif _sha256(path) != expected:
                failures.append(f"checksum drift {name}")
        if failures:
            raise WorkflowError("immutable workflow input drift: " + ", ".join(failures))

    def _append_command_log(self, entry: dict[str, Any]) -> None:
        path = self.run_root / "logs" / "commands.jsonl"
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, sort_keys=True) + "\n")

    def _record_failure(self, exc: BaseException) -> None:
        failure = {
            "timestamp": _timestamp(),
            "stage": self.state.get("current_stage"),
            "type": type(exc).__name__,
            "message": str(exc),
        }
        self.state.setdefault("failures", []).append(failure)
        stage = self.state.get("current_stage")
        if stage:
            record = self.state.setdefault("stages", {}).setdefault(stage, {})
            record["status"] = "failed"
            record["failed_at"] = failure["timestamp"]
            record["failure"] = failure
        self.state["status"] = "failed"
        self.state["current_stage"] = None
        self._write_state(self.state)

    def _write_state(self, state: dict[str, Any]) -> None:
        state["updated_at"] = _timestamp()
        _write_json_atomic(self.state_path, state)

    def _write_final_artifacts(self) -> None:
        artifact_root = self.run_root / "artifacts"
        versions = self._software_versions()
        provenance = self._provenance()
        _write_json(artifact_root / "workflow_state.json", self.state)
        _write_json(artifact_root / "software_versions.json", versions)
        _write_json(artifact_root / "provenance.json", provenance)
        job_rows = []
        for stage_name, record in self.state["stages"].items():
            for job_id in record.get("job_ids", []):
                job_rows.append(
                    {
                        "stage": stage_name,
                        "job_id": job_id,
                        "status": record.get("status"),
                    }
                )
        _write_csv(
            artifact_root / "slurm_job_ids.csv",
            ("stage", "job_id", "status"),
            job_rows,
        )
        stage_rows = []
        for stage_name, record in self.state["stages"].items():
            stage_rows.append(
                {
                    "stage": stage_name,
                    "phase": record.get("phase", ""),
                    "description": record.get("description", ""),
                    "status": record.get("status", ""),
                    "attempt_count": record.get("attempt_count", 0),
                    "started_at": record.get("started_at", ""),
                    "completed_at": record.get("completed_at", ""),
                    "job_count": len(record.get("job_ids", [])),
                    "artifact_count": len(record.get("artifacts", [])),
                    "result_path_count": len(record.get("result_paths", [])),
                    "selected_configs": ",".join(
                        str(value) for value in record.get("selected_configs", [])
                    ),
                    "selected_profiles": ",".join(
                        str(value) for value in record.get("selected_profiles", [])
                    ),
                }
            )
        _write_csv(
            artifact_root / "workflow_stages.csv",
            (
                "stage",
                "phase",
                "description",
                "status",
                "attempt_count",
                "started_at",
                "completed_at",
                "job_count",
                "artifact_count",
                "result_path_count",
                "selected_configs",
                "selected_profiles",
            ),
            stage_rows,
        )
        report = {
            "format": FINAL_REPORT_FORMAT,
            "workflow_id": self.workflow_id,
            "backend_id": self.backend_id,
            "status": self.state["status"],
            "mode": self.state["mode"],
            "requested_phase": self.requested_phase,
            "common_measurement_contract": COMMON_MEASUREMENT_CONTRACT,
            "execution_settings": self.execution_settings,
            "machine_output_contract": MACHINE_OUTPUT_CONTRACT,
            "reporting": self.state["reporting"],
            "completed_stages": self.state["completed_stages"],
            "stage_status": {
                name: record.get("status")
                for name, record in self.state["stages"].items()
            },
            "job_ids": self.state["job_ids"],
            "result_paths": self.state["result_paths"],
            "selected_configs": self.state["selected_configs"],
            "selected_profiles": self.state["selected_profiles"],
            "failures": self.state["failures"],
            "workflow_state": _display(self.state_path, self.project_root),
            "software_versions": _display(
                artifact_root / "software_versions.json", self.project_root
            ),
            "provenance": _display(artifact_root / "provenance.json", self.project_root),
            "generated_at": _timestamp(),
        }
        _write_json(artifact_root / "final_report.json", report)
        manifest_rows = []
        for path in sorted(artifact_root.rglob("*")):
            if not path.is_file() or path.name in {"SHA256SUMS", "artifact_manifest.csv"}:
                continue
            manifest_rows.append(
                {
                    "path": path.relative_to(artifact_root).as_posix(),
                    "size_bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                }
            )
        _write_csv(
            artifact_root / "artifact_manifest.csv",
            ("path", "size_bytes", "sha256"),
            manifest_rows,
        )
        manifest_path = artifact_root / "artifact_manifest.csv"
        checksum_lines = [f"{row['sha256']}  {row['path']}" for row in manifest_rows]
        checksum_lines.append(
            f"{_sha256(manifest_path)}  {manifest_path.name}"
        )
        (artifact_root / "SHA256SUMS").write_text(
            "\n".join(checksum_lines) + ("\n" if checksum_lines else ""),
            encoding="utf-8",
        )

    def _software_versions(self) -> dict[str, Any]:
        adapter = get_backend(self.backend_id)
        values = {
            "python": sys.version.splitlines()[0],
            "backend_id": self.backend_id,
            "adapter_version": adapter.adapter_version,
        }
        for name, command in (
            ("git", ["git", "--version"]),
            ("java", ["java", "-version"]),
            ("sbatch", ["sbatch", "--version"]),
            ("mpirun", ["mpirun", "--version"]),
        ):
            if shutil.which(command[0]) is None:
                values[name] = None
                continue
            completed = subprocess.run(
                command,
                cwd=self.project_root,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            output = completed.stdout or completed.stderr
            values[name] = output.splitlines()[0] if output else None
        return values

    def _provenance(self) -> dict[str, Any]:
        commit = _checked_output(["git", "rev-parse", "HEAD"], self.project_root)
        status = _checked_output(["git", "status", "--short"], self.project_root)
        return {
            "project_root": _display(self.project_root, self.project_root),
            "git_commit": commit.strip() or None,
            "git_worktree_dirty": bool(status.strip()),
            "git_status": status.splitlines(),
            "hostname": os.uname().nodename,
            "created_at": self.state["created_at"],
            "updated_at": self.state["updated_at"],
            "common_contract_sha256": _canonical_json_sha256(
                COMMON_MEASUREMENT_CONTRACT
            ),
            "execution_settings": self.execution_settings,
            "execution_settings_sha256": _canonical_json_sha256(
                self.execution_settings
            ),
            "input_checksums": self.state["input_checksums"],
        }


def _new_execution_settings(overrides: dict[str, str]) -> dict[str, Any]:
    slurm: dict[str, str | None] = {}
    for name, environment_name in SLURM_SETTING_ENV.items():
        value = overrides.get(name)
        if value is None:
            value = os.environ.get(environment_name)
        if value is None and name == "time":
            value = "02:00:00"
        if value == "" and name != "time":
            value = None
        if value is not None:
            _validate_scheduler_value(name, value)
        slurm[name] = value
    return {
        "format": EXECUTION_SETTINGS_FORMAT,
        "slurm": slurm,
        "exclusive_allocation": True,
    }


def _validated_recorded_execution_settings(value: Any) -> dict[str, Any]:
    settings = _dict(value)
    if settings.get("format") != EXECUTION_SETTINGS_FORMAT:
        raise WorkflowError("unsupported workflow execution-settings format")
    slurm = _dict(settings.get("slurm"))
    if set(slurm) != set(SLURM_SETTING_ENV):
        raise WorkflowError("recorded workflow scheduler settings are incomplete")
    for name, raw in slurm.items():
        if raw is not None:
            _validate_scheduler_value(name, raw)
    if settings.get("exclusive_allocation") is not True:
        raise WorkflowError("reproducible campaigns require exclusive allocations")
    return settings


def _validate_scheduler_value(name: str, value: Any) -> None:
    if not isinstance(value, str):
        raise WorkflowError(f"scheduler setting {name} must be text")
    if not value or len(value) > 256 or "\n" in value or "\r" in value or "\0" in value:
        raise WorkflowError(f"scheduler setting {name} has an invalid value")
    if name == "time" and not re.fullmatch(
        r"(?:(?:[0-9]+)-)?[0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?",
        value,
    ):
        raise WorkflowError(
            "Slurm wall time must use [days-]hours:minutes[:seconds]"
        )


def _common_result_failures(
    report: dict[str, Any],
    backend_id: str,
    *,
    allow_latency_disabled: bool = False,
    allow_clock_only_ineligible: bool = False,
    allow_ineligible_evidence: bool = False,
) -> list[str]:
    failures: list[str] = []
    case = _dict(report.get("case"))
    config = _dict(report.get("config"))
    system = _dict(report.get("system_under_test"))
    common = _dict(report.get("common_metrics"))
    timing = _dict(common.get("timing"))
    latency = _dict(common.get("latency_end_to_end"))
    records = _dict(common.get("records"))
    throughput = _dict(common.get("throughput"))
    delivery = _dict(common.get("producer_delivery"))
    qualification = _dict(common.get("qualification"))
    eligibility = _dict(report.get("eligibility"))
    health = _dict(report.get("backend_health"))
    if case.get("status") != "completed":
        failures.append("case status is not completed")
    if report.get("result_schema_version") != "messaging-benchmark.result.v1":
        failures.append("portable result schema is missing")
    if system.get("backend_id") != backend_id:
        failures.append("backend identity drift")
    if health.get("status") != "healthy":
        failures.append("backend health is not healthy")
    expected_timing = COMMON_MEASUREMENT_CONTRACT["timing"]
    if timing.get("warmup_sec") != expected_timing["warmup_sec"]:
        failures.append("warm-up duration drift")
    if timing.get("measurement_sec") != expected_timing["measurement_sec"]:
        failures.append("measurement duration drift")
    if timing.get("drain_timeout_sec") != expected_timing["drain_timeout_sec"]:
        failures.append("drain timeout drift")
    latency_enabled = config.get("latency_enabled") is True
    if not latency_enabled and not allow_latency_disabled:
        failures.append("end-to-end latency is disabled")
    if latency_enabled and latency.get("sample_every") != 10:
        failures.append("latency sampling is not deterministic 1-in-10")
    if (
        latency_enabled
        and latency.get("valid") is not True
        and not allow_clock_only_ineligible
        and not allow_ineligible_evidence
    ):
        failures.append("latency validation failed")
    if (
        eligibility.get("eligible") is not True
        or qualification.get("eligible") is not True
    ) and not allow_clock_only_ineligible and not allow_ineligible_evidence:
        failures.append("case is ineligible")
    if qualification.get("policy_id") != "qualification.application.v1":
        failures.append("shared qualification policy is not recorded")
    if delivery.get("backlog_denominator") != "messages_attempted":
        failures.append("backlog denominator is not measurement-period attempts")
    if not allow_ineligible_evidence:
        failures.extend(common_measurement_lifecycle_failures(report))
        for field in ("missing", "duplicate", "out_of_order", "invalid_envelope", "surplus"):
            if records.get(field) != 0:
                failures.append(f"correctness counter {field} is not zero")
    for field in (
        "producer_mib_per_sec",
        "consumer_mib_per_sec",
        "balanced_mib_per_sec",
        "producer_records_per_sec",
        "consumer_records_per_sec",
        "balanced_records_per_sec",
    ):
        value = throughput.get(field)
        if not isinstance(value, (int, float)) or value < 0:
            failures.append(f"throughput field {field} is missing or negative")
    if all(
        isinstance(throughput.get(field), (int, float))
        for field in ("producer_mib_per_sec", "consumer_mib_per_sec", "balanced_mib_per_sec")
    ) and abs(
        float(throughput["balanced_mib_per_sec"])
        - min(float(throughput["producer_mib_per_sec"]), float(throughput["consumer_mib_per_sec"]))
    ) > 1e-6:
        failures.append("balanced MiB/s is not the producer/consumer minimum")
    return failures


def _clock_only_latency_ineligibility(report: dict[str, Any]) -> bool:
    """Return true only when clock calibration is the sole invalid evidence."""
    config = _dict(report.get("config"))
    eligibility = _dict(report.get("eligibility"))
    latency = _dict(report.get("latency_validation"))
    common_latency = _dict(
        _dict(report.get("common_metrics")).get("latency_end_to_end")
    )
    qualification = _dict(
        _dict(report.get("common_metrics")).get("qualification")
    )
    health = _dict(report.get("backend_health"))
    if (
        config.get("latency_enabled") is not True
        or eligibility.get("eligible") is not False
        or latency.get("valid") is not False
        or latency.get("clock_valid") is not False
        or latency.get("envelope_valid") is not True
        or latency.get("sample_completeness_valid") is not True
        or latency.get("negative_latency_count") != 0
        or common_latency.get("valid") is not False
        or qualification.get("eligible") is not False
        or qualification.get("qualified") is not False
        or health.get("status") != "healthy"
    ):
        return False
    reasons = eligibility.get("failure_reasons")
    if not isinstance(reasons, list) or not reasons:
        return False
    return all(
        "latency" in str(reason).lower() or "clock" in str(reason).lower()
        for reason in reasons
    )


def _manifest_workload_id(row: dict[str, str], id_field: str) -> str:
    for field in ("workload_config_id", "anchor_id", "anchor"):
        value = str(row.get(field, "")).strip()
        if value:
            return value
    case_id = str(row.get(id_field, "")).strip()
    config_id = str(row.get("config_id", "")).strip()
    if config_id and config_id != case_id:
        return config_id
    return ""


def _report_preference(path: Path) -> tuple[int, int, str]:
    return (
        int(path.parent.name not in {"data", "reports"}),
        -len(path.parts),
        str(path),
    )


def _repair_attempt_from_path(path: Path) -> int:
    for part in reversed(path.parts):
        match = re.fullmatch(r"(?:repair-|attempt_)(\d+)", part)
        if match:
            return int(match.group(1))
    return 0


def _result_report_preference(
    item: tuple[Path, dict[str, Any]]
) -> tuple[int, int, int, str]:
    path, report = item
    preference = _report_preference(path)
    return (
        int(_dict(report.get("case")).get("status") == "completed"),
        preference[0],
        preference[1],
        preference[2],
    )


def _normalize_slurm_state(value: str) -> str:
    return value.strip().upper().split()[0].split("+")[0]


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical_json_sha256(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise WorkflowError(f"expected JSON object: {path}")
    return value


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _write_csv(path: Path, fields: Iterable[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _display(path: Path, project_root: Path) -> str:
    try:
        return path.resolve().relative_to(project_root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def _require_within(path: Path, root: Path) -> None:
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise WorkflowError(f"path escapes workflow root: {path}") from exc


def _unique(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(str(item) for item in values if str(item)))


def _checked_output(command: list[str], cwd: Path) -> str:
    if shutil.which(command[0]) is None:
        return ""
    completed = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return completed.stdout if completed.returncode == 0 else ""
