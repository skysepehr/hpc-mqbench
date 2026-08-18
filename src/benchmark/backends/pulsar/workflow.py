from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
from typing import Any

from src.benchmark.workflow.base import (
    BackendWorkflow,
    StageOutcome,
    StageSpec,
    WorkflowError,
    WorkflowRuntime,
)
from src.benchmark.workflow.validation import (
    manifest_config_paths,
    validate_campaign_manifest,
    verify_sha256_file,
    write_validation,
)
from src.benchmark.workflow.recovery import (
    RecoveryManifestPlan,
    write_incomplete_case_manifests,
    write_selected_case_manifests,
)


class PulsarReproducibleWorkflow(BackendWorkflow):
    backend_id = "pulsar"

    def stages(self) -> tuple[StageSpec, ...]:
        return (
            StageSpec("preflight", "setup", "check Pulsar workflow implementation and environment"),
            StageSpec("v1-generate", "phase1", "generate a private 120-case Pulsar Phase 1 campaign", ("preflight",)),
            StageSpec("v1-phase1-submit", "phase1", "submit four independent Pulsar Phase 1 batch jobs", ("v1-generate",)),
            StageSpec("v1-phase1-wait", "phase1", "wait for Pulsar Phase 1 jobs", ("v1-phase1-submit",)),
            StageSpec("v1-phase1-verify", "phase1", "verify all 120 common-contract Phase 1 case results", ("v1-phase1-wait",)),
            StageSpec("v1-phase1-analyze", "phase1", "generate Pulsar and common Phase 1 analyses and shortlist", ("v1-phase1-verify",)),
            StageSpec("v1-validation-generate", "validation", "generate five randomized blocks from the new shortlist", ("v1-phase1-analyze",)),
            StageSpec("v1-validation-submit", "validation", "submit two Pulsar repeated-validation jobs", ("v1-validation-generate",)),
            StageSpec("v1-validation-wait", "validation", "wait for Pulsar repeated validation", ("v1-validation-submit",)),
            StageSpec("v1-validation-verify", "validation", "verify all 50 repeated-validation case results", ("v1-validation-wait",)),
            StageSpec("v1-validation-analyze", "validation", "aggregate the repeated V1 campaign with the common ranking", ("v1-validation-verify",)),
            StageSpec("v1-finalize", "v1-finalize", "finalize validated Pulsar V1 JSON/CSV artifacts, manifests, and checksums", ("v1-validation-analyze",)),
            StageSpec("v2-gate-check", "v2-pilot", "verify the immutable Pulsar instrumentation and batch-runner gate", ("v1-phase1-analyze",)),
            StageSpec("v2-screening-generate", "v2-screening", "generate and authorize the 30-case Pulsar memory-profile screen", ("v2-gate-check",)),
            StageSpec("v2-screening-submit", "v2-screening", "submit one memory-profile screening job", ("v2-screening-generate",)),
            StageSpec("v2-screening-wait", "v2-screening", "wait for Pulsar profile screening", ("v2-screening-submit",)),
            StageSpec("v2-screening-verify", "v2-screening", "verify all 30 profile-screening case results", ("v2-screening-wait",)),
            StageSpec("v2-screening-analyze", "v2-screening", "select the baseline and two confirmation candidates", ("v2-screening-verify",)),
            StageSpec("v2-confirmation-generate", "v2-confirmation", "generate and authorize two additional confirmation blocks", ("v2-screening-analyze",)),
            StageSpec("v2-confirmation-submit", "v2-confirmation", "submit one Pulsar profile-confirmation job", ("v2-confirmation-generate",)),
            StageSpec("v2-confirmation-wait", "v2-confirmation", "wait for Pulsar profile confirmation", ("v2-confirmation-submit",)),
            StageSpec("v2-confirmation-verify", "v2-confirmation", "verify all required confirmation case results", ("v2-confirmation-wait",)),
            StageSpec("v2-confirmation-analyze", "v2-confirmation", "apply the predefined Pulsar memory-profile freeze rule", ("v2-confirmation-verify",)),
            StageSpec("v2-final-generate", "v2-final", "generate five blocks of the current shortlist under the frozen profile", ("v2-confirmation-analyze",)),
            StageSpec("v2-final-submit", "v2-final", "submit two frozen-profile final-validation jobs", ("v2-final-generate",)),
            StageSpec("v2-final-wait", "v2-final", "wait for Pulsar final validation", ("v2-final-submit",)),
            StageSpec("v2-final-verify", "v2-final", "verify all 50 final-validation case results", ("v2-final-wait",)),
            StageSpec("v2-final-analyze", "v2-final", "generate final validated Pulsar V2 JSON/CSV artifacts and checksums", ("v2-final-verify",)),
        )

    def required_files(self) -> tuple[str, ...]:
        return (
            "scripts/reproducible_benchmark_workflow.py",
            "scripts/run_reproducible_benchmark.sh",
            "scripts/select_slurm_partition.py",
            "src/benchmark/workflow/base.py",
            "src/benchmark/workflow/engine.py",
            "src/benchmark/workflow/recovery.py",
            "src/benchmark/backends/pulsar/workflow.py",
            "scripts/generate_pulsar_campaigns.py",
            "scripts/analyze_pulsar_phase1.py",
            "scripts/generate_pulsar_reproducible_validation.py",
            "scripts/analyze_reproducible_campaign.py",
            "scripts/analyze_kafka_reproducible_campaign.py",
            "scripts/generate_pulsar_phase2.py",
            "scripts/analyze_pulsar_phase2_screening.py",
            "scripts/generate_pulsar_phase2_confirmation.py",
            "scripts/analyze_pulsar_phase2_confirmation.py",
            "scripts/submit_backend_batch.sh",
            "scripts/finalize_reproducible_results.py",
            "configs/backends/pulsar/profiles/BASELINE_H16_D32.json",
        )

    def execute(self, stage: StageSpec, runtime: WorkflowRuntime) -> StageOutcome:
        method = getattr(self, "_" + stage.name.replace("-", "_"), None)
        if method is None:
            raise WorkflowError(f"Pulsar workflow stage is not implemented: {stage.name}")
        return method(runtime)

    def retry_failed_batches(
        self,
        submission_stage: str,
        failed_job_ids: tuple[str, ...],
        runtime: WorkflowRuntime,
    ) -> StageOutcome:
        source_batches = self._submission_inputs(runtime, submission_stage)
        if not source_batches:
            raise WorkflowError(
                f"{submission_stage}: original Pulsar manifests are unavailable for repair"
            )
        result_root = self._results_root_for_submission(runtime, submission_stage)
        repair_index = _next_repair_index(runtime, submission_stage)
        repair_root = Path(
            runtime.path("inputs", "recovery", submission_stage, f"attempt_{repair_index:02d}")
        )
        plan = write_incomplete_case_manifests(
            source_manifests=source_batches,
            results_roots=[Path(result_root)],
            output_dir=repair_root / "batches",
            backend_id="pulsar",
        )
        if not plan.manifest_paths:
            return StageOutcome(
                details={
                    "repair_not_required": True,
                    "repair_attempt": repair_index,
                    "failed_original_job_ids": list(failed_job_ids),
                    "completed_cases_preserved": plan.completed_case_count,
                    "retry_case_ids": [],
                    "reason": "all manifest cases already have completed reports",
                }
            )
        return self._submit_repair_plan(
            runtime=runtime,
            submission_stage=submission_stage,
            repair_index=repair_index,
            plan=plan,
            details={"failed_original_job_ids": list(failed_job_ids)},
        )

    def repair_invalid_cases(
        self,
        submission_stage: str,
        case_ids: tuple[str, ...],
        runtime: WorkflowRuntime,
    ) -> StageOutcome:
        source_batches = self._submission_inputs(runtime, submission_stage)
        if not source_batches:
            raise WorkflowError(
                f"{submission_stage}: original Pulsar manifests are unavailable for repair"
            )
        repair_index = _next_repair_index(runtime, submission_stage)
        repair_root = Path(
            runtime.path("inputs", "recovery", submission_stage, f"attempt_{repair_index:02d}")
        )
        plan = write_selected_case_manifests(
            source_manifests=source_batches,
            selected_case_ids=case_ids,
            output_dir=repair_root / "batches",
        )
        return self._submit_repair_plan(
            runtime=runtime,
            submission_stage=submission_stage,
            repair_index=repair_index,
            plan=plan,
            details={"invalid_case_ids": list(case_ids)},
        )

    def _submit_repair_plan(
        self,
        *,
        runtime: WorkflowRuntime,
        submission_stage: str,
        repair_index: int,
        plan: RecoveryManifestPlan,
        details: dict[str, Any],
    ) -> StageOutcome:
        outcome = runtime.submit_manifest_batches(
            stage_name=f"{submission_stage}-repair-{repair_index:02d}",
            manifest_paths=[str(path) for path in plan.manifest_paths],
            run_id=self._run_id_for_submission(runtime, submission_stage),
            allow_profile_changes="v2" in submission_stage,
            independent=True,
            repair_id=f"repair-{repair_index:02d}",
        )
        outcome.details.update(
            {
                "repair_attempt": repair_index,
                "completed_cases_preserved": plan.completed_case_count,
                "retry_case_ids": list(plan.retry_case_ids),
                **details,
            }
        )
        return outcome

    def _submission_inputs(
        self, runtime: WorkflowRuntime, submission_stage: str
    ) -> list[Path]:
        if submission_stage == "v1-phase1-submit":
            return [
                Path(path)
                for path in runtime.stage_details("v1-generate").get(
                    "batch_paths", []
                )
            ]
        if submission_stage == "v1-validation-submit":
            return [
                Path(path)
                for path in runtime.stage_details("v1-validation-generate").get(
                    "batch_paths", []
                )
            ]
        mapping = {
            "v2-screening-submit": runtime.path(
                "inputs", "pulsar-v2", "screening", "memory_screening_manifest.csv"
            ),
            "v2-confirmation-submit": runtime.path(
                "inputs", "pulsar-v2", "confirmation", "memory_confirmation_manifest.csv"
            ),
        }
        if submission_stage in mapping:
            return [Path(mapping[submission_stage])]
        if submission_stage == "v2-final-submit":
            return _batches(
                Path(runtime.path("inputs", "pulsar-v2", "final-validation", "batches"))
            )
        return []

    def _run_id_for_submission(
        self, runtime: WorkflowRuntime, submission_stage: str
    ) -> str:
        mapping = {
            "v1-phase1-submit": self._phase1_run_id,
            "v1-validation-submit": self._validation_run_id,
            "v2-screening-submit": self._screening_run_id,
            "v2-confirmation-submit": self._confirmation_run_id,
            "v2-final-submit": self._final_run_id,
        }
        method = mapping.get(submission_stage)
        if method is None:
            raise WorkflowError(
                f"unsupported Pulsar failed-batch recovery stage: {submission_stage}"
            )
        return method(runtime)

    def _results_root_for_submission(
        self, runtime: WorkflowRuntime, submission_stage: str
    ) -> str:
        return self._results_root(
            runtime, self._run_id_for_submission(runtime, submission_stage)
        )

    def _preflight(self, runtime: WorkflowRuntime) -> StageOutcome:
        output = runtime.run_command(
            ["python3", "-B", "-m", "src.benchmark.backends.lifecycle_cli", "list"]
        )
        if "pulsar" not in output.split():
            raise WorkflowError("Pulsar backend adapter is not registered")
        gate_path = self._phase1_gate_path(runtime)
        if not gate_path.is_file():
            if runtime.dry_run:
                return StageOutcome(
                    details={
                        "backend_registered": True,
                        "phase1_gate_valid": False,
                        "phase1_gate_path": str(gate_path),
                        "phase1_gate_status": "absent; dry-run planning only",
                    }
                )
            raise WorkflowError(
                "the Pulsar Phase 1 acceptance gate is missing; provide it at "
                f"{gate_path} or set PULSAR_PHASE1_GATE_REPORT"
            )
        gate = _read_json(gate_path)
        if gate.get("phase1_submission_authorized") is not True:
            raise WorkflowError("the immutable Pulsar Phase 1 acceptance gate is not valid")
        return StageOutcome(
            input_files=[str(gate_path)],
            details={
                "backend_registered": True,
                "phase1_gate_valid": True,
                "phase1_gate_path": str(gate_path),
            },
        )

    def _v1_generate(self, runtime: WorkflowRuntime) -> StageOutcome:
        root = Path(runtime.path("inputs", "pulsar-v1"))
        runtime.run_command(
            [
                "python3", "-B",
                runtime.repository_path("scripts", "generate_pulsar_campaigns.py"),
                "--output-root", str(root),
                "--phase1-size", "120",
                "--phase1-batch-size", "30",
                "--phase1-warmup-sec", "15",
                "--phase1-duration-sec", "30",
                "--phase1-drain-timeout-sec", "60",
                "--phase1-latency-sample-every", "10",
                "--phase1-gate-report", str(self._phase1_gate_path(runtime)),
            ]
        )
        manifest = root / "phase1" / "phase1_manifest.csv"
        validation = validate_campaign_manifest(
            manifest,
            project_root=Path(runtime.repository_path()),
            backend_id="pulsar",
            expected_case_count=120,
        )
        validation_path = root / "phase1" / "workflow_input_validation.json"
        write_validation(validation_path, validation)
        batches = _batches(root / "phase1" / "batches")
        if len(batches) != 4:
            raise WorkflowError(f"Pulsar Phase 1 generated {len(batches)} jobs, expected 4")
        return StageOutcome(
            artifacts=[str(root / "phase1" / "phase1_campaign_plan.json"), str(validation_path)],
            input_files=[
                str(manifest),
                *(str(path) for path in batches),
                *(str(path) for path in manifest_config_paths(manifest, Path(runtime.repository_path()))),
            ],
            details={
                "manifest": str(manifest),
                "batch_paths": [str(path) for path in batches],
                "case_count": 120,
                "job_count": 4,
            },
        )

    def _v1_phase1_submit(self, runtime: WorkflowRuntime) -> StageOutcome:
        batches = _batches(Path(runtime.path("inputs", "pulsar-v1", "phase1", "batches")))
        return runtime.submit_manifest_batches(
            stage_name="pulsar-v1-phase1",
            manifest_paths=[str(path) for path in batches],
            run_id=self._phase1_run_id(runtime),
            independent=True,
        )

    def _v1_phase1_wait(self, runtime: WorkflowRuntime) -> StageOutcome:
        return runtime.wait_for_stage_jobs("v1-phase1-submit")

    def _v1_phase1_verify(self, runtime: WorkflowRuntime) -> StageOutcome:
        return runtime.verify_results(
            manifest_path=runtime.path("inputs", "pulsar-v1", "phase1", "phase1_manifest.csv"),
            results_roots=[self._phase1_results_root(runtime)],
            expected_case_count=120,
            max_clock_invalid_cases=6,
            max_ineligible_cases=6,
            repair_clock_invalid_cases=False,
            repair_submission_stage="v1-phase1-submit",
            selected_results_group="v1-phase1",
        )

    def _v1_phase1_analyze(self, runtime: WorkflowRuntime) -> StageOutcome:
        if runtime.dry_run:
            return runtime.deferred("Pulsar Phase 1 analysis requires 120 new case results")
        root = Path(runtime.path("inputs", "pulsar-v1", "phase1"))
        output = Path(runtime.path("analysis", "v1", "phase1", "pulsar"))
        runtime.run_command(
            [
                "python3", "-B", runtime.repository_path("scripts", "analyze_pulsar_phase1.py"),
                "--results-root", self._selected_results_root(runtime, "v1-phase1", self._phase1_results_root(runtime)),
                "--manifest", str(root / "phase1_manifest.csv"),
                "--campaign-plan", str(root / "phase1_campaign_plan.json"),
                "--output-dir", str(output),
                "--machine-only",
            ]
        )
        common_output = Path(runtime.path("analysis", "v1", "phase1", "common"))
        runtime.run_command(
            [
                "python3", "-B", runtime.repository_path("scripts", "analyze_reproducible_campaign.py"),
                "--backend", "pulsar", "--stage", "phase1",
                "--manifest", str(root / "phase1_manifest.csv"),
                "--results-root", self._selected_results_root(runtime, "v1-phase1", self._phase1_results_root(runtime)),
                "--output-dir", str(common_output),
            ]
        )
        shortlist = output / "pulsar_phase1_shortlist.json"
        payload = _read_json(shortlist)
        selected = [str(row["config_id"]) for row in payload.get("configurations", [])]
        if len(selected) != 10 or len(set(selected)) != 10:
            raise WorkflowError("Pulsar Phase 1 did not produce a unique ten-workload shortlist")
        return StageOutcome(
            artifacts=[str(path) for path in output.rglob("*") if path.is_file()]
            + [str(path) for path in common_output.iterdir() if path.is_file()],
            selected_configs=selected,
            details={
                "shortlist": str(shortlist),
                "phase1_cases": str(output / "pulsar_phase1_cases.json"),
                "selected_configs": selected,
            },
        )

    def _v1_validation_generate(self, runtime: WorkflowRuntime) -> StageOutcome:
        shortlist = Path(runtime.path("analysis", "v1", "phase1", "pulsar", "pulsar_phase1_shortlist.json"))
        if not shortlist.is_file():
            return runtime.deferred("Pulsar validation generation awaits its new shortlist")
        output = Path(runtime.path("inputs", "pulsar-v1-validation"))
        profile = runtime.repository_path("configs", "backends", "pulsar", "profiles", "BASELINE_H16_D32.json")
        runtime.run_command(
            [
                "python3", "-B", runtime.repository_path("scripts", "generate_pulsar_reproducible_validation.py"),
                "--phase1-config-root", runtime.path("inputs", "pulsar-v1", "phase1", "generated_configs"),
                "--shortlist", str(shortlist),
                "--profile", profile,
                "--output-dir", str(output),
                "--authorize",
            ]
        )
        manifest = output / "validation_manifest.csv"
        validation = validate_campaign_manifest(
            manifest,
            project_root=Path(runtime.repository_path()),
            backend_id="pulsar",
            expected_case_count=50,
        )
        verify_sha256_file(output / "SHA256SUMS")
        validation_path = output / "workflow_input_validation.json"
        write_validation(validation_path, validation)
        batches = _batches(output / "batches")
        if len(batches) != 2:
            raise WorkflowError(f"Pulsar validation generated {len(batches)} jobs, expected 2")
        return StageOutcome(
            artifacts=[str(output / "validation_plan.json"), str(validation_path)],
            input_files=[
                str(shortlist),
                str(manifest),
                str(output / "SHA256SUMS"),
                *(str(path) for path in batches),
                *(str(path) for path in manifest_config_paths(manifest, Path(runtime.repository_path()))),
            ],
            details={"manifest": str(manifest), "batch_paths": [str(path) for path in batches], "case_count": 50, "job_count": 2},
        )

    def _v1_validation_submit(self, runtime: WorkflowRuntime) -> StageOutcome:
        root = Path(runtime.path("inputs", "pulsar-v1-validation"))
        if not (root / "validation_manifest.csv").is_file():
            return runtime.deferred("Pulsar validation submission awaits generated inputs")
        return runtime.submit_manifest_batches(
            stage_name="pulsar-v1-validation",
            manifest_paths=[str(path) for path in _batches(root / "batches")],
            run_id=self._validation_run_id(runtime),
        )

    def _v1_validation_wait(self, runtime: WorkflowRuntime) -> StageOutcome:
        return runtime.wait_for_stage_jobs("v1-validation-submit")

    def _v1_validation_verify(self, runtime: WorkflowRuntime) -> StageOutcome:
        return runtime.verify_results(
            manifest_path=runtime.path("inputs", "pulsar-v1-validation", "validation_manifest.csv"),
            results_roots=[self._validation_results_root(runtime)],
            expected_case_count=50,
            max_clock_invalid_cases=5,
            max_ineligible_cases=5,
            min_eligible_repeats_per_workload=3,
            repair_submission_stage="v1-validation-submit",
            selected_results_group="v1-validation",
        )

    def _v1_validation_analyze(self, runtime: WorkflowRuntime) -> StageOutcome:
        manifest = Path(runtime.path("inputs", "pulsar-v1-validation", "validation_manifest.csv"))
        if runtime.dry_run or not manifest.is_file():
            return runtime.deferred("Pulsar repeated analysis requires 50 completed case results")
        output = Path(runtime.path("analysis", "v1", "validation"))
        runtime.run_command(
            [
                "python3", "-B", runtime.repository_path("scripts", "analyze_reproducible_campaign.py"),
                "--backend", "pulsar", "--stage", "validation",
                "--manifest", str(manifest),
                "--results-root", self._selected_results_root(runtime, "v1-validation", self._validation_results_root(runtime)),
                "--output-dir", str(output),
            ]
        )
        rows = _read_json_list(output / "validation_summary_by_original.json")
        winner = str(rows[0].get("original_config_id", "")) if rows else ""
        return StageOutcome(
            artifacts=[str(path) for path in output.iterdir() if path.is_file()],
            selected_configs=[winner] if winner else [],
            details={"winner": winner, "summary": str(output / "validation_summary_by_original.json")},
        )

    def _v1_finalize(self, runtime: WorkflowRuntime) -> StageOutcome:
        validation = Path(runtime.path("analysis", "v1", "validation", "validation_summary_by_original.json"))
        if runtime.dry_run or not validation.is_file():
            return runtime.deferred("Pulsar V1 machine-result finalization awaits repeated-validation analysis")
        output = Path(runtime.path("artifacts", "v1"))
        runtime.run_command(
            [
                "python3", "-B", runtime.repository_path("scripts", "finalize_reproducible_results.py"),
                "--backend", "pulsar",
                "--phase1-dir", runtime.path("analysis", "v1", "phase1", "common"),
                "--validation-dir", runtime.path("analysis", "v1", "validation"),
                "--shortlist-json", runtime.path("analysis", "v1", "phase1", "pulsar", "pulsar_phase1_shortlist.json"),
                "--shortlist-csv", runtime.path("analysis", "v1", "phase1", "pulsar", "pulsar_phase1_shortlist.csv"),
                "--phase1-manifest", runtime.path("inputs", "pulsar-v1", "phase1", "phase1_manifest.csv"),
                "--validation-manifest", runtime.path("inputs", "pulsar-v1-validation", "validation_manifest.csv"),
                "--output-dir", str(output),
            ]
        )
        return StageOutcome(artifacts=[str(path) for path in output.rglob("*") if path.is_file()], details={"machine_results_dir": str(output)})

    def _v2_gate_check(self, runtime: WorkflowRuntime) -> StageOutcome:
        gate_path = self._phase1_gate_path(runtime)
        if runtime.dry_run and not gate_path.is_file():
            return runtime.deferred(
                "Pulsar V2 gate check requires an accepted instrumentation report"
            )
        gate = _read_json(gate_path)
        instrumentation = _dict(gate.get("instrumentation"))
        if gate.get("phase1_submission_authorized") is not True:
            raise WorkflowError("Pulsar instrumentation gate is not authorized")
        if instrumentation.get("passed") is not True:
            raise WorkflowError("Pulsar instrumentation-overhead gate failed")
        return StageOutcome(
            input_files=[str(gate_path)],
            details={
                "gate_source": str(gate_path),
                "gate_is_historical_immutable_evidence": True,
                "note": "Pulsar has no separate rate-calibration stage in its predefined memory-profile policy",
            },
        )

    def _phase1_gate_path(self, runtime: WorkflowRuntime) -> Path:
        override = os.environ.get("PULSAR_PHASE1_GATE_REPORT", "").strip()
        if override:
            return Path(override).expanduser().resolve()
        return Path(
            runtime.repository_path(
                "results",
                "published",
                "pulsar",
                "phase1-gate",
                "acceptance_report.json",
            )
        )

    def _v2_screening_generate(self, runtime: WorkflowRuntime) -> StageOutcome:
        phase1 = Path(runtime.path("analysis", "v1", "phase1", "pulsar"))
        if runtime.dry_run or not (phase1 / "pulsar_phase1_cases.json").is_file():
            return runtime.deferred("Pulsar profile screening awaits its new Phase 1 evidence")
        output = Path(runtime.path("inputs", "pulsar-v2", "screening"))
        profiles = Path(runtime.path("inputs", "pulsar-v2", "profiles"))
        baseline_source = Path(
            runtime.repository_path(
                "configs",
                "backends",
                "pulsar",
                "profiles",
                "BASELINE_H16_D32.json",
            )
        )
        baseline_snapshot = profiles / baseline_source.name
        snapshot_created, baseline_sha256 = _snapshot_immutable_input(
            baseline_source,
            baseline_snapshot,
        )
        runtime.run_command(
            [
                "python3", "-B", runtime.repository_path("scripts", "generate_pulsar_phase2.py"),
                "--phase1-config-root", runtime.path("inputs", "pulsar-v1", "phase1", "generated_configs"),
                "--shortlist", str(phase1 / "pulsar_phase1_shortlist.json"),
                "--phase1-summary", str(phase1 / "pulsar_phase1_summary.json"),
                "--phase1-cases", str(phase1 / "pulsar_phase1_cases.json"),
                "--output-dir", str(output), "--profile-root", str(profiles),
            ]
        )
        runtime.run_command(
            [
                "python3", "-B", runtime.repository_path("scripts", "authorize_pulsar_phase2_screening.py"),
                "--plan", str(output / "phase2_campaign_plan.json"),
                "--manifest", str(output / "memory_screening_manifest.csv"),
                "--profile-root", str(profiles),
                "--project-root", runtime.repository_path(),
            ]
        )
        manifest = output / "memory_screening_manifest.csv"
        validation = validate_campaign_manifest(
            manifest,
            project_root=Path(runtime.repository_path()), backend_id="pulsar", expected_case_count=30,
        )
        path = output / "workflow_input_validation.json"
        write_validation(path, validation)
        return StageOutcome(
            artifacts=[str(output / "phase2_campaign_plan.json"), str(path)],
            input_files=[
                str(baseline_snapshot),
                str(manifest),
                *(str(path) for path in manifest_config_paths(manifest, Path(runtime.repository_path()))),
            ],
            details={
                "manifest": str(manifest),
                "case_count": 30,
                "job_count": 1,
                "profile_root": str(profiles),
                "baseline_profile_snapshot": str(baseline_snapshot),
                "baseline_profile_sha256": baseline_sha256,
                "baseline_profile_snapshot_created": snapshot_created,
            },
        )

    def _v2_screening_submit(self, runtime: WorkflowRuntime) -> StageOutcome:
        manifest = Path(runtime.path("inputs", "pulsar-v2", "screening", "memory_screening_manifest.csv"))
        if not manifest.is_file():
            return runtime.deferred("Pulsar profile-screening inputs are not available")
        return runtime.submit_manifest_batches(
            stage_name="pulsar-v2-screening",
            manifest_paths=[str(manifest)],
            run_id=self._screening_run_id(runtime),
            allow_profile_changes=True,
        )

    def _v2_screening_wait(self, runtime: WorkflowRuntime) -> StageOutcome:
        return runtime.wait_for_stage_jobs("v2-screening-submit")

    def _v2_screening_verify(self, runtime: WorkflowRuntime) -> StageOutcome:
        return runtime.verify_results(
            manifest_path=runtime.path("inputs", "pulsar-v2", "screening", "memory_screening_manifest.csv"),
            results_roots=[self._screening_results_root(runtime)],
            expected_case_count=30,
            max_clock_invalid_cases=1,
            max_ineligible_cases=1,
            repair_submission_stage="v2-screening-submit",
            selected_results_group="v2-screening",
        )

    def _v2_screening_analyze(self, runtime: WorkflowRuntime) -> StageOutcome:
        if runtime.dry_run:
            return runtime.deferred("Pulsar profile selection requires 30 screening case results")
        output = Path(runtime.path("analysis", "v2", "screening"))
        inputs = Path(runtime.path("inputs", "pulsar-v2", "screening"))
        runtime.run_command(
            [
                "python3", "-B", runtime.repository_path("scripts", "analyze_pulsar_phase2_screening.py"),
                "--results-root", self._selected_results_root(runtime, "v2-screening", self._screening_results_root(runtime)),
                "--manifest", str(inputs / "memory_screening_manifest.csv"),
                "--campaign-plan", str(inputs / "phase2_campaign_plan.json"),
                "--output-dir", str(output),
                "--machine-only",
            ]
        )
        selection = _read_json(output / "pulsar_phase2_candidate_selection.json")
        profiles = [str(item) for item in selection.get("confirmation_profiles", [])]
        if selection.get("status") != "ready_for_confirmation" or len(profiles) != 3:
            raise WorkflowError("Pulsar Phase 2 candidate selection is incomplete or ambiguous")
        return StageOutcome(
            artifacts=[str(path) for path in output.iterdir() if path.is_file()],
            selected_profiles=profiles,
            details={"confirmation_profiles": profiles, "selection": str(output / "pulsar_phase2_candidate_selection.json")},
        )

    def _v2_confirmation_generate(self, runtime: WorkflowRuntime) -> StageOutcome:
        if runtime.dry_run:
            return runtime.deferred("Pulsar confirmation generation awaits screening results")
        phase1 = Path(runtime.path("analysis", "v1", "phase1", "pulsar"))
        screening = Path(runtime.path("analysis", "v2", "screening"))
        output = Path(runtime.path("inputs", "pulsar-v2", "confirmation"))
        profiles = Path(runtime.path("inputs", "pulsar-v2", "profiles"))
        runtime.run_command(
            [
                "python3", "-B", runtime.repository_path("scripts", "generate_pulsar_phase2_confirmation.py"),
                "--selection", str(screening / "pulsar_phase2_candidate_selection.json"),
                "--phase1-config-root", runtime.path("inputs", "pulsar-v1", "phase1", "generated_configs"),
                "--shortlist", str(phase1 / "pulsar_phase1_shortlist.json"),
                "--phase1-cases", str(phase1 / "pulsar_phase1_cases.json"),
                "--profile-root", str(profiles), "--output-dir", str(output),
            ]
        )
        runtime.run_command(
            [
                "python3", "-B", runtime.repository_path("scripts", "authorize_pulsar_phase2_confirmation.py"),
                "--plan", str(output / "confirmation_plan.json"),
                "--manifest", str(output / "memory_confirmation_manifest.csv"),
                "--selection", str(screening / "pulsar_phase2_candidate_selection.json"),
                "--profile-root", str(profiles), "--project-root", runtime.repository_path(),
            ]
        )
        manifest = output / "memory_confirmation_manifest.csv"
        count = _csv_count(manifest)
        validation = validate_campaign_manifest(
            manifest, project_root=Path(runtime.repository_path()), backend_id="pulsar", expected_case_count=count,
        )
        path = output / "workflow_input_validation.json"
        write_validation(path, validation)
        return StageOutcome(
            artifacts=[str(output / "confirmation_plan.json"), str(path)],
            input_files=[
                str(manifest),
                *(str(path) for path in manifest_config_paths(manifest, Path(runtime.repository_path()))),
            ],
            selected_profiles=runtime.stage_details("v2-screening-analyze").get("confirmation_profiles", []),
            details={"manifest": str(manifest), "case_count": count, "job_count": 1},
        )

    def _v2_confirmation_submit(self, runtime: WorkflowRuntime) -> StageOutcome:
        manifest = Path(runtime.path("inputs", "pulsar-v2", "confirmation", "memory_confirmation_manifest.csv"))
        if not manifest.is_file():
            return runtime.deferred("Pulsar confirmation inputs are not available")
        return runtime.submit_manifest_batches(
            stage_name="pulsar-v2-confirmation", manifest_paths=[str(manifest)],
            run_id=self._confirmation_run_id(runtime), allow_profile_changes=True,
        )

    def _v2_confirmation_wait(self, runtime: WorkflowRuntime) -> StageOutcome:
        return runtime.wait_for_stage_jobs("v2-confirmation-submit")

    def _v2_confirmation_verify(self, runtime: WorkflowRuntime) -> StageOutcome:
        manifest = Path(runtime.path("inputs", "pulsar-v2", "confirmation", "memory_confirmation_manifest.csv"))
        count = _csv_count(manifest) if manifest.is_file() else 0
        if not count:
            return runtime.deferred("Pulsar confirmation verification awaits generated inputs")
        return runtime.verify_results(
            manifest_path=str(manifest),
            results_roots=[self._confirmation_results_root(runtime)],
            expected_case_count=count,
            max_clock_invalid_cases=2,
            max_ineligible_cases=2,
            repair_submission_stage="v2-confirmation-submit",
            selected_results_group="v2-confirmation",
        )

    def _v2_confirmation_analyze(self, runtime: WorkflowRuntime) -> StageOutcome:
        if runtime.dry_run:
            return runtime.deferred("Pulsar profile freeze requires confirmation case results")
        screening = Path(runtime.path("analysis", "v2", "screening"))
        inputs = Path(runtime.path("inputs", "pulsar-v2", "confirmation"))
        output = Path(runtime.path("analysis", "v2", "confirmation"))
        runtime.run_command(
            [
                "python3", "-B", runtime.repository_path("scripts", "analyze_pulsar_phase2_confirmation.py"),
                "--screening-cases", str(screening / "pulsar_phase2_screening_cases.json"),
                "--screening-selection", str(screening / "pulsar_phase2_candidate_selection.json"),
                "--confirmation-results-root", self._selected_results_root(runtime, "v2-confirmation", self._confirmation_results_root(runtime)),
                "--confirmation-manifest", str(inputs / "memory_confirmation_manifest.csv"),
                "--confirmation-plan", str(inputs / "confirmation_plan.json"),
                "--output-dir", str(output),
                "--machine-only",
            ]
        )
        selection = _read_json(output / "pulsar_phase2_final_selection.json")
        winner = str(selection.get("frozen_profile_id", ""))
        if selection.get("status") != "profile_frozen" or not winner:
            raise WorkflowError("Pulsar Phase 2 profile freeze is absent or ambiguous")
        return StageOutcome(
            artifacts=[str(path) for path in output.iterdir() if path.is_file()],
            selected_profiles=[winner],
            details={"winner": winner, "selection": str(output / "pulsar_phase2_final_selection.json")},
        )

    def _v2_final_generate(self, runtime: WorkflowRuntime) -> StageOutcome:
        if runtime.dry_run:
            return runtime.deferred("Pulsar final generation awaits a frozen profile")
        winner = str(runtime.stage_details("v2-confirmation-analyze").get("winner", ""))
        profile = Path(runtime.path("inputs", "pulsar-v2", "profiles", f"{winner}.json"))
        shortlist = Path(runtime.path("analysis", "v1", "phase1", "pulsar", "pulsar_phase1_shortlist.json"))
        output = Path(runtime.path("inputs", "pulsar-v2", "final-validation"))
        runtime.run_command(
            [
                "python3", "-B", runtime.repository_path("scripts", "generate_pulsar_reproducible_validation.py"),
                "--phase1-config-root", runtime.path("inputs", "pulsar-v1", "phase1", "generated_configs"),
                "--shortlist", str(shortlist), "--profile", str(profile),
                "--output-dir", str(output),
                "--campaign-id", "pulsar-v2-frozen-profile-validation",
                "--stage", "v2-final-validation", "--authorize",
            ]
        )
        manifest = output / "validation_manifest.csv"
        validation = validate_campaign_manifest(
            manifest, project_root=Path(runtime.repository_path()), backend_id="pulsar", expected_case_count=50,
        )
        path = output / "workflow_input_validation.json"
        write_validation(path, validation)
        return StageOutcome(
            artifacts=[str(output / "validation_plan.json"), str(path)],
            input_files=[
                str(manifest),
                str(shortlist),
                str(profile),
                *(str(path) for path in manifest_config_paths(manifest, Path(runtime.repository_path()))),
            ],
            selected_profiles=[winner],
            details={"manifest": str(manifest), "case_count": 50, "job_count": 2, "winner": winner},
        )

    def _v2_final_submit(self, runtime: WorkflowRuntime) -> StageOutcome:
        root = Path(runtime.path("inputs", "pulsar-v2", "final-validation"))
        if not (root / "validation_manifest.csv").is_file():
            return runtime.deferred("Pulsar final-validation inputs are not available")
        return runtime.submit_manifest_batches(
            stage_name="pulsar-v2-final", manifest_paths=[str(path) for path in _batches(root / "batches")],
            run_id=self._final_run_id(runtime),
        )

    def _v2_final_wait(self, runtime: WorkflowRuntime) -> StageOutcome:
        return runtime.wait_for_stage_jobs("v2-final-submit")

    def _v2_final_verify(self, runtime: WorkflowRuntime) -> StageOutcome:
        return runtime.verify_results(
            manifest_path=runtime.path("inputs", "pulsar-v2", "final-validation", "validation_manifest.csv"),
            results_roots=[self._final_results_root(runtime)],
            expected_case_count=50,
            max_clock_invalid_cases=5,
            max_ineligible_cases=5,
            min_eligible_repeats_per_workload=3,
            repair_submission_stage="v2-final-submit",
            selected_results_group="v2-final",
        )

    def _v2_final_analyze(self, runtime: WorkflowRuntime) -> StageOutcome:
        if runtime.dry_run:
            return runtime.deferred("Pulsar final machine analysis requires 50 frozen-profile case results")
        manifest = runtime.path("inputs", "pulsar-v2", "final-validation", "validation_manifest.csv")
        analysis = Path(runtime.path("analysis", "v2", "final-validation"))
        runtime.run_command(
            [
                "python3", "-B", runtime.repository_path("scripts", "analyze_reproducible_campaign.py"),
                "--backend", "pulsar", "--stage", "validation",
                "--manifest", manifest,
                "--results-root", self._selected_results_root(runtime, "v2-final", self._final_results_root(runtime)),
                "--output-dir", str(analysis),
            ]
        )
        decisions = Path(runtime.path("analysis", "v2", "confirmation", "pulsar_phase2_final_selection.json"))
        if not decisions.is_file():
            raise WorkflowError("Pulsar V2 profile-selection artifact is missing")
        output = Path(runtime.path("artifacts", "v2"))
        runtime.run_command(
            [
                "python3", "-B", runtime.repository_path("scripts", "finalize_reproducible_results.py"),
                "--backend", "pulsar",
                "--campaign-phase", "v2",
                "--phase1-dir", runtime.path("analysis", "v1", "phase1", "common"),
                "--validation-dir", str(analysis),
                "--shortlist-json", runtime.path("analysis", "v1", "phase1", "pulsar", "pulsar_phase1_shortlist.json"),
                "--shortlist-csv", runtime.path("analysis", "v1", "phase1", "pulsar", "pulsar_phase1_shortlist.csv"),
                "--phase1-manifest", runtime.path("inputs", "pulsar-v1", "phase1", "phase1_manifest.csv"),
                "--validation-manifest", manifest,
                "--output-dir", str(output),
            ]
        )
        return StageOutcome(
            artifacts=[str(path) for path in output.rglob("*") if path.is_file()],
            input_files=[str(decisions)],
            details={"machine_results_dir": str(output)},
        )

    def _phase1_run_id(self, runtime: WorkflowRuntime) -> str:
        return f"{Path(runtime.path()).name}-pulsar-v1-phase1"

    def _validation_run_id(self, runtime: WorkflowRuntime) -> str:
        return f"{Path(runtime.path()).name}-pulsar-v1-validation"

    def _screening_run_id(self, runtime: WorkflowRuntime) -> str:
        return f"{Path(runtime.path()).name}-pulsar-v2-screening"

    def _confirmation_run_id(self, runtime: WorkflowRuntime) -> str:
        return f"{Path(runtime.path()).name}-pulsar-v2-confirmation"

    def _final_run_id(self, runtime: WorkflowRuntime) -> str:
        return f"{Path(runtime.path()).name}-pulsar-v2-final"

    def _results_root(self, runtime: WorkflowRuntime, run_id: str) -> str:
        return runtime.repository_path("results", "runs", "pulsar", run_id)

    def _phase1_results_root(self, runtime: WorkflowRuntime) -> str:
        return self._results_root(runtime, self._phase1_run_id(runtime))

    def _validation_results_root(self, runtime: WorkflowRuntime) -> str:
        return self._results_root(runtime, self._validation_run_id(runtime))

    def _screening_results_root(self, runtime: WorkflowRuntime) -> str:
        return self._results_root(runtime, self._screening_run_id(runtime))

    def _confirmation_results_root(self, runtime: WorkflowRuntime) -> str:
        return self._results_root(runtime, self._confirmation_run_id(runtime))

    def _final_results_root(self, runtime: WorkflowRuntime) -> str:
        return self._results_root(runtime, self._final_run_id(runtime))

    def _selected_results_root(
        self,
        runtime: WorkflowRuntime,
        group: str,
        fallback: str,
    ) -> str:
        selected = Path(runtime.path("selected-results", group))
        return str(selected) if selected.is_dir() else fallback


def _batches(root: Path) -> list[Path]:
    return sorted(root.glob("batch_*.csv"))


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise WorkflowError(f"expected JSON object: {path}")
    return value


def _snapshot_immutable_input(source: Path, destination: Path) -> tuple[bool, str]:
    """Copy one canonical input once and reject run-local profile drift."""
    if not source.is_file():
        raise WorkflowError(f"canonical workflow input is missing: {source}")
    source_sha256 = hashlib.sha256(source.read_bytes()).hexdigest()
    if destination.exists():
        if not destination.is_file():
            raise WorkflowError(
                f"workflow input snapshot is not a regular file: {destination}"
            )
        destination_sha256 = hashlib.sha256(destination.read_bytes()).hexdigest()
        if destination_sha256 != source_sha256:
            raise WorkflowError(
                "workflow input snapshot differs from its canonical source: "
                f"{destination}"
            )
        return False, source_sha256

    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    destination_sha256 = hashlib.sha256(destination.read_bytes()).hexdigest()
    if destination_sha256 != source_sha256:
        raise WorkflowError(f"workflow input snapshot verification failed: {destination}")
    return True, source_sha256


def _read_json_list(path: Path) -> list[dict[str, Any]]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise WorkflowError(f"expected JSON array of objects: {path}")
    return value


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _next_repair_index(runtime: WorkflowRuntime, submission_stage: str) -> int:
    root = Path(runtime.path("inputs", "recovery", submission_stage))
    if not root.exists():
        return 1
    indices = []
    for path in root.glob("attempt_*"):
        try:
            indices.append(int(path.name.rsplit("_", 1)[1]))
        except (IndexError, ValueError):
            continue
    return max(indices, default=0) + 1


def _csv_count(path: Path) -> int:
    import csv

    with path.open("r", encoding="utf-8", newline="") as handle:
        return sum(1 for _ in csv.DictReader(handle))
