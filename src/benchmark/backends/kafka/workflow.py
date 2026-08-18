from __future__ import annotations

import csv
import json
from pathlib import Path
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


class KafkaReproducibleWorkflow(BackendWorkflow):
    backend_id = "kafka"

    def stages(self) -> tuple[StageSpec, ...]:
        return (
            StageSpec("preflight", "setup", "check Kafka workflow implementation and environment"),
            StageSpec("v1-generate", "phase1", "generate the isolated 120-case reproducible V1 campaign", ("preflight",)),
            StageSpec("v1-phase1-submit", "phase1", "submit four independent Kafka Phase 1 batch jobs", ("v1-generate",)),
            StageSpec("v1-phase1-wait", "phase1", "wait for Kafka Phase 1 jobs", ("v1-phase1-submit",)),
            StageSpec("v1-phase1-verify", "phase1", "verify 120 complete Phase 1 results and the eligible-evidence threshold", ("v1-phase1-wait",)),
            StageSpec("v1-phase1-analyze", "phase1", "rank Phase 1 and derive its ten-workload shortlist", ("v1-phase1-verify",)),
            StageSpec("v1-validation-generate", "validation", "generate five randomized shortlist-validation blocks", ("v1-phase1-analyze",)),
            StageSpec("v1-validation-submit", "validation", "submit repeated V1 validation batches", ("v1-validation-generate",)),
            StageSpec("v1-validation-wait", "validation", "wait for repeated V1 validation jobs", ("v1-validation-submit",)),
            StageSpec("v1-validation-verify", "validation", "verify all 50 repeated-validation case results", ("v1-validation-wait",)),
            StageSpec("v1-validation-analyze", "validation", "aggregate and rank repeated V1 validation", ("v1-validation-verify",)),
            StageSpec("v1-finalize", "v1-finalize", "finalize validated V1 JSON/CSV artifacts, manifests, and checksums", ("v1-validation-analyze",)),
            StageSpec("v2-initialize", "v2-pilot", "generate broker profiles, instrumentation pilots, and calibration inputs", ("v1-phase1-analyze",)),
            StageSpec("v2-pilot-submit", "v2-pilot", "submit the instrumentation A/B pilot", ("v2-initialize",)),
            StageSpec("v2-pilot-wait", "v2-pilot", "wait for the instrumentation pilot", ("v2-pilot-submit",)),
            StageSpec("v2-pilot-verify", "v2-pilot", "verify all six instrumentation case results", ("v2-pilot-wait",)),
            StageSpec("v2-pilot-analyze", "v2-pilot", "apply the predefined instrumentation-overhead gate", ("v2-pilot-verify",)),
            StageSpec("v2-calibration-submit", "v2-calibration", "submit three B0 latency-anchor calibration cases", ("v2-pilot-analyze",)),
            StageSpec("v2-calibration-wait", "v2-calibration", "wait for rate calibration", ("v2-calibration-submit",)),
            StageSpec("v2-calibration-verify", "v2-calibration", "verify three valid calibration case results", ("v2-calibration-wait",)),
            StageSpec("v2-calibration-analyze", "v2-calibration", "materialize calibration analysis artifacts", ("v2-calibration-verify",)),
            StageSpec("v2-screening-generate", "v2-screening", "derive fixed offered rate and generate 30 profile-screening cases", ("v2-calibration-analyze",)),
            StageSpec("v2-screening-submit", "v2-screening", "submit the six-profile screening campaign", ("v2-screening-generate",)),
            StageSpec("v2-screening-wait", "v2-screening", "wait for broker-profile screening", ("v2-screening-submit",)),
            StageSpec("v2-screening-verify", "v2-screening", "verify all 30 screening case results", ("v2-screening-wait",)),
            StageSpec("v2-screening-analyze", "v2-screening", "select B0 and two unambiguous confirmation candidates", ("v2-screening-verify",)),
            StageSpec("v2-confirmation-generate", "v2-confirmation", "generate two additional randomized profile repetitions", ("v2-screening-analyze",)),
            StageSpec("v2-confirmation-submit", "v2-confirmation", "submit the profile-confirmation campaign", ("v2-confirmation-generate",)),
            StageSpec("v2-confirmation-wait", "v2-confirmation", "wait for profile confirmation", ("v2-confirmation-submit",)),
            StageSpec("v2-confirmation-verify", "v2-confirmation", "verify all required confirmation case results", ("v2-confirmation-wait",)),
            StageSpec("v2-confirmation-analyze", "v2-confirmation", "apply the predefined B0/non-regression profile-freeze policy", ("v2-confirmation-verify",)),
            StageSpec("v2-final-generate", "v2-final", "freeze the winner and generate five validation blocks", ("v2-confirmation-analyze",)),
            StageSpec("v2-final-submit", "v2-final", "submit frozen-profile final validation", ("v2-final-generate",)),
            StageSpec("v2-final-wait", "v2-final", "wait for final V2 validation", ("v2-final-submit",)),
            StageSpec("v2-final-verify", "v2-final", "verify all 50 frozen-profile validation case results", ("v2-final-wait",)),
            StageSpec("v2-final-analyze", "v2-final", "generate complete validated V2 JSON/CSV artifacts and checksums", ("v2-final-verify",)),
        )

    def required_files(self) -> tuple[str, ...]:
        return (
            "scripts/reproducible_benchmark_workflow.py",
            "scripts/run_reproducible_benchmark.sh",
            "scripts/select_slurm_partition.py",
            "src/benchmark/workflow/base.py",
            "src/benchmark/workflow/engine.py",
            "src/benchmark/workflow/recovery.py",
            "src/benchmark/backends/kafka/workflow.py",
            "scripts/generate_kafka_reproducible_campaigns.py",
            "scripts/analyze_kafka_reproducible_campaign.py",
            "scripts/submit_kafka_reproducible_campaign.sh",
            "scripts/submit_simultaneous_budgeted_batch.sh",
            "scripts/generate_broker_tuning_v2.py",
            "scripts/analyze_broker_tuning_v2.py",
            "scripts/submit_broker_tuning_v2_campaign.sh",
            "scripts/finalize_reproducible_results.py",
            "configs/one_broker_mpi_simultaneous.json",
        )

    def execute(self, stage: StageSpec, runtime: WorkflowRuntime) -> StageOutcome:
        method = getattr(self, "_" + stage.name.replace("-", "_"), None)
        if method is None:
            raise WorkflowError(f"Kafka workflow stage is not implemented: {stage.name}")
        return method(runtime)

    def retry_failed_batches(
        self,
        submission_stage: str,
        failed_job_ids: tuple[str, ...],
        runtime: WorkflowRuntime,
    ) -> StageOutcome:
        manifest, source_batches = self._submission_inputs(
            runtime, submission_stage
        )
        if not manifest.is_file() or not source_batches:
            raise WorkflowError(
                f"{submission_stage}: original Kafka manifests are unavailable for repair"
            )
        repair_index = _next_repair_index(runtime, submission_stage)
        result_root = self._results_root_for_submission(runtime, submission_stage)
        repair_root = Path(
            runtime.path("inputs", "recovery", submission_stage, f"attempt_{repair_index:02d}")
        )
        plan = write_incomplete_case_manifests(
            source_manifests=source_batches,
            results_roots=[Path(result_root)],
            output_dir=repair_root / "batches",
            backend_id="kafka",
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
            manifest=manifest,
            result_root=result_root,
            repair_index=repair_index,
            repair_root=repair_root,
            plan=plan,
            details={"failed_original_job_ids": list(failed_job_ids)},
        )

    def repair_invalid_cases(
        self,
        submission_stage: str,
        case_ids: tuple[str, ...],
        runtime: WorkflowRuntime,
    ) -> StageOutcome:
        manifest, source_batches = self._submission_inputs(
            runtime, submission_stage
        )
        if not manifest.is_file() or not source_batches:
            raise WorkflowError(
                f"{submission_stage}: original Kafka manifests are unavailable for repair"
            )
        repair_index = _next_repair_index(runtime, submission_stage)
        result_root = self._results_root_for_submission(runtime, submission_stage)
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
            manifest=manifest,
            result_root=result_root,
            repair_index=repair_index,
            repair_root=repair_root,
            plan=plan,
            details={"invalid_case_ids": list(case_ids)},
        )

    def _submit_repair_plan(
        self,
        *,
        runtime: WorkflowRuntime,
        submission_stage: str,
        manifest: Path,
        result_root: str,
        repair_index: int,
        repair_root: Path,
        plan: RecoveryManifestPlan,
        details: dict[str, Any],
    ) -> StageOutcome:
        ledger = Path(runtime.path("slurm", f"{submission_stage}_repair_{repair_index:02d}.tsv"))
        if submission_stage.startswith("v1-"):
            command = runtime.repository_path(
                "scripts", "submit_kafka_reproducible_campaign.sh"
            )
            environment = {
                "KAFKA_REPRO_RUN_ID": self._v1_run_id(runtime),
                "KAFKA_REPRO_STAGE": (
                    "validation" if "validation" in submission_stage else "phase1"
                ),
                "KAFKA_REPRO_BATCH_DIR": str(repair_root / "batches"),
                "KAFKA_REPRO_LEDGER_DIR": str(ledger.parent),
                "KAFKA_REPRO_LEDGER_PATH": str(ledger),
                "KAFKA_REPRO_REPAIR_ID": f"repair-{repair_index:02d}",
                "KAFKA_REPRO_INDEPENDENT_BATCHES": (
                    "1" if submission_stage == "v1-phase1-submit" else "0"
                ),
            }
        elif submission_stage in _KAFKA_V2_SUBMISSION_STEMS:
            command = runtime.repository_path(
                "scripts", "submit_broker_tuning_v2_campaign.sh"
            )
            environment = {
                "V2_RUN_ID": self._v2_run_id(runtime),
                "V2_BATCH_DIR": str(repair_root / "batches"),
                "V2_LEDGER_DIR": str(ledger.parent),
                "V2_LEDGER_PATH": str(ledger),
                "V2_REPAIR_ID": f"repair-{repair_index:02d}",
            }
        else:
            raise WorkflowError(
                f"unsupported Kafka failed-batch recovery stage: {submission_stage}"
            )
        runtime.run_command(
            [command, str(manifest)],
            environment={
                **runtime.submission_environment(),
                "DRY_RUN": "0",
                "BENCHMARK_REPORT_MODE": "machine",
                "SKIP_MONITORING_GRAPHS": "1",
                **environment,
            },
        )
        outcome = runtime.outcome_from_submission_ledger(
            ledger_path=str(ledger),
            expected_jobs=len(plan.manifest_paths),
            result_paths=[result_root],
            input_files=[str(path) for path in plan.manifest_paths],
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
    ) -> tuple[Path, list[Path]]:
        if submission_stage == "v1-phase1-submit":
            details = runtime.stage_details("v1-generate")
            return Path(str(details.get("manifest", ""))), [
                Path(path) for path in details.get("batch_paths", [])
            ]
        if submission_stage == "v1-validation-submit":
            details = runtime.stage_details("v1-validation-generate")
            return Path(str(details.get("manifest", ""))), [
                Path(path) for path in details.get("batch_paths", [])
            ]
        stem = _KAFKA_V2_SUBMISSION_STEMS.get(submission_stage)
        if stem is None:
            return Path(), []
        manifest = Path(
            runtime.path("inputs", "kafka-v2", "campaigns", f"{stem}.csv")
        )
        return manifest, _v2_batches(manifest)

    def _results_root_for_submission(
        self, runtime: WorkflowRuntime, submission_stage: str
    ) -> str:
        if submission_stage.startswith("v1-"):
            return self._v1_results_root(runtime)
        if submission_stage in _KAFKA_V2_SUBMISSION_STEMS:
            return self._v2_results_root(runtime)
        raise WorkflowError(
            f"unsupported Kafka failed-batch recovery stage: {submission_stage}"
        )

    def _preflight(self, runtime: WorkflowRuntime) -> StageOutcome:
        output = runtime.run_command(
            ["python3", "-B", "-m", "src.benchmark.backends.lifecycle_cli", "list"]
        )
        if "kafka" not in output.split():
            raise WorkflowError("Kafka backend adapter is not registered")
        return StageOutcome(details={"backend_registered": True})

    def _v1_generate(self, runtime: WorkflowRuntime) -> StageOutcome:
        root = Path(runtime.path("inputs", "kafka-v1"))
        runtime.run_command(
            [
                "python3",
                "-B",
                runtime.repository_path("scripts", "generate_kafka_reproducible_campaigns.py"),
                "--root",
                str(root),
                "--screening-batch-size",
                "30",
                "--validation-batch-size",
                "30",
            ]
        )
        manifest = root / "phase1" / "sweep_manifest.csv"
        validation = validate_campaign_manifest(
            manifest,
            project_root=Path(runtime.repository_path()),
            backend_id="kafka",
            expected_case_count=120,
        )
        verify_sha256_file(root / "SHA256SUMS")
        validation_path = root / "phase1_input_validation.json"
        write_validation(validation_path, validation)
        batches = _batches(root / "phase1" / "batches")
        if len(batches) != 4:
            raise WorkflowError(f"Kafka Phase 1 generated {len(batches)} jobs, expected 4")
        return StageOutcome(
            artifacts=[str(root / "campaign_plan.json"), str(validation_path), str(root / "SHA256SUMS")],
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
        root = Path(runtime.path("inputs", "kafka-v1"))
        manifest = root / "phase1" / "sweep_manifest.csv"
        return self._submit_v1(runtime, "phase1", manifest)

    def _v1_phase1_wait(self, runtime: WorkflowRuntime) -> StageOutcome:
        return runtime.wait_for_stage_jobs("v1-phase1-submit")

    def _v1_phase1_verify(self, runtime: WorkflowRuntime) -> StageOutcome:
        outcome = runtime.verify_results(
            manifest_path=runtime.path("inputs", "kafka-v1", "phase1", "sweep_manifest.csv"),
            results_roots=[self._v1_results_root(runtime)],
            expected_case_count=120,
            max_clock_invalid_cases=6,
            max_ineligible_cases=7,
            repair_clock_invalid_cases=False,
            repair_submission_stage="v1-phase1-submit",
            selected_results_group="v1",
        )
        clock_invalid = set(
            outcome.details.get("tolerated_clock_invalid_case_ids", [])
        )
        non_clock_ineligible = [
            case_id
            for case_id in outcome.details.get("ineligible_case_ids", [])
            if case_id not in clock_invalid
        ]
        if len(non_clock_ineligible) > 1:
            raise WorkflowError(
                "Kafka Phase 1 has more than one non-clock ineligible case: "
                + ", ".join(non_clock_ineligible)
            )
        outcome.details.update(
            {
                "max_clock_invalid_cases": 6,
                "max_non_clock_ineligible_cases": 1,
                "tolerated_non_clock_ineligible_case_ids": non_clock_ineligible,
            }
        )
        return outcome

    def _v1_phase1_analyze(self, runtime: WorkflowRuntime) -> StageOutcome:
        if runtime.dry_run:
            return runtime.deferred("Phase 1 analysis requires the 120 new case results")
        output = Path(runtime.path("analysis", "v1", "phase1"))
        manifest = runtime.path("inputs", "kafka-v1", "phase1", "sweep_manifest.csv")
        runtime.run_command(
            [
                "python3", "-B",
                runtime.repository_path("scripts", "analyze_kafka_reproducible_campaign.py"),
                "--stage", "phase1",
                "--manifest", manifest,
                "--results-root", self._selected_results_root(runtime, "v1", self._v1_results_root(runtime)),
                "--output-dir", str(output),
            ]
        )
        shortlist = output / "validation_shortlist.json"
        payload = _read_json(shortlist)
        selected = [str(item["config_id"]) for item in payload.get("shortlist", [])]
        if len(selected) != 10 or len(set(selected)) != 10:
            raise WorkflowError("Kafka Phase 1 did not produce one unique ten-workload shortlist")
        return StageOutcome(
            artifacts=[str(path) for path in output.iterdir() if path.is_file()],
            input_files=[manifest],
            selected_configs=selected,
            details={"shortlist": str(shortlist), "selected_configs": selected},
        )

    def _v1_validation_generate(self, runtime: WorkflowRuntime) -> StageOutcome:
        shortlist = Path(runtime.path("analysis", "v1", "phase1", "validation_shortlist.json"))
        if not shortlist.is_file():
            return runtime.deferred("validation generation awaits the new Phase 1 shortlist")
        root = Path(runtime.path("inputs", "kafka-v1"))
        runtime.run_command(
            [
                "python3", "-B",
                runtime.repository_path("scripts", "generate_kafka_reproducible_campaigns.py"),
                "--root", str(root),
                "--screening-batch-size", "30",
                "--validation-batch-size", "30",
                "--validation-only",
                "--validation-shortlist", str(shortlist),
            ]
        )
        manifest = root / "validation" / "validation_manifest.csv"
        validation = validate_campaign_manifest(
            manifest,
            project_root=Path(runtime.repository_path()),
            backend_id="kafka",
            expected_case_count=50,
        )
        verify_sha256_file(root / "SHA256SUMS")
        validation_path = root / "validation_input_validation.json"
        write_validation(validation_path, validation)
        batches = _batches(root / "validation" / "batches")
        if len(batches) != 2:
            raise WorkflowError(f"Kafka V1 validation generated {len(batches)} jobs, expected 2")
        return StageOutcome(
            artifacts=[str(validation_path), str(root / "campaign_plan.json")],
            input_files=[
                str(shortlist),
                str(manifest),
                str(root / "SHA256SUMS"),
                *(str(path) for path in batches),
                *(str(path) for path in manifest_config_paths(manifest, Path(runtime.repository_path()))),
            ],
            details={
                "manifest": str(manifest),
                "batch_paths": [str(path) for path in batches],
                "case_count": 50,
                "job_count": 2,
            },
        )

    def _v1_validation_submit(self, runtime: WorkflowRuntime) -> StageOutcome:
        manifest = Path(runtime.path("inputs", "kafka-v1", "validation", "validation_manifest.csv"))
        if not manifest.is_file():
            return runtime.deferred("validation submission awaits generated shortlist inputs")
        return self._submit_v1(runtime, "validation", manifest)

    def _v1_validation_wait(self, runtime: WorkflowRuntime) -> StageOutcome:
        return runtime.wait_for_stage_jobs("v1-validation-submit")

    def _v1_validation_verify(self, runtime: WorkflowRuntime) -> StageOutcome:
        return runtime.verify_results(
            manifest_path=runtime.path("inputs", "kafka-v1", "validation", "validation_manifest.csv"),
            results_roots=[self._v1_results_root(runtime)],
            expected_case_count=50,
            max_clock_invalid_cases=5,
            max_ineligible_cases=5,
            min_eligible_repeats_per_workload=3,
            repair_submission_stage="v1-validation-submit",
            selected_results_group="v1",
        )

    def _v1_validation_analyze(self, runtime: WorkflowRuntime) -> StageOutcome:
        manifest = Path(runtime.path("inputs", "kafka-v1", "validation", "validation_manifest.csv"))
        if runtime.dry_run or not manifest.is_file():
            return runtime.deferred("repeated-validation analysis requires 50 completed case results")
        output = Path(runtime.path("analysis", "v1", "validation"))
        runtime.run_command(
            [
                "python3", "-B",
                runtime.repository_path("scripts", "analyze_kafka_reproducible_campaign.py"),
                "--stage", "validation",
                "--manifest", str(manifest),
                "--results-root", self._selected_results_root(runtime, "v1", self._v1_results_root(runtime)),
                "--output-dir", str(output),
            ]
        )
        rows = _read_json_list(output / "validation_summary_by_original.json")
        winner = str(rows[0].get("original_config_id", "")) if rows else ""
        return StageOutcome(
            artifacts=[str(path) for path in output.iterdir() if path.is_file()],
            input_files=[str(manifest)],
            selected_configs=[winner] if winner else [],
            details={"winner": winner, "summary": str(output / "validation_summary_by_original.json")},
        )

    def _v1_finalize(self, runtime: WorkflowRuntime) -> StageOutcome:
        validation_dir = Path(runtime.path("analysis", "v1", "validation"))
        if runtime.dry_run or not (validation_dir / "validation_summary_by_original.json").is_file():
            return runtime.deferred("V1 machine-result finalization awaits repeated-validation analysis")
        output = Path(runtime.path("artifacts", "v1"))
        command = [
            "python3", "-B",
            runtime.repository_path("scripts", "finalize_reproducible_results.py"),
            "--backend", "kafka",
            "--phase1-dir", runtime.path("analysis", "v1", "phase1"),
            "--validation-dir", str(validation_dir),
            "--phase1-manifest", runtime.path("inputs", "kafka-v1", "phase1", "sweep_manifest.csv"),
            "--validation-manifest", runtime.path("inputs", "kafka-v1", "validation", "validation_manifest.csv"),
            "--output-dir", str(output),
        ]
        runtime.run_command(command)
        return StageOutcome(
            artifacts=[str(path) for path in output.rglob("*") if path.is_file()],
            details={"machine_results_dir": str(output)},
        )

    def _v2_initialize(self, runtime: WorkflowRuntime) -> StageOutcome:
        root = Path(runtime.path("inputs", "kafka-v2"))
        runtime.run_command(
            [
                "python3", "-B",
                runtime.repository_path("scripts", "generate_broker_tuning_v2.py"),
                "--root", str(root), "initialize",
            ]
        )
        verify_sha256_file(root / "SHA256SUMS")
        pilot = root / "campaigns" / "instrumentation_pilot.csv"
        calibration = root / "campaigns" / "rate_calibration.csv"
        self._validate_v2_manifest(runtime, pilot, 6, allow_latency_disabled=True)
        self._validate_v2_manifest(runtime, calibration, 3)
        return StageOutcome(
            artifacts=[str(root / "campaign_blueprint.json"), str(root / "SHA256SUMS")],
            input_files=[
                str(pilot),
                str(calibration),
                *(str(path) for path in manifest_config_paths(pilot, Path(runtime.repository_path()))),
                *(str(path) for path in manifest_config_paths(calibration, Path(runtime.repository_path()))),
            ],
            details={"root": str(root), "pilot_cases": 6, "calibration_cases": 3},
        )

    def _v2_pilot_submit(self, runtime: WorkflowRuntime) -> StageOutcome:
        return self._submit_v2(runtime, "instrumentation_pilot", 1)

    def _v2_pilot_wait(self, runtime: WorkflowRuntime) -> StageOutcome:
        return runtime.wait_for_stage_jobs("v2-pilot-submit")

    def _v2_pilot_verify(self, runtime: WorkflowRuntime) -> StageOutcome:
        return self._verify_v2(runtime, "instrumentation_pilot", 6)

    def _v2_pilot_analyze(self, runtime: WorkflowRuntime) -> StageOutcome:
        if runtime.dry_run:
            return runtime.deferred("instrumentation gate requires six completed pilot case results")
        output = Path(runtime.path("analysis", "v2", "instrumentation-pilot"))
        self._analyze_v2(runtime, output, stages=["instrumentation_pilot"])
        decisions_path = output / "kafka_broker_tuning_v2_decisions.json"
        decision = _dict(_read_json(decisions_path).get("instrumentation"))
        if not decision.get("sampling_decision_valid"):
            raise WorkflowError("Kafka V2 instrumentation sampling decision is invalid")
        if not decision.get("throughput_overhead_within_threshold"):
            raise WorkflowError(
                "deterministic 1-in-10 latency overhead exceeds 3%; no predefined fallback remains"
            )
        return StageOutcome(
            artifacts=[str(path) for path in output.iterdir() if path.is_file()],
            details=decision,
        )

    def _v2_calibration_submit(self, runtime: WorkflowRuntime) -> StageOutcome:
        return self._submit_v2(runtime, "rate_calibration", 1)

    def _v2_calibration_wait(self, runtime: WorkflowRuntime) -> StageOutcome:
        return runtime.wait_for_stage_jobs("v2-calibration-submit")

    def _v2_calibration_verify(self, runtime: WorkflowRuntime) -> StageOutcome:
        return self._verify_v2(runtime, "rate_calibration", 3)

    def _v2_calibration_analyze(self, runtime: WorkflowRuntime) -> StageOutcome:
        if runtime.dry_run:
            return runtime.deferred("rate analysis requires three completed calibration case results")
        output = Path(runtime.path("analysis", "v2", "rate-calibration"))
        self._analyze_v2(runtime, output, stages=["rate_calibration"])
        return StageOutcome(artifacts=[str(path) for path in output.iterdir() if path.is_file()])

    def _v2_screening_generate(self, runtime: WorkflowRuntime) -> StageOutcome:
        shortlist = Path(runtime.path("analysis", "v1", "phase1", "validation_shortlist.json"))
        if runtime.dry_run or not shortlist.is_file():
            return runtime.deferred("profile screening awaits Phase 1 and calibration results")
        root = Path(runtime.path("inputs", "kafka-v2"))
        runtime.run_command(
            [
                "python3", "-B",
                runtime.repository_path("scripts", "generate_broker_tuning_v2.py"),
                "--root", str(root),
                "prepare-screening",
                "--calibration-results", self._v2_results_root(runtime),
                "--phase1-selection", str(shortlist),
            ]
        )
        manifest = root / "campaigns" / "profile_screening.csv"
        validation_path = self._validate_v2_manifest(runtime, manifest, 30)
        metadata = _read_json(root / "campaigns" / "profile_screening_metadata.json")
        return StageOutcome(
            artifacts=[str(validation_path), str(root / "campaigns" / "profile_screening_metadata.json")],
            input_files=[
                str(shortlist),
                str(manifest),
                *(str(path) for path in manifest_config_paths(manifest, Path(runtime.repository_path()))),
            ],
            details={
                "manifest": str(manifest),
                "case_count": 30,
                "job_count": len(_v2_batches(manifest)),
                "target_records_per_sec": metadata["fixed_target_records_per_sec"],
            },
        )

    def _v2_screening_submit(self, runtime: WorkflowRuntime) -> StageOutcome:
        manifest = Path(runtime.path("inputs", "kafka-v2", "campaigns", "profile_screening.csv"))
        if not manifest.is_file():
            return runtime.deferred("profile-screening inputs require calibration results")
        return self._submit_v2(runtime, "profile_screening", 1)

    def _v2_screening_wait(self, runtime: WorkflowRuntime) -> StageOutcome:
        return runtime.wait_for_stage_jobs("v2-screening-submit")

    def _v2_screening_verify(self, runtime: WorkflowRuntime) -> StageOutcome:
        return self._verify_v2(
            runtime,
            "profile_screening",
            30,
            max_clock_invalid_cases=1,
            max_ineligible_cases=1,
        )

    def _v2_screening_analyze(self, runtime: WorkflowRuntime) -> StageOutcome:
        if runtime.dry_run:
            return runtime.deferred("screening candidate selection requires 30 completed case results")
        output = Path(runtime.path("analysis", "v2", "profile-screening"))
        self._analyze_v2(runtime, output, stages=["profile_screening"])
        decisions = _read_json(output / "kafka_broker_tuning_v2_decisions.json")
        selection = _dict(decisions.get("screening_selection"))
        profiles = [str(item) for item in selection.get("confirmation_profiles", [])]
        if len(profiles) != 3 or profiles[0] != "B0" or len(set(profiles)) != 3:
            raise WorkflowError("Kafka V2 screening did not yield B0 plus two candidates")
        return StageOutcome(
            artifacts=[str(path) for path in output.iterdir() if path.is_file()],
            selected_profiles=profiles,
            details={"confirmation_profiles": profiles},
        )

    def _v2_confirmation_generate(self, runtime: WorkflowRuntime) -> StageOutcome:
        if runtime.dry_run:
            return runtime.deferred("confirmation generation awaits screening selection")
        selection = runtime.stage_details("v2-screening-analyze")
        profiles = [str(item) for item in selection.get("confirmation_profiles", [])]
        metadata = _read_json(
            Path(runtime.path("inputs", "kafka-v2", "campaigns", "profile_screening_metadata.json"))
        )
        root = Path(runtime.path("inputs", "kafka-v2"))
        runtime.run_command(
            [
                "python3", "-B",
                runtime.repository_path("scripts", "generate_broker_tuning_v2.py"),
                "--root", str(root), "prepare-confirmation",
                "--profiles", ",".join(profiles),
                "--target-records-per-sec", str(metadata["fixed_target_records_per_sec"]),
            ]
        )
        manifest = root / "campaigns" / "profile_confirmation.csv"
        expected = len(profiles) * 5 * 2
        validation = self._validate_v2_manifest(runtime, manifest, expected)
        return StageOutcome(
            artifacts=[str(validation)],
            input_files=[
                str(manifest),
                *(str(path) for path in manifest_config_paths(manifest, Path(runtime.repository_path()))),
            ],
            selected_profiles=profiles,
            details={"manifest": str(manifest), "case_count": expected, "job_count": len(_v2_batches(manifest))},
        )

    def _v2_confirmation_submit(self, runtime: WorkflowRuntime) -> StageOutcome:
        manifest = Path(runtime.path("inputs", "kafka-v2", "campaigns", "profile_confirmation.csv"))
        if not manifest.is_file():
            return runtime.deferred("confirmation submission awaits screening selection")
        return self._submit_v2(runtime, "profile_confirmation", len(_v2_batches(manifest)))

    def _v2_confirmation_wait(self, runtime: WorkflowRuntime) -> StageOutcome:
        return runtime.wait_for_stage_jobs("v2-confirmation-submit")

    def _v2_confirmation_verify(self, runtime: WorkflowRuntime) -> StageOutcome:
        manifest = Path(runtime.path("inputs", "kafka-v2", "campaigns", "profile_confirmation.csv"))
        expected = _manifest_count(manifest) if manifest.is_file() else 0
        if not expected:
            return runtime.deferred("confirmation verification awaits generated inputs")
        return self._verify_v2(
            runtime,
            "profile_confirmation",
            expected,
            max_clock_invalid_cases=2,
            max_ineligible_cases=2,
        )

    def _v2_confirmation_analyze(self, runtime: WorkflowRuntime) -> StageOutcome:
        if runtime.dry_run:
            return runtime.deferred("profile freeze requires screening plus confirmation results")
        output = Path(runtime.path("analysis", "v2", "profile-confirmation"))
        self._analyze_v2(runtime, output, stages=None)
        decisions = _read_json(output / "kafka_broker_tuning_v2_decisions.json")
        selection = _dict(decisions.get("profile_selection"))
        winner = str(selection.get("winner", ""))
        if winner not in {"B0", "B1", "B2", "B3", "B4", "B5"}:
            raise WorkflowError("Kafka V2 profile selection is absent or ambiguous")
        return StageOutcome(
            artifacts=[str(path) for path in output.iterdir() if path.is_file()],
            selected_profiles=[winner],
            details={"winner": winner, "selection": selection},
        )

    def _v2_final_generate(self, runtime: WorkflowRuntime) -> StageOutcome:
        if runtime.dry_run:
            return runtime.deferred("final V2 generation awaits a frozen profile")
        winner = str(runtime.stage_details("v2-confirmation-analyze").get("winner", ""))
        shortlist = runtime.path("analysis", "v1", "phase1", "validation_shortlist.json")
        root = Path(runtime.path("inputs", "kafka-v2"))
        runtime.run_command(
            [
                "python3", "-B",
                runtime.repository_path("scripts", "generate_broker_tuning_v2.py"),
                "--root", str(root), "prepare-final-validation",
                "--winner", winner,
                "--confirmation-results", self._v2_results_root(runtime),
                "--workload-shortlist", shortlist,
            ]
        )
        manifest = root / "campaigns" / "final_validation.csv"
        validation = self._validate_v2_manifest(runtime, manifest, 50)
        return StageOutcome(
            artifacts=[
                str(validation),
                str(root / "frozen" / "frozen_broker_profile.json"),
                str(root / "frozen" / "frozen_broker_profile.sha256"),
                str(root / "frozen" / "selection.json"),
            ],
            input_files=[
                str(manifest),
                shortlist,
                *(str(path) for path in manifest_config_paths(manifest, Path(runtime.repository_path()))),
            ],
            selected_profiles=[winner],
            details={"manifest": str(manifest), "case_count": 50, "job_count": len(_v2_batches(manifest)), "winner": winner},
        )

    def _v2_final_submit(self, runtime: WorkflowRuntime) -> StageOutcome:
        manifest = Path(runtime.path("inputs", "kafka-v2", "campaigns", "final_validation.csv"))
        if not manifest.is_file():
            return runtime.deferred("final V2 submission awaits a frozen profile")
        return self._submit_v2(runtime, "final_validation", len(_v2_batches(manifest)))

    def _v2_final_wait(self, runtime: WorkflowRuntime) -> StageOutcome:
        return runtime.wait_for_stage_jobs("v2-final-submit")

    def _v2_final_verify(self, runtime: WorkflowRuntime) -> StageOutcome:
        return self._verify_v2(
            runtime,
            "final_validation",
            50,
            max_clock_invalid_cases=5,
            max_ineligible_cases=5,
            min_eligible_repeats_per_workload=3,
        )

    def _v2_final_analyze(self, runtime: WorkflowRuntime) -> StageOutcome:
        if runtime.dry_run:
            return runtime.deferred("complete V2 machine analysis requires final validation case results")
        manifest = runtime.path("inputs", "kafka-v2", "campaigns", "final_validation.csv")
        validation_analysis = Path(runtime.path("analysis", "v2", "final-validation"))
        runtime.run_command(
            [
                "python3", "-B",
                runtime.repository_path("scripts", "analyze_kafka_reproducible_campaign.py"),
                "--backend", "kafka",
                "--stage", "validation",
                "--manifest", manifest,
                "--results-root", self._selected_results_root(runtime, "v2", self._v2_results_root(runtime)),
                "--output-dir", str(validation_analysis),
            ]
        )
        profile_output = Path(runtime.path("analysis", "v2", "complete-profile"))
        self._analyze_v2(runtime, profile_output, stages=None)
        output = Path(runtime.path("artifacts", "v2"))
        runtime.run_command(
            [
                "python3", "-B",
                runtime.repository_path("scripts", "finalize_reproducible_results.py"),
                "--backend", "kafka",
                "--campaign-phase", "v2",
                "--phase1-dir", runtime.path("analysis", "v1", "phase1"),
                "--validation-dir", str(validation_analysis),
                "--phase1-manifest", runtime.path("inputs", "kafka-v1", "phase1", "sweep_manifest.csv"),
                "--validation-manifest", manifest,
                "--output-dir", str(output),
            ]
        )
        return StageOutcome(
            artifacts=[str(path) for path in output.rglob("*") if path.is_file()]
            + [str(path) for path in profile_output.rglob("*") if path.is_file()],
            details={"machine_results_dir": str(output)},
        )

    def _submit_v1(self, runtime: WorkflowRuntime, stage: str, manifest: Path) -> StageOutcome:
        batch_paths = _batches(manifest.parent / "batches")
        expected_jobs = len(batch_paths)
        ledger = Path(runtime.path("slurm", f"kafka-v1-{stage}_jobs.tsv"))
        runtime.run_command(
            [runtime.repository_path("scripts", "submit_kafka_reproducible_campaign.sh"), str(manifest)],
            environment={
                **runtime.submission_environment(),
                "DRY_RUN": "1" if runtime.dry_run else "0",
                "KAFKA_REPRO_RUN_ID": self._v1_run_id(runtime),
                "KAFKA_REPRO_STAGE": stage,
                "KAFKA_REPRO_LEDGER_DIR": str(ledger.parent),
                "KAFKA_REPRO_LEDGER_PATH": str(ledger),
                "KAFKA_REPRO_INDEPENDENT_BATCHES": "1" if stage == "phase1" else "0",
                "BENCHMARK_REPORT_MODE": "machine",
                "SKIP_MONITORING_GRAPHS": "1",
            },
        )
        outcome = runtime.outcome_from_submission_ledger(
            ledger_path=str(ledger),
            expected_jobs=expected_jobs,
            result_paths=[self._v1_results_root(runtime)],
            input_files=[str(manifest), *(str(path) for path in batch_paths)],
        )
        outcome.details["submission_mode"] = (
            "independent" if stage == "phase1" else "sequential"
        )
        return outcome

    def _submit_v2(self, runtime: WorkflowRuntime, manifest_stem: str, expected_jobs: int) -> StageOutcome:
        root = Path(runtime.path("inputs", "kafka-v2"))
        manifest = root / "campaigns" / f"{manifest_stem}.csv"
        if not manifest.is_file():
            return runtime.deferred(f"V2 manifest is not generated yet: {manifest_stem}")
        ledger = Path(runtime.path("slurm", f"kafka-v2-{manifest_stem}_jobs.tsv"))
        runtime.run_command(
            [runtime.repository_path("scripts", "submit_broker_tuning_v2_campaign.sh"), str(manifest)],
            environment={
                **runtime.submission_environment(),
                "DRY_RUN": "1" if runtime.dry_run else "0",
                "V2_RUN_ID": self._v2_run_id(runtime),
                "V2_LEDGER_DIR": str(ledger.parent),
                "V2_LEDGER_PATH": str(ledger),
                "BENCHMARK_REPORT_MODE": "machine",
                "SKIP_MONITORING_GRAPHS": "1",
            },
        )
        return runtime.outcome_from_submission_ledger(
            ledger_path=str(ledger),
            expected_jobs=expected_jobs,
            result_paths=[self._v2_results_root(runtime)],
            input_files=[str(manifest), *(str(path) for path in _v2_batches(manifest))],
        )

    def _verify_v2(
        self,
        runtime: WorkflowRuntime,
        stem: str,
        count: int,
        *,
        max_clock_invalid_cases: int = 0,
        max_ineligible_cases: int = 0,
        min_eligible_repeats_per_workload: int | None = None,
    ) -> StageOutcome:
        manifest = Path(runtime.path("inputs", "kafka-v2", "campaigns", f"{stem}.csv"))
        if not manifest.is_file():
            return runtime.deferred(f"V2 verification awaits {stem} inputs")
        return runtime.verify_results(
            manifest_path=str(manifest),
            results_roots=[self._v2_results_root(runtime)],
            expected_case_count=count,
            allow_latency_disabled=stem == "instrumentation_pilot",
            max_clock_invalid_cases=max_clock_invalid_cases,
            max_ineligible_cases=max_ineligible_cases,
            min_eligible_repeats_per_workload=min_eligible_repeats_per_workload,
            repair_submission_stage={
                "instrumentation_pilot": "v2-pilot-submit",
                "rate_calibration": "v2-calibration-submit",
                "profile_screening": "v2-screening-submit",
                "profile_confirmation": "v2-confirmation-submit",
                "final_validation": "v2-final-submit",
            }[stem],
            selected_results_group="v2",
        )

    def _analyze_v2(
        self,
        runtime: WorkflowRuntime,
        output: Path,
        *,
        stages: list[str] | None,
    ) -> None:
        command = [
            "python3", "-B",
            runtime.repository_path("scripts", "analyze_broker_tuning_v2.py"),
            "--results-root", self._selected_results_root(runtime, "v2", self._v2_results_root(runtime)),
            "--output-dir", str(output),
            "--machine-only",
        ]
        for stage in stages or []:
            command.extend(["--stage", stage])
        runtime.run_command(command)

    def _validate_v2_manifest(
        self,
        runtime: WorkflowRuntime,
        manifest: Path,
        count: int,
        *,
        allow_latency_disabled: bool = False,
    ) -> Path:
        validation = validate_campaign_manifest(
            manifest,
            project_root=Path(runtime.repository_path()),
            backend_id="kafka",
            expected_case_count=count,
            allow_latency_disabled=allow_latency_disabled,
        )
        path = manifest.with_suffix(".validation.json")
        write_validation(path, validation)
        return path

    def _v1_run_id(self, runtime: WorkflowRuntime) -> str:
        return f"{Path(runtime.path()).name}-v1"

    def _v2_run_id(self, runtime: WorkflowRuntime) -> str:
        return f"{Path(runtime.path()).name}-v2"

    def _v1_results_root(self, runtime: WorkflowRuntime) -> str:
        return runtime.repository_path("results", "sweeps", "kafka_v1_reproducible", self._v1_run_id(runtime))

    def _v2_results_root(self, runtime: WorkflowRuntime) -> str:
        return runtime.repository_path("results", "tuning_v2", self._v2_run_id(runtime))

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


def _v2_batches(manifest: Path) -> list[Path]:
    return sorted((manifest.parent / "batches" / manifest.stem).glob("batch_*.csv"))


def _manifest_count(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise WorkflowError(f"expected JSON object: {path}")
    return value


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


_KAFKA_V2_SUBMISSION_STEMS = {
    "v2-pilot-submit": "instrumentation_pilot",
    "v2-calibration-submit": "rate_calibration",
    "v2-screening-submit": "profile_screening",
    "v2-confirmation-submit": "profile_confirmation",
    "v2-final-submit": "final_validation",
}


def _read_json_list(path: Path) -> list[dict[str, Any]]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise WorkflowError(f"expected JSON array of objects: {path}")
    return value


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}
