from __future__ import annotations

import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.benchmark.workflow.base import (
    COMMON_MEASUREMENT_CONTRACT,
    BackendWorkflow,
    StageOutcome,
    StageSpec,
    WorkflowError,
    WorkflowRuntime,
)
from src.benchmark.workflow.engine import (
    MACHINE_OUTPUT_CONTRACT,
    WorkflowEngine,
    _clock_only_latency_ineligibility,
    _common_result_failures,
)
from src.benchmark.workflow.registry import get_workflow, list_workflows
from src.benchmark.workflow.validation import verify_sha256_file
from src.benchmark.workflow.recovery import (
    write_incomplete_case_manifests,
    write_selected_case_manifests,
)
from src.benchmark.core.result_schema import (
    common_measurement_lifecycle_failures,
    enrich_result_schema,
)
from src.benchmark.backends import get_backend
from src.benchmark.backends.pulsar.workflow import _snapshot_immutable_input

import scripts.analyze_kafka_reproducible_campaign as common_analyzer
import scripts.analyze_pulsar_phase1 as pulsar_phase1_analyzer
import scripts.analyze_pulsar_phase2_screening as pulsar_screening_analyzer
import scripts.select_slurm_partition as partition_selector
from scripts.reproducible_benchmark_workflow import parse_args as workflow_args


class _CheckpointWorkflow(BackendWorkflow):
    backend_id = "kafka"

    def stages(self) -> tuple[StageSpec, ...]:
        return (StageSpec("checkpoint", "setup", "write one immutable input"),)

    def required_files(self) -> tuple[str, ...]:
        return ()

    def execute(self, stage: StageSpec, runtime: WorkflowRuntime) -> StageOutcome:
        assert stage.name == "checkpoint"
        input_path = Path(runtime.path("inputs", "immutable.txt"))
        input_path.write_text("original\n", encoding="utf-8")
        artifact_path = Path(runtime.path("artifacts", "checkpoint.txt"))
        artifact_path.write_text("complete\n", encoding="utf-8")
        return StageOutcome(
            input_files=[str(input_path)], artifacts=[str(artifact_path)]
        )


class _ImplementationWorkflow(_CheckpointWorkflow):
    def required_files(self) -> tuple[str, ...]:
        return ("scripts/tool.py", "configs/immutable.json")


class ReproducibleWorkflowTests(unittest.TestCase):
    def test_kafka_phase1_has_separate_clock_and_non_clock_budgets(self) -> None:
        workflow = get_workflow("kafka")
        runtime = Mock()
        runtime.path.return_value = "/tmp/sweep_manifest.csv"
        runtime.repository_path.return_value = "/tmp/results"
        clock_ids = [f"cfg_{index:03d}" for index in range(1, 7)]
        runtime.verify_results.return_value = StageOutcome(
            details={
                "ineligible_case_ids": [*clock_ids, "cfg_103"],
                "tolerated_clock_invalid_case_ids": clock_ids,
            }
        )

        outcome = workflow._v1_phase1_verify(runtime)
        self.assertEqual(
            outcome.details["tolerated_non_clock_ineligible_case_ids"],
            ["cfg_103"],
        )
        self.assertEqual(outcome.details["max_non_clock_ineligible_cases"], 1)
        self.assertEqual(
            runtime.verify_results.call_args.kwargs["max_ineligible_cases"],
            7,
        )

        runtime.verify_results.return_value = StageOutcome(
            details={
                "ineligible_case_ids": [*clock_ids, "cfg_103", "cfg_104"],
                "tolerated_clock_invalid_case_ids": clock_ids,
            }
        )
        with self.assertRaisesRegex(WorkflowError, "more than one non-clock"):
            workflow._v1_phase1_verify(runtime)

    def test_pulsar_v2_profile_snapshot_is_idempotent_and_rejects_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "canonical" / "BASELINE_H16_D32.json"
            destination = root / "run" / "profiles" / source.name
            source.parent.mkdir(parents=True)
            source.write_text('{"profile_id": "BASELINE_H16_D32"}\n', encoding="utf-8")

            created, digest = _snapshot_immutable_input(source, destination)
            self.assertTrue(created)
            self.assertEqual(destination.read_bytes(), source.read_bytes())
            self.assertEqual(digest, _file_digest(source))

            created_again, repeated_digest = _snapshot_immutable_input(
                source,
                destination,
            )
            self.assertFalse(created_again)
            self.assertEqual(repeated_digest, digest)

            destination.write_text('{"profile_id": "drifted"}\n', encoding="utf-8")
            with self.assertRaisesRegex(WorkflowError, "differs from its canonical"):
                _snapshot_immutable_input(source, destination)
            self.assertIn("drifted", destination.read_text(encoding="utf-8"))

    def test_kafka_batch_submission_exports_prepared_runtime_paths(self) -> None:
        environment = {
            "DRY_RUN": "1",
            "RUN_PREFLIGHT": "0",
            "SOURCE_HPC_ENV_FILE": "0",
            "KAFKA_HOME": "/prepared/kafka",
            "PROMETHEUS_HOME": "/prepared/prometheus",
            "NODE_EXPORTER_HOME": "/prepared/node-exporter",
            "KAFKA_EXPORTER_HOME": "/prepared/kafka-exporter",
            "JMX_EXPORTER_JAR": "/prepared/jmx-agent.jar",
            "JMX_EXPORTER_CONFIG": "/prepared/jmx.yml",
        }
        completed = subprocess.run(
            [
                str(
                    PROJECT_ROOT
                    / "scripts"
                    / "submit_simultaneous_budgeted_batch.sh"
                ),
                str(
                    PROJECT_ROOT
                    / "configs"
                    / "campaigns"
                    / "kafka"
                    / "v1_reproducible"
                    / "phase1"
                    / "batches"
                    / "batch_001.csv"
                ),
            ],
            cwd=PROJECT_ROOT,
            env={**os.environ, **environment},
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=True,
        )
        for name, value in environment.items():
            if name in {"DRY_RUN", "RUN_PREFLIGHT"}:
                continue
            self.assertIn(f"{name}={value}", completed.stdout)

    def test_one_command_defaults_to_full_run_and_supports_v1_only(self) -> None:
        complete = workflow_args(["kafka"])
        self.assertTrue(complete.run)
        self.assertFalse(complete.dry_run)
        self.assertEqual(complete.backend, "kafka")
        self.assertEqual(complete.phase, "all")
        self.assertTrue(complete.retry_failed_batches)
        self.assertEqual(complete.max_case_repair_attempts, 2)

        v1_only = workflow_args(
            ["--backend", "pulsar", "--dry-run", "--phase", "v1"]
        )
        self.assertFalse(v1_only.run)
        self.assertTrue(v1_only.dry_run)
        self.assertEqual(v1_only.backend, "pulsar")
        self.assertEqual(v1_only.phase, "v1")

    def test_clock_only_latency_failure_is_an_explicit_tolerable_exclusion(self) -> None:
        report = _portable_result(clock_invalid=True)
        self.assertTrue(_clock_only_latency_ineligibility(report))
        self.assertIn(
            "latency validation failed",
            _common_result_failures(report, "kafka"),
        )
        self.assertEqual(
            _common_result_failures(
                report,
                "kafka",
                allow_clock_only_ineligible=True,
            ),
            [],
        )

        incorrect = json.loads(json.dumps(report))
        incorrect["common_metrics"]["records"]["missing"] = 1
        self.assertIn(
            "correctness counter missing is not zero",
            _common_result_failures(
                incorrect,
                "kafka",
                allow_clock_only_ineligible=True,
            ),
        )

        unhealthy = json.loads(json.dumps(report))
        unhealthy["backend_health"]["status"] = "failed"
        self.assertFalse(_clock_only_latency_ineligibility(unhealthy))

    def test_result_gate_tolerates_one_clock_exclusion_but_enforces_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            results = root / "results"
            manifest = root / "manifest.csv"
            _write_csv(
                manifest,
                [
                    {"config_id": "cfg_001", "config_path": "unused-1.json"},
                    {"config_id": "cfg_002", "config_path": "unused-2.json"},
                ],
            )
            for case_id, clock_invalid in (("cfg_001", False), ("cfg_002", True)):
                path = results / case_id / "final_report.json"
                path.parent.mkdir(parents=True, exist_ok=True)
                payload = _portable_result(clock_invalid=clock_invalid)
                payload["case"]["case_id"] = case_id
                payload["config"]["case_id"] = case_id
                _write_json(path, payload)

            engine = WorkflowEngine(
                project_root=PROJECT_ROOT,
                output_root=root / "workflows",
                workflow_id="clock-exclusion-test",
                backend=_CheckpointWorkflow(),
                dry_run=False,
                resume=False,
                requested_phase="all",
            )
            try:
                engine.state["current_stage"] = "verify"
                outcome = engine.verify_results(
                    manifest_path=str(manifest),
                    results_roots=[str(results)],
                    expected_case_count=2,
                    max_clock_invalid_cases=1,
                )
                self.assertEqual(outcome.details["eligible_case_count"], 1)
                self.assertEqual(
                    outcome.details["tolerated_clock_invalid_case_ids"],
                    ["cfg_002"],
                )
                with self.assertRaisesRegex(WorkflowError, "exceed the stage limit"):
                    engine.verify_results(
                        manifest_path=str(manifest),
                        results_roots=[str(results)],
                        expected_case_count=2,
                        max_clock_invalid_cases=0,
                    )
            finally:
                engine._release_run_lock()

    def test_repeated_gate_requires_three_eligible_repeats_per_workload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            results = root / "results"
            manifest = root / "manifest.csv"
            rows = []
            for block in range(1, 6):
                case_id = f"validation-b{block:02d}-cfg001"
                rows.append(
                    {
                        "case_id": case_id,
                        "config_id": case_id,
                        "workload_config_id": "cfg_001",
                        "config_path": f"unused-{block}.json",
                    }
                )
                path = results / case_id / "final_report.json"
                path.parent.mkdir(parents=True, exist_ok=True)
                payload = _portable_result(clock_invalid=block > 2)
                payload["case"]["case_id"] = case_id
                payload["config"]["case_id"] = case_id
                _write_json(path, payload)
            _write_csv(manifest, rows)

            engine = WorkflowEngine(
                project_root=PROJECT_ROOT,
                output_root=root / "workflows",
                workflow_id="repeat-evidence-test",
                backend=_CheckpointWorkflow(),
                dry_run=False,
                resume=False,
                requested_phase="all",
            )
            try:
                engine.state["current_stage"] = "verify"
                with self.assertRaisesRegex(WorkflowError, "only 2/5 repeats"):
                    engine.verify_results(
                        manifest_path=str(manifest),
                        results_roots=[str(results)],
                        expected_case_count=5,
                        max_clock_invalid_cases=3,
                        min_eligible_repeats_per_workload=3,
                    )
            finally:
                engine._release_run_lock()

    def test_shared_analyzer_deduplicates_identical_data_report_copy(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            payload = _portable_result(clock_invalid=False)
            top = root / "cases" / "cfg_001" / "final_report.json"
            copy = top.parent / "data" / "final_report.json"
            top.parent.mkdir(parents=True, exist_ok=True)
            copy.parent.mkdir(parents=True, exist_ok=True)
            _write_json(top, payload)
            _write_json(copy, payload)
            reports, duplicates = common_analyzer._discover_reports(
                [root], "kafka"
            )
            self.assertEqual(duplicates, [])
            self.assertEqual(set(reports), {"cfg_001"})
            self.assertEqual(reports["cfg_001"][0], top)

    def test_common_contract_and_backend_workflows_are_registered(self) -> None:
        self.assertEqual(list_workflows(), ("kafka", "pulsar"))
        self.assertEqual(get_workflow("kafka").backend_id, "kafka")
        self.assertEqual(get_workflow("pulsar").backend_id, "pulsar")
        self.assertEqual(
            COMMON_MEASUREMENT_CONTRACT["timing"],
            {"warmup_sec": 15, "measurement_sec": 30, "drain_timeout_sec": 60},
        )
        self.assertEqual(
            COMMON_MEASUREMENT_CONTRACT["latency"]["deterministic_sample_every"],
            10,
        )
        self.assertEqual(
            COMMON_MEASUREMENT_CONTRACT["producer_delivery"]["backlog_denominator"],
            "measurement-period send attempts",
        )
        self.assertEqual(
            COMMON_MEASUREMENT_CONTRACT["consumer_drain"]["target"],
            "callback-confirmed measurement-period deliveries",
        )
        self.assertFalse(
            MACHINE_OUTPUT_CONTRACT["presentation_pipeline"]["required_for_success"]
        )
        self.assertFalse(
            MACHINE_OUTPUT_CONTRACT["presentation_pipeline"]["executed_by_workflow"]
        )
        for backend_id in list_workflows():
            workflow = get_workflow(backend_id)
            self.assertTrue(
                all("report" not in stage.name for stage in workflow.stages())
            )
            self.assertNotIn(
                "scripts/generate_reproducible_report.py", workflow.required_files()
            )

    def test_phase_aliases_bound_full_v1_and_v2_workflows(self) -> None:
        for backend_id in list_workflows():
            workflow = get_workflow(backend_id)
            all_stages = workflow.resolve_phase("all")
            v1_stages = workflow.resolve_phase("v1")
            phase1_stages = workflow.resolve_phase("phase1")
            v2_stages = workflow.resolve_phase("v2")
            self.assertEqual(all_stages[0], "preflight")
            self.assertEqual(all_stages[-1], "v2-final-analyze")
            self.assertEqual(v1_stages[-1], "v1-finalize")
            self.assertEqual(phase1_stages[-1], "v1-phase1-analyze")
            self.assertEqual(v2_stages[0], "preflight")
            self.assertEqual(v2_stages[-1], "v2-final-analyze")
            self.assertTrue(set(v1_stages).issubset(all_stages))
            self.assertTrue(set(v2_stages).issubset(all_stages))

    def test_scheduler_settings_are_recorded_and_immutable_on_resume(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir) / "workflows"
            run_id = "scheduler-settings-test"
            first = WorkflowEngine(
                project_root=PROJECT_ROOT,
                output_root=output_root,
                workflow_id=run_id,
                backend=_CheckpointWorkflow(),
                dry_run=True,
                resume=False,
                requested_phase="all",
                execution_setting_overrides={
                    "account": "benchmark-account",
                    "partition": "exclusive:test",
                    "qos": "2h",
                    "time": "02:00:00",
                    "constraint": "scratch",
                },
            )
            state = first.run()
            self.assertEqual(
                state["execution_settings"]["slurm"],
                {
                    "account": "benchmark-account",
                    "partition": "exclusive:test",
                    "qos": "2h",
                    "time": "02:00:00",
                    "constraint": "scratch",
                },
            )

            resumed = WorkflowEngine(
                project_root=PROJECT_ROOT,
                output_root=output_root,
                workflow_id=run_id,
                backend=_CheckpointWorkflow(),
                dry_run=True,
                resume=True,
                requested_phase="all",
            )
            self.assertEqual(
                resumed.submission_environment()["SLURM_ACCOUNT"],
                "benchmark-account",
            )
            resumed.close()

            with self.assertRaisesRegex(
                WorkflowError, "execution settings are immutable on resume"
            ):
                WorkflowEngine(
                    project_root=PROJECT_ROOT,
                    output_root=output_root,
                    workflow_id=run_id,
                    backend=_CheckpointWorkflow(),
                    dry_run=True,
                    resume=True,
                    requested_phase="all",
                    execution_setting_overrides={"time": "01:00:00"},
                )

    def test_partition_selector_prefers_live_capacity_then_shorter_queue(self) -> None:
        candidates = ("standard96s", "medium96s")
        common = "ssd,sapphirerapids,rzg"
        statuses = partition_selector.parse_sinfo(
            "\n".join(
                [
                    f"standard96s|up|2-00:00:00|4|idle|192|515000|{common}",
                    f"standard96s|up|2-00:00:00|196|allocated|192|515000|{common}",
                    f"medium96s|up|2-00:00:00|4|idle|192|257000|{common}",
                    f"medium96s|up|2-00:00:00|343|allocated|192|257000|{common}",
                    f"medium96s|up|2-00:00:00|1|draining|192|257000|{common}",
                ]
            ),
            candidates=candidates,
            pending_jobs={"standard96s": 12, "medium96s": 30},
            minimum_cpus=192,
            minimum_memory_mb=240000,
            required_features=frozenset({"sapphirerapids"}),
        )
        selected = partition_selector.select_partition(statuses, required_nodes=4)
        self.assertEqual(selected.name, "standard96s")
        medium = next(status for status in statuses if status.name == "medium96s")
        self.assertEqual(medium.schedulable_nodes, 347)

        more_idle = [
            status
            if status.name == "standard96s"
            else partition_selector.PartitionStatus(
                **{
                    **status.__dict__,
                    "idle_nodes": 8,
                }
            )
            for status in statuses
        ]
        self.assertEqual(
            partition_selector.select_partition(more_idle, required_nodes=4).name,
            "medium96s",
        )

    def test_absent_scheduler_setting_is_removed_from_submission_environment(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {"SLURM_CONSTRAINT": ""}, clear=False):
                engine = WorkflowEngine(
                    project_root=PROJECT_ROOT,
                    output_root=Path(tmpdir) / "workflows",
                    workflow_id="unset-scheduler-setting",
                    backend=_CheckpointWorkflow(),
                    dry_run=True,
                    resume=False,
                    requested_phase="all",
                    execution_setting_overrides={
                        "account": "benchmark-account",
                        "partition": "exclusive:test",
                        "qos": "2h",
                        "time": "02:00:00",
                    },
                )
                environment = engine.submission_environment()
                self.assertIsNone(environment["SLURM_CONSTRAINT"])
                engine.run_command(
                    [
                        sys.executable,
                        "-c",
                        (
                            "import os, sys; "
                            "sys.exit(1 if 'SLURM_CONSTRAINT' in os.environ else 0)"
                        ),
                    ],
                    environment=environment,
                )
                engine.close()

    def test_invalid_scheduler_setting_is_rejected_before_state_creation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(WorkflowError, "Slurm wall time"):
                WorkflowEngine(
                    project_root=PROJECT_ROOT,
                    output_root=Path(tmpdir) / "workflows",
                    workflow_id="bad-scheduler-setting",
                    backend=_CheckpointWorkflow(),
                    dry_run=True,
                    resume=False,
                    requested_phase="all",
                    execution_setting_overrides={"time": "two hours"},
                )

    def test_recovery_manifest_contains_only_cases_without_completed_reports(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source.csv"
            _write_csv(
                source,
                [
                    {"config_id": "cfg_001", "config_path": "one.json"},
                    {"config_id": "cfg_002", "config_path": "two.json"},
                    {"config_id": "cfg_003", "config_path": "three.json"},
                ],
            )
            completed_path = root / "results" / "cfg_001" / "final_report.json"
            completed_path.parent.mkdir(parents=True)
            completed = _portable_result(clock_invalid=False)
            completed["case"]["case_id"] = "cfg_001"
            completed["config"]["case_id"] = "cfg_001"
            _write_json(completed_path, completed)

            incomplete_path = root / "results" / "cfg_002" / "final_report.json"
            incomplete_path.parent.mkdir(parents=True)
            incomplete = _portable_result(clock_invalid=False)
            incomplete["case"].update({"case_id": "cfg_002", "status": "failed"})
            incomplete["config"]["case_id"] = "cfg_002"
            _write_json(incomplete_path, incomplete)

            plan = write_incomplete_case_manifests(
                source_manifests=[source],
                results_roots=[root / "results"],
                output_dir=root / "repair",
                backend_id="kafka",
            )
            self.assertEqual(plan.source_case_count, 3)
            self.assertEqual(plan.completed_case_count, 1)
            self.assertEqual(plan.retry_case_ids, ("cfg_002", "cfg_003"))
            rows = _read_csv(plan.manifest_paths[0])
            self.assertEqual(
                [row["config_id"] for row in rows], ["cfg_002", "cfg_003"]
            )

    def test_selected_recovery_manifest_contains_exact_requested_cases(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source.csv"
            _write_csv(
                source,
                [
                    {"config_id": "cfg_001", "config_path": "one.json"},
                    {"config_id": "cfg_002", "config_path": "two.json"},
                    {"config_id": "cfg_003", "config_path": "three.json"},
                ],
            )
            plan = write_selected_case_manifests(
                source_manifests=[source],
                selected_case_ids=["cfg_003", "cfg_001"],
                output_dir=root / "repair",
            )
            self.assertEqual(plan.retry_case_ids, ("cfg_003", "cfg_001"))
            rows = _read_csv(plan.manifest_paths[0])
            self.assertEqual(
                [row["config_id"] for row in rows], ["cfg_001", "cfg_003"]
            )

    def test_result_gate_prefers_valid_repair_without_changing_original(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            manifest = root / "manifest.csv"
            _write_csv(
                manifest,
                [{"config_id": "cfg_001", "config_path": "unused.json"}],
            )
            original = _portable_result(clock_invalid=True)
            repaired = _portable_result(clock_invalid=False)
            repaired["common_metrics"]["throughput"]["balanced_mib_per_sec"] = 98.0
            repaired["common_metrics"]["throughput"]["consumer_mib_per_sec"] = 98.0
            original_path = root / "results" / "original" / "cfg_001" / "final_report.json"
            repair_path = root / "results" / "repairs" / "repair-01" / "cfg_001" / "final_report.json"
            original_path.parent.mkdir(parents=True)
            repair_path.parent.mkdir(parents=True)
            _write_json(original_path, original)
            _write_json(repair_path, repaired)
            original_digest = _file_digest(original_path)

            engine = WorkflowEngine(
                project_root=PROJECT_ROOT,
                output_root=root / "workflows",
                workflow_id="valid-repair-selection",
                backend=_CheckpointWorkflow(),
                dry_run=False,
                resume=False,
                requested_phase="all",
            )
            try:
                engine.state["current_stage"] = "verify"
                outcome = engine.verify_results(
                    manifest_path=str(manifest),
                    results_roots=[str(root / "results")],
                    expected_case_count=1,
                    max_clock_invalid_cases=1,
                    max_ineligible_cases=1,
                    selected_results_group="selection-test",
                )
                selected = (
                    Path(outcome.details["selected_results_root"])
                    / "cfg_001"
                    / "final_report.json"
                )
                self.assertEqual(
                    json.loads(selected.read_text(encoding="utf-8")),
                    repaired,
                )
                self.assertEqual(_file_digest(original_path), original_digest)
                self.assertEqual(outcome.details["eligible_case_count"], 1)
            finally:
                engine._release_run_lock()

    def test_result_gate_runs_one_isolated_repair_and_selects_it(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            manifest = root / "manifest.csv"
            _write_csv(
                manifest,
                [{"config_id": "cfg_001", "config_path": "unused.json"}],
            )
            original = _portable_result(clock_invalid=True)
            original_path = root / "results" / "original" / "cfg_001" / "final_report.json"
            original_path.parent.mkdir(parents=True)
            _write_json(original_path, original)
            original_digest = _file_digest(original_path)

            engine = WorkflowEngine(
                project_root=PROJECT_ROOT,
                output_root=root / "workflows",
                workflow_id="automatic-isolated-repair",
                backend=_CheckpointWorkflow(),
                dry_run=False,
                resume=False,
                requested_phase="all",
                retry_failed_batches=True,
            )
            try:
                engine.state["current_stage"] = "verify"
                repaired = _portable_result(clock_invalid=False)
                repaired["common_metrics"]["throughput"][
                    "balanced_mib_per_sec"
                ] = 98.0
                repaired["common_metrics"]["throughput"][
                    "consumer_mib_per_sec"
                ] = 98.0
                repair_path = (
                    root
                    / "results"
                    / "repairs"
                    / "repair-01"
                    / "cfg_001"
                    / "final_report.json"
                )

                def submit_repair(
                    submission_stage: str,
                    case_ids: tuple[str, ...],
                    runtime: WorkflowEngine,
                ) -> StageOutcome:
                    self.assertEqual(submission_stage, "submit")
                    self.assertEqual(case_ids, ("cfg_001",))
                    self.assertIs(runtime, engine)
                    repair_path.parent.mkdir(parents=True)
                    _write_json(repair_path, repaired)
                    return StageOutcome(
                        artifacts=[str(root / "repair-ledger.csv")],
                        input_files=[str(manifest)],
                        job_ids=["12345"],
                    )

                with patch.object(
                    engine.backend,
                    "repair_invalid_cases",
                    side_effect=submit_repair,
                ) as repair_mock, patch.object(
                    engine,
                    "_wait_for_job_ids",
                    return_value={"12345": "COMPLETED"},
                ):
                    outcome = engine.verify_results(
                        manifest_path=str(manifest),
                        results_roots=[str(root / "results")],
                        expected_case_count=1,
                        max_clock_invalid_cases=1,
                        max_ineligible_cases=1,
                        repair_submission_stage="submit",
                        selected_results_group="selection-test",
                    )

                repair_mock.assert_called_once()
                self.assertEqual(outcome.details["eligible_case_count"], 1)
                self.assertEqual(outcome.details["ineligible_case_ids"], [])
                self.assertEqual(outcome.details["repair_events"][0]["case_ids"], ["cfg_001"])
                self.assertEqual(_file_digest(original_path), original_digest)
                selected = (
                    Path(outcome.details["selected_results_root"])
                    / "cfg_001"
                    / "final_report.json"
                )
                self.assertEqual(
                    json.loads(selected.read_text(encoding="utf-8")),
                    repaired,
                )
                repair_record = (
                    engine.run_root
                    / "inputs"
                    / "recovery"
                    / "submit"
                    / "attempt_01"
                    / "repair_attempt.json"
                )
                self.assertEqual(
                    json.loads(repair_record.read_text(encoding="utf-8"))[
                        "status"
                    ],
                    "completed",
                )
            finally:
                engine._release_run_lock()

    def test_result_gate_adopts_pending_repair_after_controller_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            manifest = root / "manifest.csv"
            _write_csv(
                manifest,
                [{"config_id": "cfg_001", "config_path": "unused.json"}],
            )
            original_path = root / "results" / "original" / "cfg_001" / "final_report.json"
            original_path.parent.mkdir(parents=True)
            _write_json(original_path, _portable_result(clock_invalid=True))

            engine = WorkflowEngine(
                project_root=PROJECT_ROOT,
                output_root=root / "workflows",
                workflow_id="adopt-pending-repair",
                backend=_CheckpointWorkflow(),
                dry_run=False,
                resume=False,
                requested_phase="all",
                retry_failed_batches=True,
            )
            try:
                engine.state["current_stage"] = "verify"
                record_path = (
                    engine.run_root
                    / "inputs"
                    / "recovery"
                    / "submit"
                    / "attempt_01"
                    / "repair_attempt.json"
                )
                record_path.parent.mkdir(parents=True)
                _write_json(
                    record_path,
                    {
                        "format": "messaging-benchmark.case-repair-attempt.v1",
                        "submission_stage": "submit",
                        "attempt": 1,
                        "case_ids": ["cfg_001"],
                        "job_ids": ["12345"],
                        "submitted_at": "2026-08-15T00:00:00+00:00",
                        "status": "submitted",
                    },
                )
                repair_path = (
                    root
                    / "results"
                    / "repairs"
                    / "repair-01"
                    / "cfg_001"
                    / "final_report.json"
                )

                def finish_existing_repair(*_: object, **__: object) -> dict[str, str]:
                    repair_path.parent.mkdir(parents=True)
                    _write_json(repair_path, _portable_result(clock_invalid=False))
                    return {"12345": "COMPLETED"}

                with patch.object(
                    engine,
                    "_wait_for_job_ids",
                    side_effect=finish_existing_repair,
                ) as wait_mock, patch.object(
                    engine.backend,
                    "repair_invalid_cases",
                ) as repair_mock:
                    outcome = engine.verify_results(
                        manifest_path=str(manifest),
                        results_roots=[str(root / "results")],
                        expected_case_count=1,
                        max_clock_invalid_cases=1,
                        max_ineligible_cases=1,
                        repair_submission_stage="submit",
                        selected_results_group="selection-test",
                    )

                wait_mock.assert_called_once()
                repair_mock.assert_not_called()
                self.assertEqual(outcome.details["eligible_case_count"], 1)
                self.assertEqual(
                    json.loads(record_path.read_text(encoding="utf-8"))["status"],
                    "completed",
                )
            finally:
                engine._release_run_lock()

    def test_broad_screen_does_not_repair_tolerated_clock_only_case(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            manifest = root / "manifest.csv"
            _write_csv(
                manifest,
                [{"config_id": "cfg_001", "config_path": "unused.json"}],
            )
            report_path = root / "results" / "cfg_001" / "final_report.json"
            report_path.parent.mkdir(parents=True)
            _write_json(report_path, _portable_result(clock_invalid=True))

            engine = WorkflowEngine(
                project_root=PROJECT_ROOT,
                output_root=root / "workflows",
                workflow_id="tolerated-clock-only-case",
                backend=_CheckpointWorkflow(),
                dry_run=False,
                resume=False,
                requested_phase="all",
                retry_failed_batches=True,
            )
            try:
                engine.state["current_stage"] = "verify"
                with patch.object(
                    engine.backend,
                    "repair_invalid_cases",
                ) as repair_mock:
                    outcome = engine.verify_results(
                        manifest_path=str(manifest),
                        results_roots=[str(root / "results")],
                        expected_case_count=1,
                        max_clock_invalid_cases=1,
                        max_ineligible_cases=1,
                        repair_clock_invalid_cases=False,
                        repair_submission_stage="submit",
                        selected_results_group="selection-test",
                    )

                repair_mock.assert_not_called()
                self.assertEqual(outcome.details["eligible_case_count"], 0)
                self.assertEqual(
                    outcome.details["tolerated_clock_invalid_case_ids"],
                    ["cfg_001"],
                )
                self.assertFalse(outcome.details["repair_clock_invalid_cases"])
            finally:
                engine._release_run_lock()

    def test_result_gate_excludes_one_non_clock_invalid_case_within_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            manifest = root / "manifest.csv"
            _write_csv(
                manifest,
                [{"config_id": "cfg_001", "config_path": "unused.json"}],
            )
            report = _portable_result(clock_invalid=False)
            report["eligibility"] = {
                "eligible": False,
                "failure_reasons": ["unexplained surplus records observed"],
            }
            report["common_metrics"]["qualification"].update(
                {"eligible": False, "qualified": False}
            )
            report["common_metrics"]["records"]["surplus"] = 1
            report_path = root / "results" / "cfg_001" / "final_report.json"
            report_path.parent.mkdir(parents=True)
            _write_json(report_path, report)

            engine = WorkflowEngine(
                project_root=PROJECT_ROOT,
                output_root=root / "workflows",
                workflow_id="bounded-ineligible-selection",
                backend=_CheckpointWorkflow(),
                dry_run=False,
                resume=False,
                requested_phase="all",
            )
            try:
                engine.state["current_stage"] = "verify"
                outcome = engine.verify_results(
                    manifest_path=str(manifest),
                    results_roots=[str(root / "results")],
                    expected_case_count=1,
                    max_ineligible_cases=1,
                    selected_results_group="ineligible-test",
                )
                self.assertEqual(outcome.details["eligible_case_count"], 0)
                self.assertEqual(outcome.details["ineligible_case_ids"], ["cfg_001"])
            finally:
                engine._release_run_lock()

    def test_completed_repair_report_is_preferred_over_failed_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            failed = _portable_result(clock_invalid=False)
            failed["case"].update({"case_id": "cfg_001", "status": "failed"})
            failed["config"]["case_id"] = "cfg_001"
            completed = _portable_result(clock_invalid=False)
            completed["case"]["case_id"] = "cfg_001"
            completed["config"]["case_id"] = "cfg_001"
            failed_path = root / "original" / "cfg_001" / "final_report.json"
            completed_path = root / "repairs" / "cfg_001" / "final_report.json"
            failed_path.parent.mkdir(parents=True)
            completed_path.parent.mkdir(parents=True)
            _write_json(failed_path, failed)
            _write_json(completed_path, completed)

            engine = WorkflowEngine(
                project_root=PROJECT_ROOT,
                output_root=root / "workflows",
                workflow_id="prefer-repair-report",
                backend=_CheckpointWorkflow(),
                dry_run=False,
                resume=False,
                requested_phase="all",
            )
            try:
                reports = engine._discover_common_reports([str(root)])
                self.assertEqual(reports["cfg_001"]["case"]["status"], "completed")
            finally:
                engine._release_run_lock()

    def test_pulsar_analyzers_prefer_repair_and_reject_completed_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            failed = _portable_result(clock_invalid=False)
            failed["case"].update(
                {"case_id": "pulsar-cfg-001", "status": "failed"}
            )
            failed["config"].update(
                {"backend_id": "pulsar", "case_id": "pulsar-cfg-001"}
            )
            failed["system_under_test"]["backend_id"] = "pulsar"
            completed = json.loads(json.dumps(failed))
            completed["case"]["status"] = "completed"
            failed_path = root / "original" / "final_report.json"
            repair_path = root / "repairs" / "repair-01" / "final_report.json"
            failed_path.parent.mkdir(parents=True)
            repair_path.parent.mkdir(parents=True)
            _write_json(failed_path, failed)
            _write_json(repair_path, completed)

            phase1_reports, phase1_conflicts = (
                pulsar_phase1_analyzer._discover_reports([root])
            )
            screening_reports, screening_conflicts = (
                pulsar_screening_analyzer._discover_reports(root)
            )
            self.assertEqual(
                phase1_reports["pulsar-cfg-001"][1]["case"]["status"],
                "completed",
            )
            self.assertEqual(
                screening_reports["pulsar-cfg-001"][1]["case"]["status"],
                "completed",
            )
            self.assertEqual(phase1_conflicts, [])
            self.assertEqual(screening_conflicts, [])

            conflicting = json.loads(json.dumps(completed))
            conflicting["common_metrics"]["throughput"][
                "balanced_mib_per_sec"
            ] = 123.0
            conflict_path = root / "repairs" / "repair-02" / "final_report.json"
            conflict_path.parent.mkdir(parents=True)
            _write_json(conflict_path, conflicting)
            _, phase1_conflicts = pulsar_phase1_analyzer._discover_reports([root])
            _, screening_conflicts = pulsar_screening_analyzer._discover_reports(root)
            self.assertEqual(phase1_conflicts, ["pulsar-cfg-001"])
            self.assertEqual(screening_conflicts, ["pulsar-cfg-001"])

    def test_reproducible_results_require_coordinated_post_flush_drain(self) -> None:
        report = {
            "config": {
                "scenario": "simultaneous",
                "drain_timeout_sec": 60,
                "extra": {
                    "campaign_metadata": {
                        "measurement_contract_id": (
                            "measurement.messaging.reproducible.v1"
                        ),
                    },
                },
            },
            "aggregated_metrics": {
                "producers": {"messages_delivered": 1_000},
                "consumers": {
                    "consumer_rank_count": 40,
                    "coordinated_drain_rank_count": 40,
                    "first_post_flush_drain_start_time_unix": 101.0,
                    "last_post_flush_drain_end_time_unix": 102.0,
                },
                "record_correctness": {
                    "consumer_received_through_drain_records": 1_000,
                },
            },
            "coordinated_drain": {
                "enabled": True,
                "producer_flush_boundary": "all_producer_ranks_completed_flush",
                "delivered_target_records": 1_000,
                "consumed_records_at_completion": 1_000,
                "configured_post_flush_timeout_sec": 60,
                "completion_reason": "delivered_target_reached",
                "complete": True,
            },
        }
        self.assertEqual(common_measurement_lifecycle_failures(report), [])

        missing = json.loads(json.dumps(report))
        missing.pop("coordinated_drain")
        self.assertIn(
            "coordinated producer-flush/consumer-drain evidence is missing",
            common_measurement_lifecycle_failures(missing),
        )

        timed_out = json.loads(json.dumps(report))
        timed_out["coordinated_drain"].update(
            {
                "completion_reason": "drain_timeout",
                "complete": False,
            }
        )
        failures = common_measurement_lifecycle_failures(timed_out)
        self.assertIn(
            "coordinated drain did not reach the delivered-record target",
            failures,
        )
        self.assertIn("coordinated drain is not complete", failures)

        schema_probe = json.loads(json.dumps(report))
        schema_probe.update(
            {
                "case": {"status": "completed"},
                "latency_validation": {"enabled": False, "valid": False},
                "backend_health": {"status": "healthy"},
            }
        )
        schema_probe["config"].update(
            {
                "backend_id": "kafka",
                "qualification_policy_id": "qualification.application.v1",
            }
        )
        schema_probe["aggregated_metrics"]["producers"].update(
            {
                "messages_attempted": 1_000,
                "messages_enqueued": 1_000,
                "messages_failed": 0,
                "pending_messages_at_flush_start": 0,
                "max_flush_duration_sec": 0.5,
                "throughput_bytes_per_sec": 4_096_000.0,
                "throughput_msgs_per_sec": 1_000.0,
            }
        )
        schema_probe["aggregated_metrics"]["consumers"].update(
            {
                "messages_received": 1_000,
                "throughput_bytes_per_sec": 4_096_000.0,
                "throughput_msgs_per_sec": 1_000.0,
                "latency_histogram": {},
            }
        )
        schema_probe["aggregated_metrics"]["record_correctness"].update(
            {
                "missing_after_drain_records": 0,
                "invalid_envelope_count": 0,
                "duplicate_offset_count": 0,
                "out_of_order_offset_count": 0,
                "unexplained_surplus_records": 0,
            }
        )
        enriched = enrich_result_schema(schema_probe, get_backend("kafka"))
        self.assertTrue(enriched["eligibility"]["eligible"])
        portable_drain = enriched["common_metrics"]["consumer_drain"]
        self.assertTrue(portable_drain["enabled"])
        self.assertEqual(portable_drain["delivered_target_records"], 1_000)

    def test_shared_analyzer_normalizes_backend_manifest_identities(self) -> None:
        pulsar_phase1 = {
            "case_id": "pulsar-cfg-087",
            "config_id": "cfg_087",
        }
        kafka_v2_repeat = {
            "case_id": "final-b01-cfg074-B0",
            "config_id": "final-b01-cfg074-B0",
            "anchor_id": "cfg_074",
        }
        pulsar_repeat = {
            "case_id": "pulsar-v1v-b01-cfg087",
            "config_id": "cfg_087",
            "anchor": "cfg_087",
        }
        self.assertEqual(
            common_analyzer._manifest_case_id(pulsar_phase1, "phase1"),
            "pulsar-cfg-087",
        )
        self.assertEqual(
            common_analyzer._manifest_workload_config_id(kafka_v2_repeat),
            "cfg_074",
        )
        self.assertEqual(
            common_analyzer._manifest_workload_config_id(pulsar_repeat),
            "cfg_087",
        )

    def test_resume_skips_completed_stage_and_rejects_input_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir) / "workflows"
            engine = WorkflowEngine(
                project_root=PROJECT_ROOT,
                output_root=output_root,
                workflow_id="checkpoint-test",
                backend=_CheckpointWorkflow(),
                dry_run=True,
                resume=False,
                requested_phase="all",
            )
            first = engine.run()
            self.assertEqual(
                first["stages"]["checkpoint"]["status"], "dry_run_completed"
            )
            self.assertEqual(first["reporting"]["status"], "not_requested")
            self.assertFalse(first["reporting"]["required_for_success"])
            workflow_rows = _read_csv(
                output_root
                / "kafka"
                / "checkpoint-test"
                / "artifacts"
                / "workflow_stages.csv"
            )
            self.assertEqual(len(workflow_rows), 1)
            self.assertEqual(workflow_rows[0]["stage"], "checkpoint")
            artifact_root = (
                output_root / "kafka" / "checkpoint-test" / "artifacts"
            )
            verify_sha256_file(artifact_root / "SHA256SUMS")
            self.assertTrue((artifact_root / "workflow_state.json").is_file())

            resumed = WorkflowEngine(
                project_root=PROJECT_ROOT,
                output_root=output_root,
                workflow_id="checkpoint-test",
                backend=_CheckpointWorkflow(),
                dry_run=True,
                resume=True,
                requested_phase="all",
            )
            second = resumed.run()
            self.assertEqual(second["stages"]["checkpoint"]["attempt_count"], 1)

            input_path = (
                output_root / "kafka" / "checkpoint-test" / "inputs" / "immutable.txt"
            )
            input_path.write_text("changed\n", encoding="utf-8")
            with self.assertRaisesRegex(WorkflowError, "checksum drift"):
                WorkflowEngine(
                    project_root=PROJECT_ROOT,
                    output_root=output_root,
                    workflow_id="checkpoint-test",
                    backend=_CheckpointWorkflow(),
                    dry_run=True,
                    resume=True,
                    requested_phase="all",
                )

    def test_resume_can_audit_declared_source_update_but_not_config_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project = Path(tmpdir) / "project"
            output_root = project / "workflow-output"
            tool = project / "scripts" / "tool.py"
            config = project / "configs" / "immutable.json"
            tool.parent.mkdir(parents=True)
            config.parent.mkdir(parents=True)
            tool.write_text("VERSION = 1\n", encoding="utf-8")
            config.write_text("{}\n", encoding="utf-8")
            run_id = "implementation-update-test"
            WorkflowEngine(
                project_root=project,
                output_root=output_root,
                workflow_id=run_id,
                backend=_ImplementationWorkflow(),
                dry_run=True,
                resume=False,
                requested_phase="all",
            ).run()

            tool.write_text("VERSION = 2\n", encoding="utf-8")
            migrated = WorkflowEngine(
                project_root=project,
                output_root=output_root,
                workflow_id=run_id,
                backend=_ImplementationWorkflow(),
                dry_run=True,
                resume=True,
                requested_phase="all",
                accepted_implementation_updates=["scripts/tool.py"],
                implementation_update_reason="verified analyzer correction",
            )
            state = migrated.close()
            event = state["implementation_updates"][-1]
            self.assertEqual(event["files"][0]["change_type"], "updated")
            self.assertNotEqual(
                event["files"][0]["previous_sha256"],
                event["files"][0]["current_sha256"],
            )
            backup = project / event["state_backup"]
            self.assertTrue(backup.is_file())

            config.write_text('{"changed": true}\n', encoding="utf-8")
            with self.assertRaisesRegex(WorkflowError, "only declared Python"):
                WorkflowEngine(
                    project_root=project,
                    output_root=output_root,
                    workflow_id=run_id,
                    backend=_ImplementationWorkflow(),
                    dry_run=True,
                    resume=True,
                    requested_phase="all",
                    accepted_implementation_updates=["configs/immutable.json"],
                    implementation_update_reason="must be rejected",
                )

    def test_concurrent_workflow_owner_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir) / "workflows"
            run_id = "exclusive-owner-test"
            first = WorkflowEngine(
                project_root=PROJECT_ROOT,
                output_root=output_root,
                workflow_id=run_id,
                backend=_CheckpointWorkflow(),
                dry_run=True,
                resume=False,
                requested_phase="all",
            )
            with self.assertRaisesRegex(WorkflowError, "already active"):
                WorkflowEngine(
                    project_root=PROJECT_ROOT,
                    output_root=output_root,
                    workflow_id=run_id,
                    backend=_CheckpointWorkflow(),
                    dry_run=True,
                    resume=True,
                    requested_phase="all",
                )

            first.run()
            resumed = WorkflowEngine(
                project_root=PROJECT_ROOT,
                output_root=output_root,
                workflow_id=run_id,
                backend=_CheckpointWorkflow(),
                dry_run=True,
                resume=True,
                requested_phase="all",
            )
            resumed.run()

    def test_resume_rejects_a_missing_completed_stage_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir) / "workflows"
            run_id = "missing-artifact-test"
            WorkflowEngine(
                project_root=PROJECT_ROOT,
                output_root=output_root,
                workflow_id=run_id,
                backend=_CheckpointWorkflow(),
                dry_run=True,
                resume=False,
                requested_phase="all",
            ).run()
            artifact = output_root / "kafka" / run_id / "artifacts" / "checkpoint.txt"
            artifact.unlink()
            with self.assertRaisesRegex(WorkflowError, "missing artifact"):
                WorkflowEngine(
                    project_root=PROJECT_ROOT,
                    output_root=output_root,
                    workflow_id=run_id,
                    backend=_CheckpointWorkflow(),
                    dry_run=True,
                    resume=True,
                    requested_phase="all",
                ).run()

    def test_unresolved_submission_intent_stops_before_duplicate_submit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir) / "workflows"
            engine = WorkflowEngine(
                project_root=PROJECT_ROOT,
                output_root=output_root,
                workflow_id="submission-intent-test",
                backend=_CheckpointWorkflow(),
                dry_run=True,
                resume=False,
                requested_phase="all",
            )
            self.addCleanup(engine._release_run_lock)
            manifest = Path(engine.path("inputs", "batch_01.csv"))
            manifest.write_text("case_id,config_path\ncase-1,config.json\n", encoding="utf-8")
            intent = Path(
                engine.path(
                    "slurm", "test-stage_01.submission-intent.json"
                )
            )
            intent.write_text("{}\n", encoding="utf-8")
            with self.assertRaisesRegex(WorkflowError, "reconcile it with Slurm"):
                engine.submit_manifest_batches(
                    stage_name="test-stage",
                    manifest_paths=[str(manifest)],
                    run_id="submission-intent-test",
                )

    def test_phase1_dry_run_generates_and_validates_120_cases(self) -> None:
        cases = (
            ("kafka", "inputs/kafka-v1/phase1/sweep_manifest.csv", "cfg_"),
            ("pulsar", "inputs/pulsar-v1/phase1/phase1_manifest.csv", "pulsar-cfg-"),
        )
        for backend, manifest_relative, expected_prefix in cases:
            with self.subTest(backend=backend), tempfile.TemporaryDirectory() as tmpdir:
                output_root = Path(tmpdir) / "workflows"
                run_id = f"{backend}-workflow-test"
                command = [
                    str(PROJECT_ROOT / "scripts" / "run_reproducible_benchmark.sh"),
                    backend,
                    "--dry-run",
                    "--phase",
                    "phase1",
                    "--run-id",
                    run_id,
                    "--output-root",
                    str(output_root),
                ]
                if backend == "kafka":
                    command.extend(["--partition", "standard96s"])
                completed = subprocess.run(
                    command,
                    cwd=PROJECT_ROOT,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    check=True,
                )
                self.assertIn("DRY_RUN=1", completed.stdout)
                run_root = output_root / backend / run_id
                state = json.loads(
                    (run_root / "workflow_state.json").read_text(encoding="utf-8")
                )
                self.assertEqual(state["status"], "dry_run_validated")
                if backend == "kafka":
                    self.assertEqual(
                        state["execution_settings"]["slurm"]["partition"],
                        "standard96s",
                    )
                jobs = state["stages"]["v1-phase1-submit"]["job_ids"]
                self.assertEqual(len(jobs), 4)
                self.assertTrue(all(job_id.startswith("dry-run-") for job_id in jobs))
                ledger_name = (
                    "pulsar-v1-phase1_jobs.csv"
                    if backend == "pulsar"
                    else "kafka-v1-phase1_jobs.tsv"
                )
                ledger_path = run_root / "slurm" / ledger_name
                ledger_rows = _read_csv(ledger_path)
                self.assertEqual(len(ledger_rows), 4)
                self.assertTrue(all(not row["dependency"] for row in ledger_rows))
                self.assertEqual(
                    state["stages"]["v1-phase1-submit"]["details"][
                        "submission_mode"
                    ],
                    "independent",
                )
                manifest = run_root / manifest_relative
                with manifest.open("r", encoding="utf-8", newline="") as handle:
                    rows = list(csv.DictReader(handle))
                self.assertEqual(len(rows), 120)
                id_field = "case_id" if "case_id" in rows[0] else "config_id"
                self.assertEqual(len({row[id_field] for row in rows}), 120)
                self.assertTrue(
                    all(row[id_field].startswith(expected_prefix) for row in rows)
                )
                self.assertFalse(
                    (PROJECT_ROOT / "results" / "runs" / backend / run_id).exists()
                )
                commands = [
                    json.loads(line)["command"]
                    for line in (run_root / "logs" / "commands.jsonl")
                    .read_text(encoding="utf-8")
                    .splitlines()
                ]
                self.assertFalse(
                    any(
                        Path(command[0]).name in {"sbatch", "squeue", "sacct"}
                        for command in commands
                    )
                )

    def test_machine_bundle_and_optional_renderer_are_independent(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            phase1 = root / "phase1-analysis"
            validation = root / "validation-analysis"
            machine = root / "machine-results"
            presentation = root / "presentation"
            phase1.mkdir()
            validation.mkdir()

            shortlist_ids = [f"cfg_{index:03d}" for index in range(1, 11)]
            phase1_rows = []
            for index in range(1, 121):
                case_id = f"cfg_{index:03d}"
                phase1_rows.append(
                    {
                        "case_id": case_id,
                        "eligible": True,
                        "qualified": index <= 10,
                        "qualification_status": (
                            "qualified" if index <= 10 else "overdriven"
                        ),
                        "balanced_mib_per_sec": 500.0 - index,
                        "pending_backlog_percent": float(index % 8),
                        "source_report": f"phase1/{case_id}/final_report.json",
                        "source_report_sha256": _digest(case_id),
                    }
                )
            repeat_rows = []
            validation_manifest_rows = []
            for block in range(1, 6):
                for order, config_id in enumerate(shortlist_ids, start=1):
                    case_id = f"validation-b{block:02d}-{config_id}"
                    repeat_rows.append(
                        {
                            "case_id": case_id,
                            "workload_config_id": config_id,
                            "eligible": True,
                            "qualified": True,
                            "qualification_status": "qualified",
                            "source_report": f"validation/{case_id}/final_report.json",
                            "source_report_sha256": _digest(case_id),
                        }
                    )
                    validation_manifest_rows.append(
                        {
                            "case_id": case_id,
                            "config_id": case_id,
                            "workload_config_id": config_id,
                            "block": block,
                            "order": order,
                            "config_path": f"configs/{case_id}.json",
                        }
                    )
            summaries = [
                {
                    "validation_rank": rank,
                    "original_config_id": config_id,
                    "repeats": 5,
                    "eligible_count": 5,
                    "qualified_count": 5,
                    "median_balanced_mib_per_sec": 500.0 - rank,
                    "iqr_balanced_mib_per_sec": 1.0,
                    "median_balanced_records_per_sec": 100_000.0 - rank,
                    "median_latency_p99_us": 1_000.0 + rank,
                }
                for rank, config_id in enumerate(shortlist_ids, start=1)
            ]
            shortlist = [
                {"selection_order": index, "config_id": config_id}
                for index, config_id in enumerate(shortlist_ids, start=1)
            ]
            phase1_manifest_rows = [
                {
                    "config_id": f"cfg_{index:03d}",
                    "config_path": f"configs/cfg_{index:03d}.json",
                }
                for index in range(1, 121)
            ]

            _write_json(phase1 / "campaign_validation.json", {"valid": True})
            _write_json(
                validation / "campaign_validation.json", {"valid": True}
            )
            _write_json(
                phase1 / "campaign_cases.json", {"cases": phase1_rows}
            )
            _write_json(
                validation / "campaign_cases.json", {"cases": repeat_rows}
            )
            _write_json(
                validation / "validation_summary_by_original.json", summaries
            )
            _write_json(phase1 / "validation_shortlist.json", {"shortlist": shortlist})
            _write_csv(phase1 / "campaign_cases.csv", phase1_rows)
            _write_csv(phase1 / "validation_shortlist.csv", shortlist)
            _write_csv(validation / "validation_repeats_by_case.csv", repeat_rows)
            _write_csv(
                validation / "validation_summary_by_original.csv", summaries
            )
            phase1_manifest = root / "phase1_manifest.csv"
            validation_manifest = root / "validation_manifest.csv"
            _write_csv(phase1_manifest, phase1_manifest_rows)
            _write_csv(validation_manifest, validation_manifest_rows)

            subprocess.run(
                [
                    sys.executable,
                    "-B",
                    str(PROJECT_ROOT / "scripts" / "finalize_reproducible_results.py"),
                    "--backend",
                    "kafka",
                    "--phase1-dir",
                    str(phase1),
                    "--validation-dir",
                    str(validation),
                    "--phase1-manifest",
                    str(phase1_manifest),
                    "--validation-manifest",
                    str(validation_manifest),
                    "--output-dir",
                    str(machine),
                ],
                cwd=PROJECT_ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            result = json.loads(
                (machine / "final_report.json").read_text(encoding="utf-8")
            )
            self.assertEqual(result["status"], "complete")
            self.assertFalse(result["presentation_artifacts_required"])
            self.assertEqual(result["source_case_index"]["row_count"], 170)
            allowed = {".json", ".csv"}
            self.assertTrue(
                all(
                    path.name == "SHA256SUMS" or path.suffix in allowed
                    for path in machine.iterdir()
                    if path.is_file()
                )
            )
            sealed_hashes = {
                path.name: _file_digest(path)
                for path in machine.iterdir()
                if path.is_file()
            }

            phase1.rename(root / "phase1-analysis-unavailable")
            validation.rename(root / "validation-analysis-unavailable")
            subprocess.run(
                [
                    sys.executable,
                    "-B",
                    str(PROJECT_ROOT / "scripts" / "generate_reproducible_report.py"),
                    "--results-dir",
                    str(machine),
                    "--output-dir",
                    str(presentation),
                    "--skip-figures",
                ],
                cwd=PROJECT_ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertTrue((presentation / "reproducible_benchmark_report.md").is_file())
            self.assertTrue((presentation / "reproducible_benchmark_report.html").is_file())
            self.assertTrue((presentation / "reproducible_benchmark_report.tex").is_file())
            self.assertFalse((presentation / "reproducible_benchmark_report.pdf").exists())
            self.assertEqual(
                sealed_hashes,
                {
                    path.name: _file_digest(path)
                    for path in machine.iterdir()
                    if path.is_file()
                },
            )


def _write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def _portable_result(*, clock_invalid: bool) -> dict[str, object]:
    eligible = not clock_invalid
    return {
        "result_schema_version": "messaging-benchmark.result.v1",
        "case": {"case_id": "cfg_001", "status": "completed"},
        "config": {
            "backend_id": "kafka",
            "case_id": "cfg_001",
            "latency_enabled": True,
        },
        "system_under_test": {"backend_id": "kafka"},
        "backend_health": {"status": "healthy"},
        "eligibility": {
            "eligible": eligible,
            "failure_reasons": (
                []
                if eligible
                else [
                    "latency invalid: clock limits, envelope integrity, exact "
                    "sample completeness, or negative-latency checks failed"
                ]
            ),
        },
        "latency_validation": {
            "enabled": True,
            "valid": eligible,
            "clock_valid": eligible,
            "envelope_valid": True,
            "sample_completeness_valid": True,
            "negative_latency_count": 0,
            "sample_every": 10,
        },
        "common_metrics": {
            "timing": {
                "warmup_sec": 15,
                "measurement_sec": 30,
                "drain_timeout_sec": 60,
            },
            "latency_end_to_end": {
                "enabled": True,
                "valid": eligible,
                "sample_every": 10,
            },
            "records": {
                "missing": 0,
                "duplicate": 0,
                "out_of_order": 0,
                "invalid_envelope": 0,
                "surplus": 0,
            },
            "throughput": {
                "producer_mib_per_sec": 100.0,
                "consumer_mib_per_sec": 99.0,
                "balanced_mib_per_sec": 99.0,
                "producer_records_per_sec": 25_600.0,
                "consumer_records_per_sec": 25_344.0,
                "balanced_records_per_sec": 25_344.0,
            },
            "producer_delivery": {
                "backlog_denominator": "messages_attempted",
            },
            "qualification": {
                "eligible": eligible,
                "qualified": eligible,
                "policy_id": "qualification.application.v1",
            },
        },
    }


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        raise ValueError("test CSV rows must not be empty")
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=tuple(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        delimiter = "\t" if path.suffix == ".tsv" else ","
        return list(csv.DictReader(handle, delimiter=delimiter))


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


if __name__ == "__main__":
    unittest.main()
