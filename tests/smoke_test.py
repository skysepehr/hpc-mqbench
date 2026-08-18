from __future__ import annotations

import ast
import csv
import hashlib
import json
import importlib.util
import math
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path
from types import ModuleType

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from models.benchmark_config import BenchmarkConfig, benchmark_config_input_dict
from src.benchmark.broker_profile import (
    load_broker_profile,
    write_broker_profile,
)
from src.benchmark.case_compare import build_comparison_html, build_comparison_markdown
from src.benchmark.client_stats import (
    LibrdkafkaStatsTracker,
    aggregate_librdkafka_stats,
)
from src.benchmark.config_loader import load_experiment_case
from src.benchmark.config_loader import load_benchmark_config
from src.benchmark.backends import get_backend, register_backend
from src.benchmark.backend_health import (
    apply_backend_health_to_report_files,
    merge_backend_health_into_result,
)
from src.benchmark.core.result_schema import (
    enrich_result_schema,
    validate_result_schema,
)
from src.benchmark.html_report import build_html_report
from src.benchmark.monitoring_summary import (
    build_monitoring_markdown_section,
    build_monitoring_summary,
    extract_kafka_broker_throughput,
)
from src.benchmark.report_builder import ReportBuilder
from src.benchmark.roles import Role, assign_role
from src.benchmark.monitoring_client import metric_specs_for_backend, plot_metric_svg
from src.benchmark.metrics import MetricsAggregator
from src.benchmark.qualification import (
    BACKLOG_DENOMINATOR_ATTEMPTED,
    BACKLOG_DENOMINATOR_ENQUEUED,
    COMMON_QUALIFICATION_POLICY_ID,
    KAFKA_HISTORICAL_QUALIFICATION_POLICY_ID,
    PULSAR_HISTORICAL_QUALIFICATION_POLICY_ID,
    backlog_denominator_for_policy,
    producer_operational_metrics,
)
from src.benchmark.clock_calibration import estimate_clock_offset
from src.benchmark.coordinated_drain import CoordinatedDrainState
from src.benchmark.record_envelope import (
    ENVELOPE_SIZE_BYTES,
    LatencyHistogram,
    decode_record_envelope,
    encode_record_envelope,
    update_measurement_offset_order,
)
from src.benchmark.send_patterns import SteadySendPatternController
from src.benchmark.system_inventory import (
    merge_inventory,
    normalize_iperf_record,
    parse_iperf3_json,
    parse_stream_probe_output,
)
from tests.fake_backend import FakeBackendAdapter


def _load_script_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_primary_config_loads() -> None:
    case = load_experiment_case(
        PROJECT_ROOT / "configs" / "one_broker_mpi_simultaneous.json",
        case_id="smoke_case",
        case_name="smoke",
        output_dir=PROJECT_ROOT / "results" / "smoke_case",
    )

    assert case.config.mode == "single"
    assert case.config.scenario == "simultaneous"
    assert case.config.broker_count == 1
    assert case.config.total_mpi_ranks == 81
    assert case.config.producer_ranks == 40
    assert case.config.consumer_ranks == 40
    assert case.config.acks == "1"
    assert case.config.compression_type == "none"


def test_high_throughput_configs_load_client_and_broker_tuning() -> None:
    case = load_experiment_case(
        PROJECT_ROOT / "configs" / "one_broker_mpi_high_throughput.json",
        case_id="tuned_case",
        case_name="tuned",
        output_dir=PROJECT_ROOT / "results" / "tuned_case",
    )
    extra = case.config.extra

    assert case.config.topic_name == "benchmark-topic-mpi-high-throughput"
    assert case.config.scenario == "simultaneous"
    assert case.config.partitions == 192
    assert case.config.producer_ranks == 64
    assert case.config.consumer_ranks == 64
    assert case.config.payload_size_bytes == 8192
    assert extra["target_throughput_mb_s"] == 1500
    assert extra["kafka_num_network_threads"] == 16
    assert extra["kafka_producer_config"]["queue.buffering.max.messages"] == 1000000
    assert extra["kafka_consumer_config"]["fetch.min.bytes"] == 4194304

    balanced_case = load_experiment_case(
        PROJECT_ROOT / "configs" / "one_broker_mpi_balanced.json",
        case_id="balanced_case",
        case_name="balanced",
        output_dir=PROJECT_ROOT / "results" / "balanced_case",
    )
    assert balanced_case.config.topic_name == "benchmark-topic-mpi-balanced"
    assert balanced_case.config.scenario == "simultaneous"
    assert balanced_case.config.partitions == 120
    assert balanced_case.config.producer_ranks == 40
    assert balanced_case.config.consumer_ranks == 40
    assert balanced_case.config.payload_size_bytes == 4096
    assert balanced_case.config.extra["target_throughput_mb_s"] == 700

    ingress_case = load_experiment_case(
        PROJECT_ROOT / "configs" / "one_broker_mpi_ingress_only.json",
        case_id="ingress_case",
        case_name="ingress",
        output_dir=PROJECT_ROOT / "results" / "ingress_case",
    )
    assert ingress_case.config.scenario == "ingress_ramp"
    assert ingress_case.config.partitions == 120
    assert ingress_case.config.producer_ranks == 40
    assert ingress_case.config.consumer_ranks == 40
    assert ingress_case.config.topic_name == "benchmark-topic-mpi-ingress-only"
    assert ingress_case.config.acks == "1"
    assert ingress_case.config.compression_type == "none"

    egress_case = load_experiment_case(
        PROJECT_ROOT / "configs" / "one_broker_mpi_egress_only.json",
        case_id="egress_case",
        case_name="egress",
        output_dir=PROJECT_ROOT / "results" / "egress_case",
    )
    assert egress_case.config.scenario == "egress_only"
    assert egress_case.config.partitions == 120
    assert egress_case.config.producer_ranks == 40
    assert egress_case.config.consumer_ranks == 40
    assert egress_case.config.extra["egress_prefill_messages"] == 20000000
    assert egress_case.config.topic_name == "benchmark-topic-mpi-egress-only"
    assert egress_case.config.acks == "1"
    assert egress_case.config.compression_type == "none"


def test_ordinary_iteration_configs_load_and_validate() -> None:
    expected = {
        "one_broker_mpi_ingress_only.json": "ingress_ramp",
        "one_broker_mpi_egress_only.json": "egress_only",
        "one_broker_mpi_simultaneous.json": "simultaneous",
    }
    for file_name, scenario in expected.items():
        case = load_experiment_case(
            PROJECT_ROOT / "configs" / file_name,
            case_id=file_name,
            case_name=file_name.removesuffix(".json"),
            output_dir=PROJECT_ROOT / "results" / file_name,
        )
        assert case.config.mode == "single"
        assert case.config.scenario == scenario
        assert case.config.broker_count == 1
        assert case.config.partitions == 120
        assert case.config.replication_factor == 1
        assert case.config.producer_ranks == 40
        assert case.config.consumer_ranks == 40
        assert case.config.duration_sec == 120
        assert case.config.acks == "1"
        assert case.config.compression_type == "none"


def test_v1_rejects_multi_broker_configs() -> None:
    try:
        BenchmarkConfig(
            scenario="simultaneous",
            broker_count=3,
            replication_factor=3,
            producer_ranks=1,
            consumer_ranks=1,
        )
    except ValueError as exc:
        assert "broker_count" in str(exc)
    else:
        raise AssertionError("V1 should reject broker_count > 1")


def test_mpi_role_assignment_for_first_case() -> None:
    config = BenchmarkConfig(
        scenario="simultaneous",
        broker_count=1,
        replication_factor=1,
        producer_ranks=1,
        consumer_ranks=1,
    )

    roles = [assign_role(rank, config).role for rank in range(config.total_mpi_ranks)]
    assert roles == [Role.CONTROLLER, Role.PRODUCER, Role.CONSUMER]


def test_submit_wrapper_defaults_to_four_node_role_split() -> None:
    submit = (PROJECT_ROOT / "scripts" / "submit_hpc_case.sh").read_text(
        encoding="utf-8"
    )
    iteration_submit = (PROJECT_ROOT / "scripts" / "submit_iteration.sh").read_text(
        encoding="utf-8"
    )
    run_iteration = (PROJECT_ROOT / "scripts" / "run_iteration.sh").read_text(
        encoding="utf-8"
    )
    check_hpc_result = (
        PROJECT_ROOT / "scripts" / "check_hpc_result.sh"
    ).read_text(encoding="utf-8")
    run_all = (PROJECT_ROOT / "run_all.sh").read_text(encoding="utf-8")
    run_case = (PROJECT_ROOT / "scripts" / "run_case.sh").read_text(
        encoding="utf-8"
    )

    assert 'BENCHMARK_NODE_LAYOUT="${BENCHMARK_NODE_LAYOUT:-role_split}"' in submit
    assert 'SLURM_CONTROLLER_NODES="${SLURM_CONTROLLER_NODES:-0}"' in submit
    assert "MPI rank 0: colocated on first producer node" in submit
    assert 'CONTROLLER_NODE_COUNT="${SLURM_CONTROLLER_NODES:-0}"' in run_case
    assert "APPEND_RANK_SLOT_OFFSET=1" in run_case
    assert 'MPI_PLACEMENT_MODE="${MPI_PLACEMENT_MODE:-rankfile}"' in run_case
    assert "allocation_hostfile_slots" in run_case
    assert "MPI_RANKFILE_LAUNCH_DIR" in run_case
    assert 'cp "$RANKFILE" "$MPI_RANKFILE_LAUNCH_PATH"' in run_case
    assert 'printf \'%s slots=0\\n\' "$node"' in run_case
    assert "MPI_PLACEMENT_MODE" in submit
    assert "egress_prefill_start" in run_all
    assert "egress_prefill_end" in run_all
    assert "CASE_ID_OVERRIDE" in run_all
    assert "configs/one_broker_mpi_ingress_only.json" in iteration_submit
    assert "configs/one_broker_mpi_egress_only.json" in iteration_submit
    assert "configs/one_broker_mpi_simultaneous.json" in iteration_submit
    assert "scripts/run_iteration.sh" in iteration_submit
    assert "Same allocation: yes" in iteration_submit
    assert "Result layout: results/iteration_<slurm_job_id>" in iteration_submit
    assert "[submit-hpc] Nodes: %s" in iteration_submit
    assert "dry-run did not resolve to 4 nodes" in iteration_submit
    assert "Role split nodes: controller=%s producer=%s consumer=%s" in iteration_submit
    assert "dry-run did not use the expected role-split placement" in iteration_submit
    assert "case_%s_%02d_%s" in run_iteration
    assert 'ITERATION_DIR="${ITERATION_DIR_OVERRIDE:-$PROJECT_ROOT/results/$ITERATION_ID}"' in run_iteration
    assert 'CASE_DIR_OVERRIDE="$case_dir"' in run_iteration
    assert 'case_dir="$ITERATION_DIR/$case_id"' in run_iteration
    assert "iteration_summary.json" in run_iteration
    assert "All scenarios completed on the same node allocation" in run_iteration
    assert "iteration_summary_path" in check_hpc_result
    assert "iteration completed" in check_hpc_result


def test_simultaneous_budgeted_sweep_generation_and_batches() -> None:
    generator = _load_script_module(
        "generate_simultaneous_budgeted_sweep",
        PROJECT_ROOT / "scripts" / "generate_simultaneous_budgeted_sweep.py",
    )
    batcher = _load_script_module(
        "make_simultaneous_budgeted_batches",
        PROJECT_ROOT / "scripts" / "make_simultaneous_budgeted_batches.py",
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir) / "sweep"
        rows = generator.generate_sweep(
            baseline_path=PROJECT_ROOT / "configs" / "one_broker_mpi_simultaneous.json",
            output_dir=output_dir,
            max_configs=12,
            seed=7,
        )
        assert len(rows) == 12
        assert (output_dir / "sweep_manifest.csv").is_file()
        for row in rows:
            config_path = PROJECT_ROOT / row["config_path"]
            if not config_path.is_file():
                config_path = Path(row["config_path"])
            payload = json.loads(config_path.read_text(encoding="utf-8"))
            assert payload["schema_version"] == "messaging-benchmark.case.v1"
            assert payload["backend_id"] == "kafka"
            config = load_benchmark_config(config_path)
            assert config.scenario == "simultaneous"
            assert config.acks == "1"
            assert config.compression_type == "none"
            assert config.broker_count == 1
            assert config.replication_factor == 1
            assert config.duration_sec == 30
            assert config.topic_name.startswith("sweep-sim-cfg-")

        batches = batcher.make_batches(
            manifest_path=output_dir / "sweep_manifest.csv",
            output_dir=output_dir / "batches",
            batch_size=5,
        )
        assert len(batches) == 3
        assert all(count <= 5 for _, count in batches)


def test_sweep_scripts_and_light_report_mode_contracts() -> None:
    submit_script = (PROJECT_ROOT / "scripts" / "submit_simultaneous_budgeted_batch.sh").read_text(encoding="utf-8")
    runner_script = (PROJECT_ROOT / "scripts" / "run_simultaneous_budgeted_batch.sh").read_text(encoding="utf-8")
    collect_script = (PROJECT_ROOT / "scripts" / "collect_results.sh").read_text(encoding="utf-8")
    run_case_script = (PROJECT_ROOT / "scripts" / "run_case.sh").read_text(encoding="utf-8")

    assert "scenario='simultaneous'" in submit_script
    assert "compression_type='none'" in submit_script
    assert "acks='1'" in submit_script
    assert "DRY_RUN=1, not submitting" in submit_script
    assert "dry-run did not resolve to 4 nodes" in submit_script
    assert "resolve_profile_for_config" in submit_script
    assert "A historical broker-once batch cannot mix broker profiles" in submit_script
    assert "run_simultaneous_budgeted_batch.sh" in submit_script
    assert "SWEEP_RESUME" in runner_script
    assert "skipped_time_guard" in runner_script
    assert "BENCHMARK_REPORT_MODE=\"${BENCHMARK_REPORT_MODE:-light}\"" in runner_script
    assert "aggregate_simultaneous_budgeted_results.py" in runner_script
    assert "analyze_simultaneous_budgeted_results.py" in runner_script
    assert "Per-case HTML report skipped because BENCHMARK_REPORT_MODE=light" in collect_script
    assert "--report-mode" in run_case_script


def test_report_builder_light_mode_skips_html() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        config = BenchmarkConfig(
            scenario="simultaneous",
            broker_count=1,
            replication_factor=1,
            producer_ranks=1,
            consumer_ranks=1,
        )
        result = {
            "case": {"case_id": "cfg_001", "case_name": "cfg_001", "status": "completed"},
            "config": config.to_dict(),
            "world_size": config.total_mpi_ranks,
            "scenario_execution": {"active_roles": ["producer", "consumer"]},
            "aggregated_metrics": {
                "producers": {
                    "messages_attempted": 10,
                    "messages_enqueued": 10,
                    "messages_delivered": 10,
                    "messages_failed": 0,
                    "throughput_megabytes_per_sec": 1.0,
                    "first_start_time_unix": 100.0,
                    "last_send_loop_end_time_unix": 130.0,
                    "last_end_time_unix": 130.0,
                },
                "consumers": {
                    "messages_received": 10,
                    "messages_failed": 0,
                    "throughput_megabytes_per_sec": 1.0,
                    "first_start_time_unix": 100.0,
                    "last_end_time_unix": 130.0,
                },
            },
            "per_rank_results": [],
        }

        ReportBuilder(output_dir, report_mode="light").build(result)
        report_json = json.loads((output_dir / "final_report.json").read_text(encoding="utf-8"))
        assert report_json["report_artifacts"]["mode"] == "light"
        assert report_json["report_artifacts"]["html_report_skipped"] is True
        assert (output_dir / "final_report.md").is_file()
        assert not (output_dir / "final_report.html").exists()


def test_report_builder_machine_mode_writes_json_only() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        config = BenchmarkConfig(
            scenario="simultaneous",
            broker_count=1,
            replication_factor=1,
            producer_ranks=1,
            consumer_ranks=1,
        )
        result = {
            "case": {"case_id": "cfg_machine", "status": "completed"},
            "config": config.to_dict(),
            "world_size": config.total_mpi_ranks,
            "scenario_execution": {"active_roles": ["producer", "consumer"]},
            "aggregated_metrics": {
                "producers": {
                    "messages_attempted": 10,
                    "messages_enqueued": 10,
                    "messages_delivered": 10,
                    "messages_failed": 0,
                    "throughput_bytes_per_sec": 1_048_576.0,
                    "first_start_time_unix": 100.0,
                    "last_send_loop_end_time_unix": 130.0,
                    "last_end_time_unix": 130.0,
                },
                "consumers": {
                    "messages_received": 10,
                    "messages_failed": 0,
                    "throughput_bytes_per_sec": 1_048_576.0,
                    "first_start_time_unix": 100.0,
                    "last_end_time_unix": 130.0,
                },
            },
            "per_rank_results": [],
        }

        ReportBuilder(output_dir, report_mode="machine").build(result)
        report = json.loads(
            (output_dir / "final_report.json").read_text(encoding="utf-8")
        )
        assert report["report_artifacts"]["mode"] == "machine"
        assert report["report_artifacts"]["presentation_skipped"] is True
        assert (output_dir / "data" / "final_report.json").is_file()
        assert (output_dir / "runtime" / "benchmark_events.json").is_file()
        for pattern in ("*.md", "*.html", "*.tex", "*.pdf", "*.png", "*.svg"):
            assert not list(output_dir.rglob(pattern))


def test_sweep_aggregation_and_analysis_from_synthetic_reports() -> None:
    aggregator = _load_script_module(
        "aggregate_simultaneous_budgeted_results",
        PROJECT_ROOT / "scripts" / "aggregate_simultaneous_budgeted_results.py",
    )
    analyzer = _load_script_module(
        "analyze_simultaneous_budgeted_results",
        PROJECT_ROOT / "scripts" / "analyze_simultaneous_budgeted_results.py",
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        run_dir = Path(tmpdir) / "results" / "sweeps" / "simultaneous_budgeted" / "run_001"
        case_dir = run_dir / "batch_001_job_manual" / "cases" / "cfg_001"
        case_dir.mkdir(parents=True)
        config = BenchmarkConfig(
            scenario="simultaneous",
            broker_count=1,
            replication_factor=1,
            topic_name="sweep-sim-cfg-001",
            acks="1",
            compression_type="none",
            batch_size=1048576,
            linger_ms=20,
            payload_size_bytes=4096,
            producer_ranks=40,
            consumer_ranks=40,
            duration_sec=30,
            extra={
                "sweep_config_id": "cfg_001",
                "kafka_producer_config": {
                    "queue.buffering.max.messages": 1000000,
                    "queue.buffering.max.kbytes": 1048576,
                },
                "kafka_consumer_config": {
                    "fetch.min.bytes": 1048576,
                    "fetch.wait.max.ms": 50,
                    "fetch.message.max.bytes": 8388608,
                },
            },
        )
        report = {
            "case": {
                "case_id": "cfg_001",
                "case_name": "cfg_001",
                "status": "completed",
                "notes": {"config_path": "configs/sweeps/simultaneous_budgeted/generated_configs/cfg_001.json"},
            },
            "config": config.to_dict(),
            "aggregated_metrics": {
                "producers": {
                    "messages_attempted": 1000,
                    "messages_enqueued": 1000,
                    "messages_delivered": 990,
                    "messages_failed": 1,
                    "pending_messages_at_flush_start": 50,
                    "max_flush_duration_sec": 12.0,
                    "throughput_bytes_per_sec": 600.0 * 1_048_576,
                    "throughput_megabytes_per_sec": 600.0,
                },
                "consumers": {
                    "messages_received": 1000,
                    "throughput_bytes_per_sec": 550.0 * 1_048_576,
                    "throughput_megabytes_per_sec": 550.0,
                },
            },
            "monitoring": {
                "collected_metrics": [
                    {
                        "id": "node_cpu_busy_percent",
                        "series": [{"labels": {"role": "broker"}, "max_value": 75.0}],
                    },
                    {
                        "id": "node_memory_used_gb",
                        "series": [{"labels": {"role": "broker"}, "max_value": 42.0}],
                    },
                    {
                        "id": "node_network_receive_mbps",
                        "series": [{"labels": {"role": "broker"}, "max_value": 800.0}],
                    },
                    {
                        "id": "node_network_transmit_mbps",
                        "series": [{"labels": {"role": "broker"}, "max_value": 700.0}],
                    },
                ],
            },
            "kafka_broker_throughput": {
                "metrics": [
                    {"id": "kafka_jmx_bytes_in_counter_rate", "avg_megabytes_per_sec": 700.0},
                    {"id": "kafka_jmx_bytes_out_counter_rate", "avg_megabytes_per_sec": 650.0},
                ]
            },
            "throughput_verdict": {"label": "Sustained Clean Run"},
            "bottleneck_analysis": {"primary_conclusion": "synthetic"},
        }
        (case_dir / "final_report.json").write_text(json.dumps(report), encoding="utf-8")
        rows = aggregator.aggregate_run(run_dir)
        assert rows[0]["balanced_app_MBps"] == 550.0
        assert "stable_score" not in rows[0]
        assert rows[0]["producer_queue_messages"] == 1000000
        assert rows[0]["producer_queue_kbytes"] == 1048576
        assert rows[0]["consumer_fetch_min_bytes"] == 1048576
        assert rows[0]["consumer_fetch_wait_max_ms"] == 50
        assert rows[0]["consumer_fetch_message_max_bytes"] == 8388608
        aggregator.write_summary(run_dir, rows)
        summary_csv = (run_dir / "sweep_summary.csv").read_text(encoding="utf-8")
        assert "producer_queue_messages" in summary_csv
        assert "consumer_fetch_message_max_bytes" in summary_csv
        html = analyzer.build_html(run_dir, analyzer.load_summary_rows(run_dir))
        assert "Simultaneous Budgeted Sweep Summary" in html
        assert "Configuration And Result Definitions" in html
        assert "librdkafka queue.buffering.max.messages" in html
        assert "cfg_001" in html
        assert "Top Balanced App Throughput" in html
        assert "P queue msgs" in html


def test_complete_analysis_qualification_shortlist_and_validation_manifest() -> None:
    if any(
        importlib.util.find_spec(module_name) is None
        for module_name in ("numpy", "matplotlib")
    ):
        return
    generator = _load_script_module(
        "generate_complete_analysis",
        PROJECT_ROOT / "scripts" / "generate_complete_analysis.py",
    )

    assert generator.qualification_status_from_thresholds(5.0, 0.1, 10.0) == "Qualified"
    assert generator.qualification_status_from_thresholds(5.001, 0.1, 10.0) == "Overdriven"
    assert generator.qualification_status_from_thresholds(5.0, 0.1, 10.001) == "Overdriven"
    assert generator.qualification_status_from_thresholds(5.0, 0.101, 10.0) == "Overdriven"

    baseline = {
        "producer_ranks": 40,
        "consumer_ranks": 40,
        "partitions": 120,
        "batch_size": 1048576,
        "linger_ms": 20,
        "payload_size_bytes": 4096,
        "producer_queue_messages": 1000000,
        "producer_queue_kbytes": 1048576,
        "consumer_fetch_min_bytes": 1048576,
        "consumer_fetch_wait_max_ms": 50,
        "consumer_fetch_message_max_bytes": 8388608,
    }

    def row(config_id: str, balanced: float, backlog: float, flush: float, failed: float, **overrides: object) -> dict[str, object]:
        data: dict[str, object] = {
            "config_id": config_id,
            "status": "completed",
            "config_path": f"configs/sweeps/simultaneous_budgeted/generated_configs/{config_id}.json",
            "producer_delivered_MBps": balanced + 5,
            "consumer_received_MBps": balanced,
            "balanced_app_MBps": balanced,
            "broker_ingress_MBps": balanced,
            "broker_egress_MBps": balanced,
            "broker_combined_MBps": balanced * 2,
            "pending_backlog_percent": backlog,
            "flush_sec": flush,
            "failed_send_percent": failed,
            "throughput_verdict": generator.verdict_from_thresholds(backlog, failed, flush),
            **baseline,
        }
        data.update(overrides)
        generator.enrich_derived_fields(data)
        return data

    rows = [
        row("cfg_092", 391.689, 1.575, 0.295, 0.0),
        row("cfg_074", 391.430, 1.711, 0.339, 0.0),
        row("cfg_115", 386.941, 1.887, 0.403, 0.0),
        row("cfg_076", 418.564, 7.989, 1.454, 0.0),
        row("cfg_106", 523.266, 11.904, 2.953, 0.0),
        row("cfg_091", 529.833, 17.157, 3.806, 0.0),
        row("cfg_055", 565.567, 33.393, 9.007, 0.39876),
        row("cfg_119", 573.735, 90.0, 30.0, 0.2),
        row("cfg_001", 414.971, 74.399, 30.093, 0.00003),
        row("cfg_037", 556.360, 65.141, 30.095, 0.0, consumer_fetch_min_bytes=4194304),
    ]
    generator.assign_ranks(rows)

    cfg_074 = next(item for item in rows if item["config_id"] == "cfg_074")
    assert abs(
        float(cfg_074["producer_records_per_sec"])
        - float(cfg_074["producer_delivered_MBps"]) * 1048576 / 4096
    ) < 1e-9
    assert abs(
        float(cfg_074["balanced_records_per_sec"])
        - min(
            float(cfg_074["producer_records_per_sec"]),
            float(cfg_074["consumer_records_per_sec"]),
        )
    ) < 1e-9
    assert not generator.validate_record_throughput_rows(rows, "synthetic sweep")

    missing_payload = {
        "producer_delivered_MBps": 100.0,
        "consumer_received_MBps": 90.0,
        "balanced_app_MBps": 90.0,
    }
    missing_issues = generator.derive_record_throughput_fields(
        missing_payload, "missing-payload"
    )
    assert "missing or non-positive payload_size_bytes" in missing_issues[0]
    assert missing_payload["balanced_records_per_sec"] == ""

    mixed_summary = [{"original_config_id": "cfg_mixed", "config_id": "cfg_mixed"}]
    mixed_repeats = [
        {
            "case_id": "mixed_1",
            "original_config_id": "cfg_mixed",
            "config_id": "cfg_mixed",
            "status": "completed",
            "payload_size_bytes": 1024,
            "producer_delivered_MBps": 100.0,
            "consumer_received_MBps": 90.0,
            "balanced_app_MBps": 90.0,
        },
        {
            "case_id": "mixed_2",
            "original_config_id": "cfg_mixed",
            "config_id": "cfg_mixed",
            "status": "completed",
            "payload_size_bytes": 2048,
            "producer_delivered_MBps": 100.0,
            "consumer_received_MBps": 90.0,
            "balanced_app_MBps": 90.0,
        },
    ]
    assert not generator.enrich_validation_record_throughput(
        mixed_summary, mixed_repeats, []
    )
    assert mixed_summary[0]["payload_size_bytes"] == "varies"
    assert mixed_summary[0]["median_balanced_records_per_sec"] == 69120.0

    qualified_ranks = [int(item["primary_rank"]) for item in rows if item["is_qualified"]]
    overdriven_ranks = [int(item["primary_rank"]) for item in rows if not item["is_qualified"]]
    assert max(qualified_ranks) < min(overdriven_ranks)
    assert [item["config_id"] for item in sorted([r for r in rows if r["is_qualified"]], key=generator.within_group_rank_key)[:3]] == ["cfg_092", "cfg_074", "cfg_115"]

    manifest = {
        item["config_id"]: {
            "config_id": item["config_id"],
            "config_path": item["config_path"],
            "notes": "baseline" if item["config_id"] == "cfg_001" else (
                "one-factor:consumer_fetch_min_bytes=4194304" if item["config_id"] == "cfg_037" else "seeded-mixed"
            ),
        }
        for item in rows
    }
    selected = generator.select_validation_shortlist(rows, [], manifest)
    shortlist = [generator.shortlist_output_row(item) for item in selected.values()]
    ids = [item["config_id"] for item in shortlist]
    assert len(ids) == len(set(ids))
    for config_id in ["cfg_092", "cfg_074", "cfg_115", "cfg_119", "cfg_055", "cfg_001"]:
        assert config_id in ids
    assert any("closest backlog boundary" in item["selection_categories"] for item in shortlist)
    assert all(item["selection_reason"] for item in shortlist)

    with tempfile.TemporaryDirectory() as tmpdir:
        old_root = generator.REPORT_ROOT
        try:
            generator.REPORT_ROOT = Path(tmpdir)
            run_rows = generator.write_validation_manifests(shortlist)
        finally:
            generator.REPORT_ROOT = old_root
        assert len(run_rows) == len(shortlist) * 5
        by_block: dict[int, list[str]] = {}
        for item in run_rows:
            by_block.setdefault(int(item["block_number"]), []).append(str(item["config_id"]))
        assert sorted(by_block) == [1, 2, 3, 4, 5]
        for config_ids in by_block.values():
            assert sorted(config_ids) == sorted(ids)

    validation_rows = [
        {
            "validation_rank": "1",
            "original_config_id": "cfg_074",
            "config_id": "cfg_074",
            "repeats": "5",
            "qualified_count": "5",
            "qualified_frequency": "1.0",
            "median_balanced_app_MBps": "394.513",
            "mean_balanced_app_MBps": "394.331",
            "min_balanced_app_MBps": "389.800",
            "max_balanced_app_MBps": "397.736",
            "median_pending_backlog_percent": "1.263",
            "median_flush_sec": "0.305",
            "median_failed_send_percent": "0.0",
            "cv_balanced_app_MBps": "0.0074",
        },
        {
            "validation_rank": "2",
            "original_config_id": "cfg_115",
            "config_id": "cfg_115",
            "repeats": "5",
            "qualified_count": "5",
            "qualified_frequency": "1.0",
            "median_balanced_app_MBps": "390.488",
            "mean_balanced_app_MBps": "389.765",
            "min_balanced_app_MBps": "387.644",
            "max_balanced_app_MBps": "391.374",
            "median_pending_backlog_percent": "1.604",
            "median_flush_sec": "0.415",
            "median_failed_send_percent": "0.0",
            "cv_balanced_app_MBps": "0.0046",
        },
        {
            "validation_rank": "3",
            "original_config_id": "cfg_092",
            "config_id": "cfg_092",
            "repeats": "5",
            "qualified_count": "4",
            "qualified_frequency": "0.8",
            "median_balanced_app_MBps": "393.932",
            "mean_balanced_app_MBps": "384.385",
            "min_balanced_app_MBps": "347.731",
            "max_balanced_app_MBps": "394.464",
            "median_pending_backlog_percent": "1.527",
            "median_flush_sec": "0.293",
            "median_failed_send_percent": "0.0",
            "cv_balanced_app_MBps": "0.0534",
        },
    ]
    validation_repeat_rows = [
        {
            "case_id": "val_b01_o08_cfg_074",
            "original_config_id": "cfg_074",
            "config_id": "cfg_074",
            "block": "1",
            "block_order": "8",
            "status": "completed",
            "qualified": "True",
            "qualification_status": "qualified",
            "payload_size_bytes": "4096",
            "balanced_app_MBps": "389.800",
            "producer_delivered_MBps": "395.000",
            "consumer_received_MBps": "389.800",
            "pending_backlog_percent": "2.10",
            "flush_sec": "0.48",
            "failed_send_percent": "0.0",
        }
    ]
    assert not generator.enrich_validation_record_throughput(
        validation_rows, validation_repeat_rows, rows
    )
    assert validation_rows[0]["median_balanced_records_per_sec"] == 99788.8
    assert [row["config_id"] for row in generator.fully_validated_rows(validation_rows)] == ["cfg_074", "cfg_115"]
    assert generator.qualified_fraction(validation_rows[0]) == "5/5"
    markdown = generator.build_validation_results_markdown(validation_rows, validation_repeat_rows)
    assert "Primary validated recommendation: `cfg_074`" in markdown
    assert "| 3 | `cfg_092` | 4/5 |" in markdown
    assert "Median records/s" in markdown
    assert "99,789" in markdown
    latex_summary = generator.latex_validation_executive_summary(
        validation_rows[0],
        generator.fully_validated_rows(validation_rows),
        rows[7],
        rows[6],
        [r for r in rows if r["is_qualified"]],
    )
    assert r"\cfg{074}" in latex_summary
    assert "5/5 repeats" in latex_summary

    with tempfile.TemporaryDirectory() as tmpdir:
        old_root = generator.REPORT_ROOT
        old_context = dict(generator.INPUT_CONTEXT)
        try:
            generator.REPORT_ROOT = Path(tmpdir)
            generator.INPUT_CONTEXT.update(
                {
                    "validation_summary": "synthetic/validation_summary_by_original.csv",
                    "validation_repeats": "synthetic/validation_repeats_by_case.csv",
                    "validation_job_id": "14930288",
                    "validation_run_id": "run_synthetic",
                }
            )
            generator.write_validation_result_artifacts(validation_rows, validation_repeat_rows)
            assert (Path(tmpdir) / "analysis_validation_results.csv").is_file()
            assert (Path(tmpdir) / "analysis_validation_results.md").is_file()
            assert (Path(tmpdir) / "analysis_validation_repeats.csv").is_file()
            assert "median_balanced_records_per_sec" in (
                Path(tmpdir) / "analysis_validation_results.csv"
            ).read_text(encoding="utf-8")
            assert "balanced_records_per_sec" in (
                Path(tmpdir) / "analysis_validation_repeats.csv"
            ).read_text(encoding="utf-8")
        finally:
            generator.REPORT_ROOT = old_root
            generator.INPUT_CONTEXT.clear()
            generator.INPUT_CONTEXT.update(old_context)


def test_report_builder_writes_mpi_report_without_monitoring() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        config = BenchmarkConfig(
            scenario="simultaneous",
            broker_count=1,
            replication_factor=1,
            producer_ranks=1,
            consumer_ranks=1,
        )
        result = {
            "case": {
                "case_id": "smoke_case",
                "case_name": "smoke",
                "campaign_id": None,
                "status": "completed",
            },
            "config": config.to_dict(),
            "world_size": config.total_mpi_ranks,
            "scenario_execution": {
                "active_roles": ["producer", "consumer"],
            },
            "aggregated_metrics": {
                "producers": {
                    "producer_rank_count": 1,
                    "messages_attempted": 12,
                    "messages_enqueued": 12,
                    "messages_delivered": 10,
                    "messages_failed": 1,
                    "pending_messages_at_flush_start": 2,
                    "pending_bytes_at_flush_start": 2048,
                    "producer_queue_len_at_flush_start": 2,
                    "producer_queue_len_after_flush": 0,
                    "flush_remaining_messages": 0,
                    "delivery_callbacks_during_flush": 2,
                    "delivery_callback_rate_during_flush_per_sec": 20.0,
                    "messages_delivered_at_flush_start": 8,
                    "messages_delivered_during_flush": 2,
                    "messages_failed_at_flush_start": 0,
                    "messages_failed_during_flush": 1,
                    "produce_error_counts": {"BufferError": 1},
                    "delivery_error_counts": {"MSG_TIMED_OUT": 1},
                    "librdkafka_stats": {
                        "enabled": True,
                        "rank": 1,
                        "sample_count": 3,
                        "tx_bytes_last": 20480,
                        "rx_bytes_last": 4096,
                        "max_msg_cnt": 4,
                        "max_msg_size": 4096,
                        "max_broker_waitresp_cnt": 2,
                        "max_broker_rtt_p95_us": 5000,
                        "broker_state_counts": {"UP": 1},
                    },
                    "bytes_delivered": 10240,
                    "throughput_msgs_per_sec": 5.0,
                    "throughput_bytes_per_sec": 5120.0,
                    "throughput_megabytes_per_sec": 0.00512,
                    "first_start_time_unix": 1_800_000_010.0,
                    "last_send_loop_end_time_unix": 1_800_000_070.0,
                    "last_end_time_unix": 1_800_000_075.0,
                },
                "consumers": {
                    "consumer_rank_count": 1,
                    "messages_received": 8,
                    "messages_failed": 0,
                    "bytes_received": 8192,
                    "throughput_msgs_per_sec": 4.0,
                    "throughput_bytes_per_sec": 4096.0,
                    "throughput_megabytes_per_sec": 0.004096,
                    "first_start_time_unix": 1_800_000_009.0,
                    "last_end_time_unix": 1_800_000_074.0,
                },
            },
            "timeline": {
                "events": [
                    {
                        "name": "kafka_start_requested",
                        "label": "Kafka broker start requested",
                        "unix": 1_800_000_000.0,
                        "iso": "2027-01-15T08:00:00+00:00",
                        "source": "slurm",
                    }
                ]
            },
            "per_rank_results": [
                {
                    "rank": 0,
                    "role": "controller",
                    "active": True,
                    "metrics": {},
                },
                {
                    "rank": 1,
                    "role": "producer",
                    "active": True,
                    "metrics": {
                        "messages_delivered": 10,
                        "messages_failed": 0,
                        "throughput_msgs_per_sec": 5.0,
                        "throughput_megabytes_per_sec": 0.00512,
                    },
                },
                {
                    "rank": 2,
                    "role": "consumer",
                    "active": True,
                    "metrics": {
                        "messages_received": 8,
                        "messages_failed": 0,
                        "throughput_msgs_per_sec": 4.0,
                        "throughput_megabytes_per_sec": 0.004096,
                    },
                },
            ],
        }

        ReportBuilder(output_dir).build(result)

        report_json = json.loads((output_dir / "final_report.json").read_text())
        report_md = (output_dir / "final_report.md").read_text(encoding="utf-8")
        report_html = (output_dir / "final_report.html").read_text(encoding="utf-8")
        assert report_json["monitoring"]["status"] == "not_collected"
        assert "MPI benchmark" in report_md
        assert "Benchmark Configuration Used" in report_html
        assert "Full effective configuration JSON" in report_html
        assert "Executive Summary" in report_html
        assert report_html.index("Benchmark Configuration Used") < report_html.index("Executive Summary")
        assert "&quot;acks&quot;: &quot;all&quot;" in report_html
        assert "Run Verdict" in report_html
        assert "Scenario-Specific Result Interpretation" in report_html
        assert "Scenario Classification" in report_html
        assert "Simultaneous producer + consumer" in report_html
        assert "Bottleneck Analysis" in report_html
        assert "Timeline" in report_html
        assert "Definitions and Units" in report_html
        assert "Exporter Source Map" in report_html
        assert "overflow-wrap: anywhere" in report_html
        assert "repeat(auto-fit" in report_html
        assert "Metric Sources" in report_html
        assert "JMX is broker-side" in report_html
        assert "Kafka exporter is cluster/topic/lag-side" in report_html
        assert "Producer Flush Diagnostics" in report_html
        assert "Client Runtime Diagnostics" in report_html
        assert "Max broker requests waiting for response" in report_html
        assert "not a message count and not a total across all ranks" in report_html
        assert "Sustained Throughput Verdict" in report_html
        assert "Overdriven" in report_html
        assert "Pending messages at flush start" in report_html
        assert "BufferError: 1" in report_html
        assert "MSG_TIMED_OUT: 1" in report_html
        assert "Pending Messages at Flush Start" in report_md
        assert report_json["scenario_classification"]["label"] == "Simultaneous producer + consumer"
        assert "bottleneck_analysis" in report_json
        assert report_json["throughput_verdict"]["label"] == "Overdriven"
        assert report_json["aggregated_metrics"]["producers"]["librdkafka_stats"]["sample_count"] == 3
        assert (output_dir / "reports" / "final_report.html").is_file()
        assert (output_dir / "data" / "benchmark_events.json").is_file()


def test_html_report_uses_scenario_specific_measured_roles() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)

        ingress_config = BenchmarkConfig(
            scenario="ingress_ramp",
            broker_count=1,
            replication_factor=1,
            producer_ranks=1,
            consumer_ranks=1,
            acks="1",
            compression_type="none",
        )
        ingress_html = build_html_report(
            output_dir,
            _scenario_result(
                ingress_config,
                active_roles=["producer"],
                producers={
                    "messages_attempted": 100,
                    "messages_enqueued": 100,
                    "messages_delivered": 100,
                    "messages_failed": 0,
                    "throughput_megabytes_per_sec": 10.0,
                    "send_attempt_throughput_megabytes_per_sec": 10.0,
                    "max_flush_duration_sec": 0.2,
                },
                consumers={},
            ),
        )
        assert "Ingress-only producer" in ingress_html
        assert "Consumer throughput is intentionally inactive" in ingress_html
        assert "Consumer measured throughput present" not in ingress_html

        egress_config = BenchmarkConfig(
            scenario="egress_only",
            broker_count=1,
            replication_factor=1,
            producer_ranks=1,
            consumer_ranks=1,
            acks="1",
            compression_type="none",
        )
        egress_html = build_html_report(
            output_dir,
            _scenario_result(
                egress_config,
                active_roles=["consumer"],
                producers={},
                consumers={
                    "messages_received": 100,
                    "messages_failed": 0,
                    "throughput_megabytes_per_sec": 12.0,
                },
                egress_prefill={
                    "enabled": True,
                    "status": "partial_consumption",
                    "messages_delivered": 200,
                    "messages_consumed": 100,
                    "messages_remaining": 100,
                },
            ),
        )
        assert "Egress-only consumer" in egress_html
        assert "Producer prefill is setup traffic" in egress_html
        assert "Producer delivery efficiency" not in egress_html
        assert "Consumer measured throughput present" in egress_html

        simultaneous_config = BenchmarkConfig(
            scenario="simultaneous",
            broker_count=1,
            replication_factor=1,
            producer_ranks=1,
            consumer_ranks=1,
            acks="1",
            compression_type="none",
        )
        simultaneous_html = build_html_report(
            output_dir,
            _scenario_result(
                simultaneous_config,
                active_roles=["producer", "consumer"],
                producers={
                    "messages_attempted": 100,
                    "messages_enqueued": 100,
                    "messages_delivered": 95,
                    "messages_failed": 0,
                    "throughput_megabytes_per_sec": 11.0,
                    "send_attempt_throughput_megabytes_per_sec": 12.0,
                    "max_flush_duration_sec": 0.4,
                },
                consumers={
                    "messages_received": 90,
                    "messages_failed": 0,
                    "throughput_megabytes_per_sec": 10.0,
                },
            ),
        )
        assert "Simultaneous producer + consumer" in simultaneous_html
        assert "Producer delivery efficiency" in simultaneous_html
        assert "Consumer measured throughput present" in simultaneous_html


def _scenario_result(
    config: BenchmarkConfig,
    *,
    active_roles: list[str],
    producers: dict[str, object],
    consumers: dict[str, object],
    egress_prefill: dict[str, object] | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "case": {
            "case_id": f"case_{config.scenario}",
            "case_name": f"case_{config.scenario}",
            "campaign_id": None,
            "status": "completed",
            "notes": {"config_path": f"configs/{config.scenario}.json"},
        },
        "config": config.to_dict(),
        "world_size": config.total_mpi_ranks,
        "scenario_execution": {"active_roles": active_roles},
        "aggregated_metrics": {
            "producers": producers,
            "consumers": consumers,
        },
        "per_rank_results": [],
    }
    if egress_prefill is not None:
        result["egress_prefill"] = egress_prefill
    return result


def test_producer_flush_diagnostics_are_aggregated() -> None:
    aggregated = MetricsAggregator().aggregate_from_dicts(
        producer_metrics_list=[
            {
                "messages_attempted": 10,
                "messages_enqueued": 9,
                "messages_delivered": 8,
                "messages_failed": 2,
                "bytes_attempted": 10_000,
                "bytes_enqueued": 9_000,
                "bytes_delivered": 8_000,
                "delivery_callbacks_seen": 8,
                "delivery_callbacks_seen_at_flush_start": 5,
                "delivery_callbacks_during_flush": 3,
                "messages_delivered_at_flush_start": 5,
                "messages_delivered_during_flush": 3,
                "messages_failed_at_flush_start": 1,
                "messages_failed_during_flush": 1,
                "pending_messages_at_flush_start": 4,
                "pending_bytes_at_flush_start": 4_000,
                "producer_queue_len_at_flush_start": 4,
                "producer_queue_len_after_flush": 1,
                "flush_remaining_messages": 1,
                "flush_duration_sec": 2.0,
                "produce_error_counts": {"BufferError": 1},
                "delivery_error_counts": {"MSG_TIMED_OUT": 1},
                "librdkafka_stats": {
                    "enabled": True,
                    "rank": 1,
                    "sample_count": 2,
                    "max_msg_cnt": 7,
                    "max_broker_waitresp_cnt": 3,
                    "tx_bytes_last": 1000,
                    "broker_state_counts": {"UP": 1},
                },
            },
            {
                "messages_attempted": 20,
                "messages_enqueued": 20,
                "messages_delivered": 19,
                "messages_failed": 1,
                "bytes_attempted": 20_000,
                "bytes_enqueued": 20_000,
                "bytes_delivered": 19_000,
                "delivery_callbacks_seen": 20,
                "delivery_callbacks_seen_at_flush_start": 18,
                "delivery_callbacks_during_flush": 2,
                "messages_delivered_at_flush_start": 18,
                "messages_delivered_during_flush": 1,
                "messages_failed_at_flush_start": 0,
                "messages_failed_during_flush": 1,
                "pending_messages_at_flush_start": 2,
                "pending_bytes_at_flush_start": 2_000,
                "producer_queue_len_at_flush_start": 2,
                "producer_queue_len_after_flush": 0,
                "flush_remaining_messages": 0,
                "flush_duration_sec": 5.0,
                "produce_error_counts": {"BufferError": 2},
                "delivery_error_counts": {"MSG_TIMED_OUT": 1, "UNKNOWN_TOPIC": 1},
                "librdkafka_stats": {
                    "enabled": True,
                    "rank": 2,
                    "sample_count": 5,
                    "max_msg_cnt": 11,
                    "max_broker_waitresp_cnt": 4,
                    "tx_bytes_last": 2000,
                    "broker_state_counts": {"UP": 1},
                },
            },
        ],
        consumer_metrics_list=[],
    )

    producers = aggregated["producers"]
    assert producers["messages_enqueued"] == 29
    assert producers["pending_messages_at_flush_start"] == 6
    assert producers["producer_queue_len_at_flush_start"] == 6
    assert producers["flush_remaining_messages"] == 1
    assert producers["delivery_callbacks_during_flush"] == 5
    assert producers["delivery_callback_rate_during_flush_per_sec"] == 1.0
    assert producers["produce_error_counts"] == {"BufferError": 3}
    assert producers["delivery_error_counts"] == {
        "MSG_TIMED_OUT": 2,
        "UNKNOWN_TOPIC": 1,
    }
    assert producers["librdkafka_stats"]["sample_count"] == 7
    assert producers["librdkafka_stats"]["max_msg_cnt"] == 11
    assert producers["librdkafka_stats"]["max_broker_waitresp_cnt"] == 4
    assert producers["librdkafka_stats"]["tx_bytes_last"] == 3000


def test_librdkafka_stats_tracker_and_aggregate() -> None:
    tracker = LibrdkafkaStatsTracker(role="producer", rank=7)
    tracker.record(
        json.dumps(
            {
                "type": "producer",
                "name": "producer-test",
                "client_id": "benchmark-producer-rank-7",
                "tx_bytes": 1000,
                "rx_bytes": 200,
                "txerrs": 1,
                "msg_cnt": 12,
                "msg_size": 4096,
                "brokers": {
                    "broker": {
                        "state": "UP",
                        "waitresp_cnt": 3,
                        "outbuf_cnt": 2,
                        "rtt": {"avg": 400, "p95": 900},
                    }
                },
                "topics": {
                    "topic": {
                        "partitions": {
                            "0": {"msgq_cnt": 5, "xmit_msgq_cnt": 4}
                        }
                    }
                },
            }
        )
    )
    tracker.record(
        json.dumps(
            {
                "type": "producer",
                "tx_bytes": 1800,
                "rx_bytes": 400,
                "msg_cnt": 4,
                "brokers": {
                    "broker": {
                        "state": "UP",
                        "waitresp_cnt": 1,
                        "rtt": {"avg": 500, "p95": 1100},
                    }
                },
            }
        )
    )

    summary = tracker.summary()
    aggregate = aggregate_librdkafka_stats([summary])

    assert summary["sample_count"] == 2
    assert summary["max_msg_cnt"] == 12
    assert summary["max_broker_waitresp_cnt"] == 3
    assert summary["max_broker_rtt_p95_us"] == 1100
    assert summary["tx_bytes_last"] == 1800
    assert summary["tx_bytes_delta_between_stats"] == 800
    assert aggregate["sample_count"] == 2
    assert aggregate["max_msg_cnt"] == 12
    assert aggregate["tx_bytes_last"] == 1800


def test_report_builder_merges_system_inventory_and_embeds_html() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        data_dir = output_dir / "data"
        data_dir.mkdir(parents=True)
        inventory = {
            "format": "system_inventory.v1",
            "enabled": True,
            "status": "completed",
            "nodes": [
                {
                    "node": "gcn0001",
                    "role": "broker",
                    "service_address": "10.0.0.1",
                    "cpu": {
                        "model_name": "Test CPU",
                        "architecture": "x86_64",
                        "sockets": 1,
                        "cores_per_socket": 8,
                        "threads_per_core": 2,
                        "logical_cpus": 16,
                    },
                    "memory": {
                        "total_gb": 128.0,
                        "bandwidth_probe": {
                            "status": "completed",
                            "metrics": {
                                "read_gb_s": 95.0,
                                "write_gb_s": 90.0,
                                "copy_gb_s": 100.0,
                                "scale_gb_s": 98.0,
                                "add_gb_s": 110.0,
                                "triad_gb_s": 108.0,
                            },
                        },
                    },
                    "network": {
                        "interface": "ib0",
                        "link_speed_gbit": 100.0,
                    },
                    "numa": {"node_count": 2},
                }
            ],
            "network_tests": [
                {
                    "label": "producer_to_broker",
                    "source_node": "gcn0002",
                    "target_node": "gcn0001",
                    "status": "completed",
                    "gbit_per_second": 91.2,
                    "megabytes_per_second": 11400.0,
                    "parallel_streams": 4,
                }
            ],
            "probe_summary": {
                "failed_required_probe_count": 0,
                "required_network_tests_completed": True,
                "warnings": [],
            },
        }
        (data_dir / "system_inventory.json").write_text(
            json.dumps(inventory),
            encoding="utf-8",
        )

        config = BenchmarkConfig(
            scenario="simultaneous",
            broker_count=1,
            replication_factor=1,
            producer_ranks=1,
            consumer_ranks=1,
        )
        result = {
            "case": {"case_id": "inventory_case", "case_name": "inventory", "status": "completed"},
            "config": config.to_dict(),
            "world_size": config.total_mpi_ranks,
            "scenario_execution": {"active_roles": ["producer", "consumer"]},
            "aggregated_metrics": {
                "producers": {"producer_rank_count": 1, "messages_delivered": 1, "throughput_megabytes_per_sec": 1.0},
                "consumers": {"consumer_rank_count": 1, "messages_received": 1, "throughput_megabytes_per_sec": 1.0},
            },
            "per_rank_results": [],
        }

        ReportBuilder(output_dir).build(result)

        report_json = json.loads((output_dir / "final_report.json").read_text())
        report_html = (output_dir / "final_report.html").read_text(encoding="utf-8")
        assert report_json["system_inventory"]["status"] == "completed"
        assert "System Capability" in report_html
        assert "producer_to_broker" in report_html
        assert "RAM bandwidth GB/s" in report_html
        assert "Single-process RAM read/write GB/s" in report_html
        assert "read 95.0" in report_html
        assert "write 90.0" in report_html
        assert "Broker ingress avg" in report_html
        assert "Broker ingress peak" in report_html
        assert "Avg over window" in report_html
        assert "Peak sample" in report_html
        assert "100.00 Gbit/s" in report_html


def test_report_bottleneck_analysis_uses_iperf_and_exporter_lag_warning() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        data_dir = output_dir / "data"
        monitoring_dir = output_dir / "monitoring"
        data_dir.mkdir(parents=True)
        monitoring_dir.mkdir(parents=True)
        (data_dir / "system_inventory.json").write_text(
            json.dumps(
                {
                    "format": "system_inventory.v1",
                    "enabled": True,
                    "status": "completed",
                    "nodes": [
                        {
                            "node": "gcn0001",
                            "role": "broker",
                            "service_address": "10.0.0.1",
                            "network": {"interface": "ib0", "link_speed_gbit": 100.0},
                            "memory": {
                                "bandwidth_probe": {
                                    "status": "completed",
                                    "measurement_scope": "single_process_stream_style",
                                    "metrics": {
                                        "read_gb_s": 6.0,
                                        "write_gb_s": 5.5,
                                        "copy_gb_s": 12.0,
                                        "triad_gb_s": 10.0,
                                    },
                                }
                            },
                        },
                        {
                            "node": "gcn0002",
                            "role": "producer_controller",
                            "service_address": "10.0.0.2",
                        },
                        {
                            "node": "gcn0003",
                            "role": "consumer",
                            "service_address": "10.0.0.3",
                        },
                    ],
                    "network_tests": [
                        {
                            "label": "producer_to_broker",
                            "source_node": "gcn0002",
                            "target_node": "gcn0001",
                            "status": "completed",
                            "gbit_per_second": 16.0,
                            "megabytes_per_second": 2000.0,
                            "summary_source": "sum_sent",
                            "measured_seconds": 10.0,
                        },
                        {
                            "label": "broker_to_consumer",
                            "source_node": "gcn0001",
                            "target_node": "gcn0003",
                            "status": "completed",
                            "gbit_per_second": 32.0,
                            "megabytes_per_second": 4000.0,
                            "summary_source": "sum_sent",
                            "measured_seconds": 10.0,
                        },
                    ],
                    "probe_summary": {
                        "failed_required_probe_count": 0,
                        "required_network_tests_completed": True,
                        "warnings": [],
                    },
                }
            ),
            encoding="utf-8",
        )
        (monitoring_dir / "monitoring_summary.json").write_text(
            json.dumps(
                {
                    "format": "monitoring_bundle.v1",
                    "enabled": True,
                    "status": "completed",
                    "prometheus_url": "http://monitoring:9090",
                    "targets": {
                        "health_counts": {"up": 2},
                        "active_count": 2,
                        "dropped_count": 0,
                        "active_targets": [
                            {
                                "health": "up",
                                "scrape_url": "http://10.0.0.1:7101/metrics",
                                "labels": {
                                    "job": "kafka_jmx",
                                    "node": "gcn0001",
                                    "role": "broker",
                                },
                                "last_error": "",
                            },
                            {
                                "health": "up",
                                "scrape_url": "http://10.0.0.4:9308/metrics",
                                "labels": {
                                    "job": "kafka_exporter",
                                    "node": "gcn0004",
                                    "role": "monitoring",
                                },
                                "last_error": "",
                            },
                        ],
                    },
                    "collected_metrics": [
                        {
                            "id": "node_cpu_busy_percent",
                            "title": "Node CPU Busy Percent",
                            "category": "system",
                            "unit": "percent",
                            "sample_count": 2,
                            "numeric_sample_count": 2,
                            "avg_value": 20.0,
                            "max_value": 40.0,
                            "series": [
                                {
                                    "labels": {"node": "gcn0001", "role": "broker"},
                                    "avg_value": 20.0,
                                    "max_value": 40.0,
                                }
                            ],
                        },
                        {
                            "id": "node_memory_used_gb",
                            "title": "Node Memory Used",
                            "category": "system",
                            "unit": "GB",
                            "sample_count": 2,
                            "numeric_sample_count": 2,
                            "avg_value": 60.0,
                            "max_value": 80.0,
                            "series": [
                                {
                                    "labels": {"node": "gcn0001", "role": "broker"},
                                    "avg_value": 60.0,
                                    "max_value": 80.0,
                                }
                            ],
                        },
                        {
                            "id": "node_memory_total_gb",
                            "title": "Node Memory Total",
                            "category": "system",
                            "unit": "GB",
                            "sample_count": 2,
                            "numeric_sample_count": 2,
                            "avg_value": 160.0,
                            "max_value": 160.0,
                            "series": [
                                {
                                    "labels": {"node": "gcn0001", "role": "broker"},
                                    "avg_value": 160.0,
                                    "max_value": 160.0,
                                }
                            ],
                        },
                        {
                            "id": "node_network_receive_mbps",
                            "title": "Node Network Receive Rate",
                            "category": "system",
                            "unit": "MB/s",
                            "sample_count": 2,
                            "numeric_sample_count": 2,
                            "avg_value": 300.0,
                            "max_value": 500.0,
                            "series": [
                                {
                                    "labels": {"node": "gcn0001", "role": "broker"},
                                    "avg_value": 300.0,
                                    "max_value": 500.0,
                                }
                            ],
                        },
                        {
                            "id": "node_network_transmit_mbps",
                            "title": "Node Network Transmit Rate",
                            "category": "system",
                            "unit": "MB/s",
                            "sample_count": 2,
                            "numeric_sample_count": 2,
                            "avg_value": 250.0,
                            "max_value": 450.0,
                            "series": [
                                {
                                    "labels": {"node": "gcn0001", "role": "broker"},
                                    "avg_value": 250.0,
                                    "max_value": 450.0,
                                }
                            ],
                        },
                        {
                            "id": "kafka_jmx_bytes_in_counter_rate",
                            "title": "Kafka Broker Bytes In Rate",
                            "category": "kafka_jmx",
                            "unit": "bytes/sec",
                            "query": "rate(bytes_in)",
                            "sample_count": 2,
                            "numeric_sample_count": 2,
                            "avg_value": 800000000.0,
                            "max_value": 1200000000.0,
                            "series": [],
                        },
                        {
                            "id": "kafka_jmx_bytes_out_counter_rate",
                            "title": "Kafka Broker Bytes Out Rate",
                            "category": "kafka_jmx",
                            "unit": "bytes/sec",
                            "query": "rate(bytes_out)",
                            "sample_count": 2,
                            "numeric_sample_count": 2,
                            "avg_value": 1600000000.0,
                            "max_value": 2000000000.0,
                            "series": [],
                        },
                        {
                            "id": "kafka_exporter_brokers",
                            "title": "Kafka Exporter Broker Count",
                            "category": "kafka_exporter",
                            "unit": "brokers",
                            "sample_count": 1,
                            "numeric_sample_count": 1,
                            "avg_value": 1,
                            "max_value": 1,
                            "series": [],
                        },
                    ],
                    "missing_metrics": [
                        {
                            "id": "kafka_exporter_consumer_lag",
                            "title": "Kafka Consumer Group Lag",
                            "category": "kafka_exporter",
                            "required_for_complete": False,
                            "reason": "none of the required metric names are currently known to Prometheus",
                        }
                    ],
                    "graphs": [],
                    "errors": [],
                }
            ),
            encoding="utf-8",
        )

        config = BenchmarkConfig(
            scenario="simultaneous",
            broker_count=1,
            replication_factor=1,
            producer_ranks=1,
            consumer_ranks=1,
            extra={"target_throughput_mb_s": 1500},
        )
        result = {
            "case": {"case_id": "bottleneck_case", "case_name": "simultaneous_high_throughput", "status": "completed"},
            "config": config.to_dict(),
            "world_size": config.total_mpi_ranks,
            "scenario_execution": {"active_roles": ["producer", "consumer"]},
            "aggregated_metrics": {
                "producers": {
                    "producer_rank_count": 1,
                    "messages_attempted": 1000,
                    "messages_enqueued": 1000,
                    "messages_delivered": 800,
                    "messages_failed": 2,
                    "pending_messages_at_flush_start": 120,
                    "max_flush_duration_sec": 15.0,
                    "throughput_megabytes_per_sec": 900.0,
                    "send_attempt_throughput_megabytes_per_sec": 1800.0,
                },
                "consumers": {
                    "consumer_rank_count": 1,
                    "messages_received": 700,
                    "messages_failed": 0,
                    "throughput_megabytes_per_sec": 1000.0,
                },
            },
            "per_rank_results": [],
        }

        ReportBuilder(output_dir).build(result)

        report_json = json.loads((output_dir / "final_report.json").read_text())
        report_html = (output_dir / "final_report.html").read_text(encoding="utf-8")
        assert "Bottleneck Analysis" in report_html
        assert "System Saturation During Kafka" in report_html
        assert "RAM peak %" in report_html
        assert "50.0%" in report_html
        assert "40.0%" in report_html
        assert "Kafka/client pipeline is the likely bottleneck" in report_html
        assert "Sustained Throughput Verdict" in report_html
        assert "Overdriven" in report_html
        assert "Broker RAM bandwidth" in report_html
        assert "read 6.00 GB/s" in report_html
        assert "write 5.50 GB/s" in report_html
        assert "RAM bandwidth unlikely bottleneck" in report_html
        assert "Kafka exporter up, consumer lag metric unavailable" in report_html
        assert "Consumer received throughput is lower than broker egress" in report_html
        assert "Target throughput" not in report_html
        assert report_json["bottleneck_analysis"]["network_not_saturated"] is True
        assert report_json["bottleneck_analysis"]["bottleneck_flags"]["producer_backlog"] is True
        assert report_json["bottleneck_analysis"]["bottleneck_flags"]["producer_failed_sends"] is True
        assert report_json["bottleneck_analysis"]["bottleneck_flags"]["producer_flush_high"] is True
        assert report_json["bottleneck_analysis"]["bottleneck_flags"]["consumer_drain_gap"] is True
        assert report_json["bottleneck_analysis"]["bottleneck_flags"]["network_saturated"] is False
        assert report_json["bottleneck_analysis"]["bottleneck_flags"]["ram_bandwidth_saturated"] is False
        assert report_json["bottleneck_analysis"]["network_path_comparisons"][0]["avg_utilization_percent"] == 40.0
        assert report_json["bottleneck_analysis"]["system_comparisons"][0]["utilization_percent"] == 20.0
        assert report_json["bottleneck_analysis"]["system_comparisons"][0]["read_gb_s"] == 6.0
        assert report_json["bottleneck_analysis"]["system_comparisons"][0]["write_gb_s"] == 5.5
        assert report_json["throughput_verdict"]["label"] == "Overdriven"


def test_case_comparison_markdown_highlights_key_metrics() -> None:
    base = {
        "case": {"case_id": "case_a", "case_name": "a", "status": "completed"},
        "config": {
            "scenario": "simultaneous",
            "partitions": 120,
            "producer_ranks": 40,
            "consumer_ranks": 40,
            "payload_size_bytes": 4096,
            "extra": {"target_throughput_mb_s": 700},
        },
        "aggregated_metrics": {
            "producers": {
                "throughput_bytes_per_sec": 550.0 * 1_048_576,
                "throughput_megabytes_per_sec": 550.0,
                "send_attempt_throughput_bytes_per_sec": 825.0 * 1_048_576,
                "send_attempt_throughput_megabytes_per_sec": 825.0,
                "messages_failed": 5,
                "messages_attempted": 1000,
                "pending_messages_at_flush_start": 40,
                "messages_enqueued": 1000,
                "max_flush_duration_sec": 12.0,
            },
            "consumers": {
                "throughput_bytes_per_sec": 460.0 * 1_048_576,
                "throughput_megabytes_per_sec": 460.0,
            },
        },
        "bottleneck_analysis": {
            "primary_conclusion": "pipeline",
            "network_path_comparisons": [
                {"path": "producer_to_broker", "capacity_mb_s": 2000.0},
                {"path": "broker_to_consumer", "capacity_mb_s": 1800.0},
            ],
            "throughput_chain": [
                {"stage": "Broker ingress avg", "mb_s": 820.0},
                {"stage": "Broker egress avg", "mb_s": 660.0},
            ],
            "broker_combined_avg_mb_s": 1480.0,
        },
    }
    other = json.loads(json.dumps(base))
    other["case"]["case_id"] = "case_b"
    other["aggregated_metrics"]["producers"]["throughput_bytes_per_sec"] = 480.0 * 1_048_576
    other["aggregated_metrics"]["producers"]["throughput_megabytes_per_sec"] = 480.0
    other["aggregated_metrics"]["consumers"]["throughput_bytes_per_sec"] = 430.0 * 1_048_576
    other["aggregated_metrics"]["consumers"]["throughput_megabytes_per_sec"] = 430.0

    markdown = build_comparison_markdown([base, other])
    html = build_comparison_html([base, other])

    assert "Kafka Benchmark Case Comparison" in markdown
    assert "Producer delivered MiB/s" in markdown
    assert "550.0" in markdown
    assert "480.0" in markdown
    assert "Broker combined avg MiB/s" in markdown
    assert "Case Comparison" in html
    assert "Sustained verdict" in html
    assert "Overdriven" in html


def test_report_broker_jmx_falls_back_to_one_minute_rates() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        monitoring = {
            "enabled": True,
            "status": "completed",
            "collected_metrics": [
                {
                    "id": "kafka_jmx_bytes_in_counter_rate",
                    "title": "Kafka Broker Bytes In Rate",
                    "category": "kafka_jmx",
                    "unit": "bytes/sec",
                    "avg_value": 0.0,
                    "max_value": 0.0,
                    "series": [],
                },
                {
                    "id": "kafka_jmx_bytes_out_counter_rate",
                    "title": "Kafka Broker Bytes Out Rate",
                    "category": "kafka_jmx",
                    "unit": "bytes/sec",
                    "avg_value": 0.0,
                    "max_value": 0.0,
                    "series": [],
                },
                {
                    "id": "kafka_jmx_bytes_total_counter_rate",
                    "title": "Kafka Broker Combined Bytes In+Out Rate",
                    "category": "kafka_jmx",
                    "unit": "bytes/sec",
                    "avg_value": 0.0,
                    "max_value": 0.0,
                    "series": [],
                },
                {
                    "id": "kafka_jmx_bytes_in_one_minute_rate",
                    "title": "Kafka Broker Bytes In One-Minute Rate",
                    "category": "kafka_jmx",
                    "unit": "bytes/sec",
                    "avg_value": 400000000.0,
                    "max_value": 600000000.0,
                    "series": [],
                },
                {
                    "id": "kafka_jmx_bytes_out_one_minute_rate",
                    "title": "Kafka Broker Bytes Out One-Minute Rate",
                    "category": "kafka_jmx",
                    "unit": "bytes/sec",
                    "avg_value": 200000000.0,
                    "max_value": 300000000.0,
                    "series": [],
                },
            ],
            "graphs": [],
            "missing_metrics": [],
        }
        result = {
            "case": {"case_id": "fallback_case", "case_name": "fallback", "status": "completed"},
            "config": BenchmarkConfig(
                scenario="simultaneous",
                broker_count=1,
                replication_factor=1,
                producer_ranks=1,
                consumer_ranks=1,
            ).to_dict(),
            "scenario_execution": {"active_roles": ["producer", "consumer"]},
            "aggregated_metrics": {
                "producers": {"messages_delivered": 1, "throughput_megabytes_per_sec": 100.0},
                "consumers": {"messages_received": 1, "throughput_megabytes_per_sec": 100.0},
            },
            "kafka_broker_throughput": {"enabled": True, "status": "collected", "metrics": monitoring["collected_metrics"]},
            "monitoring": monitoring,
        }

        report_html = build_html_report(output_dir, result)
        assert "Broker ingress avg" in report_html
        assert "381.5 MiB/s" in report_html
        assert "Broker combined avg" in report_html
        assert "572.2 MiB/s" in report_html
        assert math.isclose(
            result["bottleneck_analysis"]["broker_combined_avg_mb_s"],
            600_000_000.0 / 1_048_576.0,
        )


def test_system_inventory_parsers() -> None:
    iperf = parse_iperf3_json(
        {
            "start": {"connected": [{}, {}, {}, {}]},
            "end": {
                "sum_received": {
                    "bits_per_second": 8_000_000_000,
                    "bytes": 1_000_000_000,
                    "seconds": 1.0,
                }
            },
        }
    )
    assert iperf["gbit_per_second"] == 8.0
    assert iperf["megabytes_per_second"] == 1000.0
    assert iperf["parallel_streams"] == 4
    assert iperf["summary_source"] == "sum_received"

    timeout_skewed = parse_iperf3_json(
        {
            "start": {"connected": [{}, {}, {}, {}]},
            "end": {
                "sum_received": {
                    "bits_per_second": 4_000_000_000,
                    "bytes": 20_000_000_000,
                    "seconds": 40.0,
                },
                "sum_sent": {
                    "bits_per_second": 16_000_000_000,
                    "bytes": 20_000_000_000,
                    "seconds": 10.0,
                },
            },
        },
        requested_seconds=10,
    )
    assert timeout_skewed["summary_source"] == "sum_sent"
    assert timeout_skewed["gbit_per_second"] == 16.0
    assert timeout_skewed["measured_seconds"] == 10.0

    stream = parse_stream_probe_output(
        json.dumps(
            {
                "status": "completed",
                "metrics": {
                    "read_gb_s": "88.5",
                    "write_gb_s": 89,
                    "copy_gb_s": "100.5",
                    "scale_gb_s": 101,
                    "add_gb_s": 102,
                    "triad_gb_s": 103,
                },
            }
        )
    )
    assert stream["metrics"]["read_gb_s"] == 88.5
    assert stream["metrics"]["write_gb_s"] == 89.0
    assert stream["metrics"]["copy_gb_s"] == 100.5
    assert stream["measurement_scope"] == "single_process_stream_style"
    assert "pure single-process" in stream["read_write_note"]

    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        raw = root / "iperf.raw.json"
        out = root / "iperf.json"
        raw.write_text(
            "[hpc-modules] loading python\n"
            + json.dumps(
                {
                    "start": {"connected": [{}, {}, {}, {}]},
                    "end": {"sum_received": {"bits_per_second": 8_000_000_000, "seconds": 10.0}},
                }
            ),
            encoding="utf-8",
        )
        normalized = normalize_iperf_record(
            raw_input=raw,
            output=out,
            label="producer_to_broker",
            source_node="producer",
            target_node="broker",
            target_address="10.0.0.1",
            port=5201,
            seconds=10,
            parallel=4,
        )
        assert normalized["status"] == "completed"
        assert normalized["gbit_per_second"] == 8.0
        assert normalized["summary_source"] == "sum_received"


def test_system_inventory_merge_marks_failed_required_iperf() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        nodes_dir = root / "nodes"
        iperf_dir = root / "iperf"
        nodes_dir.mkdir()
        iperf_dir.mkdir()
        (nodes_dir / "broker.json").write_text(
            json.dumps({"node": "gcn0001", "role": "broker"}),
            encoding="utf-8",
        )
        (iperf_dir / "producer_to_broker.json").write_text(
            json.dumps(
                {
                    "label": "producer_to_broker",
                    "source_node": "gcn0002",
                    "target_node": "gcn0001",
                    "status": "failed",
                    "reason": "client failed",
                }
            ),
            encoding="utf-8",
        )

        inventory = merge_inventory(
            nodes_dir=nodes_dir,
            iperf_dir=iperf_dir,
            output=root / "system_inventory.json",
            settings={"iperf_enabled": True},
        )

        assert inventory["status"] == "partial"
        assert inventory["probe_summary"]["failed_required_probe_count"] == 2
        assert "producer_to_broker" in inventory["probe_summary"]["failed_required_network_tests"]
        assert "broker_to_consumer" in inventory["probe_summary"]["missing_required_network_tests"]


def test_monitoring_svg_embeds_event_markers() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output = Path(tmpdir) / "graph.svg"
        payload = {
            "status": "success",
            "data": {
                "result": [
                    {
                        "metric": {"node": "gcn0001", "role": "broker"},
                        "values": [[1800000000.0, "1"], [1800000015.0, "4"]],
                    }
                ]
            },
        }
        plotted, reason = plot_metric_svg(
            payload,
            output,
            title="Node CPU Busy Percent",
            ylabel="percent",
            events=[
                {
                    "name": "kafka_start_requested",
                    "label": "Kafka broker start requested",
                    "unix": 1800000002.0,
                }
            ],
        )

        assert plotted, reason
        svg = output.read_text(encoding="utf-8")
        assert "event-line" in svg
        assert "Kafka broker start" in svg
        assert "Event key:" in svg
        assert "gcn0001 broker" in svg


def test_monitoring_bundle_report_has_clean_exporter_sections() -> None:
    bundle = {
        "format": "monitoring_bundle.v1",
        "enabled": True,
        "status": "completed",
        "prometheus_url": "http://monitoring:9090",
        "created_at": "2026-06-10T00:00:00+00:00",
        "time_window": {
            "start": "2026-06-10T00:00:00+00:00",
            "end": "2026-06-10T00:02:00+00:00",
            "step_sec": 15,
        },
        "summary_path": "monitoring/monitoring_summary.json",
        "raw_dir": "monitoring/raw",
        "csv_dir": "monitoring/csv",
        "graphs_dir": "monitoring/graphs",
        "targets": {
            "health_counts": {"up": 3},
            "active_count": 3,
            "dropped_count": 0,
        },
        "node_roles": {
            "format": "node_roles.v1",
            "nodes": [
                {
                    "node": "gcn0001",
                    "service_address": "10.0.0.1",
                    "primary_role": "broker",
                    "mpi_ranks": [],
                    "exporters": [
                        {"name": "jmx", "endpoint": "10.0.0.1:7101"},
                        {"name": "node", "endpoint": "10.0.0.1:9100"},
                    ],
                },
                {
                    "node": "gcn0002",
                    "service_address": "10.0.0.2",
                    "primary_role": "producer_controller",
                    "mpi_ranks": [
                        {"rank": 0, "role": "controller"},
                        {"rank": 1, "role": "producer"},
                    ],
                    "exporters": [
                        {"name": "node", "endpoint": "10.0.0.2:9100"},
                    ],
                },
            ],
        },
        "collected_metrics": [
            {
                "id": "node_cpu_busy_percent",
                "title": "Node CPU Busy Percent",
                "category": "system",
                "unit": "percent",
                "sample_count": 4,
                "series": [
                    {
                        "labels": {"node": "gcn0001", "role": "broker"},
                        "sample_count": 2,
                        "avg_value": 41.2,
                        "max_value": 65.4,
                    }
                ],
            },
            {
                "id": "node_memory_total_gb",
                "title": "Node Memory Total",
                "category": "system",
                "unit": "GB",
                "sample_count": 4,
                "series": [
                    {
                        "labels": {"node": "gcn0001", "role": "broker"},
                        "sample_count": 2,
                        "max_value": 540.0,
                    }
                ],
            },
            {
                "id": "kafka_jmx_bytes_in_counter_rate",
                "title": "Kafka Broker Bytes In Rate",
                "category": "kafka_jmx",
                "unit": "bytes/sec",
                "sample_count": 2,
                "avg_value": 500000000.0,
                "max_value": 800000000.0,
                "series": [
                    {
                        "labels": {"node": "gcn0001", "role": "broker"},
                        "sample_count": 2,
                        "avg_value": 500000000.0,
                        "max_value": 800000000.0,
                    }
                ],
            },
            {
                "id": "kafka_exporter_brokers",
                "title": "Kafka Exporter Broker Count",
                "category": "kafka_exporter",
                "unit": "brokers",
                "sample_count": 1,
                "avg_value": 1,
                "max_value": 1,
                "series_count": 1,
            },
        ],
        "missing_metrics": [],
        "graphs": [],
        "errors": [],
    }

    summary = build_monitoring_summary(bundle)
    markdown = build_monitoring_markdown_section(summary)
    broker_throughput = extract_kafka_broker_throughput(summary)

    assert "### Node Role Map" in markdown
    assert "### Node Exporter" in markdown
    assert "### JMX Exporter" in markdown
    assert "### Kafka Exporter" in markdown
    assert "Avg CPU %" in markdown
    assert "RAM Total GB" in markdown
    assert "gcn0001" in markdown
    assert broker_throughput["metrics"][0]["series"][0]["labels"]["node"] == "gcn0001"


def test_v2_config_envelope_and_rate_contracts() -> None:
    config = BenchmarkConfig(
        scenario="simultaneous",
        producer_ranks=4,
        consumer_ranks=4,
        payload_mode="fixed_size",
        payload_size_bytes=1024,
        warmup_sec=30,
        duration_sec=180,
        drain_timeout_sec=60,
        target_records_per_sec=2000.0,
        latency_enabled=True,
        latency_sample_every=10,
        broker_profile_id="B0",
        broker_profile_sha256="a" * 64,
    )
    assert config.record_envelope_enabled
    assert config.total_mpi_ranks == 9
    controller = SteadySendPatternController(config)
    assert controller.records_per_sec == 500.0

    invalid = dict(config.to_input_dict())
    invalid["payload_mode"] = "standard"
    try:
        BenchmarkConfig(**invalid)
    except ValueError as exc:
        assert "payload_mode='fixed_size'" in str(exc)
    else:
        raise AssertionError("V2 phase instrumentation must require fixed payloads")


def test_record_envelope_preserves_payload_and_detects_corruption() -> None:
    original = b"x" * 4096
    encoded = encode_record_envelope(
        original,
        producer_rank=17,
        sequence=123456,
        send_time_ns=9_876_543_210,
        measurement_record=True,
        latency_sampled=True,
    )
    assert len(encoded) == len(original)
    assert encoded[ENVELOPE_SIZE_BYTES:] == original[ENVELOPE_SIZE_BYTES:]
    decoded = decode_record_envelope(encoded)
    assert decoded.record_id == (17, 123456)
    assert decoded.send_time_ns == 9_876_543_210
    assert decoded.measurement_record
    assert decoded.latency_sampled

    corrupted = bytearray(encoded)
    corrupted[20] ^= 1
    try:
        decode_record_envelope(bytes(corrupted))
    except ValueError as exc:
        assert "checksum" in str(exc)
    else:
        raise AssertionError("corrupt benchmark envelope was accepted")


def test_consumer_offset_checks_exclude_warmup_records() -> None:
    last_offsets: dict[tuple[str, int], int] = {}
    assert update_measurement_offset_order(
        last_offsets,
        measurement_record=False,
        topic="v2-test-topic",
        partition=3,
        offset=10,
    ) == "warmup_ignored"
    assert update_measurement_offset_order(
        last_offsets,
        measurement_record=False,
        topic="v2-test-topic",
        partition=3,
        offset=9,
    ) == "warmup_ignored"
    assert last_offsets == {}

    assert update_measurement_offset_order(
        last_offsets,
        measurement_record=True,
        topic="v2-test-topic",
        partition=3,
        offset=20,
    ) == "ordered"
    assert update_measurement_offset_order(
        last_offsets,
        measurement_record=True,
        topic="v2-test-topic",
        partition=3,
        offset=19,
    ) == "out_of_order"
    assert update_measurement_offset_order(
        last_offsets,
        measurement_record=True,
        topic="v2-test-topic",
        partition=3,
        offset=20,
    ) == "duplicate"


def test_latency_histogram_merge_and_negative_rejection() -> None:
    first = LatencyHistogram()
    second = LatencyHistogram()
    assert first.record_ns(100_000)
    assert first.record_ns(1_000_000)
    assert second.record_ns(10_000_000)
    assert not second.record_ns(-1)
    first.merge(second)
    payload = first.to_dict()
    assert payload["count"] == 3
    assert payload["negative_count"] == 1
    assert payload["p50_ns"] is not None
    assert payload["p99_ns"] is not None
    restored = LatencyHistogram.from_dict(payload)
    assert restored.to_dict()["count"] == 3


def test_clock_offset_estimator_uses_lowest_rtt_bound() -> None:
    calibration = estimate_clock_offset(
        [
            (1_000_000, 900_000, 1_200_000),
            (2_000_000, 1_950_000, 2_020_000),
            (3_000_000, 2_900_000, 3_100_000),
        ],
        rank=7,
    )
    assert calibration.rank == 7
    assert calibration.sample_count == 3
    assert calibration.median_rtt_ns == 100_000
    assert calibration.uncertainty_ns == 10_000
    assert calibration.offset_ns == 60_000


def test_metrics_aggregator_merges_v2_latency_and_drain_evidence() -> None:
    histogram = LatencyHistogram()
    histogram.record_ns(2_000_000)
    aggregated = MetricsAggregator().aggregate_from_dicts(
        producer_metrics_list=[
            {
                "messages_attempted": 10,
                "messages_enqueued": 10,
                "messages_delivered": 10,
                "delivery_callbacks_seen": 10,
                "bytes_delivered": 10240,
                "duration_sec": 1,
                "send_loop_duration_sec": 1,
                "latency_samples_written": 1,
                "latency_samples_enqueued": 1,
                "latency_samples_delivered": 1,
            }
        ],
        consumer_metrics_list=[
            {
                "messages_received": 8,
                "late_drained_messages": 2,
                "bytes_received": 8192,
                "late_drained_bytes": 2048,
                "duration_sec": 1,
                "latency_histogram": histogram.to_dict(),
            }
        ],
    )
    correctness = aggregated["record_correctness"]
    assert correctness["missing_after_drain_records"] == 0
    assert correctness["consumer_received_through_drain_records"] == 10
    assert aggregated["consumers"]["latency_histogram"]["count"] == 1


def _write_synthetic_kafka_phase1_selection(path: Path) -> list[str]:
    config_ids = [f"cfg_{index:03d}" for index in range(1, 11)]
    payload = {
        "format": "messaging-benchmark.kafka-phase1-selection.v1",
        "source_stage": "phase1",
        "source_case_count": 120,
        "shortlist": [
            {
                "config_id": config_id,
                "selection_categories": ["synthetic_test_selection"],
            }
            for config_id in config_ids
        ],
        "v2_profile_anchors": [
            {
                "config_id": "cfg_002",
                "role": "qualified_sustainable_leader",
                "qualification_status": "qualified",
            },
            {
                "config_id": "cfg_003",
                "role": "qualified_sustainable_secondary",
                "qualification_status": "qualified",
            },
            {
                "config_id": "cfg_004",
                "role": "high_record_rate_pressure",
                "qualification_status": "overdriven",
            },
            {
                "config_id": "cfg_005",
                "role": "raw_throughput_upper_bound",
                "qualification_status": "overdriven",
            },
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return config_ids


def test_reproducible_kafka_v1_campaign_manifests() -> None:
    generator = _load_script_module(
        "generate_kafka_reproducible_campaigns_test",
        PROJECT_ROOT / "scripts" / "generate_kafka_reproducible_campaigns.py",
    )
    with tempfile.TemporaryDirectory(dir=PROJECT_ROOT) as tmpdir:
        root = Path(tmpdir) / "kafka_v1_reproducible"
        assert generator.main(["--root", str(root)]) == 0

        plan = json.loads((root / "campaign_plan.json").read_text(encoding="utf-8"))
        assert plan["qualification_policy_id"] == COMMON_QUALIFICATION_POLICY_ID
        assert plan["qualification"]["backlog_denominator"] == (
            "measurement-period send attempts"
        )
        assert plan["phase1"] == {
            "seed": 42,
            "case_count": 120,
            "batch_size": 30,
            "job_count": 4,
        }
        assert plan["timing"]["warmup_sec"] == 15
        assert plan["timing"]["measurement_sec"] == 30
        assert plan["timing"]["drain_timeout_sec"] == 60
        assert plan["case_isolation"]["restart_kafka_between_cases"] is True
        assert plan["instrumentation"]["latency_timestamp_sample_every"] == 10
        assert plan["instrumentation"]["deterministic_sampling_rule"] == (
            "sequence_number modulo 10 equals zero"
        )
        assert plan["instrumentation"]["correctness_identity_scope"] == (
            "every record"
        )
        assert plan["execution_plan"] == {
            "submission_model": (
                "four independent Phase 1 allocations with up to 30 sequential "
                "cases per exclusive four-node Slurm job"
            ),
            "submit_script": "scripts/submit_kafka_reproducible_campaign.sh",
            "wall_time": "02:00:00",
            "phase1_jobs": 4,
            "planned_validation_jobs": 2,
            "kafka_restart_model": "fresh Kafka process and RAM storage per case",
        }
        assert plan["validation"]["case_count"] == 0
        assert plan["validation"]["job_count"] == 0
        assert plan["validation"]["planned_case_count"] == 50
        assert plan["validation"]["planned_job_count"] == 2
        assert plan["validation"]["status"] == "awaiting_new_phase1_results"
        assert not (root / "validation/validation_manifest.csv").exists()

        with (root / "phase1/sweep_manifest.csv").open(
            "r", encoding="utf-8", newline=""
        ) as handle:
            phase1_rows = list(csv.DictReader(handle))
        assert len(phase1_rows) == 120
        assert len({row["config_id"] for row in phase1_rows}) == 120
        assert {row["qualification_policy_id"] for row in phase1_rows} == {
            COMMON_QUALIFICATION_POLICY_ID
        }
        assert {row["measurement_contract_id"] for row in phase1_rows} == {
            generator.MEASUREMENT_CONTRACT_ID
        }
        assert {row["broker_profile_id"] for row in phase1_rows} == {
            "V1_FIXED_B0"
        }
        assert {row["warmup_sec"] for row in phase1_rows} == {"15"}
        assert {row["measurement_sec"] for row in phase1_rows} == {"30"}
        assert {row["drain_timeout_sec"] for row in phase1_rows} == {"60"}
        assert {row["latency_sample_every"] for row in phase1_rows} == {"10"}
        assert [
            sum(1 for _ in csv.DictReader(path.open(encoding="utf-8")))
            for path in sorted((root / "phase1/batches").glob("batch_*.csv"))
        ] == [30, 30, 30, 30]

        varied_fields = (
            "producer_ranks",
            "consumer_ranks",
            "partitions",
            "batch_size",
            "linger_ms",
            "payload_size_bytes",
            "producer_queue_messages",
            "producer_queue_kbytes",
            "consumer_fetch_min_bytes",
            "consumer_fetch_wait_max_ms",
            "consumer_fetch_message_max_bytes",
        )
        with (
            PROJECT_ROOT / "configs/sweeps/simultaneous_budgeted/sweep_manifest.csv"
        ).open("r", encoding="utf-8", newline="") as handle:
            historical_rows = {
                row["config_id"]: row for row in csv.DictReader(handle)
            }
        assert set(historical_rows) == {row["config_id"] for row in phase1_rows}
        for row in phase1_rows:
            historical = historical_rows[row["config_id"]]
            assert {field: row[field] for field in varied_fields} == {
                field: historical[field] for field in varied_fields
            }

        selection_path = root / "phase1_analysis/validation_shortlist.json"
        selected_ids = _write_synthetic_kafka_phase1_selection(selection_path)
        phase1 = root / "phase1"
        phase1_checksums = {
            path.relative_to(phase1).as_posix(): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in phase1.rglob("*")
            if path.is_file()
        }
        assert generator.main(
            [
                "--root",
                str(root),
                "--validation-only",
                "--validation-shortlist",
                str(selection_path),
            ]
        ) == 0
        assert phase1_checksums == {
            path.relative_to(phase1).as_posix(): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in phase1.rglob("*")
            if path.is_file()
        }
        plan = json.loads((root / "campaign_plan.json").read_text(encoding="utf-8"))
        assert plan["validation"]["case_count"] == 50
        assert plan["validation"]["job_count"] == 2
        assert plan["validation"]["status"] == "generated_from_new_phase1_results"

        with (root / "validation/validation_manifest.csv").open(
            "r", encoding="utf-8", newline=""
        ) as handle:
            validation_rows = list(csv.DictReader(handle))
        assert len(validation_rows) == 50
        assert [
            sum(1 for _ in csv.DictReader(path.open(encoding="utf-8")))
            for path in sorted((root / "validation/batches").glob("batch_*.csv"))
        ] == [30, 20]
        for block in range(1, 6):
            block_rows = [
                row for row in validation_rows if int(row["block"]) == block
            ]
            assert len(block_rows) == 10
            assert {row["workload_config_id"] for row in block_rows} == set(selected_ids)
            assert {row["warmup_sec"] for row in block_rows} == {"15"}
            assert {row["measurement_sec"] for row in block_rows} == {"30"}
            assert {row["drain_timeout_sec"] for row in block_rows} == {"60"}
            assert {row["latency_sample_every"] for row in block_rows} == {"10"}
            assert sorted(int(row["order"]) for row in block_rows) == list(
                range(1, 11)
            )

        for line in (root / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
            expected, relative = line.split("  ", 1)
            assert hashlib.sha256((root / relative).read_bytes()).hexdigest() == expected


def test_reproducible_kafka_phase1_selection_is_result_driven() -> None:
    analyzer = _load_script_module(
        "analyze_kafka_reproducible_campaign_selection_test",
        PROJECT_ROOT / "scripts" / "analyze_kafka_reproducible_campaign.py",
    )
    rows: list[dict[str, object]] = []
    for index in range(1, 121):
        qualified = index <= 6
        row = {
            "config_id": f"cfg_{index:03d}",
            "eligible": True,
            "qualification_status": "qualified" if qualified else "overdriven",
            "balanced_mib_per_sec": 500.0 - index,
            "balanced_records_per_sec": 100_000.0 + index,
            "pending_backlog_percent": 1.0 if qualified else 5.0 + index / 100.0,
            "flush_sec": 1.0,
            "failed_send_percent": 0.0,
            "payload_size_bytes": 4096,
            "primary_rank": index,
            "raw_throughput_rank": index,
            "manifest_note": (
                "baseline"
                if index == 1
                else "one-factor:producer_ranks" if index == 7 else "mixed"
            ),
        }
        rows.append(row)

    shortlist, anchors = analyzer._select_phase1_workloads(rows)
    assert len(shortlist) == 10
    assert len({row["config_id"] for row in shortlist}) == 10
    assert [row["selection_order"] for row in shortlist] == list(range(1, 11))
    assert len(anchors) == 4
    assert len({row["config_id"] for row in anchors}) == 4
    assert anchors[0]["role"] == "qualified_sustainable_leader"
    assert anchors[1]["role"] == "qualified_sustainable_secondary"
    assert all(
        row["qualification_status"] == "qualified" for row in anchors[:2]
    )


def test_reproducible_kafka_measurement_and_case_isolation_contract() -> None:
    reproducible_config = load_benchmark_config(
        PROJECT_ROOT
        / "configs/campaigns/kafka/v1_reproducible/phase1/generated_configs/cfg_001.json"
    )
    assert get_backend("kafka").uses_coordinated_consumer_drain(
        reproducible_config
    ) is True

    producer = (PROJECT_ROOT / "src/benchmark/producer_worker.py").read_text(
        encoding="utf-8"
    )
    assert (
        "generated.sequence_number % self.config.latency_sample_every == 0"
        in producer
    )
    assert "producer_rank=self.rank" in producer
    assert "sequence=generated.sequence_number" in producer
    snapshot = "self.metrics.pending_messages_at_flush_start = max("
    flush = "flush_remaining = self.producer.flush(timeout=30.0)"
    assert producer.index("self.metrics.send_loop_end_time_unix = time.time()") < producer.index(
        snapshot
    )
    assert producer.index(snapshot) < producer.index(flush)
    assert "def run_send_phase(" in producer
    assert "def flush_and_close(" in producer

    consumer = (PROJECT_ROOT / "src/benchmark/consumer_worker.py").read_text(
        encoding="utf-8"
    )
    assert "def run_measurement_phase(" in consumer
    assert "def poll_during_producer_flush(" in consumer
    assert "def begin_post_flush_drain(" in consumer
    assert "def complete_post_flush_drain(" in consumer

    controller = (
        PROJECT_ROOT / "src/benchmark/benchmark_controller.py"
    ).read_text(encoding="utf-8")
    assert "producer.run_send_phase()" in controller
    assert "producer.flush_and_close()" in controller
    assert "consumer.poll_during_producer_flush" in controller
    assert "delivered_target_records=total_delivered" in controller

    submitter = (
        PROJECT_ROOT / "scripts/submit_simultaneous_budgeted_batch.sh"
    ).read_text(encoding="utf-8")
    runner = (PROJECT_ROOT / "scripts/run_simultaneous_budgeted_batch.sh").read_text(
        encoding="utf-8"
    )
    assert "KAFKA_RESTART_PER_CASE" in submitter
    assert "reproducible Kafka timing must be 15/30/60 seconds" in submitter
    assert "--exclusive" in submitter
    assert '--dependency="$SLURM_DEPENDENCY"' in submitter
    assert "SWEEP_BATCH_DIR_OVERRIDE" in submitter
    assert "ENABLE_BROKER_PROCESS_MONITOR" in submitter
    assert "reset_case_broker_storage" in runner
    assert "start_case_kafka" in runner
    assert "stop_case_kafka" in runner
    assert "start_case_broker_process_monitor" in runner
    assert "stop_case_broker_process_monitor" in runner
    assert "unset SLURM_EXCLUSIVE" in runner
    assert 'if not config_id:' in runner
    assert 'value.rstrip("\\r")' in runner
    assert 'backend_lifecycle.sh" kafka snapshot "$CONFIG_PATH"' in runner
    assert 'backend_lifecycle.sh" kafka check-health "$CONFIG_PATH"' in runner
    assert runner.index("reset_case_broker_storage()") < runner.index("start_case_kafka()")
    assert runner.index("start_case_kafka") < runner.index(
        '"$SCRIPT_DIR/create_topics.sh"'
    )
    assert runner.index(
        'backend_lifecycle.sh" kafka check-health "$CONFIG_PATH"'
    ) < runner.index('"$SCRIPT_DIR/collect_results.sh"')
    assert "CLEAN_BROKER_RAM_DIRS=0" in runner
    assert 'rm -rf ${q_storage}' in runner

    monitoring = (PROJECT_ROOT / "scripts/start_monitoring.sh").read_text(
        encoding="utf-8"
    )
    fallback = 'SERVICE_NODE_COUNT="${SERVICE_NODE_COUNT:-${BROKER_COUNT:-}}"'
    assert fallback in monitoring
    assert monitoring.index(fallback) < monitoring.index(
        'split_nodes "$SERVICE_NODE_COUNT"'
    )
    assert 'PROMETHEUS_READY_TIMEOUT_SEC="${PROMETHEUS_READY_TIMEOUT_SEC:-120}"' in monitoring

    profile_renderer = PROJECT_ROOT / "scripts/render_broker_profile_env.py"
    rendered_profile = subprocess.run(
        [
            sys.executable,
            "-B",
            str(profile_renderer),
            "--config",
            str(
                PROJECT_ROOT
                / "configs/campaigns/kafka/v1_reproducible/phase1/generated_configs/cfg_001.json"
            ),
            "--project-root",
            str(PROJECT_ROOT),
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    assert "BROKER_PROFILE_ID=V1_FIXED_B0" in rendered_profile
    assert "BROKER_PROFILE_SHA256=" in rendered_profile

    v1_submitter = (
        PROJECT_ROOT / "scripts/submit_kafka_reproducible_campaign.sh"
    ).read_text(encoding="utf-8")
    assert "submit_simultaneous_budgeted_batch.sh" in v1_submitter
    assert "submit_hpc_case.sh" not in v1_submitter
    assert "SWEEP_MAX_CASES=30" in v1_submitter

    v2_submitter = (
        PROJECT_ROOT / "scripts/submit_broker_tuning_v2_campaign.sh"
    ).read_text(encoding="utf-8")
    assert "submit_simultaneous_budgeted_batch.sh" in v2_submitter
    assert "submit_hpc_case.sh" not in v2_submitter
    assert "SWEEP_MAX_CASES=30" in v2_submitter


def test_broker_profile_hash_and_campaign_manifests() -> None:
    generator = _load_script_module(
        "generate_broker_tuning_v2",
        PROJECT_ROOT / "scripts" / "generate_broker_tuning_v2.py",
    )
    with tempfile.TemporaryDirectory(dir=PROJECT_ROOT) as tmpdir:
        root = Path(tmpdir) / "tuning_v2"
        generator.initialize(root)
        assert not (root / "README.md").exists()
        blueprint = json.loads(
            (root / "campaign_blueprint.json").read_text(encoding="utf-8")
        )
        assert blueprint["case_budget"]["maximum_campaign_total"] == 119
        assert blueprint["latency_sampling"] == {
            "timestamp_sample_every": 10,
            "rule": "sequence_number modulo 10 equals zero",
            "record_identity_and_correctness_scope": "every record",
            "fixed_to_match_pulsar": True,
        }
        assert blueprint["timing"] == {
            "warmup_sec": 15,
            "measurement_sec": 30,
            "rate_calibration_measurement_sec": 30,
            "profile_measurement_overrides_sec": {},
            "drain_timeout_sec": 60,
            "producer_stop_at_measurement_end": True,
            "pending_callbacks_snapshotted_before_flush": True,
            "producer_flush_before_consumer_drain": True,
        }
        assert blueprint["case_isolation"] == {
            "restart_kafka_between_cases": True,
            "clean_ram_backed_broker_storage_between_cases": True,
        }
        assert blueprint["execution_plan"] == {
            "submission_model": (
                "up to 30 sequential cases per exclusive four-node Slurm job"
            ),
            "batch_size_limit": 30,
            "kafka_restart_model": "fresh Kafka process and RAM storage per case",
            "instrumentation_pilot_jobs": 1,
            "rate_calibration_jobs": 1,
            "profile_screening_jobs": 1,
            "profile_confirmation_jobs": 1,
            "final_validation_jobs": 2,
            "maximum_total_jobs": 6,
        }

        profiles = sorted((root / "broker_profiles").glob("B*.json"))
        assert len(profiles) == 6
        for path in profiles:
            profile = load_broker_profile(path)
            assert profile.payload["fixed_semantics"]["java_major_version"] == 17
            assert profile.payload["fixed_semantics"]["garbage_collector"] == "G1"

        with (root / "campaigns/instrumentation_pilot.csv").open(
            "r",
            encoding="utf-8",
            newline="",
        ) as handle:
            pilot_rows = list(csv.DictReader(handle))
        with (root / "campaigns/rate_calibration.csv").open(
            "r",
            encoding="utf-8",
            newline="",
        ) as handle:
            rate_rows = list(csv.DictReader(handle))
        assert len(pilot_rows) == 6
        assert len(rate_rows) == 3
        assert sum(
            1
            for _ in csv.DictReader(
                (root / "campaigns/batches/instrumentation_pilot/batch_001.csv").open(
                    encoding="utf-8"
                )
            )
        ) == 6
        assert sum(
            1
            for _ in csv.DictReader(
                (root / "campaigns/batches/rate_calibration/batch_001.csv").open(
                    encoding="utf-8"
                )
            )
        ) == 3
        assert {row["block"] for row in pilot_rows} == {"1", "2", "3"}
        assert all(row["broker_profile_id"] == "B0" for row in pilot_rows)
        assert {row["latency_sample_every"] for row in pilot_rows} == {"10"}
        assert {row["latency_sample_every"] for row in rate_rows} == {"10"}
        first_pilot = json.loads(
            (PROJECT_ROOT / pilot_rows[0]["config_path"]).read_text(
                encoding="utf-8"
            )
        )
        assert first_pilot["schema_version"] == "messaging-benchmark.case.v1"
        assert first_pilot["backend_id"] == "kafka"
        for row in pilot_rows + rate_rows:
            config = load_benchmark_config(PROJECT_ROOT / row["config_path"])
            assert config.warmup_sec == 15
            assert config.duration_sec == 30
            assert config.drain_timeout_sec == 60

        confirmation_case = root / "synthetic_confirmation"
        runtime_dir = confirmation_case / "runtime"
        config_dir = runtime_dir / "brokers/configs"
        config_dir.mkdir(parents=True)
        selected_profile = load_broker_profile(
            root / "broker_profiles/B1.json"
        )
        (runtime_dir / "broker_profile_snapshot.json").write_bytes(
            selected_profile.path.read_bytes()
        )
        server_path = config_dir / "server-1.properties"
        server_path.write_text("node.id=1\n", encoding="utf-8")
        server_hash = hashlib.sha256(server_path.read_bytes()).hexdigest()
        (runtime_dir / "broker_runtime_manifest.json").write_text(
            json.dumps(
                {
                    "profile_id": "B1",
                    "profile_sha256": selected_profile.sha256,
                    "java_version": 'openjdk version "17.0.15"',
                    "kafka_version": "3.9.0",
                    "jvm_command_contract": (
                        "kafka-server-start.sh <generated-server.properties>"
                    ),
                    "server_properties": [
                        {"path": str(server_path), "sha256": server_hash}
                    ],
                }
            ),
            encoding="utf-8",
        )
        (confirmation_case / "final_report.json").write_text(
            json.dumps(
                {
                    "case": {
                        "case_id": "confirm_cfg074_B1",
                        "status": "completed",
                    },
                    "config": {
                        "case_id": "confirm_cfg074_B1",
                        "broker_profile_id": "B1",
                        "extra": {"v2_stage": "profile_confirmation"},
                    },
                    "latency_validation": {"valid": True},
                    "aggregated_metrics": {
                        "producers": {"flush_remaining_messages": 0},
                        "record_correctness": {
                            "missing_after_drain_records": 0,
                            "unexplained_surplus_records": 0,
                            "invalid_envelope_count": 0,
                            "duplicate_offset_count": 0,
                            "out_of_order_offset_count": 0,
                        },
                    },
                }
            ),
            encoding="utf-8",
        )
        phase1_selection = root / "phase1_selection.json"
        selected_ids = _write_synthetic_kafka_phase1_selection(phase1_selection)
        generator.prepare_final_validation(
            root,
            winner="B1",
            confirmation_results=confirmation_case,
            workload_shortlist=phase1_selection,
            latency_sample_every=10,
        )
        with (root / "campaigns/final_validation.csv").open(
            "r",
            encoding="utf-8",
            newline="",
        ) as handle:
            final_rows = list(csv.DictReader(handle))
        assert len(final_rows) == 50
        assert [
            sum(1 for _ in csv.DictReader(path.open(encoding="utf-8")))
            for path in sorted(
                (root / "campaigns/batches/final_validation").glob("batch_*.csv")
            )
        ] == [30, 20]
        for row in final_rows:
            config = load_benchmark_config(PROJECT_ROOT / row["config_path"])
            assert config.warmup_sec == 15
            assert config.duration_sec == 30
            assert config.drain_timeout_sec == 60
        for block in range(1, 6):
            block_ids = {
                row["anchor_id"]
                for row in final_rows
                if int(row["block"]) == block
            }
            assert block_ids == set(selected_ids)
        assert load_broker_profile(
            root / "frozen/frozen_broker_profile.json"
        ).profile_id == "B1"
        assert (
            load_broker_profile(
                root / "frozen/frozen_broker_profile.json"
            ).payload["frozen_runtime_evidence"]["kafka_version"]
            == "3.9.0"
        )


def test_v2_screening_manifest_is_deterministic_and_complete() -> None:
    generator = _load_script_module(
        "generate_broker_tuning_v2_screening",
        PROJECT_ROOT / "scripts" / "generate_broker_tuning_v2.py",
    )
    with tempfile.TemporaryDirectory(dir=PROJECT_ROOT) as tmpdir:
        root = Path(tmpdir) / "tuning_v2"
        results = Path(tmpdir) / "calibration"
        generator.initialize(root)
        phase1_selection = root / "phase1_selection.json"
        _write_synthetic_kafka_phase1_selection(phase1_selection)
        for block, rate in enumerate((100_000.0, 110_000.0, 120_000.0), start=1):
            case_dir = results / f"rate-{block}"
            case_dir.mkdir(parents=True)
            report = {
                "case": {"status": "completed"},
                "config": {
                    "extra": {
                        "v2_stage": "rate_calibration",
                        "v2_case_id": f"rate-{block}",
                    },
                    "duration_sec": 30,
                    "latency_sample_every": 10,
                },
                "aggregated_metrics": {
                    "producers": {
                        "throughput_msgs_per_sec": rate,
                        "flush_remaining_messages": 0,
                    },
                    "consumers": {"throughput_msgs_per_sec": rate - 1_000.0},
                    "record_correctness": {
                        "missing_after_drain_records": 0,
                        "unexplained_surplus_records": 0,
                        "invalid_envelope_count": 0,
                        "duplicate_offset_count": 0,
                        "out_of_order_offset_count": 0,
                    },
                },
                "latency_validation": {"valid": True},
            }
            (case_dir / "final_report.json").write_text(
                json.dumps(report),
                encoding="utf-8",
            )

        generator.prepare_screening(
            root,
            calibration_results=results,
            phase1_selection=phase1_selection,
            latency_sample_every=10,
        )
        manifest = root / "campaigns/profile_screening.csv"
        first_bytes = manifest.read_bytes()
        with manifest.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert len(rows) == 30
        assert sum(
            1
            for _ in csv.DictReader(
                (
                    root
                    / "campaigns/batches/profile_screening/batch_001.csv"
                ).open(encoding="utf-8")
            )
        ) == 30
        expected_anchors = {
            "cfg_002",
            "cfg_003",
            "cfg_004",
            "cfg_005",
            "latency_anchor",
        }
        for anchor in expected_anchors:
            anchor_rows = [
                row for row in rows if row["anchor_id"] == anchor
            ]
            assert {
                row["broker_profile_id"] for row in anchor_rows
            } == set(generator.PROFILE_IDS)
            for row in anchor_rows:
                config = load_benchmark_config(PROJECT_ROOT / row["config_path"])
                assert config.warmup_sec == 15
                assert config.duration_sec == 30
                assert config.drain_timeout_sec == 60
                assert config.latency_sample_every == 10
        latency_rows = [
            row for row in rows if row["anchor_id"] == "latency_anchor"
        ]
        assert len(latency_rows) == 6
        assert all(
            float(row["target_records_per_sec"]) == 87_200.0
            for row in latency_rows
        )

        generator.prepare_screening(
            root,
            calibration_results=results,
            phase1_selection=phase1_selection,
            latency_sample_every=10,
        )
        assert manifest.read_bytes() == first_bytes

        generator.prepare_confirmation(
            root,
            profile_ids=["B1", "B2"],
            fixed_rate=87_200.0,
            latency_sample_every=10,
        )
        with (root / "campaigns/profile_confirmation.csv").open(
            "r", encoding="utf-8", newline=""
        ) as handle:
            confirmation_rows = list(csv.DictReader(handle))
        assert len(confirmation_rows) == 30
        assert {row["latency_sample_every"] for row in confirmation_rows} == {"10"}


def test_v2_analysis_prefers_newest_case_attempt() -> None:
    analyzer = _load_script_module(
        "analyze_broker_tuning_v2_attempts",
        PROJECT_ROOT / "scripts" / "analyze_broker_tuning_v2.py",
    )
    with tempfile.TemporaryDirectory(dir=PROJECT_ROOT) as tmpdir:
        root = Path(tmpdir)

        def write_attempt(directory: Path, timestamp: float) -> None:
            report = {
                "config": {
                    "extra": {
                        "v2_case_id": "screen_b04_o01_cfg_119_B5",
                    }
                },
                "timeline": {"events": [{"unix": timestamp}]},
            }
            directory.mkdir(parents=True)
            (directory / "final_report.json").write_text(
                json.dumps(report),
                encoding="utf-8",
            )

        write_attempt(root / "old/profile_screening/case", 100.0)
        write_attempt(root / "new/profile_screening/case/data", 200.0)
        write_attempt(root / "new/profile_screening/case", 200.0)
        reports = analyzer.discover_reports(root)
        assert len(reports) == 1
        assert reports[0][0] == (
            root / "new/profile_screening/case/final_report.json"
        )


def test_v2_final_ranking_keeps_latency_secondary() -> None:
    analyzer = _load_script_module(
        "analyze_broker_tuning_v2_final_rank",
        PROJECT_ROOT / "scripts" / "analyze_broker_tuning_v2.py",
    )

    def repeat_row(
        config_id: str,
        *,
        eligible: bool,
        qualified: bool,
        throughput: float,
        p99_ms: float,
    ) -> dict[str, Any]:
        return {
            "stage": "final_validation",
            "anchor_id": config_id,
            "eligible": eligible,
            "qualified": qualified,
            "balanced_mibps": throughput,
            "balanced_records_per_sec": throughput * 256,
            "latency_p50_ms": p99_ms / 2,
            "latency_p95_ms": p99_ms * 0.9,
            "latency_p99_ms": p99_ms,
            "latency_p999_ms": p99_ms * 1.1,
            "latency_mean_ms": p99_ms * 0.6,
            "latency_max_ms": p99_ms * 1.2,
            "latency_sample_count": 1000,
            "clock_uncertainty_us_max": 100.0,
            "clock_drift_us_max": 100.0,
            "pending_backlog_percent": 1.0,
            "flush_sec": 0.5,
            "failed_send_percent": 0.0,
            "cpu_allocated_percent_mean": 10.0,
            "kafka_jmx_bytes_in_counter_rate_mean": throughput * 1_048_576,
            "kafka_jmx_bytes_out_counter_rate_mean": throughput * 1_048_576,
            "jvm_heap_used_gb_mean": 4.0,
            "kafka_to_iperf_capacity_ratio": 0.1,
            "tmpfs_used_percent_max": 20.0,
        }

    rows: list[dict[str, Any]] = []
    for repeat in range(5):
        rows.append(
            repeat_row(
                "cfg_fast_high_latency",
                eligible=repeat < 4,
                qualified=repeat < 4,
                throughput=200.0,
                p99_ms=9000.0,
            )
        )
        rows.append(
            repeat_row(
                "cfg_more_eligible_low_latency",
                eligible=True,
                qualified=repeat < 4,
                throughput=190.0,
                p99_ms=1.0,
            )
        )

    summaries = analyzer.final_validation_summary(rows)
    assert summaries[0]["config_id"] == "cfg_fast_high_latency"
    assert summaries[0]["qualified_repeats"] == 4
    assert summaries[0]["eligible_repeats"] == 4
    latex = analyzer.latex_report([], {}, summaries)
    assert "Qualified/total" in latex
    assert "Median p99 latency is secondary evidence" in latex


def test_v2_runtime_java_version_contract() -> None:
    verifier = _load_script_module(
        "verify_broker_runtime_profile",
        PROJECT_ROOT / "scripts" / "verify_broker_runtime_profile.py",
    )
    assert verifier.parse_java_major('openjdk version "17.0.15" 2025-04-15') == 17
    assert verifier.parse_java_major('java version "17.0.12"') == 17
    assert verifier.parse_java_major('openjdk version "21.0.4"') == 21
    try:
        verifier.parse_java_major("unexpected Java banner")
    except ValueError as exc:
        assert "could not parse" in str(exc)
    else:
        raise AssertionError("unparseable Java version was accepted")


def test_v2_iperf_capacity_extraction_is_directional() -> None:
    analyzer = _load_script_module(
        "analyze_broker_tuning_v2",
        PROJECT_ROOT / "scripts" / "analyze_broker_tuning_v2.py",
    )
    capacities = analyzer.extract_iperf_capacity(
        {
            "system_inventory": {
                "network_tests": [
                    {
                        "label": "producer_to_broker",
                        "status": "completed",
                        "megabytes_per_second": 2000.5,
                    },
                    {
                        "label": "broker_to_consumer",
                        "status": "completed",
                        "megabytes_per_second": 1800.25,
                    },
                    {
                        "label": "ignored",
                        "status": "completed",
                        "megabytes_per_second": 9999,
                    },
                ]
            }
        }
    )
    assert capacities["producer_to_broker_iperf_MBps"] == 2000.5
    assert capacities["broker_to_consumer_iperf_MBps"] == 1800.25


def test_v2_fixed_sampling_pilot_uses_paired_overhead() -> None:
    analyzer = _load_script_module(
        "analyze_broker_tuning_v2_fixed_sampling",
        PROJECT_ROOT / "scripts" / "analyze_broker_tuning_v2.py",
    )
    rows: list[dict[str, Any]] = []
    for block in (1, 2, 3):
        rows.extend(
            [
                {
                    "stage": "instrumentation_pilot",
                    "eligible": True,
                    "latency_enabled": False,
                    "latency_sample_every": 10,
                    "balanced_mibps": 100.0,
                    "block": block,
                },
                {
                    "stage": "instrumentation_pilot",
                    "eligible": True,
                    "latency_enabled": True,
                    "latency_sample_every": 10,
                    "balanced_mibps": 98.0,
                    "block": block,
                },
            ]
        )
    decision = analyzer.instrumentation_decision(rows)
    assert decision is not None
    assert decision["sampling_decision_valid"] is True
    assert decision["latency_sample_every"] == 10
    assert decision["throughput_overhead_fraction"] == 0.02
    assert decision["throughput_overhead_within_threshold"] is True
    assert len(decision["paired_overhead_fractions"]) == 3


def test_v2_interim_report_populates_observed_case_tables() -> None:
    analyzer = _load_script_module(
        "analyze_broker_tuning_v2_interim_report",
        PROJECT_ROOT / "scripts" / "analyze_broker_tuning_v2.py",
    )
    row = {
        "stage": "profile_screening",
        "block": 4,
        "order": 2,
        "broker_profile_id": "B4",
        "anchor_id": "cfg_119",
        "producer_ranks": 64,
        "consumer_ranks": 64,
        "partitions": 90,
        "payload_size_bytes": 16384,
        "batch_size_bytes": 1048576,
        "linger_ms": 0,
        "producer_queue_messages": 500000,
        "producer_queue_kib": 1048576,
        "consumer_fetch_min_bytes": 8388608,
        "consumer_fetch_wait_ms": 100,
        "consumer_fetch_max_bytes": 33554432,
        "target_records_per_sec": None,
        "warmup_sec": 30,
        "measurement_sec": 20,
        "drain_timeout_sec": 60,
        "eligible": False,
        "qualified": False,
        "balanced_mibps": 662.3203125,
        "balanced_records_per_sec": 42388.5,
        "latency_p50_ms": 20000.0,
        "latency_p95_ms": 38900.0,
        "latency_p99_ms": 45500.0,
        "latency_p999_ms": 51600.0,
        "latency_sample_count": 3105540,
        "latency_delivered_sample_count": 2998700,
        "clock_uncertainty_us_max": 4.79,
        "clock_drift_us_max": 36.13,
        "latency_valid": False,
        "correctness_valid": False,
        "resources_valid": True,
        "eligibility_reason": (
            "latency validation failed; record correctness failed"
        ),
        "pending_backlog_percent": 72.3537,
        "flush_sec": 30.091,
        "failed_send_percent": 0.7499,
        "missing_after_drain_records": 0,
        "unexplained_surplus_records": 106840,
        "flush_remaining_messages": 153886,
        "cpu_allocated_percent_mean": 6.205,
        "kafka_jmx_bytes_in_counter_rate_mean": 3018028518.3,
        "kafka_jmx_bytes_out_counter_rate_mean": 3019566891.6,
        "jvm_heap_used_gb_mean": 8.316,
        "kafka_to_iperf_capacity_ratio": 1.479,
        "tmpfs_used_percent_max": 56.132,
    }
    latex = analyzer.latex_report([row], {}, [])
    markdown = analyzer.markdown_report([row], {}, [])
    assert r"\section{Observed V2 Cases}" in latex
    assert r"B4 & cfg\_119" in latex
    assert "662.320" in latex
    assert "106840" in latex
    assert "153886" in latex
    assert r"\section{Final Validation}" not in latex
    assert "| `B4` | `cfg_119` |" in markdown
    assert "measurement duration(s) of 20 seconds" in markdown


def test_broker_profile_detects_manifest_tampering() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "B0.json"
        profile = write_broker_profile(
            path,
            {
                "format": "kafka_broker_profile.v2",
                "profile_id": "B0",
                "settings": {
                    "heap_opts": "-Xms8g -Xmx8g -XX:+UseG1GC",
                    "num_network_threads": 8,
                    "num_io_threads": 16,
                    "socket_send_buffer_bytes": 1048576,
                    "socket_receive_buffer_bytes": 1048576,
                    "socket_request_max_bytes": 104857600,
                    "queued_max_requests": 1000,
                    "log_segment_bytes": 1073741824,
                },
            },
        )
        assert load_broker_profile(path).sha256 == profile.sha256
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["settings"]["num_io_threads"] = 99
        path.write_text(json.dumps(payload), encoding="utf-8")
        try:
            load_broker_profile(path)
        except ValueError as exc:
            assert "hash mismatch" in str(exc)
        else:
            raise AssertionError("tampered broker profile was accepted")


def test_backend_registry_and_versioned_kafka_config_equivalence() -> None:
    assert get_backend("kafka").backend_id == "kafka"
    try:
        get_backend("not-implemented")
    except ValueError as exc:
        assert "Unknown backend_id" in str(exc)
    else:
        raise AssertionError("unknown backend was accepted")

    legacy = load_benchmark_config(
        PROJECT_ROOT / "configs" / "one_broker_mpi_simultaneous.json"
    )
    versioned = load_benchmark_config(
        PROJECT_ROOT
        / "configs"
        / "campaigns"
        / "kafka"
        / "example_simultaneous_case.json"
    )
    ignored = {
        "schema_version",
        "case_id",
        "campaign_id",
    }
    legacy_effective = {
        key: value
        for key, value in legacy.to_input_dict().items()
        if key not in ignored
    }
    versioned_effective = {
        key: value
        for key, value in versioned.to_input_dict().items()
        if key not in ignored
    }
    assert versioned.backend_id == "kafka"
    assert versioned.qualification_policy_id == "qualification.kafka.v1"
    assert legacy_effective == versioned_effective
    round_trip = versioned.to_versioned_dict()
    assert round_trip["backend_id"] == "kafka"
    assert round_trip["backend"]["kafka"]["partitions"] == 120


def test_pulsar_backend_config_profile_and_monitoring_contract() -> None:
    config_path = (
        PROJECT_ROOT
        / "configs"
        / "campaigns"
        / "pulsar"
        / "examples"
        / "example_simultaneous_case.json"
    )
    config = load_benchmark_config(config_path)
    adapter = get_backend("pulsar")
    adapter.validate_config(config)

    assert config.backend_id == "pulsar"
    assert config.qualification_policy_id == "qualification.pulsar.v1"
    assert config.backend_settings["runtime"]["product_version"] == "5.0.0-M1"
    assert config.backend_settings["standalone_properties"] == {
        "allowAutoTopicCreation": False,
        "includeStandardPrometheusMetrics": True,
        "exposeTopicLevelMetricsInPrometheus": False,
        "exposeConsumerLevelMetricsInPrometheus": False,
    }
    assert config.total_mpi_ranks == 81
    plan = adapter.resource_plan(config)
    assert plan.service_nodes == 1
    assert plan.monitoring_nodes == 1
    assert plan.total_mpi_ranks == 81

    effective_config = config.to_dict()
    for kafka_only_field in (
        "broker_count",
        "partitions",
        "replication_factor",
        "topic_name",
        "acks",
        "compression_type",
        "batch_size",
        "linger_ms",
        "benchmark_kafka_client_count",
        "estimated_max_client_broker_connections",
    ):
        assert kafka_only_field not in effective_config
    assert effective_config["benchmark_client_count"] == 80
    assert effective_config["estimated_max_client_backend_connections"] == 80

    identity = adapter.identity_from_dict(effective_config)
    assert identity == {
        "backend_id": "pulsar",
        "adapter_version": "1",
        "product_version": "5.0.0-M1",
        "profile_id": "P0",
        "profile_sha256": config.backend_settings["profile_sha256"],
    }
    round_trip = config.to_versioned_dict()
    assert round_trip["backend_id"] == "pulsar"
    assert round_trip["backend"]["pulsar"]["partitions"] == 120
    assert "kafka" not in round_trip["backend"]
    assert (
        adapter.monitoring_endpoint_specs(config)["prometheus"]["runtime_file"]
        == "runtime/monitoring/prometheus_endpoint.txt"
    )
    assert (
        adapter.monitoring_endpoint_specs(config)["pulsar_metrics"]["path"]
        == "/metrics/"
    )

    pulsar_specs = {
        spec.metric_id: spec for spec in metric_specs_for_backend("pulsar")
    }
    pulsar_metric_ids = set(pulsar_specs)
    assert "pulsar_bytes_in_rate" in pulsar_metric_ids
    assert "pulsar_message_backlog" in pulsar_metric_ids
    assert "pulsar_managed_ledger_direct_pool_allocated_bytes" in pulsar_metric_ids
    assert "pulsar_managed_ledger_direct_pool_used_bytes" in pulsar_metric_ids
    assert "pulsar_process_resident_memory_bytes" in pulsar_metric_ids
    assert "pulsar_bookie_journal_queue_size" in pulsar_metric_ids
    assert "pulsar_broker_rate_in" in pulsar_specs[
        "pulsar_messages_in_rate"
    ].query
    assert (
        pulsar_specs["pulsar_jvm_heap_used_bytes"].required_any
        == ("jvm_memory_bytes_used",)
    )
    assert "jvm_gc_collection_seconds_sum" in (
        pulsar_specs["pulsar_jvm_gc_time_rate"].query
    )
    assert not any(metric_id.startswith("kafka_") for metric_id in pulsar_metric_ids)
    kafka_metric_ids = {
        spec.metric_id for spec in metric_specs_for_backend("kafka")
    }
    assert "kafka_jmx_bytes_in_counter_rate" in kafka_metric_ids
    assert "pulsar_bytes_in_rate" not in kafka_metric_ids
    assert adapter.uses_coordinated_consumer_drain(config) is True

    result = {
        "config": config.to_dict(),
        "aggregated_metrics": {
            "producers": {
                "messages_attempted": 1_000,
                "messages_enqueued": 1_000,
                "messages_delivered": 1_000,
                "messages_failed": 1,
                "pending_messages_at_flush_start": 50,
                "max_flush_duration_sec": 10.0,
                "throughput_bytes_per_sec": 4_096_000.0,
                "throughput_msgs_per_sec": 1_000.0,
            },
            "consumers": {
                "messages_received": 1_000,
                "throughput_bytes_per_sec": 4_096_000.0,
                "throughput_msgs_per_sec": 1_000.0,
            },
            "record_correctness": {
                "missing_after_drain_records": 0,
                "duplicate_offset_count": 0,
                "out_of_order_offset_count": 0,
            },
        },
        "monitoring": {
            "status": "completed",
            "collected_metrics": [
                {"id": "pulsar_bytes_in_rate", "category": "pulsar"}
            ],
        },
    }
    assert adapter.qualification_result(result) is True

    denominator_probe = json.loads(json.dumps(result))
    denominator_probe["aggregated_metrics"]["producers"].update(
        {
            "messages_attempted": 1_000,
            "messages_enqueued": 900,
            "messages_failed": 0,
            "pending_messages_at_flush_start": 50,
            "max_flush_duration_sec": 1.0,
        }
    )
    assert adapter.qualification_result(denominator_probe) is True
    assert (
        backlog_denominator_for_policy(
            PULSAR_HISTORICAL_QUALIFICATION_POLICY_ID
        )
        == BACKLOG_DENOMINATOR_ATTEMPTED
    )
    assert (
        backlog_denominator_for_policy(KAFKA_HISTORICAL_QUALIFICATION_POLICY_ID)
        == BACKLOG_DENOMINATOR_ENQUEUED
    )
    denominator_probe["config"]["qualification_policy_id"] = (
        COMMON_QUALIFICATION_POLICY_ID
    )
    adapter.validate_config(
        BenchmarkConfig(
            **{
                **benchmark_config_input_dict(denominator_probe["config"]),
                "qualification_policy_id": COMMON_QUALIFICATION_POLICY_ID,
            }
        )
    )
    assert adapter.qualification_result(denominator_probe) is True

    enrich_result_schema(result, adapter)
    validate_result_schema(result)
    assert result["system_under_test"]["product_version"] == "5.0.0-M1"
    assert result["common_metrics"]["qualification"]["qualified"] is True
    assert result["backend_metrics"]["pulsar"]["monitoring"]["status"] == "completed"
    assert "kafka" not in result["backend_metrics"]

    boundary = producer_operational_metrics(
        {
            "messages_attempted": 1_000,
            "messages_enqueued": 1_000,
            "messages_failed": 1,
            "pending_messages_at_flush_start": 50,
            "max_flush_duration_sec": 10.0,
        }
    )
    assert boundary["producer_backlog_percent"] == 5.0
    assert boundary["failed_send_percent"] == 0.1
    assert boundary["thresholds_satisfied"] is True
    common_boundary = producer_operational_metrics(
        {
            "messages_attempted": 1_000,
            "messages_enqueued": 900,
            "messages_failed": 100,
            "pending_messages_at_flush_start": 50,
            "max_flush_duration_sec": 1.0,
        },
        backlog_denominator=BACKLOG_DENOMINATOR_ATTEMPTED,
    )
    assert common_boundary["producer_backlog_percent"] == 5.0
    assert common_boundary["backlog_denominator_value"] == 1_000
    assert producer_operational_metrics(
        {
            "messages_attempted": 1_000,
            "messages_enqueued": 1_000,
            "messages_failed": 1,
            "pending_messages_at_flush_start": 51,
            "max_flush_duration_sec": 10.0,
        }
    )["thresholds_satisfied"] is False
    assert producer_operational_metrics(
        {
            "messages_attempted": 1_000,
            "messages_enqueued": 1_000,
            "messages_failed": 0,
            "max_flush_duration_sec": 1.0,
        }
    )["thresholds_satisfied"] is False

    verified = subprocess.run(
        [
            sys.executable,
            "-B",
            str(PROJECT_ROOT / "scripts" / "verify_pulsar_profile.py"),
            str(config_path),
            "--project-root",
            str(PROJECT_ROOT),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert '"profile_id": "P0"' in verified.stdout

    p1_config_path = (
        PROJECT_ROOT
        / "configs"
        / "campaigns"
        / "pulsar"
        / "pilots"
        / "historical"
        / "hpc_pilot_case_p1.json"
    )
    p1_config = load_benchmark_config(p1_config_path)
    adapter.validate_config(p1_config)
    assert p1_config.backend_settings["profile_id"] == "P1"
    assert p1_config.backend_settings["runtime"]["jvm_memory"].endswith(
        "MaxDirectMemorySize=32g"
    )
    verified_p1 = subprocess.run(
        [
            sys.executable,
            "-B",
            str(PROJECT_ROOT / "scripts" / "verify_pulsar_profile.py"),
            str(p1_config_path),
            "--project-root",
            str(PROJECT_ROOT),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert '"profile_id": "P1"' in verified_p1.stdout

    report_lines = adapter.configuration_report_lines(config.to_dict())
    assert "- **Warm-up (sec):** 30" in report_lines
    assert any("Profile SHA-256" in line for line in report_lines)
    assert any(
        "Real Pulsar producer clients" in line
        for line in adapter.scale_report_lines(config.to_dict())
    )
    assert "producer-to-consumer end-to-end latency" in adapter.latency_report_notes()[0]

    certifi_wheel = (
        PROJECT_ROOT
        / "tools"
        / "archives"
        / "certifi-2026.7.22-py3-none-any.whl"
    )
    certifi_sha256 = (
        "62f22742b58a1a33014a2b6b706588a8d7e2a88ae7bd1a6ebe8c992928483775"
    )
    pulsar_checksums = (
        PROJECT_ROOT / "tools" / "archives" / "pulsar-checksums.txt"
    ).read_text(encoding="utf-8")
    assert f"sha256 {certifi_sha256} {certifi_wheel.name}" in pulsar_checksums
    if certifi_wheel.is_file():
        assert hashlib.sha256(certifi_wheel.read_bytes()).hexdigest() == certifi_sha256
    assert "certifi==2026.7.22" in (
        PROJECT_ROOT / "requirements-pulsar.txt"
    ).read_text(encoding="utf-8")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_root = Path(temp_dir)
        standalone_conf = temp_root / "standalone.conf"
        standalone_conf.write_text(
            "\n".join(
                [
                    "advertisedAddress=10.0.0.1",
                    "bindAddress=0.0.0.0",
                    "managedLedgerDefaultEnsembleSize=1",
                    "managedLedgerDefaultWriteQuorum=1",
                    "managedLedgerDefaultAckQuorum=1",
                    "allowAutoTopicCreation=false",
                    "includeStandardPrometheusMetrics=true",
                    "exposeTopicLevelMetricsInPrometheus=false",
                    "exposeConsumerLevelMetricsInPrometheus=false",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        runtime_manifest = temp_root / "pulsar_runtime_manifest.json"
        subprocess.run(
            [
                sys.executable,
                "-B",
                str(PROJECT_ROOT / "scripts" / "verify_pulsar_runtime_profile.py"),
                "--profile",
                str(PROJECT_ROOT / "configs/backends/pulsar/profiles/P0.json"),
                "--standalone-conf",
                str(standalone_conf),
                "--jvm-memory",
                "-Xms8g -Xmx8g -XX:MaxDirectMemorySize=8g",
                "--java-version",
                'openjdk version "21.0.12" 2026-07-21',
                "--pulsar-version",
                "5.0.0-M1",
                "--service-node",
                "node001",
                "--service-address",
                "10.0.0.1",
                "--data-root",
                "/dev/shm/messaging-benchmark/pulsar",
                "--output",
                str(runtime_manifest),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        runtime = json.loads(runtime_manifest.read_text(encoding="utf-8"))
        assert runtime["profile_sha256"] == config.backend_settings["profile_sha256"]
        assert runtime["actual_java_major_version"] == 21
        assert runtime["standalone_config"]["profiled_properties"][
            "managedLedgerDefaultWriteQuorum"
        ] == "1"
        standalone_conf.write_text(
            standalone_conf.read_text(encoding="utf-8").replace(
                "allowAutoTopicCreation=false",
                "allowAutoTopicCreation=true",
            ),
            encoding="utf-8",
        )
        drifted = subprocess.run(
            [
                sys.executable,
                "-B",
                str(PROJECT_ROOT / "scripts" / "verify_pulsar_runtime_profile.py"),
                "--profile",
                str(PROJECT_ROOT / "configs/backends/pulsar/profiles/P0.json"),
                "--standalone-conf",
                str(standalone_conf),
                "--jvm-memory",
                "-Xms8g -Xmx8g -XX:MaxDirectMemorySize=8g",
                "--java-version",
                'openjdk version "21.0.12" 2026-07-21',
                "--pulsar-version",
                "5.0.0-M1",
                "--service-node",
                "node001",
                "--service-address",
                "10.0.0.1",
                "--data-root",
                "/dev/shm/messaging-benchmark/pulsar",
                "--output",
                str(runtime_manifest),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        assert drifted.returncode != 0
        assert "allowAutoTopicCreation drift" in drifted.stderr

    compute_preflight = (
        PROJECT_ROOT / "scripts/hpc_compute_preflight.sh"
    ).read_text(encoding="utf-8")
    assert 'elif backend_id == "pulsar":' in compute_preflight
    assert "import certifi" in compute_preflight


def test_pulsar_campaign_generator_and_coordinated_drain_state() -> None:
    reached = CoordinatedDrainState(
        delivered_target_records=100,
        timeout_sec=1.0,
        step_sec=0.25,
        consumed_records=20,
    )
    assert reached.should_continue is True
    reached.record_step(80)
    assert reached.completion_reason == "running"
    reached.record_step(100)
    assert reached.complete is True
    assert reached.completion_reason == "delivered_target_reached"
    assert reached.should_continue is False

    timed_out = CoordinatedDrainState(
        delivered_target_records=100,
        timeout_sec=0.5,
        step_sec=0.25,
    )
    timed_out.record_step(10)
    timed_out.record_step(20)
    assert timed_out.completion_reason == "drain_timeout"
    assert timed_out.should_continue is False

    no_target = CoordinatedDrainState(
        delivered_target_records=0,
        timeout_sec=60.0,
    )
    assert no_target.completion_reason == "no_delivered_record_target"
    assert no_target.should_continue is False

    controller_tree = ast.parse(
        (PROJECT_ROOT / "src" / "benchmark" / "benchmark_controller.py").read_text(
            encoding="utf-8"
        )
    )
    coordinated_method = next(
        node
        for node in ast.walk(controller_tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "_run_local_role_with_coordinated_drain"
    )
    assert any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "create_producer_worker"
        for node in ast.walk(coordinated_method)
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        output_root = Path(temp_dir) / "pulsar"
        subprocess.run(
            [
                sys.executable,
                "-B",
                str(PROJECT_ROOT / "scripts" / "generate_pulsar_campaigns.py"),
                "--output-root",
                str(output_root),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        phase1_dir = output_root / "phase1"
        with (phase1_dir / "phase1_manifest.csv").open(
            "r",
            encoding="utf-8",
            newline="",
        ) as handle:
            rows = list(csv.DictReader(handle))
        assert len(rows) == 120
        assert len({row["config_id"] for row in rows}) == 120
        variant_fields = (
            "producer_ranks",
            "consumer_ranks",
            "partitions",
            "payload_size_bytes",
            "batching_max_messages",
            "batching_max_bytes",
            "batching_max_publish_delay_ms",
            "max_pending_messages",
            "max_pending_messages_across_partitions",
            "receiver_queue_size",
            "max_total_receiver_queue_size_across_partitions",
        )
        assert len(
            {tuple(row[name] for name in variant_fields) for row in rows}
        ) == 120
        plan = json.loads(
            (phase1_dir / "phase1_campaign_plan.json").read_text(
                encoding="utf-8"
            )
        )
        assert plan["design_counts"] == {
            "baseline": 1,
            "one_factor": 43,
            "rank_pair": 9,
            "seeded_mixed": 67,
        }
        assert plan["batch_sizes"] == [30, 30, 30, 30]
        assert plan["batch_count"] == 4
        assert plan["timing_sec"] == {
            "warmup": 15,
            "measurement": 30,
            "drain": 60,
        }
        phase1_gate = (
            PROJECT_ROOT
            / "results"
            / "published"
            / "pulsar"
            / "phase1-gate"
            / "acceptance_report.json"
        )
        assert plan["ready_for_submission"] is phase1_gate.is_file()
        if not phase1_gate.is_file():
            assert plan["submission_gate_status"] == "acceptance report missing"
        batch_paths = sorted((phase1_dir / "batches").glob("batch_*.csv"))
        assert len(batch_paths) == 4
        for batch_path in batch_paths:
            with batch_path.open(
                "r", encoding="utf-8", newline=""
            ) as batch_handle:
                assert len(list(csv.DictReader(batch_handle))) == 30
        assert plan["profile_id"] == "BASELINE_H16_D32"

        corrected_dir = output_root / "pilots" / "corrected"
        with (corrected_dir / "instrumentation_overhead_manifest.csv").open(
            "r",
            encoding="utf-8",
            newline="",
        ) as handle:
            instrumentation = list(csv.DictReader(handle))
        with (corrected_dir / "memory_acceptance_manifest.csv").open(
            "r",
            encoding="utf-8",
            newline="",
        ) as handle:
            memory = list(csv.DictReader(handle))
        assert len(instrumentation) == 6
        assert len(memory) == 12
        assert {row["profile_id"] for row in memory} == {
            "P1",
            "BASELINE_H16_D32",
        }
        assert all(
            sorted(int(row["order"]) for row in instrumentation if row["block"] == str(block))
            == [1, 2]
            for block in range(1, 4)
        )

    corrected_config = load_benchmark_config(
        PROJECT_ROOT
        / "configs"
        / "campaigns"
        / "pulsar"
        / "pilots"
        / "corrected"
        / "baseline_h16_d32_case.json"
    )
    assert corrected_config.backend_settings["profile_id"] == "BASELINE_H16_D32"
    assert corrected_config.backend_settings["producer"]["block_if_queue_full"] is True
    assert corrected_config.backend_settings["producer"]["max_pending_messages"] == 1_000
    assert corrected_config.backend_settings["runtime"]["jvm_memory"] == (
        "-Xms16g -Xmx16g -XX:MaxDirectMemorySize=32g"
    )
    verified = subprocess.run(
        [
            sys.executable,
            "-B",
            str(PROJECT_ROOT / "scripts" / "verify_pulsar_profile.py"),
            str(
                PROJECT_ROOT
                / "configs"
                / "campaigns"
                / "pulsar"
                / "pilots"
                / "corrected"
                / "baseline_h16_d32_case.json"
            ),
            "--project-root",
            str(PROJECT_ROOT),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert '"profile_id": "BASELINE_H16_D32"' in verified.stdout


def test_pulsar_phase1_gate_and_strict_analysis() -> None:
    gate_script = PROJECT_ROOT / "scripts" / "check_pulsar_phase1_gate.py"
    pilot_manifest = (
        PROJECT_ROOT
        / "configs"
        / "campaigns"
        / "pulsar"
        / "pilots"
        / "corrected"
        / "instrumentation_overhead_manifest.csv"
    )
    phase1_manifest = (
        PROJECT_ROOT
        / "configs"
        / "campaigns"
        / "pulsar"
        / "phase1"
        / "phase1_manifest.csv"
    )
    published_gate = (
        PROJECT_ROOT
        / "results"
        / "published"
        / "pulsar"
        / "phase1-gate"
        / "acceptance_report.json"
    )
    pilot_gate_result = subprocess.run(
        [sys.executable, "-B", str(gate_script), str(pilot_manifest)],
        check=False,
        capture_output=True,
        text=True,
    )
    phase1_gate_result = subprocess.run(
        [sys.executable, "-B", str(gate_script), str(phase1_manifest)],
        check=False,
        capture_output=True,
        text=True,
    )
    if published_gate.is_file():
        assert pilot_gate_result.returncode == 0
        assert phase1_gate_result.returncode == 0
        accepted_gate = json.loads(published_gate.read_text(encoding="utf-8"))
        assert accepted_gate["format"] == (
            "messaging-benchmark.pulsar-phase1-gate.v1"
        )
        assert accepted_gate["phase1_submission_authorized"] is True
        assert accepted_gate["instrumentation"]["required_case_count"] == 6
        assert accepted_gate["instrumentation"]["observed_case_count"] == 6
        assert accepted_gate["instrumentation"]["passed"] is True
        assert accepted_gate["batch_runner"]["required_case_count"] == 10
        assert accepted_gate["batch_runner"]["passed"] is True
        assert (
            accepted_gate["batch_runner"]["storage_reset"][
                "minimum_tmpfs_free_percent"
            ]
            == 97
        )
    else:
        assert pilot_gate_result.returncode == 0
        assert phase1_gate_result.returncode != 0
        assert "Phase 1 is gated" in phase1_gate_result.stderr

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_root = Path(temp_dir)
        missing_report = temp_root / "missing.json"
        blocked = subprocess.run(
            [
                sys.executable,
                "-B",
                str(gate_script),
                str(phase1_manifest),
                "--acceptance-report",
                str(missing_report),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        assert blocked.returncode != 0
        assert "Phase 1 is gated" in blocked.stderr

        accepted_report = temp_root / "accepted.json"
        accepted_report.write_text(
            json.dumps(
                {
                    "format": "messaging-benchmark.pulsar-phase1-gate.v1",
                    "phase1_submission_authorized": True,
                    "profile": {
                        "profile_id": "BASELINE_H16_D32",
                        "profile_sha256": (
                            "7b85a5388524a2e598f181330a4c2e585823d9bc40dab66e"
                            "11603ee484d6068c"
                        ),
                    },
                }
            ),
            encoding="utf-8",
        )
        subprocess.run(
            [
                sys.executable,
                "-B",
                str(gate_script),
                str(phase1_manifest),
                "--acceptance-report",
                str(accepted_report),
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        input_validation = subprocess.run(
            [
                sys.executable,
                "-B",
                str(PROJECT_ROOT / "scripts" / "validate_pulsar_phase1_inputs.py"),
                "--gate-report",
                str(accepted_report),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        validation_payload = json.loads(input_validation.stdout)
        assert validation_payload["valid"] is True
        assert validation_payload["batch_sizes"] == [30, 30, 30, 30]
        assert all(
            batch["conservative_required_time_sec"] == 6030
            for batch in validation_payload["batches"]
        )

        generated_root = temp_root / "campaigns" / "pulsar"
        subprocess.run(
            [
                sys.executable,
                "-B",
                str(PROJECT_ROOT / "scripts" / "generate_pulsar_campaigns.py"),
                "--output-root",
                str(generated_root),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        manifest_path = generated_root / "phase1" / "phase1_manifest.csv"
        plan_path = generated_root / "phase1" / "phase1_campaign_plan.json"
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        with manifest_path.open("r", encoding="utf-8", newline="") as handle:
            manifest_rows = list(csv.DictReader(handle))

        results_root = temp_root / "results"
        for index, manifest_row in enumerate(manifest_rows, start=1):
            case_dir = results_root / manifest_row["case_id"]
            case_dir.mkdir(parents=True)
            config_number = int(manifest_row["config_id"].split("_")[1])
            balanced = 100.0 + config_number
            report = {
                "case": {
                    "case_id": manifest_row["case_id"],
                    "campaign_id": "pulsar-phase1-screening",
                    "status": "completed",
                },
                "system_under_test": {
                    "backend_id": "pulsar",
                    "profile_id": plan["profile_id"],
                    "profile_sha256": plan["profile_sha256"],
                },
                "config": {"backend_id": "pulsar"},
                "backend_health": {"status": "healthy"},
                "eligibility": {"eligible": True, "failure_reasons": []},
                "common_metrics": {
                    "throughput": {
                        "producer_mib_per_sec": balanced + 1.0,
                        "consumer_mib_per_sec": balanced,
                        "balanced_mib_per_sec": balanced,
                        "producer_records_per_sec": balanced * 257.0,
                        "consumer_records_per_sec": balanced * 256.0,
                        "balanced_records_per_sec": balanced * 256.0,
                    },
                    "records": {
                        "consumed": 100_000,
                        "missing": 0,
                    },
                    "latency_end_to_end": {
                        "valid": True,
                        "p50_us": 1000.0 + index,
                        "p95_us": 2000.0 + index,
                        "p99_us": 3000.0 + index,
                    },
                    "qualification": {
                        "eligible": True,
                        "qualified": True,
                    },
                },
                "aggregated_metrics": {
                    "producers": {
                        "messages_attempted": 100_000,
                        "messages_enqueued": 100_000,
                        "messages_delivered": 100_000,
                        "messages_failed": 0,
                        "pending_messages_at_flush_start": 0,
                        "max_flush_duration_sec": 0.5,
                    }
                },
            }
            (case_dir / "final_report.json").write_text(
                json.dumps(report),
                encoding="utf-8",
            )

        output_dir = temp_root / "analysis"
        subprocess.run(
            [
                sys.executable,
                "-B",
                str(PROJECT_ROOT / "scripts" / "analyze_pulsar_phase1.py"),
                "--results-root",
                str(results_root),
                "--manifest",
                str(manifest_path),
                "--campaign-plan",
                str(plan_path),
                "--output-dir",
                str(output_dir),
                "--skip-figures",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        cases = json.loads(
            (output_dir / "pulsar_phase1_cases.json").read_text(encoding="utf-8")
        )
        shortlist = json.loads(
            (output_dir / "pulsar_phase1_shortlist.json").read_text(
                encoding="utf-8"
            )
        )
        validation = json.loads(
            (output_dir / "pulsar_phase1_validation.json").read_text(
                encoding="utf-8"
            )
        )
        summary = json.loads(
            (output_dir / "pulsar_phase1_summary.json").read_text(
                encoding="utf-8"
            )
        )
        sensitivity = json.loads(
            (output_dir / "pulsar_phase1_backlog_sensitivity.json").read_text(
                encoding="utf-8"
            )
        )
        with (
            output_dir / "pulsar_phase1_controlled_comparisons.csv"
        ).open("r", encoding="utf-8", newline="") as handle:
            controlled_rows = list(csv.DictReader(handle))
        latex_report = (output_dir / "pulsar_phase1_report.tex").read_text(
            encoding="utf-8"
        )
        assert validation["valid"] is True
        assert cases["case_count"] == 120
        assert cases["cases"][0]["config_id"] == "cfg_120"
        assert len(shortlist["configurations"]) == 10
        assert summary["case_count"] == 120
        assert summary["qualified_case_count"] == 120
        assert summary["qualification_policy"]["policy_id"] == (
            COMMON_QUALIFICATION_POLICY_ID
        )
        assert [row["backlog_threshold_percent"] for row in sensitivity["thresholds"]] == [
            2.0,
            5.0,
            10.0,
        ]
        assert all(
            row["qualified_case_count"] == 120
            for row in sensitivity["thresholds"]
        )
        assert len(controlled_rows) == 43
        assert (output_dir / "pulsar_phase1_report.md").is_file()
        assert (output_dir / "pulsar_phase1_report.html").is_file()
        assert (output_dir / "pulsar_phase1_report.tex").is_file()
        assert (output_dir / "artifact_manifest.csv").is_file()
        assert r"\section{Introduction}" in latex_report
        assert r"\section{Controlled Parameter Findings}" in latex_report
        assert r"\section{Resource and Bottleneck Evidence}" in latex_report
        assert r"\label{tab:backlog-sensitivity}" in latex_report
        assert r"\label{fig:throughput-backlog}" in latex_report
        assert "Failed (\\%)" in latex_report
        assert r"\texttt{cfg\_120}" in latex_report
        assert str(temp_root) not in latex_report
        assert "/home/" not in latex_report

        analyzer = _load_script_module(
            "analyze_pulsar_phase1",
            PROJECT_ROOT / "scripts" / "analyze_pulsar_phase1.py",
        )
        assert analyzer._bytes_to_mib(1_048_576) == 1.0
        assert analyzer._bytes_to_gib(1_073_741_824) == 1.0
        assert analyzer._decimal_mb_to_mib(1.048576) == 1.0
        assert analyzer._quartile([1.0, 2.0, 3.0, 4.0], 0.25) == 1.75
        ranking_rows = [
            {
                "config_id": "cfg_fast",
                "eligible": True,
                "qualified": True,
                "balanced_mib_per_sec": 200.0,
                "latency_valid": True,
                "latency_p99_us": 9_000_000.0,
                "producer_backlog_percent": 2.0,
                "max_flush_duration_sec": 1.0,
                "failed_send_percent": 0.0,
            },
            {
                "config_id": "cfg_low_latency",
                "eligible": True,
                "qualified": True,
                "balanced_mib_per_sec": 190.0,
                "latency_valid": True,
                "latency_p99_us": 1.0,
                "producer_backlog_percent": 1.0,
                "max_flush_duration_sec": 0.5,
                "failed_send_percent": 0.0,
            },
            {
                "config_id": "cfg_unqualified",
                "eligible": True,
                "qualified": False,
                "balanced_mib_per_sec": 300.0,
                "latency_valid": True,
                "latency_p99_us": 0.5,
                "producer_backlog_percent": 6.0,
                "max_flush_duration_sec": 0.5,
                "failed_send_percent": 0.0,
            },
        ]
        analyzer._rank_rows(ranking_rows)
        assert [row["config_id"] for row in ranking_rows] == [
            "cfg_fast",
            "cfg_low_latency",
            "cfg_unqualified",
        ]


def test_pulsar_phase2_memory_screen_design() -> None:
    generator = PROJECT_ROOT / "scripts" / "generate_pulsar_phase2.py"
    validator = PROJECT_ROOT / "scripts" / "validate_pulsar_phase2_inputs.py"
    authorizer = (
        PROJECT_ROOT / "scripts" / "authorize_pulsar_phase2_screening.py"
    )
    baseline_profile = (
        PROJECT_ROOT
        / "configs"
        / "backends"
        / "pulsar"
        / "profiles"
        / "BASELINE_H16_D32.json"
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_root = Path(temp_dir)
        output_dir = temp_root / "phase2"
        profile_root = temp_root / "profiles"
        profile_root.mkdir()
        (profile_root / baseline_profile.name).write_bytes(
            baseline_profile.read_bytes()
        )
        phase1_rows = [
            {
                "config_id": f"cfg_{index:03d}",
                "status": "completed",
                "eligible": True,
                "qualified": True,
                "latency_valid": True,
                "balanced_mib_per_sec": float(index),
                "balanced_records_per_sec": float(index * 1_000),
                "latency_p99_us": float(1_000 + index),
            }
            for index in range(1, 121)
        ]
        phase1_cases = temp_root / "phase1_cases.json"
        phase1_cases.write_text(
            json.dumps({"case_count": 120, "cases": phase1_rows}),
            encoding="utf-8",
        )
        phase1_summary = temp_root / "phase1_summary.json"
        phase1_summary.write_text(
            json.dumps(
                {
                    "case_count": 120,
                    "completed_case_count": 120,
                    "eligible_case_count": 120,
                    "qualified_case_count": 120,
                    "latency_valid_case_count": 120,
                }
            ),
            encoding="utf-8",
        )
        phase1_shortlist = temp_root / "phase1_shortlist.json"
        phase1_shortlist.write_text(
            json.dumps(
                {
                    "configuration_count": 10,
                    "configurations": [
                        {"config_id": f"cfg_{index:03d}"}
                        for index in range(1, 11)
                    ],
                    "historically_executed_phase2_anchor_ids": [
                        "cfg_120",
                        "cfg_101",
                        "cfg_093",
                        "cfg_089",
                        "cfg_001",
                    ],
                }
            ),
            encoding="utf-8",
        )
        subprocess.run(
            [
                sys.executable,
                "-B",
                str(generator),
                "--output-dir",
                str(output_dir),
                "--profile-root",
                str(profile_root),
                "--shortlist",
                str(phase1_shortlist),
                "--phase1-summary",
                str(phase1_summary),
                "--phase1-cases",
                str(phase1_cases),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        manifest = output_dir / "memory_screening_manifest.csv"
        plan_path = output_dir / "phase2_campaign_plan.json"
        with manifest.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        assert len(rows) == 30
        assert len({row["case_id"] for row in rows}) == 30
        assert len(
            {(row["profile_id"], row["anchor"]) for row in rows}
        ) == 30
        assert {row["anchor"] for row in rows} == {
            "cfg_120",
            "cfg_101",
            "cfg_093",
            "cfg_089",
            "cfg_001",
        }
        assert {row["heap_gib"] for row in rows} == {"16", "24", "32"}
        assert {row["direct_memory_gib"] for row in rows} == {"32", "48"}
        assert sorted(int(row["order"]) for row in rows) == list(range(1, 31))
        assert plan["configuration_count"] == 30
        assert plan["profile_count"] == 6
        assert plan["anchor_count"] == 5
        assert plan["submission_authorized"] is False
        assert plan["slurm"]["conservative_required_time_sec"] == 6030

        validated = subprocess.run(
            [
                sys.executable,
                "-B",
                str(validator),
                "--plan",
                str(plan_path),
                "--manifest",
                str(manifest),
                "--profile-root",
                str(profile_root),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        validation = json.loads(validated.stdout)
        assert validation["valid"] is True
        assert validation["case_count"] == 30
        assert validation["profile_count"] == 6
        assert validation["remaining_margin_sec"] == 1170

        authorized = subprocess.run(
            [
                sys.executable,
                "-B",
                str(authorizer),
                "--plan",
                str(plan_path),
                "--manifest",
                str(manifest),
                "--profile-root",
                str(profile_root),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        assert "submission_authorized=true" in authorized.stdout
        authorized_plan = json.loads(plan_path.read_text(encoding="utf-8"))
        assert authorized_plan["status"] == "authorized_for_submission"
        assert authorized_plan["submission_authorized"] is True
        assert len(authorized_plan["authorization"]["profile_sha256"]) == 6
        assert len(
            authorized_plan["authorization"]["generated_config_sha256"]
        ) == 30
        authorized_validation = subprocess.run(
            [
                sys.executable,
                "-B",
                str(validator),
                "--plan",
                str(plan_path),
                "--manifest",
                str(manifest),
                "--profile-root",
                str(profile_root),
                "--require-authorized",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        assert json.loads(authorized_validation.stdout)[
            "submission_authorized"
        ] is True

        planner = _load_script_module(
            "plan_backend_batch_phase2_test",
            PROJECT_ROOT / "scripts" / "plan_backend_batch.py",
        )
        batch_plan = planner.build_batch_plan(
            "pulsar",
            manifest,
            PROJECT_ROOT,
            require_one_profile=False,
        )
        assert batch_plan["profile_count"] == 6
        assert batch_plan["profile_id"] == "multiple"
        assert batch_plan["total_case_budget_sec"] == 3150
        try:
            planner.build_batch_plan(
                "pulsar",
                manifest,
                PROJECT_ROOT,
                require_one_profile=True,
            )
        except ValueError as exc:
            assert "requires one immutable backend profile" in str(exc)
        else:
            raise AssertionError("Multi-profile manifest bypassed the opt-in gate")

    runner = (PROJECT_ROOT / "scripts" / "run_backend_batch.sh").read_text(
        encoding="utf-8"
    )
    submitter = (
        PROJECT_ROOT / "scripts" / "submit_backend_batch.sh"
    ).read_text(encoding="utf-8")
    assert "BACKEND_BATCH_ALLOW_PROFILE_CHANGES" in runner
    assert "--allow-profile-changes" in runner
    assert "BACKEND_BATCH_ALLOW_PROFILE_CHANGES" in submitter
    assert "--allow-profile-changes" in submitter


def test_pulsar_phase2_screening_selection_contract() -> None:
    analyzer = _load_script_module(
        "analyze_pulsar_phase2_screening_test",
        PROJECT_ROOT / "scripts" / "analyze_pulsar_phase2_screening.py",
    )
    profiles = {
        "BASELINE_H16_D32": (16, 32, 1.00, 1.00),
        "PHASE2_HEAP16_DIRECT48": (16, 48, 1.02, 0.98),
        "PHASE2_HEAP24_DIRECT32": (24, 32, 1.04, 1.02),
        "PHASE2_HEAP24_DIRECT48": (24, 48, 1.10, 0.95),
        "PHASE2_HEAP32_DIRECT32": (32, 32, 1.03, 1.01),
        "PHASE2_HEAP32_DIRECT48": (32, 48, 1.08, 0.96),
    }
    rows = []
    anchors = (*analyzer.SUSTAINABLE_ANCHORS, analyzer.TRANSITION_ANCHOR)
    for profile_id, (heap, direct, throughput_ratio, latency_ratio) in profiles.items():
        for anchor in anchors:
            rows.append(
                {
                    "profile_id": profile_id,
                    "anchor": anchor,
                    "heap_gib": heap,
                    "direct_memory_gib": direct,
                    "status": "completed",
                    "backend_health": "healthy",
                    "eligible": True,
                    "qualified": anchor != analyzer.TRANSITION_ANCHOR,
                    "latency_valid": True,
                    "records_missing_after_drain": 0,
                    "duplicate_records": 0,
                    "out_of_order_records": 0,
                    "balanced_mib_per_sec": 100.0 * throughput_ratio,
                    "latency_p99_us": 1000.0 * latency_ratio,
                    "failed_send_percent": 0.0,
                    "producer_backlog_percent": 2.0,
                    "max_flush_duration_sec": 0.5,
                    "pulsar_process_rss_peak_gib": heap + 8.0,
                    "pulsar_jvm_heap_peak_gib": heap - 1.0,
                    "pulsar_jvm_direct_nio_peak_gib": 0.2,
                    "pulsar_managed_ledger_direct_pool_allocated_peak_gib": 2.0,
                    "pulsar_managed_ledger_direct_pool_used_peak_gib": 1.0,
                    "pulsar_direct_memory_usage_peak_percent": None,
                    "pulsar_jvm_gc_time_rate_peak": 0.01,
                    "tmpfs_used_peak_percent": 20.0,
                    "post_reset_tmpfs_free_percent": 97.0,
                }
            )
    profile_rows, selection = analyzer._profile_analysis(rows)
    assert len(profile_rows) == 6
    assert selection["status"] == "ready_for_confirmation"
    assert selection["confirmation_profiles"] == [
        "BASELINE_H16_D32",
        "PHASE2_HEAP24_DIRECT48",
        "PHASE2_HEAP32_DIRECT48",
    ]
    assert all(row["all_required_valid"] for row in profile_rows)

    with tempfile.TemporaryDirectory() as temp_dir:
        case_dir = Path(temp_dir)
        monitoring = case_dir / "monitoring"
        monitoring.mkdir()
        process_csv = monitoring / "pulsar_process_node.csv"
        process_csv.write_text(
            "timestamp,tmpfs_used_percent\n1,12.5\n2,44.25\n",
            encoding="utf-8",
        )
        reset_log = case_dir / "logs" / "pulsar" / "post-case-storage-reset.log"
        reset_log.parent.mkdir(parents=True)
        reset_log.write_text("post_reset_free_percent=97\n", encoding="utf-8")
        assert analyzer._tmpfs_evidence(case_dir) == (44.25, 97.0)


def test_pulsar_phase2_confirmation_design() -> None:
    generator = (
        PROJECT_ROOT / "scripts" / "generate_pulsar_phase2_confirmation.py"
    )
    validator = (
        PROJECT_ROOT
        / "scripts"
        / "validate_pulsar_phase2_confirmation_inputs.py"
    )
    authorizer = (
        PROJECT_ROOT / "scripts" / "authorize_pulsar_phase2_confirmation.py"
    )
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_root = Path(temp_dir)
        selection_path = temp_root / "selection.json"
        output_dir = temp_root / "confirmation"
        selection_path.write_text(
            json.dumps(
                {
                    "format": (
                        "messaging-benchmark."
                        "pulsar-phase2-candidate-selection.v1"
                    ),
                    "status": "ready_for_confirmation",
                    "confirmation_profiles": [
                        "BASELINE_H16_D32",
                        "PHASE2_HEAP24_DIRECT48",
                        "PHASE2_HEAP32_DIRECT48",
                    ],
                    "source_evidence": {"screening_results_root": "test"},
                }
            )
            + "\n",
            encoding="utf-8",
        )
        phase1_rows = [
            {
                "config_id": f"cfg_{index:03d}",
                "status": "completed",
                "eligible": True,
                "qualified": True,
                "latency_valid": True,
                "balanced_mib_per_sec": float(index),
                "balanced_records_per_sec": float(index * 1_000),
                "latency_p99_us": float(1_000 + index),
            }
            for index in range(1, 121)
        ]
        phase1_cases = temp_root / "phase1_cases.json"
        phase1_cases.write_text(
            json.dumps({"case_count": 120, "cases": phase1_rows}),
            encoding="utf-8",
        )
        phase1_shortlist = temp_root / "phase1_shortlist.json"
        phase1_shortlist.write_text(
            json.dumps(
                {
                    "historically_executed_phase2_anchor_ids": [
                        "cfg_120",
                        "cfg_101",
                        "cfg_093",
                        "cfg_089",
                        "cfg_001",
                    ]
                }
            ),
            encoding="utf-8",
        )
        subprocess.run(
            [
                sys.executable,
                "-B",
                str(generator),
                "--selection",
                str(selection_path),
                "--output-dir",
                str(output_dir),
                "--shortlist",
                str(phase1_shortlist),
                "--phase1-cases",
                str(phase1_cases),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        manifest_path = output_dir / "memory_confirmation_manifest.csv"
        with manifest_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        plan = json.loads(
            (output_dir / "confirmation_plan.json").read_text(encoding="utf-8")
        )
        assert len(rows) == 30
        assert len({row["case_id"] for row in rows}) == 30
        assert Counter(row["block"] for row in rows) == {"2": 15, "3": 15}
        assert Counter(row["profile_id"] for row in rows) == {
            "BASELINE_H16_D32": 10,
            "PHASE2_HEAP24_DIRECT48": 10,
            "PHASE2_HEAP32_DIRECT48": 10,
        }
        assert all(
            count == 2
            for count in Counter(
                (row["profile_id"], row["anchor"]) for row in rows
            ).values()
        )
        assert plan["submission_authorized"] is False
        assert plan["total_observations_per_profile_anchor_after_confirmation"] == 3
        assert plan["slurm"]["conservative_required_time_sec"] == 6030
        validated = subprocess.run(
            [
                sys.executable,
                "-B",
                str(validator),
                "--plan",
                str(output_dir / "confirmation_plan.json"),
                "--manifest",
                str(manifest_path),
                "--selection",
                str(selection_path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        assert json.loads(validated.stdout)["valid"] is True
        subprocess.run(
            [
                sys.executable,
                "-B",
                str(authorizer),
                "--plan",
                str(output_dir / "confirmation_plan.json"),
                "--manifest",
                str(manifest_path),
                "--selection",
                str(selection_path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        authorized = subprocess.run(
            [
                sys.executable,
                "-B",
                str(validator),
                "--plan",
                str(output_dir / "confirmation_plan.json"),
                "--manifest",
                str(manifest_path),
                "--selection",
                str(selection_path),
                "--require-authorized",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        assert json.loads(authorized.stdout)["submission_authorized"] is True


def test_pulsar_final_validation_design() -> None:
    generator = PROJECT_ROOT / "scripts" / "generate_pulsar_final_validation.py"
    validator = PROJECT_ROOT / "scripts" / "validate_pulsar_final_validation_inputs.py"
    authorizer = PROJECT_ROOT / "scripts" / "authorize_pulsar_final_validation.py"
    expected_ids = {
        "cfg_001",
        "cfg_007",
        "cfg_049",
        "cfg_055",
        "cfg_063",
        "cfg_080",
        "cfg_089",
        "cfg_093",
        "cfg_101",
        "cfg_120",
    }
    with tempfile.TemporaryDirectory() as tmp_dir:
        temp_root = Path(tmp_dir)
        output_dir = temp_root / "final_validation"
        phase1_cases = temp_root / "phase1_cases.json"
        phase1_cases.write_text(
            json.dumps(
                {
                    "case_count": 120,
                    "cases": [
                        {"config_id": f"cfg_{index:03d}"}
                        for index in range(1, 121)
                    ],
                }
            ),
            encoding="utf-8",
        )
        phase1_shortlist = temp_root / "phase1_shortlist.json"
        phase1_shortlist.write_text(
            json.dumps(
                {
                    "configurations": [
                        {"config_id": config_id}
                        for config_id in sorted(expected_ids)
                    ],
                    "historically_executed_final_validation_ids": [
                        "cfg_001",
                        "cfg_007",
                        "cfg_049",
                        "cfg_055",
                        "cfg_063",
                        "cfg_080",
                        "cfg_089",
                        "cfg_093",
                        "cfg_101",
                        "cfg_120",
                    ],
                }
            ),
            encoding="utf-8",
        )
        final_selection = temp_root / "final_selection.json"
        final_selection.write_text(
            json.dumps(
                {
                    "status": "profile_frozen",
                    "frozen_profile_id": "BASELINE_H16_D32",
                }
            ),
            encoding="utf-8",
        )
        subprocess.run(
            [
                sys.executable,
                "-B",
                str(generator),
                "--output-dir",
                str(output_dir),
                "--shortlist",
                str(phase1_shortlist),
                "--phase1-cases",
                str(phase1_cases),
                "--final-selection",
                str(final_selection),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        manifest_path = output_dir / "final_validation_manifest.csv"
        plan_path = output_dir / "final_validation_plan.json"
        with manifest_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        plan = json.loads(plan_path.read_text(encoding="utf-8"))

        assert len(rows) == 50
        assert len({row["case_id"] for row in rows}) == 50
        assert Counter(row["config_id"] for row in rows) == Counter(
            {config_id: 5 for config_id in expected_ids}
        )
        assert Counter(row["block"] for row in rows) == Counter(
            {str(block): 10 for block in range(1, 6)}
        )
        assert {row["profile_id"] for row in rows} == {"BASELINE_H16_D32"}
        for block in range(1, 6):
            block_rows = [row for row in rows if row["block"] == str(block)]
            assert {row["config_id"] for row in block_rows} == expected_ids
            assert sorted(int(row["order"]) for row in block_rows) == list(
                range(1, 11)
            )

        assert plan["case_count"] == 50
        assert plan["configuration_count"] == 10
        assert plan["block_count"] == 5
        assert plan["observations_per_configuration"] == 5
        assert [batch["case_count"] for batch in plan["batches"]] == [30, 20]
        assert [
            batch["conservative_required_time_sec"] for batch in plan["batches"]
        ] == [6030, 4380]
        assert plan["submission_authorized"] is False

        validated = subprocess.run(
            [
                sys.executable,
                "-B",
                str(validator),
                "--plan",
                str(plan_path),
                "--manifest",
                str(manifest_path),
                "--shortlist",
                str(phase1_shortlist),
                "--final-selection",
                str(final_selection),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        validation = json.loads(validated.stdout)
        assert validation["valid"] is True
        assert validation["case_count"] == 50
        assert validation["batch_case_counts"] == [30, 20]
        assert validation["submission_authorized"] is False

        subprocess.run(
            [
                sys.executable,
                "-B",
                str(authorizer),
                "--plan",
                str(plan_path),
                "--manifest",
                str(manifest_path),
                "--shortlist",
                str(phase1_shortlist),
                "--final-selection",
                str(final_selection),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        authorized_plan = json.loads(plan_path.read_text(encoding="utf-8"))
        assert authorized_plan["submission_authorized"] is True
        assert authorized_plan["status"] == "authorized_for_submission"
        assert len(authorized_plan["authorization"]["generated_config_sha256"]) == 50

        authorized = subprocess.run(
            [
                sys.executable,
                "-B",
                str(validator),
                "--plan",
                str(plan_path),
                "--manifest",
                str(manifest_path),
                "--shortlist",
                str(phase1_shortlist),
                "--final-selection",
                str(final_selection),
                "--require-authorized",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        assert json.loads(authorized.stdout)["submission_authorized"] is True


def test_pulsar_final_validation_aggregation_and_ranking() -> None:
    analyzer = _load_script_module(
        "analyze_pulsar_final_validation_test",
        PROJECT_ROOT / "scripts" / "analyze_pulsar_final_validation.py",
    )

    def case_row(
        config_id: str,
        repeat: int,
        *,
        eligible: bool,
        qualified: bool,
        throughput: float,
        p99_us: float,
    ) -> dict[str, object]:
        return {
            "config_id": config_id,
            "selection_categories": "test",
            "producer_ranks": 40,
            "consumer_ranks": 40,
            "partitions": 120,
            "payload_size_bytes": 4096,
            "status": "completed",
            "backend_health": "healthy",
            "eligible": eligible,
            "qualified": qualified,
            "latency_valid": eligible,
            "records_missing_after_drain": 0,
            "duplicate_records": 0,
            "out_of_order_records": 0,
            "producer_mib_per_sec": throughput + 1.0,
            "consumer_mib_per_sec": throughput,
            "balanced_mib_per_sec": throughput,
            "producer_records_per_sec": (throughput + 1.0) * 256.0,
            "consumer_records_per_sec": throughput * 256.0,
            "balanced_records_per_sec": throughput * 256.0,
            "latency_p50_us": p99_us / 3.0,
            "latency_p95_us": p99_us / 2.0,
            "latency_p99_us": p99_us,
            "latency_p99_9_us": p99_us * 1.2,
            "missing_after_drain_percent": 0.0,
            "producer_backlog_percent": 2.0 if qualified else 6.0,
            "max_flush_duration_sec": 0.2 + repeat / 100.0,
            "failed_send_percent": 0.0,
            "pulsar_bytes_in_peak_mib_per_sec": throughput + 2.0,
            "pulsar_bytes_out_peak_mib_per_sec": throughput + 2.0,
            "pulsar_process_rss_peak_gib": 20.0,
            "pulsar_jvm_heap_peak_gib": 12.0,
            "tmpfs_used_peak_percent": 25.0,
        }

    rows = []
    for repeat in range(1, 6):
        rows.append(
            case_row(
                "cfg_full",
                repeat,
                eligible=True,
                qualified=True,
                throughput=90.0 + repeat,
                p99_us=900_000.0 + repeat,
            )
        )
        rows.append(
            case_row(
                "cfg_partial",
                repeat,
                eligible=True,
                qualified=repeat <= 4,
                throughput=110.0 + repeat,
                p99_us=700_000.0 + repeat,
            )
        )
        rows.append(
            case_row(
                "cfg_ineligible",
                repeat,
                eligible=repeat <= 4,
                qualified=repeat <= 4,
                throughput=10_000.0 if repeat == 5 else 100.0 + repeat,
                p99_us=500_000.0 + repeat,
            )
        )

    summaries = analyzer.summarize_configurations(rows)
    analyzer.rank_summaries(summaries)
    assert [row["config_id"] for row in summaries] == [
        "cfg_full",
        "cfg_partial",
        "cfg_ineligible",
    ]
    assert summaries[0]["qualified_repeats"] == 5
    assert summaries[1]["median_balanced_mib_per_sec"] == 113.0
    ineligible = summaries[2]
    assert ineligible["eligible_repeats"] == 4
    assert ineligible["median_balanced_mib_per_sec"] == 102.5
    assert ineligible["balanced_mib_per_sec_iqr"] == 1.5

    ranking_contract = [
        {
            "config_id": "cfg_faster",
            "eligible_repeats": 4,
            "qualified_repeats": 4,
            "median_balanced_mib_per_sec": 200.0,
            "median_latency_p99_us": 9_000_000.0,
            "balanced_mib_per_sec_iqr": 2.0,
            "median_producer_backlog_percent": 2.0,
            "median_max_flush_duration_sec": 0.5,
            "max_failed_send_percent": 0.0,
        },
        {
            "config_id": "cfg_more_eligible_lower_latency",
            "eligible_repeats": 5,
            "qualified_repeats": 4,
            "median_balanced_mib_per_sec": 190.0,
            "median_latency_p99_us": 1.0,
            "balanced_mib_per_sec_iqr": 1.0,
            "median_producer_backlog_percent": 1.0,
            "median_max_flush_duration_sec": 0.4,
            "max_failed_send_percent": 0.0,
        },
    ]
    analyzer.rank_summaries(ranking_contract)
    assert ranking_contract[0]["config_id"] == "cfg_faster"


def test_pulsar_phase2_final_freeze_rule() -> None:
    analyzer = _load_script_module(
        "analyze_pulsar_phase2_confirmation_test",
        PROJECT_ROOT / "scripts" / "analyze_pulsar_phase2_confirmation.py",
    )
    profiles = {
        "BASELINE_H16_D32": (16, 32, 1.00, 1.00),
        "PHASE2_HEAP24_DIRECT48": (24, 48, 1.05, 1.05),
        "PHASE2_HEAP32_DIRECT48": (32, 48, 1.02, 0.95),
    }
    cells = []
    rows = []
    for profile_id, (heap, direct, throughput_ratio, latency_ratio) in profiles.items():
        for anchor in (*analyzer.SUSTAINABLE_ANCHORS, analyzer.TRANSITION_ANCHOR):
            cells.append(
                {
                    "profile_id": profile_id,
                    "anchor": anchor,
                    "heap_gib": heap,
                    "direct_memory_gib": direct,
                    "median_balanced_mib_per_sec": 100.0 * throughput_ratio,
                    "balanced_mib_per_sec_iqr": 2.0,
                    "median_latency_p99_us": 1000.0 * latency_ratio,
                }
            )
            for _ in range(3):
                rows.append(
                    {
                        "profile_id": profile_id,
                        "anchor": anchor,
                        "status": "completed",
                        "backend_health": "healthy",
                        "eligible": True,
                        "qualified": anchor != analyzer.TRANSITION_ANCHOR,
                        "latency_valid": True,
                        "records_missing_after_drain": 0,
                        "duplicate_records": 0,
                        "out_of_order_records": 0,
                        "failed_send_percent": 0.0,
                        "producer_backlog_percent": (
                            6.0 if anchor == analyzer.TRANSITION_ANCHOR else 2.0
                        ),
                        "max_flush_duration_sec": (
                            11.0 if anchor == analyzer.TRANSITION_ANCHOR else 0.5
                        ),
                        "pulsar_process_rss_peak_gib": heap + 8.0,
                        "pulsar_jvm_heap_peak_gib": heap - 1.0,
                        "pulsar_jvm_direct_nio_peak_gib": 0.2,
                        "pulsar_managed_ledger_direct_pool_allocated_peak_gib": 2.0,
                        "pulsar_managed_ledger_direct_pool_used_peak_gib": 1.0,
                        "pulsar_jvm_gc_time_rate_peak": 0.01,
                        "tmpfs_used_peak_percent": 25.0,
                        "post_reset_tmpfs_free_percent": 97.0,
                    }
                )
    profile_rows, selection = analyzer._final_profile_selection(cells, rows)
    assert selection["frozen_profile_id"] == "PHASE2_HEAP24_DIRECT48"
    assert selection["baseline_retained"] is False
    assert next(
        row
        for row in profile_rows
        if row["profile_id"] == "PHASE2_HEAP24_DIRECT48"
    )["candidate_passed"] is True
    assert next(
        row
        for row in profile_rows
        if row["profile_id"] == "PHASE2_HEAP32_DIRECT48"
    )["candidate_passed"] is False

    no_gain_cells = []
    for cell in cells:
        adjusted = dict(cell)
        if adjusted["profile_id"] == "PHASE2_HEAP24_DIRECT48":
            adjusted["median_balanced_mib_per_sec"] = 101.0
        elif adjusted["profile_id"] == "PHASE2_HEAP32_DIRECT48":
            adjusted["median_balanced_mib_per_sec"] = 99.0
        no_gain_cells.append(adjusted)
    _, baseline_selection = analyzer._final_profile_selection(
        no_gain_cells, rows
    )
    assert baseline_selection["frozen_profile_id"] == "BASELINE_H16_D32"
    assert baseline_selection["baseline_retained"] is True
    assert baseline_selection["passing_candidate_profiles"] == []


def test_pulsar_complete_report_generation_contract() -> None:
    generator = PROJECT_ROOT / "scripts" / "generate_pulsar_complete_report.py"
    profile_ids = [
        "BASELINE_H16_D32",
        "PHASE2_HEAP24_DIRECT48",
        "PHASE2_HEAP32_DIRECT48",
    ]

    def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        phase1 = root / "phase1"
        screening = root / "screening"
        phase2 = root / "phase2"
        final_validation = root / "final-validation"
        output = root / "complete"
        for path in (phase1 / "figures", screening, phase2, final_validation):
            path.mkdir(parents=True)
        # These are synthetic contract fixtures, not measured campaign results.
        (phase1 / "pulsar_phase1_report.tex").write_text(
            r"""\documentclass{article}
\usepackage{hyperref}
\hypersetup{
pdftitle={Verified Exploratory Analysis of Apache Pulsar Phase 1},
pdfsubject={120-configuration fixed-profile workload screening}
}
\newcommand{\mibs}{~MiB/s}
\title{Verified Exploratory Analysis of Apache Pulsar Phase 1\\
\large 120-Configuration Fixed-Profile Workload Screening}
\begin{document}
\section{Introduction}
Synthetic introduction.
\section{Executive Summary}
Synthetic summary.
\section{Benchmark Goal and Scope}
Synthetic scope.
\section{Resource and Bottleneck Evidence}
Phase 2 should retain process RSS, heap, GC, exact \texttt{ib0}, Pulsar ingress/egress, and add reliable total-direct-memory and BookKeeper queue telemetry if available.
\section{Retrospectively Corrected Phase 1 Shortlist}
Phase 2 must keep every workload field fixed within an anchor while varying only immutable Pulsar service profiles.
\section{Limitations}
Synthetic limitations.
\section{Conclusion}
Synthetic conclusion.
\appendix
\section{Synthetic Appendix}
Synthetic appendix.
\end{document}
""",
            encoding="utf-8",
        )
        (phase1 / "pulsar_phase1_summary.json").write_text(
            json.dumps(
                {
                    "case_count": 120,
                    "eligible_case_count": 120,
                    "qualified_case_count": 100,
                    "primary_leader": {
                        "config_id": "cfg_120",
                        "balanced_mib_per_sec": 250.0,
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )
        for name in (
            "pulsar_phase1_backlog_sensitivity.csv",
            "pulsar_phase1_backlog_sensitivity.json",
            "pulsar_phase1_cases.csv",
            "pulsar_phase1_cases.json",
            "pulsar_phase1_controlled_comparisons.csv",
            "pulsar_phase1_shortlist.csv",
            "pulsar_phase1_shortlist.json",
            "pulsar_phase1_validation.json",
        ):
            content = '{"fixture": "synthetic"}\n' if name.endswith(".json") else "fixture\nsynthetic\n"
            (phase1 / name).write_text(content, encoding="utf-8")
        confirmation_profiles = [
            {
                "final_rank": rank,
                "frozen_winner": profile_id == "PHASE2_HEAP24_DIRECT48",
                "candidate_passed": profile_id == "PHASE2_HEAP24_DIRECT48",
                "all_required_valid": True,
                "profile_id": profile_id,
                "repeat_count": 15,
                "eligible_repeat_count": 15,
                "qualified_repeat_count": 12,
                "sustainable_qualified_repeat_count": 12,
                "geometric_mean_median_throughput_ratio_to_baseline": ratio,
                "geometric_mean_median_p99_latency_ratio_to_baseline": 1.02,
                "max_anchor_throughput_iqr_percent": 2.0,
                "max_failed_send_percent": 0.0,
                "max_process_rss_gib": 25.0,
                "max_tmpfs_used_percent": 30.0,
                "max_producer_backlog_percent": 2.0,
            }
            for rank, profile_id, ratio in (
                (1, "PHASE2_HEAP24_DIRECT48", 1.05),
                (2, "BASELINE_H16_D32", 1.00),
                (3, "PHASE2_HEAP32_DIRECT48", 1.02),
            )
        ]
        write_csv(
            phase2 / "pulsar_phase2_confirmed_profiles.csv",
            confirmation_profiles,
        )
        repeated_cells = []
        for profile_id in profile_ids:
            for anchor in ("cfg_120", "cfg_101", "cfg_093", "cfg_001", "cfg_089"):
                repeated_cells.append(
                    {
                        "profile_id": profile_id,
                        "anchor": anchor,
                        "qualified_repeats": 0 if anchor == "cfg_089" else 3,
                        "median_balanced_mib_per_sec": 100.0,
                        "balanced_mib_per_sec_iqr": 2.0,
                        "median_balanced_records_per_sec": 25000.0,
                        "median_latency_p99_us": 1200000.0,
                        "median_flush_sec": 0.25,
                        "median_producer_backlog_percent": 2.0,
                    }
                )
        write_csv(phase2 / "pulsar_phase2_repeated_cells.csv", repeated_cells)
        (phase2 / "pulsar_phase2_confirmed_cases.csv").write_text(
            "case_id\ncase-1\n", encoding="utf-8"
        )
        (phase2 / "pulsar_phase2_confirmed_cases.json").write_text(
            '{"cases": []}\n', encoding="utf-8"
        )
        (phase2 / "pulsar_phase2_confirmation_validation.json").write_text(
            json.dumps({"valid": True, "observed_case_count": 30}) + "\n",
            encoding="utf-8",
        )
        (phase2 / "pulsar_phase2_final_selection.json").write_text(
            json.dumps(
                {
                    "status": "profile_frozen",
                    "frozen_profile_id": "PHASE2_HEAP24_DIRECT48",
                    "baseline_retained": False,
                    "source_evidence": {
                        "confirmation_results_root": "confirmation-test"
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )
        screening_rows = []
        for rank, (profile_id, heap, direct) in enumerate(
            (
                ("BASELINE_H16_D32", 16, 32),
                ("PHASE2_HEAP16_DIRECT48", 16, 48),
                ("PHASE2_HEAP24_DIRECT32", 24, 32),
                ("PHASE2_HEAP24_DIRECT48", 24, 48),
                ("PHASE2_HEAP32_DIRECT32", 32, 32),
                ("PHASE2_HEAP32_DIRECT48", 32, 48),
            ),
            1,
        ):
            screening_rows.append(
                {
                    "screening_rank": rank,
                    "profile_id": profile_id,
                    "heap_gib": heap,
                    "direct_memory_gib": direct,
                    "case_count": 5,
                    "eligible_count": 5,
                    "qualified_count": 4,
                    "sustainable_qualified_count": 4,
                    "geometric_mean_throughput_ratio_to_baseline": 1.0,
                    "geometric_mean_p99_latency_ratio_to_baseline": 1.0,
                    "max_producer_backlog_percent": 2.0,
                }
            )
        write_csv(
            screening / "pulsar_phase2_screening_profiles.csv", screening_rows
        )
        (screening / "pulsar_phase2_screening_cases.csv").write_text(
            "case_id\ncase-1\n", encoding="utf-8"
        )
        (screening / "pulsar_phase2_screening_cases.json").write_text(
            '{"cases": []}\n', encoding="utf-8"
        )
        (screening / "pulsar_phase2_screening_validation.json").write_text(
            '{"valid": true}\n', encoding="utf-8"
        )
        (screening / "pulsar_phase2_candidate_selection.json").write_text(
            json.dumps(
                {
                    "confirmation_profiles": profile_ids,
                    "source_evidence": {"screening_results_root": "screening-test"},
                }
            )
            + "\n",
            encoding="utf-8",
        )
        final_rows = []
        for rank, config_id in enumerate(
            (
                "cfg_120",
                "cfg_055",
                "cfg_080",
                "cfg_101",
                "cfg_093",
                "cfg_089",
                "cfg_001",
                "cfg_007",
                "cfg_049",
                "cfg_063",
            ),
            start=1,
        ):
            final_rows.append(
                {
                    "final_rank": rank,
                    "config_id": config_id,
                    "eligible_repeats": 5,
                    "qualified_repeats": 5 if rank <= 3 else 4,
                    "median_balanced_mib_per_sec": 300.0 - rank,
                    "balanced_mib_per_sec_iqr": 2.0,
                    "median_balanced_records_per_sec": 50000.0 - rank,
                    "median_latency_p99_us": 900000.0 + rank,
                    "median_producer_backlog_percent": (
                        2.0 if rank <= 3 else 6.0
                    ),
                    "median_max_flush_duration_sec": 0.5,
                    "max_failed_send_percent": 0.0,
                }
            )
        write_csv(
            final_validation / "pulsar_final_validation_summary.csv",
            final_rows,
        )
        (final_validation / "pulsar_final_validation_summary.json").write_text(
            json.dumps({"configurations": final_rows}) + "\n",
            encoding="utf-8",
        )
        (final_validation / "pulsar_final_validation_cases.csv").write_text(
            "case_id\ncase-1\n", encoding="utf-8"
        )
        (final_validation / "pulsar_final_validation_cases.json").write_text(
            '{"cases": []}\n', encoding="utf-8"
        )
        (final_validation / "pulsar_final_validation_validation.json").write_text(
            json.dumps(
                {
                    "valid": True,
                    "observed_case_count": 50,
                    "eligible_case_count": 50,
                    "qualified_case_count": 43,
                }
            )
            + "\n",
            encoding="utf-8",
        )
        (final_validation / "pulsar_final_validation_recommendation.json").write_text(
            json.dumps(
                {
                    "status": "validated_recommendation",
                    "config_id": "cfg_120",
                    "eligible_repeats": 5,
                    "qualified_repeats": 5,
                    "median_balanced_mib_per_sec": 299.0,
                    "median_latency_p99_us": 900001.0,
                    "source_evidence": {
                        "manifest_sha256": "a" * 64,
                        "plan_sha256": "b" * 64,
                        "results_roots": ["final-batch-1", "final-batch-2"],
                        "slurm_job_ids": ["333", "444"],
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )
        subprocess.run(
            [
                sys.executable,
                "-B",
                str(generator),
                "--phase1-dir",
                str(phase1),
                "--screening-dir",
                str(screening),
                "--phase2-dir",
                str(phase2),
                "--final-validation-dir",
                str(final_validation),
                "--output-dir",
                str(output),
                "--screening-job-id",
                "111",
                "--confirmation-job-id",
                "222",
                "--final-validation-job-id",
                "333",
                "--final-validation-job-id",
                "444",
                "--skip-figure",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        report = (output / "pulsar_complete_benchmark_report.tex").read_text(
            encoding="utf-8"
        )
        assert "Complete Apache Pulsar Benchmark Analysis" in report
        assert r"\section{Phase 2 Memory-Profile Screening and Confirmation}" in report
        assert r"\section{Final Repeated Workload Validation}" in report
        assert r"\label{tab:final-validation-ranking}" in report
        assert "Failed (\\%)" in report
        assert "p99 is secondary" in report
        assert r"\label{tab:p2-repeated-cells}" in report
        assert r"\texttt{cfg\_120}" in report
        assert r"\texttt{PHASE2\_HEAP24\_DIRECT48}" in report
        assert "30/30 eligible and 24/30 qualified" in report
        assert "45 were eligible and 36 qualified" in report
        assert "No profile tuning result is included in this report." not in report
        assert "Phase 2 should retain process RSS" not in report
        assert "Phase 2 must keep every workload field fixed" not in report
        assert "screening-test" in report
        assert "confirmation-test" in report
        assert "Slurm job \\texttt{111}" in report
        assert "Slurm job \\texttt{222}" in report
        assert r"\section{Final Validation Provenance}" in report
        assert r"\texttt{333, 444}" in report
        assert (
            output
            / "data"
            / "phase2-confirmation"
            / "pulsar_phase2_final_selection.json"
        ).is_file()
        assert (
            output
            / "data"
            / "final-validation"
            / "pulsar_final_validation_recommendation.json"
        ).is_file()
        (output / "build.aux").write_text("transient\n", encoding="utf-8")
        subprocess.run(
            [
                sys.executable,
                "-B",
                str(generator),
                "--output-dir",
                str(output),
                "--refresh-manifest-only",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        assert "build.aux" not in (output / "artifact_manifest.csv").read_text(
            encoding="utf-8"
        )
        checksum_text = (output / "SHA256SUMS").read_text(encoding="utf-8")
        assert "pulsar_complete_benchmark_report.tex" in checksum_text


def test_backend_specific_module_stack_selection() -> None:
    completed = subprocess.run(
        [
            "bash",
            "-c",
            r'''
set -euo pipefail
source "$1/scripts/hpc_modules.sh"
KAFKA_HPC_MODULES=kafka-java17-stack
PULSAR_HPC_MODULES=pulsar-java21-stack
unset SLURM_HPC_MODULES
select_backend_hpc_modules kafka
[[ "$HPC_MODULES" == kafka-java17-stack ]]
select_backend_hpc_modules pulsar
[[ "$HPC_MODULES" == pulsar-java21-stack ]]
SLURM_HPC_MODULES=explicit-job-stack
select_backend_hpc_modules kafka
[[ "$HPC_MODULES" == explicit-job-stack ]]
''',
            "bash",
            str(PROJECT_ROOT),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0


def test_portable_pulsar_case_summary_analyzer() -> None:
    analyzer = _load_script_module(
        "analyze_backend_results",
        PROJECT_ROOT / "scripts" / "analyze_backend_results.py",
    )
    config = load_benchmark_config(
        PROJECT_ROOT
        / "configs"
        / "campaigns"
        / "pulsar"
        / "examples"
        / "example_simultaneous_case.json"
    )
    adapter = get_backend("pulsar")
    report = {
        "case": {
            "case_id": "pulsar-summary-case",
            "campaign_id": "pulsar-summary-test",
            "status": "completed",
        },
        "config": config.to_dict(),
        "eligible": True,
        "latency_validation": {"enabled": True, "valid": True},
        "aggregated_metrics": {
            "producers": {
                "messages_attempted": 1_000,
                "messages_enqueued": 1_000,
                "messages_delivered": 1_000,
                "messages_failed": 0,
                "pending_messages_at_flush_start": 0,
                "max_flush_duration_sec": 0.25,
                "throughput_bytes_per_sec": 4_096_000.0,
                "throughput_msgs_per_sec": 1_000.0,
            },
            "consumers": {
                "messages_received": 995,
                "throughput_bytes_per_sec": 4_075_520.0,
                "throughput_msgs_per_sec": 995.0,
                "latency_histogram": {
                    "count": 995,
                    "p50_ns": 1_000_000,
                    "p95_ns": 2_000_000,
                    "p99_ns": 3_000_000,
                    "p999_ns": 4_000_000,
                },
            },
            "record_correctness": {
                "missing_after_drain_records": 5,
                "duplicate_offset_count": 0,
                "out_of_order_offset_count": 0,
            },
        },
    }
    enrich_result_schema(report, adapter)

    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir) / "run"
        case_dir = root / "cases" / "pulsar-summary-case"
        data_dir = case_dir / "data"
        output_dir = Path(tmpdir) / "analysis"
        data_dir.mkdir(parents=True)
        encoded = json.dumps(report)
        (case_dir / "final_report.json").write_text(encoded, encoding="utf-8")
        (data_dir / "final_report.json").write_text(encoded, encoding="utf-8")

        rows, skipped = analyzer.collect_rows(root, "pulsar")
        assert len(rows) == 1
        assert skipped == []
        row = rows[0]
        assert row["balanced_records_per_sec"] == 995.0
        assert row["latency_p99_us"] == 3_000.0
        assert row["qualified"] is True
        assert row["source_report"] == (
            "cases/pulsar-summary-case/final_report.json"
        )

        output_dir.mkdir()
        analyzer.write_csv(output_dir / "summary.csv", rows)
        analyzer.write_json(
            output_dir / "summary.json",
            backend_id="pulsar",
            rows=rows,
            skipped=skipped,
        )
        analyzer.write_markdown(
            output_dir / "summary.md",
            backend_id="pulsar",
            rows=rows,
            skipped=skipped,
        )
        payload = json.loads(
            (output_dir / "summary.json").read_text(encoding="utf-8")
        )
        assert payload["case_count"] == 1
        assert payload["backend_id"] == "pulsar"
        assert "995" in (output_dir / "summary.csv").read_text(encoding="utf-8")
        assert "995" in (output_dir / "summary.md").read_text(encoding="utf-8")


def test_fake_backend_contract_has_no_kafka_runtime_dependency() -> None:
    fake = FakeBackendAdapter()
    register_backend(fake, replace=True)
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir) / "fake_case.json"
        config_path.write_text(
            json.dumps(
                {
                    "schema_version": "messaging-benchmark.case.v1",
                    "backend_id": "fake",
                    "workload": {
                        "scenario": "simultaneous",
                        "producer_ranks": 1,
                        "consumer_ranks": 1,
                        "payload_size_bytes": 1024,
                        "duration_sec": 1,
                    },
                    "backend": {"fake": {"stream_count": 1}},
                    "campaign": {"case_id": "fake-case"},
                    "qualification_policy_id": "qualification.fake.test",
                }
            ),
            encoding="utf-8",
        )
        loaded = load_benchmark_config(config_path)
        assert loaded.backend_id == "fake"
        assert loaded.backend_settings == {"stream_count": 1}
        assert loaded.benchmark_client_count == 2

    config = BenchmarkConfig(
        backend_id="fake",
        qualification_policy_id="qualification.fake.test",
        scenario="simultaneous",
        producer_ranks=1,
        consumer_ranks=1,
        backend_settings={"stream_count": 1},
    )
    fake.validate_config(config)
    producer = fake.create_producer_worker(
        config=config,
        rank=1,
        bootstrap_servers="unused",
        producer_index=0,
        clock_offset_ns=0,
    )
    consumer = fake.create_consumer_worker(
        config=config,
        rank=2,
        bootstrap_servers="unused",
        case_id="fake_case",
        clock_offset_ns=0,
    )
    consumer.prepare()
    assert producer.run().to_dict()["messages_delivered"] == 10
    assert consumer.run().to_dict()["messages_received"] == 10
    consumer.close()
    lifecycle = fake.lifecycle_script("start", PROJECT_ROOT)
    assert lifecycle is not None
    completed = subprocess.run(
        [str(lifecycle)],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "fake lifecycle completed" in completed.stdout

    result = {
        "case": {
            "case_id": "fake-case",
            "case_name": "fake_backend_contract",
            "campaign_id": "fake-campaign",
            "status": "completed",
        },
        "config": config.to_dict(),
        "aggregated_metrics": {
            "producers": {
                "messages_attempted": 10,
                "messages_delivered": 10,
                "throughput_bytes_per_sec": 10240.0,
                "throughput_msgs_per_sec": 10.0,
            },
            "consumers": {
                "messages_received": 10,
                "throughput_bytes_per_sec": 10240.0,
                "throughput_msgs_per_sec": 10.0,
            },
            "record_correctness": {
                "missing_after_drain_records": 0,
                "invalid_envelope_count": 0,
                "duplicate_offset_count": 0,
                "out_of_order_offset_count": 0,
                "unexplained_surplus_records": 0,
            },
        },
        "latency_validation": {
            "enabled": True,
            "valid": True,
            "sample_completeness_valid": True,
        },
        "backend_health": {
            "format": "messaging-benchmark.backend-health.v1",
            "backend_id": "fake",
            "checkpoint": "post-workload",
            "status": "healthy",
            "checks": [],
            "failure_reasons": [],
        },
        "fake_counter": 7,
    }
    enrich_result_schema(result, fake)
    validate_result_schema(result)
    assert result["system_under_test"]["backend_id"] == "fake"
    assert result["backend_metrics"]["fake"]["fake_counter"] == 7
    assert result["common_metrics"]["throughput"]["balanced_records_per_sec"] == 10
    assert result["common_metrics"]["qualification"]["eligible"] is True
    assert "kafka" not in result["common_metrics"]
    assert fake.monitoring_endpoint_specs(config)["fake_metrics"]["stream_count"] == 1

    with tempfile.TemporaryDirectory() as tmpdir:
        report_input = json.loads(json.dumps(result))
        report_input["world_size"] = 3
        report_input["scenario_execution"] = {
            "active_roles": ["producer", "consumer"],
        }
        ReportBuilder(tmpdir, report_mode="light").build(report_input)
        report_markdown = (
            Path(tmpdir) / "final_report.md"
        ).read_text(encoding="utf-8")
        report_json = json.loads(
            (Path(tmpdir) / "final_report.json").read_text(encoding="utf-8")
        )
        assert "**Backend ID:** fake" in report_markdown
        assert "kafka" not in report_markdown.lower()
        assert report_json["system_under_test"]["backend_id"] == "fake"
        assert not (Path(tmpdir) / "final_report.html").exists()

    partial = {
        "config": config.to_dict(),
        "aggregated_metrics": {
            "producers": {
                "throughput_bytes_per_sec": 1024.0,
                "throughput_msgs_per_sec": 1.0,
            },
            "consumers": {},
        },
    }
    enrich_result_schema(partial, fake)
    assert (
        partial["common_metrics"]["throughput"]["balanced_records_per_sec"]
        is None
    )

    latency_result = {
        "config": config.to_dict(),
        "latency_validation": {"enabled": True, "valid": True},
        "aggregated_metrics": {
            "producers": {},
            "consumers": {
                "latency_histogram": {
                    "count": 5,
                    "mean_ns": 1_500_000,
                    "p50_ns": 1_000_000,
                    "p95_ns": 2_000_000,
                    "p99_ns": 2_500_000,
                    "p999_ns": 2_900_000,
                    "max_ns": 3_000_000,
                }
            },
        },
    }
    enrich_result_schema(latency_result, fake)
    portable_latency = latency_result["common_metrics"]["latency_end_to_end"]
    assert portable_latency["mean_us"] == 1500.0
    assert portable_latency["p99_us"] == 2500.0
    assert portable_latency["p99_9_us"] == 2900.0

    result["common_metrics"]["kafka_jmx_bytes"] = 1
    try:
        validate_result_schema(result)
    except ValueError as exc:
        assert "common_metrics" in str(exc)
    else:
        raise AssertionError("backend-specific common metric was accepted")


def test_backend_health_failure_is_ineligible_and_preserved_in_reports() -> None:
    config = load_benchmark_config(
        PROJECT_ROOT
        / "configs"
        / "campaigns"
        / "pulsar"
        / "pilots"
        / "historical"
        / "hpc_pilot_case_p0.json"
    )
    adapter = get_backend("pulsar")
    result = {
        "case": {
            "case_id": "pulsar-health-test",
            "case_name": "pulsar_health_test",
            "campaign_id": "health-test",
            "status": "completed",
        },
        "config": config.to_dict(),
        "world_size": config.total_mpi_ranks,
        "scenario_execution": {"active_roles": ["producer", "consumer"]},
        "aggregated_metrics": {
            "producers": {
                "messages_attempted": 100,
                "messages_enqueued": 100,
                "messages_delivered": 100,
                "messages_failed": 0,
                "pending_messages_at_flush_start": 0,
                "max_flush_duration_sec": 1.0,
                "throughput_bytes_per_sec": 409600.0,
                "throughput_msgs_per_sec": 100.0,
            },
            "consumers": {
                "messages_received": 100,
                "throughput_bytes_per_sec": 409600.0,
                "throughput_msgs_per_sec": 100.0,
            },
            "record_correctness": {
                "missing_after_drain_records": 0,
                "invalid_envelope_count": 0,
                "duplicate_offset_count": 0,
                "out_of_order_offset_count": 0,
                "unexplained_surplus_records": 0,
            },
        },
        "latency_validation": {
            "enabled": True,
            "valid": True,
            "sample_completeness_valid": True,
        },
    }
    healthy = {
        "format": "messaging-benchmark.backend-health.v1",
        "backend_id": "pulsar",
        "checkpoint": "post-workload",
        "status": "healthy",
        "checks": [],
        "failure_reasons": [],
    }
    merge_backend_health_into_result(result, healthy)
    enrich_result_schema(result, adapter)
    assert result["case"]["status"] == "completed"
    assert result["common_metrics"]["qualification"]["eligible"] is True
    assert result["common_metrics"]["qualification"]["qualified"] is True

    incomplete = json.loads(json.dumps(result))
    incomplete.pop("eligible", None)
    incomplete.pop("eligibility", None)
    incomplete["aggregated_metrics"]["record_correctness"][
        "missing_after_drain_records"
    ] = 3
    incomplete["latency_validation"] = {
        "enabled": True,
        "valid": False,
        "reason": "Latency sample completeness check failed",
    }
    enrich_result_schema(incomplete, adapter)
    incomplete_qualification = incomplete["common_metrics"]["qualification"]
    assert incomplete_qualification["eligible"] is False
    assert incomplete_qualification["qualified"] is False
    incomplete_failures = " ".join(
        incomplete["eligibility"]["failure_reasons"]
    )
    assert "records missing after drain: 3" in incomplete_failures
    assert "Latency sample completeness check failed" in incomplete_failures

    failed = {
        "format": "messaging-benchmark.backend-health.v1",
        "backend_id": "pulsar",
        "checkpoint": "post-workload",
        "checked_at": "2026-08-04T00:00:00+00:00",
        "status": "failed",
        "checks": [
            {
                "name": "service_process",
                "status": "fail",
                "detail": "Pulsar process exited",
            }
        ],
        "failure_reasons": ["Pulsar log contains an out-of-memory failure"],
    }
    merge_backend_health_into_result(result, failed)
    enrich_result_schema(result, adapter)
    qualification = result["common_metrics"]["qualification"]
    assert result["case"]["status"] == "failed"
    assert result["case"]["workload_status"] == "completed"
    assert qualification["eligible"] is False
    assert qualification["qualified"] is False
    assert "out-of-memory" in " ".join(
        result["eligibility"]["failure_reasons"]
    )

    assert adapter.lifecycle_script("check-health", PROJECT_ROOT).is_file()
    assert get_backend("kafka").lifecycle_script(
        "check-health", PROJECT_ROOT
    ).is_file()

    with tempfile.TemporaryDirectory() as tmpdir:
        output = Path(tmpdir)
        initial = json.loads(json.dumps(result))
        initial["case"]["status"] = "completed"
        initial["case"].pop("workload_status", None)
        initial.pop("backend_health", None)
        initial.pop("eligible", None)
        initial.pop("eligibility", None)
        ReportBuilder(output, report_mode="light").build(initial)
        health_path = output / "runtime" / "backend_health.json"
        health_path.parent.mkdir(parents=True, exist_ok=True)
        health_path.write_text(json.dumps(failed), encoding="utf-8")
        assert apply_backend_health_to_report_files(output) is True
        report_json = json.loads(
            (output / "final_report.json").read_text(encoding="utf-8")
        )
        report_markdown = (output / "final_report.md").read_text(
            encoding="utf-8"
        )
        assert report_json["common_metrics"]["qualification"]["eligible"] is False
        assert "## Backend Runtime Health" in report_markdown
        assert "## Measurement Eligibility" in report_markdown
        assert "Pulsar process exited" in report_markdown


def test_backend_batch_plan_and_lifecycle_contract() -> None:
    planner = _load_script_module(
        "plan_backend_batch_test",
        PROJECT_ROOT / "scripts" / "plan_backend_batch.py",
    )
    manifest = (
        PROJECT_ROOT
        / "configs"
        / "campaigns"
        / "pulsar"
        / "pilots"
        / "batch_runner_10_manifest.csv"
    )
    plan = planner.build_batch_plan(
        "pulsar",
        manifest,
        PROJECT_ROOT,
        require_one_profile=True,
    )
    assert plan["case_count"] == 10
    assert plan["node_count"] == 4
    assert plan["service_nodes"] == 1
    assert plan["monitoring_nodes"] == 1
    assert plan["max_producer_ranks"] == 64
    assert plan["max_consumer_ranks"] == 64
    assert plan["max_total_mpi_ranks"] == 129
    assert plan["total_case_budget_sec"] == 1050
    assert plan["profile_id"] == "BASELINE_H16_D32"
    assert plan["profile_sha256"] == (
        "7b85a5388524a2e598f181330a4c2e585823d9bc40dab66e11603ee484d6068c"
    )
    assert [row["config_id"] for row in plan["rows"]] == [
        "cfg_001",
        "cfg_007",
        "cfg_013",
        "cfg_087",
        "cfg_049",
        "cfg_021",
        "cfg_018",
        "cfg_029",
        "cfg_017",
        "cfg_041",
    ]
    assert {row["warmup_sec"] for row in plan["rows"]} == {15}
    assert {row["duration_sec"] for row in plan["rows"]} == {30}
    assert {row["drain_timeout_sec"] for row in plan["rows"]} == {60}
    assert len({row["topic_name"] for row in plan["rows"]}) == 10

    adapter = get_backend("pulsar")
    delete_stream = adapter.lifecycle_script("delete-stream", PROJECT_ROOT)
    assert delete_stream is not None and delete_stream.is_file()
    create_stream_text = adapter.lifecycle_script(
        "create-stream", PROJECT_ROOT
    ).read_text(encoding="utf-8")
    assert 'ensure_dir "$CASE_DIR/logs/pulsar"' in create_stream_text
    dispatcher = (PROJECT_ROOT / "scripts" / "backend_lifecycle.sh").read_text(
        encoding="utf-8"
    )
    submitter = (PROJECT_ROOT / "scripts" / "submit_backend_batch.sh").read_text(
        encoding="utf-8"
    )
    facade = (PROJECT_ROOT / "benchmark.sh").read_text(encoding="utf-8")
    runner = (PROJECT_ROOT / "scripts" / "run_backend_batch.sh").read_text(
        encoding="utf-8"
    )
    assert "delete-stream" in dispatcher
    assert "--exclusive" in submitter
    assert "check_pulsar_phase1_gate.py" in facade
    assert 'backend_lifecycle.sh" "$BACKEND_ID" start "$CONFIG_PATH"' in runner
    assert 'backend_lifecycle.sh" "$BACKEND_ID" delete-stream' in runner
    assert "for plan_line in" in runner
    assert "stop_backend_for_case" in runner
    assert "reset_backend_ram_runtime" in runner
    assert "BACKEND_STREAM_REQUIRE_TMPFS_RECOVERY=0" in runner
    assert runner.index("for plan_line in") < runner.rindex(
        'backend_lifecycle.sh" "$BACKEND_ID" start "$CONFIG_PATH"'
    )
    delete_stream_text = delete_stream.read_text(encoding="utf-8")
    assert "BACKEND_STREAM_REQUIRE_TMPFS_RECOVERY" in delete_stream_text

    validator = _load_script_module(
        "validate_backend_batch_test",
        PROJECT_ROOT / "scripts" / "validate_backend_batch.py",
    )
    summary = {
        "case_count": 10,
        "completed_count": 10,
        "failed_count": 0,
        "skipped_count": 0,
        "cases": [
            {"case_id": row["case_id"], "status": "completed"}
            for row in plan["rows"]
        ],
    }
    validation = validator._validate_batch_summary(
        summaries=[(Path("batch_summary.json"), summary)],
        expected_case_ids={row["case_id"] for row in plan["rows"]},
    )
    assert validation["valid"] is True

    with tempfile.TemporaryDirectory() as tmpdir:
        invalid_manifest = Path(tmpdir) / "invalid.csv"
        source = manifest.read_text(encoding="utf-8")
        invalid_manifest.write_text(
            source.replace("pulsar-batch10-cfg-041", "bad case", 1),
            encoding="utf-8",
        )
        try:
            planner.build_batch_plan(
                "pulsar",
                invalid_manifest,
                PROJECT_ROOT,
                require_one_profile=True,
            )
        except ValueError as exc:
            assert "unsafe case_id" in str(exc)
        else:
            raise AssertionError("Unsafe batch manifest token was accepted")


def run_all() -> None:
    test_primary_config_loads()
    test_high_throughput_configs_load_client_and_broker_tuning()
    test_ordinary_iteration_configs_load_and_validate()
    test_v1_rejects_multi_broker_configs()
    test_mpi_role_assignment_for_first_case()
    test_submit_wrapper_defaults_to_four_node_role_split()
    test_simultaneous_budgeted_sweep_generation_and_batches()
    test_sweep_scripts_and_light_report_mode_contracts()
    test_report_builder_light_mode_skips_html()
    test_report_builder_machine_mode_writes_json_only()
    test_sweep_aggregation_and_analysis_from_synthetic_reports()
    test_complete_analysis_qualification_shortlist_and_validation_manifest()
    test_report_builder_writes_mpi_report_without_monitoring()
    test_html_report_uses_scenario_specific_measured_roles()
    test_producer_flush_diagnostics_are_aggregated()
    test_librdkafka_stats_tracker_and_aggregate()
    test_report_builder_merges_system_inventory_and_embeds_html()
    test_report_bottleneck_analysis_uses_iperf_and_exporter_lag_warning()
    test_case_comparison_markdown_highlights_key_metrics()
    test_report_broker_jmx_falls_back_to_one_minute_rates()
    test_system_inventory_parsers()
    test_system_inventory_merge_marks_failed_required_iperf()
    test_monitoring_svg_embeds_event_markers()
    test_monitoring_bundle_report_has_clean_exporter_sections()
    test_v2_config_envelope_and_rate_contracts()
    test_record_envelope_preserves_payload_and_detects_corruption()
    test_consumer_offset_checks_exclude_warmup_records()
    test_latency_histogram_merge_and_negative_rejection()
    test_clock_offset_estimator_uses_lowest_rtt_bound()
    test_metrics_aggregator_merges_v2_latency_and_drain_evidence()
    test_reproducible_kafka_v1_campaign_manifests()
    test_reproducible_kafka_phase1_selection_is_result_driven()
    test_reproducible_kafka_measurement_and_case_isolation_contract()
    test_broker_profile_hash_and_campaign_manifests()
    test_v2_screening_manifest_is_deterministic_and_complete()
    test_v2_final_ranking_keeps_latency_secondary()
    test_v2_runtime_java_version_contract()
    test_v2_iperf_capacity_extraction_is_directional()
    test_v2_fixed_sampling_pilot_uses_paired_overhead()
    test_v2_interim_report_populates_observed_case_tables()
    test_broker_profile_detects_manifest_tampering()
    test_backend_registry_and_versioned_kafka_config_equivalence()
    test_pulsar_backend_config_profile_and_monitoring_contract()
    test_pulsar_campaign_generator_and_coordinated_drain_state()
    test_pulsar_phase1_gate_and_strict_analysis()
    test_pulsar_phase2_memory_screen_design()
    test_pulsar_phase2_screening_selection_contract()
    test_pulsar_phase2_confirmation_design()
    test_pulsar_final_validation_design()
    test_pulsar_final_validation_aggregation_and_ranking()
    test_pulsar_phase2_final_freeze_rule()
    test_pulsar_complete_report_generation_contract()
    test_backend_specific_module_stack_selection()
    test_portable_pulsar_case_summary_analyzer()
    test_fake_backend_contract_has_no_kafka_runtime_dependency()
    test_backend_health_failure_is_ineligible_and_preserved_in_reports()
    test_backend_batch_plan_and_lifecycle_contract()


if __name__ == "__main__":
    run_all()
