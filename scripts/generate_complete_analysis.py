#!/usr/bin/env python3
from __future__ import annotations

import csv
import argparse
import hashlib
import html
import json
import math
import os
import random
import re
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PUBLISHED_ROOT = (
    PROJECT_ROOT / "results" / "published" / "kafka" / "v1"
)
sys.path.insert(0, str(PROJECT_ROOT / "analysis_deps"))
os.environ.setdefault(
    "MPLCONFIGDIR", str(PROJECT_ROOT / ".cache" / "kafka-benchmark-mplconfig")
)

import numpy as np

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


REPORT_ROOT = DEFAULT_PUBLISHED_ROOT
COMBINED_CSV: Path | None = None
COMBINED_JSON: Path | None = None
VERIFIED_DATASET: Path | None = DEFAULT_PUBLISHED_ROOT / "analysis_verified_dataset.csv"
FINAL_REPORT_ROOT: Path | None = PROJECT_ROOT / "analysis_inputs" / "remote_reports"
CONFIG_MANIFEST = PROJECT_ROOT / "configs" / "sweeps" / "simultaneous_budgeted" / "sweep_manifest.csv"
CONFIGS_ROOT = PROJECT_ROOT / "configs" / "sweeps" / "simultaneous_budgeted" / "generated_configs"
PLOTS_DIR = DEFAULT_PUBLISHED_ROOT / "analysis_plots"
VALIDATION_SUMMARY_CSV: Path | None = None
VALIDATION_REPEATS_CSV: Path | None = None
VALIDATION_SEED = 20260716
QUALIFICATION_RULE_VERSION = "qualification.application.v1"
BYTES_PER_MIB = 1_048_576

INPUT_CONTEXT: dict[str, Any] = {
    "dataset_source": "",
    "dataset_source_kind": "",
    "combined_json": "",
    "final_reports_root": "",
    "final_reports_loaded": 0,
    "config_manifest": "",
    "configs_root": "",
    "validation_summary": "not available",
    "validation_repeats": "not available",
    "validation_job_id": "",
    "validation_run_id": "",
    "validation_rows_loaded": 0,
    "validation_repeat_rows_loaded": 0,
    "generation_timestamp": "",
    "record_throughput_issues": 0,
    "record_throughput_rows_validated": 0,
    "validation_record_throughput_issues": 0,
}

BACKLOG_LIMIT = 5.0
FLUSH_LIMIT = 10.0
FAILED_SEND_LIMIT = 0.1

CONFIG_FIELDS = [
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
]

PERFORMANCE_FIELDS = [
    "producer_attempt_MBps",
    "producer_delivered_MBps",
    "consumer_received_MBps",
    "balanced_app_MBps",
    "producer_records_per_sec",
    "consumer_records_per_sec",
    "balanced_records_per_sec",
    "broker_ingress_MBps",
    "broker_egress_MBps",
    "broker_combined_MBps",
    "pending_backlog_percent",
    "flush_sec",
    "failed_send_percent",
]

RESOURCE_FIELDS = [
    "broker_cpu_peak_percent",
    "producer_cpu_peak_percent",
    "consumer_cpu_peak_percent",
    "monitoring_cpu_peak_percent",
    "broker_ram_peak_GB",
    "producer_ram_peak_GB",
    "consumer_ram_peak_GB",
    "monitoring_ram_peak_GB",
    "broker_network_rx_peak_MBps",
    "broker_network_tx_peak_MBps",
    "producer_network_rx_peak_MBps",
    "producer_network_tx_peak_MBps",
    "consumer_network_rx_peak_MBps",
    "consumer_network_tx_peak_MBps",
    "producer_to_broker_iperf_MBps",
    "broker_to_consumer_iperf_MBps",
    "broker_combined_peak_MBps",
    "broker_request_latency_avg_ms",
]

MODEL_FEATURES = [
    "producer_ranks",
    "consumer_ranks",
    "total_worker_ranks",
    "producer_consumer_rank_ratio",
    "log2_partitions",
    "log2_batch_size",
    "linger_ms",
    "log2_payload_size_bytes",
    "log2_producer_queue_messages",
    "log2_producer_queue_kbytes",
    "log2_consumer_fetch_min_bytes",
    "consumer_fetch_wait_max_ms",
    "log2_consumer_fetch_message_max_bytes",
]

RETIRED_SCORE_FIELDS = {
    "stable_score",
    "legacy_stable_score",
    "stable_score_recalc",
    "stable_score_mismatch",
    "legacy_score_rank",
}


def main(argv: list[str] | None = None) -> int:
    configure_from_args(parse_args(argv))
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    rows = load_verified_rows()
    validation_summary_rows, validation_repeat_rows = load_validation_results(rows)
    manifest_rows = load_manifest()
    write_dataset(rows)
    stats_rows = write_descriptive_stats(rows)
    controlled_rows = write_controlled_comparisons(rows)
    correlation_rows = write_correlations(rows)
    model_rows, feature_importance = run_models(rows)
    pareto_rows = write_pareto(rows)
    qualification_sensitivity_rows = write_qualification_sensitivity(rows)
    shortlist_rows = write_validation_shortlist(rows, controlled_rows, manifest_rows)
    repetition_rows = write_repetition_plan(shortlist_rows)
    validation_run_rows = write_validation_manifests(shortlist_rows)
    write_validation_result_artifacts(validation_summary_rows, validation_repeat_rows)
    resource_quality_rows = write_resource_data_quality(rows)
    artifact_rows: list[dict[str, Any]] = []
    make_plots(rows, correlation_rows, feature_importance, pareto_rows, shortlist_rows)
    write_summary(
        rows=rows,
        stats_rows=stats_rows,
        controlled_rows=controlled_rows,
        correlation_rows=correlation_rows,
        model_rows=model_rows,
        pareto_rows=pareto_rows,
        qualification_sensitivity_rows=qualification_sensitivity_rows,
        shortlist_rows=shortlist_rows,
        resource_quality_rows=resource_quality_rows,
        validation_summary_rows=validation_summary_rows,
        validation_repeat_rows=validation_repeat_rows,
    )
    write_latex_report(
        rows=rows,
        stats_rows=stats_rows,
        controlled_rows=controlled_rows,
        correlation_rows=correlation_rows,
        model_rows=model_rows,
        pareto_rows=pareto_rows,
        qualification_sensitivity_rows=qualification_sensitivity_rows,
        repetition_rows=repetition_rows,
        shortlist_rows=shortlist_rows,
        resource_quality_rows=resource_quality_rows,
        validation_summary_rows=validation_summary_rows,
        validation_repeat_rows=validation_repeat_rows,
    )
    write_html_report(
        rows=rows,
        controlled_rows=controlled_rows,
        correlation_rows=correlation_rows,
        model_rows=model_rows,
        pareto_rows=pareto_rows,
        qualification_sensitivity_rows=qualification_sensitivity_rows,
        repetition_rows=repetition_rows,
        shortlist_rows=shortlist_rows,
        resource_quality_rows=resource_quality_rows,
        validation_summary_rows=validation_summary_rows,
        validation_repeat_rows=validation_repeat_rows,
    )
    artifact_rows = write_artifact_manifest()
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate verified exploratory analysis artifacts for the Kafka 120-config sweep."
    )
    parser.add_argument("--verified-dataset", type=Path, default=None)
    parser.add_argument("--combined-csv", type=Path, default=None)
    parser.add_argument("--combined-json", type=Path, default=None)
    parser.add_argument("--final-reports-root", type=Path, default=None)
    parser.add_argument("--config-manifest", type=Path, default=None)
    parser.add_argument("--configs-root", type=Path, default=None)
    parser.add_argument("--validation-summary", type=Path, default=None)
    parser.add_argument("--validation-repeats", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_PUBLISHED_ROOT)
    return parser.parse_args(argv)


def configure_from_args(args: argparse.Namespace) -> None:
    global REPORT_ROOT, PLOTS_DIR, COMBINED_CSV, COMBINED_JSON, VERIFIED_DATASET
    global FINAL_REPORT_ROOT, CONFIG_MANIFEST, CONFIGS_ROOT, VALIDATION_SUMMARY_CSV
    global VALIDATION_REPEATS_CSV

    REPORT_ROOT = resolve_input_path(args.output_dir) if args.output_dir else PROJECT_ROOT
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR = REPORT_ROOT / "analysis_plots"

    CONFIG_MANIFEST = resolve_input_path(args.config_manifest) if args.config_manifest else CONFIG_MANIFEST
    CONFIGS_ROOT = resolve_input_path(args.configs_root) if args.configs_root else CONFIGS_ROOT
    FINAL_REPORT_ROOT = (
        resolve_input_path(args.final_reports_root)
        if args.final_reports_root
        else PROJECT_ROOT / "analysis_inputs" / "remote_reports"
    )
    if FINAL_REPORT_ROOT and not FINAL_REPORT_ROOT.exists():
        FINAL_REPORT_ROOT = None

    explicit_combined = resolve_input_path(args.combined_csv) if args.combined_csv else None
    default_combined = first_existing_path(
        [
            PROJECT_ROOT
            / "results"
            / "sweeps"
            / "simultaneous_budgeted"
            / "combined_hpc_fixed_20260713"
            / "sweep_summary_all.csv",
            PROJECT_ROOT / "sweep_summary_all.csv",
        ]
    )
    COMBINED_CSV = explicit_combined if explicit_combined else default_combined

    explicit_json = resolve_input_path(args.combined_json) if args.combined_json else None
    default_json = first_existing_path(
        [
            PROJECT_ROOT
            / "results"
            / "sweeps"
            / "simultaneous_budgeted"
            / "combined_hpc_fixed_20260713"
            / "sweep_summary_all.json",
            PROJECT_ROOT / "sweep_summary_all.json",
        ]
    )
    COMBINED_JSON = explicit_json if explicit_json else default_json

    VERIFIED_DATASET = (
        resolve_input_path(args.verified_dataset)
        if args.verified_dataset
        else first_existing_path(
            [
                REPORT_ROOT / "analysis_verified_dataset.csv",
                DEFAULT_PUBLISHED_ROOT / "analysis_verified_dataset.csv",
                PROJECT_ROOT / "analysis_verified_dataset.csv",
            ]
        )
    )

    VALIDATION_SUMMARY_CSV = (
        resolve_input_path(args.validation_summary)
        if args.validation_summary
        else (
            find_latest_validation_artifact("validation_summary_by_original.csv")
            or first_existing_path(
                [
                    REPORT_ROOT / "analysis_validation_results.csv",
                    DEFAULT_PUBLISHED_ROOT / "analysis_validation_results.csv",
                ]
            )
        )
    )
    if VALIDATION_SUMMARY_CSV and not VALIDATION_SUMMARY_CSV.is_file():
        VALIDATION_SUMMARY_CSV = None
    VALIDATION_REPEATS_CSV = (
        resolve_input_path(args.validation_repeats)
        if args.validation_repeats
        else (
            infer_validation_repeats_csv(VALIDATION_SUMMARY_CSV)
            or first_existing_path(
                [
                    REPORT_ROOT / "analysis_validation_repeats.csv",
                    DEFAULT_PUBLISHED_ROOT / "analysis_validation_repeats.csv",
                ]
            )
        )
    )
    if VALIDATION_REPEATS_CSV and not VALIDATION_REPEATS_CSV.is_file():
        VALIDATION_REPEATS_CSV = None

    INPUT_CONTEXT.update(
        {
            "combined_json": display_path(COMBINED_JSON) if COMBINED_JSON else "not used",
            "final_reports_root": display_path(FINAL_REPORT_ROOT) if FINAL_REPORT_ROOT else "not available",
            "config_manifest": display_path(CONFIG_MANIFEST),
            "configs_root": display_path(CONFIGS_ROOT),
            "validation_summary": display_path(VALIDATION_SUMMARY_CSV) if VALIDATION_SUMMARY_CSV else "not available",
            "validation_repeats": display_path(VALIDATION_REPEATS_CSV) if VALIDATION_REPEATS_CSV else "not available",
            "generation_timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }
    )


def resolve_input_path(path: Path) -> Path:
    return path if path.is_absolute() else PROJECT_ROOT / path


def first_existing_path(paths: list[Path]) -> Path | None:
    for path in paths:
        if path.is_file():
            return path
    return None


def find_latest_validation_artifact(filename: str) -> Path | None:
    candidates = [
        path
        for root in [REPORT_ROOT, PROJECT_ROOT]
        for path in (root / "results" / "sweeps" / "simultaneous_validation").glob(
            f"**/{filename}"
        )
        if path.is_file()
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path.stat().st_mtime, str(path)))


def infer_validation_repeats_csv(summary_path: Path | None) -> Path | None:
    if summary_path is None:
        return find_latest_validation_artifact("validation_repeats_by_case.csv")
    candidate = summary_path.with_name("validation_repeats_by_case.csv")
    return candidate if candidate.is_file() else None


def load_validation_results(
    verified_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    summary_rows = load_validation_csv(VALIDATION_SUMMARY_CSV)
    repeat_rows = load_validation_csv(VALIDATION_REPEATS_CSV)
    for row in summary_rows:
        row["config_id"] = row.get("original_config_id", row.get("config_id", ""))
    for row in repeat_rows:
        row["config_id"] = row.get("original_config_id", row.get("config_id", ""))
    record_issues = enrich_validation_record_throughput(
        summary_rows, repeat_rows, verified_rows
    )
    if record_issues:
        for issue in record_issues:
            print(f"record-throughput validation: {issue}", file=sys.stderr)
        raise ValueError(
            f"Cannot derive validation record throughput for {len(record_issues)} row(s)"
        )
    summary_rows.sort(key=validation_rank_key)
    for rank, row in enumerate(summary_rows, start=1):
        row["validation_rank"] = rank
    INPUT_CONTEXT.update(
        {
            "validation_rows_loaded": len(summary_rows),
            "validation_repeat_rows_loaded": len(repeat_rows),
            "validation_record_throughput_issues": len(record_issues),
        }
    )
    if VALIDATION_SUMMARY_CSV:
        INPUT_CONTEXT.update(extract_validation_context(VALIDATION_SUMMARY_CSV))
    return summary_rows, repeat_rows


def enrich_validation_record_throughput(
    summary_rows: list[dict[str, Any]],
    repeat_rows: list[dict[str, Any]],
    verified_rows: list[dict[str, Any]],
) -> list[str]:
    payload_by_config = {
        str(row.get("config_id", "")): row.get("payload_size_bytes", "")
        for row in verified_rows
    }
    issues: list[str] = []
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    completed_before = sum(
        1 for row in repeat_rows if str(row.get("status", "")).lower() == "completed"
    )
    for row in repeat_rows:
        config_id = str(row.get("original_config_id", row.get("config_id", "")))
        if row.get("payload_size_bytes", "") in (None, ""):
            row["payload_size_bytes"] = payload_by_config.get(config_id, "")
        for field in [
            "payload_size_bytes",
            "producer_delivered_MBps",
            "consumer_received_MBps",
            "balanced_app_MBps",
            "producer_records_per_sec",
            "consumer_records_per_sec",
            "balanced_records_per_sec",
        ]:
            if field in row:
                row[field] = normalize_number(row[field])
        issues.extend(derive_record_throughput_fields(row, row.get("case_id") or config_id))
        grouped[config_id].append(row)

    completed_after = sum(
        1 for row in repeat_rows if str(row.get("status", "")).lower() == "completed"
    )
    if completed_after != completed_before:
        issues.append(
            f"completed repeat count changed from {completed_before} to {completed_after}"
        )
    issues.extend(validate_record_throughput_rows(repeat_rows, "validation repeat"))

    for summary in summary_rows:
        config_id = str(
            summary.get("original_config_id", summary.get("config_id", ""))
        )
        config_repeats = grouped.get(config_id, [])
        payloads = {
            int(fnum(row.get("payload_size_bytes")))
            for row in config_repeats
            if math.isfinite(fnum(row.get("payload_size_bytes")))
        }
        summary["payload_size_bytes"] = (
            next(iter(payloads))
            if len(payloads) == 1
            else ("varies" if len(payloads) > 1 else "")
        )
        for source_field, summary_field in [
            ("producer_records_per_sec", "median_producer_records_per_sec"),
            ("consumer_records_per_sec", "median_consumer_records_per_sec"),
            ("balanced_records_per_sec", "median_balanced_records_per_sec"),
        ]:
            values = [
                fnum(row.get(source_field))
                for row in config_repeats
                if math.isfinite(fnum(row.get(source_field)))
            ]
            summary[summary_field] = statistics.median(values) if values else ""
    return issues


def load_validation_csv(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.is_file():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def validation_rank_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        -fnum(row.get("qualified_count"), default=0.0),
        -fnum(row.get("median_balanced_app_MBps"), default=0.0),
        fnum(
            row.get("iqr_balanced_app_MBps", row.get("cv_balanced_app_MBps")),
            default=math.inf,
        ),
        fnum(row.get("median_pending_backlog_percent"), default=math.inf),
        fnum(row.get("median_flush_sec"), default=math.inf),
        fnum(row.get("median_failed_send_percent"), default=math.inf),
        str(row.get("original_config_id", row.get("config_id", ""))),
    )


def extract_validation_context(summary_path: Path) -> dict[str, Any]:
    text = summary_path.as_posix()
    job_id = ""
    run_id = ""
    job_match = re.search(r"job_(\d+)", text)
    if job_match:
        job_id = job_match.group(1)
    run_match = re.search(r"(run_[^/]+)", text)
    if run_match:
        run_id = run_match.group(1)
    context = {
        "validation_job_id": job_id,
        "validation_run_id": run_id,
    }
    log_path = PROJECT_ROOT / "logs" / f"slurm-{job_id}.out" if job_id else None
    if log_path and log_path.is_file():
        context.update(parse_validation_slurm_log(log_path))
    return context


def parse_validation_slurm_log(log_path: Path) -> dict[str, Any]:
    context: dict[str, Any] = {"validation_slurm_log": display_path(log_path)}
    patterns = {
        "validation_nodes": r"Allocated nodes: ([^\n]+)",
        "validation_elapsed": r"Elapsed: ([^,\n]+)",
        "validation_cpus_nodes": r"CPUs: ([^,\n]+), Nodes: ([^\n]+)",
        "validation_started": r"Started: ([^\n]+)",
        "validation_ended": r"Ended: ([^\n]+)",
    }
    text = log_path.read_text(encoding="utf-8", errors="replace")
    for key, pattern in patterns.items():
        match = re.search(pattern, text)
        if not match:
            continue
        if key == "validation_cpus_nodes":
            context["validation_cpus"] = match.group(1).strip()
            context["validation_node_count"] = match.group(2).strip()
        else:
            context[key] = match.group(1).strip()
    return context


def load_verified_rows() -> list[dict[str, Any]]:
    source = COMBINED_CSV if COMBINED_CSV and COMBINED_CSV.is_file() else None
    source_kind = "combined_csv"
    if source is None and VERIFIED_DATASET and VERIFIED_DATASET.is_file():
        source = VERIFIED_DATASET
        source_kind = "verified_dataset"
    if source is None:
        raise FileNotFoundError(
            "No usable dataset exists. Provide --combined-csv or --verified-dataset, "
            "or keep analysis_verified_dataset.csv in the repository root."
        )
    reports = load_final_reports()
    manifest = load_manifest()
    rows: list[dict[str, Any]] = []
    record_issues: list[str] = []
    completed_before = 0
    with source.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            if str(raw.get("status", "")).lower() == "completed":
                completed_before += 1
            row = dict(raw)
            for retired_field in RETIRED_SCORE_FIELDS:
                row.pop(retired_field, None)
            cfg_id = row["config_id"]
            report = reports.get(cfg_id)
            row["final_report_available"] = row.get("final_report_available") or (
                "yes" if report else "no"
            )
            if report and source_kind == "combined_csv":
                enrich_from_report(row, report)
            enrich_from_manifest(row, manifest.get(cfg_id, {}))
            record_issues.extend(enrich_derived_fields(row))
            rows.append(row)
    rows.sort(key=lambda row: config_sort_key(str(row["config_id"])))
    assign_ranks(rows)
    completed_after = sum(
        1 for row in rows if str(row.get("status", "")).lower() == "completed"
    )
    if completed_after != completed_before:
        record_issues.append(
            f"completed sweep count changed from {completed_before} to {completed_after}"
        )
    record_issues.extend(validate_record_throughput_rows(rows, "verified sweep"))
    if record_issues:
        for issue in record_issues:
            print(f"record-throughput validation: {issue}", file=sys.stderr)
        raise ValueError(
            f"Cannot derive verified record throughput for {len(record_issues)} row(s)"
        )
    INPUT_CONTEXT.update(
        {
            "dataset_source": display_path(source),
            "dataset_source_path": str(source),
            "dataset_source_kind": source_kind,
            "final_reports_loaded": len(reports),
            "record_throughput_issues": len(record_issues),
            "record_throughput_rows_validated": len(rows),
            "completed_rows_before_record_derivation": completed_before,
            "completed_rows_after_record_derivation": completed_after,
        }
    )
    return rows


def load_final_reports() -> dict[str, dict[str, Any]]:
    reports: dict[str, dict[str, Any]] = {}
    if FINAL_REPORT_ROOT is None:
        return reports
    for path in sorted(FINAL_REPORT_ROOT.glob("**/final_report.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        cfg_id = str(
            dict_value(payload.get("config")).get("extra", {}).get("sweep_config_id")
            or dict_value(payload.get("case")).get("case_id")
            or path.parent.name
        )
        reports[cfg_id] = payload
    return reports


def load_manifest() -> dict[str, dict[str, Any]]:
    if not CONFIG_MANIFEST.is_file():
        return {}
    with CONFIG_MANIFEST.open("r", encoding="utf-8", newline="") as handle:
        return {row["config_id"]: row for row in csv.DictReader(handle)}


def enrich_from_manifest(row: dict[str, Any], manifest_row: dict[str, Any]) -> None:
    if not manifest_row:
        return
    row["manifest_note"] = manifest_row.get("notes", row.get("manifest_note", ""))
    row["is_manifest_baseline"] = (
        "yes" if str(manifest_row.get("notes", "")).strip().lower() == "baseline" else "no"
    )
    for field in ["config_path", "topic_name", "scenario", *CONFIG_FIELDS]:
        if not row.get(field) and manifest_row.get(field):
            row[field] = manifest_row[field]


def display_path(path: Path | None) -> str:
    if path is None:
        return "not available"
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError:
        return path.name


def enrich_from_report(row: dict[str, Any], report: dict[str, Any]) -> None:
    config = dict_value(report.get("config"))
    producers = dict_value(dict_value(report.get("aggregated_metrics")).get("producers"))
    consumers = dict_value(dict_value(report.get("aggregated_metrics")).get("consumers"))
    verdict = dict_value(report.get("throughput_verdict"))
    analysis = dict_value(report.get("bottleneck_analysis"))
    system = dict_value(report.get("system_inventory"))
    monitoring = dict_value(report.get("monitoring"))
    common = dict_value(report.get("common_metrics"))
    common_throughput = dict_value(common.get("throughput"))
    common_delivery = dict_value(common.get("producer_delivery"))
    common_latency = dict_value(common.get("latency_end_to_end"))
    common_qualification = dict_value(common.get("qualification"))

    row["qualification_policy_id"] = (
        common_qualification.get("policy_id")
        or config.get("qualification_policy_id")
        or ""
    )
    row["eligible"] = common_qualification.get("eligible", "")
    row["latency_valid"] = common_latency.get("valid", "")
    row["latency_sample_count"] = common_latency.get("count", "")
    row["latency_p50_us"] = common_latency.get("p50_us", "")
    row["latency_p95_us"] = common_latency.get("p95_us", "")
    row["latency_p99_us"] = common_latency.get("p99_us", "")
    row["backlog_denominator"] = common_delivery.get(
        "backlog_denominator", ""
    )

    producer_attempt_bytes = first_number(
        producers.get("send_attempt_throughput_bytes_per_sec")
    )
    row["producer_attempt_MBps"] = (
        producer_attempt_bytes / 1_048_576.0
        if producer_attempt_bytes is not None
        else first_number(
            producers.get("send_attempt_throughput_mebibytes_per_sec"),
            verdict.get("producer_attempt_mb_s"),
        )
    )
    row["producer_mib_per_sec"] = first_number(
        common_throughput.get("producer_mib_per_sec")
    )
    row["consumer_mib_per_sec"] = first_number(
        common_throughput.get("consumer_mib_per_sec")
    )
    row["balanced_mib_per_sec"] = first_number(
        common_throughput.get("balanced_mib_per_sec")
    )
    row["clean_sustainable_MBps"] = first_number(
        common_throughput.get("balanced_mib_per_sec"),
        verdict.get("clean_sustainable_mb_s"),
    )
    row["messages_attempted"] = first_number(producers.get("messages_attempted"))
    row["messages_enqueued"] = first_number(producers.get("messages_enqueued"))
    row["messages_delivered"] = first_number(producers.get("messages_delivered"))
    row["messages_failed"] = first_number(producers.get("messages_failed"))
    row["pending_messages_at_flush_start"] = first_number(
        producers.get("pending_messages_at_flush_start")
    )
    row["pending_bytes_at_flush_start"] = first_number(
        producers.get("pending_bytes_at_flush_start")
    )
    row["producer_queue_len_at_flush_start"] = first_number(
        producers.get("producer_queue_len_at_flush_start")
    )
    row["producer_queue_len_after_flush"] = first_number(
        producers.get("producer_queue_len_after_flush")
    )
    row["flush_remaining_messages"] = first_number(producers.get("flush_remaining_messages"))
    row["delivery_callbacks_during_flush"] = first_number(
        producers.get("delivery_callbacks_during_flush")
    )
    row["consumer_messages_received"] = first_number(consumers.get("messages_received"))
    row["consumer_messages_failed"] = first_number(consumers.get("messages_failed"))

    row["monitoring_cpu_peak_percent"] = role_peak(
        monitoring, "node_cpu_busy_percent", "monitoring"
    )
    row["producer_ram_peak_GB"] = role_peak(monitoring, "node_memory_used_gb", "producer")
    row["consumer_ram_peak_GB"] = role_peak(monitoring, "node_memory_used_gb", "consumer")
    row["monitoring_ram_peak_GB"] = role_peak(
        monitoring, "node_memory_used_gb", "monitoring"
    )
    row["producer_network_rx_peak_MBps"] = role_peak(
        monitoring, "node_network_receive_mbps", "producer"
    )
    row["producer_network_tx_peak_MBps"] = role_peak(
        monitoring, "node_network_transmit_mbps", "producer"
    )
    row["consumer_network_rx_peak_MBps"] = role_peak(
        monitoring, "node_network_receive_mbps", "consumer"
    )
    row["consumer_network_tx_peak_MBps"] = role_peak(
        monitoring, "node_network_transmit_mbps", "consumer"
    )
    row["broker_combined_peak_MBps"] = first_number(
        analysis.get("broker_combined_peak_mb_s")
    )
    row["broker_request_latency_avg_ms"] = metric_value(
        monitoring, "kafka_jmx_total_request_time_mean_ms", "avg_value"
    )

    for test in list_value(system.get("network_tests")):
        label = str(dict_value(test).get("label", ""))
        if label == "producer_to_broker":
            row["producer_to_broker_iperf_MBps"] = first_number(
                dict_value(test).get("megabytes_per_second")
            )
        elif label == "broker_to_consumer":
            row["broker_to_consumer_iperf_MBps"] = first_number(
                dict_value(test).get("megabytes_per_second")
            )

    if not row.get("producer_to_broker_iperf_MBps"):
        for cmp_row in list_value(analysis.get("network_path_comparisons")):
            cmp_dict = dict_value(cmp_row)
            if cmp_dict.get("path") == "producer_to_broker":
                row["producer_to_broker_iperf_MBps"] = first_number(
                    cmp_dict.get("capacity_mb_s")
                )
            elif cmp_dict.get("path") == "broker_to_consumer":
                row["broker_to_consumer_iperf_MBps"] = first_number(
                    cmp_dict.get("capacity_mb_s")
                )


def enrich_derived_fields(row: dict[str, Any]) -> list[str]:
    for canonical, compatibility in [
        ("producer_mib_per_sec", "producer_delivered_MBps"),
        ("consumer_mib_per_sec", "consumer_received_MBps"),
        ("balanced_mib_per_sec", "balanced_app_MBps"),
    ]:
        if row.get(canonical, "") not in (None, ""):
            row[compatibility] = row[canonical]
    for field in CONFIG_FIELDS + PERFORMANCE_FIELDS + RESOURCE_FIELDS:
        if field in row:
            row[field] = normalize_number(row[field])
    producer = fnum(row.get("producer_delivered_MBps"))
    consumer = fnum(row.get("consumer_received_MBps"))
    record_issues = derive_record_throughput_fields(row, row.get("config_id", "row"))
    row["balanced_app_MBps_recalc"] = min_positive([producer, consumer])
    row["broker_combined_MBps_recalc"] = fnum(row.get("broker_ingress_MBps")) + fnum(
        row.get("broker_egress_MBps")
    )
    messages_enqueued = fnum(row.get("messages_enqueued"))
    pending = fnum(row.get("pending_messages_at_flush_start"))
    messages_attempted = fnum(row.get("messages_attempted"))
    failed = fnum(row.get("messages_failed"))
    policy_id = str(row.get("qualification_policy_id", ""))
    backlog_denominator = str(row.get("backlog_denominator", ""))
    if not backlog_denominator:
        backlog_denominator = (
            "messages_attempted"
            if policy_id == "qualification.application.v1"
            else "messages_enqueued"
        )
    backlog_denominator_value = (
        messages_attempted
        if backlog_denominator == "messages_attempted"
        else messages_enqueued
    )
    row["backlog_denominator"] = backlog_denominator
    row["backlog_denominator_value"] = backlog_denominator_value
    if backlog_denominator_value > 0:
        row["pending_backlog_percent_recalc"] = (
            pending / backlog_denominator_value * 100.0
        )
    else:
        row["pending_backlog_percent_recalc"] = fnum(row.get("pending_backlog_percent"))
    if messages_attempted > 0:
        row["failed_send_percent_recalc"] = failed / messages_attempted * 100.0
    else:
        row["failed_send_percent_recalc"] = fnum(row.get("failed_send_percent"))
    row["balanced_mismatch"] = mismatch(
        fnum(row.get("balanced_app_MBps")),
        row["balanced_app_MBps_recalc"],
        tolerance=0.01,
    )
    row["broker_combined_mismatch"] = mismatch(
        fnum(row.get("broker_combined_MBps")),
        row["broker_combined_MBps_recalc"],
        tolerance=0.02,
    )
    row["backlog_mismatch"] = mismatch(
        fnum(row.get("pending_backlog_percent")),
        row["pending_backlog_percent_recalc"],
        tolerance=0.02,
    )
    row["failed_send_mismatch"] = mismatch(
        fnum(row.get("failed_send_percent")),
        row["failed_send_percent_recalc"],
        tolerance=0.002,
    )
    expected_status = (
        "qualified"
        if row["pending_backlog_percent_recalc"] <= BACKLOG_LIMIT
        and fnum(row.get("flush_sec")) <= FLUSH_LIMIT
        and row["failed_send_percent_recalc"] <= FAILED_SEND_LIMIT
        else "overdriven"
    )
    reported_status = str(row.get("qualification_status", "")).strip().lower()
    row["qualification_mismatch"] = bool(reported_status) and (
        reported_status not in {expected_status, "ineligible"}
    )
    row["any_mismatch"] = any(
        bool(row[key])
        for key in [
            "balanced_mismatch",
            "broker_combined_mismatch",
            "backlog_mismatch",
            "failed_send_mismatch",
            "qualification_mismatch",
        ]
    )
    backlog = fnum(row.get("pending_backlog_percent"))
    flush = fnum(row.get("flush_sec"))
    failed = fnum(row.get("failed_send_percent"))
    exceeded_backlog = backlog > BACKLOG_LIMIT
    exceeded_flush = flush > FLUSH_LIMIT
    exceeded_failure = failed > FAILED_SEND_LIMIT
    eligible_value = row.get("eligible", "")
    is_eligible = (
        bool(eligible_value)
        if isinstance(eligible_value, bool)
        else str(eligible_value).strip().lower() not in {"false", "0", "no"}
    )
    is_qualified = is_eligible and not (
        exceeded_backlog or exceeded_flush or exceeded_failure
    )
    row["is_eligible"] = is_eligible
    row["exceeded_backlog_limit"] = exceeded_backlog
    row["exceeded_flush_limit"] = exceeded_flush
    row["exceeded_failure_limit"] = exceeded_failure
    row["is_qualified"] = is_qualified
    row["qualification_status"] = (
        "qualified" if is_qualified else "overdriven" if is_eligible else "ineligible"
    )
    row["qualification_reason"] = (
        qualification_reason(exceeded_backlog, exceeded_flush, exceeded_failure)
        if is_eligible
        else "eligibility or correctness checks failed"
    )
    row["is_clean"] = 1 if is_qualified else 0
    row["resource_metrics_available"] = (
        "yes" if any(math.isfinite(fnum(row.get(field))) for field in RESOURCE_FIELDS) else "no"
    )
    row["resource_semantics_status"] = (
        "partial_summary_only" if row["resource_metrics_available"] == "yes" else "unavailable"
    )
    row["total_worker_ranks"] = fnum(row.get("producer_ranks")) + fnum(
        row.get("consumer_ranks")
    )
    consumer_ranks = fnum(row.get("consumer_ranks"))
    row["producer_consumer_rank_ratio"] = (
        fnum(row.get("producer_ranks")) / consumer_ranks if consumer_ranks else math.nan
    )
    row["producer_pressure_proxy"] = (
        fnum(row.get("producer_ranks")) * fnum(row.get("payload_size_bytes"))
    )
    for source, target in [
        ("partitions", "log2_partitions"),
        ("batch_size", "log2_batch_size"),
        ("payload_size_bytes", "log2_payload_size_bytes"),
        ("producer_queue_messages", "log2_producer_queue_messages"),
        ("producer_queue_kbytes", "log2_producer_queue_kbytes"),
        ("consumer_fetch_min_bytes", "log2_consumer_fetch_min_bytes"),
        ("consumer_fetch_message_max_bytes", "log2_consumer_fetch_message_max_bytes"),
    ]:
        row[target] = log2_or_nan(row.get(source))
    return record_issues


def derive_record_throughput_fields(
    row: dict[str, Any], row_label: Any = "row"
) -> list[str]:
    label = str(row_label or "row")
    payload = fnum(row.get("payload_size_bytes"))
    producer = fnum(row.get("producer_delivered_MBps"))
    consumer = fnum(row.get("consumer_received_MBps"))
    balanced = fnum(row.get("balanced_app_MBps"))
    issues: list[str] = []

    if not math.isfinite(payload) or payload <= 0:
        issues.append(f"{label}: missing or non-positive payload_size_bytes")
    for field, value in [
        ("producer_delivered_MBps", producer),
        ("consumer_received_MBps", consumer),
        ("balanced_app_MBps", balanced),
    ]:
        if not math.isfinite(value):
            issues.append(f"{label}: missing {field}")
        elif value < 0:
            issues.append(f"{label}: negative {field}")

    if issues:
        for field in [
            "producer_records_per_sec",
            "consumer_records_per_sec",
            "balanced_records_per_sec",
        ]:
            row[field] = ""
        return issues

    conversion = BYTES_PER_MIB / payload
    producer_records = producer * conversion
    consumer_records = consumer * conversion
    row["producer_records_per_sec"] = producer_records
    row["consumer_records_per_sec"] = consumer_records
    row["balanced_records_per_sec"] = min(producer_records, consumer_records)

    if not math.isclose(
        row["balanced_records_per_sec"],
        balanced * conversion,
        rel_tol=1e-9,
        abs_tol=1e-6,
    ):
        issues.append(
            f"{label}: balanced_app_MBps is inconsistent with producer/consumer throughput"
        )
    return issues


def validate_record_throughput_rows(
    rows: list[dict[str, Any]], scope: str
) -> list[str]:
    issues: list[str] = []
    for row in rows:
        label = str(row.get("case_id") or row.get("config_id") or "row")
        producer = fnum(row.get("producer_records_per_sec"))
        consumer = fnum(row.get("consumer_records_per_sec"))
        balanced = fnum(row.get("balanced_records_per_sec"))
        if not all(math.isfinite(value) for value in [producer, consumer, balanced]):
            issues.append(f"{scope} {label}: missing derived record-throughput value")
            continue
        if any(value < 0 for value in [producer, consumer, balanced]):
            issues.append(f"{scope} {label}: negative derived record-throughput value")
        if not math.isclose(
            balanced, min(producer, consumer), rel_tol=1e-12, abs_tol=1e-6
        ):
            issues.append(
                f"{scope} {label}: balanced_records_per_sec is not the producer/consumer minimum"
            )
    return issues


def qualification_reason(
    exceeded_backlog: bool, exceeded_flush: bool, exceeded_failure: bool
) -> str:
    exceeded = []
    if exceeded_backlog:
        exceeded.append("backlog")
    if exceeded_flush:
        exceeded.append("flush")
    if exceeded_failure:
        exceeded.append("failed-send")
    if not exceeded:
        return "qualified"
    if len(exceeded) == 1:
        return f"{exceeded[0]} limit exceeded"
    if len(exceeded) == 2:
        return f"{exceeded[0]} and {exceeded[1]} limits exceeded"
    return "backlog, flush, and failed-send limits exceeded"


def qualification_status_from_thresholds(
    backlog_percent: float,
    failed_send_percent: float,
    flush_sec: float,
) -> str:
    return (
        "Qualified"
        if backlog_percent <= BACKLOG_LIMIT
        and flush_sec <= FLUSH_LIMIT
        and failed_send_percent <= FAILED_SEND_LIMIT
        else "Overdriven"
    )


def assign_ranks(rows: list[dict[str, Any]]) -> None:
    for rank, row in enumerate(sorted(rows, key=primary_rank_key), start=1):
        row["primary_rank"] = rank
    qualified = [row for row in rows if row.get("is_qualified")]
    for rank, row in enumerate(sorted(qualified, key=within_group_rank_key), start=1):
        row["qualified_rank"] = rank
    for row in rows:
        if not row.get("is_qualified"):
            row["qualified_rank"] = ""
    for rank, row in enumerate(sorted(rows, key=raw_throughput_rank_key), start=1):
        row["raw_throughput_rank"] = rank


def primary_rank_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        0 if row.get("is_qualified") else 1 if row.get("is_eligible", True) else 2,
        -fnum(row.get("balanced_app_MBps"), default=-math.inf),
        fnum(row.get("pending_backlog_percent"), default=math.inf),
        fnum(row.get("flush_sec"), default=math.inf),
        fnum(row.get("failed_send_percent"), default=math.inf),
        config_sort_key(str(row.get("config_id", ""))),
    )


def within_group_rank_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return primary_rank_key(row)[1:]


def raw_throughput_rank_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        -fnum(row.get("balanced_app_MBps"), default=-math.inf),
        fnum(row.get("pending_backlog_percent"), default=math.inf),
        fnum(row.get("flush_sec"), default=math.inf),
        fnum(row.get("failed_send_percent"), default=math.inf),
        config_sort_key(str(row.get("config_id", ""))),
    )


def write_dataset(rows: list[dict[str, Any]]) -> None:
    fieldnames = ordered_fieldnames(rows)
    write_csv(REPORT_ROOT / "analysis_verified_dataset.csv", rows, fieldnames)


def write_descriptive_stats(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = CONFIG_FIELDS + PERFORMANCE_FIELDS + RESOURCE_FIELDS + [
        "pending_backlog_percent_recalc",
        "failed_send_percent_recalc",
    ]
    out_rows = []
    for field in fields:
        values = numeric_values(rows, field)
        if not values:
            continue
        arr = np.array(values, dtype=float)
        mean = float(arr.mean())
        std = float(arr.std(ddof=1)) if len(arr) > 1 else 0.0
        out_rows.append(
            {
                "field": field,
                "count": len(values),
                "missing": len(rows) - len(values),
                "mean": mean,
                "median": float(np.median(arr)),
                "std": std,
                "min": float(arr.min()),
                "p10": float(np.percentile(arr, 10)),
                "q1": float(np.percentile(arr, 25)),
                "q3": float(np.percentile(arr, 75)),
                "p90": float(np.percentile(arr, 90)),
                "max": float(arr.max()),
                "cv": std / mean if mean else math.nan,
                "skewness": skewness(arr),
            }
        )
    write_csv(REPORT_ROOT / "analysis_descriptive_stats.csv", out_rows)
    return out_rows


def write_controlled_comparisons(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out_rows: list[dict[str, Any]] = []
    for changed in CONFIG_FIELDS:
        other_fields = [field for field in CONFIG_FIELDS if field != changed]
        groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            groups[tuple(row.get(field) for field in other_fields)].append(row)
        for group_rows in groups.values():
            unique_values = sorted(
                {fnum(row.get(changed)) for row in group_rows}, key=lambda value: value
            )
            if len(unique_values) < 2:
                continue
            sorted_group = sorted(group_rows, key=lambda row: fnum(row.get(changed)))
            base = sorted_group[0]
            for current in sorted_group[1:]:
                out_rows.append(
                    {
                        "changed_parameter": changed,
                        "base_config": base["config_id"],
                        "comparison_config": current["config_id"],
                        "base_value": base.get(changed),
                        "comparison_value": current.get(changed),
                        "delta_balanced_MBps": fnum(current.get("balanced_app_MBps"))
                        - fnum(base.get("balanced_app_MBps")),
                        "delta_backlog_percent": fnum(
                            current.get("pending_backlog_percent")
                        )
                        - fnum(base.get("pending_backlog_percent")),
                        "delta_flush_sec": fnum(current.get("flush_sec"))
                        - fnum(base.get("flush_sec")),
                        "delta_failed_send_percent": fnum(
                            current.get("failed_send_percent")
                        )
                        - fnum(base.get("failed_send_percent")),
                        "delta_broker_cpu_peak_percent": fnum(
                            current.get("broker_cpu_peak_percent")
                        )
                        - fnum(base.get("broker_cpu_peak_percent")),
                    }
                )
    write_csv(REPORT_ROOT / "analysis_controlled_comparisons.csv", out_rows)
    return out_rows


def write_correlations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    targets = [
        "balanced_app_MBps",
        "producer_delivered_MBps",
        "consumer_received_MBps",
        "pending_backlog_percent",
        "flush_sec",
        "failed_send_percent",
        "broker_combined_MBps",
        "broker_cpu_peak_percent",
        "broker_ram_peak_GB",
        "broker_network_rx_peak_MBps",
        "broker_network_tx_peak_MBps",
    ]
    out_rows = []
    p_values = []
    for feature in MODEL_FEATURES:
        x = np.array([fnum(row.get(feature)) for row in rows], dtype=float)
        for target in targets:
            y = np.array([fnum(row.get(target)) for row in rows], dtype=float)
            mask = np.isfinite(x) & np.isfinite(y)
            if mask.sum() < 4:
                continue
            rho = spearman(x[mask], y[mask])
            pearson = pearson_corr(x[mask], y[mask])
            p_value = approx_corr_pvalue(rho, int(mask.sum()))
            row = {
                "feature": feature,
                "target": target,
                "n": int(mask.sum()),
                "spearman_rho": rho,
                "spearman_p_approx": p_value,
                "pearson_r": pearson,
            }
            out_rows.append(row)
            p_values.append(p_value)
    q_values = benjamini_hochberg(p_values)
    for row, q_value in zip(out_rows, q_values):
        row["spearman_q_bh_approx"] = q_value
    write_csv(REPORT_ROOT / "analysis_correlations.csv", out_rows)
    return out_rows


def run_models(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    X_raw = np.array([[fnum(row.get(feature)) for feature in MODEL_FEATURES] for row in rows])
    X, means, stds = standardize(X_raw)
    y_targets = {
        "balanced_app_MBps": np.array([fnum(row.get("balanced_app_MBps")) for row in rows]),
        "pending_backlog_percent": np.array(
            [fnum(row.get("pending_backlog_percent")) for row in rows]
        ),
        "flush_sec": np.array([fnum(row.get("flush_sec")) for row in rows]),
        "failed_send_percent": np.array([fnum(row.get("failed_send_percent")) for row in rows]),
    }
    folds = kfold_indices(len(rows), k=5, seed=11)
    out_rows: list[dict[str, Any]] = []
    feature_importance: dict[str, list[dict[str, Any]]] = {}

    for target_name, y in y_targets.items():
        for alpha in [0.0, 0.1, 1.0, 10.0, 100.0]:
            preds = np.zeros_like(y, dtype=float)
            for train_idx, test_idx in folds:
                beta = ridge_fit(X[train_idx], y[train_idx], alpha=alpha)
                preds[test_idx] = add_intercept(X[test_idx]) @ beta
            out_rows.append(model_metric_row("Ridge", target_name, alpha, y, preds))
        rf_preds = np.zeros_like(y, dtype=float)
        for fold_no, (train_idx, test_idx) in enumerate(folds):
            forest = RandomForestLite(
                task="regression", n_trees=80, max_depth=4, min_leaf=5, seed=101 + fold_no
            )
            forest.fit(X[train_idx], y[train_idx])
            rf_preds[test_idx] = forest.predict(X[test_idx])
        out_rows.append(model_metric_row("RandomForestLite", target_name, "", y, rf_preds))

    clean_y = np.array([int(row.get("is_clean", 0)) for row in rows])
    stratified = stratified_clean_folds(clean_y)
    for alpha in [0.01, 0.1, 1.0]:
        probs = np.zeros(len(rows), dtype=float)
        preds = np.zeros(len(rows), dtype=int)
        for train_idx, test_idx in stratified:
            beta = logistic_fit(X[train_idx], clean_y[train_idx], alpha=alpha)
            probs[test_idx] = sigmoid(add_intercept(X[test_idx]) @ beta)
            preds[test_idx] = (probs[test_idx] >= 0.5).astype(int)
        out_rows.append(classification_metric_row("WeightedLogistic", alpha, clean_y, preds, probs))

    rf_probs = np.zeros(len(rows), dtype=float)
    rf_preds = np.zeros(len(rows), dtype=int)
    for fold_no, (train_idx, test_idx) in enumerate(stratified):
        forest = RandomForestLite(
            task="classification", n_trees=100, max_depth=4, min_leaf=4, seed=221 + fold_no
        )
        forest.fit(X[train_idx], clean_y[train_idx])
        rf_probs[test_idx] = forest.predict_proba(X[test_idx])
        rf_preds[test_idx] = (rf_probs[test_idx] >= 0.5).astype(int)
    out_rows.append(classification_metric_row("RandomForestLite", "", clean_y, rf_preds, rf_probs))

    for target_name in ["balanced_app_MBps", "pending_backlog_percent", "flush_sec"]:
        y = y_targets[target_name]
        forest = RandomForestLite(task="regression", n_trees=120, max_depth=4, min_leaf=5, seed=303)
        forest.fit(X, y)
        feature_importance[target_name] = permutation_importance(
            forest, X, y, MODEL_FEATURES, task="regression", seed=41
        )
    forest = RandomForestLite(task="classification", n_trees=120, max_depth=4, min_leaf=4, seed=404)
    forest.fit(X, clean_y)
    feature_importance["clean_classification"] = permutation_importance(
        forest, X, clean_y, MODEL_FEATURES, task="classification", seed=42
    )

    importance_rows = []
    for target, items in feature_importance.items():
        for item in items:
            importance_rows.append({"target": target, **item})
    write_csv(REPORT_ROOT / "analysis_feature_importance.csv", importance_rows)
    write_csv(REPORT_ROOT / "analysis_model_results.csv", out_rows)
    return out_rows, feature_importance


def write_pareto(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    objectives = [
        ("balanced_app_MBps", "max"),
        ("pending_backlog_percent", "min"),
        ("flush_sec", "min"),
        ("failed_send_percent", "min"),
    ]
    pareto = []
    for row in rows:
        dominated = False
        for other in rows:
            if other is row:
                continue
            if dominates(other, row, objectives):
                dominated = True
                break
        if not dominated:
            pareto.append(row)
    pareto_rows = [
        {
            "config_id": row["config_id"],
            "balanced_app_MBps": row["balanced_app_MBps"],
            "pending_backlog_percent": row["pending_backlog_percent"],
            "flush_sec": row["flush_sec"],
            "failed_send_percent": row["failed_send_percent"],
            "throughput_verdict": row["throughput_verdict"],
            "broker_cpu_peak_percent": row.get("broker_cpu_peak_percent", ""),
            "broker_ram_peak_GB": row.get("broker_ram_peak_GB", ""),
            **{field: row.get(field, "") for field in CONFIG_FIELDS},
        }
        for row in sorted(pareto, key=lambda item: -fnum(item.get("balanced_app_MBps")))
    ]
    write_csv(REPORT_ROOT / "analysis_pareto_front.csv", pareto_rows)
    return pareto_rows


def write_qualification_sensitivity(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    official_ids = {
        row["config_id"]
        for row in rows
        if qualifies_at_backlog_threshold(row, BACKLOG_LIMIT)
    }
    out_rows = []
    for threshold in [2.0, BACKLOG_LIMIT, 10.0]:
        qualified = [
            row for row in rows if qualifies_at_backlog_threshold(row, threshold)
        ]
        qualified_ids = {row["config_id"] for row in qualified}
        best = (
            max(qualified, key=lambda row: fnum(row.get("balanced_app_MBps")))
            if qualified
            else {}
        )
        out_rows.append(
            {
                "backlog_threshold_percent": threshold,
                "flush_threshold_sec": FLUSH_LIMIT,
                "failed_send_threshold_percent": FAILED_SEND_LIMIT,
                "qualified_config_count": len(qualified),
                "highest_qualifying_config_id": best.get("config_id", ""),
                "highest_qualifying_balanced_app_MBps": best.get("balanced_app_MBps", ""),
                "entering_vs_official_5_percent": ";".join(
                    sorted(qualified_ids - official_ids, key=config_sort_key)
                ),
                "leaving_vs_official_5_percent": ";".join(
                    sorted(official_ids - qualified_ids, key=config_sort_key)
                ),
            }
        )
    write_csv(REPORT_ROOT / "analysis_qualification_sensitivity.csv", out_rows)
    return out_rows


def qualifies_at_backlog_threshold(row: dict[str, Any], backlog_threshold: float) -> bool:
    return (
        fnum(row.get("pending_backlog_percent")) <= backlog_threshold
        and fnum(row.get("flush_sec")) <= FLUSH_LIMIT
        and fnum(row.get("failed_send_percent")) <= FAILED_SEND_LIMIT
    )


def write_validation_shortlist(
    rows: list[dict[str, Any]],
    controlled_rows: list[dict[str, Any]],
    manifest_rows: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    selected = select_validation_shortlist(rows, controlled_rows, manifest_rows)
    output_rows = [shortlist_output_row(item) for item in selected.values()]
    write_csv(
        REPORT_ROOT / "analysis_validation_shortlist.csv",
        output_rows,
        [
            "config_id",
            "selection_categories",
            "selection_reason",
            "original_manifest_note",
            "config_path",
            "producer_ranks",
            "consumer_ranks",
            "total_worker_ranks",
            "producer_consumer_rank_ratio",
            "partitions",
            "batch_size",
            "linger_ms",
            "payload_size_bytes",
            "producer_queue_messages",
            "producer_queue_kbytes",
            "consumer_fetch_min_bytes",
            "consumer_fetch_wait_max_ms",
            "consumer_fetch_message_max_bytes",
            "producer_delivered_MBps",
            "consumer_received_MBps",
            "balanced_app_MBps",
            "producer_records_per_sec",
            "consumer_records_per_sec",
            "balanced_records_per_sec",
            "broker_ingress_MBps",
            "broker_egress_MBps",
            "broker_combined_MBps",
            "pending_backlog_percent",
            "flush_sec",
            "failed_send_percent",
            "qualification_status",
            "qualification_reason",
            "primary_rank",
            "qualified_rank",
            "raw_throughput_rank",
            "controlled_comparison_parameter",
        ],
    )
    (REPORT_ROOT / "analysis_validation_shortlist.json").write_text(
        json.dumps(output_rows, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_validation_shortlist_markdown(output_rows)
    return output_rows


def select_validation_shortlist(
    rows: list[dict[str, Any]],
    controlled_rows: list[dict[str, Any]],
    manifest_rows: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    by_id = {row["config_id"]: row for row in rows}
    selected: dict[str, dict[str, Any]] = {}

    def add(row: dict[str, Any], category: str, reason: str, controlled_parameter: str = "") -> None:
        cfg_id = row["config_id"]
        entry = selected.setdefault(
            cfg_id,
            {
                "row": row,
                "categories": [],
                "reasons": [],
                "controlled_parameter": controlled_parameter,
            },
        )
        if category not in entry["categories"]:
            entry["categories"].append(category)
        if reason not in entry["reasons"]:
            entry["reasons"].append(reason)
        if controlled_parameter and not entry["controlled_parameter"]:
            entry["controlled_parameter"] = controlled_parameter

    qualified = sorted(
        [row for row in rows if row.get("is_qualified")],
        key=lambda row: -fnum(row.get("balanced_app_MBps")),
    )
    for row in qualified[:3]:
        add(row, "qualified leader", "top three qualified configurations by measured balanced throughput")

    best_qualified = qualified[0] if qualified else {}
    only_backlog = [
        row
        for row in rows
        if row.get("exceeded_backlog_limit")
        and not row.get("exceeded_flush_limit")
        and not row.get("exceeded_failure_limit")
    ]
    boundary = [
        row
        for row in only_backlog
        if fnum(row.get("balanced_app_MBps")) > fnum(best_qualified.get("balanced_app_MBps"))
    ]
    if boundary:
        add(
            sorted(
                boundary,
                key=lambda row: (
                    abs(fnum(row.get("pending_backlog_percent")) - BACKLOG_LIMIT),
                    -fnum(row.get("balanced_app_MBps")),
                ),
            )[0],
            "closest backlog boundary",
            "overdriven only by backlog, higher throughput than the best qualified run, and closest to the 5% backlog limit",
        )

    add_first_distinct_transition(
        selected,
        only_backlog,
        upper_backlog=15.0,
        category="high-throughput transition",
        reason="highest-throughput backlog-only point with backlog between 5% and 15%",
    )
    add_first_distinct_transition(
        selected,
        only_backlog,
        upper_backlog=20.0,
        category="upper transition",
        reason="highest-throughput backlog-only point with backlog between 5% and 20%",
    )

    raw_upper = max(rows, key=lambda row: fnum(row.get("balanced_app_MBps")))
    add(
        raw_upper,
        "raw upper bound",
        "highest observed balanced throughput regardless of qualification",
    )
    secondary_overload = sorted(
        [
            row
            for row in rows
            if not row.get("is_qualified") and row["config_id"] != raw_upper["config_id"]
        ],
        key=lambda row: -fnum(row.get("balanced_app_MBps")),
    )
    if secondary_overload:
        add(
            secondary_overload[0],
            "high-throughput overload reference",
            "second-highest balanced-throughput overdriven observation, retained to study overload behavior",
        )

    baseline = next(
        (
            by_id[cfg_id]
            for cfg_id, manifest_row in manifest_rows.items()
            if cfg_id in by_id
            and str(manifest_row.get("notes", "")).strip().lower() == "baseline"
        ),
        None,
    )
    if baseline:
        add(baseline, "manifest baseline", "baseline configuration marked in the sweep manifest")

    controlled = select_controlled_one_factor(rows, manifest_rows)
    if controlled:
        add(
            controlled["row"],
            "controlled one-factor comparison",
            controlled["reason"],
            controlled["changed_parameter"],
        )

    return dict(sorted(selected.items(), key=lambda item: fnum(item[1]["row"].get("primary_rank"))))


def add_first_distinct_transition(
    selected: dict[str, dict[str, Any]],
    candidates: list[dict[str, Any]],
    upper_backlog: float,
    category: str,
    reason: str,
) -> None:
    ordered = sorted(
        [
            row
            for row in candidates
            if BACKLOG_LIMIT < fnum(row.get("pending_backlog_percent")) <= upper_backlog
        ],
        key=lambda row: -fnum(row.get("balanced_app_MBps")),
    )
    if not ordered:
        return
    first = ordered[0]
    target = next((row for row in ordered if row["config_id"] not in selected), first)
    entry = selected.setdefault(
        target["config_id"],
        {"row": target, "categories": [], "reasons": [], "controlled_parameter": ""},
    )
    if category not in entry["categories"]:
        entry["categories"].append(category)
    if reason not in entry["reasons"]:
        entry["reasons"].append(reason)


def select_controlled_one_factor(
    rows: list[dict[str, Any]], manifest_rows: dict[str, dict[str, Any]]
) -> dict[str, Any] | None:
    by_id = {row["config_id"]: row for row in rows}
    baseline = next(
        (
            by_id[cfg_id]
            for cfg_id, manifest_row in manifest_rows.items()
            if cfg_id in by_id
            and str(manifest_row.get("notes", "")).strip().lower() == "baseline"
        ),
        None,
    )
    if baseline is None:
        return None
    preferred = [
        "consumer_fetch_min_bytes",
        "consumer_fetch_wait_max_ms",
        "consumer_fetch_message_max_bytes",
        "producer_queue_messages",
        "producer_queue_kbytes",
    ]
    candidates: list[dict[str, Any]] = []
    for row in rows:
        note = str(manifest_rows.get(row["config_id"], {}).get("notes", ""))
        if not note.startswith("one-factor:"):
            continue
        changed = [field for field in CONFIG_FIELDS if str(row.get(field)) != str(baseline.get(field))]
        if len(changed) != 1 or changed[0] not in preferred:
            continue
        delta = fnum(row.get("balanced_app_MBps")) - fnum(baseline.get("balanced_app_MBps"))
        candidates.append({"row": row, "changed_parameter": changed[0], "delta": delta})
    if not candidates:
        return None
    candidates.sort(
        key=lambda item: (
            preferred.index(item["changed_parameter"]),
            -abs(float(item["delta"])),
            config_sort_key(item["row"]["config_id"]),
        )
    )
    best = candidates[0]
    return {
        "row": best["row"],
        "changed_parameter": best["changed_parameter"],
        "reason": (
            f"strong one-factor client-tuning comparison against the manifest baseline; "
            f"changed {best['changed_parameter']} and shifted balanced throughput by "
            f"{fmt(best['delta'])} MiB/s"
        ),
    }


def shortlist_output_row(item: dict[str, Any]) -> dict[str, Any]:
    row = item["row"]
    return {
        "config_id": row["config_id"],
        "selection_categories": "; ".join(item["categories"]),
        "selection_reason": "; ".join(item["reasons"]),
        "original_manifest_note": row.get("manifest_note", ""),
        "config_path": row.get("config_path", ""),
        "producer_ranks": row.get("producer_ranks", ""),
        "consumer_ranks": row.get("consumer_ranks", ""),
        "total_worker_ranks": row.get("total_worker_ranks", ""),
        "producer_consumer_rank_ratio": row.get("producer_consumer_rank_ratio", ""),
        "partitions": row.get("partitions", ""),
        "batch_size": row.get("batch_size", ""),
        "linger_ms": row.get("linger_ms", ""),
        "payload_size_bytes": row.get("payload_size_bytes", ""),
        "producer_queue_messages": row.get("producer_queue_messages", ""),
        "producer_queue_kbytes": row.get("producer_queue_kbytes", ""),
        "consumer_fetch_min_bytes": row.get("consumer_fetch_min_bytes", ""),
        "consumer_fetch_wait_max_ms": row.get("consumer_fetch_wait_max_ms", ""),
        "consumer_fetch_message_max_bytes": row.get("consumer_fetch_message_max_bytes", ""),
        "producer_delivered_MBps": row.get("producer_delivered_MBps", ""),
        "consumer_received_MBps": row.get("consumer_received_MBps", ""),
        "balanced_app_MBps": row.get("balanced_app_MBps", ""),
        "producer_records_per_sec": row.get("producer_records_per_sec", ""),
        "consumer_records_per_sec": row.get("consumer_records_per_sec", ""),
        "balanced_records_per_sec": row.get("balanced_records_per_sec", ""),
        "broker_ingress_MBps": row.get("broker_ingress_MBps", ""),
        "broker_egress_MBps": row.get("broker_egress_MBps", ""),
        "broker_combined_MBps": row.get("broker_combined_MBps", ""),
        "pending_backlog_percent": row.get("pending_backlog_percent", ""),
        "flush_sec": row.get("flush_sec", ""),
        "failed_send_percent": row.get("failed_send_percent", ""),
        "qualification_status": row.get("qualification_status", ""),
        "qualification_reason": row.get("qualification_reason", ""),
        "primary_rank": row.get("primary_rank", ""),
        "qualified_rank": row.get("qualified_rank", ""),
        "raw_throughput_rank": row.get("raw_throughput_rank", ""),
        "controlled_comparison_parameter": item.get("controlled_parameter", ""),
    }


def write_validation_shortlist_markdown(rows: list[dict[str, Any]]) -> None:
    lines = [
        "# Validation Shortlist",
        "",
        "This shortlist is derived from the existing single-run 120-configuration sweep. It is a plan for future repetitions, not evidence of repeated sustainability.",
        "",
        "| Config | Categories | Balanced MiB/s | Balanced records/s | Backlog % | Flush s | Failed % | Qualification | Reason |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for row in rows:
        lines.append(
            f"| `{row['config_id']}` | {row['selection_categories']} | {fmt_mbps(row['balanced_app_MBps'])} | {fmt_records(row['balanced_records_per_sec'])} | {fmt(row['pending_backlog_percent'])} | {fmt(row['flush_sec'])} | {fmt(row['failed_send_percent'])} | {row['qualification_status']} | {row['selection_reason']} |"
        )
    lines.extend(
        [
            "",
            "Qualified rows satisfy backlog <= 5%, flush <= 10 s, and failed sends <= 0.1%. Transition and overload rows are included as objective boundary evidence, not as automatic recommendations.",
        ]
    )
    (REPORT_ROOT / "analysis_validation_shortlist.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def write_repetition_plan(shortlist_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out_rows = []
    lines = [
        "# Repetition Plan",
        "",
        "The current sweep has one run per configuration. The validation shortlist should be repeated before making publication-level sustainability claims. Five repetitions are the planned minimum validation stage.",
        "",
        "| Config | Categories | Balanced MiB/s | Backlog % | Flush s | Failed % | Qualification |",
        "| --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in shortlist_rows:
        out_rows.append(
            {
                "config_id": row["config_id"],
                "selection_categories": row["selection_categories"],
                "reason": row["selection_reason"],
                "recommended_repetitions": 5,
                "minimum_repetitions": 5,
                "balanced_app_MBps": row["balanced_app_MBps"],
                "pending_backlog_percent": row["pending_backlog_percent"],
                "flush_sec": row["flush_sec"],
                "failed_send_percent": row["failed_send_percent"],
                "qualification_status": row["qualification_status"],
            }
        )
        lines.append(
            f"| `{row['config_id']}` | {row['selection_categories']} | {fmt(row['balanced_app_MBps'])} | {fmt(row['pending_backlog_percent'])} | {fmt(row['flush_sec'])} | {fmt(row['failed_send_percent'])} | {row['qualification_status']} |"
        )
    lines.extend(
        [
            "",
            "For repeated results, report median balanced throughput, min/max, IQR or MAD, coefficient of variation, qualified-run count/frequency, median backlog, median flush, and failed-send occurrence frequency. Rank qualification-first and then use directly measured throughput and stability metrics; no composite heuristic is used.",
        ]
    )
    (REPORT_ROOT / "analysis_repetition_plan.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    write_csv(REPORT_ROOT / "analysis_repetition_plan.csv", out_rows)
    return out_rows


def write_validation_manifests(shortlist_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output_dir = REPORT_ROOT / "configs" / "sweeps" / "simultaneous_validation"
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_rows = [
        {
            "config_id": row["config_id"],
            "config_path": row["config_path"],
            "selection_categories": row["selection_categories"],
            "selection_reason": row["selection_reason"],
            "planned_repetitions": 5,
        }
        for row in shortlist_rows
    ]
    write_csv(
        output_dir / "validation_manifest.csv",
        manifest_rows,
        [
            "config_id",
            "config_path",
            "selection_categories",
            "selection_reason",
            "planned_repetitions",
        ],
    )

    rng = random.Random(VALIDATION_SEED)
    run_rows: list[dict[str, Any]] = []
    for block_number in range(1, 6):
        block = list(shortlist_rows)
        rng.shuffle(block)
        for order, row in enumerate(block, start=1):
            run_rows.append(
                {
                    "block_number": block_number,
                    "order_in_block": order,
                    "repetition": block_number,
                    "config_id": row["config_id"],
                    "config_path": row["config_path"],
                    "selection_reason": row["selection_reason"],
                }
            )
    write_csv(
        output_dir / "validation_run_order.csv",
        run_rows,
        [
            "block_number",
            "order_in_block",
            "repetition",
            "config_id",
            "config_path",
            "selection_reason",
        ],
    )
    (output_dir / "README.md").write_text(
        "\n".join(
            [
                "# Simultaneous Validation Sweep",
                "",
                "This directory defines a five-repetition validation experiment from the existing 120-configuration sweep. These files are run inputs only; generating them does not execute benchmarks.",
                "",
                f"Randomization seed: `{VALIDATION_SEED}`.",
                "",
                "- `validation_manifest.csv` lists selected configurations and why they were selected.",
                "- `validation_run_order.csv` contains five randomized blocks; each selected configuration appears exactly once per block.",
                "- Existing generated config JSON files are referenced by path and are not copied or modified.",
                "",
                "After repetitions are run, rank configurations by qualified-run frequency first, then median balanced throughput, with variability/backlog/flush/failures as secondary criteria.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return run_rows


def write_validation_result_artifacts(
    summary_rows: list[dict[str, Any]], repeat_rows: list[dict[str, Any]]
) -> None:
    if not summary_rows:
        return
    summary_fields = [
        "validation_rank",
        "original_config_id",
        "config_id",
        "repeats",
        "qualified_count",
        "qualified_frequency",
        "payload_size_bytes",
        "median_balanced_app_MBps",
        "median_producer_records_per_sec",
        "median_consumer_records_per_sec",
        "median_balanced_records_per_sec",
        "mean_balanced_app_MBps",
        "min_balanced_app_MBps",
        "max_balanced_app_MBps",
        "iqr_balanced_app_MBps",
        "stdev_balanced_app_MBps",
        "cv_balanced_app_MBps",
        "median_producer_delivered_MBps",
        "median_consumer_received_MBps",
        "median_pending_backlog_percent",
        "max_pending_backlog_percent",
        "median_flush_sec",
        "max_flush_sec",
        "median_failed_send_percent",
        "max_failed_send_percent",
        "qualified_blocks",
        "case_ids",
    ]
    write_csv(REPORT_ROOT / "analysis_validation_results.csv", summary_rows, summary_fields)
    if repeat_rows:
        repeat_fields = [
            "case_id",
            "original_config_id",
            "config_id",
            "block",
            "block_order",
            "status",
            "qualified",
            "qualification_status",
            "qualification_reason",
            "payload_size_bytes",
            "balanced_app_MBps",
            "producer_delivered_MBps",
            "consumer_received_MBps",
            "producer_records_per_sec",
            "consumer_records_per_sec",
            "balanced_records_per_sec",
            "pending_backlog_percent",
            "flush_sec",
            "failed_send_percent",
            "sustained_verdict",
            "final_report",
        ]
        write_csv(REPORT_ROOT / "analysis_validation_repeats.csv", repeat_rows, repeat_fields)
    payload = {
        "source_summary": INPUT_CONTEXT.get("validation_summary", ""),
        "source_repeats": INPUT_CONTEXT.get("validation_repeats", ""),
        "job_id": INPUT_CONTEXT.get("validation_job_id", ""),
        "run_id": INPUT_CONTEXT.get("validation_run_id", ""),
        "qualification_rule": QUALIFICATION_RULE_VERSION,
        "summary": summary_rows,
        "repeats": repeat_rows,
    }
    (REPORT_ROOT / "analysis_validation_results.json").write_text(
        json.dumps(payload, indent=2), encoding="utf-8"
    )
    (REPORT_ROOT / "analysis_validation_results.md").write_text(
        build_validation_results_markdown(summary_rows, repeat_rows), encoding="utf-8"
    )


def build_validation_results_markdown(
    summary_rows: list[dict[str, Any]], repeat_rows: list[dict[str, Any]]
) -> str:
    best = summary_rows[0] if summary_rows else {}
    lines = [
        "# Repeated Validation Results",
        "",
        f"Source summary: `{INPUT_CONTEXT.get('validation_summary', 'not available')}`.",
        f"Slurm job: `{INPUT_CONTEXT.get('validation_job_id') or 'unknown'}`; run: `{INPUT_CONTEXT.get('validation_run_id') or 'unknown'}`.",
        "Qualification is inclusive: backlog <= 5%, flush <= 10 s, and failed sends <= 0.1%.",
        "",
    ]
    if best:
        lines.append(
            f"Primary validated recommendation: `{validation_config_id(best)}` with "
            f"{qualified_fraction(best)} qualified repeats and median balanced throughput "
            f"{fmt_mbps(best.get('median_balanced_app_MBps'))} MiB/s "
            f"({fmt_records(best.get('median_balanced_records_per_sec'))} records/s)."
        )
        lines.append("")
    lines.extend(
        [
            "| Rank | Config | Qualified | Median MiB/s | Median records/s | Mean MiB/s | Min | Max | Median backlog % | Median flush s | Median failed % | CV |",
            "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in summary_rows:
        lines.append(
            f"| {row.get('validation_rank', '')} | `{validation_config_id(row)}` | {qualified_fraction(row)} | "
            f"{fmt_mbps(row.get('median_balanced_app_MBps'))} | {fmt_records(row.get('median_balanced_records_per_sec'))} | {fmt_mbps(row.get('mean_balanced_app_MBps'))} | "
            f"{fmt_mbps(row.get('min_balanced_app_MBps'))} | {fmt_mbps(row.get('max_balanced_app_MBps'))} | "
            f"{fmt(row.get('median_pending_backlog_percent'))} | {fmt(row.get('median_flush_sec'))} | "
            f"{fmt(row.get('median_failed_send_percent'), digits=4)} | {fmt(row.get('cv_balanced_app_MBps'))} |"
        )
    lines.extend(validation_record_table_markdown(summary_rows))
    if repeat_rows:
        lines.extend(
            [
                "",
                "## Per-Repeat Outcomes",
                "",
                "| Block | Order | Case | Status | Balanced MiB/s | Balanced records/s | Backlog % | Flush s | Failed % |",
                "| ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in sorted(repeat_rows, key=validation_repeat_key):
            lines.append(
                f"| {row.get('block', '')} | {row.get('block_order', '')} | `{row.get('case_id', '')}` | "
                f"{row.get('qualification_status', '')} | {fmt_mbps(row.get('balanced_app_MBps'))} | {fmt_records(row.get('balanced_records_per_sec'))} | "
                f"{fmt(row.get('pending_backlog_percent'))} | {fmt(row.get('flush_sec'))} | "
                f"{fmt(row.get('failed_send_percent'), digits=4)} |"
            )
    return "\n".join(lines) + "\n"


def selected_validation_record_rows(
    summary_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_id = {validation_config_id(row): row for row in summary_rows}
    return [
        by_id[config_id]
        for config_id in ["cfg_074", "cfg_115", "cfg_092", "cfg_119"]
        if config_id in by_id
    ]


def validation_record_table_markdown(
    summary_rows: list[dict[str, Any]],
) -> list[str]:
    selected = selected_validation_record_rows(summary_rows)
    if not selected:
        return []
    lines = [
        "",
        "## Throughput Units for Final Configurations",
        "",
        "Each records/s median is calculated from the record rate of each individual repeat before aggregation.",
        "",
        "| Config | Payload bytes | Producer MiB/s | Consumer MiB/s | Balanced MiB/s | Producer records/s | Consumer records/s | Balanced records/s |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in selected:
        lines.append(
            f"| `{validation_config_id(row)}` | {row.get('payload_size_bytes', '')} | "
            f"{fmt_mbps(row.get('median_producer_delivered_MBps'))} | "
            f"{fmt_mbps(row.get('median_consumer_received_MBps'))} | "
            f"{fmt_mbps(row.get('median_balanced_app_MBps'))} | "
            f"{fmt_records(row.get('median_producer_records_per_sec'))} | "
            f"{fmt_records(row.get('median_consumer_records_per_sec'))} | "
            f"{fmt_records(row.get('median_balanced_records_per_sec'))} |"
        )
    return lines


def validation_repeat_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        fnum(row.get("block"), default=math.inf),
        fnum(row.get("block_order"), default=math.inf),
        str(row.get("case_id", "")),
    )


def validation_config_id(row: dict[str, Any]) -> str:
    return str(row.get("original_config_id") or row.get("config_id") or "")


def qualified_fraction(row: dict[str, Any]) -> str:
    qualified = fnum(row.get("qualified_count"), default=0.0)
    repeats = fnum(row.get("repeats"), default=0.0)
    if math.isfinite(qualified) and math.isfinite(repeats):
        return f"{int(qualified)}/{int(repeats)}"
    return ""


def fully_validated_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if fnum(row.get("repeats"), default=0.0) > 0
        and fnum(row.get("qualified_count"), default=-1.0)
        == fnum(row.get("repeats"), default=0.0)
    ]


def write_resource_data_quality(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = [
        "broker_cpu_peak_percent",
        "producer_cpu_peak_percent",
        "consumer_cpu_peak_percent",
        "broker_network_rx_peak_MBps",
        "broker_network_tx_peak_MBps",
        "producer_to_broker_iperf_MBps",
        "broker_to_consumer_iperf_MBps",
        "broker_ram_peak_GB",
    ]
    out_rows = []
    for field in fields:
        available = sum(1 for row in rows if math.isfinite(fnum(row.get(field))))
        if "cpu" in field:
            semantics = "node_exporter busy-percent summary; core normalization not verified for efficiency ranking"
        elif "network" in field:
            semantics = "node-exporter network MB/s peak summary; interface aggregation/window comparability not fully verified"
        elif "iperf" in field:
            semantics = "iperf3 converted from bits/s to MB/s by system inventory; direction labels are available but not a full Kafka-path proof"
        else:
            semantics = "node-exporter memory peak summary"
        out_rows.append(
            {
                "field": field,
                "available_rows": available,
                "missing_rows": len(rows) - available,
                "unit_semantics": semantics,
                "safe_for_efficiency_ranking": "no",
            }
        )
    write_csv(REPORT_ROOT / "analysis_resource_data_quality.csv", out_rows)
    return out_rows


def write_artifact_manifest() -> list[dict[str, Any]]:
    artifact_paths = [
        "analysis_verified_dataset.csv",
        "analysis_verified_summary.md",
        "analysis_descriptive_stats.csv",
        "analysis_controlled_comparisons.csv",
        "analysis_correlations.csv",
        "analysis_model_results.csv",
        "analysis_feature_importance.csv",
        "analysis_pareto_front.csv",
        "analysis_qualification_sensitivity.csv",
        "analysis_validation_shortlist.csv",
        "analysis_validation_shortlist.md",
        "analysis_validation_shortlist.json",
        "analysis_validation_results.csv",
        "analysis_validation_results.md",
        "analysis_validation_results.json",
        "analysis_validation_repeats.csv",
        "analysis_repetition_plan.csv",
        "analysis_repetition_plan.md",
        "analysis_resource_data_quality.csv",
        "kafka_benchmark_complete_analysis.tex",
        "kafka_benchmark_complete_analysis.html",
        "configs/sweeps/simultaneous_validation/validation_manifest.csv",
        "configs/sweeps/simultaneous_validation/validation_run_order.csv",
        "configs/sweeps/simultaneous_validation/README.md",
    ]
    artifact_paths.extend(
        sorted(
            path.relative_to(REPORT_ROOT).as_posix()
            for path in (REPORT_ROOT / "analysis_diagrams").glob("*")
            if path.is_file()
        )
    )
    artifact_paths.extend(
        sorted(
            path.relative_to(REPORT_ROOT).as_posix()
            for path in PLOTS_DIR.glob("*")
            if path.is_file()
        )
    )
    rows = []
    for rel in artifact_paths:
        path = REPORT_ROOT / rel
        if not path.is_file():
            continue
        rows.append(
            {
                "artifact": rel,
                "bytes": path.stat().st_size,
                "sha256": file_sha256(path),
            }
        )
    write_csv(REPORT_ROOT / "analysis_artifact_manifest.csv", rows)
    return rows


def file_sha256(path: Path) -> str:
    if not path.is_file():
        return ""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def make_plots(
    rows: list[dict[str, Any]],
    correlation_rows: list[dict[str, Any]],
    feature_importance: dict[str, list[dict[str, Any]]],
    pareto_rows: list[dict[str, Any]],
    shortlist_rows: list[dict[str, Any]],
) -> None:
    clean_mask = np.array([bool(row.get("is_qualified")) for row in rows])
    colors = ["#1b9e77" if is_clean else "#d95f02" for is_clean in clean_mask]
    balanced = arr(rows, "balanced_app_MBps")
    backlog = arr(rows, "pending_backlog_percent")
    flush = arr(rows, "flush_sec")
    failed = arr(rows, "failed_send_percent")
    producer = arr(rows, "producer_delivered_MBps")
    consumer = arr(rows, "consumer_received_MBps")

    plt.figure(figsize=(7, 4))
    plt.hist(balanced, bins=18, color="#4c78a8", edgecolor="white")
    plt.xlabel("Balanced application throughput (MiB/s)")
    plt.ylabel("Configuration count")
    plt.title("Balanced Throughput Distribution")
    save_plot("balanced_throughput_distribution.pdf")

    plt.figure(figsize=(6, 5))
    plt.scatter(producer, consumer, c=colors, s=28, alpha=0.85)
    limit = max(np.nanmax(producer), np.nanmax(consumer)) * 1.05
    plt.plot([0, limit], [0, limit], "--", color="gray", linewidth=1)
    annotate_configs(rows, "balanced_app_MBps", count=4)
    plt.xlabel("Producer delivered (MiB/s)")
    plt.ylabel("Consumer received (MiB/s)")
    plt.title("Producer Versus Consumer Throughput")
    save_plot("producer_vs_consumer_throughput.pdf")

    scatter_plot(
        backlog,
        balanced,
        colors,
        "Pending backlog at flush start (%)",
        "Balanced application throughput (MiB/s)",
        "Throughput Versus Backlog",
        "throughput_vs_backlog.pdf",
        rows,
    )
    make_shortlist_boundary_plot(rows, shortlist_rows)
    scatter_plot(
        flush,
        balanced,
        colors,
        "Producer flush duration (s)",
        "Balanced application throughput (MiB/s)",
        "Throughput Versus Flush Time",
        "throughput_vs_flush_time.pdf",
        rows,
    )
    scatter_plot(
        failed,
        backlog,
        colors,
        "Failed sends (%)",
        "Pending backlog (%)",
        "Backlog Versus Failed Sends",
        "backlog_vs_failed_sends.pdf",
        rows,
    )
    plt.figure(figsize=(7, 4))
    clean_values = [fnum(row["balanced_app_MBps"]) for row in rows if row["is_clean"]]
    over_values = [fnum(row["balanced_app_MBps"]) for row in rows if not row["is_clean"]]
    plt.boxplot([clean_values, over_values], tick_labels=["clean", "overdriven"])
    plt.ylabel("Balanced application throughput (MiB/s)")
    plt.title("Qualified Versus Overdriven Throughput")
    save_plot("clean_vs_overdriven_throughput.pdf")

    make_parameter_plot(rows, "payload_size_bytes", "payload_effect.pdf")
    make_parameter_plot(rows, "producer_ranks", "producer_rank_effect.pdf")
    make_controlled_plot(rows, "consumer_fetch_min_bytes", "controlled_fetch_min_effect.pdf")

    make_correlation_heatmap(correlation_rows)
    make_feature_importance_plot(feature_importance.get("balanced_app_MBps", []))

    pareto_ids = {row["config_id"] for row in pareto_rows}
    pareto_colors = ["#1b9e77" if row["config_id"] in pareto_ids else "#bdbdbd" for row in rows]
    scatter_plot(
        backlog,
        balanced,
        pareto_colors,
        "Pending backlog (%)",
        "Balanced application throughput (MiB/s)",
        "Pareto Front Projection",
        "pareto_front_projection.pdf",
        rows,
        annotate_top=True,
    )
    pressure = arr(rows, "producer_pressure_proxy")
    scatter_plot(
        pressure,
        backlog,
        colors,
        "Producer pressure proxy (ranks x payload bytes)",
        "Pending backlog (%)",
        "Candidate Saturation Plot",
        "candidate_saturation_plot.pdf",
        rows,
    )
    broker_cpu = arr(rows, "broker_cpu_peak_percent")
    broker_net = arr(rows, "broker_network_rx_peak_MBps")
    scatter_plot(
        broker_cpu,
        broker_net,
        colors,
        "Broker CPU peak (%)",
        "Broker network RX peak (MB/s)",
        "Resource Bottleneck Projection",
        "resource_bottleneck_projection.pdf",
        rows,
    )


def make_shortlist_boundary_plot(
    rows: list[dict[str, Any]], shortlist_rows: list[dict[str, Any]]
) -> None:
    shortlist_ids = {row["config_id"] for row in shortlist_rows}
    plt.figure(figsize=(7.2, 5.0))
    for row in rows:
        if row.get("is_qualified"):
            marker, color, label = "o", "#1b9e77", "qualified"
        elif row["config_id"] in shortlist_ids:
            marker, color, label = "s", "#d95f02", "shortlisted transition/upper"
        else:
            marker, color, label = ".", "#bdbdbd", "other overdriven"
        existing = {text.get_text() for text in plt.gca().texts}
        plt.scatter(
            fnum(row.get("pending_backlog_percent")),
            fnum(row.get("balanced_app_MBps")),
            marker=marker,
            color=color,
            s=46 if row["config_id"] in shortlist_ids else 24,
            alpha=0.85,
            label=label if label not in existing else None,
        )
        if row["config_id"] in shortlist_ids:
            plt.annotate(
                row["config_id"],
                (fnum(row.get("pending_backlog_percent")), fnum(row.get("balanced_app_MBps"))),
                fontsize=8,
                xytext=(4, 4),
                textcoords="offset points",
            )
    plt.axvline(BACKLOG_LIMIT, color="#333333", linestyle="--", linewidth=1)
    plt.text(BACKLOG_LIMIT + 0.4, plt.ylim()[0], "5% backlog limit", fontsize=8, va="bottom")
    handles, labels = plt.gca().get_legend_handles_labels()
    dedup = dict(zip(labels, handles))
    plt.legend(dedup.values(), dedup.keys(), fontsize=8)
    plt.xlabel("Pending backlog at flush start (%)")
    plt.ylabel("Balanced application throughput (MiB/s)")
    plt.title("Shortlisted Configurations Around the Qualification Boundary")
    save_plot("shortlist_throughput_vs_backlog.pdf")


def write_summary(
    rows: list[dict[str, Any]],
    stats_rows: list[dict[str, Any]],
    controlled_rows: list[dict[str, Any]],
    correlation_rows: list[dict[str, Any]],
    model_rows: list[dict[str, Any]],
    pareto_rows: list[dict[str, Any]],
    qualification_sensitivity_rows: list[dict[str, Any]],
    shortlist_rows: list[dict[str, Any]],
    resource_quality_rows: list[dict[str, Any]],
    validation_summary_rows: list[dict[str, Any]],
    validation_repeat_rows: list[dict[str, Any]],
) -> None:
    verdicts = Counter(row["throughput_verdict"] for row in rows)
    top_balanced = max(rows, key=lambda row: fnum(row["balanced_app_MBps"]))
    clean_rows = sorted(
        [row for row in rows if row.get("is_qualified")],
        key=lambda row: -fnum(row["balanced_app_MBps"]),
    )
    mismatch_count = sum(1 for row in rows if row["any_mismatch"])
    top_corr = sorted(
        correlation_rows,
        key=lambda row: abs(fnum(row["spearman_rho"])),
        reverse=True,
    )[:12]
    best_models = best_model_rows(model_rows)
    best_validated = validation_summary_rows[0] if validation_summary_rows else {}
    fully_validated = fully_validated_rows(validation_summary_rows)
    lines = [
        "# Verified Kafka Sweep Analysis Summary",
        "",
        "## Sources Used",
        "",
        f"- Dataset source: `{INPUT_CONTEXT['dataset_source']}` ({INPUT_CONTEXT['dataset_source_kind']}).",
        f"- Combined JSON: `{INPUT_CONTEXT['combined_json']}`.",
        f"- Per-case final reports: `{INPUT_CONTEXT['final_reports_root']}` ({INPUT_CONTEXT['final_reports_loaded']} files loaded).",
        f"- Sweep manifest: `{INPUT_CONTEXT['config_manifest']}`.",
        f"- Validation summary: `{INPUT_CONTEXT['validation_summary']}` ({INPUT_CONTEXT['validation_rows_loaded']} config groups loaded).",
        f"- Validation repeats: `{INPUT_CONTEXT['validation_repeats']}` ({INPUT_CONTEXT['validation_repeat_rows_loaded']} case rows loaded).",
        "- Repository source: current `main` checkout.",
        "",
        "## Verification Results",
        "",
        f"- Configurations: {len(rows)}.",
        f"- Completed rows: {sum(1 for row in rows if row['status'] == 'completed')}.",
        f"- Verdict counts: {dict(verdicts)}.",
        f"- Qualified rows under official thresholds: {len(clean_rows)}.",
        f"- Recalculation mismatch rows: {mismatch_count}.",
        f"- Record-throughput rows validated: {INPUT_CONTEXT.get('record_throughput_rows_validated', 0)}; derivation issues: {INPUT_CONTEXT.get('record_throughput_issues', 0)}.",
        f"- Completed rows before/after record derivation: {INPUT_CONTEXT.get('completed_rows_before_record_derivation', 0)}/{INPUT_CONTEXT.get('completed_rows_after_record_derivation', 0)}.",
        f"- Repeated-validation record-throughput issues: {INPUT_CONTEXT.get('validation_record_throughput_issues', 0)}.",
        "- Balanced throughput, record throughput, qualification metrics, and verdict were recalculated for every row.",
        "- No duplicate configurations were found across the 11 varied settings.",
        "",
        "## Metrics and Units",
        "",
        "Record throughput represents the number of Kafka records successfully produced or consumed per second. It is derived from application payload throughput and the configured payload size. Balanced record throughput is the minimum of producer and consumer record throughput.",
        "",
        "Application payload throughput is reported in binary MiB/s (1 MiB = 1,048,576 bytes). Compatibility column names ending in MBps retain their historical spelling, while the declared unit and canonical fields are MiB/s.",
        "",
        "## Primary Findings",
        "",
    ]
    if best_validated:
        lines.extend(
            [
                f"- Primary validated recommendation: `{validation_config_id(best_validated)}` with {qualified_fraction(best_validated)} qualified repeats and median {fmt(best_validated.get('median_balanced_app_MBps'))} MiB/s.",
                f"- Fully qualified repeated configs: {', '.join(f'`{validation_config_id(row)}` ({qualified_fraction(row)})' for row in fully_validated) or 'none'}.",
                "- Repeated validation ranks configurations by qualified-run frequency first, then median balanced throughput.",
                f"- Highest qualified single observation from the original 120-row sweep: `{clean_rows[0]['config_id']}` with {fmt(clean_rows[0]['balanced_app_MBps'])} MiB/s.",
            ]
        )
    else:
        lines.append(
            f"- Highest qualified single observation: `{clean_rows[0]['config_id']}` with {fmt(clean_rows[0]['balanced_app_MBps'])} MiB/s."
        )
    lines.extend(
        [
        f"- Highest raw observed upper bound: `{top_balanced['config_id']}` with {fmt(top_balanced['balanced_app_MBps'])} MiB/s, but status `{top_balanced['qualification_status']}`.",
        "- Primary recommendation rank sorts qualified configurations before overdriven configurations, then measured balanced throughput.",
        "",
        "| Rank | Config | Balanced MiB/s | Balanced records/s | Qualification |",
        "| ---: | --- | ---: | ---: | --- |",
        ]
    )
    for row in sorted(rows, key=primary_rank_key)[:12]:
        lines.append(
            f"| {row['primary_rank']} | `{row['config_id']}` | {fmt_mbps(row['balanced_app_MBps'])} | "
            f"{fmt_records(row['balanced_records_per_sec'])} | {row['qualification_status']} |"
        )
    lines.append("")
    if validation_summary_rows:
        lines.extend(
            [
                "## Repeated Validation Results",
                "",
                f"- Slurm job: `{INPUT_CONTEXT.get('validation_job_id') or 'unknown'}`; run: `{INPUT_CONTEXT.get('validation_run_id') or 'unknown'}`.",
                "- Qualification is inclusive: backlog <= 5%, flush <= 10 s, and failed sends <= 0.1%.",
                "",
                "| Rank | Config | Qualified | Median MiB/s | Median records/s | Mean MiB/s | Median backlog % | Median flush s | Median failed % | CV |",
                "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in validation_summary_rows:
            lines.append(
                f"| {row.get('validation_rank', '')} | `{validation_config_id(row)}` | {qualified_fraction(row)} | "
                f"{fmt_mbps(row.get('median_balanced_app_MBps'))} | {fmt_records(row.get('median_balanced_records_per_sec'))} | {fmt_mbps(row.get('mean_balanced_app_MBps'))} | "
                f"{fmt(row.get('median_pending_backlog_percent'))} | {fmt(row.get('median_flush_sec'))} | "
                f"{fmt(row.get('median_failed_send_percent'), digits=4)} | {fmt(row.get('cv_balanced_app_MBps'))} |"
            )
        lines.append("")
        lines.extend(validation_record_table_markdown(validation_summary_rows))
    lines.extend(
        [
        "## Validation Shortlist",
        "",
        "| Config | Categories | Balanced MiB/s | Balanced records/s | Qualification |",
        "| --- | --- | ---: | ---: | --- |",
        ]
    )
    for row in shortlist_rows:
        lines.append(
            f"| `{row['config_id']}` | {row['selection_categories']} | {fmt_mbps(row['balanced_app_MBps'])} | {fmt_records(row['balanced_records_per_sec'])} | {row['qualification_status']} |"
        )
    lines.extend(
        [
            "",
            "## Qualification Sensitivity",
            "",
            "| Backlog threshold | Qualified configs | Best config | Best MiB/s | Entering vs 5% | Leaving vs 5% |",
            "| ---: | ---: | --- | ---: | --- | --- |",
        ]
    )
    for row in qualification_sensitivity_rows:
        lines.append(
            f"| {fmt(row['backlog_threshold_percent'])}% | {row['qualified_config_count']} | `{row['highest_qualifying_config_id']}` | {fmt(row['highest_qualifying_balanced_app_MBps'])} | {row['entering_vs_official_5_percent'] or '-'} | {row['leaving_vs_official_5_percent'] or '-'} |"
        )
    lines.extend(
        [
            "",
            "## Resource Metric Data Quality",
            "",
            "Resource metrics are retained as supporting evidence, but CPU-normalized efficiency rankings are intentionally omitted because core normalization, peak-window comparability, and interface aggregation semantics are not fully verified from the summary rows alone.",
            "",
            "## Top Spearman Correlations",
            "",
            "| Feature | Target | rho | q approx |",
            "| --- | --- | ---: | ---: |",
        ]
    )
    for row in top_corr:
        lines.append(
            f"| `{row['feature']}` | `{row['target']}` | {fmt(row['spearman_rho'])} | {fmt_probability(row['spearman_q_bh_approx'])} |"
        )
    lines.extend(
        [
            "",
            "## Model Results",
            "",
            "| Model | Target | Key metrics |",
            "| --- | --- | --- |",
        ]
    )
    for row in best_models:
        if row["task"] == "regression":
            metrics = f"MAE {fmt(row['mae'])}, RMSE {fmt(row['rmse'])}, CV R2 {fmt(row['r2'])}"
        else:
            metrics = (
                f"balanced accuracy {fmt(row['balanced_accuracy'])}, "
                f"precision {fmt(row['precision'])}, recall {fmt(row['recall'])}, F1 {fmt(row['f1'])}"
            )
        lines.append(f"| {row['model']} | `{row['target']}` | {metrics} |")
    lines.extend(
        [
            "",
            "## Limitations",
            "",
            "- The original 120-configuration sweep has one run per configuration; repeated validation covers only the 10 shortlisted configurations.",
            "- The sweep is structured, not a random design; correlations and ML importances are not causal evidence.",
            "- Qualified/overdriven classification is highly imbalanced: only six qualified examples.",
            "- SHAP analysis was not used because the dataset is small and contains only six qualified observations.",
            "",
            "## Important Outputs",
            "",
            "- `analysis_verified_dataset.csv`",
            "- `analysis_descriptive_stats.csv`",
            "- `analysis_controlled_comparisons.csv`",
            "- `analysis_correlations.csv`",
            "- `analysis_model_results.csv`",
            "- `analysis_feature_importance.csv`",
            "- `analysis_pareto_front.csv`",
            "- `analysis_qualification_sensitivity.csv`",
            "- `analysis_validation_shortlist.md`",
            "- `analysis_validation_results.md`",
            "- `analysis_repetition_plan.md`",
            "- `kafka_benchmark_complete_analysis.tex`",
            "- `kafka_benchmark_complete_analysis.html`",
        ]
    )
    (REPORT_ROOT / "analysis_verified_summary.md").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def write_latex_report(
    rows: list[dict[str, Any]],
    stats_rows: list[dict[str, Any]],
    controlled_rows: list[dict[str, Any]],
    correlation_rows: list[dict[str, Any]],
    model_rows: list[dict[str, Any]],
    pareto_rows: list[dict[str, Any]],
    qualification_sensitivity_rows: list[dict[str, Any]],
    repetition_rows: list[dict[str, Any]],
    shortlist_rows: list[dict[str, Any]],
    resource_quality_rows: list[dict[str, Any]],
    validation_summary_rows: list[dict[str, Any]],
    validation_repeat_rows: list[dict[str, Any]],
) -> None:
    top_balanced = max(rows, key=lambda row: fnum(row["balanced_app_MBps"]))
    qualified_rows = sorted([row for row in rows if row.get("is_qualified")], key=within_group_rank_key)
    verdicts = Counter(row["throughput_verdict"] for row in rows)
    mismatch_count = sum(1 for row in rows if row["any_mismatch"])
    best_models = best_model_rows(model_rows)
    top_corr = [
        row
        for row in sorted(correlation_rows, key=lambda item: abs(fnum(item["spearman_rho"])), reverse=True)
        if row["target"] in {"balanced_app_MBps", "pending_backlog_percent", "flush_sec"}
    ][:10]
    top_controlled = sorted(
        controlled_rows,
        key=lambda row: abs(fnum(row["delta_balanced_MBps"])),
        reverse=True,
    )[:12]
    pareto_short = pareto_rows[:12]
    primary_rows = sorted(rows, key=primary_rank_key)[:12]
    closest_boundary = first_shortlist_category(shortlist_rows, "closest backlog boundary")
    transition_rows = [
        row
        for row in shortlist_rows
        if "transition" in row.get("selection_categories", "")
    ]
    dataset_checksum = file_sha256(Path(str(INPUT_CONTEXT.get("dataset_source_path", ""))))
    validation_available = bool(validation_summary_rows)
    best_validated = validation_summary_rows[0] if validation_available else {}
    fully_validated = fully_validated_rows(validation_summary_rows)
    validation_exec_summary = latex_validation_executive_summary(
        best_validated,
        fully_validated,
        top_balanced,
        qualified_rows,
    )
    validation_scope_sentence = (
        "This report uses the completed 120-configuration sweep and the completed 50-case repeated validation run; it does not execute benchmarks."
        if validation_available
        else "This report uses only the completed sweep and prepares a future repetition plan; it does not perform that future experiment."
    )
    validation_results_section = latex_validation_results_section(
        validation_summary_rows, validation_repeat_rows, shortlist_rows
    )
    validation_methodology_section = latex_validation_methodology_section(
        validation_summary_rows, repetition_rows
    )
    conclusion_text = latex_conclusion_text(
        validation_summary_rows,
        qualified_rows,
        top_balanced,
    )
    first_limitation = (
        r"\item The original 120-configuration sweep measured each configuration once; repeated validation covers only the 10 shortlisted configurations."
        if validation_available
        else r"\item Each configuration was measured once, so reproducibility and confidence intervals are not established."
    )

    tex = rf"""
\documentclass[11pt,a4paper]{{article}}
\usepackage[T1]{{fontenc}}
\usepackage[utf8]{{inputenc}}
\usepackage{{lmodern}}
\usepackage{{microtype}}
\usepackage{{graphicx}}
\usepackage{{amsmath}}
\usepackage{{amssymb}}
\usepackage{{geometry}}
\usepackage{{booktabs}}
\usepackage{{tabularx}}
\usepackage{{longtable}}
\usepackage{{array}}
\usepackage{{hyperref}}
\usepackage{{xcolor}}
\geometry{{margin=2.2cm}}
\setlength{{\parindent}}{{0pt}}
\setlength{{\parskip}}{{0.55em}}
\renewcommand{{\arraystretch}}{{1.12}}
\newcommand{{\cfg}}[1]{{\texttt{{cfg\_#1}}}}
\newcommand{{\mbps}}{{\ensuremath{{\,\mathrm{{MiB/s}}}}}}
\hypersetup{{colorlinks=true, linkcolor=blue, urlcolor=blue}}
\title{{Verified Exploratory Analysis of a Single-Broker Kafka Sweep\\\large 120-Configuration Simultaneous Producer/Consumer Study}}
\author{{Sepehr Mahmoodian}}
\date{{July 2026}}
\begin{{document}}
\maketitle

\section{{Introduction}}
This report studies a benchmark simulator for stressing one Kafka broker with simultaneous producer and consumer traffic on GWDG HPC.  The word simulator is used here in the workload-orchestration sense: the repository creates repeatable Kafka scenarios from JSON configuration files, launches MPI producer and consumer ranks, controls topic creation and cleanup, records application-level producer and consumer metrics, and merges those metrics with broker and node telemetry.  It is not a Kafka-native performance tool; the measured workload is generated by the repository's Python/MPI clients.

Figure~\ref{{fig:benchmark-architecture}} is a logical architecture diagram for each benchmark case, not a performance result.  It is consistent with the execution methodology because it separates the four roles used by the Slurm allocation: a producer/controller node, one Kafka broker node, one consumer node, and one monitoring node.  MPI rank 0 coordinates the case on the producer/controller node, producer ranks send fixed-size Kafka records through librdkafka, consumer ranks read records at the same time, and the monitoring node collects Prometheus, JMX exporter, Kafka exporter, and node exporter data.  The solid arrows in Figure~\ref{{fig:benchmark-architecture}} denote Kafka records, the dashed arrows denote monitoring traffic, and the dotted arrow denotes MPI control and orchestration.  The broker is tested as a server under controlled client pressure: each case starts Kafka, creates the configured topic, drives the producer and consumer ranks for the configured time window, collects final reports, and removes the topic before the next case.

{latex_diagram_figure("analysis_diagrams/benchmark_architecture.png", "Logical four-node benchmark architecture.  The producer/controller node launches producer MPI ranks and coordinates the case, the broker node hosts the single Kafka broker and exporters, the consumer node runs consumer ranks, and the monitoring node collects telemetry.  Solid arrows indicate Kafka records, dashed arrows indicate monitoring traffic, and dotted arrows indicate MPI control and orchestration.", "fig:benchmark-architecture")}

\section{{Executive Summary}}
This report analyzes the already completed 120-configuration simultaneous Kafka sweep.  It does not rerun Kafka, submit Slurm jobs, alter raw measurements, or modify the generated configuration JSON files.  The primary method is constrained: first determine whether a configuration satisfies the operational limits, then rank qualified configurations by measured balanced application throughput.

{validation_exec_summary}

\section{{Benchmark Goal and Scope}}
The benchmark measures a single Kafka broker under simultaneous producer and consumer load on the GWDG HPC environment.  Producer and consumer clients are MPI ranks using Python/librdkafka.  The main measured throughput is balanced application throughput,
\begin{{equation}}
T_{{\mathrm{{balanced}}}}=\min(T_{{\mathrm{{producer}}}},T_{{\mathrm{{consumer}}}}).
\label{{eq:balanced-throughput}}
\end{{equation}}
{validation_scope_sentence}

\section{{Benchmark Architecture}}
The benchmark uses one Kafka broker node, one monitoring node, one producer/controller node, and one consumer node.  MPI rank zero coordinates the run; producer ranks send fixed-size records through Python/librdkafka clients; consumer ranks read records simultaneously.  Monitoring uses Prometheus, Kafka JMX exporter, node exporter, and Kafka exporter.  Runtime Kafka and client temporary data are placed in RAM-backed \path{{/dev/shm}} job directories.

\section{{Test Method and Execution Procedure}}
Each test starts from a JSON configuration file.  For the sweep and the repeated validation campaign, the configuration fixes the single-broker Kafka setup and varies the client-side workload: producer ranks, consumer ranks, topic partitions, payload size, producer batching, producer queue limits, and consumer fetch settings.  Broker-level tuning is not treated as an experimental factor in this report.

Figure~\ref{{fig:execution-pipeline}} summarizes the operational method.  The submit helper first validates the configuration or batch manifest, loads the GWDG module environment, and requests a role-split Slurm allocation.  In the four-node layout used here, one node hosts Kafka, one hosts monitoring services, one hosts the producer ranks and colocated MPI controller, and one hosts consumer ranks.  The intended measurement mode is a dedicated allocation for the requested nodes so that unrelated jobs do not share the benchmark nodes during a case.

Inside the allocation, the runner prepares RAM-backed runtime directories, starts node exporter, Prometheus, Kafka, JMX exporter, and Kafka exporter when enabled, creates the configured Kafka topic, and then launches the MPI workload.  During the measured window, producers and consumers run simultaneously.  After the window closes, the scripts collect application metrics, broker and node telemetry, final reports, and monitoring summaries.  For sweep batches, the topic is deleted after each case and the runner waits for broker RAM-backed storage to return to a usable state before the next case starts.

The normal single-case command sequence is:
\begin{{verbatim}}
./scripts/hpc_preflight.sh configs/one_broker_mpi_simultaneous.json
DRY_RUN=1 ./scripts/submit_hpc_case.sh configs/one_broker_mpi_simultaneous.json
./scripts/submit_hpc_case.sh configs/one_broker_mpi_simultaneous.json
\end{{verbatim}}
The repeated validation run used the batch path:
\begin{{verbatim}}
./scripts/submit_simultaneous_budgeted_batch.sh \
  configs/sweeps/simultaneous_validation/validation_run_batch.csv
\end{{verbatim}}
After the HPC run finishes, \texttt{{scripts/generate\_complete\_analysis.py}} reads the verified sweep CSV and repeated-validation CSV files, recomputes derived fields, applies the qualification rule in Equation~\ref{{eq:qualification-rule}}, generates tables and plots, and writes the Markdown, HTML, CSV, JSON, and LaTeX report artifacts.

{latex_diagram_figure("analysis_diagrams/execution_pipeline.png", "End-to-end execution pipeline from configuration JSON through preflight checks, Slurm submission, service startup, simultaneous MPI workload execution, metric collection, verified CSV merge, analysis artifact generation, repeated validation, and final recommendation.", "fig:execution-pipeline")}

\section{{Sweep Design and Parameter Space}}
The sweep contains {len(rows)} configurations and varies producer ranks, consumer ranks, partitions, payload size, producer batch size, \texttt{{linger.ms}}, producer queue limits, and consumer fetch settings.  Broker-level settings were not varied as an experimental dimension in this sweep.  Figure~\ref{{fig:sweep-design}} visualizes the parameter families, and Table~\ref{{tab:parameter-space}} lists the exact values used by the generator.

{latex_diagram_figure("analysis_diagrams/sweep_design_parameter_space.png", "Sweep design and parameter space for the 120 single-broker configurations, grouped by workload concurrency, topic layout, producer tuning, consumer tuning, and payload size.", "fig:sweep-design")}

{latex_parameter_space_table(rows)}

\section{{Metrics, Qualification Rule, and Data Verification}}
Pending backlog is producer-side work still lacking delivery callbacks at flush start.  Failed-send percentage is failed sends divided by attempted sends.  Record throughput represents the number of Kafka records successfully produced or consumed per second. It is derived from application payload throughput and the configured payload size. Balanced record throughput is the minimum of producer and consumer record throughput.  Application payload throughput is reported in binary MiB/s (1 MiB = 1,048,576 bytes). Compatibility field names ending in \texttt{{MBps}} retain their historical spelling, but the declared unit and canonical fields are MiB/s.  The record rates are calculated as
\[
\begin{{aligned}}
R_{{\mathrm{{producer}}}}&=T_{{\mathrm{{producer}}}}\frac{{1{{,}}048{{,}}576}}{{P}}, &
R_{{\mathrm{{consumer}}}}&=T_{{\mathrm{{consumer}}}}\frac{{1{{,}}048{{,}}576}}{{P}},\\
R_{{\mathrm{{balanced}}}}&=\min(R_{{\mathrm{{producer}}}},R_{{\mathrm{{consumer}}}}).&&
\end{{aligned}}
\]
where $P$ is payload size in bytes.  Producer flush uses a 30 second timeout, but the official qualification limit is stricter:
\begin{{equation}}
B \le 5\% \quad \land \quad F \le 10\,\mathrm{{s}} \quad \land \quad E \le 0.1\%.
\label{{eq:qualification-rule}}
\end{{equation}}
Values exactly equal to a limit are qualified.  A row is overdriven only when one or more limits are exceeded.  Figure~\ref{{fig:qualification-gate}} summarizes the metric flow and the qualification gate defined by Equation~\ref{{eq:qualification-rule}}.  Table~\ref{{tab:verification}} records the verification counts from the original 120-row sweep.

{latex_diagram_figure("analysis_diagrams/metrics_qualification_verification.png", "Qualification workflow linking producer and consumer measurements to balanced throughput, applying the backlog, flush-time, and failed-send limits, and summarizing verification counts for the 120-row sweep.", "fig:qualification-gate")}

\begin{{table}}[ht]
\centering
\begin{{tabular}}{{lr}}
\toprule
Check & Result \\
\midrule
Rows in combined CSV & {len(rows)} \\
Completed rows & {sum(1 for row in rows if row['status'] == 'completed')} \\
Overdriven rows & {sum(1 for row in rows if row['qualification_status'] == 'overdriven')} \\
Qualified rows & {len(qualified_rows)} \\
Rows with any recalculation mismatch & {mismatch_count} \\
Record-throughput derivation issues & {INPUT_CONTEXT.get('record_throughput_issues', 0)} \\
Duplicate 11-field configurations & 0 \\
\bottomrule
\end{{tabular}}
\caption{{Verification summary.}}
\label{{tab:verification}}
\end{{table}}

\section{{Primary Results}}
The primary recommendation rank puts qualified rows before overdriven rows, then sorts by higher measured balanced throughput, lower backlog, lower flush time, lower failed-send percentage, and configuration ID.  Table~\ref{{tab:primary-ranking}} shows the primary ranking excerpt from the single-run sweep.  Figure~\ref{{fig:shortlistboundary}} captures the qualification-first screening state that produced the validation shortlist.  Table~\ref{{tab:validation-results}} supersedes the single-run screening state for the final validated recommendation.

{latex_primary_ranking_table(primary_rows)}

Figure~\ref{{fig:shortlistboundary}} places the validation-shortlist configurations in throughput--backlog space.  The x-axis is pending producer backlog in percent, the y-axis is balanced application throughput in MiB/s, and the vertical reference line is the official 5\% backlog threshold.  Points to the right of the line are retained only as boundary or overload evidence unless they qualify under Equation~\ref{{eq:qualification-rule}}.

\begin{{figure}}[ht]
\centering
\includegraphics[width=0.82\linewidth]{{analysis_plots/shortlist_throughput_vs_backlog.pdf}}
\caption{{Balanced throughput versus backlog.  The vertical line is the official 5\% backlog threshold.  Labels are limited to validation-shortlist configurations.}}
\label{{fig:shortlistboundary}}
\end{{figure}}

The closest useful backlog-boundary point is \cfg{{{cfg_suffix_from_id(closest_boundary.get('config_id', 'cfg_000'))}}} if repeated validation confirms that its modest backlog excess is reproducible and not noise.  The raw upper-bound observation, \cfg{{{cfg_suffix(top_balanced)}}}, remains useful as an overload reference but is not a sustainable-throughput claim.

Figure~\ref{{fig:throughputdist}} summarizes the full 120-row single-run throughput distribution.  It shows that the validation candidates span both the qualified region and the high-throughput overdriven region, which is why repeated validation is required before turning an observed high-throughput point into a sustainable-throughput claim.

\begin{{figure}}[ht]
\centering
\includegraphics[width=0.82\linewidth]{{analysis_plots/balanced_throughput_distribution.pdf}}
\caption{{Distribution of balanced application throughput across the 120 configurations.}}
\label{{fig:throughputdist}}
\end{{figure}}

{validation_results_section}

\section{{Threshold Sensitivity}}
The official qualification rule remains backlog \(\le 5\%\), flush \(\le 10\) seconds, and failed sends \(\le 0.1\%\).  Table~\ref{{tab:qualification-sensitivity}} changes only the backlog threshold to show how sensitive qualification is near the boundary.

{latex_qualification_sensitivity_table(qualification_sensitivity_rows)}

\section{{Controlled Parameter Findings}}
Controlled comparisons were generated by grouping configurations that match on ten of the eleven varied client-side parameters and differ in exactly one parameter.  Table~\ref{{tab:controlled-comparisons}} reports the largest absolute changes in balanced throughput among those one-factor pairs.  The \texttt{{Base}} and \texttt{{Compare}} columns identify the two configurations in the pair; \(\Delta\)Throughput, \(\Delta\)Backlog, \(\Delta\)Flush, and \(\Delta\)Failed are calculated as \texttt{{Compare}} minus \texttt{{Base}}.  Balanced throughput is measured in MiB/s, backlog and failed sends are percentages, and flush is measured in seconds.  A positive throughput delta means the comparison configuration was faster; positive backlog, flush, or failed-send deltas mean it was less stable by that metric.

{latex_controlled_table(top_controlled)}

The strongest one-factor contrasts are dominated by concurrency and client buffering choices.  Several comparisons increase balanced throughput only while also increasing backlog or flush time, so the table supports the section's main interpretation: parameter changes can move the system toward a higher observed load, but single-run one-factor contrasts do not establish a sustainable configuration unless the qualification gate is still satisfied.

\section{{Resource and Bottleneck Evidence}}
Resource fields from node exporter, JMX, and iperf summaries are retained as supporting evidence.  CPU-normalized efficiency rankings are omitted because the summary data do not fully prove core normalization, peak-window comparability, or interface aggregation semantics.  Table~\ref{{tab:resource-quality}} defines each resource field, reports how many of the 120 sweep rows contain a value, and records the semantic caveat that limits how the field can be interpreted.  The table supports the final recommendation indirectly: resource metrics help identify possible bottlenecks and data-quality limits, but they do not override the application-level qualification rule in Equation~\ref{{eq:qualification-rule}}.

{latex_resource_quality_table(resource_quality_rows)}

Figure~\ref{{fig:resource}} projects the 120 single-run configurations onto broker CPU peak and broker network receive peak.  Each point is one completed configuration, using the node-exporter peak summaries copied into the verified dataset.  The figure is useful for checking whether high-throughput or overdriven rows cluster in a resource-pressure region, but it is not used to rank configurations because the summary fields do not prove comparable measurement windows, interface aggregation, or CPU normalization.

\begin{{figure}}[ht]
\centering
\includegraphics[width=0.82\linewidth]{{analysis_plots/resource_bottleneck_projection.pdf}}
\caption{{Broker resource projection for the 120 single-run configurations.  The horizontal axis is broker CPU peak percentage from node exporter; the vertical axis is broker network receive peak in MB/s.  Each point represents one completed configuration.  The plot is supporting bottleneck evidence only because peak-window and interface-aggregation semantics are not sufficient for CPU-normalized or network-utilization rankings.}}
\label{{fig:resource}}
\end{{figure}}

{validation_methodology_section}

\section{{Limitations}}
\begin{{itemize}}
{first_limitation}
\item The sweep is structured and partly seeded; it is not a randomized experimental design.
\item Qualified/overdriven thresholds are benchmark design choices.
\item Classification has only six qualified examples.
\item Correlations, regressions, machine-learning importance, and Pareto membership are exploratory, not causal evidence.
\end{{itemize}}

\section{{Conclusion}}
{conclusion_text}
\appendix
\section{{Correlation and Regression Details}}
Appendix~B reports exploratory associations, not causal effects.  Table~\ref{{tab:spearman-correlations}} lists the strongest Spearman rank correlations between input features and key targets.  Spearman \(\rho\) ranges from \(-1\) to \(+1\): positive values mean larger feature values tend to appear with larger target values, negative values mean the opposite, and values near zero indicate weak monotonic association.  The BH \(q\) value is an approximate Benjamini--Hochberg false-discovery-adjusted value; smaller values indicate stronger evidence against a zero rank correlation, subject to the limitation that the sweep is structured rather than randomized.

{latex_correlation_table(top_corr)}

The largest correlations connect worker concurrency and payload size with backlog and flush behavior.  This supports the overload interpretation: more concurrency and larger records can raise observed throughput, but they also tend to increase producer-side backlog and flush pressure.

Table~\ref{{tab:ridge-regression}} summarizes the best cross-validated regression rows retained for each continuous target.  MAE is mean absolute error in the target's units, RMSE is root mean squared error, median AE is median absolute error, and \(R^2\) is the cross-validated coefficient of determination.  Individual ridge coefficients are not reported as scientific effect estimates because the input features are standardized, regularized, and correlated by the sweep design; a coefficient would be a predictive weight conditional on the other encoded features, not an isolated causal contribution.  The models explain backlog and flush more effectively than failed-send percentage, while balanced-throughput prediction remains only moderately accurate.  These fits are useful as descriptive checks on the sweep, not as a replacement for repeated benchmark runs.

{latex_model_table([row for row in best_models if row['task'] == 'regression'], "Cross-validated exploratory regression model summary.", "tab:ridge-regression")}

\section{{Exploratory Machine-Learning Analysis}}
The machine-learning analysis is included to check whether the measured sweep contains learnable structure, not to claim an optimized production model.  The input features are the numeric configuration fields used throughout the report: producer and consumer ranks, total worker ranks, producer/consumer rank ratio, partitions, batch size, linger time, payload size, producer queue limits, and consumer fetch settings, with power-of-two size fields represented on a log2 scale.  The regression target for Figure~\ref{{fig:importance}} is balanced application throughput in MiB/s.  The classification target in Table~\ref{{tab:ml-classification}} is \texttt{{clean\_vs\_overdriven}}, where the positive class is a qualified row satisfying backlog \(\le 5\%\), flush \(\le 10\) s, and failed sends \(\le 0.1\%\).

The models are intentionally simple: ridge regression and a lightweight random-forest regressor for continuous targets, and weighted logistic regression plus a lightweight random-forest classifier for the clean/overdriven target.  Regression models use five-fold cross-validation over the 120 single-run sweep rows; classification uses stratified folds because there are only six qualified observations.  Table~\ref{{tab:ml-classification}} reports balanced accuracy, precision, recall, specificity, F1, and PR-AUC.  Precision is low because the qualified class is rare; recall is more informative for checking whether qualified configurations can be identified at all.  The results are therefore exploratory and should be interpreted as consistency checks supporting the benchmark analysis, not as independent evidence of sustainability.

{latex_model_table([row for row in best_models if row['task'] == 'classification'], "Exploratory clean-versus-overdriven classification summary.", "tab:ml-classification")}

Figure~\ref{{fig:importance}} reports permutation importance for the balanced-throughput prediction target.  The y-axis lists input features, and the x-axis gives the increase in mean absolute error when that feature is randomly permuted while the fitted model and the other features are held fixed.  Larger bars therefore indicate features whose values are more important for predictive accuracy in this exploratory model.  Payload size has the largest error increase, followed by producer rank count and the producer/consumer rank ratio.  This is practically useful because it points to workload scale and record size as major throughput drivers, but it remains non-causal: correlated features and the structured sweep design can inflate or suppress individual importances.

\begin{{figure}}[ht]
\centering
\includegraphics[width=0.9\linewidth]{{analysis_plots/feature_importance_balanced.pdf}}
\caption{{Exploratory permutation feature importance for balanced-throughput prediction.  Bars show the increase in mean absolute error, in MiB/s, after randomly permuting each feature.  Larger values indicate greater predictive contribution within the fitted exploratory model, not a causal effect.}}
\label{{fig:importance}}
\end{{figure}}

\section{{Full Pareto Table Excerpt}}
Pareto membership is useful for screening but not a primary recommendation method.  In this benchmark, a configuration is Pareto-optimal if no other configuration has at least as much balanced throughput while also having no worse backlog, flush time, and failed-send percentage, with at least one of those objectives strictly better.  Table~\ref{{tab:pareto-excerpt}} lists the highest-throughput excerpt of the Pareto set.  Balanced throughput is maximized, while backlog, flush, and failed sends are minimized.  The table is ordered by balanced throughput, not by the qualification-first recommendation rank, so many entries are intentionally overdriven upper-bound points.  The trade-off is the central lesson: the configurations with the largest raw throughput often carry large backlog or flush penalties, whereas the final recommendation requires repeated qualification as well as competitive throughput.

{latex_pareto_table(pareto_short)}

\section{{Provenance and Artifact Manifest}}
\begin{{table}}[ht]
\centering
\small
\begin{{tabularx}}{{\linewidth}}{{lX}}
\toprule
Field & Value \\
\midrule
Generation timestamp & \texttt{{{tex_escape(INPUT_CONTEXT['generation_timestamp'])}}} \\
Dataset source & {tex_path(INPUT_CONTEXT['dataset_source'])} \\
Dataset SHA-256 & {tex_breakable_mono(dataset_checksum)} \\
Qualification rule & \texttt{{{tex_escape(QUALIFICATION_RULE_VERSION)}}} \\
\bottomrule
\end{{tabularx}}
\caption{{Report provenance for the generated analysis artifacts.}}
\label{{tab:report-provenance}}
\end{{table}}

Generated artifact checksums are written to \texttt{{analysis\_artifact\_manifest.csv}}.

\end{{document}}
"""
    (REPORT_ROOT / "kafka_benchmark_complete_analysis.tex").write_text(
        tex, encoding="utf-8"
    )


def write_html_report(
    rows: list[dict[str, Any]],
    controlled_rows: list[dict[str, Any]],
    correlation_rows: list[dict[str, Any]],
    model_rows: list[dict[str, Any]],
    pareto_rows: list[dict[str, Any]],
    qualification_sensitivity_rows: list[dict[str, Any]],
    repetition_rows: list[dict[str, Any]],
    shortlist_rows: list[dict[str, Any]],
    resource_quality_rows: list[dict[str, Any]],
    validation_summary_rows: list[dict[str, Any]],
    validation_repeat_rows: list[dict[str, Any]],
) -> None:
    top_balanced = max(rows, key=lambda row: fnum(row["balanced_app_MBps"]))
    qualified_rows = sorted([row for row in rows if row.get("is_qualified")], key=within_group_rank_key)
    closest = first_shortlist_category(shortlist_rows, "closest backlog boundary")
    primary_rows = sorted(rows, key=primary_rank_key)[:20]
    best_models = best_model_rows(model_rows)
    top_corr = sorted(
        correlation_rows,
        key=lambda row: abs(fnum(row["spearman_rho"])),
        reverse=True,
    )[:15]
    top_controlled = sorted(
        controlled_rows,
        key=lambda row: abs(fnum(row["delta_balanced_MBps"])),
        reverse=True,
    )[:20]
    best_validated = validation_summary_rows[0] if validation_summary_rows else {}
    fully_validated = fully_validated_rows(validation_summary_rows)
    top_html_card = (
        html_card(
            "Primary validated",
            validation_config_id(best_validated),
            f"{qualified_fraction(best_validated)}; median {fmt(best_validated.get('median_balanced_app_MBps'))} MiB/s",
        )
        if best_validated
        else html_card("Highest qualified", qualified_rows[0]["config_id"], f"{fmt(qualified_rows[0]['balanced_app_MBps'])} MiB/s")
    )
    validation_notice = (
        f"<p class='note'>Repeated validation job <code>{html.escape(str(INPUT_CONTEXT.get('validation_job_id') or 'unknown'))}</code> "
        f"loaded {len(validation_repeat_rows)} repeat cases across {len(validation_summary_rows)} configs. "
        "Recommendations rank qualified-repeat frequency first, then median balanced throughput.</p>"
        if validation_summary_rows
        else "<p class='note'>Official qualification requires backlog <= 5%, flush <= 10 s, and failed sends <= 0.1%. Ranking is qualification first, followed by measured balanced throughput and operational tie-breakers.</p>"
    )

    body = [
        "<!doctype html>",
        "<html><head><meta charset='utf-8'>",
        "<title>Verified Exploratory Kafka Sweep Analysis</title>",
        "<style>",
        "body{font-family:system-ui,-apple-system,Segoe UI,sans-serif;margin:0;color:#17202a;background:#f7f8fa}",
        "main{max-width:1180px;margin:0 auto;padding:28px}",
        "h1{font-size:30px;margin:0 0 8px} h2{margin-top:34px;border-bottom:1px solid #d8dee6;padding-bottom:6px}",
        ".subtitle{color:#566573;margin-bottom:22px}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px}",
        ".card{background:white;border:1px solid #dfe5ec;border-radius:8px;padding:14px}.card .label{font-size:12px;color:#607080;text-transform:uppercase}.card .value{font-size:24px;font-weight:700;margin-top:4px}",
        "table{border-collapse:collapse;width:100%;background:white;margin:12px 0;border:1px solid #dfe5ec}th,td{padding:7px 9px;border-bottom:1px solid #edf1f5;text-align:left;font-size:13px}th{background:#eef3f7}td.num{text-align:right;font-variant-numeric:tabular-nums}",
        "code{background:#eef2f5;padding:1px 4px;border-radius:4px}.note{background:#fff7dd;border-left:4px solid #d7a928;padding:10px 12px}.ok{color:#0b7a53}.warn{color:#a35100}",
        "details{background:white;border:1px solid #dfe5ec;border-radius:8px;padding:10px 12px;margin:12px 0}summary{font-weight:650;cursor:pointer}",
        "img{max-width:100%;background:white;border:1px solid #dfe5ec;border-radius:8px;padding:8px}",
        "</style></head><body><main>",
        "<h1>Verified Exploratory Analysis of a Single-Broker Kafka Sweep</h1>",
        "<div class='subtitle'>Qualification-first analysis of the completed 120-configuration simultaneous sweep. No benchmark workload is executed by this report.</div>",
        "<section class='cards'>",
        top_html_card,
        html_card("Closest boundary", closest.get("config_id", "n/a"), f"{fmt(closest.get('balanced_app_MBps'))} MiB/s"),
        html_card("Highest raw upper bound", top_balanced["config_id"], f"{fmt(top_balanced['balanced_app_MBps'])} MiB/s"),
        "</section>",
        validation_notice,
    ]
    if validation_summary_rows:
        body.extend(
            [
                "<h2>Metrics and Units</h2>",
                "<p>Record throughput represents the number of Kafka records successfully produced or consumed per second. It is derived from application payload throughput and the configured payload size. Balanced record throughput is the minimum of producer and consumer record throughput.</p>",
                "<p>Application payload throughput uses binary MiB/s (1 MiB = 1,048,576 bytes). Compatibility field names ending in MBps retain their historical spelling.</p>",
                "<h2>Repeated Validation Results</h2>",
                html_rows_table(
                    validation_summary_rows,
                    [
                        "validation_rank",
                        "original_config_id",
                        "qualified_count",
                        "repeats",
                        "median_balanced_app_MBps",
                        "median_balanced_records_per_sec",
                        "mean_balanced_app_MBps",
                        "median_pending_backlog_percent",
                        "median_flush_sec",
                        "median_failed_send_percent",
                        "cv_balanced_app_MBps",
                    ],
                ),
                "<p>Fully qualified repeated configs: "
                + html.escape(", ".join(f"{validation_config_id(row)} ({qualified_fraction(row)})" for row in fully_validated) or "none")
                + ".</p>",
                "<h3>Throughput Units for Final Configurations</h3>",
                "<p>Record-rate medians are calculated from each individual repeat before aggregation.</p>",
                html_rows_table(
                    selected_validation_record_rows(validation_summary_rows),
                    [
                        "original_config_id",
                        "payload_size_bytes",
                        "median_producer_delivered_MBps",
                        "median_consumer_received_MBps",
                        "median_balanced_app_MBps",
                        "median_producer_records_per_sec",
                        "median_consumer_records_per_sec",
                        "median_balanced_records_per_sec",
                    ],
                ),
            ]
        )
    body.extend(
        [
        "<h2>Primary Ranking</h2>",
        html_rows_table(primary_rows, ["primary_rank", "config_id", "balanced_app_MBps", "balanced_records_per_sec", "pending_backlog_percent", "flush_sec", "failed_send_percent", "qualification_status", "qualification_reason"]),
        "<h2>Selected Validation Shortlist</h2>",
        html_rows_table(shortlist_rows, ["config_id", "selection_categories", "balanced_app_MBps", "balanced_records_per_sec", "pending_backlog_percent", "flush_sec", "failed_send_percent", "qualification_status", "selection_reason"]),
        "<h2>Shortlist Boundary Plot</h2>",
        "<img src='analysis_plots/shortlist_throughput_vs_backlog.png' alt='Throughput versus backlog shortlist plot'>",
        "<h2>Threshold Sensitivity</h2>",
        html_rows_table(qualification_sensitivity_rows, ["backlog_threshold_percent", "qualified_config_count", "highest_qualifying_config_id", "highest_qualifying_balanced_app_MBps", "entering_vs_official_5_percent", "leaving_vs_official_5_percent"]),
        "<h2>Future Repetition Plan</h2>",
        "<p>Use five randomized blocks with fixed seed <code>{}</code>. Each selected configuration appears once per block. Later recommendations should rank qualified-run frequency first, then median balanced throughput.</p>".format(VALIDATION_SEED),
        html_rows_table(repetition_rows, ["config_id", "selection_categories", "recommended_repetitions", "minimum_repetitions", "balanced_app_MBps", "qualification_status"]),
        "<h2>Configuration And Result Definitions</h2>",
        html_glossary(),
        "<details><summary>Controlled Comparisons</summary>",
        html_rows_table(top_controlled, ["changed_parameter", "base_config", "comparison_config", "base_value", "comparison_value", "delta_balanced_MBps", "delta_backlog_percent", "delta_flush_sec", "delta_failed_send_percent"]),
        "</details>",
        "<details><summary>Exploratory Correlations And ML</summary>",
        "<p>These analyses are exploratory because the sweep is structured and has only six qualified observations. SHAP analysis was not used.</p>",
        html_rows_table(top_corr, ["feature", "target", "spearman_rho", "spearman_q_bh_approx"]),
        html_rows_table(best_models, ["task", "model", "target", "mae", "rmse", "r2", "balanced_accuracy", "precision", "recall", "f1"]),
        "</details>",
        "<details><summary>Resource Metric Data Quality</summary>",
        "<p>Resource metrics are supporting evidence only; CPU-normalized efficiency rankings are omitted until normalization and windows are verified.</p>",
        html_rows_table(resource_quality_rows, ["field", "available_rows", "missing_rows", "unit_semantics", "safe_for_efficiency_ranking"]),
        "</details>",
        "</main></body></html>",
        ]
    )
    (REPORT_ROOT / "kafka_benchmark_complete_analysis.html").write_text(
        "\n".join(body) + "\n", encoding="utf-8"
    )


def html_card(label: str, title: str, value: str) -> str:
    return (
        "<div class='card'>"
        f"<div class='label'>{html.escape(label)}</div>"
        f"<div class='value'><code>{html.escape(title)}</code></div>"
        f"<div>{html.escape(value)}</div>"
        "</div>"
    )


def html_rows_table(rows: list[dict[str, Any]], fields: list[str]) -> str:
    lines = ["<table><thead><tr>"]
    for field in fields:
        lines.append(f"<th>{html.escape(field)}</th>")
    lines.append("</tr></thead><tbody>")
    for row in rows:
        lines.append("<tr>")
        for field in fields:
            value = row.get(field, "")
            css = " class='num'" if math.isfinite(fnum(value)) else ""
            if css and "records_per_sec" in field:
                rendered = fmt_records(value)
            elif css and "MBps" in field:
                rendered = fmt_mbps(value)
            else:
                rendered = fmt(value) if css else str(value)
            lines.append(f"<td{css}>{html.escape(rendered)}</td>")
        lines.append("</tr>")
    lines.append("</tbody></table>")
    return "\n".join(lines)


def html_glossary() -> str:
    rows = [
        ("balanced_app_MBps", "Minimum of producer delivered and consumer received application throughput."),
        ("producer_records_per_sec", "Producer payload throughput converted to Kafka records per second using payload size."),
        ("consumer_records_per_sec", "Consumer payload throughput converted to Kafka records per second using payload size."),
        ("balanced_records_per_sec", "Minimum of producer and consumer record throughput."),
        ("qualification_status", "Qualified only when backlog, flush, and failed-send limits are all satisfied."),
        (
            "pending_backlog_percent",
            "Producer delivery callbacks pending at flush start divided by the "
            "qualification policy's declared denominator: measurement-period "
            "send attempts for qualification.application.v1 and successfully "
            "enqueued messages for historical policies.",
        ),
        ("flush_sec", "Producer flush duration; official qualification limit is 10 seconds."),
        ("failed_send_percent", "Failed sends divided by attempted sends; official qualification limit is 0.1%."),
    ]
    return html_rows_table(
        [{"field": field, "definition": definition} for field, definition in rows],
        ["field", "definition"],
    )


# ----------------------------- utility functions ----------------------------


def dict_value(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def list_value(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def first_number(*values: Any) -> Any:
    for value in values:
        normalized = normalize_number(value)
        if normalized != "":
            return normalized
    return ""


def normalize_number(value: Any) -> Any:
    if value is None or value == "":
        return ""
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip()
    if not text:
        return ""
    try:
        number = float(text)
    except ValueError:
        return value
    if math.isfinite(number) and number.is_integer():
        return int(number)
    return number


def fnum(value: Any, default: float = math.nan) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def fmt(value: Any, digits: int = 3) -> str:
    number = fnum(value)
    if not math.isfinite(number):
        return ""
    if abs(number) >= 1000:
        return f"{number:.0f}"
    return f"{number:.{digits}f}".rstrip("0").rstrip(".")


def fmt_mbps(value: Any) -> str:
    number = fnum(value)
    return f"{number:.3f}" if math.isfinite(number) else ""


def fmt_records(value: Any) -> str:
    number = fnum(value)
    return f"{number:,.0f}" if math.isfinite(number) else ""


def fmt_records_latex(value: Any) -> str:
    return fmt_records(value).replace(",", r"{,}")


def fmt_probability(value: Any, digits: int = 3) -> str:
    number = fnum(value)
    if not math.isfinite(number):
        return ""
    if 0 <= number < 10 ** (-digits):
        return f"<0.{'0' * (digits - 1)}1"
    return fmt(number, digits=digits)


def fmt_probability_latex(value: Any, digits: int = 3) -> str:
    rendered = fmt_probability(value, digits=digits)
    if rendered.startswith("<"):
        return rf"$<{rendered[1:]}$"
    return rendered


def mismatch(stored: float, recalculated: float, tolerance: float) -> bool:
    if not math.isfinite(stored) or not math.isfinite(recalculated):
        return False
    return abs(stored - recalculated) > tolerance


def min_positive(values: Iterable[float]) -> float:
    positives = [value for value in values if math.isfinite(value) and value > 0]
    return min(positives) if positives else 0.0


def log2_or_nan(value: Any) -> float:
    number = fnum(value)
    if not math.isfinite(number) or number <= 0:
        return math.nan
    return math.log2(number)


def role_peak(monitoring: dict[str, Any], metric_id: str, role: str) -> Any:
    peaks = []
    for metric in list_value(monitoring.get("collected_metrics")):
        metric = dict_value(metric)
        if metric.get("id") != metric_id:
            continue
        for series in list_value(metric.get("series")):
            series = dict_value(series)
            labels = dict_value(series.get("labels"))
            if labels.get("role") == role:
                value = fnum(series.get("max_value"))
                if math.isfinite(value):
                    peaks.append(value)
    return max(peaks) if peaks else ""


def metric_value(monitoring: dict[str, Any], metric_id: str, key: str) -> Any:
    for metric in list_value(monitoring.get("collected_metrics")):
        metric = dict_value(metric)
        if metric.get("id") == metric_id:
            return first_number(metric.get(key))
    return ""


def skewness(arr: np.ndarray) -> float:
    if len(arr) < 3:
        return math.nan
    mean = arr.mean()
    std = arr.std(ddof=0)
    if std == 0:
        return 0.0
    return float(np.mean(((arr - mean) / std) ** 3))


def ordered_fieldnames(rows: list[dict[str, Any]]) -> list[str]:
    preferred = [
        "config_id",
        "status",
        "config_path",
        "manifest_note",
        "is_manifest_baseline",
        *CONFIG_FIELDS,
        *PERFORMANCE_FIELDS,
        "qualification_policy_id",
        "is_eligible",
        "is_qualified",
        "qualification_status",
        "qualification_reason",
        "exceeded_backlog_limit",
        "exceeded_flush_limit",
        "exceeded_failure_limit",
        "primary_rank",
        "qualified_rank",
        "raw_throughput_rank",
        "balanced_app_MBps_recalc",
        "pending_backlog_percent_recalc",
        "failed_send_percent_recalc",
        "backlog_denominator",
        "backlog_denominator_value",
        "any_mismatch",
        "balanced_mismatch",
        "broker_combined_mismatch",
        "backlog_mismatch",
        "failed_send_mismatch",
        "qualification_mismatch",
        *RESOURCE_FIELDS,
    ]
    fieldnames = []
    for field in preferred:
        if any(field in row for row in rows) and field not in fieldnames:
            fieldnames.append(field)
    for row in rows:
        for field in row:
            if field not in fieldnames:
                fieldnames.append(field)
    return fieldnames


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    if fieldnames is None:
        fieldnames = ordered_fieldnames(rows) if rows else []
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)


def numeric_values(rows: list[dict[str, Any]], field: str) -> list[float]:
    values = []
    for row in rows:
        value = fnum(row.get(field))
        if math.isfinite(value):
            values.append(value)
    return values


def arr(rows: list[dict[str, Any]], field: str) -> np.ndarray:
    return np.array([fnum(row.get(field)) for row in rows], dtype=float)


def rankdata(values: np.ndarray) -> np.ndarray:
    order = np.argsort(values)
    ranks = np.empty(len(values), dtype=float)
    i = 0
    while i < len(values):
        j = i
        while j + 1 < len(values) and values[order[j + 1]] == values[order[i]]:
            j += 1
        avg_rank = (i + j) / 2.0 + 1.0
        ranks[order[i : j + 1]] = avg_rank
        i = j + 1
    return ranks


def pearson_corr(x: np.ndarray, y: np.ndarray) -> float:
    if len(x) < 2:
        return math.nan
    x_centered = x - x.mean()
    y_centered = y - y.mean()
    denom = math.sqrt(float((x_centered**2).sum() * (y_centered**2).sum()))
    return float((x_centered * y_centered).sum() / denom) if denom else 0.0


def spearman(x: np.ndarray, y: np.ndarray) -> float:
    return pearson_corr(rankdata(x), rankdata(y))


def approx_corr_pvalue(rho: float, n: int) -> float:
    if n < 4 or not math.isfinite(rho):
        return math.nan
    z = abs(rho) * math.sqrt(max(n - 1, 1))
    return math.erfc(z / math.sqrt(2.0))


def benjamini_hochberg(p_values: list[float]) -> list[float]:
    indexed = [(idx, p) for idx, p in enumerate(p_values) if math.isfinite(p)]
    indexed.sort(key=lambda item: item[1])
    q_values = [math.nan] * len(p_values)
    m = len(indexed)
    prev = 1.0
    for rank, (idx, p) in reversed(list(enumerate(indexed, start=1))):
        q = min(prev, p * m / rank)
        q_values[idx] = q
        prev = q
    return q_values


def add_intercept(X: np.ndarray) -> np.ndarray:
    return np.column_stack([np.ones(len(X)), X])


def standardize(X: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    means = np.nanmean(X, axis=0)
    stds = np.nanstd(X, axis=0)
    stds[stds == 0] = 1.0
    X_filled = np.where(np.isfinite(X), X, means)
    return (X_filled - means) / stds, means, stds


def ridge_fit(X: np.ndarray, y: np.ndarray, alpha: float) -> np.ndarray:
    design = add_intercept(X)
    penalty = np.eye(design.shape[1]) * alpha
    penalty[0, 0] = 0.0
    return np.linalg.pinv(design.T @ design + penalty) @ design.T @ y


def kfold_indices(n: int, k: int, seed: int) -> list[tuple[np.ndarray, np.ndarray]]:
    rng = random.Random(seed)
    indices = list(range(n))
    rng.shuffle(indices)
    folds = [indices[i::k] for i in range(k)]
    out = []
    for fold in folds:
        test = np.array(sorted(fold), dtype=int)
        train = np.array([idx for idx in indices if idx not in set(fold)], dtype=int)
        out.append((train, test))
    return out


def stratified_clean_folds(clean_y: np.ndarray) -> list[tuple[np.ndarray, np.ndarray]]:
    clean_idx = [int(idx) for idx, value in enumerate(clean_y) if value == 1]
    over_idx = [int(idx) for idx, value in enumerate(clean_y) if value == 0]
    folds = [[] for _ in clean_idx]
    for fold, idx in zip(folds, clean_idx):
        fold.append(idx)
    for pos, idx in enumerate(over_idx):
        folds[pos % len(folds)].append(idx)
    out = []
    all_idx = set(range(len(clean_y)))
    for fold in folds:
        test = np.array(sorted(fold), dtype=int)
        train = np.array(sorted(all_idx - set(fold)), dtype=int)
        out.append((train, test))
    return out


def model_metric_row(model: str, target: str, alpha: Any, y: np.ndarray, pred: np.ndarray) -> dict[str, Any]:
    residual = y - pred
    mae = float(np.mean(np.abs(residual)))
    rmse = float(np.sqrt(np.mean(residual**2)))
    medae = float(np.median(np.abs(residual)))
    denom = float(np.sum((y - y.mean()) ** 2))
    r2 = 1.0 - float(np.sum(residual**2)) / denom if denom else math.nan
    return {
        "task": "regression",
        "model": model,
        "target": target,
        "features": ";".join(MODEL_FEATURES),
        "alpha": alpha,
        "mae": mae,
        "rmse": rmse,
        "median_absolute_error": medae,
        "r2": r2,
        "balanced_accuracy": "",
        "precision": "",
        "recall": "",
        "specificity": "",
        "f1": "",
        "pr_auc": "",
        "notes": "5-fold cross-validation; exploratory single-run data",
    }


def sigmoid(z: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(z, -40, 40)))


def logistic_fit(X: np.ndarray, y: np.ndarray, alpha: float) -> np.ndarray:
    design = add_intercept(X)
    beta = np.zeros(design.shape[1], dtype=float)
    positive_weight = len(y) / max(1, 2 * int(y.sum()))
    negative_weight = len(y) / max(1, 2 * int((1 - y).sum()))
    weights = np.where(y == 1, positive_weight, negative_weight)
    lr = 0.05
    for _ in range(2000):
        p = sigmoid(design @ beta)
        grad = design.T @ ((p - y) * weights) / len(y)
        grad[1:] += alpha * beta[1:]
        beta -= lr * grad
    return beta


def classification_metric_row(model: str, alpha: Any, y: np.ndarray, pred: np.ndarray, prob: np.ndarray) -> dict[str, Any]:
    tp = int(((y == 1) & (pred == 1)).sum())
    tn = int(((y == 0) & (pred == 0)).sum())
    fp = int(((y == 0) & (pred == 1)).sum())
    fn = int(((y == 1) & (pred == 0)).sum())
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    specificity = tn / (tn + fp) if (tn + fp) else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {
        "task": "classification",
        "model": model,
        "target": "clean_vs_overdriven",
        "features": ";".join(MODEL_FEATURES),
        "alpha": alpha,
        "mae": "",
        "rmse": "",
        "median_absolute_error": "",
        "r2": "",
        "balanced_accuracy": (recall + specificity) / 2.0,
        "precision": precision,
        "recall": recall,
        "specificity": specificity,
        "f1": f1,
        "pr_auc": average_precision(y, prob),
        "confusion_matrix": f"tp={tp};fp={fp};tn={tn};fn={fn}",
        "notes": "leave-one-clean-case-style stratified folds; positive class is clean",
    }


def average_precision(y: np.ndarray, prob: np.ndarray) -> float:
    order = np.argsort(-prob)
    positives = int(y.sum())
    if positives == 0:
        return math.nan
    hit = 0
    total = 0.0
    for rank, idx in enumerate(order, start=1):
        if y[idx] == 1:
            hit += 1
            total += hit / rank
    return total / positives


@dataclass
class TreeNode:
    prediction: float
    feature: int | None = None
    threshold: float | None = None
    left: "TreeNode | None" = None
    right: "TreeNode | None" = None


class RandomForestLite:
    def __init__(
        self,
        task: str,
        n_trees: int,
        max_depth: int,
        min_leaf: int,
        seed: int,
    ) -> None:
        self.task = task
        self.n_trees = n_trees
        self.max_depth = max_depth
        self.min_leaf = min_leaf
        self.seed = seed
        self.trees: list[TreeNode] = []

    def fit(self, X: np.ndarray, y: np.ndarray) -> None:
        rng = random.Random(self.seed)
        self.trees = []
        n = len(X)
        for _ in range(self.n_trees):
            sample_idx = np.array([rng.randrange(n) for _ in range(n)], dtype=int)
            self.trees.append(self._fit_tree(X[sample_idx], y[sample_idx], 0, rng))

    def predict(self, X: np.ndarray) -> np.ndarray:
        pred = np.array([self._predict_tree(tree, X) for tree in self.trees])
        return pred.mean(axis=0)

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        return np.clip(self.predict(X), 0.0, 1.0)

    def _fit_tree(self, X: np.ndarray, y: np.ndarray, depth: int, rng: random.Random) -> TreeNode:
        prediction = float(y.mean()) if len(y) else 0.0
        if depth >= self.max_depth or len(y) < self.min_leaf * 2 or len(set(np.round(y, 12))) <= 1:
            return TreeNode(prediction=prediction)
        feature_count = max(1, int(math.sqrt(X.shape[1])))
        features = rng.sample(range(X.shape[1]), feature_count)
        best = None
        for feature in features:
            values = X[:, feature]
            thresholds = np.unique(np.percentile(values, [20, 35, 50, 65, 80]))
            for threshold in thresholds:
                left_mask = values <= threshold
                right_mask = ~left_mask
                if left_mask.sum() < self.min_leaf or right_mask.sum() < self.min_leaf:
                    continue
                score = self._split_score(y[left_mask], y[right_mask])
                if best is None or score < best[0]:
                    best = (score, feature, float(threshold), left_mask, right_mask)
        if best is None:
            return TreeNode(prediction=prediction)
        _, feature, threshold, left_mask, right_mask = best
        return TreeNode(
            prediction=prediction,
            feature=feature,
            threshold=threshold,
            left=self._fit_tree(X[left_mask], y[left_mask], depth + 1, rng),
            right=self._fit_tree(X[right_mask], y[right_mask], depth + 1, rng),
        )

    def _split_score(self, left_y: np.ndarray, right_y: np.ndarray) -> float:
        if self.task == "regression":
            return float(((left_y - left_y.mean()) ** 2).sum() + ((right_y - right_y.mean()) ** 2).sum())
        return len(left_y) * gini(left_y) + len(right_y) * gini(right_y)

    def _predict_tree(self, tree: TreeNode, X: np.ndarray) -> np.ndarray:
        return np.array([self._predict_one(tree, row) for row in X], dtype=float)

    def _predict_one(self, node: TreeNode, row: np.ndarray) -> float:
        while node.feature is not None and node.left is not None and node.right is not None:
            node = node.left if row[node.feature] <= float(node.threshold) else node.right
        return node.prediction


def gini(y: np.ndarray) -> float:
    if len(y) == 0:
        return 0.0
    p = float(y.mean())
    return 1.0 - p**2 - (1.0 - p) ** 2


def permutation_importance(
    model: RandomForestLite,
    X: np.ndarray,
    y: np.ndarray,
    feature_names: list[str],
    task: str,
    seed: int,
) -> list[dict[str, Any]]:
    rng = np.random.default_rng(seed)
    if task == "regression":
        base_pred = model.predict(X)
        base_error = float(np.mean(np.abs(y - base_pred)))
    else:
        base_pred = (model.predict_proba(X) >= 0.5).astype(int)
        base_error = 1.0 - balanced_accuracy(y, base_pred)
    rows = []
    for idx, name in enumerate(feature_names):
        increases = []
        for _ in range(20):
            X_perm = X.copy()
            X_perm[:, idx] = rng.permutation(X_perm[:, idx])
            if task == "regression":
                pred = model.predict(X_perm)
                error = float(np.mean(np.abs(y - pred)))
            else:
                pred = (model.predict_proba(X_perm) >= 0.5).astype(int)
                error = 1.0 - balanced_accuracy(y, pred)
            increases.append(error - base_error)
        rows.append(
            {
                "feature": name,
                "importance_mean": float(np.mean(increases)),
                "importance_std": float(np.std(increases, ddof=1)),
            }
        )
    rows.sort(key=lambda row: row["importance_mean"], reverse=True)
    return rows


def balanced_accuracy(y: np.ndarray, pred: np.ndarray) -> float:
    tp = int(((y == 1) & (pred == 1)).sum())
    tn = int(((y == 0) & (pred == 0)).sum())
    fp = int(((y == 0) & (pred == 1)).sum())
    fn = int(((y == 1) & (pred == 0)).sum())
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    specificity = tn / (tn + fp) if (tn + fp) else 0.0
    return (recall + specificity) / 2.0


def dominates(a: dict[str, Any], b: dict[str, Any], objectives: list[tuple[str, str]]) -> bool:
    better_or_equal = True
    strictly_better = False
    for field, direction in objectives:
        av = fnum(a.get(field))
        bv = fnum(b.get(field))
        if not math.isfinite(av) or not math.isfinite(bv):
            return False
        if direction == "max":
            if av < bv:
                better_or_equal = False
            if av > bv:
                strictly_better = True
        else:
            if av > bv:
                better_or_equal = False
            if av < bv:
                strictly_better = True
    return better_or_equal and strictly_better


def save_plot(filename: str) -> None:
    plt.tight_layout()
    path = PLOTS_DIR / filename
    plt.savefig(path)
    if path.suffix.lower() == ".pdf":
        plt.savefig(path.with_suffix(".png"), dpi=160)
    plt.close()


def scatter_plot(
    x: np.ndarray,
    y: np.ndarray,
    colors: list[str],
    xlabel: str,
    ylabel: str,
    title: str,
    filename: str,
    rows: list[dict[str, Any]],
    annotate_top: bool = False,
) -> None:
    plt.figure(figsize=(6.7, 4.8))
    plt.scatter(x, y, c=colors, s=28, alpha=0.85)
    if annotate_top:
        annotate_configs(rows, "balanced_app_MBps", count=6)
    else:
        annotate_configs(rows, "balanced_app_MBps", count=3)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    save_plot(filename)


def annotate_configs(rows: list[dict[str, Any]], field: str, count: int) -> None:
    for row in sorted(rows, key=lambda item: fnum(item.get(field)), reverse=True)[:count]:
        x = plt.gca().collections[-1].get_offsets()[rows.index(row), 0]
        y = plt.gca().collections[-1].get_offsets()[rows.index(row), 1]
        plt.annotate(row["config_id"], (x, y), fontsize=8, xytext=(4, 4), textcoords="offset points")


def make_parameter_plot(rows: list[dict[str, Any]], field: str, filename: str) -> None:
    groups: dict[Any, list[float]] = defaultdict(list)
    for row in rows:
        groups[row[field]].append(fnum(row["balanced_app_MBps"]))
    keys = sorted(groups, key=lambda key: fnum(key))
    means = [float(np.mean(groups[key])) for key in keys]
    p25 = [float(np.percentile(groups[key], 25)) for key in keys]
    p75 = [float(np.percentile(groups[key], 75)) for key in keys]
    x = np.arange(len(keys))
    plt.figure(figsize=(7, 4))
    lower = np.maximum(0.0, np.array(means) - np.array(p25))
    upper = np.maximum(0.0, np.array(p75) - np.array(means))
    plt.errorbar(x, means, yerr=[lower, upper], fmt="o-", capsize=4)
    plt.xticks(x, [str(key) for key in keys], rotation=30, ha="right")
    plt.ylabel("Balanced application throughput (MiB/s)")
    plt.xlabel(field)
    plt.title(f"Parameter summary: {field}")
    save_plot(filename)


def make_controlled_plot(rows: list[dict[str, Any]], field: str, filename: str) -> None:
    other = [name for name in CONFIG_FIELDS if name != field]
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[tuple(row.get(name) for name in other)].append(row)
    plt.figure(figsize=(7, 4))
    plotted = 0
    for group_rows in groups.values():
        if len({row[field] for row in group_rows}) < 2:
            continue
        group_rows = sorted(group_rows, key=lambda row: fnum(row[field]))
        plt.plot(
            [fnum(row[field]) for row in group_rows],
            [fnum(row["balanced_app_MBps"]) for row in group_rows],
            marker="o",
            linewidth=1,
            alpha=0.7,
        )
        plotted += 1
        if plotted >= 12:
            break
    plt.xscale("log", base=2)
    plt.xlabel(field)
    plt.ylabel("Balanced application throughput (MiB/s)")
    plt.title(f"Controlled one-factor lines: {field}")
    save_plot(filename)


def make_correlation_heatmap(correlation_rows: list[dict[str, Any]]) -> None:
    targets = sorted({row["target"] for row in correlation_rows})
    features = MODEL_FEATURES
    matrix = np.zeros((len(features), len(targets)))
    lookup = {(row["feature"], row["target"]): fnum(row["spearman_rho"]) for row in correlation_rows}
    for i, feature in enumerate(features):
        for j, target in enumerate(targets):
            matrix[i, j] = lookup.get((feature, target), math.nan)
    plt.figure(figsize=(11, 6))
    plt.imshow(matrix, cmap="coolwarm", vmin=-1, vmax=1, aspect="auto")
    plt.colorbar(label="Spearman rho")
    plt.xticks(range(len(targets)), targets, rotation=45, ha="right", fontsize=8)
    plt.yticks(range(len(features)), features, fontsize=8)
    plt.title("Spearman Correlations")
    save_plot("spearman_correlation_heatmap.pdf")


def make_feature_importance_plot(items: list[dict[str, Any]]) -> None:
    top = items[:10]
    plt.figure(figsize=(7, 4))
    y = np.arange(len(top))
    plt.barh(y, [fnum(item["importance_mean"]) for item in top], color="#4c78a8")
    plt.yticks(y, [item["feature"] for item in top], fontsize=8)
    plt.gca().invert_yaxis()
    plt.xlabel("Permutation MAE increase")
    plt.title("Feature Importance: Balanced Throughput")
    save_plot("feature_importance_balanced.pdf")


def best_model_rows(model_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[tuple[str, str], dict[str, Any]] = {}
    for row in model_rows:
        key = (row["task"], row["target"])
        if row["task"] == "regression":
            metric = fnum(row["rmse"], default=math.inf)
            current = fnum(best.get(key, {}).get("rmse"), default=math.inf)
            if metric < current:
                best[key] = row
        else:
            metric = fnum(row["balanced_accuracy"])
            current = fnum(best.get(key, {}).get("balanced_accuracy"), default=-math.inf)
            if metric > current:
                best[key] = row
    return list(best.values())


def config_sort_key(value: str) -> tuple[str, int]:
    prefix, _, suffix = value.partition("_")
    try:
        return prefix, int(suffix)
    except ValueError:
        return value, 0


def cfg_suffix(row: dict[str, Any]) -> str:
    return str(row["config_id"]).split("_")[-1]


def tex_escape(text: Any) -> str:
    value = str(text)
    replacements = {
        "\\": r"\textbackslash{}",
        "_": r"\_",
        "%": r"\%",
        "&": r"\&",
        "#": r"\#",
        "$": r"\$",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    return "".join(replacements.get(char, char) for char in value)


def tex_path(text: Any) -> str:
    value = str(text)
    return r"\path{" + value.replace("}", r"\}") + "}"


def tex_breakable_mono(text: Any, chunk: int = 16) -> str:
    value = tex_escape(text)
    parts = [value[idx : idx + chunk] for idx in range(0, len(value), chunk)]
    return r"\texttt{" + r"\allowbreak{}".join(parts) + "}"


def cfg_suffix_from_id(config_id: str) -> str:
    return str(config_id).split("_")[-1]


def first_shortlist_category(rows: list[dict[str, Any]], category: str) -> dict[str, Any]:
    return next((row for row in rows if category in row.get("selection_categories", "")), {})


def latex_diagram_figure(
    rel_path: str,
    caption: str,
    label: str,
    *,
    placement: str = "!htbp",
    width: str = r"0.96\linewidth",
) -> str:
    path = REPORT_ROOT / rel_path
    if not path.is_file():
        return (
            rf"\begin{{center}}\textit{{Diagram file missing: "
            rf"\texttt{{{tex_escape(rel_path)}}}}}\end{{center}}"
        )
    return "\n".join(
        [
            rf"\begin{{figure}}[{placement}]",
            r"\centering",
            rf"\includegraphics[width={width}]{{{rel_path}}}",
            rf"\caption{{{tex_escape(caption)}}}",
            rf"\label{{{label}}}",
            r"\end{figure}",
        ]
    )


def latex_validation_executive_summary(
    best_validated: dict[str, Any],
    fully_validated: list[dict[str, Any]],
    top_balanced: dict[str, Any],
    qualified_rows: list[dict[str, Any]],
) -> str:
    if best_validated:
        full_text = ", ".join(
            rf"\cfg{{{cfg_suffix_from_id(validation_config_id(row))}}} ({qualified_fraction(row)})"
            for row in fully_validated
        )
        if not full_text:
            full_text = "none"
        return (
            rf"The repeated validation run makes \cfg{{{cfg_suffix_from_id(validation_config_id(best_validated))}}} "
            rf"the primary validated recommendation: it qualified in {qualified_fraction(best_validated)} repeats "
            rf"with median balanced throughput {fmt(best_validated.get('median_balanced_app_MBps'))}\mbps.  "
            rf"Fully qualified repeated configurations are {full_text}.  "
            rf"The highest raw single-run upper bound remains \cfg{{{cfg_suffix(top_balanced)}}} at "
            rf"{fmt(top_balanced['balanced_app_MBps'])}\mbps, but it is overdriven and is retained only as overload evidence."
        )
    return (
        rf"The highest qualified single observation is \cfg{{{cfg_suffix(qualified_rows[0])}}} "
        rf"at {fmt(qualified_rows[0]['balanced_app_MBps'])}\mbps.  The highest raw observed upper "
        rf"bound is \cfg{{{cfg_suffix(top_balanced)}}} at {fmt(top_balanced['balanced_app_MBps'])}\mbps, "
        rf"but it is overdriven and is retained only as overload evidence.  All findings require repeated "
        rf"validation before sustainability claims."
    )


def latex_validation_results_section(
    validation_summary_rows: list[dict[str, Any]],
    validation_repeat_rows: list[dict[str, Any]],
    shortlist_rows: list[dict[str, Any]],
) -> str:
    if validation_summary_rows:
        job = INPUT_CONTEXT.get("validation_job_id") or "unknown"
        run_id = INPUT_CONTEXT.get("validation_run_id") or "unknown"
        nodes = INPUT_CONTEXT.get("validation_nodes", "not recorded")
        elapsed = INPUT_CONTEXT.get("validation_elapsed", "not recorded")
        return "\n".join(
            [
                r"\section{Repeated Validation Results}",
                (
                    rf"The validation shortlist has now been executed as Slurm job \texttt{{{tex_escape(job)}}} "
                    rf"(run identifier {tex_path(run_id)}).  It produced {len(validation_repeat_rows)} case rows "
                    rf"from {len(validation_summary_rows)} configurations, using five randomized blocks.  "
                    rf"The recorded allocation used nodes \texttt{{{tex_escape(nodes)}}}; elapsed time was "
                    rf"\texttt{{{tex_escape(elapsed)}}}.  "
                    r"Figure~\ref{fig:repeated-validation-results} summarizes the repeated-validation outcome; "
                    r"Table~\ref{tab:validation-results} gives the exact numeric summary."
                ),
                "",
                latex_diagram_figure(
                    "analysis_diagrams/repeated_validation_results.png",
                    "Repeated-validation results for the selected configurations, ranked by qualified-repeat count and median balanced throughput, with the final sustained recommendation and overload reference summarized from the five-run campaign.",
                    "fig:repeated-validation-results",
                    placement="p",
                ),
                "",
                latex_validation_results_table(validation_summary_rows),
                "",
                (
                    r"The high-throughput repeated configurations remain useful overload references, but "
                    r"they are not sustained-throughput recommendations unless their qualified-repeat count "
                    r"matches their repeat count.  Median record throughput in Table~\ref{tab:validation-results} "
                    r"is the median of the five record-rate values calculated separately for the five repeats; "
                    r"it is not inferred by converting an already aggregated MiB/s median."
                ),
                "",
                r"\subsection{Validation Shortlist Provenance}",
                (
                    r"Table~\ref{tab:validation-shortlist} records why each configuration entered the validation manifest.  "
                    r"The resulting recommendation is based on Table~\ref{tab:validation-results}."
                ),
                "",
                latex_shortlist_table(
                    shortlist_rows,
                    caption="Configurations selected for the repeated validation run.",
                    label="tab:validation-shortlist",
                ),
            ]
        )
    return "\n".join(
        [
            r"\section{Validation Shortlist}",
            r"Table~\ref{tab:validation-shortlist} is selected from the current data for a future five-repetition experiment.  It includes the top qualified rows, transition-region rows, objective overload references, the raw upper bound, the manifest baseline, and one controlled one-factor comparison.  None of these single observations proves repeatable performance.",
            "",
            latex_shortlist_table(shortlist_rows, label="tab:validation-shortlist"),
        ]
    )


def latex_validation_methodology_section(
    validation_summary_rows: list[dict[str, Any]],
    repetition_rows: list[dict[str, Any]],
) -> str:
    if validation_summary_rows:
        return "\n".join(
            [
                r"\section{Repeated Validation Methodology}",
                (
                    rf"The validation manifest used five randomized blocks with fixed seed {VALIDATION_SEED}.  "
                    "Each selected configuration appeared exactly once per block.  Repeated results are ranked "
                    "by qualified-run frequency first, then median balanced throughput, with variability, backlog, "
                    "flush time, and failed sends as secondary criteria.  "
                    r"Figure~\ref{fig:validation-methodology} summarizes this block design and decision rule, "
                    r"and Table~\ref{tab:validation-design} gives the exact configurations and selection reasons."
                ),
                "",
                latex_diagram_figure(
                    "analysis_diagrams/validation_methodology.png",
                    "Repeated-validation methodology: the ten shortlisted configurations are executed once in each of five randomized blocks, then ranked by qualified-run frequency, median balanced throughput, variability, backlog, flush time, and failed sends.",
                    "fig:validation-methodology",
                ),
                "",
                (
                    r"The repeated-validation campaign contains 10 configurations and 5 repetitions per configuration, "
                    r"for 50 total case rows.  The manifest is divided into five randomized blocks; within each block, "
                    r"every selected configuration appears exactly once.  A repeat is classified as qualified only when "
                    r"its measured backlog is at most 5\%, flush time is at most 10 seconds, and failed sends are at most "
                    r"0.1\%.  Final ranking is determined first by the number of qualified repeats, then by median balanced "
                    r"throughput, with lower variability, lower backlog, lower flush time, lower failed-send percentage, "
                    r"and configuration ID used only as secondary tie-breakers."
                ),
                "",
                latex_repetition_table(repetition_rows),
            ]
        )
    return "\n".join(
        [
            r"\section{Future Repetition Methodology}",
                (
                    rf"The generated validation manifest uses five randomized blocks with fixed seed {VALIDATION_SEED}.  "
                    "Each shortlisted configuration appears once per block.  After repetitions, rank configurations by "
                    "qualified-run frequency first, then median balanced throughput, with variability, backlog, flush time, "
                    "and failed sends as secondary criteria.  "
                    r"Figure~\ref{fig:validation-methodology} summarizes this block design and decision rule, "
                    r"and Table~\ref{tab:validation-design} gives the planned configuration set."
                ),
                "",
                latex_diagram_figure(
                    "analysis_diagrams/validation_methodology.png",
                    "Planned repeated-validation methodology: the ten shortlisted configurations are executed once in each of five randomized blocks, then evaluated with a constraint-first decision rule before making sustainable-throughput claims.",
                    "fig:validation-methodology",
                ),
                "",
                (
                    r"The planned campaign contains 10 configurations and 5 repetitions per configuration, "
                    r"arranged as five randomized blocks.  A future repeat should be classified as qualified only when "
                    r"backlog is at most 5\%, flush time is at most 10 seconds, and failed sends are at most 0.1\%.  "
                    r"The final ranking should use qualified-run frequency first and median balanced throughput second."
                ),
                "",
                latex_repetition_table(repetition_rows),
            ]
        )


def latex_conclusion_text(
    validation_summary_rows: list[dict[str, Any]],
    qualified_rows: list[dict[str, Any]],
    top_balanced: dict[str, Any],
) -> str:
    if validation_summary_rows:
        best = validation_summary_rows[0]
        fully = fully_validated_rows(validation_summary_rows)
        runner_up = fully[1] if len(fully) > 1 else {}
        runner_text = (
            rf"  \cfg{{{cfg_suffix_from_id(validation_config_id(runner_up))}}} is the stable secondary option "
            rf"with {qualified_fraction(runner_up)} repeats and median {fmt(runner_up.get('median_balanced_app_MBps'))}\mbps."
            if runner_up
            else ""
        )
        return (
            rf"The defensible validated claim is that \cfg{{{cfg_suffix_from_id(validation_config_id(best))}}} "
            rf"is the best sustained configuration observed so far: {qualified_fraction(best)} qualified repeats, "
            rf"median balanced throughput {fmt(best.get('median_balanced_app_MBps'))}\mbps, median backlog "
            rf"{fmt(best.get('median_pending_backlog_percent'))}\%, and median flush "
            rf"{fmt(best.get('median_flush_sec'))} seconds.{runner_text}  "
            rf"The {fmt(top_balanced['balanced_app_MBps'])}\mbps single-run result from "
            rf"\cfg{{{cfg_suffix(top_balanced)}}} remains an observed overdriven upper bound, not a "
            rf"sustainable-throughput recommendation."
        )
    return (
        rf"The defensible current claim is a highest qualified single observation of "
        rf"{fmt(qualified_rows[0]['balanced_app_MBps'])}\mbps from \cfg{{{cfg_suffix(qualified_rows[0])}}}.  "
        rf"The {fmt(top_balanced['balanced_app_MBps'])}\mbps result from \cfg{{{cfg_suffix(top_balanced)}}} "
        rf"is an observed overdriven upper bound.  Repeated validation is required before claiming sustainable Kafka performance."
    )


def latex_validation_results_table(rows: list[dict[str, Any]]) -> str:
    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\scriptsize",
        r"\begin{tabular}{rlrrrrrrrr}",
        r"\toprule",
        r"Rank & Config & Qualified & Median & Median records & Mean & Min & Max & Backlog & Flush \\",
        r" & & repeats & (MiB/s) & (records/s) & (MiB/s) & (MiB/s) & (MiB/s) & (\%) & (s) \\",
        r"\midrule",
    ]
    for row in rows:
        lines.append(
            rf"{row.get('validation_rank', '')} & \texttt{{{tex_escape(validation_config_id(row))}}} & "
            rf"{qualified_fraction(row)} & {fmt_mbps(row.get('median_balanced_app_MBps'))} & "
            rf"{fmt_records_latex(row.get('median_balanced_records_per_sec'))} & "
            rf"{fmt_mbps(row.get('mean_balanced_app_MBps'))} & {fmt_mbps(row.get('min_balanced_app_MBps'))} & "
            rf"{fmt_mbps(row.get('max_balanced_app_MBps'))} & {fmt(row.get('median_pending_backlog_percent'))} & "
            rf"{fmt(row.get('median_flush_sec'))} \\"
        )
    lines.extend(
        [
            r"\bottomrule",
            r"\end{tabular}",
            r"\medskip",
            r"\begin{tabular}{lrrrrrrr}",
            r"\toprule",
            r"Config & Payload & Producer & Consumer & Balanced & Producer & Consumer & Balanced \\",
            r" & (bytes) & (MiB/s) & (MiB/s) & (MiB/s) & (records/s) & (records/s) & (records/s) \\",
            r"\midrule",
        ]
    )
    for row in selected_validation_record_rows(rows):
        lines.append(
            rf"\texttt{{{tex_escape(validation_config_id(row))}}} & {row.get('payload_size_bytes', '')} & "
            rf"{fmt_mbps(row.get('median_producer_delivered_MBps'))} & "
            rf"{fmt_mbps(row.get('median_consumer_received_MBps'))} & "
            rf"{fmt_mbps(row.get('median_balanced_app_MBps'))} & "
            rf"{fmt_records_latex(row.get('median_producer_records_per_sec'))} & "
            rf"{fmt_records_latex(row.get('median_consumer_records_per_sec'))} & "
            rf"{fmt_records_latex(row.get('median_balanced_records_per_sec'))} \\"
        )
    lines.extend(
        [
            r"\bottomrule",
            r"\end{tabular}",
            r"\caption{Repeated validation summary by original configuration.  The upper panel ranks all ten configurations without changing the qualification-first rule; record throughput is the median of per-repeat record rates.  The lower panel gives producer, consumer, and balanced payload throughput and record throughput for the principal recommendation, secondary option, prior single-run leader, and overload reference.  Payload throughput uses MiB/s, where 1 MiB is 1,048,576 bytes.}",
            r"\label{tab:validation-results}",
            r"\end{table}",
        ]
    )
    return "\n".join(lines)


def latex_parameter_space_table(rows: list[dict[str, Any]]) -> str:
    labels = {
        "producer_ranks": "Producer ranks",
        "consumer_ranks": "Consumer ranks",
        "partitions": "Partitions",
        "batch_size": "Batch size",
        "linger_ms": "Linger ms",
        "payload_size_bytes": "Payload bytes",
        "producer_queue_messages": "Producer queue messages",
        "producer_queue_kbytes": "Producer queue KiB",
        "consumer_fetch_min_bytes": "Consumer fetch min bytes",
        "consumer_fetch_wait_max_ms": "Consumer fetch wait ms",
        "consumer_fetch_message_max_bytes": "Consumer fetch max bytes",
    }
    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\small",
        r"\begin{tabularx}{\linewidth}{lX}",
        r"\toprule",
        r"Parameter & Tested values \\",
        r"\midrule",
    ]
    for field in CONFIG_FIELDS:
        values = sorted({str(row.get(field, "")) for row in rows if row.get(field, "") != ""}, key=lambda item: fnum(item, default=math.inf))
        lines.append(rf"{tex_escape(labels[field])} & \texttt{{{tex_escape(', '.join(values))}}} \\")
    lines.extend(
        [
            r"\bottomrule",
            r"\end{tabularx}",
            r"\caption{Parameter values tested in the 120-configuration sweep.}",
            r"\label{tab:parameter-space}",
            r"\end{table}",
        ]
    )
    return "\n".join(lines)


def latex_primary_ranking_table(rows: list[dict[str, Any]]) -> str:
    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\scriptsize",
        r"\begin{tabular}{rlrrrrrl}",
        r"\toprule",
        r"Rank & Config & Balanced & Balanced records & Backlog & Flush & Failed & Status \\",
        r" & & (MiB/s) & (records/s) & (\%) & (s) & (\%) & \\",
        r"\midrule",
    ]
    for row in rows:
        lines.append(
            rf"{row['primary_rank']} & \texttt{{{tex_escape(row['config_id'])}}} & {fmt_mbps(row['balanced_app_MBps'])} & {fmt_records_latex(row['balanced_records_per_sec'])} & {fmt(row['pending_backlog_percent'])} & {fmt(row['flush_sec'])} & {fmt(row['failed_send_percent'])} & {tex_escape(row['qualification_status'])} \\"
        )
    lines.extend(
        [
            r"\bottomrule",
            r"\end{tabular}",
            r"\caption{Primary recommendation ranking excerpt with balanced payload and record throughput.  Payload throughput uses MiB/s, where 1 MiB is 1,048,576 bytes; records/s is derived from each row's configured payload size.}",
            r"\label{tab:primary-ranking}",
            r"\end{table}",
        ]
    )
    return "\n".join(lines)


def latex_shortlist_table(
    rows: list[dict[str, Any]],
    caption: str = "Selected configurations for future repeated validation.",
    label: str = "tab:validation-shortlist",
) -> str:
    lines = [
        r"{\scriptsize",
        r"\begin{longtable}{p{1.4cm}p{3.2cm}rrrrp{4.3cm}}",
        rf"\caption{{{tex_escape(caption)}}}\label{{{label}}}\\",
        r"\toprule",
        r"Config & Selection categories & Balanced & Backlog & Flush & Failed & Reason \\",
        r" & & (MiB/s) & (\%) & (s) & (\%) & \\",
        r"\midrule",
        r"\endfirsthead",
        r"\toprule",
        r"Config & Selection categories & Balanced & Backlog & Flush & Failed & Reason \\",
        r"\midrule",
        r"\endhead",
    ]
    for row in rows:
        reason = str(row["selection_reason"]).replace("_", " ")
        lines.append(
            rf"\texttt{{{tex_escape(row['config_id'])}}} & {tex_escape(row['selection_categories'])} & {fmt(row['balanced_app_MBps'])} & {fmt(row['pending_backlog_percent'])} & {fmt(row['flush_sec'])} & {fmt(row['failed_send_percent'])} & {tex_escape(reason)} \\"
        )
    lines.extend([r"\bottomrule", r"\end{longtable}", r"}"])
    return "\n".join(lines)


def latex_qualification_sensitivity_table(rows: list[dict[str, Any]]) -> str:
    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\small",
        r"\begin{tabular}{rrrrl}",
        r"\toprule",
        r"Backlog limit & Qualified & Best MiB/s & Best config & Changes vs 5\% \\",
        r"\midrule",
    ]
    for row in rows:
        entering = row.get("entering_vs_official_5_percent") or "-"
        leaving = row.get("leaving_vs_official_5_percent") or "-"
        change = f"+{entering}; -{leaving}"
        lines.append(
            rf"{fmt(row['backlog_threshold_percent'])}\% & {row['qualified_config_count']} & {fmt(row['highest_qualifying_balanced_app_MBps'])} & \texttt{{{tex_escape(row['highest_qualifying_config_id'])}}} & {tex_escape(change)} \\"
        )
    lines.extend(
        [
            r"\bottomrule",
            r"\end{tabular}",
            r"\caption{Qualification sensitivity to backlog threshold only.}",
            r"\label{tab:qualification-sensitivity}",
            r"\end{table}",
        ]
    )
    return "\n".join(lines)


def latex_resource_quality_table(rows: list[dict[str, Any]]) -> str:
    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\small",
        r"\begin{tabularx}{\linewidth}{lrrX}",
        r"\toprule",
        r"Field & Available & Missing & Data-quality note \\",
        r"\midrule",
    ]
    for row in rows:
        lines.append(
            rf"\texttt{{{tex_escape(row['field'])}}} & {row['available_rows']} & {row['missing_rows']} & {tex_escape(row['unit_semantics'])} \\"
        )
    lines.extend(
        [
            r"\bottomrule",
            r"\end{tabularx}",
            r"\caption{Resource metric availability and semantic caveats.}",
            r"\label{tab:resource-quality}",
            r"\end{table}",
        ]
    )
    return "\n".join(lines)


def latex_key_results_table(*rows: dict[str, Any]) -> str:
    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\small",
        r"\begin{tabular}{lrrrrl}",
        r"\toprule",
        r"Config & Balanced & Backlog & Flush & Failed & Verdict \\",
        r" & (MiB/s) & (\%) & (s) & (\%) & \\",
        r"\midrule",
    ]
    for row in rows:
        lines.append(
            rf"\cfg{{{cfg_suffix(row)}}} & {fmt(row['balanced_app_MBps'])} & {fmt(row['pending_backlog_percent'])} & {fmt(row['flush_sec'])} & {fmt(row['failed_send_percent'])} & {tex_escape(row['throughput_verdict'])} \\"
        )
    lines.extend(
        [
            r"\bottomrule",
            r"\end{tabular}",
            r"\caption{Key verified configurations.}",
            r"\label{tab:keyconfigs}",
            r"\end{table}",
        ]
    )
    return "\n".join(lines)


def latex_clean_table(rows: list[dict[str, Any]]) -> str:
    lines = [
        r"\begin{center}",
        r"\small",
        r"\begin{tabular}{lrrrrr}",
        r"\toprule",
        r"Config & Producer & Consumer & Balanced & Backlog & Flush \\",
        r" & (MiB/s) & (MiB/s) & (MiB/s) & (\%) & (s) \\",
        r"\midrule",
    ]
    for row in rows:
        lines.append(
            rf"\cfg{{{cfg_suffix(row)}}} & {fmt(row['producer_delivered_MBps'])} & {fmt(row['consumer_received_MBps'])} & {fmt(row['balanced_app_MBps'])} & {fmt(row['pending_backlog_percent'])} & {fmt(row['flush_sec'])} \\"
        )
    lines.extend([r"\bottomrule", r"\end{tabular}", r"\end{center}"])
    return "\n".join(lines)


def latex_controlled_table(rows: list[dict[str, Any]]) -> str:
    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\scriptsize",
        r"\begin{tabular}{lllrrrr}",
        r"\toprule",
        r"Parameter & Base & Compare & $\Delta$Throughput & $\Delta$Backlog & $\Delta$Flush & $\Delta$Failed \\",
        r"\midrule",
    ]
    for row in rows[:10]:
        lines.append(
            rf"\texttt{{{tex_escape(row['changed_parameter'])}}} & \texttt{{{tex_escape(row['base_config'])}}} & \texttt{{{tex_escape(row['comparison_config'])}}} & {fmt(row['delta_balanced_MBps'])} & {fmt(row['delta_backlog_percent'])} & {fmt(row['delta_flush_sec'])} & {fmt(row['delta_failed_send_percent'])} \\"
        )
    lines.extend(
        [
            r"\bottomrule",
            r"\end{tabular}",
            r"\caption{Largest one-factor controlled comparisons by absolute balanced-throughput change.  Parameter is the only varied configuration field in the pair.  Base and Compare identify the two configurations.  Delta columns are Compare minus Base: throughput in MiB/s, backlog and failed sends in percentage points, and flush time in seconds.}",
            r"\label{tab:controlled-comparisons}",
            r"\end{table}",
        ]
    )
    return "\n".join(lines)


def latex_correlation_table(rows: list[dict[str, Any]]) -> str:
    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\scriptsize",
        r"\begin{tabular}{llrr}",
        r"\toprule",
        r"Feature & Target & Spearman $\rho$ & BH $q$ approx \\",
        r"\midrule",
    ]
    for row in rows[:10]:
        lines.append(
            rf"\texttt{{{tex_escape(row['feature'])}}} & \texttt{{{tex_escape(row['target'])}}} & {fmt(row['spearman_rho'])} & {fmt_probability_latex(row['spearman_q_bh_approx'])} \\"
        )
    lines.extend(
        [
            r"\bottomrule",
            r"\end{tabular}",
            r"\caption{Strongest exploratory Spearman rank correlations.  Feature is an input configuration or derived workload variable, Target is the measured response, Spearman $\rho$ is the monotonic rank-correlation coefficient, and BH $q$ approx is the approximate Benjamini--Hochberg adjusted significance value.}",
            r"\label{tab:spearman-correlations}",
            r"\end{table}",
        ]
    )
    return "\n".join(lines)


def latex_model_table(rows: list[dict[str, Any]], caption: str = "", label: str = "") -> str:
    if not rows:
        return ""
    if not caption:
        caption = "Cross-validated exploratory model summary."
    tasks = {row.get("task") for row in rows}
    if tasks == {"regression"}:
        metric_note = (
            "Model names identify the estimator; Target is the predicted continuous response; "
            "MAE, RMSE, and median absolute error are reported in the target units, and R2 is "
            "the cross-validated coefficient of determination."
        )
    elif tasks == {"classification"}:
        metric_note = (
            "Model names identify the estimator; Target is the predicted class label; balanced "
            "accuracy averages sensitivity and specificity, precision is the fraction of predicted "
            "qualified rows that are actually qualified, recall is the fraction of actual qualified "
            "rows found by the model, specificity is the fraction of overdriven rows correctly "
            "identified, F1 is the harmonic mean of precision and recall, and PR-AUC is the "
            "area under the precision--recall curve."
        )
    else:
        metric_note = (
            "Model names identify the estimator; Target is the predicted response; metrics are "
            "reported according to whether the row is a regression or classification task."
        )
    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\scriptsize",
        r"\begin{tabularx}{\textwidth}{llX}",
        r"\toprule",
        r"Model & Target & Cross-validation metrics \\",
        r"\midrule",
    ]
    for row in rows:
        if row["task"] == "regression":
            metrics = (
                f"MAE {fmt(row['mae'])}, RMSE {fmt(row['rmse'])}, "
                f"median AE {fmt(row['median_absolute_error'])}, R2 {fmt(row['r2'])}"
            )
        else:
            metrics = (
                f"balanced accuracy {fmt(row['balanced_accuracy'])}, precision {fmt(row['precision'])}, "
                f"recall {fmt(row['recall'])}, specificity {fmt(row['specificity'])}, F1 {fmt(row['f1'])}, "
                f"PR-AUC {fmt(row['pr_auc'])}"
            )
        lines.append(
            rf"{tex_escape(row['model'])} & \texttt{{{tex_escape(row['target'])}}} & {tex_escape(metrics)} \\"
        )
    lines.extend(
        [
            r"\bottomrule",
            r"\end{tabularx}",
            rf"\caption{{{tex_escape(caption)}  {tex_escape(metric_note)}}}",
        ]
    )
    if label:
        lines.append(rf"\label{{{label}}}")
    lines.append(r"\end{table}")
    return "\n".join(lines)


def latex_pareto_table(rows: list[dict[str, Any]]) -> str:
    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\scriptsize",
        r"\begin{tabular}{lrrrrl}",
        r"\toprule",
        r"Config & Balanced & Backlog & Flush & Failed & Verdict \\",
        r"\midrule",
    ]
    for row in rows[:10]:
        lines.append(
            rf"\texttt{{{tex_escape(row['config_id'])}}} & {fmt(row['balanced_app_MBps'])} & {fmt(row['pending_backlog_percent'])} & {fmt(row['flush_sec'])} & {fmt(row['failed_send_percent'])} & {tex_escape(row['throughput_verdict'])} \\"
        )
    lines.extend(
        [
            r"\bottomrule",
            r"\end{tabular}",
            r"\caption{Highest-throughput excerpt of the Pareto set.  Config is the configuration identifier; Balanced is balanced throughput in MiB/s and is maximized; Backlog, Flush, and Failed are minimized operational-risk objectives; Verdict is the qualification status from the benchmark rules.}",
            r"\label{tab:pareto-excerpt}",
            r"\end{table}",
        ]
    )
    return "\n".join(lines)


def latex_repetition_table(rows: list[dict[str, Any]]) -> str:
    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\scriptsize",
        r"\begin{tabularx}{\textwidth}{lrrX}",
        r"\toprule",
        r"Config & Balanced & Reps & Reason \\",
        r"\midrule",
    ]
    for row in rows:
        lines.append(
            rf"\texttt{{{tex_escape(row['config_id'])}}} & {fmt(row['balanced_app_MBps'])} & {row['recommended_repetitions']} & {tex_escape(row['reason'])} \\"
        )
    lines.extend(
        [
            r"\bottomrule",
            r"\end{tabularx}",
            r"\caption{Repeated-validation design by configuration.  Config is the original sweep configuration identifier, Balanced is the single-run balanced throughput used for shortlist context in MiB/s, Reps is the number of repeated validation runs assigned to the configuration, and Reason records why the configuration was included in the validation manifest.}",
            r"\label{tab:validation-design}",
            r"\end{table}",
        ]
    )
    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
