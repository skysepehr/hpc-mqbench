#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
import re
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = PROJECT_ROOT / "results" / "published" / "pulsar" / "pilots"

TEXT_FILES = (
    "final_report.md",
    "final_report.html",
    "reports/final_report.md",
    "reports/final_report.html",
    "reports/monitoring_summary.md",
)
JSON_FILES = (
    "benchmark_result.json",
    "case_config_snapshot.json",
    "final_report.json",
    "data/backend_health.json",
    "data/backend_snapshot.json",
    "data/case_config_snapshot.json",
    "data/final_report.json",
    "data/monitoring_snapshot.json",
    "data/pulsar_profile_snapshot.json",
    "data/pulsar_runtime_manifest.json",
    "data/system_inventory.json",
    "data/topic_metadata.json",
    "monitoring/monitoring_summary.json",
)
PLAIN_FILES = (
    "data/pulsar_profile.sha256",
    "data/standalone.conf.sha256",
)
SUMMARY_FIELDS = (
    "pilot_id",
    "case_id",
    "profile_id",
    "jvm_memory",
    "case_status",
    "backend_health",
    "eligible",
    "qualified",
    "producer_delivered_records",
    "consumer_received_measurement_records",
    "consumer_late_drained_records",
    "missing_after_drain_records",
    "producer_mib_per_sec",
    "consumer_mib_per_sec",
    "balanced_mib_per_sec",
    "max_flush_duration_sec",
    "latency_valid",
    "latency_sample_count",
    "latency_delivered_sample_count",
    "failure_reasons",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish compact, sanitized Pulsar P0/P1 pilot evidence."
    )
    parser.add_argument("--p0-source", required=True)
    parser.add_argument("--p1-source", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = [
        _publish_case("p0", Path(args.p0_source), output_dir / "p0"),
        _publish_case("p1", Path(args.p1_source), output_dir / "p1"),
    ]
    _write_summary(output_dir, rows)
    _write_json(
        output_dir / "campaign_status.json",
        {
            "format": "messaging-benchmark.pulsar-pilot-status.v1",
            "backend_id": "pulsar",
            "campaign": "historical-integration-pilots",
            "case_count": len(rows),
            "eligible_case_count": sum(_bool(row["eligible"]) for row in rows),
            "qualified_case_count": sum(_bool(row["qualified"]) for row in rows),
            "status": "historical_failed_or_ineligible_evidence",
            "phase1_evidence": False,
            "conclusion": (
                "P0 exposed an 8-GiB direct-memory failure. P1 retained a "
                "healthy service with 32 GiB of direct memory but was ineligible "
                "because producer flush and consumer drain were incomplete."
            ),
        },
    )
    _write_json(
        output_dir / "provenance.json",
        {
            "format": "messaging-benchmark.provenance.v1",
            "backend_id": "pulsar",
            "source_case_ids": [row["case_id"] for row in rows],
            "source_kind": "retained GWDG HPC case reports",
            "contents": (
                "Compact reports, configuration/profile/runtime snapshots, "
                "monitoring summaries, and process samples; raw Prometheus "
                "storage and service logs are intentionally excluded."
            ),
            "path_sanitization": True,
            "raw_results_modified": False,
        },
    )
    _write_manifests(output_dir)
    _reject_private_paths(output_dir)
    print(f"[pulsar-publish] cases: {len(rows)}")
    print(f"[pulsar-publish] output: {_relative(output_dir)}")
    return 0


def _publish_case(
    pilot_id: str,
    source_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    if not source_dir.is_dir():
        raise FileNotFoundError(source_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for relative_name in JSON_FILES:
        source = source_dir / relative_name
        if not source.is_file():
            continue
        payload = json.loads(source.read_text(encoding="utf-8"))
        destination = output_dir / relative_name
        _write_json(destination, _sanitize_value(payload))

    for relative_name in TEXT_FILES:
        source = source_dir / relative_name
        if not source.is_file():
            continue
        destination = output_dir / relative_name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            _sanitize_text(source.read_text(encoding="utf-8")),
            encoding="utf-8",
        )

    for relative_name in PLAIN_FILES:
        source = source_dir / relative_name
        if not source.is_file():
            continue
        destination = output_dir / relative_name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            _sanitize_text(source.read_text(encoding="utf-8")),
            encoding="utf-8",
        )

    process_files = sorted((source_dir / "monitoring").glob("pulsar_process_*.csv"))
    if process_files:
        destination = output_dir / "monitoring" / "pulsar_process.csv"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            _sanitize_text(process_files[0].read_text(encoding="utf-8")),
            encoding="utf-8",
        )

    report = json.loads(
        (source_dir / "final_report.json").read_text(encoding="utf-8")
    )
    return _summary_row(pilot_id, report)


def _summary_row(pilot_id: str, report: dict[str, Any]) -> dict[str, Any]:
    case = _dict(report.get("case"))
    config = _dict(report.get("config"))
    settings = _dict(config.get("backend_settings"))
    runtime = _dict(settings.get("runtime"))
    common = _dict(report.get("common_metrics"))
    records = _dict(common.get("records"))
    throughput = _dict(common.get("throughput"))
    qualification = _dict(common.get("qualification"))
    latency = _dict(report.get("latency_validation"))
    producers = _dict(_dict(report.get("aggregated_metrics")).get("producers"))
    eligibility = _dict(report.get("eligibility"))
    health = _dict(report.get("backend_health"))
    reasons = eligibility.get("failure_reasons", [])
    if not isinstance(reasons, list):
        reasons = [str(reasons)]
    return {
        "pilot_id": pilot_id,
        "case_id": case.get("case_id", "unknown"),
        "profile_id": settings.get("profile_id", "unknown"),
        "jvm_memory": runtime.get("jvm_memory", "unknown"),
        "case_status": case.get("status", "unknown"),
        "backend_health": health.get("status", "not_recorded"),
        "eligible": bool(qualification.get("eligible", False)),
        "qualified": bool(qualification.get("qualified", False)),
        "producer_delivered_records": int(records.get("published", 0) or 0),
        "consumer_received_measurement_records": int(
            records.get("consumed", 0) or 0
        ),
        "consumer_late_drained_records": int(
            records.get("late_drained", 0) or 0
        ),
        "missing_after_drain_records": int(records.get("missing", 0) or 0),
        "producer_mib_per_sec": float(
            throughput.get("producer_mib_per_sec", 0.0) or 0.0
        ),
        "consumer_mib_per_sec": float(
            throughput.get("consumer_mib_per_sec", 0.0) or 0.0
        ),
        "balanced_mib_per_sec": float(
            throughput.get("balanced_mib_per_sec", 0.0) or 0.0
        ),
        "max_flush_duration_sec": float(
            producers.get("max_flush_duration_sec", 0.0) or 0.0
        ),
        "latency_valid": bool(latency.get("valid", False)),
        "latency_sample_count": int(latency.get("sample_count", 0) or 0),
        "latency_delivered_sample_count": int(
            latency.get("delivered_sample_count", 0) or 0
        ),
        "failure_reasons": "; ".join(str(reason) for reason in reasons),
    }


def _write_summary(output_dir: Path, rows: list[dict[str, Any]]) -> None:
    with (output_dir / "pilot_summary.csv").open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    _write_json(output_dir / "pilot_summary.json", {"cases": rows})
    lines = [
        "# Pulsar Historical Integration Pilots",
        "",
        (
            "These are retained integration diagnostics, not Phase 1 benchmark "
            "results and not a Pulsar performance recommendation."
        ),
        "",
        "| Pilot | Profile | Health | Eligible | Qualified | Delivered | Missing after drain | Max flush (s) | Latency valid |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            "| {pilot_id} | {profile_id} | {backend_health} | {eligible} | "
            "{qualified} | {producer_delivered_records} | "
            "{missing_after_drain_records} | {max_flush_duration_sec:.3f} | "
            "{latency_valid} |".format(**row)
        )
    lines.extend(
        [
            "",
            (
                "P0 used an 8-GiB heap and 8-GiB direct-memory limit and exposed "
                "a direct-memory failure. P1 changed only the direct-memory limit "
                "to 32 GiB; its service remained healthy, but its fixed drain "
                "expired before all producer-confirmed records were consumed."
            ),
            "",
        ]
    )
    (output_dir / "pilot_summary.md").write_text(
        "\n".join(lines),
        encoding="utf-8",
    )


def _write_manifests(output_dir: Path) -> None:
    excluded = {"SHA256SUMS", "artifact_manifest.csv"}
    files = sorted(
        path
        for path in output_dir.rglob("*")
        if path.is_file() and path.name not in excluded
    )
    rows = [
        {
            "path": path.relative_to(output_dir).as_posix(),
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in files
    ]
    with (output_dir / "artifact_manifest.csv").open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=("path", "bytes", "sha256"))
        writer.writeheader()
        writer.writerows(rows)
    checksum_files = files + [output_dir / "artifact_manifest.csv"]
    (output_dir / "SHA256SUMS").write_text(
        "".join(
            f"{_sha256(path)}  {path.relative_to(output_dir).as_posix()}\n"
            for path in checksum_files
        ),
        encoding="utf-8",
    )


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _sanitize_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_value(item) for item in value]
    if isinstance(value, str):
        return _sanitize_text(value)
    return value


def _sanitize_text(value: str) -> str:
    sanitized = value
    sanitized = re.sub(
        r"/(?:mnt/vast-nhr/home/[^/\s]+/u[0-9]+|user/[^/\s]+/u[0-9]+)"
        r"/[^\s\"']*(?:kafka|messaging)[^\s\"']*",
        "${PROJECT_ROOT}",
        sanitized,
    )
    sanitized = re.sub(
        r"/(?:mnt/vast-nhr/home/[^/\s]+/u[0-9]+|user/[^/\s]+/u[0-9]+)",
        "${HPC_HOME}",
        sanitized,
    )
    sanitized = re.sub(
        r"/home/[^/\s]+/[^\s\"']*(?:kafka|messaging)[^\s\"']*",
        "${PROJECT_ROOT}",
        sanitized,
    )
    sanitized = re.sub(r"/home/[^/\s]+", "${LOCAL_HOME}", sanitized)
    sanitized = re.sub(
        r"/dev/shm/kafka-simple-u[0-9]+-[0-9]+",
        "${RAM_ROOT}",
        sanitized,
    )
    sanitized = sanitized.replace(
        "Kafka broker JMX throughput was not available.",
        "Pulsar broker throughput telemetry was not available.",
    )
    sanitized = sanitized.replace(
        "Broker JMX ingress should be inspected once broker throughput data is available.",
        "Pulsar broker ingress should be inspected once broker telemetry is available.",
    )
    sanitized = sanitized.replace(
        "Kafka broker logs plus runtime temp/cache/client directories were RAM-backed",
        "Pulsar service data plus runtime temp/cache/client directories were RAM-backed",
    )
    return sanitized


def _reject_private_paths(output_dir: Path) -> None:
    forbidden = re.compile(
        r"/(?:home/[^/\s]+|user/[^/\s]+/u[0-9]+|"
        r"mnt/vast-nhr/home/[^/\s]+/u[0-9]+)"
        r"|/dev/shm/kafka-simple-u[0-9]+-[0-9]+"
    )
    failures: list[str] = []
    for path in output_dir.rglob("*"):
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if forbidden.search(text):
            failures.append(path.relative_to(output_dir).as_posix())
    if failures:
        raise RuntimeError(
            "Published Pulsar artifacts contain private paths: "
            + ", ".join(failures)
        )


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _bool(value: Any) -> bool:
    return bool(value)


def _relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path)


if __name__ == "__main__":
    raise SystemExit(main())
