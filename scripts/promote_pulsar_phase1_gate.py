#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import statistics
import sys
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from validate_pulsar_pilots import (  # noqa: E402
    _discover_reports,
    _instrumentation_overhead,
    _read_manifest,
    _validate_manifest,
)


DEFAULT_INSTRUMENTATION_ROOT = (
    PROJECT_ROOT
    / "results"
    / "runs"
    / "pulsar"
    / "pulsar_phase1_instrumentation_20260807_retry1"
)
DEFAULT_INSTRUMENTATION_MANIFEST = (
    PROJECT_ROOT
    / "configs"
    / "campaigns"
    / "pulsar"
    / "pilots"
    / "corrected"
    / "instrumentation_overhead_manifest.csv"
)
DEFAULT_BATCH_ROOT = (
    PROJECT_ROOT
    / "results"
    / "runs"
    / "pulsar"
    / "pulsar_batch10_acceptance_20260807_isolated"
)
DEFAULT_BATCH_ACCEPTANCE = DEFAULT_BATCH_ROOT / "acceptance" / "acceptance_report.json"
DEFAULT_BATCH_MANIFEST = (
    PROJECT_ROOT
    / "configs"
    / "campaigns"
    / "pulsar"
    / "pilots"
    / "batch_runner_10_manifest.csv"
)
DEFAULT_PROFILE = (
    PROJECT_ROOT
    / "configs"
    / "backends"
    / "pulsar"
    / "profiles"
    / "BASELINE_H16_D32.json"
)
DEFAULT_OUTPUT = (
    PROJECT_ROOT / "results" / "published" / "pulsar" / "phase1-gate"
)
RESET_FREE_PATTERN = re.compile(r"^post_reset_free_percent=(\d+)$", re.MULTILINE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Promote accepted Pulsar instrumentation and batch-runner evidence "
            "into the immutable Phase 1 submission gate."
        )
    )
    parser.add_argument(
        "--instrumentation-results-root",
        type=Path,
        default=DEFAULT_INSTRUMENTATION_ROOT,
    )
    parser.add_argument(
        "--instrumentation-manifest",
        type=Path,
        default=DEFAULT_INSTRUMENTATION_MANIFEST,
    )
    parser.add_argument("--batch-results-root", type=Path, default=DEFAULT_BATCH_ROOT)
    parser.add_argument(
        "--batch-acceptance-report",
        type=Path,
        default=DEFAULT_BATCH_ACCEPTANCE,
    )
    parser.add_argument("--batch-manifest", type=Path, default=DEFAULT_BATCH_MANIFEST)
    parser.add_argument("--profile", type=Path, default=DEFAULT_PROFILE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--minimum-reset-free-percent", type=int, default=25)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    failures: list[str] = []
    profile = _validate_profile(args.profile, failures)
    profile_id = str(profile.get("profile_id", ""))
    profile_sha256 = str(profile.get("sha256", ""))

    instrumentation_manifest = _read_manifest(args.instrumentation_manifest)
    instrumentation_reports = _discover_reports(
        [args.instrumentation_results_root]
    )
    instrumentation_validation = _validate_manifest(
        instrumentation_manifest,
        instrumentation_reports,
    )
    instrumentation = _instrumentation_overhead(
        instrumentation_manifest,
        instrumentation_validation["case_rows"],
    )
    failures.extend(instrumentation_validation["failures"])
    failures.extend(instrumentation["failures"])
    if len(instrumentation_manifest) != 6:
        failures.append(
            "instrumentation manifest must contain exactly 6 paired cases"
        )
    _validate_instrumentation_identity(
        reports=instrumentation_reports,
        expected_case_ids={row["case_id"] for row in instrumentation_manifest},
        profile_id=profile_id,
        profile_sha256=profile_sha256,
        failures=failures,
    )

    batch_acceptance = _read_json(args.batch_acceptance_report)
    batch = _validate_batch_acceptance(
        batch_acceptance,
        profile_id=profile_id,
        profile_sha256=profile_sha256,
        failures=failures,
    )
    storage_reset = _validate_storage_resets(
        args.batch_results_root,
        expected_case_ids={str(row.get("case_id", "")) for row in batch_acceptance.get("cases", [])},
        minimum_free_percent=args.minimum_reset_free_percent,
        failures=failures,
    )
    batch_summary = _validate_batch_summary(args.batch_results_root, failures)

    evidence_sha256 = {
        "profile_file": _sha256(args.profile),
        "instrumentation_manifest": _sha256(args.instrumentation_manifest),
        "instrumentation_reports": {
            case_id: _sha256(Path(str(report["_source_path"])))
            for case_id, report in sorted(instrumentation_reports.items())
            if case_id in {row["case_id"] for row in instrumentation_manifest}
        },
        "batch_manifest": _sha256(args.batch_manifest),
        "batch_acceptance_report": _sha256(args.batch_acceptance_report),
        "batch_summary": batch_summary.get("sha256", ""),
        "storage_reset_logs": storage_reset.get("sha256", {}),
    }
    failures = list(dict.fromkeys(failures))
    authorized = not failures
    runtime = _dict(_dict(profile.get("settings")).get("runtime"))
    report = {
        "format": "messaging-benchmark.pulsar-phase1-gate.v1",
        "backend_id": "pulsar",
        "phase1_submission_authorized": authorized,
        "profile": {
            "profile_id": profile_id,
            "profile_sha256": profile_sha256,
            "product_version": profile.get("product_version"),
            "java_major": runtime.get("java_major"),
            "jvm_memory": runtime.get("jvm_memory"),
            "service_mode": _dict(profile.get("settings")).get("service_mode"),
            "storage": runtime.get("storage"),
            "pulsar_python_client_version": "3.13.0",
        },
        "instrumentation": {
            **instrumentation,
            "run_id": args.instrumentation_results_root.name,
            "profile_identity_valid": not any(
                "instrumentation" in reason and "profile" in reason
                for reason in failures
            ),
        },
        "batch_runner": {
            **batch,
            "run_id": args.batch_results_root.name,
            "storage_reset": storage_reset,
            "slurm_job_id": batch_summary.get("slurm_job_id", ""),
        },
        "rules": {
            "instrumentation_case_count": 6,
            "instrumentation_median_overhead_percent_max": 3.0,
            "batch_case_count": 10,
            "batch_complete": True,
            "all_batch_cases_scientifically_valid": True,
            "all_batch_cases_latency_valid": True,
            "minimum_post_reset_tmpfs_free_percent": args.minimum_reset_free_percent,
            "immutable_profile_required": True,
        },
        "source_evidence": {
            "instrumentation_run_id": args.instrumentation_results_root.name,
            "batch_run_id": args.batch_results_root.name,
            "sha256": evidence_sha256,
        },
        "failure_reasons": failures,
        "interpretation": (
            "This gate authorizes the tested Pulsar runtime, instrumentation, "
            "and sequential one-allocation case lifecycle for Phase 1. It does "
            "not make the 10 acceptance cases part of the 120-case Phase 1 "
            "performance dataset and does not claim that the fixed profile is "
            "a universal Pulsar optimum."
        ),
    }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.output_dir / "acceptance_report.json"
    md_path = args.output_dir / "acceptance_report.md"
    json_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    md_path.write_text(_markdown(report), encoding="utf-8")
    print(
        "[pulsar-phase1-promote] Phase 1 submission authorized: "
        f"{str(authorized).lower()}"
    )
    print(f"[pulsar-phase1-promote] report: {json_path}")
    return 0 if authorized else 1


def _validate_profile(path: Path, failures: list[str]) -> dict[str, Any]:
    profile = _read_json(path)
    settings = profile.get("settings")
    if not isinstance(settings, dict):
        failures.append("profile settings are missing")
        return profile
    actual = _canonical_sha256(settings)
    recorded = str(profile.get("sha256", ""))
    if actual != recorded:
        failures.append(
            f"profile checksum mismatch: calculated {actual}, recorded {recorded}"
        )
    if profile.get("backend_id") != "pulsar":
        failures.append("profile backend_id is not pulsar")
    if profile.get("profile_id") != "BASELINE_H16_D32":
        failures.append("Phase 1 profile is not BASELINE_H16_D32")
    runtime = _dict(settings.get("runtime"))
    expected_runtime = {
        "product_version": "5.0.0-M1",
        "java_major": 21,
        "jvm_memory": "-Xms16g -Xmx16g -XX:MaxDirectMemorySize=32g",
        "storage": "ram-backed",
    }
    for name, expected in expected_runtime.items():
        if runtime.get(name) != expected:
            failures.append(
                f"profile runtime {name} is {runtime.get(name)!r}, expected {expected!r}"
            )
    return profile


def _validate_instrumentation_identity(
    *,
    reports: dict[str, dict[str, Any]],
    expected_case_ids: set[str],
    profile_id: str,
    profile_sha256: str,
    failures: list[str],
) -> None:
    for case_id in sorted(expected_case_ids):
        report = reports.get(case_id)
        if report is None:
            continue
        system = _dict(report.get("system_under_test"))
        if system.get("backend_id") != "pulsar":
            failures.append(f"instrumentation {case_id}: backend is not pulsar")
        if system.get("profile_id") != profile_id:
            failures.append(f"instrumentation {case_id}: profile ID mismatch")
        if system.get("profile_sha256") != profile_sha256:
            failures.append(f"instrumentation {case_id}: profile checksum mismatch")


def _validate_batch_acceptance(
    report: dict[str, Any],
    *,
    profile_id: str,
    profile_sha256: str,
    failures: list[str],
) -> dict[str, Any]:
    expected_count = 10
    checks = {
        "format": report.get("format") == "messaging-benchmark.backend-batch-acceptance.v1",
        "backend": report.get("backend_id") == "pulsar",
        "batch_complete": report.get("batch_complete") is True,
        "expected_count": report.get("expected_case_count") == expected_count,
        "observed_count": report.get("observed_required_case_count") == expected_count,
        "completed_count": report.get("completed_case_count") == expected_count,
        "scientifically_valid_count": report.get("scientifically_valid_case_count") == expected_count,
        "eligible_count": report.get("eligible_case_count") == expected_count,
        "latency_valid_count": report.get("latency_valid_case_count") == expected_count,
        "profile_id": report.get("profile_id") == profile_id,
        "profile_sha256": report.get("profile_sha256") == profile_sha256,
        "no_missing_cases": not report.get("missing_case_ids"),
        "no_duplicate_cases": not report.get("duplicate_case_ids"),
        "no_mechanical_failures": not report.get("mechanical_failure_reasons"),
    }
    for name, passed in checks.items():
        if not passed:
            failures.append(f"batch acceptance check failed: {name}")
    cases = report.get("cases") if isinstance(report.get("cases"), list) else []
    if len(cases) != expected_count:
        failures.append(f"batch acceptance has {len(cases)} case rows, expected 10")
    for row in cases:
        case_id = str(_dict(row).get("case_id", "unknown"))
        required = {
            "status": row.get("status") == "completed",
            "backend_health": row.get("backend_health") == "healthy",
            "profile_id": row.get("profile_id") == profile_id,
            "profile_sha256": row.get("profile_sha256") == profile_sha256,
            "scientifically_valid": row.get("scientifically_valid") is True,
            "latency_valid": row.get("latency_valid") is True,
            "missing_records": row.get("records_missing_after_drain") == 0,
            "invalid_envelopes": row.get("invalid_envelopes") == 0,
            "duplicate_records": row.get("duplicate_records") == 0,
            "out_of_order_records": row.get("out_of_order_records") == 0,
        }
        for name, passed in required.items():
            if not passed:
                failures.append(f"batch {case_id}: check failed: {name}")
    return {
        "required_case_count": expected_count,
        "observed_case_count": len(cases),
        "completed_case_count": int(report.get("completed_case_count", 0) or 0),
        "scientifically_valid_case_count": int(
            report.get("scientifically_valid_case_count", 0) or 0
        ),
        "eligible_case_count": int(report.get("eligible_case_count", 0) or 0),
        "qualified_case_count": int(report.get("qualified_case_count", 0) or 0),
        "latency_valid_case_count": int(
            report.get("latency_valid_case_count", 0) or 0
        ),
        "passed": all(checks.values()) and len(cases) == expected_count,
    }


def _validate_storage_resets(
    results_root: Path,
    *,
    expected_case_ids: set[str],
    minimum_free_percent: int,
    failures: list[str],
) -> dict[str, Any]:
    reset_paths = sorted(results_root.rglob("post-case-storage-reset.log"))
    observed: dict[str, list[Path]] = {}
    for path in reset_paths:
        case_id = path.parents[2].name
        if case_id in expected_case_ids:
            observed.setdefault(case_id, []).append(path)
    rows: list[dict[str, Any]] = []
    hashes: dict[str, str] = {}
    for case_id in sorted(expected_case_ids):
        paths = observed.get(case_id, [])
        if len(paths) != 1:
            failures.append(
                f"batch {case_id}: expected one storage-reset log, found {len(paths)}"
            )
            continue
        path = paths[0]
        match = RESET_FREE_PATTERN.search(path.read_text(encoding="utf-8"))
        if match is None:
            failures.append(f"batch {case_id}: reset free percentage is missing")
            continue
        free_percent = int(match.group(1))
        if free_percent < minimum_free_percent:
            failures.append(
                f"batch {case_id}: post-reset tmpfs free is {free_percent}%"
            )
        rows.append({"case_id": case_id, "tmpfs_free_percent": free_percent})
        hashes[case_id] = _sha256(path)
    values = [row["tmpfs_free_percent"] for row in rows]
    return {
        "required_case_count": len(expected_case_ids),
        "observed_case_count": len(rows),
        "minimum_tmpfs_free_percent": min(values) if values else None,
        "median_tmpfs_free_percent": statistics.median(values) if values else None,
        "maximum_tmpfs_free_percent": max(values) if values else None,
        "passed": len(rows) == len(expected_case_ids)
        and bool(values)
        and min(values) >= minimum_free_percent,
        "cases": rows,
        "sha256": hashes,
    }


def _validate_batch_summary(
    results_root: Path,
    failures: list[str],
) -> dict[str, Any]:
    paths = sorted(results_root.rglob("batch_summary.json"))
    if len(paths) != 1:
        failures.append(f"expected one batch summary, found {len(paths)}")
        return {}
    path = paths[0]
    summary = _read_json(path)
    checks = {
        "format": summary.get("format") == "messaging-benchmark.backend-batch-summary.v1",
        "backend": summary.get("backend_id") == "pulsar",
        "case_count": summary.get("case_count") == 10,
        "completed_count": summary.get("completed_count") == 10,
        "failed_count": summary.get("failed_count") == 0,
        "skipped_count": summary.get("skipped_count") == 0,
    }
    for name, passed in checks.items():
        if not passed:
            failures.append(f"batch summary check failed: {name}")
    return {
        "run_id": summary.get("run_id"),
        "slurm_job_id": summary.get("slurm_job_id"),
        "sha256": _sha256(path),
    }


def _markdown(report: dict[str, Any]) -> str:
    instrumentation = report["instrumentation"]
    batch = report["batch_runner"]
    reset = batch["storage_reset"]
    profile = report["profile"]
    lines = [
        "# Pulsar Phase 1 Submission Gate",
        "",
        "- Phase 1 submission authorized: "
        f"**{str(report['phase1_submission_authorized']).lower()}**",
        f"- Profile: `{profile['profile_id']}` (`{profile['profile_sha256']}`)",
        f"- Pulsar: `{profile['product_version']}`",
        f"- Java: `{profile['java_major']}`",
        f"- Pulsar Python client: `{profile['pulsar_python_client_version']}`",
        f"- JVM memory: `{profile['jvm_memory']}`",
        "",
        "This gate validates the fixed runtime, latency instrumentation, and "
        "sequential one-allocation lifecycle. The acceptance cases are not "
        "included in the Phase 1 performance dataset.",
        "",
        "## Instrumentation A/B",
        "",
        "| Block | Latency off (MiB/s) | Latency on (MiB/s) | Overhead (%) |",
        "|---:|---:|---:|---:|",
    ]
    for row in instrumentation["block_results"]:
        lines.append(
            "| {block} | {disabled_balanced_mib_per_sec:.3f} | "
            "{enabled_balanced_mib_per_sec:.3f} | {overhead_percent:.3f} |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "Median instrumentation overhead: "
            f"**{instrumentation['median_overhead_percent']:.3f}%** "
            "(acceptance limit: 3%).",
            "",
            "## Sequential Batch Acceptance",
            "",
            f"- Complete cases: {batch['completed_case_count']}/10",
            f"- Scientifically valid cases: {batch['scientifically_valid_case_count']}/10",
            f"- Eligible cases: {batch['eligible_case_count']}/10",
            f"- Qualified cases: {batch['qualified_case_count']}/10",
            f"- Latency-valid cases: {batch['latency_valid_case_count']}/10",
            f"- Slurm job: `{batch['slurm_job_id']}`",
            "- Post-case tmpfs free: "
            f"minimum {reset['minimum_tmpfs_free_percent']}%, "
            f"median {reset['median_tmpfs_free_percent']:.1f}%, "
            f"maximum {reset['maximum_tmpfs_free_percent']}%",
            "",
            "## Decision",
            "",
        ]
    )
    if report["failure_reasons"]:
        lines.extend(f"- {reason}" for reason in report["failure_reasons"])
    else:
        lines.append(
            "- Accepted. The fixed profile and one-allocation runner may be "
            "used for the 120-case Phase 1 screening campaign."
        )
    lines.extend(["", report["interpretation"], ""])
    return "\n".join(lines)


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object: {path}")
    return value


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
