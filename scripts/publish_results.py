#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PUBLISHED_ROOT = PROJECT_ROOT / "results" / "published" / "kafka"
V1_ROOT = PUBLISHED_ROOT / "v1"
V2_ROOT = PUBLISHED_ROOT / "v2" / "screening"
COMBINED_ROOT = PUBLISHED_ROOT / "combined"
V2_SOURCE = PROJECT_ROOT / "results" / "tuning_v2" / "combined_screening_analysis"

V1_FILES = (
    "analysis_artifact_manifest.csv",
    "analysis_controlled_comparisons.csv",
    "analysis_correlations.csv",
    "analysis_descriptive_stats.csv",
    "analysis_feature_importance.csv",
    "analysis_model_results.csv",
    "analysis_pareto_front.csv",
    "analysis_qualification_sensitivity.csv",
    "analysis_repetition_plan.csv",
    "analysis_repetition_plan.md",
    "analysis_resource_data_quality.csv",
    "analysis_validation_repeats.csv",
    "analysis_validation_results.csv",
    "analysis_validation_results.json",
    "analysis_validation_results.md",
    "analysis_validation_shortlist.csv",
    "analysis_validation_shortlist.json",
    "analysis_validation_shortlist.md",
    "analysis_verified_dataset.csv",
    "analysis_verified_summary.md",
    "kafka_benchmark_complete_analysis.html",
    "kafka_benchmark_complete_analysis.pdf",
    "kafka_benchmark_complete_analysis.tex",
)
V1_DIRS = ("analysis_diagrams", "analysis_plots")
V2_PREFIX = "kafka_broker_tuning_v2_"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build or verify compact published Kafka result bundles"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify existing bundles without changing them",
    )
    parser.add_argument(
        "--combined-only",
        action="store_true",
        help="refresh only combined-report metadata and checksums",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.check:
        verify_all()
        return 0
    if args.combined_only:
        publish_combined_metadata()
        verify_combined()
        return 0
    publish_v1()
    publish_v2()
    write_bundle_metadata(
        V1_ROOT,
        {
            "backend_id": "kafka",
            "campaign": "v1",
            "status": "complete",
            "screening_rows": 120,
            "validation_rows": 50,
            "validated_recommendation": "cfg_074",
            "notes": (
                "Historical single-broker screening and repeated validation; "
                "not pooled with V2."
            ),
        },
    )
    write_bundle_metadata(
        V2_ROOT,
        {
            "backend_id": "kafka",
            "campaign": "v2/screening",
            "status": "interim",
            "case_rows": 30,
            "eligible_rows": 26,
            "qualified_rows": 24,
            "frozen_broker_winner": None,
            "notes": (
                "Focused broker-profile screening only; confirmation and final "
                "validation are incomplete."
            ),
        },
    )
    publish_combined_metadata()
    verify_all()
    return 0


def publish_combined_metadata() -> None:
    required = (
        COMBINED_ROOT / "kafka_benchmark_complete_report.tex",
        COMBINED_ROOT / "kafka_benchmark_complete_report.pdf",
        COMBINED_ROOT / "data" / "v1_analysis_verified_dataset.csv",
        COMBINED_ROOT / "data" / "v1_analysis_validation_repeats.csv",
        COMBINED_ROOT / "data" / f"{V2_PREFIX}cases.csv",
    )
    missing = [path for path in required if not path.is_file()]
    if missing:
        raise FileNotFoundError(
            "Combined publication is incomplete: "
            + ", ".join(str(path) for path in missing)
        )
    write_bundle_metadata(
        COMBINED_ROOT,
        {
            "backend_id": "kafka",
            "campaign": "combined",
            "status": "integrated",
            "v1_screening_rows": 120,
            "v1_validation_rows": 50,
            "v1_validated_recommendation": "cfg_074",
            "v2_screening_rows": 30,
            "v2_eligible_rows": 26,
            "v2_qualified_rows": 24,
            "v2_frozen_broker_winner": None,
            "notes": (
                "One report integrates complete V1 evidence and interim V2 "
                "screening without pooling observations across phases."
            ),
        },
    )


def publish_v1() -> None:
    V1_ROOT.mkdir(parents=True, exist_ok=True)
    for name in V1_FILES:
        source = PROJECT_ROOT / name
        destination = V1_ROOT / name
        if source.is_file() and source.resolve() != destination.resolve():
            shutil.copy2(source, destination)
        if not destination.is_file():
            raise FileNotFoundError(f"Missing V1 publication input: {name}")
    for name in V1_DIRS:
        source = PROJECT_ROOT / name
        destination = V1_ROOT / name
        if source.is_dir() and source.resolve() != destination.resolve():
            shutil.copytree(source, destination, dirs_exist_ok=True)
        if not destination.is_dir():
            raise FileNotFoundError(f"Missing V1 publication directory: {name}")
    _write_v1_analysis_manifest()


def publish_v2() -> None:
    V2_ROOT.mkdir(parents=True, exist_ok=True)
    source_json = V2_SOURCE / f"{V2_PREFIX}cases.json"
    if not source_json.is_file():
        existing = V2_ROOT / f"{V2_PREFIX}cases.json"
        if not existing.is_file():
            raise FileNotFoundError(
                "Missing compact V2 source dataset: " + str(source_json)
            )
        source_json = existing
    rows = json.loads(source_json.read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        raise ValueError("V2 compact cases JSON must contain a list")
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("V2 compact case row must be an object")
        row["report_path"] = (
            f"raw/{row.get('stage', 'unknown')}/"
            f"{row.get('case_id', 'unknown')}/final_report.json"
        )
    compact_json = V2_ROOT / f"{V2_PREFIX}cases.json"
    compact_json.write_text(
        json.dumps(rows, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_csv(V2_ROOT / f"{V2_PREFIX}cases.csv", rows)
    subprocess.run(
        [
            sys.executable,
            "-B",
            str(PROJECT_ROOT / "scripts" / "analyze_broker_tuning_v2.py"),
            "--case-summary",
            str(compact_json),
            "--stage",
            "profile_screening",
            "--output-dir",
            str(V2_ROOT),
            "--compile-pdf",
        ],
        cwd=PROJECT_ROOT,
        check=True,
    )
    generated_tex = V2_ROOT / f"{V2_PREFIX}report.tex"
    source_tex = V2_SOURCE / f"{V2_PREFIX}report.tex"
    source_pdf = V2_SOURCE / f"{V2_PREFIX}report.pdf"
    if (
        source_tex.is_file()
        and source_pdf.is_file()
        and sha256_file(generated_tex) == sha256_file(source_tex)
    ):
        shutil.copy2(source_pdf, V2_ROOT / f"{V2_PREFIX}report.pdf")
    for suffix in (".aux", ".log", ".out"):
        (V2_ROOT / f"{V2_PREFIX}report{suffix}").unlink(missing_ok=True)


def _write_v1_analysis_manifest() -> None:
    artifact_paths = [
        V1_ROOT / name
        for name in V1_FILES
        if name != "analysis_artifact_manifest.csv"
    ]
    for directory_name in V1_DIRS:
        artifact_paths.extend(
            path
            for path in sorted((V1_ROOT / directory_name).rglob("*"))
            if path.is_file()
        )
    rows = [
        {
            "artifact": path.relative_to(V1_ROOT).as_posix(),
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in artifact_paths
    ]
    _write_csv(V1_ROOT / "analysis_artifact_manifest.csv", rows)


def write_bundle_metadata(root: Path, status: dict[str, Any]) -> None:
    existing_provenance = root / "provenance.json"
    generated_utc = None
    if existing_provenance.is_file():
        try:
            generated_utc = json.loads(
                existing_provenance.read_text(encoding="utf-8")
            ).get("generated_utc")
        except (json.JSONDecodeError, AttributeError):
            generated_utc = None
    provenance = {
        "format": "messaging-benchmark.provenance.v1",
        "generated_utc": generated_utc
        or datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "repository": "kafka-simple-benchmark",
        "backend_id": status["backend_id"],
        "campaign": status["campaign"],
        "raw_data_policy": (
            "Compact evidence is published here. Raw Prometheus and per-case "
            "run directories remain outside normal Git history."
        ),
    }
    (root / "campaign_status.json").write_text(
        json.dumps(status, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (root / "provenance.json").write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifact_paths = [
        path
        for path in sorted(root.rglob("*"))
        if path.is_file()
        and path.name not in {"SHA256SUMS", "artifact_manifest.csv"}
        and path.suffix not in {".aux", ".log", ".out", ".toc"}
    ]
    manifest_rows = [
        {
            "path": path.relative_to(root).as_posix(),
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in artifact_paths
    ]
    _write_csv(root / "artifact_manifest.csv", manifest_rows)
    checksum_paths = artifact_paths + [root / "artifact_manifest.csv"]
    (root / "SHA256SUMS").write_text(
        "".join(
            f"{sha256_file(path)}  {path.relative_to(root).as_posix()}\n"
            for path in checksum_paths
        ),
        encoding="utf-8",
    )


def verify_all() -> None:
    verify_bundle(V1_ROOT)
    verify_bundle(V2_ROOT)
    verify_combined()
    v1_rows = _csv_count(V1_ROOT / "analysis_verified_dataset.csv")
    validation_rows = _csv_count(V1_ROOT / "analysis_validation_repeats.csv")
    v2_rows = _read_csv(V2_ROOT / f"{V2_PREFIX}cases.csv")
    if v1_rows != 120:
        raise ValueError(f"V1 screening row count changed: {v1_rows}")
    if validation_rows != 50:
        raise ValueError(f"V1 validation row count changed: {validation_rows}")
    eligible = sum(_bool(row.get("eligible")) for row in v2_rows)
    qualified = sum(_bool(row.get("qualified")) for row in v2_rows)
    if (len(v2_rows), eligible, qualified) != (30, 26, 24):
        raise ValueError(
            "V2 screening counts changed: "
            f"{len(v2_rows)} rows, {eligible} eligible, {qualified} qualified"
        )
    print(
        "[publish] verified V1=120 screening/50 validation; "
        "V2=30 cases/26 eligible/24 qualified; combined report present"
    )


def verify_combined() -> None:
    verify_bundle(COMBINED_ROOT)
    source_pairs = (
        (
            V1_ROOT / "analysis_verified_dataset.csv",
            COMBINED_ROOT / "data" / "v1_analysis_verified_dataset.csv",
        ),
        (
            V1_ROOT / "analysis_validation_repeats.csv",
            COMBINED_ROOT / "data" / "v1_analysis_validation_repeats.csv",
        ),
        (
            V2_ROOT / f"{V2_PREFIX}cases.csv",
            COMBINED_ROOT / "data" / f"{V2_PREFIX}cases.csv",
        ),
        (
            V2_ROOT / f"{V2_PREFIX}cases.json",
            COMBINED_ROOT / "data" / f"{V2_PREFIX}cases.json",
        ),
        (
            V2_ROOT / f"{V2_PREFIX}decisions.json",
            COMBINED_ROOT / "data" / f"{V2_PREFIX}decisions.json",
        ),
    )
    for source, bundled in source_pairs:
        if sha256_file(source) != sha256_file(bundled):
            raise ValueError(
                "Combined compact evidence differs from its published source: "
                f"{bundled}"
            )
    v1_rows = _csv_count(
        COMBINED_ROOT / "data" / "v1_analysis_verified_dataset.csv"
    )
    validation_rows = _csv_count(
        COMBINED_ROOT / "data" / "v1_analysis_validation_repeats.csv"
    )
    v2_rows = _read_csv(
        COMBINED_ROOT / "data" / f"{V2_PREFIX}cases.csv"
    )
    eligible = sum(_bool(row.get("eligible")) for row in v2_rows)
    qualified = sum(_bool(row.get("qualified")) for row in v2_rows)
    if (v1_rows, validation_rows) != (120, 50):
        raise ValueError(
            "Combined V1 row counts changed: "
            f"{v1_rows} screening, {validation_rows} validation"
        )
    if (len(v2_rows), eligible, qualified) != (30, 26, 24):
        raise ValueError(
            "Combined V2 row counts changed: "
            f"{len(v2_rows)} rows, {eligible} eligible, "
            f"{qualified} qualified"
        )
    print(
        "[publish] verified combined V1=120 screening/50 validation; "
        "V2=30 cases/26 eligible/24 qualified"
    )


def verify_bundle(root: Path) -> None:
    checksum_path = root / "SHA256SUMS"
    manifest_path = root / "artifact_manifest.csv"
    if not checksum_path.is_file():
        raise FileNotFoundError(f"Missing bundle checksums: {checksum_path}")
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Missing bundle manifest: {manifest_path}")
    manifest_rows = _read_csv(manifest_path)
    manifest_names = {row["path"] for row in manifest_rows}
    actual_names = {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file()
        and path.name not in {"SHA256SUMS", "artifact_manifest.csv"}
        and path.suffix not in {".aux", ".log", ".out", ".toc"}
    }
    if manifest_names != actual_names:
        missing = sorted(actual_names - manifest_names)
        stale = sorted(manifest_names - actual_names)
        raise ValueError(
            f"Bundle manifest coverage mismatch in {root}: "
            f"missing={missing}, stale={stale}"
        )
    for row in manifest_rows:
        path = root / row["path"]
        if int(row["bytes"]) != path.stat().st_size:
            raise ValueError(f"Manifest byte count mismatch: {path}")
        if row["sha256"] != sha256_file(path):
            raise ValueError(f"Manifest checksum mismatch: {path}")

    checksum_names: set[str] = set()
    for line in checksum_path.read_text(encoding="utf-8").splitlines():
        expected, relative = line.split("  ", 1)
        checksum_names.add(relative)
        path = root / relative
        actual = sha256_file(path)
        if actual != expected:
            raise ValueError(f"Checksum mismatch: {path}")
    expected_checksum_names = actual_names | {"artifact_manifest.csv"}
    if checksum_names != expected_checksum_names:
        raise ValueError(f"SHA256SUMS coverage mismatch in {root}")
    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() in {
            ".csv",
            ".json",
            ".md",
            ".tex",
            ".html",
            ".txt",
        }:
            text = path.read_text(encoding="utf-8", errors="replace")
            private_path = re.search(
                r"/(?:home/[^/\s]+|tmp|mnt/vast-nhr)(?:/|\b)",
                text,
            )
            if private_path:
                raise ValueError(f"Developer-specific absolute path in {path}")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _csv_count(path: Path) -> int:
    return len(_read_csv(path))


def _bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


if __name__ == "__main__":
    raise SystemExit(main())
