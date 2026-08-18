#!/usr/bin/env python3
"""Build a validated, presentation-free reproducible result bundle."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
from typing import Any


RESULT_FORMAT = "messaging-benchmark.reproducible-results.v1"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Finalize validated reproducible JSON/CSV artifacts without "
            "generating figures, prose reports, LaTeX, or PDF files."
        )
    )
    parser.add_argument("--backend", required=True)
    parser.add_argument("--campaign-phase", choices=("v1", "v2"), default="v1")
    parser.add_argument("--phase1-dir", type=Path, required=True)
    parser.add_argument("--validation-dir", type=Path, required=True)
    parser.add_argument(
        "--shortlist-json",
        type=Path,
        help="Exact shortlist JSON used to generate validation (defaults to Phase 1 output).",
    )
    parser.add_argument(
        "--shortlist-csv",
        type=Path,
        help="Exact shortlist CSV used to generate validation (defaults to Phase 1 output).",
    )
    parser.add_argument("--phase1-manifest", type=Path, required=True)
    parser.add_argument("--validation-manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    phase1_dir = args.phase1_dir.resolve()
    validation_dir = args.validation_dir.resolve()
    shortlist_json = (
        args.shortlist_json.resolve()
        if args.shortlist_json
        else phase1_dir / "validation_shortlist.json"
    )
    shortlist_csv = (
        args.shortlist_csv.resolve()
        if args.shortlist_csv
        else phase1_dir / "validation_shortlist.csv"
    )
    output = args.output_dir.resolve()
    _require_separate_output(output, (phase1_dir, validation_dir))

    phase1_validation = _object(phase1_dir / "campaign_validation.json")
    repeated_validation = _object(validation_dir / "campaign_validation.json")
    if phase1_validation.get("valid") is not True:
        raise ValueError("Phase 1 campaign validation is not valid")
    if repeated_validation.get("valid") is not True:
        raise ValueError("repeated-validation campaign validation is not valid")

    phase1 = _cases(phase1_dir / "campaign_cases.json")
    repeats = _cases(validation_dir / "campaign_cases.json")
    summaries = _array(validation_dir / "validation_summary_by_original.json")
    if len(phase1) != 120:
        raise ValueError(f"Phase 1 has {len(phase1)} rows, expected 120")
    if len(repeats) != 50:
        raise ValueError(f"validation has {len(repeats)} rows, expected 50")
    if len(summaries) != 10:
        raise ValueError(f"validation summary has {len(summaries)} rows, expected 10")
    if len({str(row.get('case_id', '')) for row in repeats}) != 50:
        raise ValueError("repeated-validation case IDs are not unique")

    shortlist_ids = _shortlist_ids(shortlist_json)
    shortlist_csv_ids = [
        str(row.get("config_id", "")).strip() for row in _csv_rows(shortlist_csv)
    ]
    if shortlist_csv_ids != shortlist_ids:
        raise ValueError("shortlist CSV does not match shortlist JSON order and IDs")
    phase1_manifest_rows = _csv_rows(args.phase1_manifest.resolve())
    phase1_manifest_ids = [
        str(row.get("case_id") or row.get("config_id") or "").strip()
        for row in phase1_manifest_rows
    ]
    phase1_case_ids = [str(row.get("case_id", "")).strip() for row in phase1]
    if (
        not all(phase1_manifest_ids)
        or len(set(phase1_manifest_ids)) != 120
        or set(phase1_manifest_ids) != set(phase1_case_ids)
    ):
        raise ValueError("Phase 1 case rows do not match the executed manifest")
    summary_ids = {str(row.get("original_config_id", "")).strip() for row in summaries}
    if summary_ids != set(shortlist_ids):
        raise ValueError(
            "validation summary workloads do not match the exact executed shortlist"
        )
    manifest_rows = _csv_rows(args.validation_manifest.resolve())
    manifest_ids = [_manifest_workload_id(row) for row in manifest_rows]
    if any(not item for item in manifest_ids) or set(manifest_ids) != set(shortlist_ids):
        raise ValueError(
            "validation manifest workloads do not match the exact executed shortlist"
        )
    if any(manifest_ids.count(config_id) != 5 for config_id in shortlist_ids):
        raise ValueError("each shortlisted workload must occur exactly five times")
    _validate_randomized_blocks(manifest_rows, shortlist_ids)
    repeat_workload_ids = [
        str(row.get("workload_config_id", "")).strip() for row in repeats
    ]
    if (
        any(not item for item in repeat_workload_ids)
        or set(repeat_workload_ids) != set(shortlist_ids)
        or any(repeat_workload_ids.count(config_id) != 5 for config_id in shortlist_ids)
    ):
        raise ValueError("validation case rows do not contain five repeats per shortlist ID")
    if any(int(row.get("repeats", 0)) != 5 for row in summaries):
        raise ValueError("validation summaries must contain five repeats per workload")

    summaries.sort(key=lambda row: int(row["validation_rank"]))
    winner = summaries[0]
    output.mkdir(parents=True, exist_ok=True)

    copies = {
        phase1_dir / "campaign_cases.json": output / "phase1_cases.json",
        phase1_dir / "campaign_cases.csv": output / "phase1_cases.csv",
        phase1_dir / "campaign_validation.json": output / "phase1_validation.json",
        shortlist_json: output / "validation_shortlist.json",
        shortlist_csv: output / "validation_shortlist.csv",
        validation_dir / "campaign_cases.json": output / "validation_repeats.json",
        validation_dir / "validation_repeats_by_case.csv": output / "validation_repeats.csv",
        validation_dir / "validation_summary_by_original.json": output / "validation_summary.json",
        validation_dir / "validation_summary_by_original.csv": output / "validation_summary.csv",
        validation_dir / "campaign_validation.json": output / "validation_validation.json",
        args.phase1_manifest.resolve(): output / "phase1_manifest.csv",
        args.validation_manifest.resolve(): output / "validation_manifest.csv",
    }
    for source, destination in copies.items():
        _copy(source, destination)

    _require_csv_rows(output / "phase1_cases.csv", 120)
    _require_csv_rows(output / "validation_repeats.csv", 50)
    _require_csv_rows(output / "validation_summary.csv", 10)
    _require_csv_rows(output / "phase1_manifest.csv", 120)
    _require_csv_rows(output / "validation_manifest.csv", 50)

    source_cases = _source_case_index(phase1, repeats)
    _write_json(output / "source_case_index.json", source_cases)
    _write_csv(
        output / "source_case_index.csv",
        ("stage", "case_id", "source_report", "source_report_sha256"),
        source_cases,
    )

    report = {
        "format": RESULT_FORMAT,
        "backend_id": args.backend,
        "campaign_phase": args.campaign_phase,
        "status": "complete",
        "presentation_artifacts_required": False,
        "detailed_source_of_truth": "per-case final_report.json files",
        "comparison_format": "CSV",
        "measurement_contract_id": "measurement.messaging.reproducible.v1",
        "qualification_policy_id": "qualification.application.v1",
        "phase1": {
            "case_count": len(phase1),
            "eligible_count": sum(bool(row["eligible"]) for row in phase1),
            "qualified_count": sum(bool(row["qualified"]) for row in phase1),
            "overdriven_count": sum(
                row["qualification_status"] == "overdriven" for row in phase1
            ),
        },
        "repeated_validation": {
            "case_count": len(repeats),
            "configuration_count": len(summaries),
            "repeats_per_configuration": 5,
        },
        "source_case_index": {
            "row_count": len(source_cases),
            "csv": "source_case_index.csv",
            "json": "source_case_index.json",
        },
        "recommendation": {
            "config_id": winner["original_config_id"],
            "qualified_repeats": winner["qualified_count"],
            "eligible_repeats": winner["eligible_count"],
            "median_balanced_mib_per_sec": winner["median_balanced_mib_per_sec"],
            "median_balanced_records_per_sec": winner[
                "median_balanced_records_per_sec"
            ],
            "median_latency_p99_us": winner["median_latency_p99_us"],
        },
        "ranking_rule": [
            "qualified-repeat count descending",
            "median balanced MiB/s descending",
            "balanced-throughput IQR ascending",
            "producer backlog ascending",
            "flush duration ascending",
            "failed-send percentage ascending",
            "configuration ID ascending",
        ],
        "source_sha256": {
            destination.name: _sha256(source)
            for source, destination in copies.items()
        },
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    _write_json(output / "final_report.json", report)
    _write_artifact_manifest(output)
    print(f"[reproducible-results] backend: {args.backend}")
    print(f"[reproducible-results] winner: {winner['original_config_id']}")
    print(f"[reproducible-results] output: {output}")
    return 0


def _require_separate_output(output: Path, sources: tuple[Path, ...]) -> None:
    for source in sources:
        if output == source or source in output.parents or output in source.parents:
            raise ValueError("machine bundle output must be separate from analysis inputs")


def _require_csv_rows(path: Path, expected: int) -> None:
    with path.open("r", encoding="utf-8", newline="") as handle:
        count = sum(1 for _ in csv.DictReader(handle))
    if count != expected:
        raise ValueError(f"{path.name} has {count} rows, expected {expected}")


def _csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        raise FileNotFoundError(path)
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _shortlist_ids(path: Path) -> list[str]:
    payload = _object(path)
    rows = payload.get("shortlist")
    if not isinstance(rows, list):
        rows = payload.get("configurations")
    if not isinstance(rows, list) or len(rows) != 10:
        raise ValueError(f"{path}: shortlist must contain exactly 10 rows")
    ids = [
        str(row.get("config_id", "")).strip()
        for row in rows
        if isinstance(row, dict)
    ]
    if len(ids) != 10 or not all(ids) or len(set(ids)) != 10:
        raise ValueError(f"{path}: shortlist IDs must be non-empty and unique")
    return ids


def _manifest_workload_id(row: dict[str, str]) -> str:
    for field in ("workload_config_id", "anchor_id", "anchor"):
        value = str(row.get(field, "")).strip()
        if value:
            return value
    case_id = str(row.get("case_id", "")).strip()
    config_id = str(row.get("config_id", "")).strip()
    return config_id if config_id and config_id != case_id else ""


def _validate_randomized_blocks(
    rows: list[dict[str, str]], shortlist_ids: list[str]
) -> None:
    blocks: dict[int, list[tuple[int, str]]] = {}
    for row in rows:
        try:
            block = int(row.get("block", ""))
            order = int(row.get("order", ""))
        except ValueError as exc:
            raise ValueError("validation block/order values must be integers") from exc
        blocks.setdefault(block, []).append((order, _manifest_workload_id(row)))
    if set(blocks) != {1, 2, 3, 4, 5}:
        raise ValueError("validation manifest must contain randomized blocks 1 through 5")
    for block, members in blocks.items():
        if len(members) != 10:
            raise ValueError(f"validation block {block} must contain exactly 10 rows")
        if {order for order, _ in members} != set(range(1, 11)):
            raise ValueError(f"validation block {block} order must be 1 through 10")
        if {config_id for _, config_id in members} != set(shortlist_ids):
            raise ValueError(
                f"validation block {block} must contain every shortlisted workload once"
            )


def _source_case_index(
    phase1: list[dict[str, Any]], repeats: list[dict[str, Any]]
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for stage, cases in (("phase1", phase1), ("validation", repeats)):
        for case in cases:
            case_id = str(case.get("case_id", "")).strip()
            source = str(case.get("source_report", "")).strip()
            digest = str(case.get("source_report_sha256", "")).strip()
            try:
                digest_is_hex = len(digest) == 64 and int(digest, 16) >= 0
            except ValueError:
                digest_is_hex = False
            if not case_id or not source or not digest_is_hex:
                raise ValueError(
                    f"{stage} case {case_id or '<unknown>'} lacks source-report provenance"
                )
            rows.append(
                {
                    "stage": stage,
                    "case_id": case_id,
                    "source_report": source,
                    "source_report_sha256": digest,
                }
            )
    return rows


def _object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _array(path: Path) -> list[dict[str, Any]]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, list) or not all(isinstance(row, dict) for row in value):
        raise ValueError(f"expected JSON array of objects: {path}")
    return value


def _cases(path: Path) -> list[dict[str, Any]]:
    rows = _object(path).get("cases")
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError(f"expected cases array: {path}")
    return rows


def _copy(source: Path, destination: Path) -> None:
    if not source.is_file():
        raise FileNotFoundError(source)
    shutil.copy2(source, destination)


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _write_csv(
    path: Path, fieldnames: tuple[str, ...], rows: list[dict[str, Any]]
) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_artifact_manifest(output: Path) -> None:
    manifest_path = output / "artifact_manifest.csv"
    checksum_path = output / "SHA256SUMS"
    rows = []
    for path in sorted(output.rglob("*")):
        if not path.is_file() or path in {manifest_path, checksum_path}:
            continue
        rows.append(
            {
                "path": path.relative_to(output).as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
        )
    with manifest_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("path", "size_bytes", "sha256"))
        writer.writeheader()
        writer.writerows(rows)
    checksum_rows = [*rows, {
        "path": manifest_path.name,
        "size_bytes": manifest_path.stat().st_size,
        "sha256": _sha256(manifest_path),
    }]
    checksum_path.write_text(
        "".join(f"{row['sha256']}  {row['path']}\n" for row in checksum_rows),
        encoding="utf-8",
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
