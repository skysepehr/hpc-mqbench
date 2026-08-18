#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Iterable


SUMMARY_SCHEMA_VERSION = "messaging-benchmark.case-summary.v1"

CSV_FIELDS = (
    "case_id",
    "campaign_id",
    "status",
    "backend_id",
    "product_version",
    "profile_id",
    "profile_sha256",
    "qualification_policy_id",
    "eligible",
    "qualified",
    "payload_size_bytes",
    "producer_mib_per_sec",
    "consumer_mib_per_sec",
    "balanced_mib_per_sec",
    "producer_records_per_sec",
    "consumer_records_per_sec",
    "balanced_records_per_sec",
    "latency_valid",
    "latency_sample_count",
    "latency_p50_us",
    "latency_p95_us",
    "latency_p99_us",
    "latency_p99_9_us",
    "records_offered",
    "records_published",
    "records_consumed",
    "records_missing",
    "records_duplicate",
    "records_out_of_order",
    "warmup_sec",
    "measurement_sec",
    "drain_timeout_sec",
    "source_report",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build portable case-summary artifacts from versioned benchmark "
            "final_report.json files."
        )
    )
    parser.add_argument("--backend", required=True)
    parser.add_argument("--results-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    backend_id = args.backend.strip().lower()
    if not backend_id:
        raise SystemExit("--backend must not be empty")
    if not args.results_root.exists():
        raise SystemExit(f"results root does not exist: {args.results_root}")

    rows, skipped = collect_rows(args.results_root, backend_id)
    if not rows:
        raise SystemExit(
            f"no versioned {backend_id!r} final_report.json files were found "
            f"under {args.results_root}"
        )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{backend_id}_case_summary"
    write_csv(args.output_dir / f"{stem}.csv", rows)
    write_json(
        args.output_dir / f"{stem}.json",
        backend_id=backend_id,
        rows=rows,
        skipped=skipped,
    )
    write_markdown(
        args.output_dir / f"{stem}.md",
        backend_id=backend_id,
        rows=rows,
        skipped=skipped,
    )
    print(f"[analyze] Backend: {backend_id}")
    print(f"[analyze] Cases: {len(rows)}")
    print(f"[analyze] Output: {args.output_dir}")
    return 0


def collect_rows(
    results_root: Path,
    backend_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    candidates = (
        [results_root]
        if results_root.is_file()
        else sorted(results_root.rglob("final_report.json"))
    )
    selected: dict[tuple[str, str], tuple[Path, dict[str, Any]]] = {}
    skipped: list[dict[str, str]] = []

    for path in candidates:
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            skipped.append({"source_report": path.name, "reason": str(exc)})
            continue
        if not isinstance(report, dict):
            skipped.append(
                {"source_report": path.name, "reason": "top level is not an object"}
            )
            continue
        observed_backend = _backend_id(report)
        if observed_backend != backend_id:
            continue
        common = _dict(report.get("common_metrics"))
        if not common:
            skipped.append(
                {
                    "source_report": _relative_source(path, results_root),
                    "reason": "missing common_metrics versioned result namespace",
                }
            )
            continue
        case = _dict(report.get("case"))
        case_id = str(case.get("case_id") or path.parent.name)
        campaign_id = str(case.get("campaign_id") or "")
        key = (campaign_id, case_id)
        current = selected.get(key)
        if current is None or _preferred_path(path, current[0]):
            selected[key] = (path, report)

    rows = [
        _row_from_report(report, path, results_root, backend_id)
        for path, report in selected.values()
    ]
    rows.sort(key=lambda row: (str(row["campaign_id"]), str(row["case_id"])))
    return rows, skipped


def _row_from_report(
    report: dict[str, Any],
    path: Path,
    results_root: Path,
    backend_id: str,
) -> dict[str, Any]:
    case = _dict(report.get("case"))
    config = _dict(report.get("config"))
    system = _dict(report.get("system_under_test"))
    common = _dict(report.get("common_metrics"))
    records = _dict(common.get("records"))
    throughput = _dict(common.get("throughput"))
    latency = _dict(common.get("latency_end_to_end"))
    timing = _dict(common.get("timing"))
    qualification = _dict(common.get("qualification"))
    return {
        "case_id": str(case.get("case_id") or path.parent.name),
        "campaign_id": str(case.get("campaign_id") or ""),
        "status": case.get("status"),
        "backend_id": backend_id,
        "product_version": system.get("product_version"),
        "profile_id": system.get("profile_id"),
        "profile_sha256": system.get("profile_sha256"),
        "qualification_policy_id": qualification.get("policy_id"),
        "eligible": qualification.get("eligible"),
        "qualified": qualification.get("qualified"),
        "payload_size_bytes": config.get("payload_size_bytes"),
        "producer_mib_per_sec": throughput.get("producer_mib_per_sec"),
        "consumer_mib_per_sec": throughput.get("consumer_mib_per_sec"),
        "balanced_mib_per_sec": throughput.get("balanced_mib_per_sec"),
        "producer_records_per_sec": throughput.get("producer_records_per_sec"),
        "consumer_records_per_sec": throughput.get("consumer_records_per_sec"),
        "balanced_records_per_sec": throughput.get("balanced_records_per_sec"),
        "latency_valid": latency.get("valid"),
        "latency_sample_count": latency.get("count"),
        "latency_p50_us": latency.get("p50_us"),
        "latency_p95_us": latency.get("p95_us"),
        "latency_p99_us": latency.get("p99_us"),
        "latency_p99_9_us": latency.get("p99_9_us"),
        "records_offered": records.get("offered"),
        "records_published": records.get("published"),
        "records_consumed": records.get("consumed"),
        "records_missing": records.get("missing"),
        "records_duplicate": records.get("duplicate"),
        "records_out_of_order": records.get("out_of_order"),
        "warmup_sec": timing.get("warmup_sec"),
        "measurement_sec": timing.get("measurement_sec"),
        "drain_timeout_sec": timing.get("drain_timeout_sec"),
        "source_report": _relative_source(path, results_root),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def write_json(
    path: Path,
    *,
    backend_id: str,
    rows: list[dict[str, Any]],
    skipped: list[dict[str, str]],
) -> None:
    payload = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "backend_id": backend_id,
        "case_count": len(rows),
        "skipped_report_count": len(skipped),
        "skipped_reports": skipped,
        "cases": rows,
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_markdown(
    path: Path,
    *,
    backend_id: str,
    rows: list[dict[str, Any]],
    skipped: list[dict[str, str]],
) -> None:
    qualified = sum(row.get("qualified") is True for row in rows)
    eligible = sum(row.get("eligible") is True for row in rows)
    lines = [
        f"# {backend_id.capitalize()} Case Summary",
        "",
        f"- Cases: {len(rows)}",
        f"- Eligible: {eligible}",
        f"- Qualified: {qualified}",
        f"- Skipped malformed or unversioned reports: {len(skipped)}",
        "",
        (
            "Throughput uses the portable common result namespace. MiB/s is "
            "bytes per second divided by 1,048,576; balanced throughput is the "
            "minimum of producer and consumer throughput. Latency is end-to-end "
            "producer-to-consumer latency and is reported only when instrumented."
        ),
        "",
        "| Case | Status | Eligible | Qualified | Balanced MiB/s | Balanced records/s | p99 (us) | Missing | Profile |",
        "|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        lines.append(
            "| {case} | {status} | {eligible} | {qualified} | {mib} | {rps} | "
            "{p99} | {missing} | {profile} |".format(
                case=_md(row.get("case_id")),
                status=_md(row.get("status")),
                eligible=_display_bool(row.get("eligible")),
                qualified=_display_bool(row.get("qualified")),
                mib=_number(row.get("balanced_mib_per_sec"), 3),
                rps=_number(row.get("balanced_records_per_sec"), 0),
                p99=_number(row.get("latency_p99_us"), 3),
                missing=_number(row.get("records_missing"), 0),
                profile=_md(row.get("profile_id")),
            )
        )
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def _backend_id(report: dict[str, Any]) -> str:
    system = _dict(report.get("system_under_test"))
    config = _dict(report.get("config"))
    return str(system.get("backend_id") or config.get("backend_id") or "").lower()


def _preferred_path(candidate: Path, current: Path) -> bool:
    candidate_data = candidate.parent.name == "data"
    current_data = current.parent.name == "data"
    if candidate_data != current_data:
        return not candidate_data
    return len(candidate.parts) < len(current.parts)


def _relative_source(path: Path, root: Path) -> str:
    if root.is_file():
        return root.name
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.name


def _number(value: Any, digits: int) -> str:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return "N/A"
    if digits == 0:
        return f"{float(value):,.0f}"
    return f"{float(value):,.{digits}f}"


def _display_bool(value: Any) -> str:
    if value is True:
        return "yes"
    if value is False:
        return "no"
    return "N/A"


def _md(value: Any) -> str:
    return str(value if value not in (None, "") else "N/A").replace("|", "\\|")


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


if __name__ == "__main__":
    raise SystemExit(main())
