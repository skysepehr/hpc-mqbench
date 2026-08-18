#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
import sys
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

import analyze_pulsar_phase2_screening as screening  # noqa: E402


DEFAULT_SCREENING_CASES = (
    PROJECT_ROOT
    / "results"
    / "rebuilt"
    / "pulsar"
    / "phase2-screening"
    / "pulsar_phase2_screening_cases.json"
)
DEFAULT_SELECTION = DEFAULT_SCREENING_CASES.with_name(
    "pulsar_phase2_candidate_selection.json"
)
DEFAULT_CONFIRMATION_ROOT = (
    PROJECT_ROOT / "configs" / "campaigns" / "pulsar" / "phase2" / "confirmation"
)
DEFAULT_MANIFEST = DEFAULT_CONFIRMATION_ROOT / "memory_confirmation_manifest.csv"
DEFAULT_PLAN = DEFAULT_CONFIRMATION_ROOT / "confirmation_plan.json"
DEFAULT_OUTPUT = PROJECT_ROOT / "results" / "rebuilt" / "pulsar" / "phase2"
BASELINE_PROFILE = "BASELINE_H16_D32"
SUSTAINABLE_ANCHORS = screening.SUSTAINABLE_ANCHORS
TRANSITION_ANCHOR = screening.TRANSITION_ANCHOR

CELL_FIELDS = (
    "profile_id",
    "anchor",
    "anchor_purpose",
    "heap_gib",
    "direct_memory_gib",
    "repeat_count",
    "eligible_repeats",
    "qualified_repeats",
    "median_balanced_mib_per_sec",
    "balanced_mib_per_sec_iqr",
    "median_balanced_records_per_sec",
    "balanced_records_per_sec_iqr",
    "median_latency_p99_us",
    "latency_p99_us_iqr",
    "median_producer_backlog_percent",
    "max_producer_backlog_percent",
    "median_flush_sec",
    "max_failed_send_percent",
    "max_process_rss_gib",
    "max_jvm_heap_gib",
    "max_jvm_direct_nio_gib",
    "max_managed_ledger_direct_pool_allocated_gib",
    "max_managed_ledger_direct_pool_used_gib",
    "max_jvm_gc_time_rate",
    "max_tmpfs_used_percent",
    "min_post_reset_tmpfs_free_percent",
)

PROFILE_FIELDS = (
    "final_rank",
    "frozen_winner",
    "candidate_passed",
    "profile_id",
    "heap_gib",
    "direct_memory_gib",
    "repeat_count",
    "eligible_repeat_count",
    "qualified_repeat_count",
    "sustainable_qualified_repeat_count",
    "all_required_valid",
    "geometric_mean_median_throughput_ratio_to_baseline",
    "worst_anchor_median_throughput_ratio_to_baseline",
    "geometric_mean_median_p99_latency_ratio_to_baseline",
    "max_anchor_throughput_iqr_percent",
    "max_producer_backlog_percent",
    "max_failed_send_percent",
    "max_flush_sec",
    "max_process_rss_gib",
    "max_jvm_heap_gib",
    "max_jvm_direct_nio_gib",
    "max_managed_ledger_direct_pool_allocated_gib",
    "max_managed_ledger_direct_pool_used_gib",
    "max_jvm_gc_time_rate",
    "max_tmpfs_used_percent",
    "min_post_reset_tmpfs_free_percent",
    "decision_reasons",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze repeated Pulsar Phase 2 memory confirmation and apply the "
            "predeclared profile-freeze rule."
        )
    )
    parser.add_argument("--screening-cases", type=Path, default=DEFAULT_SCREENING_CASES)
    parser.add_argument("--screening-selection", type=Path, default=DEFAULT_SELECTION)
    parser.add_argument("--confirmation-results-root", type=Path, required=True)
    parser.add_argument("--confirmation-manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--confirmation-plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--machine-only",
        action="store_true",
        help="Write only validated JSON/CSV and checksums.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    screening_cases_payload = _read_json(args.screening_cases)
    selection = _read_json(args.screening_selection)
    confirmation_manifest = screening._read_manifest(args.confirmation_manifest)
    confirmation_plan = _read_json(args.confirmation_plan)
    reports, duplicates = screening._discover_reports(args.confirmation_results_root)
    validation, confirmation_rows = _collect_confirmation(
        manifest=confirmation_manifest,
        plan=confirmation_plan,
        selection=selection,
        reports=reports,
        duplicates=duplicates,
        results_root=args.confirmation_results_root,
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    validation_path = args.output_dir / "pulsar_phase2_confirmation_validation.json"
    _write_json(validation_path, validation)
    if not validation["valid"]:
        if not args.machine_only:
            screening._write_incomplete_report(
                args.output_dir / "pulsar_phase2_report.md", validation
            )
        print("[pulsar-phase2-confirmation] analysis incomplete")
        for reason in validation["failure_reasons"]:
            print(f"[pulsar-phase2-confirmation] {reason}")
        return 1

    confirmed_profiles = [str(item) for item in selection["confirmation_profiles"]]
    screening_rows = [
        dict(row, block=1, repeat_index=1, stage="phase2-memory-screening")
        for row in screening_cases_payload.get("cases", [])
        if row.get("profile_id") in confirmed_profiles
    ]
    combined_rows = sorted(
        [*screening_rows, *confirmation_rows],
        key=lambda row: (row["profile_id"], row["anchor"], row["block"]),
    )
    combined_failures = _validate_combined_repeats(combined_rows, confirmed_profiles)
    if combined_failures:
        validation["valid"] = False
        validation["failure_reasons"].extend(combined_failures)
        _write_json(validation_path, validation)
        if not args.machine_only:
            screening._write_incomplete_report(
                args.output_dir / "pulsar_phase2_report.md", validation
            )
        return 1

    cells = _cell_summaries(combined_rows)
    profiles, final_selection = _final_profile_selection(cells, combined_rows)
    final_selection["source_evidence"] = {
        "screening_cases_sha256": _sha256(args.screening_cases),
        "screening_selection_sha256": _sha256(args.screening_selection),
        "confirmation_manifest_sha256": _sha256(args.confirmation_manifest),
        "confirmation_plan_sha256": _sha256(args.confirmation_plan),
        "confirmation_validation_sha256": _sha256(validation_path),
        "confirmation_results_root": args.confirmation_results_root.name,
        "slurm_job_ids": screening._slurm_job_ids(
            args.confirmation_results_root
        ),
    }

    case_fields = (
        "block",
        "repeat_index",
        "stage",
        *screening.CASE_FIELDS,
    )
    screening._write_csv(
        args.output_dir / "pulsar_phase2_confirmed_cases.csv",
        case_fields,
        combined_rows,
    )
    _write_json(
        args.output_dir / "pulsar_phase2_confirmed_cases.json",
        {
            "format": "messaging-benchmark.pulsar-phase2-confirmed-cases.v1",
            "case_count": len(combined_rows),
            "backlog_denominator": screening.BACKLOG_DENOMINATOR_ATTEMPTED,
            "backlog_denominator_scope": "measurement-period publication attempts",
            "cases": combined_rows,
        },
    )
    screening._write_csv(
        args.output_dir / "pulsar_phase2_repeated_cells.csv", CELL_FIELDS, cells
    )
    screening._write_csv(
        args.output_dir / "pulsar_phase2_confirmed_profiles.csv",
        PROFILE_FIELDS,
        profiles,
    )
    _write_json(args.output_dir / "pulsar_phase2_final_selection.json", final_selection)
    if not args.machine_only:
        _write_markdown(
            args.output_dir / "pulsar_phase2_report.md",
            cells,
            profiles,
            final_selection,
            validation,
        )
    screening._write_checksums(args.output_dir)
    print(f"[pulsar-phase2-confirmation] repeated cases: {len(combined_rows)}")
    print(
        "[pulsar-phase2-confirmation] frozen profile: "
        f"{final_selection['frozen_profile_id']}"
    )
    print(f"[pulsar-phase2-confirmation] output: {args.output_dir}")
    return 0


def _collect_confirmation(
    *,
    manifest: list[dict[str, str]],
    plan: dict[str, Any],
    selection: dict[str, Any],
    reports: dict[str, tuple[Path, dict[str, Any]]],
    duplicates: list[str],
    results_root: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    failures: list[str] = []
    expected_profiles = {
        str(item["profile_id"]): str(item["profile_sha256"])
        for item in plan.get("profiles", [])
        if isinstance(item, dict)
    }
    expected_ids = [row["case_id"] for row in manifest]
    if len(manifest) != 30:
        failures.append(f"confirmation manifest has {len(manifest)} rows, expected 30")
    if len(set(expected_ids)) != 30:
        failures.append("confirmation manifest case IDs are not unique")
    if set(expected_profiles) != set(selection.get("confirmation_profiles", [])):
        failures.append("confirmation profile set differs from screening selection")
    if duplicates:
        failures.append("duplicate top-level reports: " + ", ".join(duplicates))
    rows: list[dict[str, Any]] = []
    missing: list[str] = []
    repeat_counter: dict[tuple[str, str], int] = {}
    for manifest_row in manifest:
        case_id = manifest_row["case_id"]
        selected = reports.get(case_id)
        if selected is None:
            missing.append(case_id)
            continue
        path, report = selected
        row = screening._case_row(manifest_row, report, path, results_root)
        row["block"] = int(manifest_row["block"])
        key = (row["profile_id"], row["anchor"])
        repeat_counter[key] = repeat_counter.get(key, 1) + 1
        row["repeat_index"] = repeat_counter[key]
        row["stage"] = "phase2-memory-confirmation"
        rows.append(row)
        reasons = _case_validation_reasons(
            row,
            manifest_row,
            expected_profiles.get(row["profile_id"]),
        )
        failures.extend(f"{case_id}: {reason}" for reason in reasons)
    if missing:
        failures.append(
            f"missing {len(missing)} confirmation report(s): " + ", ".join(missing)
        )
    validation = {
        "format": "messaging-benchmark.pulsar-phase2-confirmation-validation.v1",
        "valid": not failures,
        "backlog_denominator": screening.BACKLOG_DENOMINATOR_ATTEMPTED,
        "backlog_denominator_scope": "measurement-period publication attempts",
        "expected_case_count": 30,
        "observed_case_count": len(rows),
        "eligible_case_count": sum(bool(row["eligible"]) for row in rows),
        "qualified_case_count": sum(bool(row["qualified"]) for row in rows),
        "results_root": results_root.name,
        "extra_case_ids": sorted(set(reports) - set(expected_ids)),
        "failure_reasons": list(dict.fromkeys(failures)),
    }
    return validation, rows


def _case_validation_reasons(
    row: dict[str, Any], manifest: dict[str, str], expected_sha: str | None
) -> list[str]:
    reasons: list[str] = []
    checks = (
        (row["status"] == "completed", "status is not completed"),
        (row["backend_health"] == "healthy", "backend is not healthy"),
        (row["profile_id"] == manifest["profile_id"], "profile ID drift"),
        (row["profile_sha256"] == manifest["profile_sha256"], "manifest profile checksum drift"),
        (row["profile_sha256"] == expected_sha, "campaign profile checksum drift"),
        (
            not row["eligible"] or row["latency_valid"],
            "eligible case has invalid latency",
        ),
        (
            not row["eligible"] or row["records_missing_after_drain"] == 0,
            "eligible case has missing records",
        ),
        (
            not row["eligible"] or row["duplicate_records"] == 0,
            "eligible case has duplicate records",
        ),
        (
            not row["eligible"] or row["out_of_order_records"] == 0,
            "eligible case has out-of-order records",
        ),
        (
            row["records_delivered"]
            == row["records_consumed"]
            + row["records_late_drained"]
            + row["records_missing_after_drain"],
            "record accounting does not balance",
        ),
        (row["balanced_mib_per_sec"] is not None, "balanced throughput is missing"),
        (row["balanced_records_per_sec"] is not None, "balanced record rate is missing"),
        (row["tmpfs_used_peak_percent"] is not None, "tmpfs peak evidence is missing"),
        (row["post_reset_tmpfs_free_percent"] is not None, "post-reset tmpfs evidence is missing"),
    )
    reasons.extend(reason for passed, reason in checks if not passed)
    expected_qualified = (
        row["eligible"]
        and row["producer_backlog_percent"] <= screening.BACKLOG_LIMIT_PERCENT
        and row["max_flush_duration_sec"] <= screening.FLUSH_LIMIT_SEC
        and row["failed_send_percent"] <= screening.FAILED_SEND_LIMIT_PERCENT
    )
    if row["qualified"] is not expected_qualified:
        reasons.append("qualification result does not match policy")
    return reasons


def _validate_combined_repeats(
    rows: list[dict[str, Any]], profiles: list[str]
) -> list[str]:
    failures: list[str] = []
    if len(rows) != 45:
        failures.append(f"combined repeated dataset has {len(rows)} rows, expected 45")
    for profile_id in profiles:
        for anchor in (*SUSTAINABLE_ANCHORS, TRANSITION_ANCHOR):
            cell = [
                row
                for row in rows
                if row["profile_id"] == profile_id and row["anchor"] == anchor
            ]
            if len(cell) != 3:
                failures.append(f"{profile_id}/{anchor}: expected 3 repeats, found {len(cell)}")
            elif {row["block"] for row in cell} != {1, 2, 3}:
                failures.append(f"{profile_id}/{anchor}: blocks 1, 2, and 3 are not all present")
    return failures


def _cell_summaries(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    keys = sorted({(row["profile_id"], row["anchor"]) for row in rows})
    for profile_id, anchor in keys:
        items = [
            row for row in rows if row["profile_id"] == profile_id and row["anchor"] == anchor
        ]
        mib = [row["balanced_mib_per_sec"] for row in items]
        records = [row["balanced_records_per_sec"] for row in items]
        p99 = [row["latency_p99_us"] for row in items]
        output.append(
            {
                "profile_id": profile_id,
                "anchor": anchor,
                "anchor_purpose": items[0]["anchor_purpose"],
                "heap_gib": items[0]["heap_gib"],
                "direct_memory_gib": items[0]["direct_memory_gib"],
                "repeat_count": len(items),
                "eligible_repeats": sum(row["eligible"] for row in items),
                "qualified_repeats": sum(row["qualified"] for row in items),
                "median_balanced_mib_per_sec": statistics.median(mib),
                "balanced_mib_per_sec_iqr": _iqr(mib),
                "median_balanced_records_per_sec": statistics.median(records),
                "balanced_records_per_sec_iqr": _iqr(records),
                "median_latency_p99_us": statistics.median(p99),
                "latency_p99_us_iqr": _iqr(p99),
                "median_producer_backlog_percent": statistics.median(
                    row["producer_backlog_percent"] for row in items
                ),
                "max_producer_backlog_percent": max(
                    row["producer_backlog_percent"] for row in items
                ),
                "median_flush_sec": statistics.median(
                    row["max_flush_duration_sec"] for row in items
                ),
                "max_failed_send_percent": max(row["failed_send_percent"] for row in items),
                "max_process_rss_gib": _max_optional(row["pulsar_process_rss_peak_gib"] for row in items),
                "max_jvm_heap_gib": _max_optional(row["pulsar_jvm_heap_peak_gib"] for row in items),
                "max_jvm_direct_nio_gib": _max_optional(row["pulsar_jvm_direct_nio_peak_gib"] for row in items),
                "max_managed_ledger_direct_pool_allocated_gib": _max_optional(
                    row["pulsar_managed_ledger_direct_pool_allocated_peak_gib"] for row in items
                ),
                "max_managed_ledger_direct_pool_used_gib": _max_optional(
                    row["pulsar_managed_ledger_direct_pool_used_peak_gib"] for row in items
                ),
                "max_jvm_gc_time_rate": _max_optional(row["pulsar_jvm_gc_time_rate_peak"] for row in items),
                "max_tmpfs_used_percent": max(row["tmpfs_used_peak_percent"] for row in items),
                "min_post_reset_tmpfs_free_percent": min(
                    row["post_reset_tmpfs_free_percent"] for row in items
                ),
            }
        )
    return output


def _final_profile_selection(
    cells: list[dict[str, Any]], rows: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    by_cell = {(row["profile_id"], row["anchor"]): row for row in cells}
    profile_ids = sorted({row["profile_id"] for row in cells})
    baseline_cells = {anchor: by_cell[(BASELINE_PROFILE, anchor)] for anchor in SUSTAINABLE_ANCHORS}
    baseline_rows = [row for row in rows if row["profile_id"] == BASELINE_PROFILE]
    baseline_qualified = sum(
        row["qualified"] for row in baseline_rows if row["anchor"] in SUSTAINABLE_ANCHORS
    )
    baseline_max_failed = max(row["failed_send_percent"] for row in baseline_rows)
    records: list[dict[str, Any]] = []
    for profile_id in profile_ids:
        profile_cells = [row for row in cells if row["profile_id"] == profile_id]
        profile_rows = [row for row in rows if row["profile_id"] == profile_id]
        throughput_ratios = [
            by_cell[(profile_id, anchor)]["median_balanced_mib_per_sec"]
            / baseline_cells[anchor]["median_balanced_mib_per_sec"]
            for anchor in SUSTAINABLE_ANCHORS
        ]
        latency_ratios = [
            by_cell[(profile_id, anchor)]["median_latency_p99_us"]
            / baseline_cells[anchor]["median_latency_p99_us"]
            for anchor in SUSTAINABLE_ANCHORS
        ]
        sustainable_qualified = sum(
            row["qualified"]
            for row in profile_rows
            if row["anchor"] in SUSTAINABLE_ANCHORS
        )
        all_valid = (
            len(profile_rows) == 15
            and all(row["status"] == "completed" for row in profile_rows)
            and all(row["backend_health"] == "healthy" for row in profile_rows)
            and all(row["eligible"] for row in profile_rows)
            and all(row["latency_valid"] for row in profile_rows)
            and all(row["records_missing_after_drain"] == 0 for row in profile_rows)
            and all(row["duplicate_records"] == 0 for row in profile_rows)
            and all(row["out_of_order_records"] == 0 for row in profile_rows)
        )
        gm_throughput = _geometric_mean(throughput_ratios)
        gm_latency = _geometric_mean(latency_ratios)
        max_failed = max(row["failed_send_percent"] for row in profile_rows)
        max_tmpfs = max(row["tmpfs_used_peak_percent"] for row in profile_rows)
        reasons = []
        if profile_id != BASELINE_PROFILE:
            if not all_valid:
                reasons.append("not all 15 observations are scientifically valid")
            if sustainable_qualified < baseline_qualified:
                reasons.append(
                    f"sustainable qualified repeats {sustainable_qualified} < baseline {baseline_qualified}"
                )
            if gm_throughput < 1.03:
                reasons.append(f"throughput ratio {gm_throughput:.6f} < 1.03")
            if gm_latency > 1.10:
                reasons.append(f"p99 latency ratio {gm_latency:.6f} > 1.10")
            if max_failed > baseline_max_failed + 1e-12:
                reasons.append(
                    f"failed-send maximum {max_failed:.9f}% > baseline {baseline_max_failed:.9f}%"
                )
            if max_tmpfs >= 75.0:
                reasons.append(f"tmpfs peak {max_tmpfs:.3f}% is not below 75%")
        candidate_passed = profile_id != BASELINE_PROFILE and not reasons
        iqr_percentages = [
            100.0 * row["balanced_mib_per_sec_iqr"] / row["median_balanced_mib_per_sec"]
            for row in profile_cells
            if row["median_balanced_mib_per_sec"] > 0
        ]
        records.append(
            {
                "final_rank": None,
                "frozen_winner": False,
                "candidate_passed": candidate_passed,
                "profile_id": profile_id,
                "heap_gib": profile_cells[0]["heap_gib"],
                "direct_memory_gib": profile_cells[0]["direct_memory_gib"],
                "repeat_count": len(profile_rows),
                "eligible_repeat_count": sum(row["eligible"] for row in profile_rows),
                "qualified_repeat_count": sum(row["qualified"] for row in profile_rows),
                "sustainable_qualified_repeat_count": sustainable_qualified,
                "all_required_valid": all_valid,
                "geometric_mean_median_throughput_ratio_to_baseline": gm_throughput,
                "worst_anchor_median_throughput_ratio_to_baseline": min(throughput_ratios),
                "geometric_mean_median_p99_latency_ratio_to_baseline": gm_latency,
                "max_anchor_throughput_iqr_percent": max(iqr_percentages),
                "max_producer_backlog_percent": max(
                    row["producer_backlog_percent"] for row in profile_rows
                ),
                "max_failed_send_percent": max_failed,
                "max_flush_sec": max(row["max_flush_duration_sec"] for row in profile_rows),
                "max_process_rss_gib": _max_optional(row["pulsar_process_rss_peak_gib"] for row in profile_rows),
                "max_jvm_heap_gib": _max_optional(row["pulsar_jvm_heap_peak_gib"] for row in profile_rows),
                "max_jvm_direct_nio_gib": _max_optional(row["pulsar_jvm_direct_nio_peak_gib"] for row in profile_rows),
                "max_managed_ledger_direct_pool_allocated_gib": _max_optional(
                    row["pulsar_managed_ledger_direct_pool_allocated_peak_gib"] for row in profile_rows
                ),
                "max_managed_ledger_direct_pool_used_gib": _max_optional(
                    row["pulsar_managed_ledger_direct_pool_used_peak_gib"] for row in profile_rows
                ),
                "max_jvm_gc_time_rate": _max_optional(row["pulsar_jvm_gc_time_rate_peak"] for row in profile_rows),
                "max_tmpfs_used_percent": max_tmpfs,
                "min_post_reset_tmpfs_free_percent": min(
                    row["post_reset_tmpfs_free_percent"] for row in profile_rows
                ),
                "decision_reasons": (
                    "control profile; retained when no candidate passes"
                    if profile_id == BASELINE_PROFILE
                    else (
                        "; ".join(reasons)
                        if reasons
                        else "all candidate freeze rules passed"
                    )
                ),
            }
        )

    passing = sorted(
        [row for row in records if row["candidate_passed"]],
        key=lambda row: (
            -row["geometric_mean_median_throughput_ratio_to_baseline"],
            row["geometric_mean_median_p99_latency_ratio_to_baseline"],
            row["max_anchor_throughput_iqr_percent"],
            row["max_jvm_gc_time_rate"] if row["max_jvm_gc_time_rate"] is not None else math.inf,
            row["profile_id"],
        ),
    )
    winner_id = passing[0]["profile_id"] if passing else BASELINE_PROFILE
    ranked = sorted(
        records,
        key=lambda row: (
            row["profile_id"] != winner_id,
            not row["candidate_passed"],
            -row["sustainable_qualified_repeat_count"],
            -row["geometric_mean_median_throughput_ratio_to_baseline"],
            row["geometric_mean_median_p99_latency_ratio_to_baseline"],
            row["profile_id"],
        ),
    )
    for rank, row in enumerate(ranked, start=1):
        row["final_rank"] = rank
        row["frozen_winner"] = row["profile_id"] == winner_id
    selection = {
        "format": "messaging-benchmark.pulsar-phase2-final-selection.v1",
        "status": "profile_frozen",
        "backlog_denominator": screening.BACKLOG_DENOMINATOR_ATTEMPTED,
        "backlog_denominator_scope": "measurement-period publication attempts",
        "frozen_profile_id": winner_id,
        "baseline_retained": winner_id == BASELINE_PROFILE,
        "passing_candidate_profiles": [row["profile_id"] for row in passing],
        "observations_per_profile_anchor": 3,
        "sustainable_anchor_count": len(SUSTAINABLE_ANCHORS),
        "rules": {
            "all_required_valid": True,
            "qualified_repeats_not_below_baseline": True,
            "minimum_geometric_mean_median_throughput_ratio": 1.03,
            "maximum_geometric_mean_median_p99_latency_ratio": 1.10,
            "failed_send_regression_allowed": False,
            "correctness_regression_allowed": False,
            "maximum_tmpfs_used_percent_exclusive": 75.0,
        },
        "interpretation": (
            f"{winner_id} is the frozen memory profile for this single-standalone "
            "GWDG environment under the historically executed anchors. The profile "
            "decision was recomputed with the shared Kafka/Pulsar application-level "
            "qualification rule; it does "
            "not imply that the retrospectively corrected Phase 1 shortlist was run. "
            "The result is not a universal Pulsar optimum and does not compare Pulsar "
            "with Kafka."
        ),
        "profiles": ranked,
    }
    return ranked, selection


def _write_markdown(
    path: Path,
    cells: list[dict[str, Any]],
    profiles: list[dict[str, Any]],
    selection: dict[str, Any],
    validation: dict[str, Any],
) -> None:
    lines = [
        "# Pulsar Phase 2 Repeated Memory-Profile Validation",
        "",
        f"All {validation['observed_case_count']} confirmation cases validated. "
        "Together with screening block 1, each confirmed profile/workload cell "
        "has three observations.",
        "",
        "Qualification uses producer backlog <= 5%, maximum flush <= 10 s, and "
        "failed sends <= 0.1%. Missing, duplicate, and out-of-order records remain "
        "eligibility/correctness checks. The five anchors are the historical Phase 2 "
        "execution set, not the retrospectively corrected Phase 1 shortlist.",
        "",
        f"**Frozen profile:** `{selection['frozen_profile_id']}`",
        "",
        "| Rank | Profile | Heap/direct GiB | Qualified sustainable repeats | Throughput ratio | p99 ratio | Max IQR % | Pass |",
        "|---:|---|---:|---:|---:|---:|---:|:---:|",
    ]
    for row in profiles:
        lines.append(
            f"| {row['final_rank']} | `{row['profile_id']}` | "
            f"{row['heap_gib']}/{row['direct_memory_gib']} | "
            f"{row['sustainable_qualified_repeat_count']}/12 | "
            f"{row['geometric_mean_median_throughput_ratio_to_baseline']:.3f} | "
            f"{row['geometric_mean_median_p99_latency_ratio_to_baseline']:.3f} | "
            f"{row['max_anchor_throughput_iqr_percent']:.3f} | "
            f"{'yes' if row['candidate_passed'] else 'no'} |"
        )
    lines.extend(
        [
            "",
            "## Repeated Cells",
            "",
            "| Profile | Anchor | Qualified | Median MiB/s | IQR MiB/s | Median p99 ms | Median backlog % | Max RSS GiB | Max tmpfs % |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in cells:
        lines.append(
            f"| `{row['profile_id']}` | `{row['anchor']}` | "
            f"{row['qualified_repeats']}/3 | "
            f"{row['median_balanced_mib_per_sec']:.3f} | "
            f"{row['balanced_mib_per_sec_iqr']:.3f} | "
            f"{row['median_latency_p99_us'] / 1000.0:.3f} | "
            f"{row['median_producer_backlog_percent']:.3f} | "
            f"{_fmt(row['max_process_rss_gib'])} | "
            f"{row['max_tmpfs_used_percent']:.3f} |"
        )
    lines.extend(["", selection["interpretation"], ""])
    path.write_text("\n".join(lines), encoding="utf-8")


def _iqr(values: Iterable[float]) -> float:
    ordered = sorted(float(value) for value in values)
    return _quantile(ordered, 0.75) - _quantile(ordered, 0.25)


def _quantile(ordered: list[float], probability: float) -> float:
    if not ordered:
        raise ValueError("quantile requires values")
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (position - lower) * (ordered[upper] - ordered[lower])


def _geometric_mean(values: Iterable[float]) -> float:
    clean = [float(value) for value in values]
    if not clean or any(value <= 0 for value in clean):
        raise ValueError("geometric mean requires positive values")
    return math.exp(sum(math.log(value) for value in clean) / len(clean))


def _max_optional(values: Iterable[float | None]) -> float | None:
    clean = [float(value) for value in values if value is not None]
    return max(clean) if clean else None


def _fmt(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.3f}"


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
