#!/usr/bin/env python3
"""Derive paper-review tables from retained evidence; never run a benchmark.

The generated supplement belongs to this paper, not to the benchmark's runtime
output contract. --check compares files with fresh calculations without writing.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import math
from pathlib import Path
import random
import re
from statistics import median, quantiles

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data/kafka/source"


def read_csv(path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def csv_text(rows):
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=list(rows[0]), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def derive():
    screening = read_csv(ROOT / "data/audit/phase1_cases.csv")
    eligible = [row for row in screening if row["eligible"].lower() == "true"]
    policies = [("pending_percent", p, 10, 0.1) for p in (2, 5, 10)]
    policies += [("flush_seconds", 5, f, 0.1) for f in (1, 3, 5, 10, 30)]
    policies += [("failed_send_percent", 5, 10, f) for f in (0, 0.01, 0.1, 1)]
    sensitivity = []
    for varied, pending, flush, failed in policies:
        qualified = [row for row in eligible
                     if 100 * int(row["pending_messages_at_flush_start"]) / int(row["messages_attempted"]) <= pending
                     and float(row["flush_sec"]) <= flush
                     and 100 * int(row["messages_failed"]) / int(row["messages_attempted"]) <= failed]
        best = max(qualified, key=lambda row: float(row["balanced_mib_per_sec"]))
        sensitivity.append(dict(varied_limit=varied, pending_limit_percent=pending,
                                flush_limit_seconds=flush, failed_send_limit_percent=failed,
                                eligible=len(eligible), qualified=len(qualified),
                                overdriven=len(eligible)-len(qualified),
                                highest_qualified_config=best["config_id"],
                                highest_balanced_endpoint_mibps=float(best["balanced_mib_per_sec"])))

    cases = json.loads((SOURCE / "analysis/v2/complete-profile/kafka_broker_tuning_v2_cases.json").read_text())
    confirmed = [row for row in cases if row["stage"] in ("profile_screening", "profile_confirmation")
                 and row["broker_profile_id"] in ("B0", "B3", "B5")]
    anchors = ("cfg_107", "cfg_061", "cfg_094", "cfg_101", "latency_anchor")
    profiles = []
    for anchor in anchors:
        baseline = [row for row in confirmed if row["anchor_id"] == anchor and row["broker_profile_id"] == "B0"]
        base_rate = median(row["balanced_mibps"] for row in baseline if row["eligible"])
        base_p99 = median(row["latency_p99_ms"] for row in baseline if row["eligible"])
        for profile in ("B0", "B3", "B5"):
            group = [row for row in confirmed if row["anchor_id"] == anchor and row["broker_profile_id"] == profile]
            assert len(group) == 3 and all(row["eligible"] for row in group)
            rate = median(row["balanced_mibps"] for row in group)
            p99 = median(row["latency_p99_ms"] for row in group)
            profiles.append(dict(anchor_id=anchor, broker_profile=profile,
                                 included_in_rate_geometric_mean=anchor != "latency_anchor",
                                 observations=len(group), eligible=sum(row["eligible"] for row in group),
                                 qualified=sum(row["qualified"] for row in group),
                                 median_balanced_endpoint_mibps=rate, matching_B0_median_mibps=base_rate,
                                 rate_ratio_to_B0=rate/base_rate, median_p99_ms=p99,
                                 matching_B0_median_p99_ms=base_p99, p99_ratio_to_B0=p99/base_p99))
    decision = json.loads((SOURCE / "analysis/v2/complete-profile/kafka_broker_tuning_v2_decisions.json").read_text())
    ratios = {profile: math.prod(row["rate_ratio_to_B0"] for row in profiles
                                if row["broker_profile"] == profile and row["included_in_rate_geometric_mean"]) ** 0.25
              for profile in ("B0", "B3", "B5")}

    shortlist = json.loads((SOURCE / "analysis/v1/phase1/validation_shortlist.json").read_text())["shortlist"]
    ids = [row["config_id"] for row in sorted(shortlist, key=lambda row: row["selection_order"])]
    allocations = []
    all_orders = []
    for stage, manifest_key in (("v1", "workload_config_id"), ("v2", "anchor_id")):
        manifest = read_csv(SOURCE / f"artifacts/{stage}/validation_manifest.csv")
        audit = read_csv(ROOT / f"data/audit/{stage}_validation_repeats.csv")
        jobs = {row["job_id"] for row in read_csv(SOURCE / "artifacts/slurm_job_ids.csv")
                if row["stage"] == ("v1-validation-submit" if stage == "v1" else "v2-final-submit")}
        rng = random.Random(20260728)
        orders = []
        for block in range(1, 6):
            expected = ids.copy()
            rng.shuffle(expected)
            rows = sorted([row for row in manifest if int(row["block"]) == block], key=lambda row: int(row["order"]))
            assert [row[manifest_key] for row in rows] == expected
            orders.append(expected)
            for row in rows:
                retained = next(item for item in audit if int(item["block"]) == block and item["config_id"] == row[manifest_key])
                match = re.search(r"/batch_(\d+)(?:_job_(\d+))?/", retained["source_report"])
                assert match and (match[2] is None or match[2] in jobs)
                assert int(match[1]) == (1 if block <= 3 else 2)
                allocations.append(dict(stage=stage.upper(), block=block, order=int(row["order"]),
                                        config_id=row[manifest_key], batch=int(match[1]), allocation_job_id=match[2] or "",
                                        stage_allocation_job_ids=";".join(sorted(jobs)),
                                        allocation_mapping="source_report" if match[2] else "stage IDs only; case-to-job mapping not retained",
                                        qualified=retained["qualified"],
                                        pending_percent=retained["pending_backlog_percent"],
                                        source_report=retained["source_report"]))
        all_orders.append(orders)
    assert all_orders[0] == all_orders[1]
    ranking_sensitivity = []
    for stage in ("v1", "v2"):
        audit = read_csv(ROOT / f"data/audit/{stage}_validation_repeats.csv")
        summaries = []
        for config in sorted({row["config_id"] for row in audit}):
            group = [row for row in audit if row["config_id"] == config and row["eligible"].lower() == "true"]
            qualified = [row for row in group if row["qualified"].lower() == "true"]
            rates = [float(row["balanced_mib_per_sec"]) for row in group]
            q1, _, q3 = quantiles(rates, n=4, method="inclusive")
            summaries.append(dict(stage=stage.upper(), config_id=config,
                                  eligible_count=len(group), qualified_count=len(qualified),
                                  eligible_median_mibps=median(rates),
                                  qualified_median_mibps=median(float(row["balanced_mib_per_sec"]) for row in qualified) if qualified else None,
                                  eligible_iqr_mibps=q3-q1,
                                  eligible_median_pending_percent=median(float(row["pending_backlog_percent"]) for row in group),
                                  eligible_median_flush_sec=median(float(row["flush_sec"]) for row in group),
                                  eligible_median_failed_percent=median(float(row["failed_send_percent"]) for row in group)))
        def key(row, median_field):
            rate = row[median_field]
            return (-row["qualified_count"], -rate if rate is not None else math.inf,
                    row["eligible_iqr_mibps"], row["eligible_median_pending_percent"],
                    row["eligible_median_flush_sec"], row["eligible_median_failed_percent"], row["config_id"])
        for field, rank_field in (("eligible_median_mibps", "implemented_rank"),
                                  ("qualified_median_mibps", "qualified_only_rank")):
            for rank, row in enumerate(sorted(summaries, key=lambda row: key(row, field)), 1):
                row[rank_field] = rank
        retained = read_csv(SOURCE / f"artifacts/{stage}/validation_summary.csv")
        assert {row["original_config_id"]: int(row["validation_rank"]) for row in retained} == {
            row["config_id"]: row["implemented_rank"] for row in summaries}
        ranking_sensitivity.extend(sorted(summaries, key=lambda row: row["implemented_rank"]))
    checks = dict(validation_order_seed_replayed=20260728,
                  validation_randomization="Python random.Random(20260728).shuffle; one generator per stage, advancing across blocks; fresh shortlist copy for each block.",
                  seed_evidence="Current generator constant; replay agrees with all retained manifest orders, not a recovered campaign seed log.",
                  identical_V1_V2_orders=True,
                  allocation_id_source="V1 IDs appear in source_report paths and stage lists. V2 paths retain batch IDs only; stage lists contain both job IDs but do not explicitly map each batch to a job.",
                  profile_rate_geometric_means=ratios,
                  retained_profile_selection=decision["profile_selection"],
                  screening_pending_min_percent=min(float(row["pending_backlog_percent"]) for row in screening))
    return {"policy_sensitivity.csv": csv_text(sensitivity),
            "profile_anchor_comparison.csv": csv_text(profiles),
            "validation_allocations.csv": csv_text(allocations),
            "qualified_only_ranking.csv": csv_text(ranking_sensitivity),
            "review_checks.json": json.dumps(checks, indent=2, sort_keys=True) + "\n"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    folder = ROOT / "supplement"
    if not args.check:
        folder.mkdir(exist_ok=True)
    for name, content in derive().items():
        path = folder / name
        if args.check:
            assert path.read_text() == content, f"Supplement differs from retained evidence: {name}"
        else:
            path.write_text(content)
    print("Referee supplement verified" if args.check else "Referee supplement generated from retained evidence")


if __name__ == "__main__":
    main()
