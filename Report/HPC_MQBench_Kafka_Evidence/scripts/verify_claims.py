#!/usr/bin/env python3
"""Check manuscript prose claims from retained rows without changing any files.

Complements verify_paper.py: checks screening examples, workload contrasts,
correlations, the Appendix A workload levels, and additional resource/anchor
claims. It does not reconstruct raw timing, latency histograms, or monitoring
traces. Requires only Python's standard library and the companion data folder.
"""

from __future__ import annotations

import csv
import math
from pathlib import Path
from statistics import mean, median


ROOT = Path(__file__).resolve().parents[1]
AUDIT = ROOT / "data/audit"
SOURCE = ROOT / "data/kafka/source"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def number(row: dict[str, str], key: str) -> float:
    return float(row[key])


def require(condition: bool, description: str) -> None:
    if not condition:
        raise AssertionError(description)


def ranks(values: list[float]) -> list[float]:
    """Return one-based average ranks, including ties, without SciPy."""
    result = [0.0] * len(values)
    order = sorted(range(len(values)), key=values.__getitem__)
    start = 0
    while start < len(order):
        stop = start + 1
        while stop < len(order) and values[order[stop]] == values[order[start]]:
            stop += 1
        for index in order[start:stop]:
            result[index] = (start + 1 + stop) / 2
        start = stop
    return result


def spearman(left: list[float], right: list[float]) -> float:
    x, y = ranks(left), ranks(right)
    mx, my = mean(x), mean(y)
    return sum((a - mx) * (b - my) for a, b in zip(x, y)) / math.sqrt(
        sum((a - mx) ** 2 for a in x) * sum((b - my) ** 2 for b in y)
    )


def main() -> int:
    rows = read_csv(AUDIT / "phase1_cases.csv")
    by_id = {row["config_id"]: row for row in rows}
    eligible = [row for row in rows if row["eligible"].lower() == "true"]
    qualified = [row for row in eligible if row["qualified"].lower() == "true"]
    require((len(rows), len(by_id), len(eligible), len(qualified)) == (120, 120, 113, 99),
            "Unexpected screening population")
    require([int(row["surplus_records"]) for row in rows if int(row["surplus_records"])] == [83350],
            "Screening surplus-record claim")
    require(sum(row["latency_valid"].lower() == "false" for row in rows) == 7,
            "Seven screening latency-invalid observations")

    # Expected values are the rounded observations cited in the manuscript.
    examples = [
        ("cfg_001", "1,767.1", "269", "0.296"),
        ("cfg_101", "3,080.0", None, "5.599"),
        ("cfg_107", "3,001.5", "5890", "2.776"),
        ("cfg_094", "999.8", "712", None),
        ("cfg_090", "938.3", "19.5", None),
        ("cfg_096", "2,624.8", "221", None),
        ("cfg_007", "2,698.7", "9810", "4.571"),
    ]
    for config, rate, p99, pending in examples:
        row = by_id[config]
        require(f'{number(row, "balanced_mib_per_sec"):,.1f}' == rate, f"{config} rate")
        if p99 is not None:
            require(f'{number(row, "latency_p99_us") / 1000:g}' == p99, f"{config} p99")
        if pending is not None:
            require(f'{number(row, "pending_backlog_percent"):.3f}' == pending, f"{config} pending")
    require(max(qualified, key=lambda row: number(row, "balanced_records_per_sec"))["config_id"] == "cfg_094",
            "Qualified record-rate leader")
    require(min(qualified, key=lambda row: number(row, "latency_p99_us"))["config_id"] == "cfg_090",
            "Qualified p99 minimum")
    for config, rate in (("cfg_094", "1,023,762"), ("cfg_096", "167,984")):
        require(f'{number(by_id[config], "balanced_records_per_sec"):,.0f}' == rate,
                f"{config} screening record rate")
    require(f'{max(number(row, "latency_p99_us") for row in qualified) / 1e6:.1f}' == "11.1",
            "Maximum qualified screening p99")

    cohorts = [(8192, 7, 2, "19.29", "18.2", None),
               (16384, 5, 0, "32.00", "24.2", "2,743.0")]
    for payload, count, qcount, pending, p99, rate in cohorts:
        group = [row for row in eligible if int(row["payload_size_bytes"]) == payload
                 and int(row["producer_ranks"]) + int(row["consumer_ranks"]) >= 104]
        require(len(group) == count and sum(row["qualified"].lower() == "true" for row in group) == qcount,
                f"High-concurrency {payload}-byte cohort counts")
        require(f'{median(number(row, "pending_backlog_percent") for row in group):.2f}' == pending,
                f"High-concurrency {payload}-byte pending median")
        require(f'{median(number(row, "latency_p99_us") for row in group) / 1e6:.1f}' == p99,
                f"High-concurrency {payload}-byte p99 median")
        if rate is not None:
            require(f'{median(number(row, "balanced_mib_per_sec") for row in group):,.1f}' == rate,
                    f"High-concurrency {payload}-byte rate median")
        low = [row for row in eligible if int(row["payload_size_bytes"]) == payload
               and int(row["producer_ranks"]) + int(row["consumer_ranks"]) <= 64]
        require(len(low) == 3 and all(row["qualified"].lower() == "true" for row in low)
                and median(number(row, "pending_backlog_percent") for row in low) < 0.1,
                f"Low-concurrency {payload}-byte cohort")

    expected_levels = {
        "producer_ranks": [16, 24, 32, 40, 48, 56, 64],
        "consumer_ranks": [16, 24, 32, 40, 48, 56, 64],
        "partitions": [60, 90, 120, 180, 240],
        "batch_size": [262144, 524288, 1048576, 2097152, 4194304],
        "linger_ms": [0, 5, 10, 20, 40, 80],
        "payload_size_bytes": [1024, 2048, 4096, 8192, 16384],
        "producer_queue_messages": [500000, 1000000, 2000000],
        "producer_queue_kbytes": [524288, 1048576, 2097152],
        "consumer_fetch_min_bytes": [262144, 1048576, 2097152, 4194304, 8388608],
        "consumer_fetch_wait_max_ms": [10, 25, 50, 100],
        "consumer_fetch_message_max_bytes": [4194304, 8388608, 16777216, 33554432],
    }
    baseline = by_id["cfg_001"]
    for field, expected in expected_levels.items():
        require(sorted({int(number(row, field)) for row in rows}) == expected,
                f"Appendix A screened levels: {field}")
    expected_baseline = [40, 40, 120, 1048576, 20, 4096, 1000000, 1048576, 1048576, 50, 8388608]
    require([int(number(baseline, field)) for field in expected_levels] == expected_baseline,
            "Appendix A baseline settings")

    contrasts = [("producer_ranks", 64, "2,698.7", "52.7", "9810"),
                 ("payload_size_bytes", 8192, "2,546.8", "44.1", "11100"),
                 ("linger_ms", 80, None, "4.8", "185"),
                 ("partitions", 60, None, "2.6", "188")]
    for field, value, rate, gain, p99 in contrasts:
        group = [row for row in rows if number(row, field) == value
                 and all(number(row, key) == number(baseline, key)
                         for key in expected_levels if key != field)]
        require(len(group) == 1, f"One-factor contrast: {field}={value}")
        row = group[0]
        if rate is not None:
            require(f'{number(row, "balanced_mib_per_sec"):,.1f}' == rate, f"{field} rate")
        measured_gain = 100 * (number(row, "balanced_mib_per_sec") / number(baseline, "balanced_mib_per_sec") - 1)
        require(f"{measured_gain:.1f}" == gain, f"{field} gain")
        require(f'{number(row, "latency_p99_us") / 1000:g}' == p99, f"{field} p99")
    payload16 = by_id["cfg_030"]
    require(payload16["qualified"].lower() == "false"
            and f'{number(payload16, "pending_backlog_percent"):.3f}' == "22.151"
            and number(payload16, "latency_p99_us") / 1000 == 19100,
            "16-KiB one-factor overload")
    nonimproving = ["consumer_ranks", "batch_size", "producer_queue_messages", "producer_queue_kbytes",
                   "consumer_fetch_min_bytes", "consumer_fetch_wait_max_ms", "consumer_fetch_message_max_bytes"]
    for field in nonimproving:
        group = [row for row in qualified if number(row, field) != number(baseline, field)
                 and all(number(row, key) == number(baseline, key)
                         for key in expected_levels if key != field)]
        require(all(number(row, "balanced_mib_per_sec") <= number(baseline, "balanced_mib_per_sec") for row in group),
                f"Non-improving qualified one-factor alternatives: {field}")
    for field, expected in (("pending_backlog_percent", "0.756"), ("latency_p99_us", "0.759")):
        rho = spearman([number(row, "producer_ranks") for row in eligible],
                       [number(row, field) for row in eligible])
        require(f"{rho:.3f}" == expected, f"Spearman correlation: {field}")

    cases = read_csv(SOURCE / "analysis/v2/complete-profile/kafka_broker_tuning_v2_cases.csv")
    final = [row for row in cases if row["stage"] == "final_validation"]
    require(len(cases) == 119 and len(final) == 50, "Profile/final observation counts")
    for config, queue, total, receive in (("cfg_101", "0.770", "4.755", "3,272"),
                                           ("cfg_007", "0.488", "4.264", "3,057")):
        group = [row for row in final if row["anchor_id"] == config]
        require(len(group) == 5, f"Final {config} count")
        for field, expected in (("kafka_request_queue_time_p99_ms_mean", queue),
                                ("kafka_total_time_p99_ms_mean", total)):
            require(f'{median(number(row, field) for row in group):.3f}' == expected,
                    f"{config} request timing: {field}")
        require(f'{median(number(row, "ib0_rx_mibps_mean") for row in group):,.0f}' == receive,
                f"{config} interface receive mean")
    for config, expected in (("cfg_007", "749,340"), ("cfg_096", "167,838"), ("cfg_094", "1,026,286")):
        require(f'{median(number(row, "balanced_records_per_sec") for row in final if row["anchor_id"] == config):,.0f}' == expected,
                f"{config} final median record rate")

    calibration = [row for row in cases if row["stage"] == "rate_calibration"]
    require(len(calibration) == 3, "Calibration observation count")
    target = 0.8 * median(number(row, "balanced_records_per_sec") for row in calibration)
    require(f"{target:,.0f}" == "285,371", "Rounded calibration-derived target")
    anchors = [row for row in cases if row["anchor_id"] == "latency_anchor"
               and row["stage"] in ("profile_screening", "profile_confirmation")]
    require(len(anchors) == 12, "Latency-anchor observation count")
    for row in anchors:
        for field, expected in {
            "producer_ranks": 40, "consumer_ranks": 40, "partitions": 120,
            "payload_size_bytes": 4096, "batch_size_bytes": 32768,
            "linger_ms": 1, "consumer_fetch_min_bytes": 1,
            "consumer_fetch_wait_ms": 1, "target_records_per_sec": target,
        }.items():
            require(math.isclose(number(row, field), expected, rel_tol=1e-12),
                    f"Latency-anchor setting: {field}")

    print("Additional manuscript claims verified: 120 screening rows (113 eligible, 99 qualified); "
          "7 cited configurations, 4 workload cohorts, 11 setting ranges/baseline, "
          "4 positive contrasts, 7 non-improving parameter groups, 2 correlations; "
          "50 final observations, request/interface summaries, 3 record-rate claims, "
          "3 calibration runs and 12 latency-anchor observations.")
    print("No files or checksums changed. Raw timing, latency and monitoring inputs are not reconstructed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
