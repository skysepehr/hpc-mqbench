#!/usr/bin/env python3
"""Create compact V2 figures for the combined Kafka report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROFILE_ORDER = ("B0", "B1", "B2", "B3", "B4", "B5")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cases",
        type=Path,
        default=(
            PROJECT_ROOT
            / "results/published/kafka/combined/data"
            / "kafka_broker_tuning_v2_cases.json"
        ),
    )
    parser.add_argument(
        "--decisions",
        type=Path,
        default=(
            PROJECT_ROOT
            / "results/published/kafka/combined/data"
            / "kafka_broker_tuning_v2_decisions.json"
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=(
            PROJECT_ROOT
            / "results/published/kafka/combined/analysis_plots"
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cases = _load_list(args.cases)
    decisions = _load_dict(args.decisions)
    screening = decisions.get("screening_selection")
    if not isinstance(screening, dict):
        raise ValueError("screening_selection is missing from decisions JSON")

    summaries = screening.get("profiles")
    if not isinstance(summaries, list):
        raise ValueError("screening profile summaries are missing")
    by_profile = {
        str(item["profile_id"]): item
        for item in summaries
        if isinstance(item, dict) and item.get("profile_id")
    }
    if set(PROFILE_ORDER) - set(by_profile):
        raise ValueError("screening summary does not contain B0 through B5")

    latency_rows = {
        str(row.get("broker_profile_id")): row
        for row in cases
        if row.get("anchor_id") == "latency_anchor"
        and row.get("stage") == "profile_screening"
    }
    baseline_p99 = float(latency_rows["B0"]["latency_p99_ms"])
    throughput_ratios = [
        float(by_profile[profile]["geometric_mean_throughput_ratio_to_B0"])
        for profile in PROFILE_ORDER
    ]
    latency_ratios = [
        float(latency_rows[profile]["latency_p99_ms"]) / baseline_p99
        for profile in PROFILE_ORDER
    ]
    valid_counts = [
        int(by_profile[profile]["valid_run_count"])
        for profile in PROFILE_ORDER
    ]
    candidates = set(screening.get("confirmation_profiles", [])) - {"B0"}

    import matplotlib.pyplot as plt

    plt.rcParams.update(
        {
            "font.size": 9,
            "axes.titlesize": 10,
            "axes.labelsize": 9,
            "figure.dpi": 140,
            "savefig.dpi": 300,
        }
    )
    fig, axes = plt.subplots(1, 2, figsize=(9.2, 3.8), constrained_layout=True)
    colors = [
        "#4d4d4d" if profile == "B0" else
        "#2f6f9f" if profile in candidates else
        "#a9b7c6"
        for profile in PROFILE_ORDER
    ]
    hatches = ["//" if profile in candidates else "" for profile in PROFILE_ORDER]

    for axis, values, title, ylabel, limit in (
        (
            axes[0],
            throughput_ratios,
            "Maximum-load throughput relative to B0",
            "Geometric-mean balanced-throughput ratio",
            1.03,
        ),
        (
            axes[1],
            latency_ratios,
            "Fixed-rate p99 latency relative to B0",
            "p99 latency ratio",
            1.10,
        ),
    ):
        bars = axis.bar(PROFILE_ORDER, values, color=colors, edgecolor="#333333")
        for bar, hatch in zip(bars, hatches):
            bar.set_hatch(hatch)
        axis.axhline(1.0, color="#222222", linewidth=1.0, linestyle="--")
        axis.axhline(limit, color="#a23b3b", linewidth=1.0, linestyle=":")
        axis.set_title(title)
        axis.set_ylabel(ylabel)
        axis.grid(axis="y", color="#dddddd", linewidth=0.6)
        axis.set_axisbelow(True)
        lower = min(values + [1.0, limit])
        upper = max(values + [1.0, limit])
        padding = max((upper - lower) * 0.22, 0.035)
        axis.set_ylim(max(0.0, lower - padding), upper + padding)
        for index, (bar, value) in enumerate(zip(bars, values)):
            annotation = f"{value:.3f}"
            if axis is axes[0]:
                annotation += f"\n{valid_counts[index]}/5 valid"
            axis.text(
                bar.get_x() + bar.get_width() / 2,
                value + padding * 0.12,
                annotation,
                ha="center",
                va="bottom",
                fontsize=7.5,
            )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    stem = args.output_dir / "v2_profile_screening_summary"
    fig.savefig(stem.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(stem.with_suffix(".png"), bbox_inches="tight")
    plt.close(fig)
    return 0


def _load_list(path: Path) -> list[dict[str, Any]]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, list):
        raise ValueError(f"Expected a JSON array: {path}")
    return [item for item in value if isinstance(item, dict)]


def _load_dict(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object: {path}")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
