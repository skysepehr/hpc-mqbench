#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "analysis_deps"))
os.environ.setdefault(
    "MPLCONFIGDIR", str(PROJECT_ROOT / ".cache" / "kafka-benchmark-mplconfig")
)

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, Rectangle


DEFAULT_REPORT_ROOT = PROJECT_ROOT / "results" / "published" / "kafka" / "v1"
OUT_DIR = DEFAULT_REPORT_ROOT / "analysis_diagrams"
VALIDATION_RESULTS = DEFAULT_REPORT_ROOT / "analysis_validation_results.csv"


def setup_axes(width: float = 14.48, height: float = 10.86):
    fig, ax = plt.subplots(figsize=(width, height), dpi=100)
    fig.subplots_adjust(left=0, right=1, bottom=0, top=1)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")
    return fig, ax


def add_box(ax, x, y, w, h, *, lw=1.2, edge="#202020", face="white"):
    patch = Rectangle((x, y), w, h, linewidth=lw, edgecolor=edge, facecolor=face)
    ax.add_patch(patch)
    return patch


def add_text(ax, x, y, text, *, size=13, weight="normal", ha="left", va="center", color="#111111"):
    ax.text(
        x,
        y,
        text,
        fontsize=size,
        fontweight=weight,
        ha=ha,
        va=va,
        color=color,
        family="DejaVu Sans",
    )


def add_arrow(ax, start, end, *, color="#0b4bb3", style="-", lw=1.7, mutation_scale=16):
    ax.add_patch(
        FancyArrowPatch(
            start,
            end,
            arrowstyle="-|>",
            mutation_scale=mutation_scale,
            linewidth=lw,
            color=color,
            linestyle=style,
            shrinkA=0,
            shrinkB=0,
        )
    )


def add_value_boxes(ax, x, y, values: Iterable[str], *, box_w=0.045, box_h=0.043, gap=0.012, size=12):
    for idx, value in enumerate(values):
        bx = x + idx * (box_w + gap)
        add_box(ax, bx, y, box_w, box_h, lw=0.8, edge="#303030", face="#fbfbfb")
        add_text(ax, bx + box_w / 2, y + box_h / 2, str(value), size=size, ha="center")


def create_sweep_design() -> None:
    fig, ax = setup_axes()

    panel_w = 0.48
    right_panel_w = 0.45
    panel_h = 0.32
    left_x = 0.025
    right_x = 0.525

    add_box(ax, left_x, 0.63, panel_w, panel_h)
    add_text(ax, left_x + 0.02, 0.91, "1.  Workload Concurrency", size=19, weight="bold")
    add_text(ax, left_x + 0.02, 0.84, "Producer ranks", size=13)
    add_value_boxes(ax, left_x + 0.145, 0.82, ["16", "24", "32", "40", "48", "56", "64"], box_w=0.034, gap=0.008)
    ax.plot([left_x + 0.02, left_x + panel_w - 0.02], [0.775, 0.775], color="#3c78d8", lw=1)
    add_text(ax, left_x + 0.02, 0.735, "Consumer ranks", size=13)
    add_value_boxes(ax, left_x + 0.145, 0.715, ["16", "24", "32", "40", "48", "56", "64"], box_w=0.034, gap=0.008)
    ax.plot([left_x + 0.02, left_x + panel_w - 0.02], [0.67, 0.67], color="#3c78d8", lw=1)
    add_text(
        ax,
        left_x + 0.02,
        0.655,
        "Producer and consumer ranks vary together across configurations.",
        size=10.8,
        va="top",
    )

    add_box(ax, right_x, 0.63, right_panel_w, panel_h)
    add_text(ax, right_x + 0.02, 0.91, "2.  Topic Layout", size=19, weight="bold")
    add_text(ax, right_x + 0.02, 0.79, "Partitions", size=13)
    add_value_boxes(ax, right_x + 0.14, 0.77, ["60", "90", "120", "180", "240"], box_w=0.052, gap=0.014)

    add_box(ax, left_x, 0.30, panel_w, 0.30)
    add_text(ax, left_x + 0.02, 0.56, "3.  Producer Tuning", size=19, weight="bold")
    producer_rows = [
        ("Batch size (bytes)", ["262144", "524288", "1048576", "2097152", "4194304"]),
        ("Linger ms", ["0", "5", "10", "20", "40", "80"]),
        ("Producer queue\nmessages", ["500000", "1000000", "2000000"]),
        ("Producer queue\nKiB", ["524288", "1048576", "2097152"]),
    ]
    y = 0.50
    for idx, (label, values) in enumerate(producer_rows):
        add_text(ax, left_x + 0.02, y + 0.018, label, size=11, va="center")
        box_w = 0.066 if len(values) <= 3 else 0.047
        gap = 0.012 if len(values) <= 3 else 0.006
        if len(values) == 6:
            box_w = 0.040
        add_value_boxes(ax, left_x + 0.155, y, values, box_w=box_w, gap=gap, size=9.2)
        if idx < len(producer_rows) - 1:
            ax.plot([left_x + 0.02, left_x + panel_w - 0.02], [y - 0.015, y - 0.015], color="#7da7e8", lw=0.8)
        y -= 0.064

    add_box(ax, right_x, 0.30, right_panel_w, 0.30)
    add_text(ax, right_x + 0.02, 0.56, "4.  Consumer Tuning", size=19, weight="bold")
    consumer_rows = [
        ("Consumer fetch\nmin bytes", ["262144", "1048576", "2097152", "4194304", "8388608"]),
        ("Consumer fetch\nwait ms", ["10", "25", "50", "100"]),
        ("Consumer fetch\nmax bytes", ["4194304", "8388608", "16777216", "33554432"]),
    ]
    y = 0.49
    for idx, (label, values) in enumerate(consumer_rows):
        add_text(ax, right_x + 0.02, y + 0.02, label, size=10.5, va="center")
        box_w = 0.049 if len(values) == 5 else 0.060
        add_value_boxes(ax, right_x + 0.16, y, values, box_w=box_w, gap=0.006, size=8.8)
        if idx < len(consumer_rows) - 1:
            ax.plot([right_x + 0.02, right_x + right_panel_w - 0.02], [y - 0.02, y - 0.02], color="#7da7e8", lw=0.8)
        y -= 0.085

    add_box(ax, 0.025, 0.155, 0.95, 0.105)
    add_text(ax, 0.045, 0.225, "5.  Payload Size", size=19, weight="bold")
    add_text(ax, 0.045, 0.19, "Payload bytes", size=12)
    add_value_boxes(ax, 0.225, 0.185, ["1024", "2048", "4096", "8192", "16384"], box_w=0.055, gap=0.016)
    ax.plot([0.66, 0.66], [0.185, 0.235], color="#7da7e8", lw=1)
    add_arrow(ax, (0.69, 0.218), (0.92, 0.218), color="#0b4bb3", lw=1.5, mutation_scale=14)
    add_text(ax, 0.69, 0.18, "Small", size=10, color="#0b4bb3")
    add_text(ax, 0.92, 0.18, "Large", size=10, ha="right", color="#0b4bb3")

    add_box(ax, 0.025, 0.045, 0.95, 0.075, edge="#8a5a00")
    add_text(
        ax,
        0.045,
        0.083,
        "Broker-level settings were not varied as an experimental dimension in this sweep.",
        size=15,
    )

    fig.savefig(OUT_DIR / "sweep_design_parameter_space.png")
    plt.close(fig)


def read_validation_rows() -> list[dict[str, str]]:
    with VALIDATION_RESULTS.open(newline="") as handle:
        return list(csv.DictReader(handle))


def fmt_float(value: str, digits: int = 3) -> str:
    return f"{float(value):.{digits}f}"


def create_repeated_validation_results() -> None:
    rows = read_validation_rows()
    fig, ax = setup_axes()

    flow = [
        ("1. Input shortlist", "10 configurations"),
        ("2. Design", "5 randomized blocks,\nfixed seed 20260716"),
        ("3. Execution", "50 total case rows"),
        ("4. Ranking", "qualified-run frequency\nfirst, then median\nthroughput"),
        ("5. Outcome", "validated\nrecommendation"),
    ]
    x_positions = [0.02, 0.22, 0.42, 0.62, 0.82]
    for idx, ((title, body), x) in enumerate(zip(flow, x_positions)):
        add_box(ax, x, 0.855, 0.16, 0.105)
        add_text(ax, x + 0.08, 0.925, title, size=13.5, weight="bold", ha="center")
        add_text(ax, x + 0.08, 0.885, body, size=10.5, ha="center")
        if idx < len(x_positions) - 1:
            add_arrow(ax, (x + 0.16, 0.907), (x_positions[idx + 1], 0.907), color="#111111", lw=1.3)

    add_box(ax, 0.02, 0.33, 0.96, 0.485)
    add_text(ax, 0.045, 0.792, "Repeated validation summary", size=18, weight="bold")
    headers = ["Rank", "Configuration", "Qualified repeats (out of 5)", "Median balanced throughput"]
    col_x = [0.03, 0.13, 0.36, 0.66]
    col_w = [0.10, 0.23, 0.30, 0.31]
    y_top = 0.705
    row_h = 0.034
    for x, w, header in zip(col_x, col_w, headers):
        add_box(ax, x, y_top, w, row_h, lw=0.8, face="#eef4fb")
        add_text(ax, x + w / 2, y_top + row_h / 2, header, size=11.5, weight="bold", ha="center")
    for idx, row in enumerate(rows):
        y = y_top - (idx + 1) * row_h
        values = [
            row["validation_rank"],
            row["original_config_id"],
            f"{row['qualified_count']}/{row['repeats']}",
            f"{fmt_float(row['median_balanced_app_MBps'])} MB/s",
        ]
        for x, w, value in zip(col_x, col_w, values):
            add_box(ax, x, y, w, row_h, lw=0.6, face="white")
            add_text(ax, x + w / 2, y + row_h / 2, value, size=11.5, ha="center")

    def find(config_id: str) -> dict[str, str]:
        return next(row for row in rows if row["original_config_id"] == config_id)

    cards = [
        ("Primary validated recommendation", "cfg_074", "#eef8ef"),
        ("Stable secondary option", "cfg_115", "#eef8ef"),
        ("Important overload reference", "cfg_119", "#fff1f1"),
    ]
    for title, config_id, face in cards:
        row = find(config_id)
        x = 0.02 + cards.index((title, config_id, face)) * 0.32
        add_box(ax, x, 0.095, 0.30, 0.225, face=face)
        add_text(ax, x + 0.15, 0.285, title, size=12.5, weight="bold", ha="center")
        add_box(ax, x + 0.06, 0.25, 0.18, 0.03, edge="#508a50" if config_id != "cfg_119" else "#c05050", face="white")
        add_text(ax, x + 0.15, 0.265, config_id, size=15, ha="center")
        metrics = [
            ("Qualified repeats", f"{row['qualified_count']}/{row['repeats']}"),
            ("Median balanced throughput", f"{fmt_float(row['median_balanced_app_MBps'])} MB/s"),
            ("Median backlog", f"{fmt_float(row['median_pending_backlog_percent'])}%"),
            ("Median flush", f"{fmt_float(row['median_flush_sec'])} s"),
        ]
        y = 0.215
        for metric_idx, (label, value) in enumerate(metrics):
            add_text(ax, x + 0.015, y, label, size=9.5)
            add_text(ax, x + 0.285, y, value, size=9.5, ha="right")
            if metric_idx < len(metrics) - 1:
                ax.plot([x + 0.012, x + 0.288], [y - 0.014, y - 0.014], color="#777777", lw=0.55)
            y -= 0.034

    add_box(ax, 0.02, 0.035, 0.96, 0.055)
    add_text(
        ax,
        0.50,
        0.063,
        "Defensible validated claim: cfg_074 is the best sustained configuration observed so far.",
        size=15,
        weight="bold",
        ha="center",
    )

    fig.savefig(OUT_DIR / "repeated_validation_results.png")
    plt.close(fig)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Regenerate paper diagrams from compact V1 result tables"
    )
    parser.add_argument("--output-dir", type=Path, default=OUT_DIR)
    parser.add_argument(
        "--validation-results",
        type=Path,
        default=VALIDATION_RESULTS,
    )
    return parser.parse_args()


def main() -> int:
    global OUT_DIR, VALIDATION_RESULTS
    args = parse_args()
    OUT_DIR = (
        args.output_dir
        if args.output_dir.is_absolute()
        else PROJECT_ROOT / args.output_dir
    )
    VALIDATION_RESULTS = (
        args.validation_results
        if args.validation_results.is_absolute()
        else PROJECT_ROOT / args.validation_results
    )
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    create_sweep_design()
    create_repeated_validation_results()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
