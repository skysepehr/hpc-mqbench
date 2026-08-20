#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import html
import json
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Optionally render Markdown/HTML/LaTeX/PDF from an already finalized "
            "reproducible JSON/CSV result bundle."
        )
    )
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--compile-pdf", action="store_true")
    parser.add_argument("--skip-figures", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    results = args.results_dir.resolve()
    output = args.output_dir.resolve()
    if output == results or results in output.parents or output in results.parents:
        raise ValueError(
            "report output must be separate from the immutable machine-result bundle"
        )
    _verify_checksums(results)
    report = _object(results / "final_report.json")
    if report.get("status") != "complete":
        raise ValueError("machine-result bundle is not complete")
    backend = str(report.get("backend_id", "")).strip()
    if not backend:
        raise ValueError("machine-result bundle has no backend_id")
    phase1 = _cases(results / "phase1_cases.json")
    summaries = _array(results / "validation_summary.json")
    if len(phase1) != 120:
        raise ValueError(f"Phase 1 has {len(phase1)} rows, expected 120")
    if len(summaries) != 10:
        raise ValueError(f"validation summary has {len(summaries)} rows, expected 10")
    summaries.sort(key=lambda row: int(row["validation_rank"]))
    winner = summaries[0]
    output.mkdir(parents=True, exist_ok=True)
    figures = output / "figures"
    if not args.skip_figures:
        figures.mkdir(parents=True, exist_ok=True)
        _write_figures(figures, phase1, summaries)

    markdown = _markdown(backend, report, phase1, summaries, not args.skip_figures)
    (output / "reproducible_benchmark_report.md").write_text(markdown + "\n", encoding="utf-8")
    (output / "reproducible_benchmark_report.html").write_text(
        _html(backend, report, phase1, summaries, not args.skip_figures),
        encoding="utf-8",
    )
    tex = output / "reproducible_benchmark_report.tex"
    tex.write_text(
        _latex(backend, report, phase1, summaries, not args.skip_figures),
        encoding="utf-8",
    )
    if args.compile_pdf:
        if shutil.which("pdflatex") is None:
            raise RuntimeError("pdflatex is required by --compile-pdf")
        for _ in range(2):
            subprocess.run(
                ["pdflatex", "-interaction=nonstopmode", "-halt-on-error", tex.name],
                cwd=output,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
    _write_json(
        output / "report_metadata.json",
        {
            "format": "messaging-benchmark.optional-report.v1",
            "backend_id": backend,
            "source_results_dir": results.name,
            "source_final_report_sha256": _sha256(results / "final_report.json"),
            "reporting_required_for_benchmark_success": False,
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        },
    )
    _write_artifact_manifest(output)
    print(f"[reproducible-report] backend: {backend}")
    print(f"[reproducible-report] winner: {winner['original_config_id']}")
    print(f"[reproducible-report] output: {output}")
    return 0


def _write_figures(
    output: Path,
    phase1: list[dict[str, Any]],
    summaries: list[dict[str, Any]],
) -> None:
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError("Matplotlib is required unless --skip-figures is used") from exc

    colors = {
        "qualified": "#2f7d32",
        "overdriven": "#c84f25",
        "ineligible": "#777777",
    }
    fig, axis = plt.subplots(figsize=(8.4, 5.2))
    for status in ("qualified", "overdriven", "ineligible"):
        rows = [row for row in phase1 if row["qualification_status"] == status]
        axis.scatter(
            [float(row["pending_backlog_percent"]) for row in rows],
            [float(row["balanced_mib_per_sec"]) for row in rows],
            s=28,
            alpha=0.8,
            label=status.title(),
            color=colors[status],
        )
    axis.axvline(5.0, color="#111111", linestyle="--", linewidth=1.2, label="5% backlog limit")
    axis.set_xlabel("Producer backlog at flush start (% of measurement send attempts)")
    axis.set_ylabel("Balanced throughput (MiB/s)")
    axis.grid(alpha=0.2)
    axis.legend(frameon=False)
    fig.tight_layout()
    for suffix in ("png", "pdf"):
        fig.savefig(output / f"phase1_throughput_backlog.{suffix}", dpi=180)
    plt.close(fig)

    labels = [str(row["original_config_id"]) for row in summaries]
    medians = [float(row["median_balanced_mib_per_sec"]) for row in summaries]
    iqr = [float(row["iqr_balanced_mib_per_sec"]) for row in summaries]
    fig, axis = plt.subplots(figsize=(8.4, 5.2))
    positions = list(range(len(labels)))
    axis.errorbar(positions, medians, yerr=[value / 2 for value in iqr], fmt="o", capsize=4)
    axis.set_xticks(positions, labels, rotation=40, ha="right")
    axis.set_ylabel("Median balanced throughput (MiB/s)")
    axis.set_xlabel("Validation configuration (ranking order)")
    axis.grid(axis="y", alpha=0.2)
    fig.tight_layout()
    for suffix in ("png", "pdf"):
        fig.savefig(output / f"validation_throughput_variability.{suffix}", dpi=180)
    plt.close(fig)


def _markdown(
    backend: str,
    report: dict[str, Any],
    phase1: list[dict[str, Any]],
    summaries: list[dict[str, Any]],
    include_figures: bool = True,
) -> str:
    winner = report["recommendation"]
    campaign_label = _campaign_label(report)
    lines = [
        f"# Reproducible {backend.title()} {campaign_label} Benchmark",
        "",
        "## Measurement Contract",
        "",
        "Each case uses a 15-second excluded warm-up, 30-second measurement, and up to 60 seconds of consumer drain. The backend is restarted and RAM-backed storage is cleaned between cases. Record identity and correctness cover every record; end-to-end timestamps use deterministic 1-in-10 sampling and 100-sample pre/post clock calibration.",
        "",
        "Backlog is pending producer callbacks at flush start divided by measurement-period send attempts. A correct, latency-valid run is **Qualified** when backlog is at most 5%, flush is at most 10 seconds, and failed sends are at most 0.1%. An eligible run outside those limits is **Overdriven**; correctness or latency failure is **Ineligible**.",
        "",
        "## Phase 1",
        "",
        f"The screening contains {len(phase1)} single observations: {report['phase1']['qualified_count']} qualified, {report['phase1']['overdriven_count']} overdriven, and {len(phase1) - report['phase1']['eligible_count']} ineligible.",
        "",
        "## Repeated Validation",
        "",
        "The ten selected workloads occur once in each of five randomized blocks. Aggregate metrics use eligible individual repeats. Ranking is qualification frequency first, followed by median balanced throughput, throughput IQR, backlog, flush, failed sends, and configuration ID.",
        "",
        "| Rank | Config | Eligible | Qualified | Median MiB/s | IQR MiB/s | Median records/s | Median p99 (us) |",
        "|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summaries:
        lines.append(
            "| {validation_rank} | `{original_config_id}` | {eligible_count}/5 | "
            "{qualified_count}/5 | {median_balanced_mib_per_sec:.3f} | "
            "{iqr_balanced_mib_per_sec:.3f} | {median_balanced_records_per_sec:,.0f} | "
            "{median_latency_p99_us:,.3f} |".format(**row)
        )
    if include_figures:
        lines[lines.index("## Repeated Validation"):lines.index("## Repeated Validation")] = [
            "![Balanced throughput versus attempted-send backlog](figures/phase1_throughput_backlog.png)",
            "",
        ]
    lines.extend([""])
    if include_figures:
        lines.extend([
            "![Repeated-validation throughput and variability](figures/validation_throughput_variability.png)",
            "",
        ])
    lines.extend(
        [
            "## Recommendation",
            "",
            f"`{winner['config_id']}` is the reproducible V1 leader: {winner['qualified_repeats']}/5 qualified repeats and median balanced throughput {float(winner['median_balanced_mib_per_sec']):.3f} MiB/s. Latency remains a secondary reported metric and never excuses an overdriven run.",
        ]
    )
    return "\n".join(lines)


def _html(
    backend: str,
    report: dict[str, Any],
    phase1: list[dict[str, Any]],
    summaries: list[dict[str, Any]],
    include_figures: bool = True,
) -> str:
    rows = "".join(
        "<tr>"
        f"<td>{int(row['validation_rank'])}</td>"
        f"<td><code>{html.escape(str(row['original_config_id']))}</code></td>"
        f"<td>{int(row['eligible_count'])}/5</td>"
        f"<td>{int(row['qualified_count'])}/5</td>"
        f"<td>{float(row['median_balanced_mib_per_sec']):.3f}</td>"
        f"<td>{float(row['iqr_balanced_mib_per_sec']):.3f}</td>"
        f"<td>{float(row['median_balanced_records_per_sec']):,.0f}</td>"
        f"<td>{float(row['median_latency_p99_us']):,.3f}</td>"
        "</tr>"
        for row in summaries
    )
    winner = report["recommendation"]
    phase1_figure = (
        '<img src="figures/phase1_throughput_backlog.png" alt="Balanced throughput versus producer backlog">'
        if include_figures else ""
    )
    validation_figure = (
        '<img src="figures/validation_throughput_variability.png" alt="Repeated validation throughput and variability">'
        if include_figures else ""
    )
    campaign_label = _campaign_label(report)
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Reproducible {html.escape(backend.title())} {campaign_label} Benchmark</title>
<style>body{{font:16px/1.5 system-ui,sans-serif;max-width:1100px;margin:2rem auto;padding:0 1rem;color:#171717}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #bbb;padding:.45rem;text-align:right}}th:nth-child(2),td:nth-child(2){{text-align:left}}img{{max-width:100%}}code{{font-family:ui-monospace,monospace}}</style></head><body>
<h1>Reproducible {html.escape(backend.title())} {campaign_label} Benchmark</h1>
<h2>Measurement contract</h2><p>15 s warm-up, 30 s measurement, up to 60 s drain, fresh backend and RAM-backed storage per case, attempted-send backlog denominator, deterministic 1-in-10 latency sampling, and full record correctness.</p>
<h2>Phase 1</h2><p>{len(phase1)} cases; {report['phase1']['qualified_count']} qualified and {report['phase1']['overdriven_count']} overdriven.</p>{phase1_figure}
<h2>Repeated validation</h2><table><thead><tr><th>Rank</th><th>Config</th><th>Eligible</th><th>Qualified</th><th>Median MiB/s</th><th>IQR MiB/s</th><th>Median records/s</th><th>Median p99 us</th></tr></thead><tbody>{rows}</tbody></table>
{validation_figure}
<h2>Recommendation</h2><p><code>{html.escape(str(winner['config_id']))}</code> leads with {winner['qualified_repeats']}/5 qualified repeats and {float(winner['median_balanced_mib_per_sec']):.3f} MiB/s median balanced throughput.</p></body></html>"""


def _latex(
    backend: str,
    report: dict[str, Any],
    phase1: list[dict[str, Any]],
    summaries: list[dict[str, Any]],
    include_figures: bool = True,
) -> str:
    rows = "\n".join(
        f"{int(row['validation_rank'])} & {_tex(str(row['original_config_id']))} & "
        f"{int(row['eligible_count'])}/5 & {int(row['qualified_count'])}/5 & "
        f"{float(row['median_balanced_mib_per_sec']):.3f} & "
        f"{float(row['iqr_balanced_mib_per_sec']):.3f} & "
        f"{float(row['median_balanced_records_per_sec']):,.0f} & "
        f"{float(row['median_latency_p99_us']):,.3f} \\\\"
        for row in summaries
    )
    winner = report["recommendation"]
    phase1_figure = rf"""
\begin{{figure}}[htbp]\centering
\includegraphics[width=0.9\linewidth]{{figures/phase1_throughput_backlog.pdf}}
\caption{{Balanced throughput versus producer backlog for all 120 Phase 1 cases. Backlog is pending callbacks at flush start divided by measurement-period send attempts; the dashed line marks the 5\% qualification threshold.}}
\label{{fig:phase1-backlog}}
\end{{figure}}
""" if include_figures else ""
    validation_figure = rf"""
\begin{{figure}}[htbp]\centering
\includegraphics[width=0.9\linewidth]{{figures/validation_throughput_variability.pdf}}
\caption{{Median balanced throughput of the ten repeated configurations. Error bars show one half of the throughput IQR to provide a compact variability comparison.}}
\label{{fig:validation-variability}}
\end{{figure}}
""" if include_figures else ""
    campaign_label = _campaign_label(report)
    return rf"""\documentclass[11pt]{{article}}
\usepackage[margin=1in]{{geometry}}
\usepackage{{booktabs,graphicx,hyperref,longtable}}
\title{{Reproducible {_tex(backend.title())} {_tex(campaign_label)} Benchmark}}
\author{{HPC-MQBench}}
\date{{}}
\begin{{document}}
\maketitle
\section{{Measurement Contract}}
Each case uses a 15-second excluded warm-up, a 30-second measurement, and up to 60 seconds of consumer drain. The backend is restarted and RAM-backed storage is cleaned between cases. Record identity and correctness cover every record; end-to-end timestamps use deterministic 1-in-10 sampling and 100-sample pre/post clock calibration.

Producer backlog is the number of pending delivery callbacks at flush start divided by measurement-period send attempts. An eligible run is \emph{{Qualified}} when backlog is at most 5\%, flush is at most 10 seconds, and failed sends are at most 0.1\%. An eligible run outside those limits is \emph{{Overdriven}}; a correctness or latency failure is \emph{{Ineligible}}.

\section{{Phase 1 Screening}}
The campaign contains {len(phase1)} single observations: {report['phase1']['qualified_count']} qualified, {report['phase1']['overdriven_count']} overdriven, and {len(phase1) - report['phase1']['eligible_count']} ineligible.
{phase1_figure}

\section{{Repeated Validation}}
Ten selected workloads occur once in each of five randomized blocks. Aggregates use eligible repeats. Ranking is qualified-repeat count, median balanced throughput, throughput IQR, backlog, flush, failed sends, and configuration ID.
\small
\begin{{longtable}}{{r l r r r r r r}}
\caption{{Repeated-validation ranking. IQR is the interquartile range; p99 is the 99th-percentile producer-to-consumer latency in microseconds.}}\\
\toprule Rank & Config & Eligible & Qualified & Median MiB/s & IQR MiB/s & Median records/s & Median p99 $\mu$s \\
\midrule
{rows}
\bottomrule
\end{{longtable}}
\normalsize
{validation_figure}

\section{{Recommendation}}
\texttt{{{_tex(str(winner['config_id']))}}} is the reproducible V1 leader, with {winner['qualified_repeats']}/5 qualified repeats and median balanced throughput {float(winner['median_balanced_mib_per_sec']):.3f} MiB/s. Latency is secondary evidence and cannot make an overdriven run qualified.
\end{{document}}
"""


def _write_artifact_manifest(output: Path) -> None:
    rows = []
    for path in sorted(output.rglob("*")):
        if path.is_file() and path.name not in {"SHA256SUMS", "artifact_manifest.csv"}:
            rows.append(
                {
                    "path": path.relative_to(output).as_posix(),
                    "size_bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                }
            )
    with (output / "artifact_manifest.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("path", "size_bytes", "sha256"))
        writer.writeheader()
        writer.writerows(rows)
    (output / "SHA256SUMS").write_text(
        "".join(f"{row['sha256']}  {row['path']}\n" for row in rows),
        encoding="utf-8",
    )


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
    value = _object(path)
    rows = value.get("cases")
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError(f"expected cases array: {path}")
    return rows


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _campaign_label(report: dict[str, Any]) -> str:
    phase = str(report.get("campaign_phase", "v1")).strip().upper()
    return phase if phase in {"V1", "V2"} else "Campaign"


def _verify_checksums(results: Path) -> None:
    checksum_path = results / "SHA256SUMS"
    if not checksum_path.is_file():
        raise FileNotFoundError(checksum_path)
    failures: list[str] = []
    for line in checksum_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            expected, relative = line.split(None, 1)
        except ValueError:
            failures.append(f"malformed checksum row: {line!r}")
            continue
        path = (results / relative.strip()).resolve()
        if results not in path.parents:
            failures.append(f"checksum path escapes result bundle: {relative}")
        elif not path.is_file():
            failures.append(f"missing checksum input: {relative}")
        elif _sha256(path) != expected:
            failures.append(f"checksum mismatch: {relative}")
    if failures:
        raise ValueError("invalid machine-result checksums: " + "; ".join(failures))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _tex(value: str) -> str:
    return (
        value.replace("\\", r"\textbackslash{}")
        .replace("_", r"\_")
        .replace("%", r"\%")
        .replace("&", r"\&")
        .replace("#", r"\#")
    )


if __name__ == "__main__":
    raise SystemExit(main())
