#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import shutil
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PHASE1 = PROJECT_ROOT / "results" / "published" / "pulsar" / "phase1"
DEFAULT_PHASE2 = PROJECT_ROOT / "results" / "rebuilt" / "pulsar" / "phase2"
DEFAULT_SCREENING = (
    PROJECT_ROOT / "results" / "rebuilt" / "pulsar" / "phase2-screening"
)
DEFAULT_FINAL_VALIDATION = (
    PROJECT_ROOT / "results" / "rebuilt" / "pulsar" / "final-validation"
)
DEFAULT_OUTPUT = PROJECT_ROOT / "results" / "published" / "pulsar" / "complete"
PHASE1_DATA_FILES = (
    "pulsar_phase1_backlog_sensitivity.csv",
    "pulsar_phase1_backlog_sensitivity.json",
    "pulsar_phase1_cases.csv",
    "pulsar_phase1_cases.json",
    "pulsar_phase1_controlled_comparisons.csv",
    "pulsar_phase1_shortlist.csv",
    "pulsar_phase1_shortlist.json",
    "pulsar_phase1_summary.json",
    "pulsar_phase1_validation.json",
)
SCREENING_DATA_FILES = (
    "pulsar_phase2_candidate_selection.json",
    "pulsar_phase2_screening_cases.csv",
    "pulsar_phase2_screening_cases.json",
    "pulsar_phase2_screening_profiles.csv",
    "pulsar_phase2_screening_validation.json",
)
CONFIRMATION_DATA_FILES = (
    "pulsar_phase2_confirmation_validation.json",
    "pulsar_phase2_confirmed_cases.csv",
    "pulsar_phase2_confirmed_cases.json",
    "pulsar_phase2_repeated_cells.csv",
    "pulsar_phase2_confirmed_profiles.csv",
    "pulsar_phase2_final_selection.json",
)
FINAL_VALIDATION_DATA_FILES = (
    "pulsar_final_validation_validation.json",
    "pulsar_final_validation_cases.csv",
    "pulsar_final_validation_cases.json",
    "pulsar_final_validation_summary.csv",
    "pulsar_final_validation_summary.json",
    "pulsar_final_validation_recommendation.json",
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate one complete Apache Pulsar LaTeX report from verified "
            "Phase 1, repeated Phase 2, and final-validation artifacts."
        )
    )
    parser.add_argument("--phase1-dir", type=Path, default=DEFAULT_PHASE1)
    parser.add_argument("--phase2-dir", type=Path, default=DEFAULT_PHASE2)
    parser.add_argument("--screening-dir", type=Path, default=DEFAULT_SCREENING)
    parser.add_argument(
        "--final-validation-dir", type=Path, default=DEFAULT_FINAL_VALIDATION
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--screening-job-id")
    parser.add_argument("--confirmation-job-id")
    parser.add_argument(
        "--final-validation-job-id",
        action="append",
        help="Optional Slurm job ID override; may be supplied twice.",
    )
    parser.add_argument("--skip-figure", action="store_true")
    parser.add_argument(
        "--refresh-manifest-only",
        action="store_true",
        help="Refresh artifact_manifest.csv and SHA256SUMS after PDF compilation.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.refresh_manifest_only:
        if not args.output_dir.is_dir():
            raise ValueError(f"Complete-report output directory does not exist: {args.output_dir}")
        _write_artifact_manifest(args.output_dir)
        print(f"[pulsar-complete-report] refreshed checksums: {args.output_dir}")
        return 0
    phase1_tex_path = args.phase1_dir / "pulsar_phase1_report.tex"
    phase1_summary = _read_json(args.phase1_dir / "pulsar_phase1_summary.json")
    final_selection = _read_json(args.phase2_dir / "pulsar_phase2_final_selection.json")
    confirmation_validation = _read_json(
        args.phase2_dir / "pulsar_phase2_confirmation_validation.json"
    )
    screening_selection = _read_json(
        args.screening_dir / "pulsar_phase2_candidate_selection.json"
    )
    screening_profiles = _read_csv(
        args.screening_dir / "pulsar_phase2_screening_profiles.csv"
    )
    confirmed_profiles = _read_csv(
        args.phase2_dir / "pulsar_phase2_confirmed_profiles.csv"
    )
    repeated_cells = _read_csv(
        args.phase2_dir / "pulsar_phase2_repeated_cells.csv"
    )
    final_validation_available = all(
        (args.final_validation_dir / name).is_file()
        for name in FINAL_VALIDATION_DATA_FILES
    )
    final_validation = None
    final_cases: list[dict[str, str]] = []
    final_summaries: list[dict[str, str]] = []
    final_recommendation = None
    if final_validation_available:
        final_validation = _read_json(
            args.final_validation_dir / "pulsar_final_validation_validation.json"
        )
        final_cases = _read_csv(
            args.final_validation_dir / "pulsar_final_validation_cases.csv"
        )
        final_summaries = _read_csv(
            args.final_validation_dir / "pulsar_final_validation_summary.csv"
        )
        final_recommendation = _read_json(
            args.final_validation_dir
            / "pulsar_final_validation_recommendation.json"
        )
    if confirmation_validation.get("valid") is not True:
        raise ValueError("Phase 2 confirmation validation is not valid")
    if final_selection.get("status") != "profile_frozen":
        raise ValueError("Phase 2 has no frozen profile")
    if final_validation_available and final_validation.get("valid") is not True:
        raise ValueError("Final repeated workload validation is not valid")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    figures_dir = args.output_dir / "figures"
    figures_dir.mkdir(exist_ok=True)
    for source in (args.phase1_dir / "figures").glob("*"):
        if source.is_file():
            shutil.copy2(source, figures_dir / source.name)
    _copy_compact_data(args.phase1_dir, args.output_dir / "data" / "phase1", PHASE1_DATA_FILES)
    _copy_compact_data(
        args.screening_dir,
        args.output_dir / "data" / "phase2-screening",
        SCREENING_DATA_FILES,
    )
    _copy_compact_data(
        args.phase2_dir,
        args.output_dir / "data" / "phase2-confirmation",
        CONFIRMATION_DATA_FILES,
    )
    if final_validation_available:
        _copy_compact_data(
            args.final_validation_dir,
            args.output_dir / "data" / "final-validation",
            FINAL_VALIDATION_DATA_FILES,
        )
    figure_available = False
    if not args.skip_figure:
        figure_available = _write_phase2_figure(
            figures_dir / "pulsar_phase2_profile_decision.pdf",
            figures_dir / "pulsar_phase2_profile_decision.png",
            confirmed_profiles,
        )
    final_figure_available = False
    if final_validation_available and not args.skip_figure:
        final_figure_available = _write_final_validation_figure(
            figures_dir / "pulsar_final_validation_summary.pdf",
            figures_dir / "pulsar_final_validation_summary.png",
            final_summaries,
        )

    text = phase1_tex_path.read_text(encoding="utf-8")
    report_subject = (
        "Phase 1 workload screening, Phase 2 memory-profile validation, and "
        "final repeated workload validation"
        if final_validation_available
        else "Phase 1 workload screening and Phase 2 repeated memory-profile validation"
    )
    report_subtitle = (
        "Phase 1 Screening, Phase 2 Profile Tuning, and Final Repeated Workload Validation"
        if final_validation_available
        else "Phase 1 Workload Screening and Phase 2 Repeated Memory-Profile Validation"
    )
    text = text.replace(
        "pdftitle={Verified Exploratory Analysis of Apache Pulsar Phase 1}",
        "pdftitle={Complete Apache Pulsar Benchmark Analysis}",
    ).replace(
        "pdfsubject={120-configuration fixed-profile workload screening}",
        f"pdfsubject={{{report_subject}}}",
    )
    text = text.replace(
        "\\title{Verified Exploratory Analysis of Apache Pulsar Phase 1\\\\\n"
        "\\large 120-Configuration Fixed-Profile Workload Screening}",
        "\\title{Complete Apache Pulsar Benchmark Analysis\\\\\n"
        f"\\large {report_subtitle}}}",
    )
    text = _replace_section(
        text,
        "Introduction",
        "Executive Summary",
        _introduction_text(final_validation_available),
    )
    text = _replace_section(
        text,
        "Executive Summary",
        "Benchmark Goal and Scope",
        _executive_summary(
            phase1_summary,
            final_selection,
            confirmed_profiles,
            final_summaries=final_summaries,
            final_recommendation=final_recommendation,
        ),
    )
    old_resource_transition = (
        "Phase 2 should retain process RSS, heap, GC, exact "
        "\\texttt{ib0}, Pulsar ingress/egress, and add reliable "
        "total-direct-memory and BookKeeper queue telemetry if available."
    )
    new_resource_transition = (
        "Phase 2 retained process RSS, heap, GC, exact \\texttt{ib0}, "
        "Pulsar ingress/egress, partial NIO direct-buffer evidence, and tmpfs "
        "use. Reliable total-direct-memory and BookKeeper queue telemetry "
        "remained unavailable and is not estimated."
    )
    if old_resource_transition not in text:
        raise ValueError("Phase 1 resource transition sentence was not found")
    text = text.replace(old_resource_transition, new_resource_transition)
    text = text.replace(
        "Phase 2 must keep every workload field fixed within an anchor while "
        "varying only immutable Pulsar service profiles.",
        "Phase 2 kept every workload field fixed within each of the five "
        "selected anchors while varying only immutable Pulsar memory profiles.",
    )
    phase2_section = _phase2_section(
        screening_profiles,
        screening_selection,
        repeated_cells,
        confirmed_profiles,
        final_selection,
        figure_available,
    )
    final_section = (
        _final_validation_section(
            final_summaries,
            final_cases,
            final_recommendation,
            final_validation,
            final_figure_available,
        )
        if final_validation_available
        else ""
    )
    marker = "\\section{Limitations}"
    if marker not in text:
        raise ValueError("Limitations section marker was not found")
    inserted = phase2_section + ("\n\n" + final_section if final_section else "")
    text = text.replace(marker, inserted + "\n\n" + marker, 1)
    text = _replace_section(
        text,
        "Limitations",
        "Conclusion",
        _limitations_text(final_validation_available),
    )
    text = _replace_section(
        text,
        "Conclusion",
        None,
        _conclusion_text(
            phase1_summary,
            final_selection,
            confirmed_profiles,
            final_recommendation=final_recommendation,
        ),
        end_marker="\\appendix",
    )
    provenance = _phase2_provenance(
        screening_selection,
        final_selection,
        confirmation_validation,
        screening_job_id=args.screening_job_id,
        confirmation_job_id=args.confirmation_job_id,
    )
    if final_validation_available:
        provenance += _final_validation_provenance(
            final_validation,
            final_recommendation,
            job_ids=args.final_validation_job_id,
        )
    text = text.replace("\\end{document}", provenance + "\n\\end{document}", 1)
    output_tex = args.output_dir / "pulsar_complete_benchmark_report.tex"
    output_tex.write_text(text, encoding="utf-8")
    _write_artifact_manifest(args.output_dir)
    print(f"[pulsar-complete-report] TeX: {output_tex}")
    print(f"[pulsar-complete-report] frozen profile: {final_selection['frozen_profile_id']}")
    return 0


def _introduction_text(final_validation_available: bool) -> str:
    opening = r"""This report presents the Apache Pulsar campaign of the distributed messaging benchmark suite. Phase 1 screened 120 simultaneous producer/consumer workload configurations under one fixed service profile. Phase 2 then held five representative workloads constant while changing only explicit, checksum-backed JVM heap and direct-memory profiles. Screening nominated candidates; two additional randomized blocks supplied three observations per confirmed profile/workload cell before a profile was frozen."""
    if final_validation_available:
        opening += r""" The final stage reran ten historically selected configurations in five randomized blocks under that frozen profile, producing 50 repeated workload observations for the final recommendation. Because the qualification rule was corrected after execution, that immutable workload set differs from the retrospectively corrected Phase 1 shortlist; newly shortlisted configurations were not validated."""
    return opening + r"""

The method is qualification-first and conservative. Application throughput, record rate, sampled producer-to-consumer latency, exact post-drain record accounting, producer backlog at flush start, flush behavior, and supporting service/resource telemetry are reported separately. Kafka and Pulsar use the same application-level operational thresholds: backlog at most 5\%, flush at most 10~s, and failed sends at most 0.1\%. Post-drain missing, duplicate, and out-of-order records are eligibility and correctness checks. A high byte rate cannot compensate for an unqualified operational state. Phase 1 single observations are not pooled with later repetitions.

The system under test is Apache Pulsar 5.0.0-M1 in standalone mode on one service node, with colocated broker, BookKeeper, and metadata roles and RAM-backed storage. This milestone build and topology remain explicit limitations. The report identifies the best supported memory profile among the profiles and workloads actually tested; it does not claim a universal Pulsar optimum and does not make a Kafka/Pulsar comparison.

Figure~\ref{fig:architecture} describes the logical allocation used for every case. It is an architecture diagram rather than a result: solid horizontal arrows represent application records, dashed arrows represent monitoring collection, and the standalone service box contains the colocated broker, BookKeeper, and metadata roles.

\begin{figure}[!htbp]
\centering
\includegraphics[width=0.97\linewidth]{figures/pulsar_phase1_architecture.pdf}
\caption{Logical four-node Pulsar benchmark architecture. The producer/controller node launches MPI producer ranks and coordinates the case; the service node runs one standalone Pulsar process containing broker, BookKeeper, and metadata roles; the consumer node uses a shared subscription and coordinated post-flush drain; and the monitoring node collects native Pulsar, process, and node telemetry.}
\label{fig:architecture}
\end{figure}"""


def _executive_summary(
    phase1: dict[str, Any],
    selection: dict[str, Any],
    profiles: list[dict[str, str]],
    *,
    final_summaries: list[dict[str, str]],
    final_recommendation: dict[str, Any] | None,
) -> str:
    winner = _profile(profiles, selection["frozen_profile_id"])
    phase1_leader = _dict(phase1.get("primary_leader"))
    winner_sentence = (
        f"Phase 2 froze {_tt(selection['frozen_profile_id'])}. "
        + (
            "No tested candidate passed every predeclared improvement and "
            "non-regression rule, so the Phase 1 baseline was retained."
            if selection.get("baseline_retained")
            else (
                "It passed every predeclared rule with a geometric-mean "
                f"within-anchor median-throughput ratio of {_float(winner, 'geometric_mean_median_throughput_ratio_to_baseline'):.3f} "
                f"and p99-latency ratio of {_float(winner, 'geometric_mean_median_p99_latency_ratio_to_baseline'):.3f} relative to the baseline."
            )
        )
    )
    result = (
        f"Phase 1 produced {int(phase1['case_count'])} completed and eligible "
        f"workload observations, of which {int(phase1['qualified_case_count'])} "
        "qualified. The highest qualified byte-throughput observation was "
        f"{_tt(phase1_leader['config_id'])} at "
        f"{float(phase1_leader['balanced_mib_per_sec']):.3f}\\mibs. "
        "The separate record-rate and latency leaders demonstrate that byte "
        "throughput, records/s, and tail latency are distinct objectives. The "
        "Phase 1 primary rank orders eligible observations by qualification, "
        "balanced throughput, backlog, flush, failed sends, and configuration "
        "ID; p99 latency is secondary.\n\n"
        "Phase 2 first crossed six heap/direct-memory profiles with five fixed "
        "historically selected anchors in one randomized 30-case screening block. "
        "Those anchors came from the pre-correction selection and are not presented "
        "as the retrospectively corrected Phase 1 shortlist. The baseline "
        "and two selected candidates then ran in two additional randomized "
        "blocks, yielding three observations for each of 15 confirmed "
        "profile/anchor cells. Medians and interquartile ranges were calculated "
        "within each cell; unlike workloads were never pooled.\n\n"
        + winner_sentence
        + " Resource metrics remain supporting evidence and do not override "
        "eligibility or qualification."
    )
    if final_recommendation is not None:
        leader = _profile(final_summaries, str(final_recommendation["config_id"]))
        status = str(final_recommendation.get("status", ""))
        if status == "validated_recommendation":
            claim = "The qualification-first ranking yields the validated recommendation"
        elif status == "best_observed_but_not_fully_validated":
            claim = (
                "No workload met the full five-repeat recommendation gate; "
                "the highest-ranked observation is"
            )
        else:
            claim = "No sustainable recommendation was established; the highest-ranked scheduled workload is"
        result += (
            "\n\nFinal workload validation reran all ten configurations in the "
            "historical final-validation manifest "
            "five times under the frozen profile. "
            f"{claim} {_tt(str(final_recommendation['config_id']))}: "
            f"{int(float(leader['eligible_repeats']))}/5 eligible repeats, "
            f"{int(float(leader['qualified_repeats']))}/5 qualified repeats, "
            f"median balanced throughput {_fmt_num(leader.get('median_balanced_mib_per_sec'))}\\mibs, "
            f"and median producer-to-consumer p99 latency "
            f"{_fmt_seconds_from_us(leader.get('median_latency_p99_us'))}~s. Its "
            f"median backlog was {_fmt_num(leader.get('median_producer_backlog_percent'))}\\%, "
            f"median maximum flush was {_fmt_num(leader.get('median_max_flush_duration_sec'))}~s, "
            f"and maximum failed-send percentage was {_fmt_num(leader.get('max_failed_send_percent'))}\\%. "
            "The final rank uses qualified-repeat count, median balanced "
            "throughput, throughput IQR, backlog, flush, failed sends, and "
            "configuration ID; eligibility is prerequisite and p99 is secondary."
        )
    return result


def _phase2_section(
    screening_profiles: list[dict[str, str]],
    screening_selection: dict[str, Any],
    cells: list[dict[str, str]],
    profiles: list[dict[str, str]],
    final_selection: dict[str, Any],
    figure_available: bool,
) -> str:
    selected = set(screening_selection["confirmation_profiles"])
    screening_eligible = sum(
        int(float(row["eligible_count"])) for row in screening_profiles
    )
    screening_count = sum(
        int(float(row["case_count"])) for row in screening_profiles
    )
    screening_qualified = sum(
        int(float(row["qualified_count"])) for row in screening_profiles
    )
    confirmed_eligible = sum(
        int(float(row["eligible_repeat_count"])) for row in profiles
    )
    confirmed_count = sum(
        int(float(row["repeat_count"])) for row in profiles
    )
    confirmed_qualified = sum(
        int(float(row["qualified_repeat_count"])) for row in profiles
    )
    screening_rows = []
    for row in screening_profiles:
        screening_rows.append(
            f"{int(float(row['screening_rank']))} & {_tt(row['profile_id'])} & "
            f"{int(float(row['heap_gib']))}/{int(float(row['direct_memory_gib']))} & "
            f"{int(float(row['sustainable_qualified_count']))}/4 & "
            f"{_fmt_num(row.get('geometric_mean_throughput_ratio_to_baseline'))} & "
            f"{_fmt_num(row.get('geometric_mean_p99_latency_ratio_to_baseline'))} & "
            f"{_fmt_num(row.get('max_producer_backlog_percent'))} & "
            f"{'yes' if row['profile_id'] in selected else 'no'} \\\\"
        )
    cell_rows = []
    for row in cells:
        cell_rows.append(
            f"{_tt(_profile_key(row['profile_id']))} & {_tt(row['anchor'])} & "
            f"{int(float(row['qualified_repeats']))}/3 & "
            f"{_fmt_num(row['median_balanced_mib_per_sec'])} & "
            f"{_fmt_num(row['balanced_mib_per_sec_iqr'])} & "
            f"{float(row['median_balanced_records_per_sec']):,.0f} & "
            f"{float(row['median_latency_p99_us']) / 1_000_000.0:.3f} & "
            f"{_fmt_num(row['median_producer_backlog_percent'])} & "
            f"{_fmt_num(row['median_flush_sec'])} \\\\"
        )
    profile_rows = []
    for row in profiles:
        profile_rows.append(
            f"{int(float(row['final_rank']))} & {_tt(_profile_key(row['profile_id']))} & "
            f"{int(float(row['sustainable_qualified_repeat_count']))}/12 & "
            f"{_fmt_num(row['geometric_mean_median_throughput_ratio_to_baseline'])} & "
            f"{_fmt_num(row['geometric_mean_median_p99_latency_ratio_to_baseline'])} & "
            f"{_fmt_num(row['max_anchor_throughput_iqr_percent'])} & "
            f"{_fmt_num(row['max_process_rss_gib'])} & "
            f"{_fmt_num(row['max_tmpfs_used_percent'])} & "
            f"{'winner' if _bool(row.get('frozen_winner')) else ('pass' if _bool(row.get('candidate_passed')) else 'no')} \\\\"
        )
    winner_id = str(final_selection["frozen_profile_id"])
    winner = _profile(profiles, winner_id)
    screening_candidates = sorted(
        (
            row
            for row in screening_profiles
            if row["profile_id"] in selected
            and row["profile_id"] != "BASELINE_H16_D32"
        ),
        key=lambda row: int(float(row["screening_rank"])),
    )
    screening_interpretation = (
        "Screening nominated "
        + " and ".join(
            f"{_tt(row['profile_id'])} (throughput ratio "
            f"{_float(row, 'geometric_mean_throughput_ratio_to_baseline'):.3f}, "
            f"p99 ratio {_float(row, 'geometric_mean_p99_latency_ratio_to_baseline'):.3f})"
            for row in screening_candidates
        )
        + ". These are single-observation ratios used only to choose profiles "
        "for repetition; they are not a tuning conclusion."
    )
    outcome = (
        f"No candidate passed every rule, so the control profile {_tt(winner_id)} "
        "remained frozen."
        if final_selection.get("baseline_retained")
        else (
            f"Profile {_tt(winner_id)} passed every rule and was frozen. Its "
            "geometric mean of the four historical comparison-anchor median-throughput "
            f"ratios was {_float(winner, 'geometric_mean_median_throughput_ratio_to_baseline'):.3f}; "
            "the corresponding p99-latency ratio was "
            f"{_float(winner, 'geometric_mean_median_p99_latency_ratio_to_baseline'):.3f}."
        )
    )
    decision_interpretation = _decision_interpretation(profiles, winner_id)
    figure = ""
    if figure_available:
        figure = r"""
\begin{figure}[!htbp]
\centering
\includegraphics[width=0.94\linewidth]{figures/pulsar_phase2_profile_decision.pdf}
\caption{Repeated Phase 2 profile decision ratios. The left panel is the geometric mean, across the four historically designated profile-comparison anchors, of each profile's median balanced-throughput ratio to the baseline; values above the declared 1.03 line satisfy the throughput-improvement rule. The right panel is the analogous geometric mean p99-latency ratio; values at or below 1.10 satisfy the latency non-regression rule. Each anchor median contains screening block 1 and confirmation blocks 2 and 3. Ratios do not override eligibility, corrected qualification, record correctness, or resource checks.}
\label{fig:p2-profile-decision}
\end{figure}
"""
    return r"""\clearpage
\section{Phase 2 Memory-Profile Screening and Confirmation}
\label{sec:phase2}
Phase 2 tested whether bounded JVM memory changes improve sustainable performance without weakening correctness or tail latency. The service topology, Pulsar and Java versions, client settings, workload timings, the shared Kafka/Pulsar application-level qualification rule, RAM-backed storage, and exclusive four-node placement remained fixed. Only initial/maximum heap and maximum direct-memory limit changed. Total direct-memory use is not inferred from the configured limit.

\subsection{Screening Design}
Six immutable profiles formed a $3\times2$ design: heap was 16, 24, or 32~GiB and maximum direct memory was 32 or 48~GiB. The immutable historical manifest used five anchors selected under the original analysis: \texttt{cfg\_120}, \texttt{cfg\_101}, \texttt{cfg\_093}, \texttt{cfg\_089}, and \texttt{cfg\_001}. Every profile/anchor pair ran once in randomized order with seed \texttt{20260810}. Pulsar and its RAM-backed BookKeeper storage were restarted and cleared between cases. These are the workloads actually executed; the analysis does not claim that the retrospectively corrected Phase 1 shortlist was run.

Table~\ref{tab:p2-screening} presents the screening result after applying the corrected backlog rule to the historical cases. """ + (
        f"The generated case evidence contains {screening_eligible}/{screening_count} "
        f"eligible and {screening_qualified}/{screening_count} qualified screening "
        "observations. "
    ) + r"""Anchor-set Q is counted over the four profile-comparison anchors; \texttt{cfg\_089} remains a fifth transition anchor outside that count. Throughput and p99 columns are geometric means of within-anchor ratios to \texttt{BASELINE\_H16\_D32}. Max backlog is the largest per-case percentage of measurement-period publication attempts still pending at flush start. Screening selected candidates for repetition only.

\begin{table}[!htbp]
\centering
\scriptsize
\resizebox{\linewidth}{!}{%
\begin{tabular}{r l r r r r r r}
\toprule
Rank & Profile & Heap/direct (GiB) & Anchor-set Q & Throughput ratio & p99 ratio & Max backlog (\%) & Confirmed \\
\midrule
""" + "\n".join(screening_rows) + r"""
\bottomrule
\end{tabular}}
\caption{Single-observation Phase 2 memory-profile screening, recomputed with the shared Kafka/Pulsar application-level rule: producer backlog at most 5\%, flush at most 10~s, and failed sends at most 0.1\%. Ratios compare each profile with the baseline within the same four historically designated comparison workloads; Confirmed identifies the baseline and two candidates advanced to repeated validation.}
\label{tab:p2-screening}
\end{table}

""" + screening_interpretation + r"""

\subsection{Repeated Confirmation Method}
The baseline and two candidates ran in two additional randomized blocks with seed \texttt{20260811}. Each block contained all 15 profile/anchor pairs exactly once. Together with screening, each cell therefore contains three individual observations. """ + (
        f"Across these {confirmed_count} combined observations, "
        f"{confirmed_eligible} were eligible and {confirmed_qualified} qualified "
        "under the corrected rule. "
    ) + r"""For compact tables, a key such as \texttt{H24-D48} means 24~GiB of initial/maximum heap and a 48-GiB maximum direct-memory limit; \texttt{H16-D32} is \texttt{BASELINE\_H16\_D32}. Table~\ref{tab:p2-repeated-cells} reports the median of the three per-run metrics and the throughput interquartile range (IQR), $Q_3-Q_1$. Record-rate medians are calculated from each repeat's records/s value, not reconstructed from a median byte rate. A repeat qualifies when pending messages at flush start are at most 5\% of measurement-period publication attempts, maximum producer flush duration is at most 10~s, and failed publications are at most 0.1\% of attempted publications. Eligibility separately requires valid latency and zero post-drain missing, duplicate, or out-of-order records.

\begingroup
\scriptsize
\setlength{\tabcolsep}{3pt}
\begin{longtable}{llrrrrrrr}
\caption{Repeated Phase 2 profile/anchor cells. Q is corrected qualified repeats out of three; Median MiB/s and records/s are balanced producer/consumer application rates; IQR is the balanced-throughput interquartile range; p99 is sampled producer-to-consumer latency in seconds; Backlog is the median per-repeat producer backlog percentage; and Flush is median maximum producer flush duration in seconds.}\label{tab:p2-repeated-cells}\\
\toprule
Profile & Anchor & Q & Median MiB/s & IQR & Median records/s & p99 (s) & Backlog (\%) & Flush (s) \\
\midrule
\endfirsthead
\caption[]{Repeated Phase 2 profile/anchor cells (continued).}\\
\toprule
Profile & Anchor & Q & Median MiB/s & IQR & Median records/s & p99 (s) & Backlog (\%) & Flush (s) \\
\midrule
\endhead
""" + "\n".join(cell_rows) + r"""
\bottomrule
\end{longtable}
\endgroup

\subsection{Profile Decision and Resource Evidence}
The freeze rule required all 15 observations to be complete, healthy, eligible, latency-valid, and record-correct; corrected qualified-repeat count over the four historical comparison anchors could not be fewer than the baseline; the geometric mean of within-anchor median-throughput ratios had to be at least 1.03; the analogous p99 ratio could not exceed 1.10; failed sends could not regress; and maximum observed tmpfs use had to remain below 75\%. Thus p99 remains a Phase 2 profile non-regression criterion, but it is not part of the Phase 1 or final workload ranking. Table~\ref{tab:p2-decision} applies these rules. Max IQR is the largest anchor-level throughput IQR divided by that anchor's median, in percent. RSS is maximum standalone-process resident memory and includes heap, native/direct buffers, mapped storage, thread stacks, and colocated service components. Tmpfs is maximum observed RAM-filesystem use during a case.

\begin{table}[!htbp]
\centering
\scriptsize
\begin{tabularx}{\linewidth}{@{}r X r r r r r r l@{}}
\toprule
Rank & Profile & Q/12 & Throughput ratio & p99 ratio & Max IQR (\%) & Max RSS (GiB) & Max tmpfs (\%) & Decision \\
\midrule
""" + "\n".join(profile_rows) + r"""
\bottomrule
\end{tabularx}
\caption{Repeated Phase 2 profile decision after retrospective qualification correction. Q/12 counts qualified observations over four historically designated comparison anchors and three blocks. Ratios use the geometric mean of the four anchor-level medians relative to the repeated baseline. Decision is winner, pass, or no according to all profile-freeze rules.}
\label{tab:p2-decision}
\end{table}
""" + figure + "\n" + outcome + " " + decision_interpretation + r"""

The JVM NIO direct-buffer and managed-ledger pool metrics describe observable components of direct memory but are not a complete accounting of Pulsar/Netty off-heap allocation. Consequently, configured direct-memory limit, partial direct-pool telemetry, process RSS, GC rate, and tmpfs use are interpreted jointly as supporting evidence. They do not replace application correctness, eligibility, qualification, throughput, or latency.
"""


def _final_validation_section(
    summaries: list[dict[str, str]],
    cases: list[dict[str, str]],
    recommendation: dict[str, Any],
    validation: dict[str, Any],
    figure_available: bool,
) -> str:
    rows = []
    for row in sorted(summaries, key=lambda item: int(float(item["final_rank"]))):
        rows.append(
            f"{int(float(row['final_rank']))} & {_tt(row['config_id'])} & "
            f"{int(float(row['eligible_repeats']))}/5 & "
            f"{int(float(row['qualified_repeats']))}/5 & "
            f"{_fmt_num(row['median_balanced_mib_per_sec'])} & "
            f"{_fmt_num(row['balanced_mib_per_sec_iqr'])} & "
            f"{_fmt_integer(row['median_balanced_records_per_sec'])} & "
            f"{_fmt_seconds_from_us(row['median_latency_p99_us'])} & "
            f"{_fmt_num(row['median_producer_backlog_percent'])} & "
            f"{_fmt_num(row['median_max_flush_duration_sec'])} & "
            f"{_fmt_num(row['max_failed_send_percent'])} \\\\"
        )
    leader = _profile(summaries, str(recommendation["config_id"]))
    runner_up = next(
        (
            row
            for row in sorted(
                summaries, key=lambda item: int(float(item["final_rank"]))
            )
            if row["config_id"] != leader["config_id"]
        ),
        None,
    )
    runner_up_text = ""
    if runner_up is not None:
        runner_up_text = (
            f" The next-ranked workload was {_tt(runner_up['config_id'])} with "
            f"{int(float(runner_up['qualified_repeats']))}/5 qualified repeats "
            f"and median balanced throughput "
            f"{_fmt_num(runner_up.get('median_balanced_mib_per_sec'))}\\mibs."
        )
    status = str(recommendation.get("status", ""))
    ineligible_cases = [
        row
        for row in cases
        if str(row.get("eligible", "")).lower() in {"false", "0", "no"}
    ]
    ineligible_note = ""
    if len(ineligible_cases) == 1:
        ineligible = ineligible_cases[0]
        ineligible_note = (
            f" The sole ineligible observation was {_tt(ineligible['case_id'])} "
            f"({_tt(ineligible['config_id'])}): its maximum measured clock drift "
            f"of {_fmt_num(ineligible.get('clock_drift_us_max'))}~microseconds "
            "exceeded the 250-microsecond latency-validity limit, and its maximum "
            f"producer flush duration of "
            f"{_fmt_num(ineligible.get('max_flush_duration_sec'))}~s also exceeded "
            "the 10-s qualification threshold. The row remains in the evidence "
            "and contributes to neither eligible-repeat medians nor qualification."
        )
    elif ineligible_cases:
        ineligible_note = (
            f" {len(ineligible_cases)} observations were ineligible; their case-level "
            "validity evidence remains in the compact CSV/JSON data and they "
            "contribute to neither eligible-repeat medians nor qualification."
        )
    if status == "validated_recommendation":
        outcome_intro = "The final validated recommendation is"
        scope_sentence = (
            " This is the best repeatedly observed workload among the ten "
            "historically selected configurations for the tested single-standalone "
            "environment, not a universal Pulsar optimum."
        )
    elif status == "best_observed_but_not_fully_validated":
        outcome_intro = (
            "No workload met the full five-eligible/five-qualified recommendation "
            "gate; the highest-ranked observed workload is"
        )
        scope_sentence = (
            " This ordering is useful evidence but does not establish a fully "
            "validated workload recommendation."
        )
    else:
        outcome_intro = (
            "No sustainable workload recommendation was established; the "
            "highest-ranked scheduled workload is"
        )
        scope_sentence = (
            " This ordering must not be interpreted as a sustainable-performance "
            "recommendation."
        )
    figure = ""
    if figure_available:
        incomplete_rows = [
            row
            for row in summaries
            if int(float(row.get("eligible_repeats", 0))) < 5
            or int(float(row.get("qualified_repeats", 0))) < 5
        ]
        incomplete_note = ""
        if incomplete_rows:
            incomplete = sorted(
                incomplete_rows,
                key=lambda item: int(float(item["final_rank"])),
            )[0]
            incomplete_note = (
                f" {_tt(incomplete['config_id'])} provides the contrasting "
                f"transition-region case: it had {incomplete['eligible_repeats']}/5 "
                f"eligible but {incomplete['qualified_repeats']}/5 qualified repeats, "
                "so neither its byte rate nor its latency can promote it above fully "
                "qualified workloads."
            )
        figure = (
            r"""
Figure~\ref{fig:final-validation-summary} separates the three principal decision views rather than combining them into a score. The qualification panel establishes repeatability first; the throughput panel then compares eligible-repeat medians and IQRs; and the latency panel supplies a tail-latency comparison among those same configurations. """
            + "The latency panel is secondary and does not order the final rank. "
            + f"{_tt(leader['config_id'])} combines full qualification with the "
            "highest median balanced throughput among the configurations with "
            "five qualified repeats, which is why it leads the qualification-first ranking."
            + incomplete_note
            + r"""

\clearpage
\begin{figure}[!htbp]
\centering
\includegraphics[width=0.98\linewidth]{figures/pulsar_final_validation_summary.pdf}
\caption{Final repeated workload validation after retrospective qualification correction. The left panel counts corrected qualified repeats out of five. The center panel plots median balanced application throughput in MiB/s, with horizontal bars equal to half the eligible-repeat interquartile range on either side of the median. The right panel plots median sampled producer-to-consumer p99 latency in seconds. Each row is one of the ten historically executed configurations under the frozen profile; green identifies the final qualification-first leader. Panels use different axes and must not be read as a single composite score.}
\label{fig:final-validation-summary}
\end{figure}
"""
        )
    return r"""\clearpage
\section{Final Repeated Workload Validation}
\label{sec:final-validation}
After Phase 2 froze the service profile, the historical final-validation manifest ran \texttt{cfg\_001}, \texttt{cfg\_007}, \texttt{cfg\_049}, \texttt{cfg\_055}, \texttt{cfg\_063}, \texttt{cfg\_080}, \texttt{cfg\_089}, \texttt{cfg\_093}, \texttt{cfg\_101}, and \texttt{cfg\_120} in five fixed-seed randomized blocks (seed \texttt{20260812}); every block contained each configuration exactly once, for 50 cases in total. All cases used \texttt{BASELINE\_H16\_D32}, 15~s warm-up, 30~s measurement, up to 60~s coordinated drain, and deterministic one-in-ten latency sampling. This set was chosen before the qualification correction and differs from the retrospectively corrected Phase 1 shortlist in Table~\ref{tab:shortlist}. Newly shortlisted configurations were not validated.

A repeat is eligible only when its report is complete, backend health and clock-calibrated latency are valid, delivered-record accounting balances, and no missing, duplicate, or out-of-order record remains after drain. An eligible repeat qualifies when pending messages at flush start are at most 5\% of measurement-period publication attempts, maximum producer flush duration is at most 10~s, and failed publications are at most 0.1\% of attempted publications. Post-drain correctness is not substituted for producer backlog.

Table~\ref{tab:final-validation-ranking} aggregates the five individual observations per configuration. E/5 and Q/5 are eligible and corrected qualified repeat counts. Eligibility is a prerequisite: only eligible repeats contribute to the aggregate performance metrics, but E/5 is not a performance-ranking field. Median MiB/s is the median balanced rate, $\min(T_{\mathrm{producer}},T_{\mathrm{consumer}})$, over eligible repeats; records/s applies the same minimum to per-repeat record rates. IQR is $Q_3-Q_1$ for eligible balanced MiB/s. p99 is the median of the eligible, clock-valid per-repeat producer-to-consumer 99th-percentile latencies and remains secondary. Backlog and Flush are medians of the eligible per-repeat producer-backlog percentages and maximum producer-client flush durations; Failed is the maximum eligible failed-publication percentage. The ranking is lexicographic: qualified-repeat count, median balanced MiB/s, throughput IQR, median backlog, median flush, maximum failed-publication fraction, and configuration ID. Thus p99 latency or raw throughput cannot rescue an overdriven configuration.

\begingroup
\scriptsize
\setlength{\tabcolsep}{3.5pt}
\begin{longtable}{rllrrrrrrrr}
\caption{Final five-repeat historical-workload ranking under the frozen Pulsar profile and shared Kafka/Pulsar application-level qualification rule. Rates, p99 latency, backlog, and flush are medians of eligible individual repeats; IQR quantifies within-configuration balanced-throughput spread; Failed is the maximum eligible failed-publication percentage.}\label{tab:final-validation-ranking}\\
\toprule
Rank & Configuration & E/5 & Q/5 & Median MiB/s & IQR & Median records/s & p99 (s) & Backlog (\%) & Flush (s) & Failed (\%) \\
\midrule
\endfirsthead
\caption[]{Final five-repeat workload ranking (continued).}\\
\toprule
Rank & Configuration & E/5 & Q/5 & Median MiB/s & IQR & Median records/s & p99 (s) & Backlog (\%) & Flush (s) & Failed (\%) \\
\midrule
\endhead
""" + "\n".join(rows) + r"""
\bottomrule
\end{longtable}
\endgroup
""" + figure + (
        f"\nThe validator found {int(validation['observed_case_count'])}/50 required "
        f"reports, {int(validation['eligible_case_count'])} eligible repeats, and "
        f"{int(validation['qualified_case_count'])} qualified repeats. "
        + ineligible_note
        + f" {outcome_intro} {_tt(str(recommendation['config_id']))}: "
        f"{int(float(leader['eligible_repeats']))}/5 eligible and "
        f"{int(float(leader['qualified_repeats']))}/5 qualified repeats, median "
        f"balanced throughput {_fmt_num(leader.get('median_balanced_mib_per_sec'))}\\mibs, "
        f"median record rate {_fmt_integer(leader.get('median_balanced_records_per_sec'))}~records/s, "
        f"median p99 latency {_fmt_seconds_from_us(leader.get('median_latency_p99_us'))}~s, "
        f"median backlog {_fmt_num(leader.get('median_producer_backlog_percent'))}\\%, "
        f"median maximum flush {_fmt_num(leader.get('median_max_flush_duration_sec'))}~s, "
        f"and maximum failed sends {_fmt_num(leader.get('max_failed_send_percent'))}\\%."
        + runner_up_text
        + scope_sentence
        + "\n\n\\clearpage\n"
    )


def _limitations_text(final_validation_available: bool) -> str:
    final_note = (
        r"""\item Final workload validation contains five repeats for each of ten historically selected configurations. This supports medians, IQRs, and qualification frequency for those workloads, but it is not a repeat of all 120 Phase 1 configurations or a validation of the retrospectively corrected shortlist.
"""
        if final_validation_available
        else ""
    )
    return r"""\begin{itemize}
\item Phase 1 has one observation per workload. Its rankings select anchors and bounded single-run observations; they do not provide within-workload confidence intervals.
\item Phase 2 repeats only five representative workloads and six memory profiles. Confirmation contains three observations per confirmed profile/anchor cell, which supports medians and IQRs but remains a modest sample.
""" + final_note + r"""\item The mixed Phase 1 design is not factorial. Associations among mixed workloads are confounded and are not causal parameter effects. The Phase 2 $3\times2$ memory screen is factorial only for heap and direct-memory limits within the five selected anchors.
\item Latency is sampled producer-to-consumer latency under maximum offered load, not a fixed-rate service-latency curve or idle-response metric.
\item The standalone process colocates broker, BookKeeper, and metadata roles on one node with RAM-backed storage and quorum 1/1/1. Results do not generalize directly to a replicated production Pulsar cluster.
\item Apache Pulsar 5.0.0-M1 is a milestone build. Future Pulsar and client versions may differ.
\item Monitoring summaries are broad-window samples. NIO direct-buffer and managed-ledger pool metrics are partial; they do not establish total direct-memory use. Detailed BookKeeper queue/cache evidence also remains limited.
\item The frozen profile is the best supported choice among the tested memory profiles for this GWDG environment. Thread, cache, disk, replication, and alternative garbage-collector tuning were not part of this campaign.
\item Kafka and Pulsar use the same application-level backlog, flush, and failed-send qualification thresholds. Cross-system interpretation still requires explicit normalization of delivery semantics, durability, topology, client behavior, and software versions.
\end{itemize}"""


def _conclusion_text(
    phase1: dict[str, Any],
    selection: dict[str, Any],
    profiles: list[dict[str, str]],
    *,
    final_recommendation: dict[str, Any] | None,
) -> str:
    winner_id = str(selection["frozen_profile_id"])
    winner = _profile(profiles, winner_id)
    phase1_leader = _dict(phase1.get("primary_leader"))
    phase2 = (
        f"No candidate met every improvement and non-regression criterion, so "
        f"the tested baseline {_tt(winner_id)} remains frozen."
        if selection.get("baseline_retained")
        else (
            f"{_tt(winner_id)} passed every predeclared rule and was frozen, with "
            f"a geometric-mean median-throughput ratio of "
            f"{_float(winner, 'geometric_mean_median_throughput_ratio_to_baseline'):.3f} "
            f"and p99 ratio of {_float(winner, 'geometric_mean_median_p99_latency_ratio_to_baseline'):.3f} "
            "relative to the repeated baseline across the four historically designated comparison anchors."
        )
    )
    result = (
        f"Pulsar Phase 1 is complete: {int(phase1['case_count'])} unique workloads "
        f"produced {int(phase1['eligible_case_count'])} eligible and "
        f"{int(phase1['qualified_case_count'])} qualified observations. The "
        f"highest qualified byte-throughput observation was {_tt(phase1_leader['config_id'])} "
        f"at {float(phase1_leader['balanced_mib_per_sec']):.3f}\\mibs.\n\n"
        "Phase 2 completed a six-profile screening and repeated confirmation "
        "of the baseline plus two candidates over five fixed anchors. "
        + phase2
        + " The conclusion is limited to Apache Pulsar 5.0.0-M1, the declared "
        "single-standalone GWDG topology, RAM-backed storage, and tested workloads. "
        "The campaign completes the Pulsar benchmark on its own terms; a fair "
        "cross-system comparison remains separate work."
    )
    if final_recommendation is not None:
        status = str(final_recommendation.get("status", ""))
        if status == "validated_recommendation":
            claim = "The qualification-first result validates"
            qualification = (
                "This recommendation remains bounded to the historically executed workload set, "
                "profile, software versions, topology, and GWDG environment."
            )
        elif status == "best_observed_but_not_fully_validated":
            claim = "The qualification-first result ranks"
            qualification = (
                "Because the full five-repeat gate was not met, this is a highest-ranked "
                "observation rather than a validated recommendation."
            )
        else:
            claim = "The qualification-first result places"
            qualification = (
                "No sustainable recommendation was established by these repetitions."
            )
        result += (
            "\n\nFinal workload validation added five randomized observations for "
            "each of the ten configurations in the historical final-validation manifest under the frozen "
            f"profile. {claim} {_tt(str(final_recommendation['config_id']))} first, with "
            f"{int(final_recommendation['eligible_repeats'])}/5 eligible and "
            f"{int(final_recommendation['qualified_repeats'])}/5 qualified repeats, "
            f"median balanced throughput "
            f"{_fmt_num(final_recommendation.get('median_balanced_mib_per_sec'))}\\mibs, "
            f"and median p99 latency "
            f"{_fmt_seconds_from_us(final_recommendation.get('median_latency_p99_us'))}~s. "
            "The recommendation rank does not use p99; it uses qualified-repeat "
            "count, balanced throughput, throughput variability, backlog, flush, "
            "failed sends, and configuration ID after the eligibility prerequisite. "
            + qualification
        )
    return result


def _phase2_provenance(
    screening: dict[str, Any],
    selection: dict[str, Any],
    validation: dict[str, Any],
    *,
    screening_job_id: str | None,
    confirmation_job_id: str | None,
) -> str:
    screen_root = _dict(screening.get("source_evidence")).get(
        "screening_results_root", "not recorded"
    )
    confirm_root = _dict(selection.get("source_evidence")).get(
        "confirmation_results_root", "not recorded"
    )
    screen_jobs = screening_job_id or ", ".join(
        str(item)
        for item in _dict(screening.get("source_evidence")).get(
            "slurm_job_ids", []
        )
    ) or "not recorded"
    confirm_jobs = confirmation_job_id or ", ".join(
        str(item)
        for item in _dict(selection.get("source_evidence")).get(
            "slurm_job_ids", []
        )
    ) or "not recorded"
    return r"""
\clearpage
\section{Phase 2 Provenance}
\label{app:p2-provenance}
The Phase 2 screening raw run ID was """ + _nolinkurl(str(screen_root)) + r""" (Slurm job """ + _tt(screen_jobs) + r""") and the confirmation raw run ID was """ + _nolinkurl(str(confirm_root)) + r""" (Slurm job """ + _tt(confirm_jobs) + r"""). The screening and confirmation manifests used fixed seeds \texttt{20260810} and \texttt{20260811}, respectively. Every case records backend/profile identity, immutable profile SHA-256, effective configuration, software/runtime snapshots, final application metrics, latency calibration, backend health, monitoring summaries, and post-case storage reset evidence.

The final confirmation validator observed """ + str(int(validation["observed_case_count"])) + r""" of 30 required confirmation reports and returned \texttt{valid=true}. Compact Phase 2 CSV/JSON artifacts and their \texttt{SHA256SUMS} file accompany this report. Phase 1 and Phase 2 values remain separate datasets; Phase 1 is historical workload screening, while Phase 2 contains service-profile screening and repetitions.
"""


def _final_validation_provenance(
    validation: dict[str, Any],
    recommendation: dict[str, Any],
    *,
    job_ids: list[str] | None,
) -> str:
    source = _dict(recommendation.get("source_evidence"))
    recorded_jobs = [str(item) for item in source.get("slurm_job_ids", [])]
    jobs = ", ".join(job_ids or recorded_jobs) or "not recorded"
    roots = ", ".join(str(item) for item in source.get("results_roots", []))
    return r"""
\clearpage
\section{Final Validation Provenance}
\label{app:final-validation-provenance}
The final repeated workload validation used fixed seed \texttt{20260812} and two sequential batch jobs containing randomized blocks 1--3 and 4--5. The raw run IDs were """ + _nolinkurl(roots or "not recorded") + r""" and the Slurm job IDs were """ + _tt(jobs) + r""". The immutable campaign manifest and plan SHA-256 values are \texttt{""" + str(source.get("manifest_sha256", "not recorded")) + r"""} and \texttt{""" + str(source.get("plan_sha256", "not recorded")) + r"""}, respectively.

The strict validator observed """ + str(int(validation["observed_case_count"])) + r""" of 50 required reports across five complete randomized blocks and returned \texttt{valid=true}. Compact per-case CSV/JSON, per-configuration summaries, the recommendation decision, validation record, figures, and checksums accompany this report under \texttt{data/final-validation}. Phase 1 screening, Phase 2 profile comparison, and final workload repetitions remain separately identifiable evidence and are not pooled across unlike conditions.
"""


def _write_phase2_figure(
    pdf_path: Path, png_path: Path, profiles: list[dict[str, str]]
) -> bool:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return False
    ordered = sorted(profiles, key=lambda row: int(float(row["final_rank"])), reverse=True)
    labels = [_profile_key(row["profile_id"]) for row in ordered]
    throughput = [
        _float(row, "geometric_mean_median_throughput_ratio_to_baseline")
        for row in ordered
    ]
    latency = [
        _float(row, "geometric_mean_median_p99_latency_ratio_to_baseline")
        for row in ordered
    ]
    colors = ["#166534" if _bool(row.get("frozen_winner")) else "#1d4ed8" for row in ordered]
    fig, axes = plt.subplots(1, 2, figsize=(10.4, 3.8), constrained_layout=True)
    positions = list(range(len(labels)))
    axes[0].scatter(throughput, positions, c=colors, s=64, zorder=3)
    axes[0].axvline(1.0, color="#111827", linewidth=1)
    axes[0].axvline(1.03, color="#b45309", linestyle="--", linewidth=1.2)
    axes[0].set_xlabel("Geometric-mean median throughput ratio")
    axes[0].set_title("Balanced throughput")
    axes[1].scatter(latency, positions, c=colors, s=64, zorder=3)
    axes[1].axvline(1.0, color="#111827", linewidth=1)
    axes[1].axvline(1.10, color="#b45309", linestyle="--", linewidth=1.2)
    axes[1].set_xlabel("Geometric-mean median p99 ratio")
    axes[1].set_title("End-to-end p99 latency")
    for axis, values, threshold in (
        (axes[0], throughput, 1.03),
        (axes[1], latency, 1.10),
    ):
        axis.set_yticks(positions, labels)
        lower = min([1.0, threshold, *values])
        upper = max([1.0, threshold, *values])
        margin = max(0.015, (upper - lower) * 0.18)
        axis.set_xlim(lower - margin, upper + margin)
        axis.grid(axis="x", alpha=0.22)
        axis.set_axisbelow(True)
    fig.savefig(pdf_path, bbox_inches="tight")
    fig.savefig(png_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return True


def _write_final_validation_figure(
    pdf_path: Path, png_path: Path, summaries: list[dict[str, str]]
) -> bool:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return False
    ordered = sorted(
        summaries,
        key=lambda row: int(float(row["final_rank"])),
        reverse=True,
    )
    numeric_fields = (
        "qualified_repeats",
        "median_balanced_mib_per_sec",
        "balanced_mib_per_sec_iqr",
        "median_latency_p99_us",
    )
    if any(
        row.get(field) in (None, "", "None")
        for row in ordered
        for field in numeric_fields
    ):
        return False
    labels = [row["config_id"] for row in ordered]
    positions = list(range(len(ordered)))
    qualified = [int(float(row["qualified_repeats"])) for row in ordered]
    throughput = [float(row["median_balanced_mib_per_sec"]) for row in ordered]
    throughput_iqr = [float(row["balanced_mib_per_sec_iqr"]) for row in ordered]
    latency = [float(row["median_latency_p99_us"]) / 1_000_000.0 for row in ordered]
    colors = [
        "#166534" if int(float(row["final_rank"])) == 1 else "#1d4ed8"
        for row in ordered
    ]
    fig, axes = plt.subplots(1, 3, figsize=(12.2, 5.2), constrained_layout=True)
    axes[0].barh(positions, qualified, color=colors)
    axes[0].set_xlim(0, 5.25)
    axes[0].set_xlabel("Qualified repeats (out of 5)")
    axes[1].errorbar(
        throughput,
        positions,
        xerr=[value / 2.0 for value in throughput_iqr],
        fmt="o",
        color="#1d4ed8",
        ecolor="#64748b",
        capsize=3,
    )
    axes[1].set_xlabel("Median balanced MiB/s (half-IQR bars)")
    axes[2].scatter(latency, positions, c=colors, s=46)
    axes[2].set_xlabel("Median producer-to-consumer p99 (s)")
    for axis in axes:
        axis.set_yticks(positions, labels)
        axis.grid(axis="x", alpha=0.22)
        axis.set_axisbelow(True)
    fig.savefig(pdf_path, bbox_inches="tight")
    fig.savefig(png_path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return True


def _replace_section(
    text: str,
    section: str,
    next_section: str | None,
    content: str,
    *,
    end_marker: str | None = None,
) -> str:
    start_marker = f"\\section{{{section}}}"
    start = text.find(start_marker)
    if start < 0:
        raise ValueError(f"Section not found: {section}")
    body_start = start + len(start_marker)
    marker = end_marker or f"\\section{{{next_section}}}"
    end = text.find(marker, body_start)
    if end < 0:
        raise ValueError(f"End marker not found for section: {section}")
    return text[:body_start] + "\n" + content.strip() + "\n\n" + text[end:]


def _write_artifact_manifest(output_dir: Path) -> None:
    checksum_path = output_dir / "SHA256SUMS"
    manifest_path = output_dir / "artifact_manifest.csv"
    latex_transient_suffixes = {".aux", ".log", ".out", ".toc"}
    paths = sorted(
        path
        for path in output_dir.rglob("*")
        if path.is_file()
        and path not in {checksum_path, manifest_path}
        and path.suffix.lower() not in latex_transient_suffixes
    )
    with manifest_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(("artifact", "bytes", "sha256"))
        for path in paths:
            writer.writerow(
                (
                    path.relative_to(output_dir).as_posix(),
                    path.stat().st_size,
                    _sha256(path),
                )
            )
    checksum_paths = [*paths, manifest_path]
    checksum_path.write_text(
        "".join(
            f"{_sha256(path)}  {path.relative_to(output_dir).as_posix()}\n"
            for path in checksum_paths
        ),
        encoding="utf-8",
    )


def _copy_compact_data(source_dir: Path, output_dir: Path, names: tuple[str, ...]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for name in names:
        source = source_dir / name
        if not source.is_file():
            raise ValueError(f"Required compact report input is missing: {source}")
        shutil.copy2(source, output_dir / name)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def _profile(rows: list[dict[str, str]], profile_id: str) -> dict[str, str]:
    return next(
        row
        for row in rows
        if row.get("profile_id") == profile_id or row.get("config_id") == profile_id
    )


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _float(row: dict[str, Any], key: str) -> float:
    return float(row[key])


def _fmt_num(value: Any) -> str:
    if value in (None, "", "None"):
        return "n/a"
    return f"{float(value):.3f}"


def _fmt_integer(value: Any) -> str:
    if value in (None, "", "None"):
        return "n/a"
    return f"{float(value):,.0f}"


def _fmt_seconds_from_us(value: Any) -> str:
    if value in (None, "", "None"):
        return "n/a"
    return f"{float(value) / 1_000_000.0:.3f}"


def _bool(value: Any) -> bool:
    return value is True or str(value).strip().lower() in {"1", "true", "yes"}


def _tt(value: str) -> str:
    escaped = value.replace("_", r"\_").replace("%", r"\%")
    return f"\\texttt{{{escaped}}}"


def _nolinkurl(value: str) -> str:
    return f"\\nolinkurl{{{value}}}"


def _profile_key(profile_id: str) -> str:
    if profile_id == "BASELINE_H16_D32":
        return "H16-D32"
    marker = "HEAP"
    direct_marker = "_DIRECT"
    if marker in profile_id and direct_marker in profile_id:
        heap = profile_id.split(marker, 1)[1].split(direct_marker, 1)[0]
        direct = profile_id.split(direct_marker, 1)[1]
        if heap.isdigit() and direct.isdigit():
            return f"H{heap}-D{direct}"
    return profile_id


def _decision_interpretation(
    profiles: list[dict[str, str]], winner_id: str
) -> str:
    baseline = _profile(profiles, "BASELINE_H16_D32")
    baseline_qualified = int(float(baseline["sustainable_qualified_repeat_count"]))
    baseline_failed = _float(baseline, "max_failed_send_percent")
    sentences: list[str] = []
    candidates = sorted(
        (item for item in profiles if item["profile_id"] != "BASELINE_H16_D32"),
        key=lambda item: int(float(item["final_rank"])),
    )
    for row in candidates:
        reasons: list[str] = []
        if not _bool(row.get("all_required_valid")):
            reasons.append("not all 15 required observations were valid")
        qualified = int(float(row["sustainable_qualified_repeat_count"]))
        if qualified < baseline_qualified:
            reasons.append(
                f"its sustainable qualified count was {qualified}, below the "
                f"baseline's {baseline_qualified}"
            )
        throughput = _float(
            row, "geometric_mean_median_throughput_ratio_to_baseline"
        )
        if throughput < 1.03:
            reasons.append(
                f"its throughput ratio was {throughput:.3f}, below the 1.03 "
                "requirement"
            )
        latency = _float(
            row, "geometric_mean_median_p99_latency_ratio_to_baseline"
        )
        if latency > 1.10:
            reasons.append(
                f"its p99 ratio was {latency:.3f}, above the 1.10 limit"
            )
        failed = _float(row, "max_failed_send_percent")
        if failed > baseline_failed + 1e-12:
            reasons.append(
                f"its maximum failed-publication fraction was {failed:.3f}\\%, "
                f"above the baseline's {baseline_failed:.3f}\\%"
            )
        tmpfs = _float(row, "max_tmpfs_used_percent")
        if tmpfs >= 75.0:
            reasons.append(
                f"its maximum tmpfs use was {tmpfs:.3f}\\%, not below 75\\%"
            )
        if row["profile_id"] == winner_id:
            result = "passed all candidate rules and became the frozen profile"
        elif reasons:
            result = "did not pass because " + "; ".join(reasons)
        else:
            result = "passed the candidate rules but ranked behind the frozen profile"
        sentences.append(f"{_tt(row['profile_id'])} {result}.")
    return " ".join(sentences)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
