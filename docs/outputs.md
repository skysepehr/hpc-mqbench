# Kafka workflow outputs

A completed reproducible Kafka workflow produces measurements **and automatic
analysis**, not just monitoring CSV files. It validates cases, classifies their
delivery pressure, selects workloads and broker profiles, summarizes repeated
runs, and packages the recommendation with provenance.

This page describes the current Kafka workflow invoked through
`scripts/run_reproducible_benchmark.sh`. A partial phase produces only the
artifacts for the stages it completes. The workflow does not generate a paper,
LaTeX, or PDF.

## Where to find the results

With the default workflow output root and a run ID of `my-run`:

```text
results/workflows/kafka/my-run/                   Workflow state and analysis
results/sweeps/kafka_v1_reproducible/my-run-v1/    V1 case evidence
results/tuning_v2/my-run-v2/                      V2 case evidence
```

The case evidence is stored separately from the workflow directory. Consult
`workflow_state.json` and the finalized `source_case_index.csv` or `.json` for
the selected reports, including any repair runs. Preserve the referenced case
directories when archiving a campaign; a comparison bundle is not a copy of all
raw evidence. See the [Kafka workflow implementation](../src/benchmark/backends/kafka/workflow.py).

## What each case retains

| Artifact, relative to the case directory | Contents |
| --- | --- |
| `benchmark_result.json` | Workload result, including per-rank and aggregated counters, timing, configuration, and latency validation. |
| `case_config_snapshot.json` | Configuration used for the case. |
| `final_report.json` | Enriched result: workload measurements, common metrics, eligibility and qualification, available monitoring, health and inventory evidence, and diagnostic flags. |
| `runtime/benchmark_events.json` | Ordered workload and orchestration events. |
| `monitoring/raw/*.json`, `monitoring/csv/*.csv` | Available Prometheus query responses and metric samples. |
| `monitoring/broker_process_*.csv` | Broker process, interface-traffic, and tmpfs samples when that collector is enabled. |
| `monitoring/monitoring_summary.json` | Collection window, metric summaries, missing metrics, and collection errors. |
| `runtime/`, `data/`, `logs/` | Available backend health, effective broker-profile/runtime snapshots, system inventory, environment information, and execution logs. |

The workload result, configuration, final report, and event timeline also have
copies under `data/`. Monitoring and inventory availability depend on the
configured collectors and their successful execution; missing evidence must not
be interpreted as a zero measurement.

Latency is retained as histogram buckets and summary statistics, including
sample count, mean, extrema, and percentiles. It is **not** an individual-message
latency trace. Record checks and aggregate counts likewise do not constitute a
complete end-to-end reconciliation of every record identity.

Sources: [case result writer](../src/benchmark/benchmark_controller.py),
[report builder](../src/benchmark/report_builder.py),
[latency histogram](../src/benchmark/record_envelope.py), and
[artifact collection](../scripts/collect_results.sh).

## Analysis performed automatically

Paths below are relative to `results/workflows/kafka/<run-id>/`.

| Directory | Main outputs and purpose |
| --- | --- |
| `analysis/v1/phase1/` | `campaign_cases.csv/json`, `campaign_validation.json`, `sweep_summary.csv/json`, and `validation_shortlist.csv/json`: completeness/contract checks, case classification, screening ranks, and the validation shortlist. |
| `analysis/v1/validation/`, `analysis/v2/final-validation/` | Case tables and validity report; `validation_repeats_by_case.csv/json` and `validation_summary_by_original.csv/json`: individual repeats and ranked summaries per workload. |
| `analysis/v2/complete-profile/` | `kafka_broker_tuning_v2_cases.csv/json`, `kafka_broker_tuning_v2_final_ranking.csv`, and `kafka_broker_tuning_v2_decisions.json`: profile/resource evidence, final workload ranking, instrumentation checks, and profile-selection decisions. Earlier V2 stages retain their own analysis directories. |
| `artifacts/v1/`, `artifacts/v2/` | Validated comparison bundles described below. |
| `artifacts/` | Workflow-level `final_report.json`, `workflow_state.json`, `software_versions.json`, `provenance.json`, `slurm_job_ids.csv`, `workflow_stages.csv`, `artifact_manifest.csv`, and `SHA256SUMS`. |
| `inputs/`, `slurm/`, `logs/` | Generated configurations/manifests, submission records, and command history. |

Repeated-run summaries include eligible and qualified counts, throughput median,
mean, minimum, maximum, IQR, standard deviation and coefficient of variation,
plus backlog, flush, failed-send and p99-latency summaries. Numerical summaries
use **eligible repeats, including overdriven repeats**. Ranking first maximizes
qualified-repeat count, then median balanced throughput, and uses IQR, backlog,
flush duration, failed-send percentage and configuration ID as tie-breakers.

Profile analysis summarizes broker process and JMX samples within the producer
measurement interval, ending at send-loop completion. It retains CPU, memory,
tmpfs, traffic, queue, request-timing and garbage-collection indicators when
available. A last queue sample from that interval does not establish the queue
state after flush or drain.

These analyses are invoked by the workflow itself; users do not need to run them
manually. Sources: [campaign analyzer](../scripts/analyze_kafka_reproducible_campaign.py),
[profile analyzer](../scripts/analyze_broker_tuning_v2.py), and
[workflow stage calls](../src/benchmark/backends/kafka/workflow.py).

## Final comparison bundles

Each completed `artifacts/v1/` or `artifacts/v2/` bundle contains:

```text
phase1_cases.csv                  phase1_cases.json
validation_repeats.csv            validation_repeats.json
validation_summary.csv            validation_summary.json
validation_shortlist.csv          validation_shortlist.json
phase1_manifest.csv               validation_manifest.csv
phase1_validation.json            validation_validation.json
source_case_index.csv             source_case_index.json
final_report.json
artifact_manifest.csv
SHA256SUMS
```

Here `final_report.json` records the campaign recommendation and ranking rule.
It differs from both the detailed per-case file and the workflow-level file of
the same name. Profile/resource summaries remain in the stage-analysis
directories rather than being copied into this bundle.

Source: [result finalizer](../scripts/finalize_reproducible_results.py).

## Report modes and downstream work

| Invocation or mode | Presentation behavior |
| --- | --- |
| Reproducible workflow | Forces `BENCHMARK_REPORT_MODE=machine`, disables monitoring plots, and invokes the profile analyzer with `--machine-only`. Completion requires machine-readable results, not presentation files. |
| Standalone case, default `full` mode | Also renders Markdown, HTML, and benchmark graphs for Kafka. Monitoring graphs depend on the monitoring configuration. |
| `light` mode, the batch-script default | Also writes Markdown, while skipping per-case benchmark graphs and HTML. Monitoring-graph settings are separate. |
| Standalone `machine` mode | Writes the canonical JSON and event artifacts without per-case Markdown, HTML, or benchmark graphs. Set `SKIP_MONITORING_GRAPHS=1` separately to suppress monitoring plots. |

The repository also contains
[`scripts/generate_complete_analysis.py`](../scripts/generate_complete_analysis.py)
for downstream descriptive statistics, controlled parameter contrasts,
correlations, exploratory models, Pareto comparisons, qualification-threshold
sensitivity, resource-data-quality checks, and presentation files. The
reproducible workflow does **not** invoke that script. Creating scientific
figures or a manuscript from retained results is separate from running the
benchmark.

## Interpreting the evidence

- **Backlog** is unresolved producer delivery callbacks at flush start divided
  by measurement-period send attempts under `qualification.application.v1`.
  It is neither consumer-group offset lag nor broker request-queue depth.
- Diagnostic flags are threshold-based inspection aids, not proof of a physical
  bottleneck. There is a known missing-capacity diagnostic defect in
  [`html_report.py`](../src/benchmark/html_report.py): `_network_path_comparison`
  returns placeholder objects when path-capacity measurements are absent, and
  `_build_bottleneck_analysis` can then emit a conclusion that Kafka traffic is
  close to measured path capacity. Check the comparison status and numerical
  evidence; do not use that generated sentence as a finding. This diagnostic
  enrichment also runs in machine mode despite the module's name.
- Missing CPU/RAM data can produce saturation flags set to `false` through
  zero-valued helper defaults. Such a flag does not establish that the resource
  was measured and found unsaturated.
- `software_versions.json` records Python, adapter, Git, Java, Slurm and MPI
  command versions where available. It is not a complete Kafka/client-library
  version inventory. The workflow's Git provenance identifies the checkout
  inspected at finalization; preserve campaign-time software and environment
  records for publication reproducibility. See
  [workflow provenance collection](../src/benchmark/workflow/engine.py).
