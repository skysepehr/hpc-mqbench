# Companion evidence and manuscript map

This guide maps the retained Kafka evidence to the included manuscript snapshot.
Publication and access status are recorded in `README.md` and `PROVENANCE.json`.

All paths below are relative to this folder unless explicitly called repository
paths. Stable LaTeX labels identify the tables and figures; their displayed
numbers may change during revision. The measurements are historical retained
evidence. Files under `supplement/` are manuscript checks or reviewed
configuration copies, not outputs of a new benchmark run.

## Inventory and audit boundary

| Material | Local location and scope |
| --- | --- |
| Selected screening observations | `data/audit/phase1_cases.csv`: 120 rows, including the last selected repair of `cfg_103`. This is not a list of every screening execution. |
| Workload validation | `data/audit/v1_validation_repeats.csv` and `data/audit/v2_validation_repeats.csv`: 50 rows each, ten configurations over five blocks in each stage. |
| Auxiliary experiments and V2 resources | `data/kafka/source/analysis/v2/complete-profile/kafka_broker_tuning_v2_cases.csv` and its JSON counterpart: 119 rows comprising six instrumentation, three calibration, 30 profile-screening, 30 profile-confirmation, and 50 final-validation observations. The final 50 overlap the V2 audit dataset; they are not additional experiments. |
| Profile and instrumentation decisions | `data/kafka/source/analysis/v2/{instrumentation-pilot,rate-calibration,profile-screening,profile-confirmation,complete-profile}/kafka_broker_tuning_v2_decisions.json`, with stage case tables in the same directories. |
| Policies and contracts | `data/common_measurement_contract.json`, `data/machine_output_contract.json`, `data/state_machine_contract.json`; retained workflow definitions also appear in `data/kafka/source/artifacts/workflow_state.json`. |
| Order, batches, and job identifiers | `data/kafka/source/artifacts/{v1,v2}/validation_manifest.csv`, the audit rows' source-report paths, `data/kafka/source/artifacts/slurm_job_ids.csv`, and `supplement/validation_allocations.csv`. |
| Screening design and selection | `data/kafka/source/artifacts/v1/{phase1_manifest.csv,validation_shortlist.csv,validation_shortlist.json}`; selected-report and repair history in `data/kafka/source/artifacts/v1-phase1-verify_result_selection.json`. |
| Configuration details | Workload settings are retained in the audit rows and manifests; B0 settings are also summarized in `data/evidence_summary.json`. Six reviewed B0–B5 profile files are in `supplement/reviewed_broker_profiles/`; their provenance and canonical-hash comparison are described below. |
| Summary and provenance records | `data/evidence_summary.json`, `data/source_manifest.json`, `data/reviewed_source_manifest.json`, and `data/kafka/source/artifacts/{provenance,software_versions,workflow_state}.json`. |
| Paper-specific analyses | `supplement/`, generated or checked by `scripts/build_referee_supplement.py`; `supplement/README.md` explains each derived file. |
| Paper and checks | One `hpc_mqbench_paper.tex`, `references.bib`, the supplied figure PDF, `scripts/verify_paper.py`, `scripts/plot_operating_limits.py`, and `Makefile`. Root `SHA256SUMS` covers retained deliverable files and excludes temporary build files. |

Endpoint minima, pending and failed-send ratios, classifications from retained
validity flags/counters, and aggregation across observations can be recomputed.
The original endpoint rates, latency percentiles, clock flags, and monitoring
aggregates are retained summaries. Their raw per-rank timing, latency histograms,
clock exchanges, and monitoring traces are not supplied here. Repeating a formula
or testing present source behavior does not reconstruct those missing inputs.

## Table-to-input map

The following map covers every manuscript table, including Appendix A.
Numerical checks refer to `scripts/verify_paper.py`; the tables themselves are
inline in the single LaTeX source, rather than separate generated TeX files.

| Stable table label (current topic) | Inputs and check scope |
| --- | --- |
| `tab:related-comparison` (related tools) | The cited primary documentation in `references.bib` and the inspected implementation. This is a documented-scope comparison, not campaign data or a measured comparison of frameworks. |
| `tab:auditability` (evidence levels) | The inventory above; `data/source_manifest.json`, `data/reviewed_source_manifest.json`, `data/evidence_summary.json`, and retained `provenance.json`/`software_versions.json`. The audit-boundary section above and `PROVENANCE.json` explain inspection boundaries. This table is a qualitative audit, not a derived metric. |
| `tab:kafka-backlog-sensitivity` (delivery-policy sensitivity) | `data/audit/phase1_cases.csv` supplies counters, flush durations, rates, and eligibility flags. The table covers all three policy dimensions: pending deliveries, flush duration, and failed sends, varying one limit at a time. `scripts/build_referee_supplement.py` creates the 12-policy `supplement/policy_sensitivity.csv`; the table groups tested values with identical outcomes. The verifier recomputes the policy outcomes. Eligibility stays fixed while delivery limits vary. |
| `tab:kafka-final` (final workload validation) | `data/audit/v2_validation_repeats.csv`; original ranking order in `data/kafka/source/artifacts/v2/validation_summary.csv`. The verifier recomputes qualified counts, balanced-rate medians and inclusive IQRs, and table rounding. p99 and endpoint rates are accepted per-observation summaries; raw latency and rate timing are not reconstructed. |
| `tab:kafka-resources` (broker resources) | Filter `stage=final_validation` in `data/kafka/source/analysis/v2/complete-profile/kafka_broker_tuning_v2_cases.csv`. The verifier checks medians of monitoring-window summaries, tmpfs maxima, unit conversions, and selected workload rows. It does not recompute those monitoring summaries from original traces. |
| `tab:network-probes` (directional iperf3 summaries) | The same complete-profile CSV, filtered to final validation and grouped by blocks 1–3 versus 4–5. The verifier checks that each group has one shared directional pair, counts its 30 or 20 rows, and converts decimal MB/s to Gbit/s. These are two batch-level pairs, not 50 independent probes; raw iperf3 JSON and historical command flags are unavailable. |
| `tab:system-hardware` (Appendix A system description) | Published GWDG CPU-partition and Intel 8468 documentation cited in `references.bib`. Retained allocation/partition settings come from workflow state and provenance; role placement/storage is summarized in `data/evidence_summary.json`. Published CPU, memory, OS, and fabric specifications are not a campaign-time node inventory. |
| `tab:parameter-space` (Appendix A workload levels) | `data/audit/phase1_cases.csv`, `data/kafka/source/artifacts/v1/phase1_manifest.csv`, and `data/evidence_summary.json`. `cfg_001` provides baseline settings; distinct screened values provide the levels. The verifier also checks that `cfg_007` changes only producer ranks among the listed settings. |
| `tab:broker-profiles` (Appendix A B0–B5 settings) | `supplement/reviewed_broker_profiles/B0.json` through `B5.json`. Their canonical content hashes match the corresponding retained profile experiment rows. `data/evidence_summary.json` separately describes V1's fixed B0 profile; the inspected repository generator `scripts/generate_broker_tuning_v2.py` defines the same six tuning settings. Matching these configuration payloads does not recover the campaign source revision. |

## Figure-to-input map

| Stable figure label (current topic) | Inputs and generation |
| --- | --- |
| `fig:system-architecture` (four-node deployment) | Inline TikZ in the main TeX. Role placement in `data/evidence_summary.json`, retained execution settings, and inspected orchestration/monitoring code identified in `PROVENANCE.json`. It is a deployment schematic, not measured topology or traffic. |
| `fig:analysis-outputs` (case evidence to campaign outputs) | Inline TikZ in the main TeX. `data/machine_output_contract.json`, retained `data/kafka/source/{analysis,artifacts}/` bundles, and inspected analyzers and result schema identified in `PROVENANCE.json`. This describes current workflow outputs; the compact companion folder does not contain every raw file that an ordinary full campaign generates. |
| `fig:qualification-workflow` (case timing) | Inline TikZ in the main TeX. `data/common_measurement_contract.json` and inspected producer, consumer, and controller code identified in `data/reviewed_source_manifest.json`. Timing boundaries are a method schematic, not reconstructed historical timelines. |
| `fig:kafka-operating-limits` (screening scatter plots) | `scripts/plot_operating_limits.py` reads `data/kafka/derived/phase1_configuration_outcomes.csv` and writes `figures/kafka_operating_limits.pdf`. The verifier cross-checks its 120 rows against `data/audit/phase1_cases.csv`, including states, pending ratios, rate/latency fields, and axis coverage. The latency panel uses the 113 eligible rows. |
| `fig:validation-blocks` (qualification and cfg_007 by block) | Inline TikZ plot coordinates in the main TeX, independently checked from `data/audit/{v1,v2}_validation_repeats.csv`. Batch membership comes from retained source-report paths; stage job counts come from `data/kafka/source/artifacts/slurm_job_ids.csv`. The same evidence is exposed in `supplement/validation_allocations.csv`. |

Additional prose results have direct inputs: the three instrumentation pairs and
three rate-calibration observations are identified by `stage` in the
complete-profile table. Profile-anchor medians, qualification counts, ratios,
and decision comparisons are provided by `supplement/profile_anchor_comparison.csv`
and `supplement/review_checks.json`. Screening parameter contrasts use the
selected audit rows. The descriptive analyses under `data/kafka/derived/` are
copied downstream analyses, not additional benchmark observations.

`supplement/qualified_only_ranking.csv` maps the V1 and V2 validation rows to
both the implemented ranking and the sensitivity ranking that substitutes a
qualified-only rate median. It contains 20 rows, one per configuration and
stage, with eligible/qualified counts, both medians, ranks, and the unchanged
subsequent tie-break statistics. The calculation changes only the secondary
rate median. A configuration with no qualified observations has no such median
and remains behind configurations with positive qualified counts. The generator
checks its implemented order against each stage's retained
`data/kafka/source/artifacts/{v1,v2}/validation_summary.csv`.

## Validation allocation evidence

Each retained validation case path contains its batch directory. For both
stages, all observations in blocks 1–3 have `batch_001` paths and those in blocks
4–5 have `batch_002` paths. This establishes the grouping even where a numeric
job identifier is absent from a case path.

V1 paths identify jobs 15303484 and 15303485 for those groups. The V2 stage list
records two completed jobs, 15324660 and 15324661, but the retained V2 case paths
do not establish which job belongs to which batch. The supplement leaves the
per-case V2 job field blank. Neither list order nor the numerical order of job
IDs is used to invent that association.

## Reviewed source and configuration identity

`PROVENANCE.json` records the exact source revision inspected for this deposit:

```text
145d847120975ba347410969a0bf2a6ba3a5d25b
```

All 20 listed implementation/test files have the same hashes as the earlier
reviewed source commit `bca9a4d18892f5ebff80d474da6ba57a0da749d2`.
The campaign provenance records `git_commit: null`; neither current revision is
asserted to be the campaign commit. Repository access status is recorded
separately from source identity.

The six profile files under `supplement/reviewed_broker_profiles/` were copied
unchanged from repository path
`configs/campaigns/kafka/v2_reproducible/broker_profiles/`. Before copying, each
profile's content hash was recomputed with the procedure in
`src/benchmark/broker_profile.py`: remove the top-level `profile_sha256`, serialize
JSON with sorted keys, separators `(',', ':')`, and `ensure_ascii=True`, encode
as UTF-8, then compute SHA-256. Each result equals the file's `profile_sha256`
and occurs in the retained complete-profile experiment rows for that profile.
The canonical profile hash differs from a byte-for-byte file checksum; the
latter is recorded in the root `SHA256SUMS`.

B0's matched payload is the tuning profile used in instrumentation, calibration,
screening, and confirmation. Final-validation rows identify a separately
frozen B0 profile hash; the copied B0 payload is not mislabeled as that frozen
file. This configuration match supports the six settings in Appendix A,
not recovery of an entire historical source tree or every generated case file.

## Checking and rebuilding

Follow the commands in `README.md`. Verification is read-only. The root
`MANIFEST.json` and `SHA256SUMS` cover this exact package; original nested
manifests retain historical paths and are evidence, not portable file indexes.
Check the supplied hashes before rebuilding. The supplied manuscript and plot
make a standalone numerical audit possible without a source checkout or TeX.
A rebuild additionally needs pdfLaTeX/BibTeX, and plot regeneration needs
matplotlib. Present-source checks are conditional on the repository being
available two directories above this folder; skipped source checks do not mean
that historical code was verified.

## Missing evidence

The package cannot reconstruct missing campaign-time source and library
versions, raw rate/latency/clock/monitoring traces, raw network probe records,
complete node inventory, or the exact V2 batch-to-job association.
Historical paths, job IDs, and hashes establish links between retained records;
they do not guarantee that the original raw files still exist.
