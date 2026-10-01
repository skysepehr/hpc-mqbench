# Retained data and verification guide

Paths are relative to this folder. The package contains historical evidence and
derived numerical analyses, not a new benchmark run or a complete raw archive.
Publication text and figures are maintained separately.

## Inputs and checks

| Dataset or configuration | Location and verification scope |
| --- | --- |
| Selected screening | `data/audit/phase1_cases.csv`: 120 distinct configurations, including the last selected repair of `cfg_103`. Row checks recompute endpoint minima, counter ratios, qualification, and workload-design counts. |
| Repeated validation | `data/audit/{v1,v2}_validation_repeats.csv`: 50 observations per stage, ten configurations in five blocks. Checks recompute counts, medians, inclusive IQRs, variation, and retained validation-summary fields. |
| Resource and auxiliary observations | `data/kafka/source/analysis/v2/complete-profile/kafka_broker_tuning_v2_cases.csv` and `.json`: 6 instrumentation, 3 calibration, 30 screening, 30 confirmation, and 50 final-validation observations. The final 50 are the V2 audit observations again. Resource checks aggregate retained monitoring summaries; they do not reconstruct monitoring traces. |
| Instrumentation and profile decisions | Stage directories under `data/kafka/source/analysis/v2/` contain case tables and `kafka_broker_tuning_v2_decisions.json`. Checks compare retained decisions with paired instrumentation arithmetic and per-anchor profile comparisons. |
| Derived analyses | `data/kafka/derived/`: configuration outcomes, backlog sensitivity, resource summaries, and profile decisions. Supported summaries are compared with the source rows. |
| Policies and contracts | `data/common_measurement_contract.json`, `data/machine_output_contract.json`, `data/state_machine_contract.json`, and retained `data/kafka/source/artifacts/workflow_state.json`. |
| Selection and order | `data/kafka/source/artifacts/{v1,v2}/` contains screening/validation manifests, shortlists, and summaries. Repair history is in `data/kafka/source/artifacts/v1-phase1-verify_result_selection.json`. |
| Jobs and batches | `data/kafka/source/artifacts/slurm_job_ids.csv`, case source paths, and `supplement/validation_allocations.csv`. Only recorded associations are used. |
| Supplemental analyses | `supplement/`: policy/ranking sensitivities, per-anchor comparisons, allocation/order mapping, seed replay, and reviewed profile settings. `scripts/build_referee_supplement.py --check` recomputes the derived CSV/JSON outputs without writes. |
| Provenance | `PROVENANCE.json`, `data/evidence_summary.json`, `data/source_manifest.json`, `data/reviewed_source_manifest.json`, and retained workflow/provenance/software-version records. |

The verification entry points are documented in `README.md`. They check data
and calculations without loading a manuscript, PDF, bibliography, or figure.
Optional plotting code reads `data/kafka/derived/phase1_configuration_outcomes.csv`
and writes only to local `build/figures/` when explicitly invoked.

## Allocation and configuration identity

Case paths place blocks 1-3 in `batch_001` and blocks 4-5 in `batch_002` for
both stages. V1 paths identify jobs 15303484 and 15303485 respectively. The V2
stage list records jobs 15324660 and 15324661, but does not establish which job
belongs to which batch. V2 per-case job identifiers remain blank in the
supplement; list order is not used to infer the association.

The six JSON files under `supplement/reviewed_broker_profiles/` were copied from
`configs/campaigns/kafka/v2_reproducible/broker_profiles/` in the inspected
repository. Their canonical hashes match retained tuning rows. To calculate a
profile hash, remove top-level `profile_sha256`, serialize with sorted keys,
separators `(',', ':')` and `ensure_ascii=True`, encode as UTF-8, then use SHA-256.
This differs from the byte-level checksum in root `SHA256SUMS`.

The copied B0 payload belongs to the tuning experiments. Final validation uses a
separately frozen B0 profile hash. The copied file is not asserted to be that
frozen payload or a recovered campaign source snapshot.

## Integrity and evidence boundary

`MANIFEST.json` and root `SHA256SUMS` inventory this exact package. Nested
manifests retain historical paths and are evidence, not portable package
inventories. `build/` is local generated output and is excluded.

`PROVENANCE.json` records the inspected source revision and the older evidence
snapshot separately from the current package. The campaign source commit is
unknown. When the source tree is present, verification reports the current
histogram/profile probes and static source checks separately; otherwise it
explicitly reports them as skipped.

Raw endpoint timing, latency histograms, clock exchanges, monitoring traces,
iperf3 records/options, exact campaign library versions, complete hardware
inventory, and the V2 batch-to-job association are unavailable. Recomputing
derived statistics does not independently verify the absent raw measurements.
