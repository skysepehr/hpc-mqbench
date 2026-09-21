# HPC-MQBench: Kafka companion evidence

This folder contains the retained evidence and verification scripts for
**HPC-MQBench: Qualification-First Benchmarking on Slurm — Design and Evaluation
with Apache Kafka**, by Sepehr Mahmoodian and Julian Kunkel.

The evidence belongs to the reported campaign. It is not a new benchmark run,
and it is not a complete raw campaign archive. The manuscript PDF and its single
LaTeX source are included as a publication snapshot so the table and figure
checks are self-contained; the benchmark itself does not generate this paper.

## Contents

| Location | Contents |
| --- | --- |
| `data/audit/` | 120 selected screening rows; 50 V1 and 50 V2 validation observations. |
| `data/kafka/source/` | Retained screening/validation bundles, instrumentation and calibration rows, profile experiments and decisions, shortlists, block manifests, available jobs, workflow/provenance records, and contracts. |
| `supplement/` | Derived policy/ranking sensitivities, allocation and order mapping, per-anchor profile comparisons, and six reviewed profile configurations. |
| `scripts/` | Integrity, numerical, prose-claim, supplement, and plotting checks. |
| `ARTIFACT_GUIDE.md` | Every paper table and figure mapped to its inputs, plus evidence and calculation boundaries. |
| `PROVENANCE.json` | Exact inspected source revision and hashes, observation counts, access status, and unavailable historical evidence. |
| `MANIFEST.json`, `SHA256SUMS` | Complete portable file inventory, sizes, roles, and SHA-256 hashes. |
| `hpc_mqbench_paper.tex`, `.pdf`, `references.bib`, `figures/` | Checked manuscript snapshot and figure. |

The profile table has 119 rows: **69 auxiliary observations** (6 instrumentation,
3 calibration, 30 profile screening, 30 profile confirmation) and the **same
50 V2 validation observations** included in the audit tables. Repeated file
representations do not add experiments. These are campaign-specific auxiliary
experiments, not a claim that every ordinary benchmark invocation executes 69
additional tests. The 120 selected screening rows omit the original cfg_103 and
its superseded repair; the retained repair history explains the 291 executed
cases versus 289 selected main-plus-auxiliary observations.

## Verify the supplied artifact

Requires Python 3 and Poppler (`pdftotext`, `pdfinfo`); no Kafka cluster or Slurm
allocation is required. From this folder, run:

```sh
python3 -B scripts/verify_artifact.py
python3 -B scripts/build_referee_supplement.py --check
python3 -B scripts/verify_paper.py
python3 -B scripts/verify_claims.py
```

These commands do not rewrite the supplied evidence, manifest, or checksums.
Integrity checks cover every packaged file, including original nested checksum
lists. The numerical checks recompute quantities supported by retained rows;
they do not reconstruct raw measurements that are absent.

When this folder is kept at `Report/HPC_MQBench_Kafka_Evidence` in the source
repository, the paper verifier additionally exercises the available source
implementation. In a standalone copy those guarded source checks are skipped.
The current source revision is distinct from the **unknown campaign-time
revision**; see `PROVENANCE.json`. The same distinction applies to the current
histogram and diagnostic logic: checking current code does not prove historical
code identity.

## Rebuild the paper or derived checks

The supplied PDF can be audited without TeX. To rebuild it, install pdfLaTeX and
BibTeX, then run `make pdf`. The supplied plot can be regenerated with
`make figures` when matplotlib is available. For ShareLaTeX/Overleaf upload the
single `.tex`, `references.bib`, and `figures/kafka_operating_limits.pdf`.

Rebuilding can change file bytes, so check the supplied integrity **before**
rebuilding. Maintainers intentionally updating a snapshot can refresh its
inventory with `python3 -B scripts/verify_artifact.py --write-manifest`.
Ordinary verification never refreshes it. `make verify` runs all four checks.

## Availability and limitations

Repository: <https://github.com/skysepehr/hpc-mqbench>.
The evidence snapshot is identified by tag `kafka-paper-evidence-v1`; an ordinary
Git tag is not a GitHub immutable release. Anonymous access returned HTTP 404
when checked on 22 September 2026, although authenticated Git access succeeded.
Public access and GitHub release-immutability protection remain unverified.
No DOI, public release, or complete experimental reproducibility is claimed.

Missing inputs include raw per-rank timings, latency histograms/clock exchanges,
monitoring traces, raw iperf3 output, exact campaign Kafka/client versions and
source commit, complete node inventory, and the explicit V2 batch-to-job mapping.
Retained endpoint rates, latency percentiles, and monitoring aggregates remain
reported summaries. The guide states what can be recomputed from them.

The package uses the repository's [MIT License](LICENSE). Please cite the paper
and identify the evidence snapshot when reusing these data.
