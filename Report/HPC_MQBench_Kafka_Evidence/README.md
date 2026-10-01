# HPC-MQBench: retained Kafka evidence

This folder contains campaign data, configurations, provenance, and analysis
code. It contains no manuscript, bibliography, rendered figure, or narrative
publication report. The retained evidence is not a complete raw campaign archive.

## Contents

| Location | Contents |
| --- | --- |
| `data/audit/` | 120 selected screening rows; 50 V1 and 50 V2 validation observations. |
| `data/kafka/source/` | Retained case and summary tables, instrumentation/calibration and profile experiments, decisions, shortlists, block manifests, available job identifiers, workflow records, and contracts. |
| `data/kafka/derived/` | Derived configuration outcomes, sensitivity and resource summaries, and profile comparisons. |
| `supplement/` | Derived policy/ranking sensitivities, allocation/order mapping, per-anchor comparisons, and six reviewed profile configurations. |
| `scripts/` | Integrity verification, numerical checks, supplemental analysis, and optional plotting code. |
| `ARTIFACT_GUIDE.md` | Dataset paths, calculation scope, configuration identity, and missing inputs. |
| `PROVENANCE.json` | Inspected source revision, observation counts, and historical evidence limitations. |
| `MANIFEST.json`, `SHA256SUMS` | Exact package inventory, sizes, roles, and SHA-256 hashes. |

The complete-profile table has 119 rows: 69 auxiliary observations (6
instrumentation, 3 calibration, 30 profile screening, 30 profile confirmation)
and the same 50 V2 validation observations included in the audit tables.
Duplicate file representations are not extra experiments. The 69 auxiliary
observations belong to this campaign; they are not mandatory additional tests
in every ordinary benchmark invocation. The selected screening table keeps the
last repair of `cfg_103`; repair history accounts for 291 executed cases versus
289 selected main and auxiliary observations.

## Verify

Requires Python 3.10+ with the standard library; no broker, Slurm allocation,
TeX, or PDF tools are required. From this folder, run:

```sh
make verify
```

Equivalent commands, if Make is unavailable:

```sh
python3 -B scripts/verify_artifact.py
python3 -B scripts/verify_evidence.py
python3 -B scripts/verify_analysis.py
```

These checks do not rewrite files. Integrity verification checks every
packaged file and the original nested checksum lists as retained evidence.
Numerical verification recomputes supported quantities from the retained rows
and checks the supplemental files. It does not inspect a paper or reconstruct
absent raw measurements. Run without Python's `-O` optimization option.

When kept at this repository path, verification also checks selected current
source behavior and source-manifest hashes. A standalone copy explicitly
reports those source checks as skipped. The current source revision is distinct
from the unknown campaign-time revision; see `PROVENANCE.json`.

## Optional local analysis

To check the supplemental outputs alone:

```sh
python3 -B scripts/build_referee_supplement.py --check
```

Omitting `--check` regenerates their CSV/JSON files from the retained evidence.
The optional `scripts/plot_operating_limits.py` needs matplotlib and writes a
local plot to ignored `build/figures/`; `make verify` does not generate it.

After an intentional package edit, maintainers can refresh its inventory with
`python3 -B scripts/verify_artifact.py --write-manifest`. Ordinary verification
never refreshes hashes. Check supplied integrity before regenerating files.

## Provenance and limitations

Repository: <https://github.com/skysepehr/hpc-mqbench>.
Identify this package by the commit of your checkout. The historical tag
`kafka-paper-evidence-v1` identifies an earlier package containing publication
files, not this code-and-data package. No DOI or immutable release is claimed.

Missing inputs include raw per-rank timings, latency histograms and clock
exchanges, monitoring traces, raw iperf3 output, exact campaign Kafka/client
versions and source commit, a complete node inventory, and the explicit V2
batch-to-job mapping. Endpoint rates, latency percentiles, and monitoring
aggregates remain retained summaries.

The package uses the repository's [MIT License](LICENSE).
