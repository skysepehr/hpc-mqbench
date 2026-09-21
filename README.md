# HPC-MQBench

**A Reproducible, Slurm-Orchestrated, Qualification-First Benchmark Suite for
Distributed Messaging Systems**

HPC-MQBench runs controlled messaging experiments on HPC clusters. It provides
a shared MPI workload and measurement contract, Slurm orchestration, immutable
campaign manifests, restartable workflows, and backend adapters. Apache Kafka
and Apache Pulsar are currently implemented.

This source-only repository excludes benchmark results, generated papers,
runtime binaries, credentials, and site-specific environment files.

## What It Measures

The common application-level contract reports:

- producer, consumer, and balanced throughput in MiB/s and records/s;
- producer backlog at flush start, flush duration, and failed-send percentage;
- sampled submission-to-consumer p50, p95, and p99 latency;
- envelope validity, repeated/regressing consumer offsets, and aggregate
  missing/surplus record counts;
- backend health, configuration snapshots, checksums, and provenance.

Generated reproducible Kafka campaign cases use a 15-second warm-up, a
30-second measurement window, and up to 60 seconds of consumer drain. Other
case configurations can use different timing and disable latency measurement.
Every record carries a producer/sequence envelope; its checksum protects the
envelope metadata, not the full payload. Offset checks are local to each
consumer, and aggregate delivery counts are not a reconciliation of every
record identity. Latency is sampled every tenth measurement record and retained
as a histogram and summaries, not an individual-message trace.

Producer rates include flush time; consumer rates count measurement-tagged
arrivals by the consumer measurement deadline. Balanced throughput is the lower
endpoint rate. Under `qualification.application.v1`, backlog is pending producer
delivery callbacks at flush start divided by measurement-period send attempts.
It is not consumer-group offset lag or broker queue depth. The legacy
`qualification.kafka.v1` policy uses an enqueued-record denominator.

Under the application policy, an eligible run is **Qualified** when:

```text
producer backlog <= 5%
producer flush duration <= 10 seconds
failed sends <= 0.1%
```

Eligibility separately requires valid latency, healthy execution, complete
evidence, and no unexplained correctness failure. Eligible runs outside the
thresholds are **Overdriven**; invalid runs are **Ineligible**.

## Architecture

```text
Campaigns and immutable manifests
               |
Portable benchmark core
MPI orchestration | timing | latency | correctness | qualification | schemas
               |
Backend adapter
clients | service lifecycle | stream setup | product metrics | profiles
```

The default HPC topology uses four exclusive nodes: producer/controller,
backend service, consumer, and monitoring. Kafka and Pulsar keep their native
client and service settings inside their adapters while sharing workload,
timing, correctness, and qualification semantics.

## Requirements

Local validation requires Bash 4+, Python 3.10+, and a POSIX environment. HPC
execution additionally requires Slurm, OpenMPI, `mpi4py`, Java, backend runtime
distributions, and the configured monitoring tools. Runtime archives are not
stored in this source repository; see [`tools/README.md`](tools/README.md).

## Local checks and planning

```bash
git clone git@github.com:skysepehr/hpc-mqbench.git
cd hpc-mqbench

./benchmark.sh backend list
./benchmark.sh check
```

Cloning over SSH requires GitHub access and a configured SSH key. Local checks
do not require a running broker or Slurm. They run the smoke/workflow tests,
shell and Python checks, and config validation; pytest runs only if installed.

Preview one case's submission without submitting a job:

```bash
./benchmark.sh dry-run kafka \
  configs/campaigns/kafka/example_simultaneous_case.json
```

Preview a complete reproducible campaign:

```bash
./benchmark.sh reproducible kafka --dry-run --phase all --run-id kafka-preview-001
```

Dry-run generates plans and local artifacts; it does not test cluster runtime
availability or submit jobs. The example single case above runs for 120 seconds,
with no warm-up, drain, or latency sampling; it is not the reproducible campaign
measurement contract. Use the generated campaign for the qualification study.

## Prepare and run on a cluster

Stage the runtime archives described in [Runtime Assets](tools/README.md) first.
The preparation helper is offline; a fresh source clone does not contain those
archives. It defaults to GWDG module names. On another cluster, set
`HPC_MODULES` to your site's modules, or use `HPC_MODULES=''` if the required
compiler, Python, Java, and MPI environment is already active.

```bash
./benchmark.sh prepare-hpc kafka
./benchmark.sh preflight kafka configs/campaigns/kafka/example_simultaneous_case.json
```

Preflight checks installed runtimes, monitoring tools, and Slurm commands. It is
expected to fail on a source-only workstation; it does not submit a benchmark
job. Validate the Python/MPI ABI on the intended compute nodes as described in
the runtime guide. The Kafka scripts default to the `ib0` fabric interface;
set `KAFKA_HPC_NETWORK_INTERFACE` for your deployment. Account, partition, QoS,
wall time, placement, and storage settings also need to match the cluster.

Run or resume a campaign after preparation (replace the account and partition):

```bash
./benchmark.sh reproducible kafka \
  --run --phase all --run-id kafka-example-001 \
  --slurm-account YOUR_ACCOUNT --partition YOUR_PARTITION

./benchmark.sh reproducible kafka \
  --resume --phase all --run-id kafka-example-001
```

Omitting `--dry-run` executes the workflow, even when `--run` is omitted.
Use a new run ID for execution rather than reusing a preview. Resume retains
the original scheduler settings and validates recorded inputs and completed
artifacts. Phase choices include `phase1`, `validation`, `v1`, and `v2`; later
phases still need their prerequisites. Use `./benchmark.sh reproducible kafka
--help` for all options. The `auto` partition selector has site-specific
defaults; use an explicit partition or configure its candidates and requirements.

The other implemented adapter has its own setup requirements; see
[Runtime Assets](tools/README.md) before using `prepare-hpc pulsar`.

## Workflow And Results

The workflow generates campaigns, submits Slurm batches, polls jobs, verifies
case reports, analyzes Phase 1, selects the validation shortlist, runs repeated
validation, and advances through backend-specific V2 tuning gates. Completed
stages are checksum-validated and skipped during resume. Repair runs use new
directories and never overwrite historical evidence.

The workflow's state, analysis, and final comparison bundles are stored under:

```text
results/workflows/<backend>/<run-id>/
```

For Kafka, per-case evidence is stored separately under
`results/sweeps/kafka_v1_reproducible/<run-id>-v1/` and
`results/tuning_v2/<run-id>-v2/`. Preserve those directories as well as the
workflow directory when archiving a run.

| Output | What it provides |
| --- | --- |
| Per-case `final_report.json` | Rates, counters, latency histograms, validity/qualification, available monitoring and diagnostic indicators. |
| Screening CSV/JSON | Validity checks, ranked cases, shortlist and selection reasons. |
| Repeated-validation CSV/JSON | Individual repeats; qualified counts, throughput medians/variability, backlog, flush, failure and latency summaries. |
| Profile/resource CSV/JSON | Broker resource summaries, instrumentation checks and profile-selection decisions. |
| Final bundles and provenance | Recommendation, manifests, source-report paths/hashes, job IDs, workflow state and checksums. |

These analyses run automatically. Repeated-run summaries include eligible
overdriven runs; ranking prioritizes the number of qualified repeats.
The full workflow uses machine mode and does not generate a manuscript, LaTeX,
or PDF. Standalone cases default to full presentation mode, which can also write
Markdown, HTML and plots. Optional downstream scripts create additional
analyses and presentation artifacts. See [Kafka workflow outputs](docs/outputs.md)
for filenames, report modes, traceability and known diagnostic limitations.

Generated bottleneck messages are inspection aids, not established causes.
Missing measurements must not be treated as zero or evidence of spare capacity.

## Repository Layout

```text
benchmark.sh             Stable command facade
configs/                 Workloads, profiles, and campaign manifests
models/                  Runtime configuration models
schemas/                 Versioned case and result schemas
scripts/                 Slurm workflow and analysis commands
src/benchmark/core/      Portable measurement contracts
src/benchmark/backends/  Kafka and Pulsar adapters
src/benchmark/workflow/  Restartable state machine
tests/                   Contract, smoke, and recovery tests
```

## Adding A Backend

A backend implements the adapter contract under
`src/benchmark/backends/<backend_id>/`, registers itself in the backend
registry, supplies producer/consumer workers and lifecycle operations, keeps
product configuration under `backend.<backend_id>`, and emits product evidence
under `backend_metrics.<backend_id>`. New adapters must pass the shared schema,
correctness, qualification, workflow, and fake-backend tests.

## Scope And Safety

Default Kafka and Pulsar profiles are single-service, non-replicated research
setups, not production deployment advice. Fair comparisons require matched
hardware, placement, payload, timing, offered load, correctness, and delivery
semantics.

Never commit credentials, `.env` files, private keys, cluster inventories, raw
results, or generated operator environments. Review [`SECURITY.md`](SECURITY.md)
before sharing artifacts.

## License

HPC-MQBench is licensed under the [MIT License](LICENSE).
