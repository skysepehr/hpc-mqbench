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
- end-to-end p50, p95, and p99 latency;
- missing, duplicate, out-of-order, and surplus records;
- backend health, configuration snapshots, checksums, and provenance.

Each reproducible case uses a 15-second warm-up, 30-second measurement window,
and up to 60 seconds of consumer drain. Record identity is tracked for every
record; latency timestamps are sampled deterministically once every ten
records. The backend is restarted and its case-local storage is cleaned between
cases.

An eligible run is **Qualified** when:

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

## Quick Start

```bash
git clone git@github.com:skysepehr/distributed-messaging-benchmark-suite.git hpc-mqbench
cd hpc-mqbench

./benchmark.sh backend list
./benchmark.sh check
./benchmark.sh prepare-hpc kafka
./benchmark.sh prepare-hpc pulsar
```

Validate one case without submitting a job:

```bash
./benchmark.sh preflight kafka \
  configs/campaigns/kafka/example_simultaneous_case.json
./benchmark.sh dry-run kafka \
  configs/campaigns/kafka/example_simultaneous_case.json
```

Preview a complete reproducible campaign:

```bash
./scripts/run_reproducible_benchmark.sh kafka --dry-run --phase all
./scripts/run_reproducible_benchmark.sh pulsar --dry-run --phase all
```

Run or resume a campaign:

```bash
./scripts/run_reproducible_benchmark.sh kafka \
  --run --phase all --run-id kafka-example-001

./scripts/run_reproducible_benchmark.sh kafka \
  --resume --phase all --run-id kafka-example-001
```

Use `--phase phase1`, `--phase validation`, `--phase v1`, or `--phase v2` to
restrict execution. Site-specific account, partition, QoS, module, network,
and storage settings are supplied through command options or environment
variables rather than committed configuration files.

## Workflow And Results

The workflow generates campaigns, submits Slurm batches, polls jobs, verifies
case reports, analyzes Phase 1, selects the validation shortlist, runs repeated
validation, and advances through backend-specific V2 tuning gates. Completed
stages are checksum-validated and skipped during resume. Repair runs use new
directories and never overwrite historical evidence.

Machine-readable results are stored under:

```text
results/workflows/<backend>/<run-id>/
```

JSON is the detailed source of truth; CSV is the convenient comparison format.
The benchmark finishes after producing validated reports, summaries, manifests,
provenance, checksums, Slurm job IDs, and workflow state. Figures, LaTeX, and PDF
reports are an optional downstream pipeline and are not required for success.

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
