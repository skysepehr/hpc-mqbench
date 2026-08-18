# Distributed Messaging Benchmark Suite

A reproducible Slurm/MPI benchmark for distributed messaging systems. Kafka
and Apache Pulsar are implemented backends. The benchmark core owns workload
timing, MPI orchestration, rate control, record identity, correctness,
end-to-end latency, qualification, and portable result schemas. Backend
adapters own product clients, service lifecycle, stream creation, immutable
profiles, and product-specific monitoring.

This source repository is intentionally separate from benchmark evidence.
Runtime binaries, raw measurements, generated reports, cluster credentials,
and operator-specific environment files are not part of the public source
release.

## Status And Scope

- Executable backends: `kafka` and `pulsar`.
- Execution platform: Slurm allocations with MPI workers.
- Default topology: four exclusive nodes with service, monitoring,
  producer/controller, and consumer roles.
- Supported public workflow: deterministic V1 workload screening and repeated
  validation, followed by backend-specific V2 tuning stages.
- Output boundary: validated JSON/CSV evidence, manifests, provenance,
  checksums, and workflow state. Scientific reports are optional downstream
  products and are not required for benchmark completion.

The suite is designed for controlled performance experiments. Its default
single-service, non-replicated profiles are not production deployment advice.

## Common Measurement Contract

New reproducible Kafka and Pulsar campaigns use the same application-level
contract:

1. Start a clean backend instance for the case.
2. Warm up for 15 seconds.
3. Measure for 30 seconds.
4. Stop producing at measurement end.
5. Snapshot pending producer callbacks before flush.
6. Flush producers while consumers continue polling.
7. Drain consumers for up to 60 seconds.
8. Perform post-run health, correctness, profile, and checksum checks.
9. Stop the backend and clean case-local RAM-backed storage.

Every record carries a unique identity containing producer rank and sequence
number. Record identity and correctness accounting apply to every record.
Producer timestamps are sampled deterministically once every ten records for
end-to-end latency. The controller performs 100 clock-calibration samples
before and after each case; latency is invalid when uncertainty or drift
exceeds 250 microseconds.

Portable metrics include:

- Producer, consumer, and balanced throughput in MiB/s and records/s.
- Producer backlog at flush start.
- Producer flush duration and failed-send percentage.
- End-to-end p50, p95, and p99 latency.
- Missing, duplicate, out-of-order, and surplus record counts.
- Backend health, latency validity, and configuration provenance.

Balanced throughput is the minimum producer/consumer application payload rate.
The common backlog formula is:

```text
backlog_percent = 100 * pending_callbacks_at_flush_start
                      / measurement_period_send_attempts
```

Eligibility requires a complete valid report, healthy backend, valid latency,
and no unexplained missing, duplicate, or out-of-order records after drain.
Eligibility is a prerequisite, not a performance score.

An eligible run is **Qualified** under `qualification.application.v1` when:

```text
backlog <= 5%
flush duration <= 10 seconds
failed sends <= 0.1%
```

An eligible run outside these thresholds is **Overdriven**. A run failing
correctness, latency validity, backend health, schema, or provenance checks is
**Ineligible**.

## Architecture

```text
Campaign layer
  workload + backend profile + qualification policy + randomized run order
                              |
Benchmark core
  MPI roles, timing, envelopes, latency, correctness, common metrics,
  qualification, schemas, workflow state, checksums, and provenance
                              |
Backend adapter
  clients, validation, lifecycle, readiness, stream operations,
  monitoring discovery, effective settings, and backend metrics
```

The default node layout is:

```text
producer/controller node -> service node -> consumer node
          |                    |                |
          +------------- monitoring node ------+
```

- MPI rank 0 coordinates the case on the producer/controller node.
- Each producer MPI rank creates one real backend producer client.
- Each consumer MPI rank creates one real backend consumer client.
- Virtual-device IDs may be multiplexed through producer ranks; they are not
  separate TCP connections.
- Kafka uses one broker in the default profile.
- Pulsar uses one standalone service in the default profile.
- Prometheus and node/process exporters run on the monitoring allocation.
- Backend-native metrics remain under `backend_metrics.<backend_id>`.

The core obtains workers and lifecycle operations from the backend registry;
it does not import a Kafka or Pulsar worker directly.

## Repository Layout

```text
benchmark.sh                     Stable command facade
run_all.sh                       One-case Slurm entrypoint
configs/                         Workloads, backend profiles, and campaigns
models/                          Runtime data models
monitoring/                      Prometheus and Kafka JMX configuration
schemas/                         Versioned case and result schemas
scripts/                         Workflow, HPC, lifecycle, and analysis tools
src/benchmark/core/              Portable schema contracts
src/benchmark/backends/          Backend adapters and registry
src/benchmark/workflow/          Backend-independent state machine
tests/                           Contract, smoke, recovery, and fake-backend tests
tools/stream_probe.c              Source for the network probe helper
tools/README.md                  Runtime-asset preparation guidance
```

Generated `results/`, `Report/`, `.local/`, logs, runtime archives, and source
release directories are ignored. Historical flat Kafka configurations remain
accepted by the compatibility loader.

## Requirements

Local validation requires:

- Linux or another POSIX-like environment.
- Bash 4 or newer.
- Python 3.10 or newer.
- A C compiler for the optional stream probe and native clients.

HPC execution additionally requires:

- Slurm commands (`sbatch`, `squeue`, and `sacct`).
- OpenMPI and `mpi4py` built against the allocation's MPI implementation.
- Java 17 or newer for Kafka 4.x.
- Java 21 or newer for the pinned Pulsar 5.0.0-M1 runtime.
- Kafka/Pulsar distributions and monitoring tools prepared locally.
- `iperf3` for path-capacity probes when enabled.

Third-party archives are intentionally excluded from the source-only Git
repository. See [`tools/README.md`](tools/README.md) before running
`prepare-hpc`.

## Quick Start

Clone the source repository and inspect the available backends:

```bash
git clone YOUR_PRIVATE_REPOSITORY_URL messaging-benchmark-suite
cd messaging-benchmark-suite
./benchmark.sh backend list
./benchmark.sh check
```

Prepare runtime assets, then create repository-local installations on the HPC
login node:

```bash
./benchmark.sh prepare-hpc kafka
./benchmark.sh prepare-hpc pulsar
```

Validate a case without submitting it:

```bash
./benchmark.sh preflight kafka \
  configs/campaigns/kafka/example_simultaneous_case.json
./benchmark.sh dry-run kafka \
  configs/campaigns/kafka/example_simultaneous_case.json

./benchmark.sh preflight pulsar \
  configs/campaigns/pulsar/examples/example_simultaneous_case.json
./benchmark.sh dry-run pulsar \
  configs/campaigns/pulsar/examples/example_simultaneous_case.json
```

Never place a password, SSH key, access token, or cluster credential in a case
JSON file. Scheduler and site-specific values are environment variables.

## Stable Command Interface

```bash
./benchmark.sh backend list
./benchmark.sh check
./benchmark.sh prepare-hpc kafka
./benchmark.sh preflight BACKEND CONFIG.json
./benchmark.sh dry-run BACKEND CONFIG.json
./benchmark.sh submit-case BACKEND CONFIG.json
./benchmark.sh submit-batch BACKEND MANIFEST.csv
./benchmark.sh submit-campaign BACKEND MANIFEST.csv
./benchmark.sh reproducible BACKEND --dry-run
./benchmark.sh analyze BACKEND RESULTS_ROOT STAGE
./benchmark.sh report MACHINE_RESULTS REPORT_OUTPUT --compile-pdf
```

The facade preserves the exit status of the underlying command. Unknown
backends and backend/config mismatches fail before submission.

## One-Command Reproducible Workflow

Always preview a campaign first:

```bash
./scripts/run_reproducible_benchmark.sh kafka --dry-run --phase all
./scripts/run_reproducible_benchmark.sh pulsar --dry-run --phase all
```

Run a full backend campaign:

```bash
./scripts/run_reproducible_benchmark.sh kafka \
  --run --phase all --run-id kafka-example-001
```

Restrict execution when needed:

```bash
# Phase 1 screening only.
./scripts/run_reproducible_benchmark.sh kafka \
  --run --phase phase1 --run-id kafka-example-001

# Complete V1: screening, shortlist, and repeated validation.
./scripts/run_reproducible_benchmark.sh kafka \
  --run --phase v1 --run-id kafka-example-001

# Resume the same immutable run into V2.
./scripts/run_reproducible_benchmark.sh kafka \
  --resume --phase v2 --run-id kafka-example-001
```

The backend can also be passed as `--backend kafka`. Supported phase selectors
are `all`, `v1`, `phase1`, `validation`, and `v2`.

V1 Phase 1 contains 120 independent workload configurations. The default
generator creates four Slurm allocations, each running up to 30 cases
sequentially on one exclusive four-node placement. After all four allocations
finish, the workflow verifies and analyzes the 120 reports, derives a new
ten-configuration shortlist, and generates five randomized validation blocks
(50 cases) split into two allocations. V2 then follows the registered
backend-specific tuning gates.

Pulsar real-run submission additionally requires the measured Phase 1
acceptance report. Keep that result outside source control and either place it
at the ignored default path
`results/published/pulsar/phase1-gate/acceptance_report.json` or set
`PULSAR_PHASE1_GATE_REPORT` to its location. A Pulsar `--dry-run` may generate
and validate campaign inputs without this report, but it records the gate as
absent and never authorizes a real submission.

### Scheduler Settings

Pass site-specific values on the command line or through environment variables:

```bash
SLURM_ACCOUNT=YOUR_ACCOUNT \
SLURM_PARTITION=YOUR_PARTITION \
SLURM_QOS=YOUR_QOS \
SLURM_TIME=02:00:00 \
./scripts/run_reproducible_benchmark.sh kafka \
  --run --phase v1 --run-id kafka-example-001
```

The workflow records scheduler settings in `workflow_state.json` and rejects
changes when the same run ID is resumed. This prevents silent execution drift.

On GWDG, partition selection can be restricted to an explicit candidate list:

```bash
./scripts/run_reproducible_benchmark.sh kafka \
  --list-partitions \
  --partition-candidates standard96s,medium96s

./scripts/run_reproducible_benchmark.sh kafka \
  --run --phase phase1 --run-id kafka-example-001 \
  --partition auto \
  --partition-candidates standard96s,medium96s
```

`--partition auto` is a point-in-time heuristic over only the supplied
candidates. It is not a scheduler-priority prediction or dispatch guarantee.

### Resume And Recovery

The workflow is idempotent. A completed stage is skipped only when its recorded
artifacts still exist and all checkpointed input checksums match.

Real runs use bounded automatic recovery by default. If a Slurm allocation
fails, a report is missing, or a completed report is ineligible, the workflow
writes an immutable repair manifest containing only affected cases and submits
it to a separate `repairs/` subtree. The default limit is two repair attempts
per submission stage. Use `--max-case-repair-attempts N` to change it or
`--no-auto-repair` to disable automatic repair. Original reports are never
deleted or overwritten.

A valid repair can supersede an invalid attempt. Two differing eligible reports
for the same case are an ambiguity and remain a hard stop. Broad screening can
continue within its declared ineligible-case budget after repairs are
exhausted, but an ineligible case cannot enter a shortlist or win a ranking.
Decision-critical gates stop when evidence is insufficient.

## Configuration Schema

New cases use `messaging-benchmark.case.v1`:

```json
{
  "schema_version": "messaging-benchmark.case.v1",
  "backend_id": "kafka",
  "workload": {
    "scenario": "simultaneous",
    "producer_ranks": 40,
    "consumer_ranks": 40,
    "payload_size_bytes": 4096,
    "warmup_sec": 15,
    "duration_sec": 30,
    "drain_timeout_sec": 60,
    "latency_enabled": true,
    "latency_sample_every": 10
  },
  "backend": {
    "kafka": {
      "broker_count": 1,
      "partitions": 120,
      "replication_factor": 1,
      "topic_name": "benchmark-topic",
      "acks": "1",
      "compression_type": "none",
      "batch_size": 1048576,
      "linger_ms": 20,
      "extra": {}
    }
  },
  "campaign": {
    "case_id": "example",
    "campaign_id": "example-campaign"
  },
  "qualification_policy_id": "qualification.application.v1"
}
```

The namespace under `backend` must exactly match `backend_id`. Portable
workload settings do not contain backend credentials. See
`schemas/case-v1.schema.json`, `schemas/result-v1.schema.json`, and the tracked
examples under `configs/campaigns/`.

Flat historical Kafka JSON remains accepted as
`legacy.kafka.flat.v1`. New reproducible campaigns explicitly use the common
application qualification policy.

## Workflow State And Results

Each invocation creates a unique tree under:

```text
results/workflows/<backend>/<run-id>/
```

The durable state file records stages, transitions, timestamps, Slurm job IDs,
paths, failures, selected configurations/profiles, and immutable execution
settings. Completed machine bundles contain:

```text
final_report.json
phase1_cases.json
phase1_cases.csv
phase1_validation.json
validation_shortlist.json
validation_shortlist.csv
validation_repeats.json
validation_repeats.csv
validation_summary.json
validation_summary.csv
phase1_manifest.csv
validation_manifest.csv
source_case_index.json
source_case_index.csv
provenance.json
artifact_manifest.csv
SHA256SUMS
```

JSON is the detailed source of truth. CSV is the convenient comparison format.
Figures, Markdown, HTML, LaTeX, and PDF are produced only by the optional
reporting pipeline, which reads a sealed machine bundle and does not mutate it.
A reporting failure does not make a completed benchmark fail.

Raw results may include node names, internal addresses, scheduler identifiers,
software paths, and hardware inventory. Review and sanitize them before sharing
outside the cluster or institution.

## Backend Contract

Every backend adapter provides:

- Backend/product/adapter version and immutable profile identity.
- Configuration validation and effective-setting snapshots.
- Producer and consumer worker factories.
- Installation and preflight checks.
- Start, readiness, stream creation, optional prefill, snapshot, cleanup, and
  stop lifecycle operations.
- Monitoring endpoint discovery and backend metric normalization.
- A deterministic workflow stage graph and backend-specific V2 decisions.

The generic lifecycle dispatcher is:

```text
validate -> preflight -> start -> wait-ready -> create-stream -> prefill
         -> start-monitoring -> stop-monitoring -> snapshot -> stop
```

Backend-specific fields belong under `backend.<backend_id>` in cases and
`backend_metrics.<backend_id>` in results. They must not leak into the common
metric namespace.

## Adding A Backend

1. Implement `BackendAdapter` under `src/benchmark/backends/<backend_id>/`.
2. Register it in `src/benchmark/backends/registry.py`.
3. Keep product configuration under `backend.<backend_id>`.
4. Implement producer/consumer workers using the portable metrics interface.
5. Implement the generic lifecycle operations.
6. Normalize product evidence under `backend_metrics.<backend_id>`.
7. Define an explicit qualification policy without pretending unlike delivery
   semantics are equivalent.
8. Add local/fake-backend and effective-setting tests.
9. Add deterministic campaign generation under `configs/campaigns/`.
10. Implement and register `BackendWorkflow` without adding product branches to
    the common workflow engine.

Delivery guarantees, acknowledgements, batching, partitioning, subscriptions,
persistence, replication, retention, sessions, and QoS must remain explicit.

## Validation

Run the dependency-light repository checks:

```bash
./benchmark.sh check
```

Before sharing a source snapshot, create and audit a clean export:

```bash
python3 -B scripts/prepare_public_repository.py \
  --output /tmp/messaging-benchmark-suite-source
```

The export uses an allowlist, excludes Git history/results/reports/runtime
binaries, rejects common secret formats and personal infrastructure strings,
checks file sizes, and writes `SOURCE_MANIFEST.json` plus `SHA256SUMS`.

## Security And Data Handling

- Never commit SSH private keys, API tokens, passwords, `.env` files, cluster
  credentials, or generated `.local/hpc_env.sh` files.
- Pass Slurm account, partition, QoS, module, interface, and storage settings as
  environment variables or command options.
- Do not expose the benchmark's plaintext service ports to untrusted networks.
- Treat raw system inventories, node names, internal IP addresses, scheduler
  IDs, and filesystem paths as potentially sensitive operational metadata.
- Do not make this working repository public merely by changing its GitHub
  visibility. Its historical commits may contain old result artifacts and
  infrastructure metadata. Publish a fresh source-only export with new Git
  history instead.

See [`SECURITY.md`](SECURITY.md) for vulnerability reporting and the release
checklist.

## Limitations

- Default Kafka and Pulsar topologies are single-service, non-replicated test
  systems and are not fault-tolerant production deployments.
- Pulsar `5.0.0-M1` is a milestone build rather than a stable Pulsar 5.0
  production release.
- Pulsar currently supports the simultaneous producer/consumer workload path.
- Fair cross-backend comparison requires matched hardware, placement, payload,
  offered load, duration, correctness, latency, and delivery semantics.
- Capability probes and resource metrics support diagnosis; they do not replace
  application correctness and qualification.

## License

The benchmark source is licensed under the MIT License. See [`LICENSE`](LICENSE).
