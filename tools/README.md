# Runtime assets and cluster preparation

The source repository excludes downloaded distributions, wheels, JARs and build
archives. `./benchmark.sh prepare-hpc kafka` installs from files already on disk;
it does not download missing dependencies. It installs into repository-local
`.local/` and `tools/` directories without `sudo` or global package installation.

## Kafka assets

These are **defaults in the current installer scripts**, not proof of the
versions used in a historical experiment. Paths are relative to the repository.
Record the actual deployed versions and archive hashes for each campaign.

| Component | Default input | Supported path override |
| --- | --- | --- |
| Kafka | `tools/archives/kafka_2.13-4.2.0.tgz` | `LOCAL_KAFKA_ARCHIVE` |
| librdkafka | `tools/archives/librdkafka-2.14.1.tar.gz` | `LOCAL_LIBRDKAFKA_ARCHIVE` |
| confluent-kafka-python | `tools/confluent-kafka-python-master.zip` | `LOCAL_CONFLUENT_KAFKA_ARCHIVE`; this helper extracts a ZIP source archive |
| mpi4py | `tools/archives/mpi4py-4.1.1.tar.gz`, or a matching Python-ABI wheel | `LOCAL_MPI4PY_ARCHIVE` or `LOCAL_MPI4PY_WHEEL` |
| Cython, for a source build of mpi4py | `tools/archives/cython-3.2.4-py3-none-any.whl` | `LOCAL_CYTHON_WHEEL` |
| Wheel build helper, if the active Python lacks it | `tools/archives/wheel-0.45.1-py3-none-any.whl` | `LOCAL_WHEEL_BUILD_ARCHIVE` |
| Prometheus | `tools/archives/prometheus-3.11.2.linux-amd64.tar.gz` | `ARCHIVE_DIR` changes the directory, not this filename |
| node_exporter | `tools/archives/node_exporter-1.11.1.linux-amd64.tar.gz` | `ARCHIVE_DIR` |
| kafka_exporter | `tools/archives/kafka_exporter-1.9.0.linux-amd64.tar.gz` | `ARCHIVE_DIR` |
| JMX exporter | `tools/jmx_exporter/jmx_prometheus_javaagent-1.5.0.jar` | The JAR is under `INSTALL_ROOT/jmx_exporter`; it is not read from `ARCHIVE_DIR` |

The monitoring installer defaults `INSTALL_ROOT` to `tools/`. If changing it,
also update the runtime environment: the preparation helper writes monitoring
paths under the default `tools/` location. Setting `ARCHIVE_DIR` alone relocates
the input archives without changing installation paths.

Obtain assets from the respective projects' official distribution channels and
verify their published checksums. The scripts do not uniformly verify every
input; replacing an archive may also require a matching checksum override.
For example, the mpi4py and Cython helpers accept `LOCAL_MPI4PY_SHA256` and
`LOCAL_CYTHON_SHA256`. Inspect the selected installer before changing versions.
A moving `master` ZIP filename does not identify a reproducible client version;
use a pinned source ZIP and retain its hash and version.

Sources: [Kafka](../scripts/install_local_kafka.sh),
[librdkafka](../scripts/install_local_librdkafka.sh),
[Python client](../scripts/install_local_confluent_kafka.sh),
[mpi4py](../scripts/install_local_mpi4py.sh),
[pip/build helper](../scripts/python_pip_common.sh), and
[monitoring](../scripts/install_monitoring_tools.sh).

## Cluster setup

Provide a compatible compiler/toolchain, Python, Java and MPI environment.
Source builds need development headers and tools such as `make`, `pkg-config`
and an MPI compiler wrapper (`mpicc`). Python/MPI libraries must match the
compute-node ABI; a successful login-node installation is not enough to prove
that a distributed run will work.

The preparation helper defaults to GWDG-specific module names. Set
`HPC_MODULES` to the modules available on your cluster. If the required
environment is already active and no module loading is needed:

```bash
HPC_MODULES='' ./benchmark.sh prepare-hpc kafka
./benchmark.sh preflight kafka configs/campaigns/kafka/example_simultaneous_case.json
```

Preparation writes `.local/hpc_env.sh`, which the command facade loads on later
invocations. Set `INSTALL_PYTHON_DEPS=0` only when the required Python runtime
dependencies have already been supplied by the site; backend and monitoring
assets are still required. Optional plotting/Python tools are a separate
installation choice, not required to finalize machine-readable campaign results.

Inside a suitable compute allocation, the repository provides:

```bash
./scripts/hpc_prepare_compute_python.sh
./scripts/hpc_compute_preflight.sh configs/one_broker_mpi_simultaneous.json
```

These commands build/check the compute Python layer; they are not a substitute
for obtaining an allocation with the site's scheduler settings. Local
`./benchmark.sh check` and explicit workflow `--dry-run` can be used without
those installed cluster runtimes. See the [main instructions](../README.md).

## Additional adapter assets

For `./benchmark.sh prepare-hpc pulsar`, the current helpers also expect:

| Component | Default input and constraint |
| --- | --- |
| Pulsar | `tools/archives/apache-pulsar-5.0.0-M1-bin.tar.gz`; override `PULSAR_ARCHIVE` and, for a replacement, `PULSAR_ARCHIVE_SHA512` |
| Java | `tools/archives/OpenJDK21U-jdk_x64_linux_hotspot_21.0.12_8.tar.gz`; override `TEMURIN_JDK21_ARCHIVE` and matching `TEMURIN_JDK21_ARCHIVE_SHA256` |
| Python client | `pulsar_client-3.13.0-cp311-cp311-manylinux_2_28_x86_64.whl` or the corresponding `cp312` wheel under `tools/archives/` |
| CA bundle | `tools/archives/certifi-2026.7.22-py3-none-any.whl` |

The client helper currently supports the listed Linux x86_64 Python 3.11/3.12
wheels, with fixed hashes for those client and certifi versions. Changing a
version environment variable alone is insufficient for a different wheel.
The Java/server default checksums are also recorded in
`tools/archives/temurin-jdk21-checksums.txt` and
`tools/archives/pulsar-checksums.txt`. These defaults are not a compatibility
claim for untested platforms or releases. The shared MPI and monitoring assets
remain required by the preparation helper.

Sources: [server installer](../scripts/install_local_pulsar.sh),
[Java installer](../scripts/install_local_jdk21.sh), and
[client installer](../scripts/install_local_pulsar_client.sh).
