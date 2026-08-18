# Runtime Assets

The public source repository does not commit downloaded distributions, wheels,
JARs, or build archives. Several HPC installation helpers are deliberately
offline: they install only from files already present under `tools/archives/`
or an operator-provided path.

The tested source tree expects these runtime families:

| Component | Tested artifact or version |
| --- | --- |
| Kafka | `kafka_2.13-4.2.0.tgz` |
| Pulsar | `apache-pulsar-5.0.0-M1-bin.tar.gz` |
| Pulsar Java | Temurin 21.0.12+8 Linux x64 |
| librdkafka | `librdkafka-2.14.1.tar.gz` |
| confluent-kafka-python | Source archive compatible with librdkafka 2.14.1 |
| Prometheus | 3.11.2 Linux amd64 |
| node_exporter | 1.11.1 Linux amd64 |
| kafka_exporter | 1.9.0 Linux amd64 |
| JMX exporter agent | 1.5.0 |
| Pulsar Python client | 3.13.0 for the selected Python ABI |
| mpi4py | 4.1.1 for the selected Python/MPI ABI |

Download artifacts only from the relevant official Apache, Adoptium, GitHub
release, or Python Package Index page. Verify publisher checksums and licensing
before placing them in the repository-local asset directory. Pulsar and Temurin
checksums used by the tested environment are recorded in:

- `tools/archives/pulsar-checksums.txt`
- `tools/archives/temurin-jdk21-checksums.txt`

The large files themselves are ignored. You may also point each installer to a
site-managed mirror or shared filesystem using its documented `LOCAL_*_ARCHIVE`
environment override. Do not commit authenticated mirror URLs or credentials.

After the assets are available, run:

```bash
./benchmark.sh prepare-hpc kafka
./benchmark.sh prepare-hpc pulsar
```

The installers extract into ignored `.local/` or `tools/*-current` paths and do
not use `sudo` or install packages globally.
