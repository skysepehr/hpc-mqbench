#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Prepare a cloned repository on the HPC login node.
#
# This script loads the requested module stack, extracts repo-local messaging
# backends and monitoring tools, and optionally installs Python dependencies into
# .local/python. Compute-node mpi4py can be built later into .local/python-hpc
# with scripts/hpc_prepare_compute_python.sh. It does not use sudo and does not
# download anything.
# -----------------------------------------------------------------------------

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

INSTALL_PYTHON_DEPS="${INSTALL_PYTHON_DEPS:-1}"
INSTALL_OPTIONAL_PYTHON_TOOLS="${INSTALL_OPTIONAL_PYTHON_TOOLS:-0}"
INSTALL_KAFKA_BACKEND="${INSTALL_KAFKA_BACKEND:-1}"
INSTALL_PULSAR_BACKEND="${INSTALL_PULSAR_BACKEND:-1}"

# GWDG-friendly default discovered from the login node. Override HPC_MODULES for
# another cluster or set HPC_MODULES= to skip module loading explicitly.
export AUTO_LOAD_GWDG_MODULES="${AUTO_LOAD_GWDG_MODULES:-1}"

# shellcheck source=./hpc_modules.sh
source "$PROJECT_ROOT/scripts/hpc_modules.sh"

KAFKA_HPC_MODULES="${KAFKA_HPC_MODULES:-${HPC_MODULES:-$GWDG_DEFAULT_HPC_MODULES}}"
PULSAR_HPC_MODULES="${PULSAR_HPC_MODULES:-$GWDG_DEFAULT_PULSAR_HPC_MODULES}"
export KAFKA_HPC_MODULES PULSAR_HPC_MODULES

if [[ "$INSTALL_KAFKA_BACKEND" == "0" \
    && "$INSTALL_PULSAR_BACKEND" == "1" \
    && -n "${PULSAR_HPC_MODULES:-}" ]]; then
    export HPC_MODULES="$PULSAR_HPC_MODULES"
fi

load_hpc_modules

if [[ "$INSTALL_PULSAR_BACKEND" == "1" ]]; then
    ./scripts/install_local_jdk21.sh
    export PULSAR_JAVA_HOME="$PROJECT_ROOT/.local/jdk21-current"
    if [[ "$INSTALL_KAFKA_BACKEND" == "0" ]]; then
        activate_backend_java pulsar
    fi
fi

# shellcheck source=./python_env_common.sh
source "$PROJECT_ROOT/scripts/python_env_common.sh"

printf '[hpc-prepare] Project root: %s\n' "$PROJECT_ROOT"
printf '[hpc-prepare] Modules: %s\n' "$HPC_MODULES"
printf '[hpc-prepare] Python: %s\n' "$(python3 --version 2>&1)"
printf '[hpc-prepare] Java: %s\n' "$(java -version 2>&1 | head -1)"
printf '[hpc-prepare] mpirun: %s\n' "$(command -v mpirun || true)"

./scripts/install_monitoring_tools.sh
if [[ "$INSTALL_KAFKA_BACKEND" == "1" ]]; then
    ./scripts/install_local_kafka.sh
fi
if [[ "$INSTALL_PULSAR_BACKEND" == "1" ]]; then
    ./scripts/install_local_pulsar.sh
fi

if [[ "$INSTALL_PYTHON_DEPS" == "1" ]]; then
    if [[ "$INSTALL_KAFKA_BACKEND" == "1" ]]; then
        ./scripts/install_local_librdkafka.sh
        ./scripts/install_local_confluent_kafka.sh
    fi
    ./scripts/install_local_mpi4py.sh
    if [[ "$INSTALL_PULSAR_BACKEND" == "1" ]]; then
        ./scripts/install_local_pulsar_client.sh
    fi
fi

if [[ "$INSTALL_OPTIONAL_PYTHON_TOOLS" == "1" ]]; then
    ./scripts/install_local_optional_python_tools.sh
fi

mkdir -p "$PROJECT_ROOT/.local"
cat > "$PROJECT_ROOT/.local/hpc_env.sh" <<EOF
# Source this file before Slurm submission or let run_all.sh source it.
export HPC_MODULES="$HPC_MODULES"
export KAFKA_HPC_MODULES="\${KAFKA_HPC_MODULES:-$KAFKA_HPC_MODULES}"
export PULSAR_HPC_MODULES="\${PULSAR_HPC_MODULES:-${PULSAR_HPC_MODULES:-}}"
export PULSAR_JAVA_HOME="\${PULSAR_JAVA_HOME:-$PROJECT_ROOT/.local/jdk21-current}"
export KAFKA_HOME="$PROJECT_ROOT/.local/kafka-current"
export PULSAR_HOME="$PROJECT_ROOT/.local/pulsar-current"
export PROMETHEUS_HOME="$PROJECT_ROOT/tools/prometheus-current"
export NODE_EXPORTER_HOME="$PROJECT_ROOT/tools/node_exporter-current"
export KAFKA_EXPORTER_HOME="$PROJECT_ROOT/tools/kafka_exporter-current"
export JMX_EXPORTER_JAR="$PROJECT_ROOT/tools/jmx_exporter/jmx_prometheus_javaagent-1.5.0.jar"
export JMX_EXPORTER_CONFIG="$PROJECT_ROOT/monitoring/jmx_exporter_config.yml"
export KAFKA_HPC_PYTHON_HPC_DIR="$PROJECT_ROOT/.local/python-hpc"
export KAFKA_HPC_PYTHON_SHARED_DIR="$PROJECT_ROOT/.local/python"
if [[ -f "$PROJECT_ROOT/scripts/python_env_common.sh" ]]; then
    source "$PROJECT_ROOT/scripts/python_env_common.sh"
    export_kafka_hpc_python_env "$PROJECT_ROOT"
else
    export PYTHONPATH="$PROJECT_ROOT/.local/python:$PROJECT_ROOT:\${PYTHONPATH:-}"
    export LD_LIBRARY_PATH="$PROJECT_ROOT/.local/librdkafka/lib:\${LD_LIBRARY_PATH:-}"
fi
EOF

printf '[hpc-prepare] Wrote environment file: %s\n' "$PROJECT_ROOT/.local/hpc_env.sh"
printf '[hpc-prepare] Suggested backend checks:\n'
if [[ "$INSTALL_KAFKA_BACKEND" == "1" ]]; then
    printf '  ./benchmark.sh preflight kafka configs/one_broker_mpi_simultaneous.json\n'
fi
if [[ "$INSTALL_PULSAR_BACKEND" == "1" ]]; then
    printf '  ./benchmark.sh preflight pulsar configs/campaigns/pulsar/examples/example_simultaneous_case.json\n'
fi
printf '[hpc-prepare] On a compute node, build and validate the MPI Python layer with:\n'
printf '  ./scripts/hpc_prepare_compute_python.sh\n'
printf '  ./scripts/hpc_compute_preflight.sh configs/one_broker_mpi_simultaneous.json\n'
