#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Validate the HPC/Slurm environment before submitting or running a benchmark.
#
# This script does not start Kafka, Slurm jobs, MPI ranks, or monitoring. It only
# checks commands, config validity, expected tool paths, and allocation size when
# it is run inside an existing Slurm allocation.
# -----------------------------------------------------------------------------

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# shellcheck source=./common.sh
source "$PROJECT_ROOT/scripts/common.sh"
# shellcheck source=./hpc_modules.sh
source "$PROJECT_ROOT/scripts/hpc_modules.sh"
# shellcheck source=./python_env_common.sh
source "$PROJECT_ROOT/scripts/python_env_common.sh"

if [[ "${SOURCE_HPC_ENV_FILE:-1}" == "1" && -f "$PROJECT_ROOT/.local/hpc_env.sh" ]]; then
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/.local/hpc_env.sh"
fi

export AUTO_LOAD_GWDG_MODULES="${AUTO_LOAD_GWDG_MODULES:-1}"
load_hpc_modules

export_kafka_hpc_python_env "$PROJECT_ROOT"

CONFIG_PATH="${1:-configs/one_broker_mpi_simultaneous.json}"
ENABLE_MONITORING="${ENABLE_MONITORING:-1}"
ENABLE_NODE_EXPORTER="${ENABLE_NODE_EXPORTER:-1}"
ENABLE_KAFKA_EXPORTER="${ENABLE_KAFKA_EXPORTER:-1}"
KAFKA_HPC_NETWORK_INTERFACE="${KAFKA_HPC_NETWORK_INTERFACE:-ib0}"
KAFKA_HPC_REQUIRE_FABRIC="${KAFKA_HPC_REQUIRE_FABRIC:-1}"

errors=0
warnings=0

pass() {
    printf '[hpc-preflight] PASS: %s\n' "$*"
}

warn() {
    warnings=$((warnings + 1))
    printf '[hpc-preflight] WARN: %s\n' "$*" >&2
}

fail() {
    errors=$((errors + 1))
    printf '[hpc-preflight] FAIL: %s\n' "$*" >&2
}

check_command() {
    local command_name="${1:?command name required}"
    if command -v "$command_name" >/dev/null 2>&1; then
        pass "command available: $command_name"
    else
        fail "command missing: $command_name"
    fi
}

check_file() {
    local path="${1:?path required}"
    local label="${2:-file}"
    if [[ -f "$path" ]]; then
        pass "$label exists: $path"
    else
        fail "$label missing: $path"
    fi
}

check_executable() {
    local path="${1:?path required}"
    local label="${2:-executable}"
    if [[ -x "$path" ]]; then
        pass "$label executable: $path"
    else
        fail "$label missing or not executable: $path"
    fi
}

printf '[hpc-preflight] Project root: %s\n' "$PROJECT_ROOT"
printf '[hpc-preflight] Config: %s\n' "$CONFIG_PATH"
printf '[hpc-preflight] Kafka network interface: %s\n' "$KAFKA_HPC_NETWORK_INTERFACE"
printf '[hpc-preflight] Require fabric interface: %s\n' "$KAFKA_HPC_REQUIRE_FABRIC"

check_file "$CONFIG_PATH" "benchmark config"

required_commands=(python3 java sbatch srun scontrol mpirun)

for command_name in "${required_commands[@]}"; do
    check_command "$command_name"
done

if python3 - <<'PY' >/dev/null 2>&1
import sys
raise SystemExit(0 if sys.version_info >= (3, 10) else 1)
PY
then
    pass "python3 version is new enough: $(python3 --version 2>&1)"
else
    fail "python3 must be >= 3.10; current version is: $(python3 --version 2>&1 || printf unknown)"
fi

if python3 - <<'PY' >/dev/null 2>&1
import mpi4py
import confluent_kafka
print(mpi4py.__version__, confluent_kafka.version())
PY
then
    pass "Python runtime imports mpi4py and confluent_kafka"
else
    fail "Python runtime cannot import mpi4py and confluent_kafka. Run scripts/hpc_prepare_repo.sh on the login node and scripts/hpc_prepare_compute_python.sh on a compute node."
fi

if [[ -n "${KAFKA_HOME:-}" ]]; then
    check_executable "$KAFKA_HOME/bin/kafka-server-start.sh" "Kafka server start"
    check_executable "$KAFKA_HOME/bin/kafka-storage.sh" "Kafka storage"
    check_executable "$KAFKA_HOME/bin/kafka-topics.sh" "Kafka topics"
    check_executable "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" "Kafka broker API versions"
else
    fail "KAFKA_HOME is not set; the Slurm path requires a cluster Kafka install"
fi

JMX_EXPORTER_JAR="${JMX_EXPORTER_JAR:-$PROJECT_ROOT/tools/jmx_exporter/jmx_prometheus_javaagent-1.5.0.jar}"
JMX_EXPORTER_CONFIG="${JMX_EXPORTER_CONFIG:-$PROJECT_ROOT/monitoring/jmx_exporter_config.yml}"
check_file "$JMX_EXPORTER_JAR" "JMX exporter jar"
check_file "$JMX_EXPORTER_CONFIG" "JMX exporter config"

if [[ "$ENABLE_MONITORING" == "1" ]]; then
    if [[ -n "${PROMETHEUS_HOME:-}" ]]; then
        check_executable "$PROMETHEUS_HOME/prometheus" "Prometheus"
    else
        fail "PROMETHEUS_HOME is required when ENABLE_MONITORING=1"
    fi

    if [[ "$ENABLE_NODE_EXPORTER" == "1" ]]; then
        if [[ -n "${NODE_EXPORTER_HOME:-}" ]]; then
            check_executable "$NODE_EXPORTER_HOME/node_exporter" "node_exporter"
        else
            fail "NODE_EXPORTER_HOME is required when ENABLE_MONITORING=1 and ENABLE_NODE_EXPORTER=1"
        fi
    fi

    if [[ "$ENABLE_KAFKA_EXPORTER" == "1" ]]; then
        KAFKA_EXPORTER_HOME="${KAFKA_EXPORTER_HOME:-$PROJECT_ROOT/tools/kafka_exporter-current}"
        check_executable "$KAFKA_EXPORTER_HOME/kafka_exporter" "kafka_exporter"
    fi
else
    warn "ENABLE_MONITORING is not 1; final reports will not contain Kafka broker throughput"
fi

if [[ -d /dev/shm && -w /dev/shm ]]; then
    pass "/dev/shm exists and is writable on this node"
else
    warn "/dev/shm is not writable here; verify compute nodes before production runs"
fi

if [[ -n "${SLURM_JOB_NODELIST:-}" ]]; then
    if mapfile -t PREFLIGHT_NODES < <(get_allocated_nodes) && (( ${#PREFLIGHT_NODES[@]} > 0 )); then
        if resolved_address="$(resolve_node_service_address "${PREFLIGHT_NODES[0]}")"; then
            pass "network interface ${KAFKA_HPC_NETWORK_INTERFACE} resolves on ${PREFLIGHT_NODES[0]} as ${resolved_address}"
        else
            fail "network interface ${KAFKA_HPC_NETWORK_INTERFACE} did not resolve on ${PREFLIGHT_NODES[0]}"
        fi
    else
        warn "Could not inspect allocated nodes for network interface validation"
    fi
else
    warn "Not inside a Slurm allocation; ${KAFKA_HPC_NETWORK_INTERFACE} will be validated on compute nodes"
fi

CONFIG_INFO=""
if CONFIG_INFO="$(
    PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" python3 - "$CONFIG_PATH" <<'PY'
import shlex
import sys
from pathlib import Path

from src.benchmark.config_loader import load_benchmark_config

path = Path(sys.argv[1])
config = load_benchmark_config(path)
if config.mode != "single":
    raise SystemExit("The current runner supports only mode='single'")

print(f"BACKEND_ID={shlex.quote(config.backend_id)}")
print(f"MODE={shlex.quote(config.mode)}")
print("ESTIMATED_CASES=1")
print(f"BROKER_COUNT_MAX={config.broker_count}")
print(f"PRODUCER_RANKS={config.producer_ranks}")
print(f"CONSUMER_RANKS={config.consumer_ranks}")
print(f"TOTAL_MPI_RANKS={config.total_mpi_ranks}")
PY
)"; then
    eval "$CONFIG_INFO"
    pass "config validates as mode=${MODE:-unknown}, estimated cases=${ESTIMATED_CASES:-unknown}"
else
    fail "config validation failed: $CONFIG_PATH"
fi

if [[ -n "${SLURM_JOB_NODELIST:-}" ]] && command -v scontrol >/dev/null 2>&1; then
    mapfile -t allocated_nodes < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
    allocated_count="${#allocated_nodes[@]}"
    controller_nodes="${SLURM_CONTROLLER_NODES:-0}"
    producer_nodes="${SLURM_PRODUCER_NODES:-$(( PRODUCER_RANKS > 0 ? 1 : 0 ))}"
    consumer_nodes="${SLURM_CONSUMER_NODES:-$(( CONSUMER_RANKS > 0 ? 1 : 0 ))}"
    needed_count=$(( ${BROKER_COUNT_MAX:-1} + 1 + controller_nodes + producer_nodes + consumer_nodes ))
    if (( allocated_count >= needed_count )); then
        pass "Slurm allocation has ${allocated_count} nodes; need at least ${needed_count}"
    else
        fail "Slurm allocation has ${allocated_count} nodes; need at least ${needed_count}"
    fi
else
    warn "not running inside a Slurm allocation; node-count check skipped"
fi

printf '[hpc-preflight] Summary: %s error(s), %s warning(s)\n' "$errors" "$warnings"
if (( errors > 0 )); then
    exit 1
fi
