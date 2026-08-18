#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Resolve the backend before loading modules so the one-command facade selects
# the same tested runtime stack as its Slurm jobs.
REQUESTED_BACKEND=""
if [[ $# -gt 0 && "$1" != --* ]]; then
    REQUESTED_BACKEND="$1"
fi
for ((index = 1; index <= $#; index++)); do
    argument="${!index}"
    if [[ "$argument" == --backend=* ]]; then
        REQUESTED_BACKEND="${argument#--backend=}"
        break
    fi
    if [[ "$argument" == "--backend" && $index -lt $# ]]; then
        next_index=$((index + 1))
        REQUESTED_BACKEND="${!next_index}"
        break
    fi
done

# The public shell interface accepts a concise partition option and resolves
# "auto" before the Python workflow freezes its scheduler settings. A resumed
# workflow therefore keeps the originally selected partition.
PARTITION_REQUEST=""
PARTITION_CANDIDATES="${BENCHMARK_PARTITION_CANDIDATES:-standard96s,medium96s}"
LIST_PARTITIONS=0
WORKFLOW_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --partition|--slurm-partition)
            [[ $# -ge 2 ]] || {
                printf '%s requires a value\n' "$1" >&2
                exit 2
            }
            [[ -z "$PARTITION_REQUEST" ]] || {
                printf 'Slurm partition was specified more than once\n' >&2
                exit 2
            }
            PARTITION_REQUEST="$2"
            shift 2
            ;;
        --partition=*|--slurm-partition=*)
            [[ -z "$PARTITION_REQUEST" ]] || {
                printf 'Slurm partition was specified more than once\n' >&2
                exit 2
            }
            PARTITION_REQUEST="${1#*=}"
            shift
            ;;
        --partition-candidates)
            [[ $# -ge 2 ]] || {
                printf '%s requires a value\n' "$1" >&2
                exit 2
            }
            PARTITION_CANDIDATES="$2"
            shift 2
            ;;
        --partition-candidates=*)
            PARTITION_CANDIDATES="${1#*=}"
            shift
            ;;
        --list-partitions)
            LIST_PARTITIONS=1
            shift
            ;;
        *)
            WORKFLOW_ARGS+=("$1")
            shift
            ;;
    esac
done

if [[ -f "$PROJECT_ROOT/.local/hpc_env.sh" ]]; then
    # shellcheck source=/dev/null
    source "$PROJECT_ROOT/.local/hpc_env.sh"
fi
# shellcheck source=./hpc_modules.sh
source "$PROJECT_ROOT/scripts/hpc_modules.sh"
if [[ -n "$REQUESTED_BACKEND" ]]; then
    select_backend_hpc_modules "$REQUESTED_BACKEND"
fi
load_hpc_modules

# Keep the direct command equivalent to the root facade. Only benchmark runtime
# dependencies belong on this path; reporting dependencies are loaded by the
# separate render command.
# shellcheck source=./python_env_common.sh
source "$PROJECT_ROOT/scripts/python_env_common.sh"
export_benchmark_hpc_python_env "$PROJECT_ROOT"

PARTITION_SELECTOR=(
    python3 -B "$PROJECT_ROOT/scripts/select_slurm_partition.py"
    --candidates "$PARTITION_CANDIDATES"
    --required-nodes "${BENCHMARK_REQUIRED_NODES:-4}"
    --minimum-cpus "${BENCHMARK_MIN_CPUS_PER_NODE:-192}"
    --minimum-memory-mb "${BENCHMARK_MIN_MEMORY_MB_PER_NODE:-240000}"
)
IFS=',' read -r -a REQUIRED_PARTITION_FEATURES <<< \
    "${BENCHMARK_PARTITION_REQUIRED_FEATURES:-sapphirerapids}"
for feature in "${REQUIRED_PARTITION_FEATURES[@]}"; do
    [[ -n "$feature" ]] || continue
    PARTITION_SELECTOR+=(--required-feature "$feature")
done

if [[ "$LIST_PARTITIONS" == "1" ]]; then
    exec "${PARTITION_SELECTOR[@]}" --format table
fi

if [[ "$PARTITION_REQUEST" == "auto" ]]; then
    PARTITION_REQUEST="$("${PARTITION_SELECTOR[@]}" --format value)"
fi
if [[ -n "$PARTITION_REQUEST" ]]; then
    WORKFLOW_ARGS+=(--slurm-partition "$PARTITION_REQUEST")
fi

exec python3 -B "$PROJECT_ROOT/scripts/reproducible_benchmark_workflow.py" \
    "${WORKFLOW_ARGS[@]}"
