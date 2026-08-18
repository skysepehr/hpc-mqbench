#!/usr/bin/env bash

# -----------------------------------------------------------------------------
# Source-able helper for repository-local Python/native runtime paths.
#
# Path order for Slurm/HPC runs:
#   1. .local/python-hpc  (compute-node mpi4py, when present)
#   2. .local/python      (shared backend client packages)
#   3. project root       (src/ and models/)
#
# Set KAFKA_HPC_DISABLE_LOCAL_PYTHONPATH=1 to use only the project root plus
# whatever Python packages the active environment already provides.
# -----------------------------------------------------------------------------

benchmark_hpc_pythonpath() {
    local project_root="${1:?project root is required}"
    local -a candidates=()
    local -a paths=()

    if [[ "${KAFKA_HPC_DISABLE_LOCAL_PYTHONPATH:-0}" != "1" ]]; then
        if [[ -d "$project_root/.local/python-hpc/mpi4py" \
            || -f "$project_root/.local/python-hpc/mpi4py.py" ]]; then
            candidates+=("$project_root/.local/python-hpc")
        fi

        if [[ -d "$project_root/.local/python" ]]; then
            candidates+=("$project_root/.local/python")
        fi
    fi

    candidates+=("$project_root")

    if [[ -n "${PYTHONPATH:-}" ]]; then
        local -a existing_pythonpath=()
        IFS=':' read -r -a existing_pythonpath <<< "$PYTHONPATH"
        candidates+=("${existing_pythonpath[@]}")
    fi

    local candidate_path existing_path found
    for candidate_path in "${candidates[@]}"; do
        [[ -n "$candidate_path" ]] || continue
        found=0
        for existing_path in "${paths[@]}"; do
            if [[ "$existing_path" == "$candidate_path" ]]; then
                found=1
                break
            fi
        done
        if [[ "$found" == "0" ]]; then
            paths+=("$candidate_path")
        fi
    done

    local joined=""
    local path_entry
    for path_entry in "${paths[@]}"; do
        if [[ -z "$joined" ]]; then
            joined="$path_entry"
        else
            joined="$joined:$path_entry"
        fi
    done

    printf '%s\n' "$joined"
}

export_benchmark_hpc_python_env() {
    local project_root="${1:?project root is required}"

    if [[ -f "$project_root/.local/librdkafka/env.sh" ]]; then
        # shellcheck source=/dev/null
        source "$project_root/.local/librdkafka/env.sh"
    elif [[ -d "$project_root/.local/librdkafka/lib" ]]; then
        export LD_LIBRARY_PATH="$project_root/.local/librdkafka/lib:${LD_LIBRARY_PATH:-}"
    fi

    export PYTHONPATH
    PYTHONPATH="$(benchmark_hpc_pythonpath "$project_root")"
}

print_benchmark_hpc_python_env() {
    local project_root="${1:?project root is required}"
    printf 'PYTHONPATH=%s\n' "$(benchmark_hpc_pythonpath "$project_root")"
    printf 'LD_LIBRARY_PATH=%s\n' "${LD_LIBRARY_PATH:-}"
}

# Compatibility aliases retained for existing Kafka scripts and external users.
kafka_hpc_pythonpath() {
    benchmark_hpc_pythonpath "$@"
}

export_kafka_hpc_python_env() {
    export_benchmark_hpc_python_env "$@"
}

print_kafka_hpc_python_env() {
    print_benchmark_hpc_python_env "$@"
}
