#!/usr/bin/env bash

# -----------------------------------------------------------------------------
# Source-able helper for loading HPC environment modules.
#
# Callers can set:
#   HPC_MODULES="gcc/14.2.0 openmpi/5.0.6 python/3.11.9 openjdk/17.0.11_9"
#
# The helper is intentionally no-op when HPC_MODULES is empty so the benchmark
# remains portable to clusters that manage environments differently.
# -----------------------------------------------------------------------------

GWDG_DEFAULT_HPC_MODULES="gcc/14.2.0 openmpi/5.0.6 python/3.11.9 openjdk/17.0.11_9"
GWDG_DEFAULT_PULSAR_HPC_MODULES="gcc/14.2.0 openmpi/5.0.6 python/3.11.9"

apply_default_hpc_modules() {
    if [[ ! ${HPC_MODULES+x} && "${AUTO_LOAD_GWDG_MODULES:-0}" == "1" ]]; then
        export HPC_MODULES="$GWDG_DEFAULT_HPC_MODULES"
    fi
}

select_backend_hpc_modules() {
    local backend_id="${1:?backend id required}"

    if [[ -n "${SLURM_HPC_MODULES:-}" ]]; then
        export HPC_MODULES="$SLURM_HPC_MODULES"
    elif [[ "$backend_id" == "kafka" && -n "${KAFKA_HPC_MODULES:-}" ]]; then
        export HPC_MODULES="$KAFKA_HPC_MODULES"
    elif [[ "$backend_id" == "pulsar" && -n "${PULSAR_HPC_MODULES:-}" ]]; then
        export HPC_MODULES="$PULSAR_HPC_MODULES"
    fi

    activate_backend_java "$backend_id"
}

activate_backend_java() {
    local backend_id="${1:?backend id required}"

    if [[ "$backend_id" != "pulsar" || -z "${PULSAR_JAVA_HOME:-}" ]]; then
        return 0
    fi
    if [[ ! -x "$PULSAR_JAVA_HOME/bin/java" ]]; then
        printf '[hpc-modules] ERROR: PULSAR_JAVA_HOME does not contain bin/java: %s\n' \
            "$PULSAR_JAVA_HOME" >&2
        return 1
    fi

    export JAVA_HOME="$PULSAR_JAVA_HOME"
    case ":$PATH:" in
        *":$JAVA_HOME/bin:"*) ;;
        *) export PATH="$JAVA_HOME/bin:$PATH" ;;
    esac
}

ensure_module_function() {
    if type module >/dev/null 2>&1; then
        return 0
    fi

    for init_file in \
        /etc/profile.d/modules.sh \
        /etc/profile.d/lmod.sh \
        /usr/share/lmod/lmod/init/bash; do
        if [[ -f "$init_file" ]]; then
            # shellcheck source=/dev/null
            source "$init_file"
            if type module >/dev/null 2>&1; then
                return 0
            fi
        fi
    done

    return 1
}

load_hpc_modules() {
    apply_default_hpc_modules

    local module_list="${HPC_MODULES:-}"
    [[ -n "$module_list" ]] || return 0

    if ! ensure_module_function; then
        printf '[hpc-modules] ERROR: HPC_MODULES is set but the module command is unavailable\n' >&2
        return 1
    fi

    # Keep the user's current module state unless they explicitly ask for a
    # purge. This avoids surprising cluster-specific default environments.
    if [[ "${HPC_MODULE_PURGE_FIRST:-0}" == "1" ]]; then
        module purge
    fi

    local module_spec
    for module_spec in $module_list; do
        load_hpc_module_spec "$module_spec" || return 1
    done
}

load_hpc_module_spec() {
    local module_spec="${1:?module spec required}"
    local loaded=0
    local module_name
    local -a module_candidates

    IFS='|' read -r -a module_candidates <<< "$module_spec"
    for module_name in "${module_candidates[@]}"; do
        [[ -n "$module_name" ]] || continue

        printf '[hpc-modules] loading %s\n' "$module_name"
        if module load "$module_name" >/dev/null 2>&1 && module is-loaded "$module_name" >/dev/null 2>&1; then
            loaded=1
            break
        fi

        printf '[hpc-modules] retrying with --ignore_cache: %s\n' "$module_name" >&2
        if module --ignore_cache load "$module_name" >/dev/null 2>&1 && module is-loaded "$module_name" >/dev/null 2>&1; then
            loaded=1
            break
        fi
    done

    if [[ "$loaded" != "1" ]]; then
        printf '[hpc-modules] ERROR: could not load any module candidate from: %s\n' "$module_spec" >&2
        return 1
    fi
}
