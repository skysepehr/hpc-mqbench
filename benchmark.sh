#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

prepare_backend_shell() {
    local requested_backend="${1:-}"

    if [[ -f "$PROJECT_ROOT/.local/hpc_env.sh" ]]; then
        # shellcheck source=/dev/null
        source "$PROJECT_ROOT/.local/hpc_env.sh"
    fi
    # shellcheck source=./scripts/hpc_modules.sh
    source "$PROJECT_ROOT/scripts/hpc_modules.sh"
    if [[ -n "$requested_backend" ]]; then
        select_backend_hpc_modules "$requested_backend"
    fi
    load_hpc_modules
    # shellcheck source=./scripts/python_env_common.sh
    source "$PROJECT_ROOT/scripts/python_env_common.sh"
    export_kafka_hpc_python_env "$PROJECT_ROOT"
}

usage() {
    cat <<'EOF'
HPC-MQBench
A reproducible, Slurm-orchestrated, qualification-first benchmark suite
for distributed messaging systems

Usage:
  ./benchmark.sh reproducible BACKEND
  ./benchmark.sh reproducible BACKEND [--phase all|v1|phase1|validation|v2] [OPTIONS]
      Partition options: --partition NAME|auto
                         --partition-candidates NAME[,NAME...]
                         --list-partitions
  ./benchmark.sh backend list
  ./benchmark.sh report MACHINE_RESULTS_DIR REPORT_OUTPUT_DIR [OPTIONS]
  ./benchmark.sh check
  ./benchmark.sh prepare-hpc [kafka|pulsar|all]
  ./benchmark.sh preflight BACKEND CONFIG
  ./benchmark.sh dry-run BACKEND CONFIG
  ./benchmark.sh submit-case BACKEND CONFIG
  ./benchmark.sh submit-batch BACKEND MANIFEST
  ./benchmark.sh submit-campaign BACKEND MANIFEST
  ./benchmark.sh analyze BACKEND RESULTS_ROOT [STAGE]
  ./benchmark.sh publish
  ./benchmark.sh verify-results
EOF
}

require_backend() {
    local requested="${1:-}"
    [[ -n "$requested" ]] || { usage >&2; exit 2; }
    prepare_backend_shell "$requested"
    PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" python3 -B - "$requested" <<'PY'
import sys
from src.benchmark.backends import get_backend
get_backend(sys.argv[1])
PY
}

command_name="${1:-}"
case "$command_name" in
    reproducible)
        [[ -n "${2:-}" ]] || { usage >&2; exit 2; }
        exec "$PROJECT_ROOT/scripts/run_reproducible_benchmark.sh" "${@:2}"
        ;;
    report)
        [[ -n "${2:-}" && -n "${3:-}" ]] || { usage >&2; exit 2; }
        exec "$PROJECT_ROOT/scripts/render_reproducible_report.sh" "${@:2}"
        ;;
    backend)
        [[ "${2:-}" == "list" ]] || { usage >&2; exit 2; }
        prepare_backend_shell
        PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}" \
            python3 -B -m src.benchmark.backends.lifecycle_cli list
        ;;
    check)
        prepare_backend_shell "${BACKEND_ID:-}"
        exec "$PROJECT_ROOT/scripts/dev_check.sh"
        ;;
    prepare-hpc)
        selected_backend="${2:-all}"
        case "$selected_backend" in
            kafka)
                export INSTALL_KAFKA_BACKEND=1
                export INSTALL_PULSAR_BACKEND=0
                ;;
            pulsar)
                export INSTALL_KAFKA_BACKEND=0
                export INSTALL_PULSAR_BACKEND=1
                ;;
            all)
                export INSTALL_KAFKA_BACKEND=1
                export INSTALL_PULSAR_BACKEND=1
                ;;
            *)
                printf 'Unknown backend selection for prepare-hpc: %s\n' \
                    "$selected_backend" >&2
                usage >&2
                exit 2
                ;;
        esac
        exec "$PROJECT_ROOT/scripts/hpc_prepare_repo.sh"
        ;;
    preflight)
        require_backend "${2:-}"
        exec "$PROJECT_ROOT/scripts/backend_lifecycle.sh" "$2" preflight "${3:-}"
        ;;
    dry-run)
        require_backend "${2:-}"
        "$PROJECT_ROOT/scripts/backend_lifecycle.sh" "$2" validate "${3:-}" >/dev/null
        DRY_RUN=1 exec "$PROJECT_ROOT/scripts/submit_hpc_case.sh" "$3"
        ;;
    submit-case)
        require_backend "${2:-}"
        "$PROJECT_ROOT/scripts/backend_lifecycle.sh" "$2" validate "${3:-}" >/dev/null
        exec "$PROJECT_ROOT/scripts/submit_hpc_case.sh" "$3"
        ;;
    submit-batch)
        require_backend "${2:-}"
        manifest="${3:-}"
        [[ -f "$manifest" ]] || { usage >&2; exit 2; }
        if [[ "$2" == "pulsar" ]]; then
            python3 -B "$PROJECT_ROOT/scripts/check_pulsar_phase1_gate.py" \
                "$manifest"
        fi
        exec "$PROJECT_ROOT/scripts/submit_backend_batch.sh" "$2" "$manifest"
        ;;
    submit-campaign)
        require_backend "${2:-}"
        manifest="${3:-}"
        [[ -f "$manifest" ]] || { usage >&2; exit 2; }
        if [[ "$2" == "pulsar" ]]; then
            python3 -B "$PROJECT_ROOT/scripts/check_pulsar_phase1_gate.py" \
                "$manifest"
        fi
        if [[ "$2" == "kafka" \
            && "$(head -n 1 "$manifest")" == *stage* ]]; then
            exec "$PROJECT_ROOT/scripts/submit_broker_tuning_v2_campaign.sh" \
                "$manifest"
        fi
        exec "$PROJECT_ROOT/scripts/submit_campaign.sh" "$2" "${3:-}"
        ;;
    analyze)
        require_backend "${2:-}"
        results_root="${3:-}"
        [[ -n "$results_root" ]] || { usage >&2; exit 2; }
        stage="${4:-case-summary}"
        if [[ "$2" == "pulsar" && "$stage" == "phase1" ]]; then
            exec python3 -B "$PROJECT_ROOT/scripts/analyze_pulsar_phase1.py" \
                --results-root "$results_root" \
                --output-dir "$PROJECT_ROOT/results/rebuilt/pulsar/phase1"
        fi
        if [[ "$2" != "kafka" ]]; then
            exec python3 -B "$PROJECT_ROOT/scripts/analyze_backend_results.py" \
                --backend "$2" \
                --results-root "$results_root" \
                --output-dir "$PROJECT_ROOT/results/rebuilt/$2/case-summary"
        fi
        stage="${4:-profile_screening}"
        analysis_input=(--results-root "$results_root")
        if [[ -f "$results_root" ]]; then
            analysis_input=(--case-summary "$results_root")
        elif [[ -f "$results_root/kafka_broker_tuning_v2_cases.json" ]]; then
            analysis_input=(
                --case-summary
                "$results_root/kafka_broker_tuning_v2_cases.json"
            )
        elif [[ -f "$results_root/kafka_broker_tuning_v2_cases.csv" ]]; then
            analysis_input=(
                --case-summary
                "$results_root/kafka_broker_tuning_v2_cases.csv"
            )
        fi
        exec python3 -B "$PROJECT_ROOT/scripts/analyze_broker_tuning_v2.py" \
            "${analysis_input[@]}" \
            --stage "$stage" \
            --output-dir "$PROJECT_ROOT/results/rebuilt/kafka/v2/$stage"
        ;;
    publish)
        prepare_backend_shell
        exec python3 -B "$PROJECT_ROOT/scripts/publish_results.py"
        ;;
    verify-results)
        prepare_backend_shell
        exec python3 -B "$PROJECT_ROOT/scripts/publish_results.py" --check
        ;;
    -h|--help|help|"")
        usage
        ;;
    *)
        printf 'Unknown command: %s\n\n' "$command_name" >&2
        usage >&2
        exit 2
        ;;
esac
