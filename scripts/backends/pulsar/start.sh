#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./common.sh
source "$SCRIPT_DIR/common.sh"

pulsar_require_case
pulsar_load_nodes
require_nonempty "PULSAR_MEM" "${PULSAR_MEM:-}"
require_nonempty "PULSAR_PROFILE_ID" "${PULSAR_PROFILE_ID:-}"
require_nonempty "PULSAR_PROFILE_SHA256" "${PULSAR_PROFILE_SHA256:-}"
require_command srun

RUNTIME_DIR="$(pulsar_runtime_dir)"
CONF_DIR="$RUNTIME_DIR/conf"
PID_DIR="$RUNTIME_DIR/pids"
LOG_DIR="$CASE_DIR/logs/pulsar"
DATA_ROOT="$(pulsar_ram_root)"
PID_FILE="$PID_DIR/pulsar.pid"
SRUN_PID_FILE="$PID_DIR/pulsar.srun.pid"
LOG_FILE="$LOG_DIR/standalone.log"
CONFIG_FILE="$CONF_DIR/standalone.conf"
PROFILE_PATH="$PROJECT_ROOT/configs/backends/pulsar/profiles/${PULSAR_PROFILE_ID}.json"
HPC_NODE_ENV_SNIPPET="$(build_hpc_node_env_snippet "$PROJECT_ROOT")"

ensure_dir "$CONF_DIR"
ensure_dir "$PID_DIR"
ensure_dir "$LOG_DIR"
require_file "$PROFILE_PATH"
python3 -B "$PROJECT_ROOT/scripts/verify_pulsar_profile.py" \
    "$CONFIG_PATH" --project-root "$PROJECT_ROOT" >/dev/null
cp "$PROFILE_PATH" "$RUNTIME_DIR/pulsar_profile_snapshot.json"
printf '%s\n' "$PULSAR_PROFILE_SHA256" > "$RUNTIME_DIR/pulsar_profile.sha256"
cp -a "$PULSAR_HOME/conf/." "$CONF_DIR/"

python3 "$SCRIPT_DIR/set_property.py" "$CONFIG_FILE" advertisedAddress "$PULSAR_ADDRESS"
python3 "$SCRIPT_DIR/set_property.py" "$CONFIG_FILE" bindAddress 0.0.0.0
python3 "$SCRIPT_DIR/set_property.py" "$CONFIG_FILE" managedLedgerDefaultEnsembleSize 1
python3 "$SCRIPT_DIR/set_property.py" "$CONFIG_FILE" managedLedgerDefaultWriteQuorum 1
python3 "$SCRIPT_DIR/set_property.py" "$CONFIG_FILE" managedLedgerDefaultAckQuorum 1
python3 "$SCRIPT_DIR/set_property.py" "$CONFIG_FILE" allowAutoTopicCreation false
python3 "$SCRIPT_DIR/set_property.py" "$CONFIG_FILE" includeStandardPrometheusMetrics true
python3 "$SCRIPT_DIR/set_property.py" "$CONFIG_FILE" exposeTopicLevelMetricsInPrometheus false
python3 "$SCRIPT_DIR/set_property.py" "$CONFIG_FILE" exposeConsumerLevelMetricsInPrometheus false
sha256sum "$CONFIG_FILE" > "$CONFIG_FILE.sha256"

JAVA_VERSION_TEXT="$(java -version 2>&1 | head -n 1)"
PULSAR_VERSION_TEXT="$("$PULSAR_HOME/bin/pulsar" version 2>&1)"
python3 -B "$PROJECT_ROOT/scripts/verify_pulsar_runtime_profile.py" \
    --profile "$PROFILE_PATH" \
    --standalone-conf "$CONFIG_FILE" \
    --jvm-memory "$PULSAR_MEM" \
    --java-version "$JAVA_VERSION_TEXT" \
    --pulsar-version "$PULSAR_VERSION_TEXT" \
    --service-node "$PULSAR_NODE" \
    --service-address "$PULSAR_ADDRESS" \
    --data-root "$DATA_ROOT" \
    --output "$RUNTIME_DIR/pulsar_runtime_manifest.json"
log_info "Pulsar profile: ${PULSAR_PROFILE_ID} (${PULSAR_PROFILE_SHA256})"

srun --overlap --cpu-bind=none --nodes=1 --ntasks=1 -w "$PULSAR_NODE" bash -lc "
    set -euo pipefail
    ${HPC_NODE_ENV_SNIPPET}
    export PULSAR_MEM='${PULSAR_MEM}'
    export PULSAR_LOG_DIR='${LOG_DIR}'
    export PULSAR_LOG_FILE='standalone-service.log'
    rm -rf '${DATA_ROOT}'
    mkdir -p '${DATA_ROOT}/bookkeeper' '${DATA_ROOT}/metadata' '${DATA_ROOT}/tmp'
    export TMPDIR='${DATA_ROOT}/tmp'
    echo \$\$ > '${PID_FILE}'
    exec '${PULSAR_HOME}/bin/pulsar' standalone \
        --config '${CONFIG_FILE}' \
        --advertised-address '${PULSAR_ADDRESS}' \
        --bookkeeper-dir '${DATA_ROOT}/bookkeeper' \
        --metadata-dir '${DATA_ROOT}/metadata' \
        --num-bookies 1 \
        --no-functions-worker \
        --no-stream-storage \
        --wipe-data
" >"$LOG_FILE" 2>&1 &
echo "$!" > "$SRUN_PID_FILE"

printf 'pulsar://%s:6650\n' "$PULSAR_ADDRESS" > "$CASE_DIR/runtime/bootstrap_servers.txt"
printf 'http://%s:8080\n' "$PULSAR_ADDRESS" > "$CASE_DIR/runtime/pulsar_admin_url.txt"
printf '%s:8080\n' "$PULSAR_ADDRESS" > "$CASE_DIR/runtime/pulsar_metrics_endpoint.txt"
printf '%s\n' "$PULSAR_NODE" > "$RUNTIME_DIR/service_nodes.txt"
printf '%s\n' "$PULSAR_ADDRESS" > "$RUNTIME_DIR/service_addresses.txt"
write_node_service_address_map "$CASE_DIR/runtime/node_service_addresses.tsv" "${ALL_NODES[@]}"
log_info "Pulsar standalone start requested on $PULSAR_NODE ($PULSAR_ADDRESS)"
