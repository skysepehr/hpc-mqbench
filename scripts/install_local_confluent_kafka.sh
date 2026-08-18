#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Install confluent-kafka-python from the vendored source archive.
#
# The archive contains source code and a native C extension. It cannot be used by
# simply adding the zip file to PYTHONPATH; it must be built against librdkafka.
# This helper installs into .local/python so the benchmark can import
# confluent_kafka without a global Python package installation.
# -----------------------------------------------------------------------------

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

ARCHIVE="${LOCAL_CONFLUENT_KAFKA_ARCHIVE:-$PROJECT_ROOT/tools/confluent-kafka-python-master.zip}"
SOURCE_DIR="${LOCAL_CONFLUENT_KAFKA_SOURCE_DIR:-$PROJECT_ROOT/.local/confluent-kafka-python-src}"
TARGET_DIR="${LOCAL_PYTHON_DEPS_DIR:-$PROJECT_ROOT/.local/python}"
BUILD_LOG="${LOCAL_CONFLUENT_KAFKA_BUILD_LOG:-$PROJECT_ROOT/.local/confluent-kafka-build.log}"
LOCAL_LIBRDKAFKA_ENV="${LOCAL_LIBRDKAFKA_ENV:-$PROJECT_ROOT/.local/librdkafka/env.sh}"
LOCAL_LIBRDKAFKA_INSTALLER="$PROJECT_ROOT/scripts/install_local_librdkafka.sh"

# shellcheck source=./python_pip_common.sh
source "$PROJECT_ROOT/scripts/python_pip_common.sh"

log() {
    printf '[install-local-confluent-kafka] %s\n' "$*"
}

die() {
    printf '[install-local-confluent-kafka] ERROR: %s\n' "$*" >&2
    exit 1
}

require_command() {
    local command_name="${1:?command name required}"
    command -v "$command_name" >/dev/null 2>&1 || die "Required command not found: $command_name"
}

require_file() {
    local file_path="${1:?file path required}"
    [[ -f "$file_path" ]] || die "Required file not found: $file_path"
}

check_import_from_target() {
    PYTHONPATH="$TARGET_DIR:${PYTHONPATH:-}" \
    LD_LIBRARY_PATH="$PROJECT_ROOT/.local/librdkafka/lib:${LD_LIBRARY_PATH:-}" \
        python3 - <<'PY' >/dev/null 2>&1
import confluent_kafka
print(confluent_kafka.version())
PY
}

load_local_librdkafka_env() {
    if [[ -f "$LOCAL_LIBRDKAFKA_ENV" ]]; then
        # shellcheck source=/dev/null
        source "$LOCAL_LIBRDKAFKA_ENV"
        export PKG_CONFIG_PATH LD_LIBRARY_PATH CFLAGS LDFLAGS
    fi
}

ensure_librdkafka_available() {
    load_local_librdkafka_env

    if command -v pkg-config >/dev/null 2>&1 && pkg-config --exists rdkafka; then
        log "librdkafka visible through pkg-config: $(pkg-config --modversion rdkafka)"
        return
    fi

    if [[ -x "$LOCAL_LIBRDKAFKA_INSTALLER" ]]; then
        log "librdkafka not found; building repo-local librdkafka first"
        "$LOCAL_LIBRDKAFKA_INSTALLER"
        load_local_librdkafka_env
    fi

    if command -v pkg-config >/dev/null 2>&1 && pkg-config --exists rdkafka; then
        log "repo-local librdkafka ready: $(pkg-config --modversion rdkafka)"
        return
    fi

    log "WARNING: librdkafka is still not visible through pkg-config. Build may fail."
}

extract_source_archive() {
    if [[ -f "$SOURCE_DIR/pyproject.toml" && -d "$SOURCE_DIR/src/confluent_kafka" ]]; then
        log "Source already extracted: $SOURCE_DIR"
        return
    fi

    local tmp_dir
    tmp_dir="$PROJECT_ROOT/.local/confluent-kafka-extract.$$"
    rm -rf "$tmp_dir"
    mkdir -p "$tmp_dir"

    log "Extracting source archive: $ARCHIVE"
    python3 -m zipfile -e "$ARCHIVE" "$tmp_dir"

    local extracted_dir
    extracted_dir="$(find "$tmp_dir" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
    [[ -n "$extracted_dir" ]] || die "Archive did not contain a top-level source directory"

    rm -rf "$SOURCE_DIR"
    mkdir -p "$(dirname "$SOURCE_DIR")"
    mv "$extracted_dir" "$SOURCE_DIR"
    rm -rf "$tmp_dir"
}

print_librdkafka_hint() {
    cat >&2 <<'EOF'
[install-local-confluent-kafka] Build failed.
[install-local-confluent-kafka]
[install-local-confluent-kafka] The vendored confluent-kafka-python archive is source code.
[install-local-confluent-kafka] Building it requires librdkafka development headers, Python headers, and a C compiler.
[install-local-confluent-kafka]
[install-local-confluent-kafka] This repo can build librdkafka locally from:
[install-local-confluent-kafka]   tools/archives/librdkafka-*.tar.gz
[install-local-confluent-kafka]
[install-local-confluent-kafka] Python headers must match your Python interpreter. On Ubuntu/Debian, that
[install-local-confluent-kafka] package is usually python3-dev.
[install-local-confluent-kafka]
[install-local-confluent-kafka] This helper does not use sudo, does not download anything,
[install-local-confluent-kafka] and does not install global packages.
[install-local-confluent-kafka]
[install-local-confluent-kafka] If tools/archives/librdkafka-*.tar.gz exists, run:
[install-local-confluent-kafka]   ./scripts/install_local_librdkafka.sh
EOF
}

require_command python3
require_command gcc
require_file "$ARCHIVE"
ensure_python_pip || die "pip is required to build the local confluent-kafka package"

if ! command -v pkg-config >/dev/null 2>&1; then
    log "WARNING: pkg-config is not installed; cannot pre-check librdkafka headers."
fi

mkdir -p "$TARGET_DIR" "$(dirname "$BUILD_LOG")"
ensure_librdkafka_available

if check_import_from_target; then
    log "confluent_kafka already imports from local target: $TARGET_DIR"
    PYTHONPATH="$TARGET_DIR:${PYTHONPATH:-}" \
    LD_LIBRARY_PATH="$PROJECT_ROOT/.local/librdkafka/lib:${LD_LIBRARY_PATH:-}" \
        python3 - <<'PY'
import confluent_kafka
print(f"[install-local-confluent-kafka] version: {confluent_kafka.version()}")
print(f"[install-local-confluent-kafka] module: {confluent_kafka.__file__}")
PY
    exit 0
fi

extract_source_archive

log "Installing into: $TARGET_DIR"
log "Build log: $BUILD_LOG"

if ! python_pip install \
    --no-index \
    --no-build-isolation \
    --no-deps \
    --upgrade \
    --force-reinstall \
    --target "$TARGET_DIR" \
    "$SOURCE_DIR" \
    >"$BUILD_LOG" 2>&1; then
    tail -n 80 "$BUILD_LOG" >&2 || true
    print_librdkafka_hint
    exit 1
fi

if ! check_import_from_target; then
    tail -n 80 "$BUILD_LOG" >&2 || true
    die "confluent_kafka build finished, but import validation failed"
fi

PYTHONPATH="$TARGET_DIR:${PYTHONPATH:-}" \
LD_LIBRARY_PATH="$PROJECT_ROOT/.local/librdkafka/lib:${LD_LIBRARY_PATH:-}" \
    python3 - <<'PY'
import confluent_kafka
print(f"[install-local-confluent-kafka] version: {confluent_kafka.version()}")
print(f"[install-local-confluent-kafka] module: {confluent_kafka.__file__}")
PY

log "Local Python dependency path:"
log "  $TARGET_DIR"
log "Benchmark modules and local wrappers add this path automatically for local runs."
