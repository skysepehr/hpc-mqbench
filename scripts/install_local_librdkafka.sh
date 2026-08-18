#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Build librdkafka locally from the vendored source archive.
#
# This script does not use sudo and does not install system packages. It builds
# librdkafka into .local/librdkafka so the vendored confluent-kafka-python source
# can compile without a globally installed librdkafka-dev package.
# -----------------------------------------------------------------------------

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

LIBRDKAFKA_VERSION="${LIBRDKAFKA_VERSION:-2.14.1}"
ARCHIVE="${LOCAL_LIBRDKAFKA_ARCHIVE:-$PROJECT_ROOT/tools/archives/librdkafka-${LIBRDKAFKA_VERSION}.tar.gz}"
EXPECTED_SHA256="${LOCAL_LIBRDKAFKA_SHA256:-}"
SOURCE_PARENT="${LOCAL_LIBRDKAFKA_SOURCE_PARENT:-$PROJECT_ROOT/.local/librdkafka-src}"
SOURCE_DIR="${LOCAL_LIBRDKAFKA_SOURCE_DIR:-$SOURCE_PARENT/librdkafka-${LIBRDKAFKA_VERSION}}"
PREFIX="${LOCAL_LIBRDKAFKA_PREFIX:-$PROJECT_ROOT/.local/librdkafka}"
BUILD_LOG="${LOCAL_LIBRDKAFKA_BUILD_LOG:-$PROJECT_ROOT/.local/librdkafka-build.log}"
ENV_FILE="$PREFIX/env.sh"

# Keep the default build dependency-light for local PLAINTEXT benchmarks. External
# compression/security features can be re-enabled by overriding this variable.
CONFIGURE_FLAGS="${LOCAL_LIBRDKAFKA_CONFIGURE_FLAGS:---disable-ssl --disable-gssapi --disable-curl --disable-zstd --disable-zlib --disable-lz4-ext}"

if [[ -z "$EXPECTED_SHA256" \
    && "$LIBRDKAFKA_VERSION" == "2.14.1" \
    && "$ARCHIVE" == "$PROJECT_ROOT/tools/archives/librdkafka-2.14.1.tar.gz" ]]; then
    EXPECTED_SHA256="bb246e754dee3560e9b42bf4e844dc05de4b146a3cae937e36301ffacdc456e7"
fi

log() {
    printf '[install-local-librdkafka] %s\n' "$*"
}

die() {
    printf '[install-local-librdkafka] ERROR: %s\n' "$*" >&2
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

verify_archive() {
    if [[ -z "$EXPECTED_SHA256" ]]; then
        log "Archive checksum validation skipped by LOCAL_LIBRDKAFKA_SHA256="
        return
    fi

    if ! command -v sha256sum >/dev/null 2>&1; then
        log "WARNING: sha256sum not found; cannot validate archive checksum"
        return
    fi

    local actual_sha256
    actual_sha256="$(sha256sum "$ARCHIVE" | awk '{print $1}')"
    if [[ "$actual_sha256" != "$EXPECTED_SHA256" ]]; then
        die "Archive checksum mismatch for $ARCHIVE"
    fi
}

cpu_count() {
    if command -v nproc >/dev/null 2>&1; then
        nproc
    else
        printf '2\n'
    fi
}

write_env_file() {
    mkdir -p "$PREFIX"
    cat > "$ENV_FILE" <<EOF
# Source this file to build or run Python clients against repo-local librdkafka.
export LOCAL_LIBRDKAFKA_HOME="$PREFIX"
export PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig:\${PKG_CONFIG_PATH:-}"
export LD_LIBRARY_PATH="$PREFIX/lib:\${LD_LIBRARY_PATH:-}"
export CFLAGS="-I$PREFIX/include \${CFLAGS:-}"
export LDFLAGS="-L$PREFIX/lib -Wl,-rpath,$PREFIX/lib \${LDFLAGS:-}"
EOF
}

validate_install() {
    require_file "$PREFIX/include/librdkafka/rdkafka.h"
    require_file "$PREFIX/lib/pkgconfig/rdkafka.pc"

    if [[ ! -f "$PREFIX/lib/librdkafka.so" && ! -f "$PREFIX/lib/librdkafka.a" ]]; then
        die "Installed librdkafka library not found under $PREFIX/lib"
    fi

    # shellcheck source=/dev/null
    source "$ENV_FILE"
    if command -v pkg-config >/dev/null 2>&1; then
        pkg-config --modversion rdkafka >/dev/null || die "pkg-config cannot see repo-local librdkafka"
    fi
}

extract_source() {
    if [[ -x "$SOURCE_DIR/configure" && -d "$SOURCE_DIR/src" ]]; then
        log "Source already extracted: $SOURCE_DIR"
        return
    fi

    rm -rf "$SOURCE_DIR"
    mkdir -p "$SOURCE_PARENT"
    log "Extracting source archive: $ARCHIVE"
    tar -xzf "$ARCHIVE" -C "$SOURCE_PARENT"
}

require_command tar
require_command gzip
require_command gcc
require_command g++
require_command make
require_file "$ARCHIVE"
verify_archive

write_env_file

if [[ -f "$PREFIX/include/librdkafka/rdkafka.h" && -f "$PREFIX/lib/pkgconfig/rdkafka.pc" ]]; then
    log "Repo-local librdkafka already appears installed: $PREFIX"
    validate_install
    log "Version: $(PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig:${PKG_CONFIG_PATH:-}" pkg-config --modversion rdkafka 2>/dev/null || printf 'unknown')"
    log "Environment file: $ENV_FILE"
    exit 0
fi

extract_source
mkdir -p "$(dirname "$BUILD_LOG")"

log "Building librdkafka $LIBRDKAFKA_VERSION"
log "Install prefix: $PREFIX"
log "Configure flags: $CONFIGURE_FLAGS"
log "Build log: $BUILD_LOG"

(
    cd "$SOURCE_DIR"
    ./configure \
        --prefix="$PREFIX" \
        --libdir="$PREFIX/lib" \
        $CONFIGURE_FLAGS
    make -j"$(cpu_count)"
    make install
) >"$BUILD_LOG" 2>&1 || {
    tail -n 120 "$BUILD_LOG" >&2 || true
    die "librdkafka build failed. See: $BUILD_LOG"
}

validate_install

log "Repo-local librdkafka installed at: $PREFIX"
log "Version: $(PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig:${PKG_CONFIG_PATH:-}" pkg-config --modversion rdkafka 2>/dev/null || printf 'unknown')"
log "Environment file: $ENV_FILE"
log "Suggested export:"
printf 'source "$PWD/.local/librdkafka/env.sh"\n'
