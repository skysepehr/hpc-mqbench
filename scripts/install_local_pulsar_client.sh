#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${LOCAL_PYTHON_DEPS_DIR:-$PROJECT_ROOT/.local/python}"
VERSION="${PULSAR_CLIENT_VERSION:-3.13.0}"
CERTIFI_VERSION="${CERTIFI_VERSION:-2026.7.22}"

# shellcheck source=./python_pip_common.sh
source "$PROJECT_ROOT/scripts/python_pip_common.sh"

die() {
    printf '[install-local-pulsar-client] ERROR: %s\n' "$*" >&2
    exit 1
}

python_tag="$(python3 -c 'import sys; print(f"cp{sys.version_info.major}{sys.version_info.minor}")')"
case "$python_tag" in
    cp311)
        wheel="pulsar_client-${VERSION}-cp311-cp311-manylinux_2_28_x86_64.whl"
        expected_sha256="f958a6149b28cfe354d33f2af229e2257d0bc4e1f195eefbd4cbae5d7fd7961d"
        ;;
    cp312)
        wheel="pulsar_client-${VERSION}-cp312-cp312-manylinux_2_28_x86_64.whl"
        expected_sha256="863fd7b8cdc162f3cc0ea7217c37604c66a7bc9bb5cd554566c5d07f89a50e5e"
        ;;
    *)
        die "no vendored pulsar-client wheel for Python tag $python_tag; use Python 3.11 or 3.12"
        ;;
esac

wheel_path="$PROJECT_ROOT/tools/archives/$wheel"
[[ -f "$wheel_path" ]] || die "wheel not found: $wheel_path"
actual_sha256="$(sha256sum "$wheel_path" | awk '{print $1}')"
[[ "$actual_sha256" == "$expected_sha256" ]] || die "wheel SHA-256 mismatch"
certifi_wheel="certifi-${CERTIFI_VERSION}-py3-none-any.whl"
certifi_wheel_path="$PROJECT_ROOT/tools/archives/$certifi_wheel"
certifi_expected_sha256="62f22742b58a1a33014a2b6b706588a8d7e2a88ae7bd1a6ebe8c992928483775"
[[ -f "$certifi_wheel_path" ]] || die "wheel not found: $certifi_wheel_path"
certifi_actual_sha256="$(sha256sum "$certifi_wheel_path" | awk '{print $1}')"
[[ "$certifi_actual_sha256" == "$certifi_expected_sha256" ]] || \
    die "certifi wheel SHA-256 mismatch"
ensure_python_pip || die "pip is required for local wheel installation"
mkdir -p "$TARGET_DIR"
python_pip install \
    --no-index \
    --no-deps \
    --upgrade \
    --target "$TARGET_DIR" \
    "$certifi_wheel_path" \
    "$wheel_path"

PYTHONPATH="$TARGET_DIR:${PYTHONPATH:-}" python3 - <<'PY'
from pathlib import Path

import certifi
import pulsar
assert Path(certifi.where()).is_file()
print(f"[install-local-pulsar-client] module: {pulsar.__file__}")
print(f"[install-local-pulsar-client] CA bundle: {certifi.where()}")
PY
