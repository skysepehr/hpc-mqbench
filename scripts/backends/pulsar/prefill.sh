#!/usr/bin/env bash
set -euo pipefail

if [[ "${SCENARIO:-simultaneous}" != "simultaneous" ]]; then
    printf '[pulsar-prefill] ERROR: the initial Pulsar adapter supports simultaneous cases only\n' >&2
    exit 1
fi
printf '[pulsar-prefill] No prefill is required for a simultaneous case\n'
