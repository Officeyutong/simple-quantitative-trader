#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${1:-${PROJECT_ROOT}/config/paper.toml}"
BINARY_PATH="${QUANT_BINARY:-${PROJECT_ROOT}/target/release/simple-quantitative-trader}"
RUNTIME_DIR="${QUANT_RUNTIME_DIR:-${PROJECT_ROOT}/run}"
STOP_FILE="${RUNTIME_DIR}/screen.stop"

mkdir -p "${RUNTIME_DIR}"
rm -f "${STOP_FILE}"

while [[ ! -e "${STOP_FILE}" ]]; do
    echo "$(date -u +%FT%TZ) starting quantitative trading daemon"
    set +e
    "${BINARY_PATH}" --config "${CONFIG_PATH}" daemon
    EXIT_CODE=$?
    set -e
    echo "$(date -u +%FT%TZ) daemon exited with code ${EXIT_CODE}"
    [[ -e "${STOP_FILE}" ]] && break
    sleep 5
done

echo "$(date -u +%FT%TZ) screen runner stopped"
