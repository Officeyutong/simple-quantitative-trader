#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${1:-${PROJECT_ROOT}/config/paper.toml}"
SESSION_NAME="${QUANT_SCREEN_SESSION:-quant-trader}"
LOG_DIR="${QUANT_LOG_DIR:-${PROJECT_ROOT}/logs}"
RUNTIME_DIR="${QUANT_RUNTIME_DIR:-${PROJECT_ROOT}/run}"
BINARY_PATH="${QUANT_BINARY:-${PROJECT_ROOT}/target/release/simple-quantitative-trader}"

command -v screen >/dev/null || {
    echo "GNU screen is required" >&2
    exit 1
}
[[ -x "${BINARY_PATH}" ]] || {
    echo "release binary not found: ${BINARY_PATH}; run cargo build --release" >&2
    exit 1
}
[[ -r "${CONFIG_PATH}" ]] || {
    echo "configuration not found: ${CONFIG_PATH}" >&2
    exit 1
}
if screen -list | grep -q "[.]${SESSION_NAME}[[:space:]]"; then
    echo "screen session already exists: ${SESSION_NAME}" >&2
    exit 1
fi

mkdir -p "${LOG_DIR}" "${RUNTIME_DIR}"
rm -f "${RUNTIME_DIR}/screen.stop"
LOG_FILE="${LOG_DIR}/quant-$(date -u +%Y%m%dT%H%M%SZ).log"

QUANT_BINARY="${BINARY_PATH}" \
QUANT_RUNTIME_DIR="${RUNTIME_DIR}" \
screen -L -Logfile "${LOG_FILE}" -dmS "${SESSION_NAME}" \
    "${PROJECT_ROOT}/deploy/screen-run.sh" "${CONFIG_PATH}"

echo "started screen session ${SESSION_NAME}"
echo "log: ${LOG_FILE}"
echo "attach: screen -r ${SESSION_NAME}"
