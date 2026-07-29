#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${1:-${PROJECT_ROOT}/config/paper.toml}"
SESSION_NAME="${QUANT_SCREEN_SESSION:-quant-trader}"
RUNTIME_DIR="${QUANT_RUNTIME_DIR:-${PROJECT_ROOT}/run}"
BINARY_PATH="${QUANT_BINARY:-${PROJECT_ROOT}/target/release/simple-quantitative-trader}"

mkdir -p "${RUNTIME_DIR}"
touch "${RUNTIME_DIR}/screen.stop"
"${BINARY_PATH}" --config "${CONFIG_PATH}" shutdown >/dev/null 2>&1 || true

for _ in $(seq 1 30); do
    if ! screen -list | grep -q "[.]${SESSION_NAME}[[:space:]]"; then
        echo "stopped screen session ${SESSION_NAME}"
        exit 0
    fi
    sleep 1
done

screen -S "${SESSION_NAME}" -X quit
echo "forced screen session ${SESSION_NAME} to stop after graceful timeout"
