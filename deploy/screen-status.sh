#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${1:-${PROJECT_ROOT}/config/paper.toml}"
SESSION_NAME="${QUANT_SCREEN_SESSION:-quant-trader}"
BINARY_PATH="${QUANT_BINARY:-${PROJECT_ROOT}/target/release/simple-quantitative-trader}"

if screen -list | grep -q "[.]${SESSION_NAME}[[:space:]]"; then
    echo "screen: running (${SESSION_NAME})"
else
    echo "screen: stopped (${SESSION_NAME})"
fi

"${BINARY_PATH}" --config "${CONFIG_PATH}" status
"${BINARY_PATH}" --config "${CONFIG_PATH}" monitor metrics
