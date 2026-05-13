#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}"

timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

if ! source "${ROOT_DIR}/scripts/_pybin.sh" 2>>"${LOG_DIR}/host_health.log"; then
  echo "[$(timestamp)] ERROR host_health: no python3.10+ found" >> "${LOG_DIR}/host_health.log"
  exit 1
fi

exec "${PYTHON_BIN}" "${ROOT_DIR}/scripts/host_health_check.py" "$@"
