#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}"

if ! source "${ROOT_DIR}/scripts/_pybin.sh" 2>>"${LOG_DIR}/health_alert.log"; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR health_alert_watchdog: no python3.10+ found" >> "${LOG_DIR}/health_alert.log"
  exit 1
fi

exec "${PYTHON_BIN}" "${ROOT_DIR}/scripts/health_alert_watchdog.py" "$@"

