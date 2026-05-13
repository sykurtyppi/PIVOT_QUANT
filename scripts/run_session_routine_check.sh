#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}"

# Resolve >=3.10 Python via shared helper; log + exit cleanly on failure.
if ! source "${ROOT_DIR}/scripts/_pybin.sh" 2>>"${LOG_DIR}/session_routine.log"; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR session_routine_check: no python3.10+ found" >> "${LOG_DIR}/session_routine.log"
  exit 1
fi

exec "${PYTHON_BIN}" "${ROOT_DIR}/scripts/session_routine_check.py" "$@"
