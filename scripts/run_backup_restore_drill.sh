#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}"

timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

if ! source "${ROOT_DIR}/scripts/_pybin.sh" 2>>"${LOG_DIR}/restore_drill.log"; then
  echo "[$(timestamp)] ERROR restore_drill: no python3.10+ found" >> "${LOG_DIR}/restore_drill.log"
  exit 1
fi

exec "${PYTHON_BIN}" "${ROOT_DIR}/scripts/backup_restore_drill.py" "$@"
