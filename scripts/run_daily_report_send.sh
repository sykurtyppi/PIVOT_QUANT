#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}" "${LOG_DIR}/reports"

timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

if [ -x "${ROOT_DIR}/.venv/bin/python" ]; then
  PYTHON="${ROOT_DIR}/.venv/bin/python"
else
  PYTHON="python3"
fi

PIVOT_DB_PATH="${PIVOT_DB:-${ROOT_DIR}/data/pivot_events.sqlite}"
REPORT_OUTPUT=""
REPORT_PATH=""

echo "[$(timestamp)] START daily_report_send" >> "${LOG_DIR}/report_delivery.log"

if REPORT_OUTPUT="$("${PYTHON}" "${ROOT_DIR}/scripts/generate_daily_ml_report.py" --db "${PIVOT_DB_PATH}" --out-dir "${LOG_DIR}/reports" 2>&1)"; then
  printf '%s\n' "${REPORT_OUTPUT}" >> "${LOG_DIR}/report_delivery.log"
  REPORT_PATH="$(printf '%s\n' "${REPORT_OUTPUT}" | tail -n 1)"
else
  printf '%s\n' "${REPORT_OUTPUT}" >> "${LOG_DIR}/report_delivery.log"
  echo "[$(timestamp)] ERROR daily report generation failed" >> "${LOG_DIR}/report_delivery.log"
  exit 1
fi

if [[ -z "${REPORT_PATH}" || ! -f "${REPORT_PATH}" ]]; then
  echo "[$(timestamp)] ERROR report path missing after generation" >> "${LOG_DIR}/report_delivery.log"
  exit 1
fi

if "${PYTHON}" "${ROOT_DIR}/scripts/send_daily_report.py" --report "${REPORT_PATH}" >> "${LOG_DIR}/report_delivery.log" 2>&1; then
  echo "[$(timestamp)] DONE  daily_report_send" >> "${LOG_DIR}/report_delivery.log"
else
  echo "[$(timestamp)] ERROR notification send failed" >> "${LOG_DIR}/report_delivery.log"
  exit 1
fi
