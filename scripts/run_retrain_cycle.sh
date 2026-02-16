#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}"
LOCK_DIR="${LOG_DIR}/run_retrain_cycle.lock"
LOCK_OWNED=0

timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

acquire_lock() {
  local existing_pid=""
  if mkdir "${LOCK_DIR}" 2>/dev/null; then
    printf '%s\n' "$$" > "${LOCK_DIR}/pid"
    LOCK_OWNED=1
    return 0
  fi

  if [[ -f "${LOCK_DIR}/pid" ]]; then
    existing_pid="$(cat "${LOCK_DIR}/pid" 2>/dev/null || true)"
  fi

  if [[ -n "${existing_pid}" ]] && kill -0 "${existing_pid}" >/dev/null 2>&1; then
    echo "[$(timestamp)] SKIP retrain: already running (pid ${existing_pid})." | tee -a "${LOG_DIR}/retrain.log"
    exit 0
  fi

  rm -rf "${LOCK_DIR}" 2>/dev/null || true
  if mkdir "${LOCK_DIR}" 2>/dev/null; then
    printf '%s\n' "$$" > "${LOCK_DIR}/pid"
    LOCK_OWNED=1
    return 0
  fi

  echo "[$(timestamp)] ERROR retrain: unable to acquire lock at ${LOCK_DIR}" | tee -a "${LOG_DIR}/retrain.log"
  exit 1
}

release_lock() {
  if [[ "${LOCK_OWNED}" -ne 1 ]]; then
    return 0
  fi
  rm -rf "${LOCK_DIR}" 2>/dev/null || true
  LOCK_OWNED=0
}

run_step() {
  local name="$1"
  shift
  echo "[$(timestamp)] START ${name}" | tee -a "${LOG_DIR}/retrain.log"
  "$@" >> "${LOG_DIR}/retrain.log" 2>&1
  echo "[$(timestamp)] DONE  ${name}" | tee -a "${LOG_DIR}/retrain.log"
}

trap release_lock EXIT INT TERM
acquire_lock

cd "${ROOT_DIR}"

# Prefer the project venv to avoid picking up a wrong system Python.
if [ -x "${ROOT_DIR}/.venv/bin/python" ]; then
  PYTHON="${ROOT_DIR}/.venv/bin/python"
else
  PYTHON="python3"
fi
RETRAIN_SYMBOLS="${RETRAIN_SYMBOLS:-SPY}"
PIVOT_DB_PATH="${PIVOT_DB:-${ROOT_DIR}/data/pivot_events.sqlite}"
REPORT_PATH=""
REPORT_OUTPUT=""
NOTIFY_ON_RETRAIN="${ML_REPORT_NOTIFY_ON_RETRAIN:-false}"

# Backfill recent days to capture any gaps (dashboard outages, weekends).
# Uses 5m bars from Yahoo (supports longer ranges than 1m's 7-day limit).
run_step "backfill" "${PYTHON}" scripts/backfill_events.py --symbols "${RETRAIN_SYMBOLS}" --range 7d --interval 5m --source yahoo

run_step "build_labels"    "${PYTHON}" scripts/build_labels.py --horizons 5 15 60 --incremental
run_step "export_parquet"  "${PYTHON}" scripts/export_parquet.py
run_step "duckdb_view"     "${PYTHON}" scripts/build_duckdb_view.py
run_step "train_artifacts" "${PYTHON}" scripts/train_rf_artifacts.py

# Tell the running ML server to hot-reload the new model artifacts.
echo "[$(timestamp)] Reloading ML server models..." | tee -a "${LOG_DIR}/retrain.log"
curl -sf -X POST http://127.0.0.1:5003/reload >> "${LOG_DIR}/retrain.log" 2>&1 || {
  echo "[$(timestamp)] WARN: ML server reload failed (server may need manual restart)" | tee -a "${LOG_DIR}/retrain.log"
}

echo "[$(timestamp)] Generating daily ML report..." | tee -a "${LOG_DIR}/retrain.log"
if REPORT_OUTPUT="$("${PYTHON}" scripts/generate_daily_ml_report.py --db "${PIVOT_DB_PATH}" --out-dir "${LOG_DIR}/reports" 2>&1)"; then
  printf '%s\n' "${REPORT_OUTPUT}" >> "${LOG_DIR}/retrain.log"
  REPORT_PATH="$(printf '%s\n' "${REPORT_OUTPUT}" | tail -n 1)"
  echo "[$(timestamp)] DONE  daily_report" | tee -a "${LOG_DIR}/retrain.log"
else
  printf '%s\n' "${REPORT_OUTPUT}" >> "${LOG_DIR}/retrain.log"
  echo "[$(timestamp)] WARN: daily report generation failed" | tee -a "${LOG_DIR}/retrain.log"
fi

if [[ -n "${REPORT_PATH}" ]] && [[ -f "${REPORT_PATH}" ]] && [[ "${NOTIFY_ON_RETRAIN,,}" =~ ^(1|true|yes|y|on)$ ]]; then
  echo "[$(timestamp)] Sending daily report notification..." | tee -a "${LOG_DIR}/retrain.log"
  if "${PYTHON}" scripts/send_daily_report.py --report "${REPORT_PATH}" >> "${LOG_DIR}/retrain.log" 2>&1; then
    echo "[$(timestamp)] DONE  notify_daily_report" | tee -a "${LOG_DIR}/retrain.log"
  else
    echo "[$(timestamp)] WARN: daily report notification failed" | tee -a "${LOG_DIR}/retrain.log"
  fi
elif [[ -n "${REPORT_PATH}" ]] && [[ -f "${REPORT_PATH}" ]]; then
  echo "[$(timestamp)] INFO: retrain notification disabled (ML_REPORT_NOTIFY_ON_RETRAIN=false)" | tee -a "${LOG_DIR}/retrain.log"
else
  echo "[$(timestamp)] WARN: skipped notification (report path missing)" | tee -a "${LOG_DIR}/retrain.log"
fi

echo "[$(timestamp)] Retrain cycle complete." | tee -a "${LOG_DIR}/retrain.log"
