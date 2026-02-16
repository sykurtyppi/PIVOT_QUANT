#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}"

timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

run_step() {
  local name="$1"
  shift
  echo "[$(timestamp)] START ${name}" | tee -a "${LOG_DIR}/retrain.log"
  "$@" >> "${LOG_DIR}/retrain.log" 2>&1
  echo "[$(timestamp)] DONE  ${name}" | tee -a "${LOG_DIR}/retrain.log"
}

cd "${ROOT_DIR}"

# Prefer the project venv to avoid picking up a wrong system Python.
if [ -x "${ROOT_DIR}/.venv/bin/python" ]; then
  PYTHON="${ROOT_DIR}/.venv/bin/python"
else
  PYTHON="python3"
fi

# Backfill recent days to capture any gaps (dashboard outages, weekends).
# Uses 5m bars from Yahoo (supports longer ranges than 1m's 7-day limit).
run_step "backfill" "${PYTHON}" scripts/backfill_events.py --range 7d --interval 5m --source yahoo

run_step "build_labels"    "${PYTHON}" scripts/build_labels.py --horizons 5 15 60 --incremental
run_step "export_parquet"  "${PYTHON}" scripts/export_parquet.py
run_step "duckdb_view"     "${PYTHON}" scripts/build_duckdb_view.py
run_step "train_artifacts" "${PYTHON}" scripts/train_rf_artifacts.py

# Tell the running ML server to hot-reload the new model artifacts.
echo "[$(timestamp)] Reloading ML server models..." | tee -a "${LOG_DIR}/retrain.log"
curl -sf -X POST http://127.0.0.1:5003/reload >> "${LOG_DIR}/retrain.log" 2>&1 || {
  echo "[$(timestamp)] WARN: ML server reload failed (server may need manual restart)" | tee -a "${LOG_DIR}/retrain.log"
}

echo "[$(timestamp)] Retrain cycle complete." | tee -a "${LOG_DIR}/retrain.log"
