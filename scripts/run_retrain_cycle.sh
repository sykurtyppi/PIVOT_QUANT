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

run_step "build_labels" npm run ml:build-labels
run_step "export_parquet" npm run ml:export-parquet
run_step "duckdb_view" npm run ml:duckdb-view
run_step "train_artifacts" npm run ml:train-artifacts

# Tell the running ML server to hot-reload the new model artifacts.
echo "[$(timestamp)] Reloading ML server models..." | tee -a "${LOG_DIR}/retrain.log"
curl -sf -X POST http://127.0.0.1:5003/reload >> "${LOG_DIR}/retrain.log" 2>&1 || {
  echo "[$(timestamp)] WARN: ML server reload failed (server may need manual restart)" | tee -a "${LOG_DIR}/retrain.log"
}

echo "[$(timestamp)] Retrain cycle complete." | tee -a "${LOG_DIR}/retrain.log"
