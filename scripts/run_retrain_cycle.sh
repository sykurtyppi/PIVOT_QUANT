#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}"
LOCK_DIR="${LOG_DIR}/run_retrain_cycle.lock"
LOCK_OWNED=0
ENV_FILE="${ROOT_DIR}/.env"

load_env_file() {
  local env_path="$1"
  local raw line key value
  [[ -f "${env_path}" ]] || return 0

  while IFS= read -r raw || [[ -n "${raw}" ]]; do
    line="${raw#"${raw%%[![:space:]]*}"}"
    [[ -z "${line}" ]] && continue
    [[ "${line:0:1}" == "#" ]] && continue
    [[ "${line}" == "export "* ]] && line="${line#export }"
    [[ "${line}" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue

    key="${line%%=*}"
    value="${line#*=}"
    export "${key}=${value}"
  done < "${env_path}"
}

# Load .env in a strict, non-executing way so malformed/comment prose
# lines never crash retrain runs under launchd.
load_env_file "${ENV_FILE}"

timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

now_ms() {
  echo $(( $(date +%s) * 1000 ))
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

run_ops_smoke() {
  local attempt
  echo "[$(timestamp)] INFO ops_smoke running direct unittest runner" | tee -a "${LOG_DIR}/retrain.log"
  for attempt in 1 2; do
    if "${PYTHON}" -m unittest discover -s tests/python -p "test_*.py" -v >> "${LOG_DIR}/retrain.log" 2>&1; then
      if (( attempt > 1 )); then
        echo "[$(timestamp)] WARN ops_smoke passed on retry ${attempt}" | tee -a "${LOG_DIR}/retrain.log"
      fi
      return 0
    fi
    if (( attempt < 2 )); then
      echo "[$(timestamp)] WARN ops_smoke direct unittest failed; retrying once" | tee -a "${LOG_DIR}/retrain.log"
      sleep 2
    fi
  done
  return 1
}

is_truthy() {
  local raw="${1:-}"
  local lowered
  lowered="$(printf '%s' "${raw}" | tr '[:upper:]' '[:lower:]')"
  case "${lowered}" in
    1|true|yes|y|on) return 0 ;;
    *) return 1 ;;
  esac
}

PYTHON=""
RETRAIN_SYMBOLS="${RETRAIN_SYMBOLS:-SPY}"
PIVOT_DB_PATH="${PIVOT_DB:-${ROOT_DIR}/data/pivot_events.sqlite}"
MODEL_DIR="${RF_MODEL_DIR:-data/models}"
REPORT_PATH=""
REPORT_OUTPUT=""
NOTIFY_ON_RETRAIN="${ML_REPORT_NOTIFY_ON_RETRAIN:-false}"
RUN_OPS_SMOKE_ON_RETRAIN="${ML_RUN_OPS_SMOKE_ON_RETRAIN:-true}"
RELOAD_STATUS="not_attempted"

ops_set() {
  if [[ -z "${PYTHON}" ]]; then
    return 0
  fi
  "${PYTHON}" scripts/ops_status.py --db "${PIVOT_DB_PATH}" "$@" >> "${LOG_DIR}/retrain.log" 2>&1 || true
}

mark_failure() {
  local exit_code="$?"
  local failed_cmd="${BASH_COMMAND:-unknown}"
  local ts_ms
  local reload_for_failure
  ts_ms="$(now_ms)"
  reload_for_failure="${RELOAD_STATUS}"
  if [[ "${reload_for_failure}" == "unknown" || "${reload_for_failure}" == "not_attempted" ]]; then
    reload_for_failure="skipped_due_to_failure"
  fi
  echo "[$(timestamp)] ERROR retrain: command failed (${failed_cmd}) exit=${exit_code}" | tee -a "${LOG_DIR}/retrain.log"
  ops_set \
    --set "retrain_state=idle" \
    --set "retrain_last_status=failed" \
    --set "retrain_last_end_ms=${ts_ms}" \
    --set "retrain_last_error=${failed_cmd}" \
    --set "reload_last_status=${reload_for_failure}" \
    --set "reload_last_at_ms=${ts_ms}"
}

trap mark_failure ERR
trap release_lock EXIT INT TERM
acquire_lock

cd "${ROOT_DIR}"

# Prefer the project venv to avoid picking up a wrong system Python.
if [ -x "${ROOT_DIR}/.venv/bin/python3" ]; then
  PYTHON="${ROOT_DIR}/.venv/bin/python3"
elif [ -x "${ROOT_DIR}/.venv/bin/python" ]; then
  PYTHON="${ROOT_DIR}/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON="$(command -v python3)"
else
  echo "[$(timestamp)] ERROR retrain: python3 not found" | tee -a "${LOG_DIR}/retrain.log"
  exit 1
fi

run_step "python_env_check" "${PYTHON}" -c "import sys; assert sys.version_info >= (3, 10), f'Python {sys.version.split()[0]} too old; require >=3.10'; print(sys.executable, sys.version.split()[0])"
run_step "python_deps_check" "${PYTHON}" -c "import importlib.util, sys; required=['duckdb','pandas','numpy','joblib','sklearn']; missing=[m for m in required if importlib.util.find_spec(m) is None]; print('deps ok' if not missing else 'missing deps: ' + ', '.join(missing)); sys.exit(0 if not missing else 1)"

if is_truthy "${RUN_OPS_SMOKE_ON_RETRAIN}"; then
  echo "[$(timestamp)] START ops_smoke" | tee -a "${LOG_DIR}/retrain.log"
  if run_ops_smoke; then
    echo "[$(timestamp)] DONE  ops_smoke" | tee -a "${LOG_DIR}/retrain.log"
  else
    echo "[$(timestamp)] ERROR ops_smoke failed; aborting retrain" | tee -a "${LOG_DIR}/retrain.log"
    "${PYTHON}" scripts/health_alert_watchdog.py \
      --notify-subject "${ML_ALERT_SUBJECT_PREFIX:-[ALERT]} OPS SMOKE FAILED" \
      --notify-body "host=$(hostname) step=ops_smoke status=failed log=${LOG_DIR}/retrain.log" \
      >> "${LOG_DIR}/retrain.log" 2>&1 || true
    exit 1
  fi
else
  echo "[$(timestamp)] INFO ops_smoke skipped (ML_RUN_OPS_SMOKE_ON_RETRAIN=false)" | tee -a "${LOG_DIR}/retrain.log"
fi

ops_set \
  --set "retrain_state=running" \
  --set "retrain_last_status=running" \
  --set "retrain_last_start_ms=$(now_ms)" \
  --set "retrain_last_error="

# Backfill recent days to capture any gaps (dashboard outages, weekends).
# Uses 5m bars from Yahoo (supports longer ranges than 1m's 7-day limit).
run_step "backfill" "${PYTHON}" scripts/backfill_events.py --symbols "${RETRAIN_SYMBOLS}" --range 7d --interval 5m --source yahoo

run_step "build_labels"    "${PYTHON}" scripts/build_labels.py --horizons 5 15 60 --incremental
run_step "export_parquet"  "${PYTHON}" scripts/export_parquet.py
run_step "duckdb_view"     "${PYTHON}" scripts/build_duckdb_view.py
run_step "train_artifacts" "${PYTHON}" scripts/train_rf_artifacts.py
if is_truthy "${MODEL_GOV_FORCE_PROMOTE:-false}"; then
  run_step "governance_evaluate" \
    "${PYTHON}" scripts/model_governance.py \
    --models-dir "${MODEL_DIR}" \
    --ops-db "${PIVOT_DB_PATH}" \
    evaluate \
    --force-promote
else
  run_step "governance_evaluate" \
    "${PYTHON}" scripts/model_governance.py \
    --models-dir "${MODEL_DIR}" \
    --ops-db "${PIVOT_DB_PATH}" \
    evaluate
fi

# Tell the running ML server to hot-reload the new model artifacts.
echo "[$(timestamp)] Reloading ML server models..." | tee -a "${LOG_DIR}/retrain.log"
curl -sf -X POST http://127.0.0.1:5003/reload >> "${LOG_DIR}/retrain.log" 2>&1 || {
  RELOAD_STATUS="failed"
  ops_set \
    --set "reload_last_status=failed" \
    --set "reload_last_at_ms=$(now_ms)"
  echo "[$(timestamp)] WARN: ML server reload failed (server may need manual restart)" | tee -a "${LOG_DIR}/retrain.log"
}
if [[ "${RELOAD_STATUS}" != "failed" ]]; then
  RELOAD_STATUS="ok"
  ops_set \
    --set "reload_last_status=ok" \
    --set "reload_last_at_ms=$(now_ms)"
fi

echo "[$(timestamp)] Generating daily ML report..." | tee -a "${LOG_DIR}/retrain.log"
if REPORT_OUTPUT="$("${PYTHON}" scripts/generate_daily_ml_report.py --db "${PIVOT_DB_PATH}" --out-dir "${LOG_DIR}/reports" 2>&1)"; then
  printf '%s\n' "${REPORT_OUTPUT}" >> "${LOG_DIR}/retrain.log"
  REPORT_PATH="$(printf '%s\n' "${REPORT_OUTPUT}" | tail -n 1)"
  echo "[$(timestamp)] DONE  daily_report" | tee -a "${LOG_DIR}/retrain.log"
else
  printf '%s\n' "${REPORT_OUTPUT}" >> "${LOG_DIR}/retrain.log"
  echo "[$(timestamp)] WARN: daily report generation failed" | tee -a "${LOG_DIR}/retrain.log"
fi

if [[ -n "${REPORT_PATH}" ]] && [[ -f "${REPORT_PATH}" ]] && is_truthy "${NOTIFY_ON_RETRAIN}"; then
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
ops_set \
  --set "retrain_state=idle" \
  --set "retrain_last_status=ok" \
  --set "retrain_last_end_ms=$(now_ms)" \
  --set "reload_last_status=${RELOAD_STATUS}"
