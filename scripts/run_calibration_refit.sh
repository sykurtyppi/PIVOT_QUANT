#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}"
LOCK_DIR="${LOG_DIR}/run_calibration_refit.lock"
LOCK_OWNED=0
PYTHON=""
PIVOT_DB_PATH="${PIVOT_DB:-${ROOT_DIR}/data/pivot_events.sqlite}"
AUDIT_ENABLED="${ML_AUDIT_ENABLED:-true}"

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
    echo "[$(timestamp)] SKIP calibration_refit: already running (pid ${existing_pid})." | tee -a "${LOG_DIR}/calibration_refit.log"
    exit 0
  fi

  rm -rf "${LOCK_DIR}" 2>/dev/null || true
  if mkdir "${LOCK_DIR}" 2>/dev/null; then
    printf '%s\n' "$$" > "${LOCK_DIR}/pid"
    LOCK_OWNED=1
    return 0
  fi

  echo "[$(timestamp)] ERROR calibration_refit: unable to acquire lock at ${LOCK_DIR}" | tee -a "${LOG_DIR}/calibration_refit.log"
  exit 1
}

release_lock() {
  if [[ "${LOCK_OWNED}" -ne 1 ]]; then
    return 0
  fi
  rm -rf "${LOCK_DIR}" 2>/dev/null || true
  LOCK_OWNED=0
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

ops_set() {
  if [[ -z "${PYTHON:-}" ]]; then
    return 0
  fi
  "${PYTHON}" scripts/ops_status.py --db "${PIVOT_DB_PATH}" "$@" >> "${LOG_DIR}/calibration_refit.log" 2>&1 || true
}

audit_event() {
  if ! is_truthy "${AUDIT_ENABLED}"; then
    return 0
  fi
  local runner="${PYTHON:-}"
  if [[ -z "${runner}" ]]; then
    if [[ -x "${ROOT_DIR}/.venv/bin/python3" ]]; then
      runner="${ROOT_DIR}/.venv/bin/python3"
    elif command -v python3 >/dev/null 2>&1; then
      runner="$(command -v python3)"
    else
      return 0
    fi
  fi
  local event_type="${1:-}"
  local message="${2:-}"
  shift 2 || true
  if [[ -z "${event_type}" ]]; then
    return 0
  fi
  "${runner}" scripts/audit_log.py --db "${PIVOT_DB_PATH}" log \
    --event-type "${event_type}" \
    --source "run_calibration_refit.sh" \
    --message "${message}" \
    "$@" >> "${LOG_DIR}/calibration_refit.log" 2>&1 || true
}

mark_failure() {
  local exit_code="$?"
  local failed_cmd="${BASH_COMMAND:-unknown}"
  local ts_ms
  ts_ms="$(now_ms)"
  echo "[$(timestamp)] ERROR calibration_refit: command failed (${failed_cmd}) exit=${exit_code}" | tee -a "${LOG_DIR}/calibration_refit.log"
  ops_set \
    --set "calibration_refit_state=idle" \
    --set "calibration_refit_last_status=failed" \
    --set "calibration_refit_last_end_ms=${ts_ms}" \
    --set "calibration_refit_last_error=${failed_cmd}"
  audit_event "calibration_refit_failed" "calibration refit command failed" \
    --detail "command=${failed_cmd}" \
    --detail "exit_code=${exit_code}"
}

run_step() {
  local name="$1"
  shift
  echo "[$(timestamp)] START ${name}" | tee -a "${LOG_DIR}/calibration_refit.log"
  "$@" >> "${LOG_DIR}/calibration_refit.log" 2>&1
  echo "[$(timestamp)] DONE  ${name}" | tee -a "${LOG_DIR}/calibration_refit.log"
}

trap mark_failure ERR
trap release_lock EXIT INT TERM
acquire_lock

cd "${ROOT_DIR}"

if [ -x "${ROOT_DIR}/.venv/bin/python3" ]; then
  PYTHON="${ROOT_DIR}/.venv/bin/python3"
elif [ -x "${ROOT_DIR}/.venv/bin/python" ]; then
  PYTHON="${ROOT_DIR}/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON="$(command -v python3)"
else
  echo "[$(timestamp)] ERROR calibration_refit: python3 not found" | tee -a "${LOG_DIR}/calibration_refit.log"
  exit 1
fi

CALIB_DUCKDB_PATH="${DUCKDB_PATH:-${ROOT_DIR}/data/pivot_training.duckdb}"
CALIB_MODEL_DIR="${RF_MODEL_DIR:-data/models}"
CALIB_VIEW="${DUCKDB_VIEW:-training_events_v1}"
CALIB_DAYS="${CALIB_REFIT_CALIB_DAYS:-5}"
CALIB_MIN_EVENTS="${CALIB_REFIT_MIN_CALIB_EVENTS:-40}"
CALIB_MIN_THRESHOLD_EVENTS="${CALIB_REFIT_MIN_THRESHOLD_EVENTS:-20}"
CALIB_PRECISION_FLOOR="${CALIB_REFIT_PRECISION_FLOOR:-0.40}"
CALIB_RETUNE_THRESHOLDS="${CALIB_REFIT_RETUNE_THRESHOLDS:-false}"
CALIB_THRESHOLD_OBJECTIVE="${CALIB_REFIT_THRESHOLD_OBJECTIVE:-${RF_THRESHOLD_OBJECTIVE:-f1}}"
CALIB_THRESHOLD_MIN_SIGNALS="${CALIB_REFIT_THRESHOLD_MIN_SIGNALS:-10}"
CALIB_THRESHOLD_TRADE_COST_BPS="${CALIB_REFIT_THRESHOLD_TRADE_COST_BPS:-${RF_THRESHOLD_TRADE_COST_BPS:-${ML_COST_TOTAL_BPS:-1.3}}}"
CALIB_THRESHOLD_STABILITY_BAND="${CALIB_REFIT_THRESHOLD_STABILITY_BAND:-${RF_THRESHOLD_STABILITY_BAND:-0.0}}"
CALIB_FIT_FRACTION="${CALIB_REFIT_CALIB_FIT_FRACTION:-0.6}"
CALIB_MIN_FIT_EVENTS="${CALIB_REFIT_CALIB_MIN_FIT_EVENTS:-20}"
CALIB_METHOD="${CALIB_REFIT_METHOD:-auto}"
CALIB_REFRESH_DUCKDB="${CALIB_REFIT_REFRESH_DUCKDB:-true}"
CALIB_BUILD_LABELS="${CALIB_REFIT_BUILD_LABELS:-true}"
CALIB_RELOAD="${CALIB_REFIT_RELOAD:-true}"
CALIB_TARGETS="${CALIB_REFIT_TARGETS:-}"
CALIB_HORIZONS="${CALIB_REFIT_HORIZONS:-}"
CALIB_EXTRA_ARGS=("$@")

# Skip if full retrain is actively running.
if [[ -d "${LOG_DIR}/run_retrain_cycle.lock" ]] && [[ -f "${LOG_DIR}/run_retrain_cycle.lock/pid" ]]; then
  RETRAIN_PID="$(cat "${LOG_DIR}/run_retrain_cycle.lock/pid" 2>/dev/null || true)"
  if [[ -n "${RETRAIN_PID}" ]] && kill -0 "${RETRAIN_PID}" >/dev/null 2>&1; then
    echo "[$(timestamp)] WARN calibration_refit: retrain lock active (pid ${RETRAIN_PID}); skipping run." | tee -a "${LOG_DIR}/calibration_refit.log"
    exit 0
  fi
fi

ops_set \
  --set "calibration_refit_state=running" \
  --set "calibration_refit_last_status=running" \
  --set "calibration_refit_last_start_ms=$(now_ms)" \
  --set "calibration_refit_last_error="

run_step "python_env_check" "${PYTHON}" -c "import sys; assert sys.version_info >= (3, 10); print(sys.executable, sys.version.split()[0])"
audit_event "calibration_refit_started" "calibration refit cycle started" \
  --detail "duckdb_path=${CALIB_DUCKDB_PATH}" \
  --detail "model_dir=${CALIB_MODEL_DIR}" \
  --detail "view=${CALIB_VIEW}" \
  --detail "method=${CALIB_METHOD}"

if is_truthy "${CALIB_BUILD_LABELS}"; then
  run_step "build_labels" "${PYTHON}" scripts/build_labels.py --horizons 5 15 60 --incremental
else
  echo "[$(timestamp)] INFO build_labels skipped (CALIB_REFIT_BUILD_LABELS=false)" | tee -a "${LOG_DIR}/calibration_refit.log"
fi

if is_truthy "${CALIB_REFRESH_DUCKDB}"; then
  run_step "export_parquet" "${PYTHON}" scripts/export_parquet.py
  run_step "duckdb_view" "${PYTHON}" scripts/build_duckdb_view.py
else
  echo "[$(timestamp)] INFO duckdb refresh skipped (CALIB_REFIT_REFRESH_DUCKDB=false)" | tee -a "${LOG_DIR}/calibration_refit.log"
fi

CALIB_ARGS=(
  --db "${CALIB_DUCKDB_PATH}"
  --view "${CALIB_VIEW}"
  --models-dir "${CALIB_MODEL_DIR}"
  --calib-days "${CALIB_DAYS}"
  --calib-fit-fraction "${CALIB_FIT_FRACTION}"
  --calib-min-fit-events "${CALIB_MIN_FIT_EVENTS}"
  --min-calib-events "${CALIB_MIN_EVENTS}"
  --min-threshold-events "${CALIB_MIN_THRESHOLD_EVENTS}"
  --precision-floor "${CALIB_PRECISION_FLOOR}"
  --threshold-objective "${CALIB_THRESHOLD_OBJECTIVE}"
  --threshold-min-signals "${CALIB_THRESHOLD_MIN_SIGNALS}"
  --threshold-trade-cost-bps "${CALIB_THRESHOLD_TRADE_COST_BPS}"
  --threshold-stability-band "${CALIB_THRESHOLD_STABILITY_BAND}"
  --calibration "${CALIB_METHOD}"
  --summary-out "${LOG_DIR}/calibration_refit_last.json"
)
if is_truthy "${CALIB_RETUNE_THRESHOLDS}"; then
  CALIB_ARGS+=(--retune-thresholds)
fi
if [[ -n "${CALIB_TARGETS}" ]]; then
  CALIB_ARGS+=(--targets "${CALIB_TARGETS}")
fi
if [[ -n "${CALIB_HORIZONS}" ]]; then
  CALIB_ARGS+=(--horizons "${CALIB_HORIZONS}")
fi
if (( ${#CALIB_EXTRA_ARGS[@]} > 0 )); then
  CALIB_ARGS+=("${CALIB_EXTRA_ARGS[@]}")
fi

run_step "calibration_refit" "${PYTHON}" scripts/refit_calibration.py "${CALIB_ARGS[@]}"

UPDATED_PAIRS="$("${PYTHON}" - <<'PY'
import json
from pathlib import Path
p = Path("logs/calibration_refit_last.json")
if not p.exists():
    print(0)
else:
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
        print(int(payload.get("updated_pairs") or 0))
    except Exception:
        print(0)
PY
)"

if is_truthy "${CALIB_RELOAD}" && [[ "${UPDATED_PAIRS}" -gt 0 ]]; then
  run_step "ml_reload" curl -sf -X POST http://127.0.0.1:5003/reload
else
  echo "[$(timestamp)] INFO ml_reload skipped (updated_pairs=${UPDATED_PAIRS}, CALIB_REFIT_RELOAD=${CALIB_RELOAD})" | tee -a "${LOG_DIR}/calibration_refit.log"
fi

ops_set \
  --set "calibration_refit_state=idle" \
  --set "calibration_refit_last_status=ok" \
  --set "calibration_refit_last_end_ms=$(now_ms)" \
  --set "calibration_refit_last_updated_pairs=${UPDATED_PAIRS}"
audit_event "calibration_refit_completed" "calibration refit cycle completed" \
  --detail "updated_pairs=${UPDATED_PAIRS}" \
  --detail "reload_enabled=${CALIB_RELOAD}"

echo "[$(timestamp)] Calibration refit cycle complete (updated_pairs=${UPDATED_PAIRS})." | tee -a "${LOG_DIR}/calibration_refit.log"
