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
  capture_ops_smoke_failure_details
  echo "[$(timestamp)] WARN ops_smoke summary: ${OPS_SMOKE_FAILURE_SUMMARY}" | tee -a "${LOG_DIR}/retrain.log"
  echo "[$(timestamp)] WARN ops_smoke hint: ${OPS_SMOKE_FAILURE_HINT}" | tee -a "${LOG_DIR}/retrain.log"
  return 1
}

sanitize_single_line() {
  local raw="${1:-}"
  printf '%s' "${raw}" | tr '\n\r' '  ' | sed -E 's/[[:space:]]+/ /g; s/^ //; s/ $//'
}

capture_ops_smoke_failure_details() {
  local window summary hint
  window="$(tail -n 800 "${LOG_DIR}/retrain.log" 2>/dev/null || true)"
  summary=""
  hint="Check retrain.log for the latest traceback."

  if [[ -n "${window}" ]]; then
    summary="$(printf '%s\n' "${window}" | grep -E "AssertionError: " | tail -n 1 || true)"
    if [[ -z "${summary}" ]]; then
      summary="$(printf '%s\n' "${window}" | grep -E "FAILED \\(failures=[0-9]+" | tail -n 1 || true)"
    fi
    if [[ -z "${summary}" ]]; then
      summary="$(printf '%s\n' "${window}" | grep -E "(ModuleNotFoundError:|ImportError:)" | tail -n 1 || true)"
    fi
    if [[ -z "${summary}" ]]; then
      summary="$(printf '%s\n' "${window}" | grep -E "^ERROR: " | tail -n 1 || true)"
    fi
  fi

  summary="$(sanitize_single_line "${summary}")"
  if [[ -z "${summary}" ]]; then
    summary="ops_smoke failed after retry; no concise failure marker was found."
  fi

  if [[ "${summary}" == *"AssertionError:"* && "${summary}" == *" not found in "* ]]; then
    hint="Likely smoke contract mismatch: local script/config is behind expected test contract. Pull latest main and rerun retrain."
  elif [[ "${summary}" == *"ModuleNotFoundError:"* || "${summary}" == *"ImportError:"* ]]; then
    hint="Missing Python dependency in retrain runtime env. Run deps check/install for .venv and rerun."
  elif [[ "${summary}" == *"FAILED (failures="* ]]; then
    hint="One or more ops smoke tests failed; inspect the last traceback block in retrain.log."
  fi

  OPS_SMOKE_FAILURE_SUMMARY="${summary}"
  OPS_SMOKE_FAILURE_HINT="${hint}"
}

build_ops_smoke_alert_body() {
  printf 'host=%s\nstep=ops_smoke\nstatus=failed\nsummary=%s\nhint=%s\nlog=%s\n' \
    "$(hostname)" \
    "${OPS_SMOKE_FAILURE_SUMMARY}" \
    "${OPS_SMOKE_FAILURE_HINT}" \
    "${LOG_DIR}/retrain.log"
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
# Back-compat alias used in ad-hoc operator runs.
# OPSMOKE_SKIP=1 (or OPS_SMOKE_SKIP=1) forces smoke skip even when
# ML_RUN_OPS_SMOKE_ON_RETRAIN is unset/true.
if is_truthy "${OPSMOKE_SKIP:-${OPS_SMOKE_SKIP:-false}}"; then
  RUN_OPS_SMOKE_ON_RETRAIN="false"
fi
SCORE_UNSCORED_ON_RETRAIN="${RETRAIN_SCORE_UNSCORED_ON_RETRAIN:-true}"
SCORE_UNSCORED_LOOKBACK_DAYS="${RETRAIN_SCORE_UNSCORED_LOOKBACK_DAYS:-7}"
SCORE_UNSCORED_LIMIT="${RETRAIN_SCORE_UNSCORED_LIMIT:-3000}"
SCORE_UNSCORED_BATCH_SIZE="${RETRAIN_SCORE_UNSCORED_BATCH_SIZE:-16}"
SCORE_UNSCORED_TIMEOUT_SEC="${RETRAIN_SCORE_UNSCORED_TIMEOUT_SEC:-20}"
SCORE_UNSCORED_MAX_ATTEMPTS="${RETRAIN_SCORE_UNSCORED_MAX_ATTEMPTS:-3}"
SCORE_UNSCORED_RETRY_BASE_SEC="${RETRAIN_SCORE_UNSCORED_RETRY_BASE_SEC:-0.5}"
SCORE_UNSCORED_RETRY_MAX_SEC="${RETRAIN_SCORE_UNSCORED_RETRY_MAX_SEC:-5}"
SCORE_UNSCORED_VERIFY_ON_RETRAIN="${RETRAIN_SCORE_UNSCORED_VERIFY_ON_RETRAIN:-true}"
SCORE_UNSCORED_MAX_REMAINING="${RETRAIN_SCORE_UNSCORED_MAX_REMAINING:-0}"
SCORE_UNSCORED_FAIL_ON_PARTIAL="${RETRAIN_SCORE_UNSCORED_FAIL_ON_PARTIAL:-false}"
SCORE_UNSCORED_BACKLOG_SWEEP_ON_RETRAIN="${RETRAIN_SCORE_UNSCORED_BACKLOG_SWEEP_ON_RETRAIN:-true}"
SCORE_UNSCORED_BACKLOG_SWEEP_LIMIT="${RETRAIN_SCORE_UNSCORED_BACKLOG_SWEEP_LIMIT:-10000}"
SCORE_UNSCORED_BACKLOG_SWEEP_MIN_BACKLOG="${RETRAIN_SCORE_UNSCORED_BACKLOG_SWEEP_MIN_BACKLOG:-1}"
REFRESH_ML_METRICS_ON_RETRAIN="${RETRAIN_REFRESH_ML_METRICS_ON_RETRAIN:-true}"
RETRAIN_METRICS_DUCKDB_PATH="${RETRAIN_METRICS_DUCKDB_PATH:-${DUCKDB_PATH:-data/pivot_training.duckdb}}"
RETRAIN_METRICS_DUCKDB_VIEW="${RETRAIN_METRICS_DUCKDB_VIEW:-${DUCKDB_VIEW:-training_events_v1}}"
RETRAIN_METRICS_TARGET="${RETRAIN_METRICS_TARGET:-reject}"
RETRAIN_METRICS_HORIZON_MIN="${RETRAIN_METRICS_HORIZON_MIN:-15}"
RETRAIN_METRICS_TRAIN_DAYS="${RETRAIN_METRICS_TRAIN_DAYS:-30}"
RETRAIN_METRICS_CALIB_DAYS="${RETRAIN_METRICS_CALIB_DAYS:-5}"
RETRAIN_METRICS_TEST_DAYS="${RETRAIN_METRICS_TEST_DAYS:-5}"
RETRAIN_METRICS_MAX_FOLDS="${RETRAIN_METRICS_MAX_FOLDS:-12}"
RETRAIN_METRICS_MIN_EVENTS="${RETRAIN_METRICS_MIN_EVENTS:-200}"
RETRAIN_METRICS_SPLIT_MODE="${RETRAIN_METRICS_SPLIT_MODE:-rolling}"
RETRAIN_METRICS_OUT="${RETRAIN_METRICS_OUT:-data/exports/rf_walkforward_metrics.json}"
RETRAIN_METRICS_FEATURE_OUT="${RETRAIN_METRICS_FEATURE_OUT:-data/exports/rf_feature_report.json}"
RETRAIN_METRICS_FEATURE_CSV="${RETRAIN_METRICS_FEATURE_CSV:-data/exports/rf_feature_report.csv}"
RETRAIN_RF_CALIB_DAYS="${RETRAIN_RF_CALIB_DAYS:-${RF_CALIB_DAYS:-10}}"
RETRAIN_METRICS_CALIB_OUT="${RETRAIN_METRICS_CALIB_OUT:-data/exports/rf_calibration_curve.json}"
RETRAIN_METRICS_CALIB_CSV="${RETRAIN_METRICS_CALIB_CSV:-data/exports/rf_calibration_curve.csv}"
OPS_SMOKE_FAILURE_SUMMARY=""
OPS_SMOKE_FAILURE_HINT=""
RELOAD_STATUS="not_attempted"
RETRAIN_LAST_STATUS="ok"
RETRAIN_LAST_ERROR=""

ops_set() {
  if [[ -z "${PYTHON}" ]]; then
    return 0
  fi
  "${PYTHON}" scripts/ops_status.py --db "${PIVOT_DB_PATH}" "$@" >> "${LOG_DIR}/retrain.log" 2>&1 || true
}

append_retrain_error() {
  local message="${1:-}"
  [[ -n "${message}" ]] || return 0
  if [[ -z "${RETRAIN_LAST_ERROR}" ]]; then
    RETRAIN_LAST_ERROR="${message}"
  else
    RETRAIN_LAST_ERROR="${RETRAIN_LAST_ERROR}; ${message}"
  fi
}

mark_soft_failure() {
  local reason="${1:-unknown_soft_failure}"
  RETRAIN_LAST_STATUS="failed"
  append_retrain_error "${reason}"
}

count_unscored_non_preview() {
  if [[ -z "${PYTHON}" ]]; then
    echo 0
    return 0
  fi
  "${PYTHON}" - "${PIVOT_DB_PATH}" <<'PY'
import sqlite3
import sys

db_path = str(sys.argv[1]) if len(sys.argv) > 1 else ""

try:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='touch_events'")
    if cur.fetchone() is None:
        print(0)
        raise SystemExit(0)
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='prediction_log'")
    if cur.fetchone() is None:
        cur.execute("SELECT COUNT(*) FROM touch_events")
        print(int(cur.fetchone()[0] or 0))
        raise SystemExit(0)
    cur.execute(
        """
        SELECT COUNT(*)
        FROM touch_events t
        LEFT JOIN prediction_log p
          ON p.event_id = t.event_id
         AND COALESCE(p.is_preview, 0) = 0
        WHERE p.event_id IS NULL
        """
    )
    print(int(cur.fetchone()[0] or 0))
except Exception:
    print(0)
PY
}

file_mtime_ms() {
  local file_path="${1:-}"
  if [[ -z "${PYTHON}" || -z "${file_path}" ]]; then
    echo 0
    return 0
  fi
  "${PYTHON}" - "${file_path}" <<'PY'
import pathlib
import sys

if len(sys.argv) < 2:
    print(0)
    raise SystemExit(0)

path = pathlib.Path(sys.argv[1])
if not path.exists():
    print(0)
    raise SystemExit(0)

print(int(path.stat().st_mtime_ns // 1_000_000))
PY
}

log_threshold_guard_summary() {
  local manifest_path="${MODEL_DIR%/}/manifest_runtime_latest.json"
  if [[ "${manifest_path}" != /* ]]; then
    manifest_path="${ROOT_DIR}/${manifest_path}"
  fi
  if [[ -z "${PYTHON}" || ! -f "${manifest_path}" ]]; then
    return 0
  fi
  "${PYTHON}" - "${manifest_path}" <<'PY' || true
import json
import pathlib
import sys

if len(sys.argv) < 2:
    raise SystemExit(0)

path = pathlib.Path(sys.argv[1])
if not path.exists():
    raise SystemExit(0)

try:
    manifest = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    raise SystemExit(0)

thresholds = manifest.get("thresholds") or {}
thresholds_meta = manifest.get("thresholds_meta") or {}
targets = sorted(set(list(thresholds.keys()) + list(thresholds_meta.keys())))

def _to_sorted_horizons(values):
    def _key(v):
        try:
            return int(str(v))
        except Exception:
            return 10**9
    return sorted(values, key=_key)

def _fmt(value):
    if value is None:
        return "na"
    try:
        return f"{float(value):.6f}"
    except Exception:
        return str(value)

for target in targets:
    target_thresholds = thresholds.get(target) or {}
    target_meta = thresholds_meta.get(target) or {}
    horizons = _to_sorted_horizons(set(list(target_thresholds.keys()) + list(target_meta.keys())))
    for horizon in horizons:
        meta = target_meta.get(str(horizon)) or {}
        threshold = target_thresholds.get(str(horizon))
        guard_reason = str(meta.get("guard_reason") or "none").strip()
        if not guard_reason:
            guard_reason = "none"
        guard_reason = guard_reason.replace("\n", " ")
        print(
            "target={target} horizon={horizon} threshold={threshold} "
            "fallback={fallback} guard={guard} reason={reason} score={score} signals={signals} "
            "tp_util={tp_util} fp_util={fp_util} corr_pos={corr_pos} tune_rows={tune_rows} fit_rows={fit_rows}".format(
                target=target,
                horizon=horizon,
                threshold=_fmt(threshold),
                fallback=str(bool(meta.get("fallback"))).lower(),
                guard=str(bool(meta.get("guard_applied"))).lower(),
                reason=guard_reason,
                score=_fmt(meta.get("score")),
                signals=str(meta.get("signals") if meta.get("signals") is not None else "na"),
                tp_util=_fmt(meta.get("selected_tp_utility_sum")),
                fp_util=_fmt(meta.get("selected_fp_utility_sum")),
                corr_pos=_fmt(meta.get("tune_prob_utility_corr_pos")),
                tune_rows=str(meta.get("threshold_tune_size") if meta.get("threshold_tune_size") is not None else "na"),
                fit_rows=str(meta.get("calibration_fit_size") if meta.get("calibration_fit_size") is not None else "na"),
            )
        )
PY
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
RETRAIN_REQUIRED_MODULES=(duckdb pandas numpy joblib sklearn)
if is_truthy "${RUN_OPS_SMOKE_ON_RETRAIN}"; then
  # Keep preflight aligned with ops smoke imports to fail fast on missing env deps.
  RETRAIN_REQUIRED_MODULES+=(fastapi ib_insync uvicorn)
fi
RETRAIN_MODULES_CSV="$(IFS=,; echo "${RETRAIN_REQUIRED_MODULES[*]}")"
run_step "python_deps_check" "${PYTHON}" -c "import importlib.util, sys; required=[m for m in sys.argv[1].split(',') if m]; missing=[m for m in required if importlib.util.find_spec(m) is None]; print('deps ok: ' + ', '.join(required) if not missing else 'missing deps: ' + ', '.join(missing)); sys.exit(0 if not missing else 1)" "${RETRAIN_MODULES_CSV}"

if is_truthy "${RUN_OPS_SMOKE_ON_RETRAIN}"; then
  echo "[$(timestamp)] START ops_smoke" | tee -a "${LOG_DIR}/retrain.log"
  if run_ops_smoke; then
    echo "[$(timestamp)] DONE  ops_smoke" | tee -a "${LOG_DIR}/retrain.log"
  else
    ALERT_BODY="$(build_ops_smoke_alert_body)"
    echo "[$(timestamp)] ERROR ops_smoke failed; aborting retrain" | tee -a "${LOG_DIR}/retrain.log"
    "${PYTHON}" scripts/health_alert_watchdog.py \
      --notify-subject "${ML_ALERT_SUBJECT_PREFIX:-[ALERT]} OPS SMOKE FAILED" \
      --notify-body "${ALERT_BODY}" \
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

run_step "build_labels"    "${PYTHON}" scripts/build_labels.py --horizons 5 15 30 60 --incremental
run_step "export_parquet"  "${PYTHON}" scripts/export_parquet.py
run_step "duckdb_view"     "${PYTHON}" scripts/build_duckdb_view.py
echo "[$(timestamp)] INFO train_artifacts config calib_days=${RETRAIN_RF_CALIB_DAYS}" | tee -a "${LOG_DIR}/retrain.log"
run_step "train_artifacts" "${PYTHON}" scripts/train_rf_artifacts.py --calib-days "${RETRAIN_RF_CALIB_DAYS}"
threshold_summary_output="$(log_threshold_guard_summary)"
if [[ -n "${threshold_summary_output}" ]]; then
  while IFS= read -r summary_line; do
    [[ -n "${summary_line}" ]] || continue
    echo "[$(timestamp)] INFO threshold_summary ${summary_line}" | tee -a "${LOG_DIR}/retrain.log"
  done <<< "${threshold_summary_output}"
fi
if is_truthy "${REFRESH_ML_METRICS_ON_RETRAIN}"; then
  local_metrics_started_ms="$(now_ms)"
  ops_set \
    --set "metrics_refresh_last_status=running" \
    --set "metrics_refresh_last_at_ms=${local_metrics_started_ms}" \
    --set "metrics_refresh_last_error="
  echo "[$(timestamp)] START refresh_ml_metrics" | tee -a "${LOG_DIR}/retrain.log"
  echo "[$(timestamp)] INFO refresh_ml_metrics config target=${RETRAIN_METRICS_TARGET} horizon=${RETRAIN_METRICS_HORIZON_MIN} split=${RETRAIN_METRICS_SPLIT_MODE} folds=${RETRAIN_METRICS_MAX_FOLDS}" | tee -a "${LOG_DIR}/retrain.log"
  if "${PYTHON}" scripts/train_rf.py \
      --db "${RETRAIN_METRICS_DUCKDB_PATH}" \
      --view "${RETRAIN_METRICS_DUCKDB_VIEW}" \
      --target "${RETRAIN_METRICS_TARGET}" \
      --horizon-min "${RETRAIN_METRICS_HORIZON_MIN}" \
      --train-days "${RETRAIN_METRICS_TRAIN_DAYS}" \
      --calib-days "${RETRAIN_METRICS_CALIB_DAYS}" \
      --test-days "${RETRAIN_METRICS_TEST_DAYS}" \
      --max-folds "${RETRAIN_METRICS_MAX_FOLDS}" \
      --min-events "${RETRAIN_METRICS_MIN_EVENTS}" \
      --split-mode "${RETRAIN_METRICS_SPLIT_MODE}" \
      --out "${RETRAIN_METRICS_OUT}" \
      --feature-out "${RETRAIN_METRICS_FEATURE_OUT}" \
      --feature-csv "${RETRAIN_METRICS_FEATURE_CSV}" \
      --calib-out "${RETRAIN_METRICS_CALIB_OUT}" \
      --calib-csv "${RETRAIN_METRICS_CALIB_CSV}" \
      >> "${LOG_DIR}/retrain.log" 2>&1; then
    metrics_mtime_ms="$(file_mtime_ms "${RETRAIN_METRICS_OUT}")"
    calib_mtime_ms="$(file_mtime_ms "${RETRAIN_METRICS_CALIB_OUT}")"
    if [[ "${metrics_mtime_ms}" =~ ^[0-9]+$ && "${calib_mtime_ms}" =~ ^[0-9]+$ ]] \
      && (( metrics_mtime_ms >= local_metrics_started_ms )) \
      && (( calib_mtime_ms >= local_metrics_started_ms )); then
      echo "[$(timestamp)] DONE  refresh_ml_metrics" | tee -a "${LOG_DIR}/retrain.log"
      ops_set \
        --set "metrics_refresh_last_status=ok" \
        --set "metrics_refresh_last_at_ms=$(now_ms)" \
        --set "metrics_refresh_last_error="
    else
      echo "[$(timestamp)] WARN: refresh_ml_metrics did not produce fresh artifact timestamps (metrics=${metrics_mtime_ms} calib=${calib_mtime_ms})" | tee -a "${LOG_DIR}/retrain.log"
      mark_soft_failure "ml_metrics_refresh_stale"
      ops_set \
        --set "metrics_refresh_last_status=failed" \
        --set "metrics_refresh_last_at_ms=$(now_ms)" \
        --set "metrics_refresh_last_error=artifacts_not_refreshed"
    fi
  else
    echo "[$(timestamp)] WARN: refresh_ml_metrics failed (continuing retrain)" | tee -a "${LOG_DIR}/retrain.log"
    mark_soft_failure "ml_metrics_refresh_failed"
    ops_set \
      --set "metrics_refresh_last_status=failed" \
      --set "metrics_refresh_last_at_ms=$(now_ms)" \
      --set "metrics_refresh_last_error=train_rf_failed"
  fi
else
  echo "[$(timestamp)] INFO refresh_ml_metrics skipped (RETRAIN_REFRESH_ML_METRICS_ON_RETRAIN=false)" | tee -a "${LOG_DIR}/retrain.log"
  ops_set \
    --set "metrics_refresh_last_status=skipped" \
    --set "metrics_refresh_last_at_ms=$(now_ms)"
fi
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
  mark_soft_failure "ml_reload_failed"
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

if is_truthy "${SCORE_UNSCORED_ON_RETRAIN}" && [[ "${RELOAD_STATUS}" == "ok" ]]; then
  echo "[$(timestamp)] START score_unscored" | tee -a "${LOG_DIR}/retrain.log"
  echo "[$(timestamp)] INFO score_unscored config lookback_days=${SCORE_UNSCORED_LOOKBACK_DAYS} limit=${SCORE_UNSCORED_LIMIT} batch_size=${SCORE_UNSCORED_BATCH_SIZE} timeout_sec=${SCORE_UNSCORED_TIMEOUT_SEC} max_attempts=${SCORE_UNSCORED_MAX_ATTEMPTS} verify=${SCORE_UNSCORED_VERIFY_ON_RETRAIN} max_remaining=${SCORE_UNSCORED_MAX_REMAINING}" | tee -a "${LOG_DIR}/retrain.log"
  score_unscored_args=(
    --db "${PIVOT_DB_PATH}"
    --lookback-days "${SCORE_UNSCORED_LOOKBACK_DAYS}"
    --limit "${SCORE_UNSCORED_LIMIT}"
    --batch-size "${SCORE_UNSCORED_BATCH_SIZE}"
    --timeout-sec "${SCORE_UNSCORED_TIMEOUT_SEC}"
    --max-attempts "${SCORE_UNSCORED_MAX_ATTEMPTS}"
    --retry-base-sec "${SCORE_UNSCORED_RETRY_BASE_SEC}"
    --retry-max-sec "${SCORE_UNSCORED_RETRY_MAX_SEC}"
    --score-url "http://127.0.0.1:5003/score"
    --single-fallback-on-failure
  )
  if is_truthy "${SCORE_UNSCORED_VERIFY_ON_RETRAIN}"; then
    score_unscored_args+=(--verify-after --max-remaining "${SCORE_UNSCORED_MAX_REMAINING}")
  fi
  if is_truthy "${SCORE_UNSCORED_FAIL_ON_PARTIAL}"; then
    score_unscored_args+=(--fail-on-partial)
  fi
  if "${PYTHON}" scripts/score_unscored_touch_events.py \
      "${score_unscored_args[@]}" \
      >> "${LOG_DIR}/retrain.log" 2>&1; then
    echo "[$(timestamp)] DONE  score_unscored" | tee -a "${LOG_DIR}/retrain.log"
  else
    echo "[$(timestamp)] WARN: score_unscored failed (continuing retrain)" | tee -a "${LOG_DIR}/retrain.log"
    mark_soft_failure "score_unscored_failed"
  fi
elif is_truthy "${SCORE_UNSCORED_ON_RETRAIN}"; then
  echo "[$(timestamp)] INFO score_unscored skipped (reload status: ${RELOAD_STATUS})" | tee -a "${LOG_DIR}/retrain.log"
else
  echo "[$(timestamp)] INFO score_unscored skipped (RETRAIN_SCORE_UNSCORED_ON_RETRAIN=false)" | tee -a "${LOG_DIR}/retrain.log"
fi

if is_truthy "${SCORE_UNSCORED_ON_RETRAIN}" && is_truthy "${SCORE_UNSCORED_BACKLOG_SWEEP_ON_RETRAIN}" && [[ "${RELOAD_STATUS}" == "ok" ]]; then
  backlog_before="$(count_unscored_non_preview)"
  min_backlog="${SCORE_UNSCORED_BACKLOG_SWEEP_MIN_BACKLOG}"
  if ! [[ "${min_backlog}" =~ ^[0-9]+$ ]]; then
    min_backlog=1
  fi
  echo "[$(timestamp)] INFO score_unscored backlog before sweep=${backlog_before}" | tee -a "${LOG_DIR}/retrain.log"
  if [[ "${backlog_before}" =~ ^[0-9]+$ ]] && (( backlog_before >= min_backlog )); then
    sweep_limit="${SCORE_UNSCORED_BACKLOG_SWEEP_LIMIT}"
    if ! [[ "${sweep_limit}" =~ ^[0-9]+$ ]]; then
      sweep_limit=0
    fi
    if (( sweep_limit <= 0 || sweep_limit > backlog_before )); then
      sweep_limit="${backlog_before}"
    fi
    if (( sweep_limit > 0 )); then
      echo "[$(timestamp)] START score_unscored_backlog_sweep" | tee -a "${LOG_DIR}/retrain.log"
      echo "[$(timestamp)] INFO score_unscored_backlog_sweep config limit=${sweep_limit} batch_size=${SCORE_UNSCORED_BATCH_SIZE} timeout_sec=${SCORE_UNSCORED_TIMEOUT_SEC} max_attempts=${SCORE_UNSCORED_MAX_ATTEMPTS}" | tee -a "${LOG_DIR}/retrain.log"
      if "${PYTHON}" scripts/score_unscored_touch_events.py \
          --db "${PIVOT_DB_PATH}" \
          --lookback-days 0 \
          --limit "${sweep_limit}" \
          --batch-size "${SCORE_UNSCORED_BATCH_SIZE}" \
          --timeout-sec "${SCORE_UNSCORED_TIMEOUT_SEC}" \
          --max-attempts "${SCORE_UNSCORED_MAX_ATTEMPTS}" \
          --retry-base-sec "${SCORE_UNSCORED_RETRY_BASE_SEC}" \
          --retry-max-sec "${SCORE_UNSCORED_RETRY_MAX_SEC}" \
          --score-url "http://127.0.0.1:5003/score" \
          --single-fallback-on-failure \
          >> "${LOG_DIR}/retrain.log" 2>&1; then
        echo "[$(timestamp)] DONE  score_unscored_backlog_sweep" | tee -a "${LOG_DIR}/retrain.log"
      else
        echo "[$(timestamp)] WARN: score_unscored_backlog_sweep failed (continuing retrain)" | tee -a "${LOG_DIR}/retrain.log"
        mark_soft_failure "score_unscored_backlog_sweep_failed"
      fi
      backlog_after="$(count_unscored_non_preview)"
      echo "[$(timestamp)] INFO score_unscored backlog after sweep=${backlog_after}" | tee -a "${LOG_DIR}/retrain.log"
    else
      echo "[$(timestamp)] INFO score_unscored_backlog_sweep skipped (limit resolved to 0)" | tee -a "${LOG_DIR}/retrain.log"
    fi
  else
    echo "[$(timestamp)] INFO score_unscored_backlog_sweep skipped (backlog below threshold)" | tee -a "${LOG_DIR}/retrain.log"
  fi
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

if [[ "${RETRAIN_LAST_STATUS}" == "ok" ]]; then
  echo "[$(timestamp)] Retrain cycle complete." | tee -a "${LOG_DIR}/retrain.log"
else
  echo "[$(timestamp)] WARN: Retrain cycle completed with issues (${RETRAIN_LAST_ERROR})" | tee -a "${LOG_DIR}/retrain.log"
fi
ops_set \
  --set "retrain_state=idle" \
  --set "retrain_last_status=${RETRAIN_LAST_STATUS}" \
  --set "retrain_last_end_ms=$(now_ms)" \
  --set "retrain_last_error=${RETRAIN_LAST_ERROR}" \
  --set "reload_last_status=${RELOAD_STATUS}"
