#!/usr/bin/env bash
# Phase-1 daily orchestration for the levels data product.
#   maturation (labels) -> emit forecasts -> score -> track record -> morning map -> publish
# Read-only on the live trading DB except the standard, additive label maturation
# (skippable with LEVELS_SKIP_LABELS=1 if another cron already matures labels).
# Writes only: data/levels_product.sqlite, evidence/levels_product/, logs/levels_product/.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LP_DIR="${ROOT_DIR}/scripts/levels_product"
LOG_DIR="${ROOT_DIR}/logs/levels_product"
LOCK_DIR="${LOG_DIR}/.daily.lock"
SYMBOL="${LEVELS_SYMBOL:-SPY}"
mkdir -p "${LOG_DIR}"

# resolve a >=3.10 interpreter (mirrors run_retrain_evidence_pack.resolve_training_python)
PY="${PYTHON_BIN:-}"
if [[ -z "${PY}" ]]; then
  for cand in "${ROOT_DIR}/.venv313/bin/python" "${ROOT_DIR}/.venv/bin/python"; do
    [[ -x "${cand}" ]] && { PY="${cand}"; break; }
  done
fi
[[ -z "${PY}" ]] && { echo "no project python found"; exit 1; }

ts() { date '+%Y-%m-%d %H:%M:%S'; }
log() { echo "[$(ts)] $*" | tee -a "${LOG_DIR}/daily.log"; }

# --- lock (skip if a run is already in progress) ---
if ! mkdir "${LOCK_DIR}" 2>/dev/null; then
  if [[ -f "${LOCK_DIR}/pid" ]] && kill -0 "$(cat "${LOCK_DIR}/pid" 2>/dev/null)" 2>/dev/null; then
    log "WARN another daily run in progress; skipping"; exit 0
  fi
  rm -rf "${LOCK_DIR}"; mkdir "${LOCK_DIR}"
fi
echo "$$" > "${LOCK_DIR}/pid"
trap 'rm -rf "${LOCK_DIR}" >/dev/null 2>&1 || true' EXIT INT TERM HUP

cd "${ROOT_DIR}"

# trading-day gate (reuse the repo's single source of truth) — skip weekends and
# US market holidays so we never email on a non-trading day. LEVELS_FORCE=1 overrides.
if [[ "${LEVELS_FORCE:-0}" != "1" ]]; then
  if ! "${PY}" -c "import sys; sys.path.insert(0, '${ROOT_DIR}/scripts'); from trading_calendar import is_trading_day; from datetime import datetime; from zoneinfo import ZoneInfo; sys.exit(0 if is_trading_day(datetime.now(ZoneInfo('America/New_York')).date()) else 1)"; then
    log "non-trading day (ET); skipping levels run"
    exit 0
  fi
fi

log "=== levels product daily start (symbol=${SYMBOL}, py=${PY}, channel=${LEVELS_CHANNEL:-auto}) ==="

if [[ "${LEVELS_SKIP_LABELS:-0}" != "1" ]]; then
  log "step 1/6 build_labels --incremental (maturation)"
  "${PY}" scripts/build_labels.py --incremental >>"${LOG_DIR}/daily.log" 2>&1 || log "WARN build_labels failed (continuing)"
else
  log "step 1/6 build_labels SKIPPED (LEVELS_SKIP_LABELS=1)"
fi

log "step 2/6 forecast_store emit"
"${PY}" "${LP_DIR}/forecast_store.py" emit --symbol "${SYMBOL}" >>"${LOG_DIR}/daily.log" 2>&1

log "step 3/6 forecast_store score -> scoreboard.json"
"${PY}" "${LP_DIR}/forecast_store.py" score --symbol "${SYMBOL}" > "${LOG_DIR}/scoreboard_${SYMBOL}.json" 2>>"${LOG_DIR}/daily.log"

log "step 4/6 build_track_record"
"${PY}" "${LP_DIR}/build_track_record.py" --symbol "${SYMBOL}" >>"${LOG_DIR}/daily.log" 2>&1

log "step 5/6 morning_level_map"
"${PY}" "${LP_DIR}/morning_level_map.py" --symbol "${SYMBOL}" >>"${LOG_DIR}/daily.log" 2>&1

log "step 6/6 publish (morning post)"
"${PY}" "${LP_DIR}/publish.py" --symbol "${SYMBOL}" >>"${LOG_DIR}/daily.log" 2>&1

log "=== levels product daily done ==="
