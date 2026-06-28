#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}" "${LOG_DIR}/reports"
STATE_FILE="${LOG_DIR}/report_delivery_state.json"
LOCK_DIR="${LOG_DIR}/.daily_report_send.lock"
LOCK_WAIT_SEC="${ML_REPORT_LOCK_WAIT_SEC:-120}"

timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

acquire_lock() {
  local start_ts now_ts lock_pid
  start_ts="$(date +%s)"
  while ! mkdir "${LOCK_DIR}" 2>/dev/null; do
    if [[ -f "${LOCK_DIR}/pid" ]]; then
      lock_pid="$(cat "${LOCK_DIR}/pid" 2>/dev/null || true)"
      if [[ -n "${lock_pid}" && "${lock_pid}" =~ ^[0-9]+$ ]]; then
        if ! kill -0 "${lock_pid}" 2>/dev/null; then
          rm -rf "${LOCK_DIR}" 2>/dev/null || true
          continue
        fi
      fi
    fi
    now_ts="$(date +%s)"
    if (( now_ts - start_ts >= LOCK_WAIT_SEC )); then
      echo "[$(timestamp)] WARN daily_report_send lock busy (waited ${LOCK_WAIT_SEC}s); skipping" >> "${LOG_DIR}/report_delivery.log"
      exit 0
    fi
    sleep 1
  done
  echo "$$" > "${LOCK_DIR}/pid"
  trap 'rm -rf "${LOCK_DIR}" >/dev/null 2>&1 || true' EXIT INT TERM HUP
}

state_has_sent() {
  local report_date="$1"
  local schedule_mode="$2"
  "${PYTHON}" - "$STATE_FILE" "$report_date" "$schedule_mode" <<'PY'
import json
import sys
from pathlib import Path

state_path = Path(sys.argv[1])
report_date = sys.argv[2]
schedule_mode = sys.argv[3]
if not state_path.exists():
    raise SystemExit(1)
try:
    payload = json.loads(state_path.read_text(encoding="utf-8"))
except Exception:
    raise SystemExit(1)
sent = payload.get("sent", {})
key = f"{report_date}|{schedule_mode}"
if sent.get(key) == "ok":
    raise SystemExit(0)
raise SystemExit(1)
PY
}

state_mark_sent() {
  local report_date="$1"
  local schedule_mode="$2"
  local status="$3"
  "${PYTHON}" - "$STATE_FILE" "$report_date" "$schedule_mode" "$status" <<'PY'
import json
import time
import sys
from pathlib import Path

state_path = Path(sys.argv[1])
report_date = sys.argv[2]
schedule_mode = sys.argv[3]
status = sys.argv[4]
key = f"{report_date}|{schedule_mode}"

payload = {}
if state_path.exists():
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}
sent = payload.get("sent")
if not isinstance(sent, dict):
    sent = {}
sent[key] = status
payload["sent"] = sent
payload["updated_at_ms"] = int(time.time() * 1000)
state_path.parent.mkdir(parents=True, exist_ok=True)
tmp = state_path.with_suffix(state_path.suffix + ".tmp")
tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
tmp.replace(state_path)
PY
}

# Resolve >=3.10 Python via shared helper (replaces the previous
# resolve-then-version-check block; the helper already enforces >=3.10).
if ! source "${ROOT_DIR}/scripts/_pybin.sh" 2>>"${LOG_DIR}/report_delivery.log"; then
  echo "[$(timestamp)] ERROR daily_report_send: no python3.10+ found" >> "${LOG_DIR}/report_delivery.log"
  exit 1
fi
PYTHON="${PYTHON_BIN}"
PYTHON_INFO="$("${PYTHON}" -c "import sys; print(f'{sys.executable} {sys.version.split()[0]}')")"

acquire_lock

PIVOT_DB_PATH="${PIVOT_DB:-${ROOT_DIR}/data/pivot_events.sqlite}"
REPORT_OUTPUT=""
REPORT_PATH=""
EXPLICIT_REPORT_DATE="${ML_REPORT_REPORT_DATE:-}"
REPORT_DATE="${EXPLICIT_REPORT_DATE}"
REPORT_DATE_MODE="${ML_REPORT_REPORT_DATE_MODE:-auto}"
SCHEDULE_MODE="${ML_REPORT_SCHEDULE_MODE:-}"
SCHEDULE_MODE_EFFECTIVE="${SCHEDULE_MODE:-manual}"
FORCE_SEND="${ML_REPORT_FORCE_SEND:-false}"
FORCE_SEND_LC="$(printf '%s' "${FORCE_SEND}" | tr '[:upper:]' '[:lower:]')"

echo "[$(timestamp)] START daily_report_send (${PYTHON_INFO})" >> "${LOG_DIR}/report_delivery.log"

# ---------------------------------------------------------------------------
# Trading-day gate — skip weekends and US market holidays (NYSE calendar).
# run_daily_report_send.sh is the policy layer; send_daily_report.py is the
# low-level primitive that has no scheduling policy of its own.  All
# operational send paths MUST go through this wrapper so this gate applies.
# ML_REPORT_FORCE_SEND=true bypasses the gate for operator overrides.
#
# ORDERING (critical — see Codex review finding #1):
#   Step 1: Determine CURRENT_ET_DATE from the real clock (no rollback).
#   Step 2: Gate on CURRENT_ET_DATE *before* any date rollback occurs.
#   Step 3: Resolve REPORT_DATE (may roll back to Friday in auto mode).
#   Step 4: Validate an explicit REPORT_DATE is also a trading day.
#
# This order prevents the Saturday→Friday rollback from bypassing the gate.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# ML_REPORT_FAKE_ET_DATE — TEST-ONLY.  If set to a non-empty value it is used
# as CURRENT_ET_DATE instead of computing from the real clock.  This does NOT
# affect REPORT_DATE resolution or any other logic.  Never set in production.
# ---------------------------------------------------------------------------
is_trading_day() {
  local check_date="$1"
  # Delegate to the single source of truth: scripts/trading_calendar.py.
  # argv[1] = scripts dir (prepended to sys.path so the import resolves),
  # argv[2] = the date to check.
  "${PYTHON}" - "${ROOT_DIR}/scripts" "${check_date}" <<'PY'
import sys
from datetime import date

sys.path.insert(0, sys.argv[1])
from trading_calendar import is_trading_day

check = date.fromisoformat(sys.argv[2])
raise SystemExit(0 if is_trading_day(check) else 1)
PY
}

# Step 1: Determine CURRENT_ET_DATE — always the real current ET date, no rollback.
# ML_REPORT_FAKE_ET_DATE may override this for tests only.
if [[ -n "${ML_REPORT_FAKE_ET_DATE:-}" ]]; then
  CURRENT_ET_DATE="${ML_REPORT_FAKE_ET_DATE}"
else
  if ! CURRENT_ET_DATE="$(
    "${PYTHON}" -c "from datetime import datetime; from zoneinfo import ZoneInfo; print(datetime.now(ZoneInfo('America/New_York')).date().isoformat())"
  )"; then
    echo "[$(timestamp)] ERROR failed to determine current ET date" >> "${LOG_DIR}/report_delivery.log"
    exit 1
  fi
fi

# Step 2: Gate on CURRENT_ET_DATE before any date rollback.
# This is the critical fix: on Saturday the rollback hasn't happened yet, so
# is_trading_day(Saturday) correctly returns false and we exit before generating.
if [[ "${FORCE_SEND_LC}" != "true" ]]; then
  if ! is_trading_day "${CURRENT_ET_DATE}"; then
    echo "[$(timestamp)] INFO non-trading day (current ET date: ${CURRENT_ET_DATE}); skipping report send (set ML_REPORT_FORCE_SEND=true to override)" >> "${LOG_DIR}/report_delivery.log"
    exit 0
  fi
fi

# Step 3: Resolve REPORT_DATE using the existing logic (rollback is safe here
# because we have already confirmed today is a trading day at Step 2).
if [[ -z "${REPORT_DATE}" ]]; then
  if [[ "${REPORT_DATE_MODE}" == "auto" && "${SCHEDULE_MODE}" == "close" ]]; then
    REPORT_DATE_MODE="et_today"
  fi
  if ! REPORT_DATE="$(
    REPORT_DATE_MODE="${REPORT_DATE_MODE}" "${PYTHON}" - "${ROOT_DIR}/scripts" <<'PY'
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
import sys

sys.path.insert(0, sys.argv[1])
from trading_calendar import roll_back_to_trading_day

mode = os.getenv('REPORT_DATE_MODE', 'auto').strip().lower()
now = datetime.now(ZoneInfo('America/New_York'))
if mode == 'et_today':
    day = now.date()
else:
    day = now.date() if now.hour >= 16 else (now - timedelta(days=1)).date()
print(roll_back_to_trading_day(day).isoformat())
PY
  )"; then
    echo "[$(timestamp)] ERROR failed to determine report date" >> "${LOG_DIR}/report_delivery.log"
    exit 1
  fi
fi

# Step 4: If an explicit ML_REPORT_REPORT_DATE was provided, also verify it is
# a trading day. An operator who provides a nonsensical date (e.g. a holiday)
# must use FORCE_SEND=true to proceed.
if [[ -n "${EXPLICIT_REPORT_DATE}" && "${FORCE_SEND_LC}" != "true" ]]; then
  if ! is_trading_day "${EXPLICIT_REPORT_DATE}"; then
    echo "[$(timestamp)] INFO explicit report date ${EXPLICIT_REPORT_DATE} is a non-trading day; skipping (set ML_REPORT_FORCE_SEND=true to override)" >> "${LOG_DIR}/report_delivery.log"
    exit 0
  fi
fi

echo "[$(timestamp)] INFO current_et_date=${CURRENT_ET_DATE} report_date=${REPORT_DATE} schedule_mode=${SCHEDULE_MODE:-unset} date_mode=${REPORT_DATE_MODE}" >> "${LOG_DIR}/report_delivery.log"

if [[ "${FORCE_SEND_LC}" != "true" ]]; then
  if state_has_sent "${REPORT_DATE}" "${SCHEDULE_MODE_EFFECTIVE}"; then
    echo "[$(timestamp)] INFO report already sent for report_date=${REPORT_DATE} schedule_mode=${SCHEDULE_MODE_EFFECTIVE}; skipping" >> "${LOG_DIR}/report_delivery.log"
    exit 0
  fi
fi

if REPORT_OUTPUT="$("${PYTHON}" "${ROOT_DIR}/scripts/generate_daily_ml_report.py" --db "${PIVOT_DB_PATH}" --out-dir "${LOG_DIR}/reports" --report-date "${REPORT_DATE}" 2>&1)"; then
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

SEND_OUTPUT=""
if SEND_OUTPUT="$("${PYTHON}" "${ROOT_DIR}/scripts/send_daily_report.py" --report "${REPORT_PATH}" --db "${PIVOT_DB_PATH}" 2>&1)"; then
  printf '%s\n' "${SEND_OUTPUT}" >> "${LOG_DIR}/report_delivery.log"
  state_mark_sent "${REPORT_DATE}" "${SCHEDULE_MODE_EFFECTIVE}" "ok"
  echo "[$(timestamp)] DONE  daily_report_send" >> "${LOG_DIR}/report_delivery.log"
else
  printf '%s\n' "${SEND_OUTPUT}" >> "${LOG_DIR}/report_delivery.log"
  if printf '%s\n' "${SEND_OUTPUT}" | grep -qiE 'SMTPDataError 550|5\.4\.5 .*sending limit'; then
    state_mark_sent "${REPORT_DATE}" "${SCHEDULE_MODE_EFFECTIVE}" "rate_limited"
    echo "[$(timestamp)] WARN notification skipped (SMTP daily send limit reached)" >> "${LOG_DIR}/report_delivery.log"
    exit 0
  fi
  echo "[$(timestamp)] ERROR notification send failed" >> "${LOG_DIR}/report_delivery.log"
  exit 1
fi

# ---------------------------------------------------------------------------
# Levels data product (read-only add-on). We only reach here on a trading day
# after a successful report send, so it inherits this wrapper's trading-day gate
# and SMTP config and emails the validated SPY level map + track record. Fully
# guarded: any failure is logged and ignored — it can never affect the ML report
# above. Disable with LEVELS_PRODUCT_SKIP=1.
# ---------------------------------------------------------------------------
if [[ "${LEVELS_PRODUCT_SKIP:-0}" != "1" ]]; then
  if LEVELS_SKIP_LABELS="${LEVELS_SKIP_LABELS:-1}" LEVELS_CHANNEL="${LEVELS_CHANNEL:-email}" LEVELS_FORCE=1 \
       /bin/bash "${ROOT_DIR}/scripts/levels_product/run_levels_product_daily.sh" \
       >> "${LOG_DIR}/report_delivery.log" 2>&1; then
    echo "[$(timestamp)] DONE  levels_product email" >> "${LOG_DIR}/report_delivery.log"
  else
    echo "[$(timestamp)] WARN levels_product step failed (non-fatal)" >> "${LOG_DIR}/report_delivery.log"
  fi
fi
