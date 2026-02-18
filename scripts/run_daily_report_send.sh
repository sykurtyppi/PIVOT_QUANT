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

if [ -x "${ROOT_DIR}/.venv/bin/python3" ]; then
  PYTHON="${ROOT_DIR}/.venv/bin/python3"
elif [ -x "${ROOT_DIR}/.venv/bin/python" ]; then
  PYTHON="${ROOT_DIR}/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON="$(command -v python3)"
else
  echo "[$(timestamp)] ERROR daily_report_send: python3 not found" >> "${LOG_DIR}/report_delivery.log"
  exit 1
fi

if ! PYTHON_INFO="$("${PYTHON}" -c "import sys; assert sys.version_info >= (3, 10), f'Python {sys.version.split()[0]} too old; require >=3.10'; print(f'{sys.executable} {sys.version.split()[0]}')" 2>&1)"; then
  echo "[$(timestamp)] ERROR daily_report_send: ${PYTHON_INFO}" >> "${LOG_DIR}/report_delivery.log"
  exit 1
fi

acquire_lock

PIVOT_DB_PATH="${PIVOT_DB:-${ROOT_DIR}/data/pivot_events.sqlite}"
REPORT_OUTPUT=""
REPORT_PATH=""
REPORT_DATE="${ML_REPORT_REPORT_DATE:-}"
REPORT_DATE_MODE="${ML_REPORT_REPORT_DATE_MODE:-auto}"
SCHEDULE_MODE="${ML_REPORT_SCHEDULE_MODE:-}"
SCHEDULE_MODE_EFFECTIVE="${SCHEDULE_MODE:-manual}"
FORCE_SEND="${ML_REPORT_FORCE_SEND:-false}"
FORCE_SEND_LC="$(printf '%s' "${FORCE_SEND}" | tr '[:upper:]' '[:lower:]')"

echo "[$(timestamp)] START daily_report_send (${PYTHON_INFO})" >> "${LOG_DIR}/report_delivery.log"

if [[ -z "${REPORT_DATE}" ]]; then
  if [[ "${REPORT_DATE_MODE}" == "auto" && "${SCHEDULE_MODE}" == "close" ]]; then
    REPORT_DATE_MODE="et_today"
  fi
  if ! REPORT_DATE="$(
    REPORT_DATE_MODE="${REPORT_DATE_MODE}" "${PYTHON}" -c "from datetime import datetime, timedelta; from zoneinfo import ZoneInfo; import os
mode = os.getenv('REPORT_DATE_MODE', 'auto').strip().lower()
now = datetime.now(ZoneInfo('America/New_York'))
if mode == 'et_today':
    day = now.date()
else:
    day = now.date() if now.hour >= 16 else (now - timedelta(days=1)).date()
while day.weekday() >= 5:
    day = day - timedelta(days=1)
print(day.isoformat())"
  )"; then
    echo "[$(timestamp)] ERROR failed to determine report date" >> "${LOG_DIR}/report_delivery.log"
    exit 1
  fi
fi

echo "[$(timestamp)] INFO report_date=${REPORT_DATE} schedule_mode=${SCHEDULE_MODE:-unset} date_mode=${REPORT_DATE_MODE}" >> "${LOG_DIR}/report_delivery.log"

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
