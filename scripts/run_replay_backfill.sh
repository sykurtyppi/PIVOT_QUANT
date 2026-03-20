#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

PYTHON="${PYTHON:-./.venv/bin/python3}"
LIVE_DB="${PIVOT_DB:-data/pivot_events.sqlite}"
REPLAY_DB="${REPLAY_DB:-}"
START_DATE="${START_DATE:-}"
END_DATE="${END_DATE:-}"
PORT="${REPLAY_ML_PORT:-5103}"
LIMIT="${REPLAY_LIMIT:-500000}"
BATCH_SIZE="${REPLAY_BATCH_SIZE:-16}"
TIMEOUT_SEC="${REPLAY_TIMEOUT_SEC:-20}"
MAX_ATTEMPTS="${REPLAY_MAX_ATTEMPTS:-3}"
SYMBOLS="${REPLAY_SYMBOLS:-}"
LOG_PATH="${REPLAY_LOG_PATH:-logs/ml_server_replay.log}"

usage() {
  cat <<'USAGE'
Usage:
  scripts/run_replay_backfill.sh --start-date YYYY-MM-DD --end-date YYYY-MM-DD [options]

Options:
  --start-date DATE     Inclusive UTC date (required)
  --end-date DATE       Inclusive UTC date (required)
  --replay-db PATH      Replay DB path (default: data/pivot_events_replay_<start>_<end>.sqlite)
  --live-db PATH        Live DB path (default: $PIVOT_DB or data/pivot_events.sqlite)
  --port N              Replay ML server port (default: 5103)
  --limit N             Max events (default: 500000)
  --batch-size N        Scorer batch size (default: 16)
  --timeout-sec N       Scorer timeout sec (default: 20)
  --max-attempts N      Scorer max attempts (default: 3)
  --symbols CSV         Optional symbol filter
  --log PATH            Replay ML server log path (default: logs/ml_server_replay.log)
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start-date) START_DATE="${2:-}"; shift 2 ;;
    --end-date) END_DATE="${2:-}"; shift 2 ;;
    --replay-db) REPLAY_DB="${2:-}"; shift 2 ;;
    --live-db) LIVE_DB="${2:-}"; shift 2 ;;
    --port) PORT="${2:-}"; shift 2 ;;
    --limit) LIMIT="${2:-}"; shift 2 ;;
    --batch-size) BATCH_SIZE="${2:-}"; shift 2 ;;
    --timeout-sec) TIMEOUT_SEC="${2:-}"; shift 2 ;;
    --max-attempts) MAX_ATTEMPTS="${2:-}"; shift 2 ;;
    --symbols) SYMBOLS="${2:-}"; shift 2 ;;
    --log) LOG_PATH="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "${START_DATE}" || -z "${END_DATE}" ]]; then
  echo "ERROR: --start-date and --end-date are required." >&2
  usage
  exit 2
fi

if [[ -z "${REPLAY_DB}" ]]; then
  REPLAY_DB="data/pivot_events_replay_${START_DATE//-/}_${END_DATE//-/}.sqlite"
fi

# Hard safety rail: refuse suspicious production-like replay target paths.
if [[ "${REPLAY_DB}" == *"pivot_events.sqlite" && "${REPLAY_DB}" != *"replay"* ]]; then
  echo "ERROR: Replay target looks like production DB. Aborting." >&2
  exit 1
fi

mkdir -p "$(dirname "${LOG_PATH}")"

LIVE_DB_ABS="$(cd "$(dirname "${LIVE_DB}")" && pwd)/$(basename "${LIVE_DB}")"
REPLAY_DB_ABS="$(cd "$(dirname "${REPLAY_DB}")" && pwd)/$(basename "${REPLAY_DB}")"
if [[ "${LIVE_DB_ABS}" == "${REPLAY_DB_ABS}" ]]; then
  echo "ERROR: Replay DB path matches live DB path. Aborting." >&2
  exit 1
fi

echo "[replay] backing up ${LIVE_DB} -> ${REPLAY_DB}"
sqlite3 "${LIVE_DB}" ".backup '${REPLAY_DB}'"

echo "[replay] snapshotting prediction_log and deleting target-range rows"
sqlite3 "${REPLAY_DB}" "
DROP TABLE IF EXISTS prediction_log_pre_replay;
CREATE TABLE prediction_log_pre_replay AS SELECT * FROM prediction_log;
DELETE FROM prediction_log
WHERE event_id IN (
  SELECT event_id
  FROM touch_events
  WHERE ts_event >= (strftime('%s','${START_DATE}') * 1000)
    AND ts_event <  (strftime('%s','${END_DATE}','+1 day') * 1000)
);
"

replay_pid=""
cleanup() {
  if [[ -n "${replay_pid}" ]] && kill -0 "${replay_pid}" 2>/dev/null; then
    kill "${replay_pid}" 2>/dev/null || true
    wait "${replay_pid}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "[replay] starting isolated ML server on :${PORT}"
ML_SERVER_PORT="${PORT}" \
PIVOT_DB="${REPLAY_DB}" \
PREDICTION_LOG_DB="${REPLAY_DB}" \
ML_ANALOG_DB="${REPLAY_DB}" \
"${PYTHON}" -m uvicorn server.ml_server:app --host 127.0.0.1 --port "${PORT}" \
  > "${LOG_PATH}" 2>&1 &
replay_pid="$!"

echo "[replay] waiting for health..."
ready="0"
for _ in $(seq 1 90); do
  if curl -fsS "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1; then
    ready="1"
    break
  fi
  sleep 1
done
if [[ "${ready}" != "1" ]]; then
  echo "ERROR: replay ML server failed to become healthy on :${PORT}" >&2
  tail -n 80 "${LOG_PATH}" >&2 || true
  exit 1
fi

common_args=(
  --db "${REPLAY_DB}"
  --score-url "http://127.0.0.1:${PORT}/score"
  --start-date "${START_DATE}"
  --end-date "${END_DATE}"
  --limit "${LIMIT}"
  --preview
)
if [[ -n "${SYMBOLS}" ]]; then
  common_args+=(--symbols "${SYMBOLS}")
fi

echo "[replay] dry run"
"${PYTHON}" scripts/score_unscored_touch_events.py "${common_args[@]}" --dry-run

echo "[replay] scoring"
"${PYTHON}" scripts/score_unscored_touch_events.py \
  "${common_args[@]}" \
  --batch-size "${BATCH_SIZE}" \
  --timeout-sec "${TIMEOUT_SEC}" \
  --max-attempts "${MAX_ATTEMPTS}" \
  --verify-after \
  --max-remaining 0

echo "[replay] post-run dry run"
"${PYTHON}" scripts/score_unscored_touch_events.py "${common_args[@]}" --dry-run

echo "[replay] complete"
