#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UID_NUM="$(id -u)"
LOG_DIR="${ROOT_DIR}/logs"

for LABEL in com.pivotquant.dashboard com.pivotquant.retrain com.pivotquant.daily_report; do
  TARGET="gui/${UID_NUM}/${LABEL}"
  echo "LaunchAgent target: ${TARGET}"
  if launchctl print "${TARGET}" >/dev/null 2>&1; then
    launchctl print "${TARGET}" | sed -n '1,90p'
  else
    echo "Not loaded."
  fi
  echo
done

echo
echo "Recent logs:"
for file in \
  "${LOG_DIR}/launchd.out.log" \
  "${LOG_DIR}/launchd.err.log" \
  "${LOG_DIR}/retrain.launchd.out.log" \
  "${LOG_DIR}/retrain.launchd.err.log" \
  "${LOG_DIR}/daily_report.launchd.out.log" \
  "${LOG_DIR}/daily_report.launchd.err.log" \
  "${LOG_DIR}/report_delivery.log"; do
  if [[ -f "${file}" ]]; then
    echo "--- ${file}"
    tail -n 30 "${file}"
  fi
done
