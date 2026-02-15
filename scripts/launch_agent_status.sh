#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LABEL="com.pivotquant.dashboard"
UID_NUM="$(id -u)"
TARGET="gui/${UID_NUM}/${LABEL}"
LOG_DIR="${ROOT_DIR}/logs"

echo "LaunchAgent target: ${TARGET}"
echo

if launchctl print "${TARGET}" >/dev/null 2>&1; then
  launchctl print "${TARGET}" | sed -n '1,120p'
else
  echo "Not loaded."
fi

echo
echo "Recent logs:"
for file in "${LOG_DIR}/launchd.out.log" "${LOG_DIR}/launchd.err.log"; do
  if [[ -f "${file}" ]]; then
    echo "--- ${file}"
    tail -n 30 "${file}"
  fi
done
