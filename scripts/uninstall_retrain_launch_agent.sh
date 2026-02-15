#!/usr/bin/env bash
set -euo pipefail

LABEL="com.pivotquant.retrain"
PLIST_PATH="${HOME}/Library/LaunchAgents/${LABEL}.plist"
UID_NUM="$(id -u)"
TARGET="gui/${UID_NUM}/${LABEL}"

if launchctl print "${TARGET}" >/dev/null 2>&1; then
  launchctl bootout "gui/${UID_NUM}" "${PLIST_PATH}" >/dev/null 2>&1 || true
  launchctl disable "${TARGET}" >/dev/null 2>&1 || true
  echo "Stopped ${LABEL}"
else
  echo "${LABEL} is not currently loaded."
fi

if [[ -f "${PLIST_PATH}" ]]; then
  rm -f "${PLIST_PATH}"
  echo "Removed ${PLIST_PATH}"
fi
