#!/usr/bin/env bash
set -euo pipefail

UID_NUM="$(id -u)"
PLIST_DIR="${HOME}/Library/LaunchAgents"
LABELS=(
  "com.pivotquant.nightly_backup"
  "com.pivotquant.restore_drill"
  "com.pivotquant.host_health"
)

for label in "${LABELS[@]}"; do
  target="gui/${UID_NUM}/${label}"
  plist_path="${PLIST_DIR}/${label}.plist"
  launchctl bootout "${target}" >/dev/null 2>&1 || true
  launchctl bootout "gui/${UID_NUM}" "${plist_path}" >/dev/null 2>&1 || true
  launchctl remove "${label}" >/dev/null 2>&1 || true
  launchctl disable "${target}" >/dev/null 2>&1 || true
  rm -f "${plist_path}"
  echo "Uninstalled ${label}"
done
