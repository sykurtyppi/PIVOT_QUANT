#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LABEL="com.pivotquant.dashboard"
PLIST_PATH="${HOME}/Library/LaunchAgents/${LABEL}.plist"
LOG_DIR="${ROOT_DIR}/logs"
UID_NUM="$(id -u)"
TARGET="gui/${UID_NUM}/${LABEL}"
STARTUP_TIMEOUT_SEC="${STARTUP_TIMEOUT_SEC:-120}"

mkdir -p "${LOG_DIR}" "${HOME}/Library/LaunchAgents"

write_plist() {
  cat > "${PLIST_PATH}" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${LABEL}</string>

  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${ROOT_DIR}/server/run_persistent_stack.sh</string>
  </array>

  <key>WorkingDirectory</key>
  <string>${ROOT_DIR}</string>

  <key>RunAtLoad</key>
  <true/>

  <key>KeepAlive</key>
  <true/>

  <key>StandardOutPath</key>
  <string>${LOG_DIR}/launchd.out.log</string>

  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/launchd.err.log</string>

  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    <key>STARTUP_TIMEOUT_SEC</key>
    <string>${STARTUP_TIMEOUT_SEC}</string>
  </dict>
</dict>
</plist>
PLIST
}

clear_existing_service() {
  launchctl bootout "${TARGET}" >/dev/null 2>&1 || true
  launchctl bootout "gui/${UID_NUM}" "${PLIST_PATH}" >/dev/null 2>&1 || true
  launchctl remove "${LABEL}" >/dev/null 2>&1 || true
  launchctl disable "${TARGET}" >/dev/null 2>&1 || true
}

bootstrap_service() {
  local output=""
  if output="$(launchctl bootstrap "gui/${UID_NUM}" "${PLIST_PATH}" 2>&1)"; then
    return 0
  fi
  echo "${output}" >&2
  return 1
}

write_plist

chmod 644 "${PLIST_PATH}"
chmod 755 "${ROOT_DIR}/server/run_persistent_stack.sh" "${ROOT_DIR}/server/run_all.sh"
xattr -dr com.apple.quarantine "${PLIST_PATH}" "${ROOT_DIR}" >/dev/null 2>&1 || true
plutil -lint "${PLIST_PATH}" >/dev/null

clear_existing_service
launchctl enable "${TARGET}" >/dev/null 2>&1 || true

if ! bootstrap_service; then
  echo "First bootstrap failed; retrying after state cleanup..."
  clear_existing_service
  launchctl enable "${TARGET}" >/dev/null 2>&1 || true
  bootstrap_service
fi

launchctl kickstart -k "${TARGET}" >/dev/null 2>&1 || true

if ! launchctl print "${TARGET}" >/dev/null 2>&1; then
  echo "[ERROR] LaunchAgent did not load: ${TARGET}" >&2
  exit 1
fi

echo "Installed and started ${LABEL}"
echo "Plist: ${PLIST_PATH}"
echo "Use these commands:"
echo "  launchctl print ${TARGET}"
echo "  tail -f ${LOG_DIR}/launchd.out.log ${LOG_DIR}/launchd.err.log"
