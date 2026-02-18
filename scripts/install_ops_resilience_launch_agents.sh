#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
PLIST_DIR="${HOME}/Library/LaunchAgents"
UID_NUM="$(id -u)"

BACKUP_LABEL="com.pivotquant.nightly_backup"
DRILL_LABEL="com.pivotquant.restore_drill"
HOST_LABEL="com.pivotquant.host_health"

BACKUP_HOUR="${BACKUP_HOUR:-22}"
BACKUP_MINUTE="${BACKUP_MINUTE:-20}"
DRILL_WEEKDAY="${RESTORE_DRILL_WEEKDAY:-0}"  # 0 = Sunday
DRILL_HOUR="${RESTORE_DRILL_HOUR:-23}"
DRILL_MINUTE="${RESTORE_DRILL_MINUTE:-0}"
HOST_INTERVAL="${HOST_HEALTH_CHECK_INTERVAL_SEC:-900}"

mkdir -p "${LOG_DIR}" "${PLIST_DIR}"

install_plist() {
  local label="$1"
  local plist_path="$2"
  local kickstart_now="${3:-true}"

  chmod 644 "${plist_path}"
  plutil -lint "${plist_path}" >/dev/null
  xattr -dr com.apple.quarantine "${plist_path}" "${ROOT_DIR}" >/dev/null 2>&1 || true

  launchctl bootout "gui/${UID_NUM}/${label}" >/dev/null 2>&1 || true
  launchctl bootout "gui/${UID_NUM}" "${plist_path}" >/dev/null 2>&1 || true
  launchctl remove "${label}" >/dev/null 2>&1 || true
  launchctl disable "gui/${UID_NUM}/${label}" >/dev/null 2>&1 || true
  launchctl enable "gui/${UID_NUM}/${label}" >/dev/null 2>&1 || true

  if ! launchctl bootstrap "gui/${UID_NUM}" "${plist_path}" >/dev/null 2>&1; then
    launchctl bootout "gui/${UID_NUM}/${label}" >/dev/null 2>&1 || true
    launchctl bootout "gui/${UID_NUM}" "${plist_path}" >/dev/null 2>&1 || true
    launchctl remove "${label}" >/dev/null 2>&1 || true
    launchctl enable "gui/${UID_NUM}/${label}" >/dev/null 2>&1 || true
    launchctl bootstrap "gui/${UID_NUM}" "${plist_path}"
  fi

  if [[ "${kickstart_now}" == "true" ]]; then
    launchctl kickstart -k "gui/${UID_NUM}/${label}" >/dev/null 2>&1 || true
  fi
  launchctl print "gui/${UID_NUM}/${label}" >/dev/null
}

BACKUP_PLIST="${PLIST_DIR}/${BACKUP_LABEL}.plist"
cat > "${BACKUP_PLIST}" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>${BACKUP_LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${ROOT_DIR}/scripts/run_nightly_backup.sh</string>
  </array>
  <key>WorkingDirectory</key><string>${ROOT_DIR}</string>
  <key>RunAtLoad</key><false/>
  <key>StartCalendarInterval</key>
  <array>
    <dict><key>Hour</key><integer>${BACKUP_HOUR}</integer><key>Minute</key><integer>${BACKUP_MINUTE}</integer></dict>
  </array>
  <key>StandardOutPath</key><string>${LOG_DIR}/backup.launchd.out.log</string>
  <key>StandardErrorPath</key><string>${LOG_DIR}/backup.launchd.err.log</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
</dict>
</plist>
PLIST

DRILL_PLIST="${PLIST_DIR}/${DRILL_LABEL}.plist"
cat > "${DRILL_PLIST}" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>${DRILL_LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${ROOT_DIR}/scripts/run_backup_restore_drill.sh</string>
  </array>
  <key>WorkingDirectory</key><string>${ROOT_DIR}</string>
  <key>RunAtLoad</key><false/>
  <key>StartCalendarInterval</key>
  <array>
    <dict>
      <key>Weekday</key><integer>${DRILL_WEEKDAY}</integer>
      <key>Hour</key><integer>${DRILL_HOUR}</integer>
      <key>Minute</key><integer>${DRILL_MINUTE}</integer>
    </dict>
  </array>
  <key>StandardOutPath</key><string>${LOG_DIR}/restore_drill.launchd.out.log</string>
  <key>StandardErrorPath</key><string>${LOG_DIR}/restore_drill.launchd.err.log</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
</dict>
</plist>
PLIST

HOST_PLIST="${PLIST_DIR}/${HOST_LABEL}.plist"
cat > "${HOST_PLIST}" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>${HOST_LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${ROOT_DIR}/scripts/run_host_health_check.sh</string>
  </array>
  <key>WorkingDirectory</key><string>${ROOT_DIR}</string>
  <key>RunAtLoad</key><true/>
  <key>StartInterval</key><integer>${HOST_INTERVAL}</integer>
  <key>StandardOutPath</key><string>${LOG_DIR}/host_health.launchd.out.log</string>
  <key>StandardErrorPath</key><string>${LOG_DIR}/host_health.launchd.err.log</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
</dict>
</plist>
PLIST

chmod 755 \
  "${ROOT_DIR}/scripts/run_nightly_backup.sh" \
  "${ROOT_DIR}/scripts/run_backup_restore_drill.sh" \
  "${ROOT_DIR}/scripts/run_host_health_check.sh"

install_plist "${BACKUP_LABEL}" "${BACKUP_PLIST}" "true"
install_plist "${DRILL_LABEL}" "${DRILL_PLIST}" "false"
install_plist "${HOST_LABEL}" "${HOST_PLIST}" "true"

echo "Installed ops resilience LaunchAgents:"
echo "  ${BACKUP_LABEL} (daily ${BACKUP_HOUR}:$(printf '%02d' "${BACKUP_MINUTE}"))"
echo "  ${DRILL_LABEL} (weekday ${DRILL_WEEKDAY} ${DRILL_HOUR}:$(printf '%02d' "${DRILL_MINUTE}"))"
echo "  ${HOST_LABEL} (interval ${HOST_INTERVAL}s)"
echo "Use:"
echo "  launchctl print gui/${UID_NUM}/${BACKUP_LABEL}"
echo "  launchctl print gui/${UID_NUM}/${DRILL_LABEL}"
echo "  launchctl print gui/${UID_NUM}/${HOST_LABEL}"
echo "Note: restore drill agent is installed without immediate kickstart; it runs on schedule."
