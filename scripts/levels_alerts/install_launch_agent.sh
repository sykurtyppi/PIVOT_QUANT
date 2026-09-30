#!/usr/bin/env bash
# Install (or uninstall) the level-alert watcher launch agent, which runs the
# watcher every ~60s (self-gating to RTH). Reproducible record of the setup.
#   install:   bash scripts/levels_alerts/install_launch_agent.sh
#   uninstall: bash scripts/levels_alerts/install_launch_agent.sh --uninstall
set -euo pipefail

LABEL="com.pivotquant.levels-alerts"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PLIST="${HOME}/Library/LaunchAgents/${LABEL}.plist"
DOMAIN="gui/$(id -u)"

if [[ "${1:-}" == "--uninstall" ]]; then
  launchctl bootout "${DOMAIN}/${LABEL}" 2>/dev/null || true
  rm -f "${PLIST}"
  echo "uninstalled ${LABEL}"
  exit 0
fi

mkdir -p "${HOME}/Library/LaunchAgents" "${ROOT}/logs"
cat > "${PLIST}" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>${LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${ROOT}/scripts/levels_alerts/run_watcher.sh</string>
  </array>
  <key>WorkingDirectory</key><string>${ROOT}</string>
  <key>StartInterval</key><integer>60</integer>
  <key>RunAtLoad</key><true/>
  <key>StandardOutPath</key><string>${ROOT}/logs/levels_alerts.out.log</string>
  <key>StandardErrorPath</key><string>${ROOT}/logs/levels_alerts.err.log</string>
  <key>EnvironmentVariables</key>
  <dict><key>PATH</key><string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string></dict>
</dict>
</plist>
PLIST

launchctl bootout "${DOMAIN}/${LABEL}" 2>/dev/null || true
launchctl bootstrap "${DOMAIN}" "${PLIST}"
echo "installed ${LABEL} (every 60s, self-gating to RTH). Uninstall: $0 --uninstall"
