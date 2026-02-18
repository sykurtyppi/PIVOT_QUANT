# Remote 24/7 Dashboard Setup (MacBook Air host -> MacBook Pro client)

This keeps the stack running on the host (Air) and makes the dashboard reachable from another Mac on the same LAN.

## 1) One-time host setup

```bash
cd /Users/tristanalejandro/PIVOT_QUANT
bash scripts/install_launch_agent.sh
```

What this installs:

- LaunchAgent: `com.pivotquant.dashboard`
- Runner: `server/run_persistent_stack.sh` -> `server/run_all.sh`
- Keep-awake: `caffeinate` while stack runs
- External binds: dashboard `0.0.0.0:3000`, ML `0.0.0.0:5003`

## 2) Verify host health

```bash
cd /Users/tristanalejandro/PIVOT_QUANT
bash scripts/launch_agent_status.sh
bash scripts/verify_host_ready.sh
```

Live logs:

```bash
tail -f logs/launchd.out.log logs/launchd.err.log logs/dashboard.log logs/ml_server.log
```

## 3) Open from the client Mac

- `http://<AIR_LAN_IP>:3000`
- `http://<AIR_LOCAL_HOSTNAME>.local:3000`

## 4) If launchctl reports `Bootstrap failed: 5`

Run this exactly on the host:

```bash
cd /Users/tristanalejandro/PIVOT_QUANT
UIDN="$(id -u)"
LABEL="com.pivotquant.dashboard"
PLIST="$HOME/Library/LaunchAgents/${LABEL}.plist"

launchctl bootout "gui/${UIDN}/${LABEL}" 2>/dev/null || true
launchctl bootout "gui/${UIDN}" "${PLIST}" 2>/dev/null || true
launchctl remove "${LABEL}" 2>/dev/null || true

bash scripts/install_launch_agent.sh
launchctl print "gui/${UIDN}/${LABEL}"
```

## 5) Manual fallback (no LaunchAgent)

```bash
cd /Users/tristanalejandro/PIVOT_QUANT
source .venv/bin/activate
bash server/run_persistent_stack.sh
```

## 6) Host OS settings

- Keep host plugged into power.
- Keep user logged in (LaunchAgent is user-session scoped).
- Allow incoming for `node` and `python3` in Firewall if prompted.
- Disable disk sleep if you observe stalls.

## 7) Remove services

```bash
cd /Users/tristanalejandro/PIVOT_QUANT
bash scripts/uninstall_launch_agent.sh
bash scripts/uninstall_retrain_launch_agent.sh
bash scripts/uninstall_daily_report_launch_agent.sh
bash scripts/uninstall_health_alert_launch_agent.sh
bash scripts/uninstall_ops_resilience_launch_agents.sh
```

## Notes on ML "always learning"

24/7 uptime keeps inference/services alive. It does not retrain by itself.

To schedule periodic retraining:

```bash
cd /Users/tristanalejandro/PIVOT_QUANT
bash scripts/install_retrain_launch_agent.sh
```

Retrain cadence is every 6 hours via `scripts/run_retrain_cycle.sh`.

## 8) Daily report delivery (email / iMessage / webhook)

Use a dedicated daily LaunchAgent so you get one clean email on schedule (instead of every retrain).

Create `/Users/tristanalejandro/PIVOT_QUANT/.env` on the host:

```bash
# choose one or more: email,imessage,webhook
ML_REPORT_NOTIFY_CHANNELS=email

# avoid 6h retrain spam if scheduled agent is enabled
ML_REPORT_NOTIFY_ON_RETRAIN=false
ML_REPORT_EMAIL_STYLE=compact
ML_REPORT_ANOMALY_LIMIT=6

# email settings (SMTP)
ML_REPORT_EMAIL_TO=you@example.com
ML_REPORT_EMAIL_FROM=you@example.com
ML_REPORT_SMTP_HOST=smtp.gmail.com
ML_REPORT_SMTP_PORT=587
ML_REPORT_SMTP_USER=you@example.com
ML_REPORT_SMTP_PASS=your_app_password
ML_REPORT_SMTP_USE_TLS=true

# include useful runtime logs in the email body
ML_REPORT_INCLUDE_LOG_TAILS=true
ML_REPORT_LOG_TAIL_LINES=80
ML_COST_SPREAD_BPS=0.8
ML_COST_SLIPPAGE_BPS=0.4
ML_COST_COMMISSION_BPS=0.1
# session-aware staleness thresholds (regular session hours)
ML_STALENESS_WARN_SESSION_HOURS=13
ML_STALENESS_KILL_SESSION_HOURS=19.5

# optional iMessage recipients (comma-separated phone/email)
# ML_REPORT_IMESSAGE_TO=+15551234567

# optional webhook target
# ML_REPORT_WEBHOOK_URL=https://your-webhook-endpoint
# auto-failover if email hits Gmail auth/rate-limit errors
ML_REPORT_FAILOVER_CHANNELS=webhook,imessage

# immediate downtime alerts (state-change only by default)
ML_ALERT_NOTIFY_CHANNELS=email
ML_ALERT_ML_HEALTH_URL=http://127.0.0.1:5003/health
ML_ALERT_COLLECTOR_HEALTH_URL=http://127.0.0.1:5004/health
ML_ALERT_CHECK_INTERVAL_SEC=60
ML_ALERT_TIMEOUT_SEC=4
ML_ALERT_REPEAT_MIN=0
# optional separate recipients/sender
# ML_ALERT_EMAIL_TO=you@example.com
# ML_ALERT_EMAIL_FROM=you@example.com
# optional alert-only webhook and failover order
# ML_ALERT_WEBHOOK_URL=https://your-alert-webhook
ML_ALERT_FAILOVER_CHANNELS=webhook,imessage
```

Install scheduled delivery:

```bash
cd /Users/tristanalejandro/PIVOT_QUANT
# modes: close (17:10 local weekdays), morning (08:05 local weekdays), both
bash scripts/install_daily_report_launch_agent.sh close
```

Note: schedule uses Mac host local timezone. You can override times via `.env`:
`ML_REPORT_MORNING_HOUR`, `ML_REPORT_MORNING_MINUTE`, `ML_REPORT_CLOSE_HOUR`, `ML_REPORT_CLOSE_MINUTE`.

Test once manually:

```bash
cd /Users/tristanalejandro/PIVOT_QUANT
source .venv/bin/activate
npm run ml:notify-report
```

Logs:

```bash
tail -f logs/report_delivery.log logs/daily_report.launchd.err.log
```

## 9) Immediate downtime paging (ML / collector)

Install always-on watchdog alerts (runs every minute by default):

```bash
cd /Users/tristanalejandro/PIVOT_QUANT
bash scripts/install_health_alert_launch_agent.sh
```

Manual dry-run check:

```bash
cd /Users/tristanalejandro/PIVOT_QUANT
source .venv/bin/activate
npm run ml:health-alert:check
```

Watchdog logs:

```bash
tail -f logs/health_alert.log logs/health_alert.launchd.err.log
```

## 10) Backups + restore drill + host health checks

Add to `.env` on host:

```bash
PIVOT_BACKUP_ROOT=/Users/tristanalejandro/PIVOT_QUANT/backups
BACKUP_DAILY_KEEP=30
BACKUP_WEEKLY_KEEP=8
BACKUP_HOUR=22
BACKUP_MINUTE=20
RESTORE_DRILL_WEEKDAY=0
RESTORE_DRILL_HOUR=23
RESTORE_DRILL_MINUTE=0
HOST_HEALTH_CHECK_INTERVAL_SEC=900
HOST_HEALTH_DISK_WARN_PCT=15
HOST_HEALTH_DISK_CRIT_PCT=8
HOST_HEALTH_DB_GROWTH_WARN_MB=2048
HOST_HEALTH_DB_GROWTH_CRIT_MB=4096
HOST_HEALTH_RESTART_WARN_DELTA=5
```

Install ops LaunchAgents:

```bash
cd /Users/tristanalejandro/PIVOT_QUANT
bash scripts/install_ops_resilience_launch_agents.sh
```

Manual validation:

```bash
cd /Users/tristanalejandro/PIVOT_QUANT
source .venv/bin/activate
bash scripts/run_nightly_backup.sh
bash scripts/run_backup_restore_drill.sh
bash scripts/run_host_health_check.sh
```

Ops logs:

```bash
tail -f logs/backup.log logs/restore_drill.log logs/host_health.log
```
