# Ops Resilience

This document defines the operational resilience layer for PivotQuant:
- nightly backups
- retention policy
- weekly restore drills
- host health checks
- alert failover behavior

## 1) Nightly Backups

Script:
- `scripts/nightly_backup.py` (wrapper: `scripts/run_nightly_backup.sh`)

Backed up each run:
- `data/pivot_events.sqlite` (consistent snapshot via SQLite backup API)
- `data/models/` (`models.tar.gz`)
- `logs/reports/` (`reports.tar.gz`)

Output location:
- `${PIVOT_BACKUP_ROOT}/snapshots/YYYYMMDD_HHMMSS/`
- `${PIVOT_BACKUP_ROOT}/latest` symlink points to most recent snapshot

Concurrency/consistency protections:
- snapshots are built in a hidden staging directory (`.YYYYMMDD_HHMMSS.inprogress`) and atomically renamed when complete
- restore drill only considers snapshots that include all expected files and a complete manifest
- backup + restore drill share a lock file (`PIVOT_OPS_LOCK_FILE`) to prevent overlapping runs
- if lock wait exceeds `PIVOT_OPS_LOCK_TIMEOUT_SEC`, run is marked `skipped_lock_busy`

## 2) Retention

Configured in `.env`:
- `PIVOT_OPS_LOCK_FILE=/Users/tristanalejandro/PIVOT_QUANT/logs/ops_resilience.lock`
- `PIVOT_OPS_LOCK_TIMEOUT_SEC=300`
- `BACKUP_DAILY_KEEP=30`
- `BACKUP_WEEKLY_KEEP=8`

Effective policy:
- keep latest 30 daily snapshots
- plus one snapshot per ISO week for the latest 8 weeks

## 3) Restore Drill (Weekly)

Script:
- `scripts/backup_restore_drill.py` (wrapper: `scripts/run_backup_restore_drill.sh`)

Checks:
- restore latest DB backup to temp path
- `PRAGMA quick_check`
- key table row counts
- extract `models.tar.gz` and verify model manifest
- extract `reports.tar.gz` and verify report files
- choose latest complete snapshot (manifest + required archives + DB file)

State keys (SQLite `ops_status`):
- `backup_restore_last_status`
- `backup_restore_last_run_ms`
- `backup_restore_last_snapshot`
- `backup_restore_last_error`

## 4) Alert Failover

Failover behavior implemented in:
- `scripts/send_daily_report.py`
- `scripts/health_alert_watchdog.py`

If email fails with Gmail auth/rate-limit class errors (`535`, `550/5.4.5`, `5.7.8`), the notifier automatically attempts fallback channels.

Config:
- `ML_REPORT_FAILOVER_CHANNELS=webhook,imessage`
- `ML_ALERT_FAILOVER_CHANNELS=webhook,imessage`
- webhook/url and iMessage recipients must be configured.

## 5) Host Health Checks

Script:
- `scripts/host_health_check.py` (wrapper: `scripts/run_host_health_check.sh`)

Checks:
- disk free percent
- DB file size and growth/day
- launchd state/runs/last exit code for core PivotQuant agents

State keys:
- `host_health_last_status`
- `host_health_last_run_ms`
- `host_health_disk_free_pct`
- `host_health_db_size_bytes`
- `host_health_db_growth_mb_per_day`
- `host_health_warn_count`
- `host_health_crit_count`
- `host_health_last_error`

## 6) LaunchAgents

Installer:
- `scripts/install_ops_resilience_launch_agents.sh`

Installs:
- `com.pivotquant.nightly_backup`
- `com.pivotquant.restore_drill`
- `com.pivotquant.host_health`

Uninstall:
- `scripts/uninstall_ops_resilience_launch_agents.sh`

## 7) Validation Commands

```bash
bash scripts/run_nightly_backup.sh
bash scripts/run_backup_restore_drill.sh
bash scripts/run_host_health_check.sh
bash scripts/launch_agent_status.sh
```

## References

- SQLite backup API guidance: https://www.sqlite.org/backup.html
- CISA ransomware guide (offline backups + periodic restoration testing): https://www.cisa.gov/stopransomware/ransomware-guide
- AWS backup and restore best practices: https://docs.aws.amazon.com/prescriptive-guidance/latest/backup-recovery/building-blocks.html
- Google Cloud backup/recovery best practices: https://cloud.google.com/architecture/disaster-recovery
