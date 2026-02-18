# PivotQuant Incident Runbook

Use this when live ops degrade or fail.

## A) First 3 Commands

```bash
cd /Users/tristanalejandro/PIVOT_QUANT
bash scripts/verify_host_ready.sh
curl -fsS http://127.0.0.1:5003/health && curl -fsS http://127.0.0.1:5004/health
```

## B) Rapid Logs

```bash
tail -n 120 logs/retrain.log logs/ml_server.log logs/live_collector.log logs/health_alert.log
tail -n 120 logs/backup.log logs/restore_drill.log logs/host_health.log
```

## C) Common Failures

1. ML server unhealthy or stale:
- Run retrain cycle:
  - `bash scripts/run_retrain_cycle.sh`
- Verify active manifest:
  - `python3 scripts/model_governance.py --models-dir data/models status`

2. Collector not scoring:
- Check collector health:
  - `curl -fsS http://127.0.0.1:5004/health`
- Restart stack:
  - `launchctl kickstart -k gui/$(id -u)/com.pivotquant.dashboard`

3. Daily report email failure (Gmail 535/550):
- Confirm fallback channels:
  - `grep -nE '^ML_REPORT_FAILOVER_CHANNELS=|^ML_REPORT_WEBHOOK_URL=|^ML_REPORT_IMESSAGE_TO=' .env`
- Dry-run notifier:
  - `python3 scripts/send_daily_report.py --report logs/reports/ml_daily_latest.md --db data/pivot_events.sqlite --dry-run`

4. Backup or restore drill failure:
- Run manual backup:
  - `bash scripts/run_nightly_backup.sh`
- Run manual drill:
  - `bash scripts/run_backup_restore_drill.sh`

## D) Rollback Command (Model)

Rollback active manifest to previous:

```bash
python3 scripts/model_governance.py --models-dir data/models rollback
curl -sf -X POST http://127.0.0.1:5003/reload
```

Rollback to a specific version:

```bash
python3 scripts/model_governance.py --models-dir data/models rollback --to-version v010
curl -sf -X POST http://127.0.0.1:5003/reload
```

## E) Post-Incident Checklist

1. Capture root cause in `logs/` with timestamp.
2. Confirm health endpoints return `ok`.
3. Verify next scheduled retrain and daily report agents are loaded.
4. If backup was involved, run restore drill once before close.
