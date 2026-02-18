#!/usr/bin/env python3
"""Host-level health checks for PivotQuant ops resilience."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = ROOT / ".env"
DEFAULT_LOG_FILE = ROOT / "logs" / "host_health.log"
DEFAULT_STATE_FILE = ROOT / "logs" / "host_health_state.json"
DEFAULT_DB = Path(os.getenv("PIVOT_DB", str(ROOT / "data" / "pivot_events.sqlite")))

DEFAULT_LABELS = [
    "com.pivotquant.dashboard",
    "com.pivotquant.retrain",
    "com.pivotquant.daily_report",
    "com.pivotquant.health_alert",
    "com.pivotquant.nightly_backup",
    "com.pivotquant.restore_drill",
    "com.pivotquant.host_health",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run host health checks.")
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_FILE))
    parser.add_argument("--log-file", default=str(DEFAULT_LOG_FILE))
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE))
    parser.add_argument("--db-path", default=str(DEFAULT_DB))
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            os.environ.setdefault(key, value)


def log_line(path: Path, message: str) -> None:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{stamp}] {message}"
    print(line)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def now_ms() -> int:
    return int(time.time() * 1000)


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)


def set_ops_status(db_path: Path, pairs: dict[str, str]) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ops_status (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at INTEGER NOT NULL
            )
            """
        )
        ts = now_ms()
        for key, value in pairs.items():
            conn.execute(
                """
                INSERT INTO ops_status(key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE
                  SET value = excluded.value,
                      updated_at = excluded.updated_at
                """,
                (key, value, ts),
            )
        conn.commit()
    finally:
        conn.close()


def parse_launchctl(label: str, uid: int) -> dict[str, Any]:
    target = f"gui/{uid}/{label}"
    proc = subprocess.run(
        ["launchctl", "print", target],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        return {
            "label": label,
            "loaded": False,
            "state": "not_loaded",
            "runs": 0,
            "last_exit_code": "unknown",
        }

    out = proc.stdout
    state = re.search(r"state = ([^\n]+)", out)
    runs = re.search(r"runs = ([^\n]+)", out)
    exit_code = re.search(r"last exit code = ([^\n]+)", out)
    return {
        "label": label,
        "loaded": True,
        "state": state.group(1).strip() if state else "unknown",
        "runs": int(runs.group(1).strip()) if runs and runs.group(1).strip().isdigit() else 0,
        "last_exit_code": exit_code.group(1).strip() if exit_code else "unknown",
    }


def run() -> int:
    args = parse_args()
    load_env_file(Path(args.env_file).expanduser())

    log_file = Path(args.log_file).expanduser()
    state_file = Path(args.state_file).expanduser()
    db_path = Path(args.db_path).expanduser()
    ops_db = Path(os.getenv("PIVOT_DB", str(DEFAULT_DB))).expanduser()

    disk_warn_pct = float(os.getenv("HOST_HEALTH_DISK_WARN_PCT", "15"))
    disk_crit_pct = float(os.getenv("HOST_HEALTH_DISK_CRIT_PCT", "8"))
    db_growth_warn_mb = float(os.getenv("HOST_HEALTH_DB_GROWTH_WARN_MB", "2048"))
    db_growth_crit_mb = float(os.getenv("HOST_HEALTH_DB_GROWTH_CRIT_MB", "4096"))
    restart_warn_delta = int(os.getenv("HOST_HEALTH_RESTART_WARN_DELTA", "5"))

    previous = load_state(state_file)
    previous_db_size = int(previous.get("db_size_bytes") or 0)
    previous_ts = int(previous.get("checked_at_ms") or 0)
    previous_runs = previous.get("launchd_runs", {})

    checked_at_ms = now_ms()
    issues_warn: list[str] = []
    issues_crit: list[str] = []

    usage = shutil.disk_usage(ROOT)
    free_pct = (usage.free / usage.total) * 100 if usage.total > 0 else 0.0
    if free_pct <= disk_crit_pct:
        issues_crit.append(f"disk_free_pct={free_pct:.2f} below crit {disk_crit_pct:.2f}")
    elif free_pct <= disk_warn_pct:
        issues_warn.append(f"disk_free_pct={free_pct:.2f} below warn {disk_warn_pct:.2f}")

    db_size = db_path.stat().st_size if db_path.exists() else 0
    growth_mb = 0.0
    growth_mb_per_day = 0.0
    if previous_db_size > 0 and checked_at_ms > previous_ts > 0:
        growth_mb = (db_size - previous_db_size) / (1024 * 1024)
        elapsed_days = max((checked_at_ms - previous_ts) / (1000 * 60 * 60 * 24), 1e-6)
        growth_mb_per_day = growth_mb / elapsed_days
        if growth_mb_per_day >= db_growth_crit_mb:
            issues_crit.append(f"db_growth_mb_per_day={growth_mb_per_day:.2f} above crit {db_growth_crit_mb:.2f}")
        elif growth_mb_per_day >= db_growth_warn_mb:
            issues_warn.append(f"db_growth_mb_per_day={growth_mb_per_day:.2f} above warn {db_growth_warn_mb:.2f}")

    uid = os.getuid()
    labels = [x.strip() for x in os.getenv("HOST_HEALTH_LABELS", ",".join(DEFAULT_LABELS)).split(",") if x.strip()]
    launchd_metrics: dict[str, Any] = {}
    for label in labels:
        metric = parse_launchctl(label, uid)
        launchd_metrics[label] = metric
        if not metric["loaded"]:
            issues_warn.append(f"{label}: not loaded")
            continue
        if metric["last_exit_code"] not in {"0", "(never exited)"}:
            issues_warn.append(f"{label}: last_exit_code={metric['last_exit_code']}")
        prev_runs = int(previous_runs.get(label, 0)) if isinstance(previous_runs, dict) else 0
        if prev_runs > 0:
            delta = metric["runs"] - prev_runs
            if delta >= restart_warn_delta:
                issues_warn.append(f"{label}: runs delta={delta} since last check")

    status = "ok"
    if issues_crit:
        status = "critical"
    elif issues_warn:
        status = "warning"

    summary = (
        f"host_health status={status} disk_free_pct={free_pct:.2f} "
        f"db_size_mb={db_size / (1024 * 1024):.2f} "
        f"db_growth_mb_per_day={growth_mb_per_day:.2f} "
        f"warn={len(issues_warn)} crit={len(issues_crit)}"
    )
    log_line(log_file, summary)
    if issues_warn:
        log_line(log_file, "warnings: " + "; ".join(issues_warn))
    if issues_crit:
        log_line(log_file, "criticals: " + "; ".join(issues_crit))

    set_ops_status(
        ops_db,
        {
            "host_health_last_status": status,
            "host_health_last_run_ms": str(checked_at_ms),
            "host_health_disk_free_pct": f"{free_pct:.2f}",
            "host_health_db_size_bytes": str(db_size),
            "host_health_db_growth_mb_per_day": f"{growth_mb_per_day:.2f}",
            "host_health_warn_count": str(len(issues_warn)),
            "host_health_crit_count": str(len(issues_crit)),
            "host_health_last_error": "; ".join(issues_crit + issues_warn),
        },
    )

    next_state = {
        "checked_at_ms": checked_at_ms,
        "db_size_bytes": db_size,
        "launchd_runs": {k: v.get("runs", 0) for k, v in launchd_metrics.items()},
        "status": status,
    }
    if not args.dry_run:
        save_state(state_file, next_state)

    return 0


if __name__ == "__main__":
    raise SystemExit(run())
