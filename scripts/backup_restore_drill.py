#!/usr/bin/env python3
"""Weekly restore drill for PivotQuant backups.

Validates the latest snapshot can be restored and queried:
- sqlite quick_check
- key table counts
- models/report archives can be extracted
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import tarfile
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from ops_lock import hold_lock


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = ROOT / ".env"
DEFAULT_LOG_FILE = ROOT / "logs" / "restore_drill.log"
DEFAULT_BACKUP_ROOT = ROOT / "backups"
DEFAULT_LOCK_FILE = ROOT / "logs" / "ops_resilience.lock"
DEFAULT_DB = Path(os.getenv("PIVOT_DB", str(ROOT / "data" / "pivot_events.sqlite")))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run backup restore drill.")
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_FILE))
    parser.add_argument("--backup-root", default=str(DEFAULT_BACKUP_ROOT))
    parser.add_argument("--log-file", default=str(DEFAULT_LOG_FILE))
    parser.add_argument(
        "--lock-file",
        default=os.getenv("PIVOT_OPS_LOCK_FILE", str(DEFAULT_LOCK_FILE)),
    )
    parser.add_argument(
        "--lock-timeout-sec",
        type=int,
        default=int(os.getenv("PIVOT_OPS_LOCK_TIMEOUT_SEC", "300")),
    )
    parser.add_argument("--keep-temp", action="store_true")
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


def latest_snapshot(snapshots_root: Path) -> Path:
    candidates: list[tuple[datetime, Path]] = []
    for child in snapshots_root.iterdir():
        if not child.is_dir():
            continue
        try:
            dt = datetime.strptime(child.name, "%Y%m%d_%H%M%S")
        except ValueError:
            continue
        candidates.append((dt, child))
    if not candidates:
        raise FileNotFoundError(f"No backup snapshots found in {snapshots_root}")

    reasons: list[str] = []
    for _, snapshot in sorted(candidates, key=lambda item: item[0], reverse=True):
        db_backup = snapshot / "pivot_events.sqlite"
        models_archive = snapshot / "models.tar.gz"
        reports_archive = snapshot / "reports.tar.gz"
        manifest_path = snapshot / "manifest.json"
        if not db_backup.exists():
            reasons.append(f"{snapshot.name}: missing pivot_events.sqlite")
            continue
        if not models_archive.exists():
            reasons.append(f"{snapshot.name}: missing models.tar.gz")
            continue
        if not reports_archive.exists():
            reasons.append(f"{snapshot.name}: missing reports.tar.gz")
            continue
        if not manifest_path.exists():
            reasons.append(f"{snapshot.name}: missing manifest.json")
            continue
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8", errors="replace"))
        except json.JSONDecodeError:
            reasons.append(f"{snapshot.name}: invalid manifest.json")
            continue
        if str(manifest.get("status", "complete")) != "complete":
            reasons.append(f"{snapshot.name}: manifest status is not complete")
            continue
        return snapshot

    reason = "; ".join(reasons[:3])
    raise FileNotFoundError(
        f"No complete backup snapshots found in {snapshots_root}"
        + (f" (latest issues: {reason})" if reason else "")
    )


def check_sqlite(db_file: Path) -> dict[str, Any]:
    conn = sqlite3.connect(str(db_file))
    try:
        quick = conn.execute("PRAGMA quick_check").fetchone()
        quick_value = str(quick[0]) if quick else "unknown"
        tables = ["bar_data", "touch_events", "prediction_log", "event_labels"]
        counts: dict[str, int] = {}
        for table in tables:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            if not exists:
                counts[table] = -1
                continue
            value = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            counts[table] = int(value)
        return {"quick_check": quick_value, "counts": counts}
    finally:
        conn.close()


def run() -> int:
    args = parse_args()
    load_env_file(Path(args.env_file).expanduser())

    backup_root = Path(args.backup_root).expanduser()
    snapshots_root = backup_root / "snapshots"
    log_file = Path(args.log_file).expanduser()
    lock_file = Path(args.lock_file).expanduser()
    ops_db = Path(os.getenv("PIVOT_DB", str(DEFAULT_DB))).expanduser()

    log_line(log_file, f"restore drill start dry_run={args.dry_run}")
    try:
        with hold_lock(lock_file, args.lock_timeout_sec, "backup_restore_drill"):
            snapshot = latest_snapshot(snapshots_root)
            db_backup = snapshot / "pivot_events.sqlite"
            models_archive = snapshot / "models.tar.gz"
            reports_archive = snapshot / "reports.tar.gz"

            if args.dry_run:
                log_line(log_file, f"restore drill dry-run snapshot={snapshot.name}")
                return 0

            tmp_root = Path(tempfile.mkdtemp(prefix="pq_restore_drill_"))
            try:
                restored_db = tmp_root / "pivot_events.sqlite"
                shutil.copy2(db_backup, restored_db)
                db_check = check_sqlite(restored_db)
                if db_check["quick_check"] != "ok":
                    raise RuntimeError(f"sqlite quick_check failed: {db_check['quick_check']}")

                models_extract = tmp_root / "models_extract"
                reports_extract = tmp_root / "reports_extract"
                models_extract.mkdir(parents=True, exist_ok=True)
                reports_extract.mkdir(parents=True, exist_ok=True)
                with tarfile.open(models_archive, "r:gz") as tar:
                    try:
                        tar.extractall(models_extract, filter="data")
                    except TypeError:
                        tar.extractall(models_extract)
                with tarfile.open(reports_archive, "r:gz") as tar:
                    try:
                        tar.extractall(reports_extract, filter="data")
                    except TypeError:
                        tar.extractall(reports_extract)

                model_manifest = models_extract / "models" / "manifest_latest.json"
                if not model_manifest.exists():
                    raise FileNotFoundError("restored models missing manifest_latest.json")
                report_files = list((reports_extract / "reports").glob("ml_daily_*.md"))
                if not report_files:
                    raise FileNotFoundError("restored reports missing ml_daily_*.md")

                summary = {
                    "snapshot": snapshot.name,
                    "db_quick_check": db_check["quick_check"],
                    "counts": db_check["counts"],
                    "restored_report_files": len(report_files),
                }
                report_path = ROOT / "logs" / f"restore_drill_{snapshot.name}.json"
                report_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
                log_line(log_file, f"restore drill ok snapshot={snapshot.name} report={report_path.name}")

                set_ops_status(
                    ops_db,
                    {
                        "backup_restore_last_status": "ok",
                        "backup_restore_last_run_ms": str(now_ms()),
                        "backup_restore_last_snapshot": snapshot.name,
                        "backup_restore_last_error": "",
                    },
                )
            finally:
                if not args.keep_temp:
                    shutil.rmtree(tmp_root, ignore_errors=True)

            return 0
    except TimeoutError:
        log_line(log_file, f"restore drill skipped: lock busy ({lock_file})")
        set_ops_status(
            ops_db,
            {
                "backup_restore_last_status": "skipped_lock_busy",
                "backup_restore_last_run_ms": str(now_ms()),
                "backup_restore_last_snapshot": "",
                "backup_restore_last_error": f"lock busy: {lock_file}",
            },
        )
        return 0
    except Exception as exc:  # pragma: no cover
        log_line(log_file, f"restore drill failed: {exc}")
        set_ops_status(
            ops_db,
            {
                "backup_restore_last_status": "failed",
                "backup_restore_last_run_ms": str(now_ms()),
                "backup_restore_last_snapshot": "",
                "backup_restore_last_error": str(exc),
            },
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(run())
