#!/usr/bin/env python3
"""Nightly backup runner for PivotQuant artifacts.

Backs up:
- SQLite DB (consistent copy via sqlite backup API)
- data/models/
- logs/reports/

Retention policy:
- keep latest N daily snapshots
- additionally keep one snapshot per ISO week for W weeks
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import tarfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = ROOT / ".env"
DEFAULT_LOG_FILE = ROOT / "logs" / "backup.log"
DEFAULT_STATE_FILE = ROOT / "logs" / "backup_state.json"
DEFAULT_BACKUP_ROOT = ROOT / "backups"
DEFAULT_DB = Path(os.getenv("PIVOT_DB", str(ROOT / "data" / "pivot_events.sqlite")))
DEFAULT_MODELS_DIR = Path(os.getenv("RF_MODEL_DIR", str(ROOT / "data" / "models")))
DEFAULT_REPORTS_DIR = ROOT / "logs" / "reports"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create nightly PivotQuant backups.")
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_FILE))
    parser.add_argument("--backup-root", default=str(DEFAULT_BACKUP_ROOT))
    parser.add_argument("--db-path", default=str(DEFAULT_DB))
    parser.add_argument("--models-dir", default=str(DEFAULT_MODELS_DIR))
    parser.add_argument("--reports-dir", default=str(DEFAULT_REPORTS_DIR))
    parser.add_argument("--daily-keep", type=int, default=int(os.getenv("BACKUP_DAILY_KEEP", "30")))
    parser.add_argument("--weekly-keep", type=int, default=int(os.getenv("BACKUP_WEEKLY_KEEP", "8")))
    parser.add_argument("--log-file", default=str(DEFAULT_LOG_FILE))
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE))
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


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def backup_sqlite(src_db: Path, dst_db: Path) -> None:
    if not src_db.exists():
        raise FileNotFoundError(f"DB not found: {src_db}")
    dst_db.parent.mkdir(parents=True, exist_ok=True)
    src_conn = sqlite3.connect(f"file:{src_db}?mode=ro", uri=True)
    dst_conn = sqlite3.connect(str(dst_db))
    try:
        src_conn.backup(dst_conn)
        dst_conn.commit()
    finally:
        dst_conn.close()
        src_conn.close()


def create_tar_gz(src_dir: Path, dst_tar: Path, arcname: str) -> None:
    dst_tar.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(dst_tar, "w:gz") as tar:
        if src_dir.exists():
            tar.add(src_dir, arcname=arcname)


def parse_snapshot_name(name: str) -> datetime | None:
    try:
        return datetime.strptime(name, "%Y%m%d_%H%M%S")
    except ValueError:
        return None


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


def select_snapshots_to_keep(
    snapshots: list[tuple[Path, datetime]],
    daily_keep: int,
    weekly_keep: int,
) -> set[Path]:
    keep: set[Path] = set()
    ordered = sorted(snapshots, key=lambda item: item[1], reverse=True)

    for path, _ in ordered[: max(0, daily_keep)]:
        keep.add(path)

    week_keys: set[tuple[int, int]] = set()
    for path, dt in ordered:
        iso = dt.isocalendar()
        week_key = (iso.year, iso.week)
        if week_key in week_keys:
            continue
        keep.add(path)
        week_keys.add(week_key)
        if len(week_keys) >= max(0, weekly_keep):
            break

    return keep


def prune_snapshots(
    snapshots_root: Path,
    daily_keep: int,
    weekly_keep: int,
    dry_run: bool,
    log_file: Path,
) -> tuple[int, list[str]]:
    if not snapshots_root.exists():
        return 0, []

    parsed: list[tuple[Path, datetime]] = []
    for child in snapshots_root.iterdir():
        if not child.is_dir():
            continue
        parsed_dt = parse_snapshot_name(child.name)
        if parsed_dt is None:
            continue
        parsed.append((child, parsed_dt))

    keep = select_snapshots_to_keep(parsed, daily_keep, weekly_keep)
    removed: list[str] = []
    for path, _ in parsed:
        if path in keep:
            continue
        removed.append(path.name)
        if dry_run:
            continue
        shutil.rmtree(path, ignore_errors=True)

    if removed:
        log_line(log_file, f"retention pruned {len(removed)} snapshot(s): {', '.join(sorted(removed))}")
    else:
        log_line(log_file, "retention pruned 0 snapshot(s)")
    return len(removed), removed


def write_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)


def run() -> int:
    args = parse_args()
    load_env_file(Path(args.env_file).expanduser())

    backup_root = Path(args.backup_root).expanduser()
    snapshots_root = backup_root / "snapshots"
    db_path = Path(args.db_path).expanduser()
    models_dir = Path(args.models_dir).expanduser()
    reports_dir = Path(args.reports_dir).expanduser()
    log_file = Path(args.log_file).expanduser()
    state_file = Path(args.state_file).expanduser()
    ops_db = Path(os.getenv("PIVOT_DB", str(DEFAULT_DB))).expanduser()

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    snapshot_dir = snapshots_root / stamp
    db_backup = snapshot_dir / "pivot_events.sqlite"
    models_archive = snapshot_dir / "models.tar.gz"
    reports_archive = snapshot_dir / "reports.tar.gz"
    manifest_path = snapshot_dir / "manifest.json"

    log_line(log_file, f"backup start snapshot={stamp} dry_run={args.dry_run}")

    try:
        if not args.dry_run:
            snapshot_dir.mkdir(parents=True, exist_ok=False)
            backup_sqlite(db_path, db_backup)
            create_tar_gz(models_dir, models_archive, "models")
            create_tar_gz(reports_dir, reports_archive, "reports")

            manifest = {
                "snapshot": stamp,
                "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "db_source": str(db_path),
                "models_source": str(models_dir),
                "reports_source": str(reports_dir),
                "files": {
                    "pivot_events.sqlite": {
                        "size_bytes": db_backup.stat().st_size,
                        "sha256": sha256_file(db_backup),
                    },
                    "models.tar.gz": {
                        "size_bytes": models_archive.stat().st_size,
                        "sha256": sha256_file(models_archive),
                    },
                    "reports.tar.gz": {
                        "size_bytes": reports_archive.stat().st_size,
                        "sha256": sha256_file(reports_archive),
                    },
                },
            }
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            latest_link = backup_root / "latest"
            tmp_link = backup_root / ".latest.tmp"
            if tmp_link.exists() or tmp_link.is_symlink():
                tmp_link.unlink()
            tmp_link.symlink_to(snapshot_dir, target_is_directory=True)
            tmp_link.replace(latest_link)

        removed_count, _ = prune_snapshots(
            snapshots_root=snapshots_root,
            daily_keep=args.daily_keep,
            weekly_keep=args.weekly_keep,
            dry_run=args.dry_run,
            log_file=log_file,
        )

        set_ops_status(
            ops_db,
            {
                "backup_last_status": "ok",
                "backup_last_run_ms": str(now_ms()),
                "backup_last_snapshot": stamp,
                "backup_last_error": "",
                "backup_last_removed_count": str(removed_count),
            },
        )

        state_payload = {
            "last_status": "ok",
            "last_snapshot": stamp,
            "last_run_ms": now_ms(),
            "daily_keep": args.daily_keep,
            "weekly_keep": args.weekly_keep,
        }
        write_state(state_file, state_payload)
        log_line(log_file, f"backup done snapshot={stamp}")
        return 0
    except Exception as exc:  # pragma: no cover
        log_line(log_file, f"backup failed: {exc}")
        set_ops_status(
            ops_db,
            {
                "backup_last_status": "failed",
                "backup_last_run_ms": str(now_ms()),
                "backup_last_snapshot": stamp,
                "backup_last_error": str(exc),
            },
        )
        write_state(
            state_file,
            {
                "last_status": "failed",
                "last_snapshot": stamp,
                "last_run_ms": now_ms(),
                "last_error": str(exc),
            },
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(run())
