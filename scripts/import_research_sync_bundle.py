#!/usr/bin/env python3
"""Import an Air research sync bundle into a Mini-safe research workspace.

The import creates:
- immutable baseline DB copied from Air
- separate working DB for backfills/simulations
- models directory from manifests or full models archive
- env files that point research jobs at the working DB
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DEST_ROOT = ROOT / "data" / "air_research_sync"
LEGACY_CANDIDATE_MANIFEST = "manifest_latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import Air -> Mini research sync bundle.")
    parser.add_argument("--bundle-dir", required=True, help="Path to copied bundle directory.")
    parser.add_argument("--dest-root", default=str(DEFAULT_DEST_ROOT))
    parser.add_argument("--bundle-alias", default="", help="Override destination folder name.")
    parser.add_argument(
        "--manifest-preference",
        choices=("active", "latest"),
        default="active",
        help="Which manifest env files should point to by default.",
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_git_output(args: list[str]) -> str:
    try:
        return subprocess.check_output(args, cwd=str(ROOT), text=True).strip()
    except Exception:
        return ""


def copy_tree_contents(src_dir: Path, dst_dir: Path) -> None:
    dst_dir.mkdir(parents=True, exist_ok=True)
    for item in src_dir.iterdir():
        dst = dst_dir / item.name
        if item.is_dir():
            shutil.copytree(item, dst, dirs_exist_ok=True)
        else:
            shutil.copy2(item, dst)


def resolve_manifest(models_dir: Path, preference: str) -> Path:
    candidates = []
    if preference == "active":
        candidates.extend(
            [
                models_dir / "manifest_active.json",
                models_dir / "manifest_runtime_latest.json",
                models_dir / LEGACY_CANDIDATE_MANIFEST,
            ]
        )
    else:
        candidates.extend(
            [
                models_dir / "manifest_runtime_latest.json",
                models_dir / LEGACY_CANDIDATE_MANIFEST,
                models_dir / "manifest_active.json",
            ]
        )
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(f"no manifest found in {models_dir}")


def write_env_file(path: Path, vars_map: dict[str, str]) -> None:
    lines = [f'export {key}="{value}"' for key, value in vars_map.items()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def check_sqlite_tables(db_file: Path) -> dict[str, Any]:
    conn = sqlite3.connect(str(db_file))
    try:
        quick = conn.execute("PRAGMA quick_check").fetchone()
        quick_value = str(quick[0]) if quick else "unknown"
        counts = {}
        for table in ("bar_data", "touch_events", "prediction_log", "event_labels"):
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table,),
            ).fetchone()
            counts[table] = int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) if exists else -1
        return {"quick_check": quick_value, "counts": counts}
    finally:
        conn.close()


def main() -> int:
    args = parse_args()
    bundle_dir = Path(args.bundle_dir).expanduser().resolve()
    dest_root = Path(args.dest_root).expanduser().resolve()

    bundle_manifest_path = bundle_dir / "bundle_manifest.json"
    if not bundle_manifest_path.exists():
        raise FileNotFoundError(f"bundle manifest not found: {bundle_manifest_path}")

    bundle_manifest = json.loads(bundle_manifest_path.read_text(encoding="utf-8", errors="replace"))
    bundle_name = args.bundle_alias.strip() or str(bundle_manifest.get("bundle_name") or bundle_dir.name)
    dest_dir = dest_root / bundle_name

    report = {
        "bundle_dir": str(bundle_dir),
        "bundle_name": bundle_name,
        "dest_dir": str(dest_dir),
        "manifest_preference": args.manifest_preference,
        "local_git_branch": safe_git_output(["git", "branch", "--show-current"]),
        "local_git_head": safe_git_output(["git", "rev-parse", "--short", "HEAD"]),
        "source_git_branch": (bundle_manifest.get("source_snapshot") or {}).get("git_branch"),
        "source_git_head": (bundle_manifest.get("source_snapshot") or {}).get("git_head"),
    }

    if args.dry_run:
        print(json.dumps(report, indent=2))
        return 0

    if dest_dir.exists():
        if not args.overwrite:
            raise FileExistsError(f"destination already exists: {dest_dir}")
        shutil.rmtree(dest_dir)

    baseline_dir = dest_dir / "baseline"
    working_dir = dest_dir / "working"
    models_dir = dest_dir / "models"
    bundle_meta_dir = dest_dir / "bundle_meta"

    baseline_dir.mkdir(parents=True, exist_ok=False)
    working_dir.mkdir(parents=True, exist_ok=False)
    models_dir.mkdir(parents=True, exist_ok=False)
    bundle_meta_dir.mkdir(parents=True, exist_ok=False)

    db_src = bundle_dir / "pivot_events.sqlite"
    baseline_db = baseline_dir / "pivot_events.sqlite"
    working_db = working_dir / "pivot_events.sqlite"
    shutil.copy2(db_src, baseline_db)
    shutil.copy2(db_src, working_db)

    models_archive = bundle_dir / "models.tar.gz"
    export_models_dir = bundle_dir / "models_export"
    if models_archive.exists():
        with tarfile.open(models_archive, "r:gz") as tar:
            try:
                tar.extractall(dest_dir, filter="data")
            except TypeError:
                tar.extractall(dest_dir)
    elif export_models_dir.exists():
        copy_tree_contents(export_models_dir, models_dir)
    else:
        raise FileNotFoundError("bundle missing both models.tar.gz and models_export/")

    if not any(models_dir.iterdir()):
        raise RuntimeError(f"models import is empty: {models_dir}")

    selected_manifest = resolve_manifest(models_dir, args.manifest_preference)
    latest_manifest = resolve_manifest(models_dir, "latest")

    shutil.copy2(bundle_manifest_path, bundle_meta_dir / "bundle_manifest.json")
    source_snapshot = bundle_dir / "source_snapshot.json"
    if source_snapshot.exists():
        shutil.copy2(source_snapshot, bundle_meta_dir / "source_snapshot.json")

    env_vars = {
        "PIVOT_DB": str(working_db),
        "RF_MODEL_DIR": str(models_dir),
        "RF_MANIFEST_PATH": str(selected_manifest),
        "RF_ACTIVE_MANIFEST": str(models_dir / "manifest_active.json"),
        "RF_CANDIDATE_MANIFEST": str(latest_manifest),
        "PIVOT_RESEARCH_BASELINE_DB": str(baseline_db),
        "PIVOT_RESEARCH_BUNDLE_DIR": str(dest_dir),
    }
    env_path = dest_dir / "air_research.env"
    write_env_file(env_path, env_vars)

    latest_payload = {
        "bundle_name": bundle_name,
        "dest_dir": str(dest_dir),
        "env_file": str(env_path),
        "imported_at_utc": now_iso(),
    }
    dest_root.mkdir(parents=True, exist_ok=True)
    (dest_root / "latest.json").write_text(json.dumps(latest_payload, indent=2), encoding="utf-8")
    write_env_file(dest_root / "latest.env", env_vars)

    import_report = {
        **report,
        "imported_at_utc": now_iso(),
        "baseline_db_sha256": sha256_file(baseline_db),
        "working_db_sha256": sha256_file(working_db),
        "baseline_db_check": check_sqlite_tables(baseline_db),
        "working_db_check": check_sqlite_tables(working_db),
        "selected_manifest": str(selected_manifest),
        "latest_manifest": str(latest_manifest),
        "bundle_matches_local_git": report["local_git_head"] == report["source_git_head"],
    }
    (dest_dir / "import_report.json").write_text(json.dumps(import_report, indent=2), encoding="utf-8")

    print(json.dumps(import_report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
