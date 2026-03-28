#!/usr/bin/env python3
"""Export a one-way Air -> Mini research sync bundle.

The bundle is intended for research only:
- SQLite DB snapshot
- model manifests + registry metadata
- optional full model directory archive
- source parity/environment summary
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
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = ROOT / ".env"
DEFAULT_DB = Path(os.getenv("PIVOT_DB", str(ROOT / "data" / "pivot_events.sqlite")))
DEFAULT_MODELS_DIR = Path(os.getenv("RF_MODEL_DIR", str(ROOT / "data" / "models")))
DEFAULT_OUTPUT_ROOT = ROOT / "backups" / "research_sync"
DEFAULT_HISTORY_TAIL = 10
LEGACY_CANDIDATE_MANIFEST = "manifest_latest.json"
ENV_SUBSET_KEYS = [
    "MODEL_GOV_REQUIRED_TARGETS",
    "MODEL_GOV_REQUIRED_HORIZONS",
    "MODEL_GOV_THRESHOLD_UTILITY_MIN_SCORE",
    "MODEL_GOV_MIN_CORR_POS",
    "MODEL_GOV_MIN_TUNE_SIGNALS",
    "MODEL_GOV_ENFORCE_LIVE_EMISSION_GATE",
    "MODEL_GOV_EMISSION_MAX_ABSTAIN_RATE",
    "RF_CALIB_DAYS",
    "RF_THRESHOLD_MIN_UTILITY_SCORE",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Air -> Mini research sync bundle.")
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_FILE))
    parser.add_argument("--db-path", default=str(DEFAULT_DB))
    parser.add_argument("--models-dir", default=str(DEFAULT_MODELS_DIR))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--bundle-name", default="")
    parser.add_argument("--history-tail", type=int, default=DEFAULT_HISTORY_TAIL)
    parser.add_argument(
        "--skip-models-archive",
        action="store_true",
        help="Export manifests/registry only without the full models.tar.gz archive.",
    )
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


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


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


def parse_env_map(path: Path) -> dict[str, str]:
    env_map: dict[str, str] = {}
    if not path.exists():
        return env_map
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            env_map[key] = value
    return env_map


def safe_git_output(args: list[str]) -> str:
    try:
        return subprocess.check_output(args, cwd=str(ROOT), text=True).strip()
    except Exception:
        return ""


def build_source_snapshot(env_file: Path, models_dir: Path, history_tail_count: int) -> dict[str, Any]:
    env_map = parse_env_map(env_file)
    active_manifest = models_dir / "manifest_active.json"
    latest_manifest = models_dir / "manifest_runtime_latest.json"
    registry = models_dir / "model_registry.json"

    history_tail: list[dict[str, Any]] = []
    if registry.exists():
        try:
            payload = json.loads(registry.read_text(encoding="utf-8", errors="replace"))
            history = payload.get("history", [])
            if isinstance(history, list):
                history_tail = history[-max(0, history_tail_count) :]
        except json.JSONDecodeError:
            history_tail = []

    def manifest_info(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {"path": str(path), "exists": False}
        try:
            payload = json.loads(path.read_text(encoding="utf-8", errors="replace"))
        except json.JSONDecodeError:
            payload = {}
        return {
            "path": str(path),
            "exists": True,
            "version": payload.get("version"),
            "feature_version": payload.get("feature_version"),
            "sha256": sha256_file(path),
        }

    return {
        "host": safe_git_output(["hostname"]) or os.uname().nodename,
        "git_branch": safe_git_output(["git", "branch", "--show-current"]),
        "git_head": safe_git_output(["git", "rev-parse", "--short", "HEAD"]),
        "git_dirty": bool(safe_git_output(["git", "status", "--porcelain"])),
        "env_subset": {key: env_map.get(key) for key in ENV_SUBSET_KEYS},
        "active_manifest": manifest_info(active_manifest),
        "latest_manifest": manifest_info(latest_manifest),
        "history_tail": history_tail,
    }


def copy_if_exists(src: Path, dst: Path) -> None:
    if src.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)


def allocate_bundle_dirs(root: Path, requested_name: str) -> tuple[str, Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    if requested_name:
        final_dir = root / requested_name
        staging_dir = root / f".{requested_name}.inprogress"
        if final_dir.exists() or staging_dir.exists():
            raise FileExistsError(f"bundle already exists: {requested_name}")
        return requested_name, final_dir, staging_dir

    while True:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        name = f"{stamp}_{safe_git_output(['hostname']) or os.uname().nodename}"
        final_dir = root / name
        staging_dir = root / f".{name}.inprogress"
        if not final_dir.exists() and not staging_dir.exists():
            return name, final_dir, staging_dir
        time.sleep(1)


def build_models_inventory(models_dir: Path) -> dict[str, int]:
    inventory = {
        "json_files": 0,
        "pkl_files": 0,
        "metadata_runtime_files": 0,
    }
    if not models_dir.exists():
        return inventory
    inventory["json_files"] = sum(1 for _ in models_dir.glob("*.json"))
    inventory["pkl_files"] = sum(1 for _ in models_dir.glob("*.pkl"))
    metadata_runtime = models_dir / "metadata_runtime"
    if metadata_runtime.exists():
        inventory["metadata_runtime_files"] = sum(1 for _ in metadata_runtime.glob("*.json"))
    return inventory


def create_models_archive(src_dir: Path, dst_tar: Path) -> None:
    dst_tar.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(dst_tar, "w:gz") as tar:
        tar.add(src_dir, arcname="models")


def main() -> int:
    args = parse_args()
    env_file = Path(args.env_file).expanduser()
    load_env_file(env_file)

    db_path = Path(args.db_path).expanduser()
    models_dir = Path(args.models_dir).expanduser()
    output_root = Path(args.output_root).expanduser()

    bundle_name, final_dir, staging_dir = allocate_bundle_dirs(output_root, args.bundle_name.strip())

    source_snapshot = build_source_snapshot(env_file, models_dir, args.history_tail)
    inventory = build_models_inventory(models_dir)

    if args.dry_run:
        print(
            json.dumps(
                {
                    "bundle_name": bundle_name,
                    "final_dir": str(final_dir),
                    "db_path": str(db_path),
                    "models_dir": str(models_dir),
                    "skip_models_archive": bool(args.skip_models_archive),
                    "source_snapshot": source_snapshot,
                    "models_inventory": inventory,
                },
                indent=2,
            )
        )
        return 0

    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=False)

    try:
        db_dst = staging_dir / "pivot_events.sqlite"
        backup_sqlite(db_path, db_dst)

        export_models_dir = staging_dir / "models_export"
        export_models_dir.mkdir(parents=True, exist_ok=True)
        copy_if_exists(models_dir / "manifest_active.json", export_models_dir / "manifest_active.json")
        copy_if_exists(models_dir / "manifest_active_prev.json", export_models_dir / "manifest_active_prev.json")
        copy_if_exists(models_dir / "manifest_runtime_latest.json", export_models_dir / "manifest_runtime_latest.json")
        copy_if_exists(models_dir / LEGACY_CANDIDATE_MANIFEST, export_models_dir / LEGACY_CANDIDATE_MANIFEST)
        copy_if_exists(models_dir / "model_registry.json", export_models_dir / "model_registry.json")
        metadata_runtime = models_dir / "metadata_runtime"
        if metadata_runtime.exists():
            shutil.copytree(metadata_runtime, export_models_dir / "metadata_runtime")

        source_snapshot_path = staging_dir / "source_snapshot.json"
        source_snapshot_path.write_text(json.dumps(source_snapshot, indent=2), encoding="utf-8")

        if not args.skip_models_archive and models_dir.exists():
            create_models_archive(models_dir, staging_dir / "models.tar.gz")

        files: dict[str, Any] = {}
        for path in sorted(staging_dir.rglob("*")):
            if path.is_file():
                files[str(path.relative_to(staging_dir))] = {
                    "size_bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                }

        manifest = {
            "bundle_type": "air_research_sync",
            "bundle_name": bundle_name,
            "created_at_utc": now_iso(),
            "source_repo_root": str(ROOT),
            "db_source": str(db_path),
            "models_source": str(models_dir),
            "models_archive_included": not args.skip_models_archive,
            "models_inventory": inventory,
            "source_snapshot": source_snapshot,
            "files": files,
            "usage_notes": {
                "production_truth": "Air remains production truth.",
                "research_worker": "Mini should import this bundle into a separate research workspace.",
            },
        }
        (staging_dir / "bundle_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        os.replace(staging_dir, final_dir)
    except Exception:
        if staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)
        raise

    print(json.dumps({"bundle_dir": str(final_dir), "bundle_name": bundle_name}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
