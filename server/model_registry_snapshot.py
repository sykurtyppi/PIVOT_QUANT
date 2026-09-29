from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Callable


BuildEntryFn = Callable[[Path, dict[str, Any], str | None], dict[str, Any]]
ExtractHorizonDetailsFn = Callable[[dict[str, Any]], list[dict[str, Any]]]


@dataclass(frozen=True)
class RegistryRecord:
    metadata_path: Path
    manifest_path: Path
    payload: dict[str, Any]
    manifest_payload: dict[str, Any] | None
    manifest_version: str | None
    entry: dict[str, Any]
    horizon_details: list[dict[str, Any]]


@dataclass(frozen=True)
class RegistrySnapshot:
    roots: tuple[Path, ...]
    metadata_files: tuple[Path, ...]
    fingerprint: tuple[tuple[str, int, int], ...]
    records: tuple[RegistryRecord, ...]
    records_by_id: dict[str, RegistryRecord]
    created_at_utc: str


_SNAPSHOT_CACHE: dict[tuple[str, ...], RegistrySnapshot] = {}
_SNAPSHOT_LOCK = Lock()


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        return None
    return None


def _metadata_files_for_roots(roots: tuple[Path, ...]) -> tuple[Path, ...]:
    files: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        files.extend(sorted(root.rglob("metadata_*.json")))
    return tuple(sorted(set(files)))


def _manifest_path(metadata_path: Path) -> Path:
    return metadata_path.parent.parent / "manifest_runtime_latest.json"


def _file_state(path: Path) -> tuple[str, int, int]:
    if not path.exists():
        return (str(path.resolve()), -1, -1)
    stat = path.stat()
    return (str(path.resolve()), int(stat.st_mtime_ns), int(stat.st_size))


def _fingerprint(metadata_files: tuple[Path, ...]) -> tuple[tuple[str, int, int], ...]:
    states: list[tuple[str, int, int]] = []
    for metadata_path in metadata_files:
        states.append(_file_state(metadata_path))
        states.append(_file_state(_manifest_path(metadata_path)))
    return tuple(states)


def _cache_key(roots: tuple[Path, ...]) -> tuple[str, ...]:
    return tuple(str(path.resolve()) for path in roots)


def clear_registry_snapshot_cache() -> None:
    with _SNAPSHOT_LOCK:
        _SNAPSHOT_CACHE.clear()


def get_registry_snapshot(
    *,
    roots: list[Path] | tuple[Path, ...],
    build_entry: BuildEntryFn,
    extract_horizon_details: ExtractHorizonDetailsFn,
) -> RegistrySnapshot:
    resolved_roots = tuple(path.resolve() for path in roots if path.exists())
    metadata_files = _metadata_files_for_roots(resolved_roots)
    fingerprint = _fingerprint(metadata_files)
    key = _cache_key(resolved_roots)

    with _SNAPSHOT_LOCK:
        cached = _SNAPSHOT_CACHE.get(key)
        if cached and cached.fingerprint == fingerprint:
            return cached

        records: list[RegistryRecord] = []
        for metadata_path in metadata_files:
            payload = _load_json(metadata_path)
            if not payload:
                continue
            manifest_path = _manifest_path(metadata_path)
            manifest_payload = _load_json(manifest_path) if manifest_path.exists() else None
            manifest_version = None
            if isinstance(manifest_payload, dict) and manifest_payload.get("version") is not None:
                manifest_version = str(manifest_payload.get("version"))
            entry = build_entry(metadata_path, payload, manifest_version)
            horizon_details = extract_horizon_details(payload)
            records.append(
                RegistryRecord(
                    metadata_path=metadata_path,
                    manifest_path=manifest_path,
                    payload=payload,
                    manifest_payload=manifest_payload,
                    manifest_version=manifest_version,
                    entry=entry,
                    horizon_details=horizon_details,
                )
            )

        records.sort(
            key=lambda record: (
                record.entry.get("trained_end_ts") or 0,
                record.entry.get("version") or "",
            ),
            reverse=True,
        )
        snapshot = RegistrySnapshot(
            roots=resolved_roots,
            metadata_files=metadata_files,
            fingerprint=fingerprint,
            records=tuple(records),
            records_by_id={
                str(record.entry.get("id") or ""): record
                for record in records
                if str(record.entry.get("id") or "").strip()
            },
            created_at_utc=_utc_now(),
        )
        _SNAPSHOT_CACHE[key] = snapshot
        return snapshot
