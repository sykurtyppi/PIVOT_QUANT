from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any


def canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def read_jsonl_reviews(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    entries: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            entries.append(payload)
    return entries


def _connect(store_path: Path) -> sqlite3.Connection:
    store_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(store_path))
    conn.row_factory = sqlite3.Row
    return conn


def initialize_store(store_path: Path) -> None:
    conn = _connect(store_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS review_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS review_entries (
                sequence_id INTEGER PRIMARY KEY AUTOINCREMENT,
                review_id TEXT NOT NULL UNIQUE,
                recorded_at_utc TEXT NOT NULL,
                reviewer TEXT NOT NULL,
                note TEXT NOT NULL,
                model_id TEXT NOT NULL,
                model_version TEXT NOT NULL,
                decision_verdict TEXT,
                prev_hash TEXT NOT NULL,
                entry_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO review_meta(key, value)
            VALUES ('schema_version', '1')
            """
        )
        conn.commit()
    finally:
        conn.close()


def _entry_from_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = json.loads(str(row["payload_json"]))
    return payload if isinstance(payload, dict) else None


def review_count(store_path: Path) -> int:
    initialize_store(store_path)
    conn = _connect(store_path)
    try:
        row = conn.execute("SELECT COUNT(*) AS count FROM review_entries").fetchone()
        return int(row["count"] or 0) if row is not None else 0
    finally:
        conn.close()


def latest_review(store_path: Path) -> dict[str, Any] | None:
    initialize_store(store_path)
    conn = _connect(store_path)
    try:
        row = conn.execute(
            "SELECT payload_json FROM review_entries ORDER BY sequence_id DESC LIMIT 1"
        ).fetchone()
        return _entry_from_row(row)
    finally:
        conn.close()


def append_review(store_path: Path, entry_body: dict[str, Any]) -> dict[str, Any]:
    initialize_store(store_path)
    conn = _connect(store_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        prev_row = conn.execute(
            "SELECT entry_hash FROM review_entries ORDER BY sequence_id DESC LIMIT 1"
        ).fetchone()
        prev_hash = str(prev_row["entry_hash"] or "") if prev_row is not None else ""
        body = dict(entry_body)
        body["prev_hash"] = prev_hash
        entry_hash = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
        recorded_at = str(body.get("recorded_at_utc") or "")
        review_id = f"{recorded_at.replace(':', '').replace('-', '')}_{entry_hash[:12]}"
        entry = {**body, "review_id": review_id, "entry_hash": entry_hash}
        model = entry.get("model") or {}
        decision_summary = entry.get("decision_summary") or {}
        conn.execute(
            """
            INSERT INTO review_entries(
                review_id,
                recorded_at_utc,
                reviewer,
                note,
                model_id,
                model_version,
                decision_verdict,
                prev_hash,
                entry_hash,
                payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                review_id,
                recorded_at,
                str(entry.get("reviewer") or ""),
                str(entry.get("note") or ""),
                str(model.get("id") or ""),
                str(model.get("version") or ""),
                str(decision_summary.get("verdict") or ""),
                prev_hash,
                entry_hash,
                json.dumps(entry, ensure_ascii=True),
            ),
        )
        conn.commit()
        return entry
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def list_reviews(store_path: Path) -> list[dict[str, Any]]:
    initialize_store(store_path)
    conn = _connect(store_path)
    try:
        rows = conn.execute(
            "SELECT payload_json FROM review_entries ORDER BY sequence_id ASC"
        ).fetchall()
        entries: list[dict[str, Any]] = []
        for row in rows:
            entry = _entry_from_row(row)
            if entry:
                entries.append(entry)
        return entries
    finally:
        conn.close()


def get_review(store_path: Path, review_id: str) -> dict[str, Any] | None:
    initialize_store(store_path)
    conn = _connect(store_path)
    try:
        row = conn.execute(
            "SELECT payload_json FROM review_entries WHERE review_id = ? LIMIT 1",
            (review_id,),
        ).fetchone()
        return _entry_from_row(row)
    finally:
        conn.close()


def import_reviews(store_path: Path, entries: list[dict[str, Any]]) -> dict[str, int]:
    initialize_store(store_path)
    imported = 0
    skipped = 0
    conn = _connect(store_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        for entry in entries:
            if not isinstance(entry, dict):
                skipped += 1
                continue
            review_id = str(entry.get("review_id") or "").strip()
            entry_hash = str(entry.get("entry_hash") or "").strip()
            recorded_at = str(entry.get("recorded_at_utc") or "").strip()
            if not review_id or not entry_hash or not recorded_at:
                skipped += 1
                continue
            existing = conn.execute(
                "SELECT 1 FROM review_entries WHERE review_id = ? LIMIT 1",
                (review_id,),
            ).fetchone()
            if existing is not None:
                skipped += 1
                continue
            model = entry.get("model") or {}
            decision_summary = entry.get("decision_summary") or {}
            conn.execute(
                """
                INSERT INTO review_entries(
                    review_id,
                    recorded_at_utc,
                    reviewer,
                    note,
                    model_id,
                    model_version,
                    decision_verdict,
                    prev_hash,
                    entry_hash,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    review_id,
                    recorded_at,
                    str(entry.get("reviewer") or ""),
                    str(entry.get("note") or ""),
                    str(model.get("id") or ""),
                    str(model.get("version") or ""),
                    str(decision_summary.get("verdict") or ""),
                    str(entry.get("prev_hash") or ""),
                    entry_hash,
                    json.dumps(entry, ensure_ascii=True),
                ),
            )
            imported += 1
        conn.commit()
        return {"imported": imported, "skipped": skipped}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def migrate_jsonl_to_sqlite(log_path: Path, store_path: Path) -> dict[str, int]:
    entries = read_jsonl_reviews(log_path)
    stats = import_reviews(store_path, entries)
    return {
        "source_entries": len(entries),
        "imported": stats["imported"],
        "skipped": stats["skipped"],
        "store_entries": review_count(store_path),
    }


def verify_hash_chain(store_path: Path) -> dict[str, Any]:
    entries = list_reviews(store_path)
    previous_hash = ""
    for index, entry in enumerate(entries, start=1):
        expected_prev_hash = str(entry.get("prev_hash") or "")
        if expected_prev_hash != previous_hash:
            return {
                "valid": False,
                "count": len(entries),
                "broken_review_id": entry.get("review_id"),
                "reason": "prev_hash_mismatch",
                "position": index,
            }
        body = dict(entry)
        entry_hash = str(body.pop("entry_hash", "") or "")
        body.pop("review_id", None)
        computed_hash = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
        if entry_hash != computed_hash:
            return {
                "valid": False,
                "count": len(entries),
                "broken_review_id": entry.get("review_id"),
                "reason": "entry_hash_mismatch",
                "position": index,
            }
        previous_hash = entry_hash
    return {
        "valid": True,
        "count": len(entries),
        "broken_review_id": None,
        "reason": None,
        "position": None,
    }
