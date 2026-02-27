#!/usr/bin/env python3
"""Append-only, tamper-evident audit log for PivotQuant operations."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import sqlite3
import subprocess
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = Path(os.getenv("PIVOT_DB", str(ROOT / "data" / "pivot_events.sqlite")))
MAX_TAIL_LIMIT = 500
MS_PER_DAY = 24 * 60 * 60 * 1000


def _env_int(name: str, default: int, *, minimum: int | None = None) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        value = default
    else:
        try:
            value = int(raw)
        except ValueError:
            value = default
    if minimum is not None and value < minimum:
        value = minimum
    return value


DEFAULT_AUDIT_RETENTION_DAYS = _env_int("ML_AUDIT_RETENTION_DAYS", 90, minimum=1)
DEFAULT_AUDIT_PRUNE_INTERVAL_MS = _env_int(
    "ML_AUDIT_PRUNE_INTERVAL_MS",
    MS_PER_DAY,
    minimum=0,
)


def now_ms() -> int:
    return int(time.time() * 1000)


def parse_key_value(raw: str) -> tuple[str, str]:
    if "=" not in raw:
        raise ValueError(f"Invalid detail '{raw}', expected key=value")
    key, value = raw.split("=", 1)
    key = key.strip()
    if not key:
        raise ValueError(f"Invalid detail '{raw}', key is empty")
    return key, value


def detect_commit_hash(cwd: Path) -> str:
    env_hash = (os.getenv("GIT_COMMIT") or os.getenv("COMMIT_SHA") or "").strip()
    if env_hash:
        return env_hash
    proc = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode == 0:
        return proc.stdout.strip()
    return ""


def canonical_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def connect_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_status (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ops_audit_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_ms INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            source TEXT NOT NULL,
            actor TEXT NOT NULL,
            host TEXT NOT NULL,
            commit_hash TEXT NOT NULL,
            message TEXT NOT NULL,
            details_json TEXT NOT NULL,
            prev_hash TEXT NOT NULL,
            event_hash TEXT NOT NULL UNIQUE
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_ops_audit_events_ts
          ON ops_audit_events(ts_ms)
        """
    )
    conn.commit()


def _set_ops_status(conn: sqlite3.Connection, pairs: dict[str, str], ts: int | None = None) -> None:
    updated_at = ts if ts is not None else now_ms()
    for key, value in pairs.items():
        conn.execute(
            """
            INSERT INTO ops_status(key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE
              SET value = excluded.value,
                  updated_at = excluded.updated_at
            """,
            (key, str(value), updated_at),
        )


def _get_ops_status_value(conn: sqlite3.Connection, key: str) -> str:
    row = conn.execute("SELECT value FROM ops_status WHERE key = ?", (key,)).fetchone()
    if not row or row[0] is None:
        return ""
    return str(row[0])


def _prune_audit_prefix(
    conn: sqlite3.Connection,
    *,
    now_ts_ms: int,
    retention_days: int,
) -> dict[str, Any]:
    if retention_days <= 0:
        raise ValueError("retention_days must be > 0")

    cutoff_ms = now_ts_ms - (retention_days * MS_PER_DAY)
    first_keep = conn.execute(
        """
        SELECT id, prev_hash
        FROM ops_audit_events
        WHERE ts_ms >= ?
        ORDER BY id ASC
        LIMIT 1
        """,
        (cutoff_ms,),
    ).fetchone()

    if first_keep:
        first_keep_id = int(first_keep[0])
        anchor_prev_hash = str(first_keep[1] or "")
        delete_cur = conn.execute("DELETE FROM ops_audit_events WHERE id < ?", (first_keep_id,))
    else:
        first_keep_id = 0
        anchor_prev_hash = ""
        delete_cur = conn.execute("DELETE FROM ops_audit_events")

    deleted_rows = int(delete_cur.rowcount if delete_cur.rowcount is not None else 0)
    remaining_rows = int(conn.execute("SELECT COUNT(*) FROM ops_audit_events").fetchone()[0])
    _set_ops_status(
        conn,
        {
            "audit_retention_days": str(retention_days),
            "audit_prune_last_ms": str(now_ts_ms),
            "audit_prune_cutoff_ms": str(cutoff_ms),
            "audit_prune_deleted_rows": str(deleted_rows),
            "audit_prune_remaining_rows": str(remaining_rows),
            # When a prefix is pruned, chain verification must start from
            # the retained head's prev_hash instead of the implicit empty hash.
            "audit_chain_anchor_prev_hash": anchor_prev_hash,
            "audit_chain_anchor_event_id": str(first_keep_id),
            "audit_chain_anchor_set_ms": str(now_ts_ms),
        },
        now_ts_ms,
    )
    return {
        "retention_days": retention_days,
        "cutoff_ms": cutoff_ms,
        "deleted_rows": deleted_rows,
        "remaining_rows": remaining_rows,
        "anchor_prev_hash": anchor_prev_hash,
        "anchor_event_id": first_keep_id,
    }


def maybe_prune_audit_prefix(
    conn: sqlite3.Connection,
    *,
    now_ts_ms: int,
    retention_days: int,
    prune_interval_ms: int,
) -> dict[str, Any] | None:
    if retention_days <= 0:
        return None
    last_prune_raw = _get_ops_status_value(conn, "audit_prune_last_ms")
    try:
        last_prune_ms = int(last_prune_raw)
    except ValueError:
        last_prune_ms = 0

    if prune_interval_ms > 0 and last_prune_ms > 0 and (now_ts_ms - last_prune_ms) < prune_interval_ms:
        return None
    return _prune_audit_prefix(conn, now_ts_ms=now_ts_ms, retention_days=retention_days)


def prune_history(
    *,
    db_path: Path,
    retention_days: int,
    now_ts_ms: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    if retention_days <= 0:
        raise ValueError("retention_days must be > 0")
    ts_now = now_ts_ms if now_ts_ms is not None else now_ms()
    cutoff_ms = ts_now - (retention_days * MS_PER_DAY)

    conn = connect_db(db_path)
    try:
        ensure_schema(conn)
        first_keep = conn.execute(
            """
            SELECT id, prev_hash
            FROM ops_audit_events
            WHERE ts_ms >= ?
            ORDER BY id ASC
            LIMIT 1
            """,
            (cutoff_ms,),
        ).fetchone()
        if first_keep:
            first_keep_id = int(first_keep[0])
            anchor_prev_hash = str(first_keep[1] or "")
            deleted_rows = int(
                conn.execute("SELECT COUNT(*) FROM ops_audit_events WHERE id < ?", (first_keep_id,)).fetchone()[0]
            )
            remaining_rows = int(
                conn.execute("SELECT COUNT(*) FROM ops_audit_events WHERE id >= ?", (first_keep_id,)).fetchone()[0]
            )
        else:
            first_keep_id = 0
            anchor_prev_hash = ""
            deleted_rows = int(conn.execute("SELECT COUNT(*) FROM ops_audit_events").fetchone()[0])
            remaining_rows = 0

        summary = {
            "retention_days": retention_days,
            "cutoff_ms": cutoff_ms,
            "deleted_rows": deleted_rows,
            "remaining_rows": remaining_rows,
            "anchor_prev_hash": anchor_prev_hash,
            "anchor_event_id": first_keep_id,
        }
        if dry_run:
            return {"status": "dry_run", **summary}

        applied = _prune_audit_prefix(conn, now_ts_ms=ts_now, retention_days=retention_days)
        conn.commit()
        return {"status": "ok", **applied}
    finally:
        conn.close()


def append_event(
    *,
    db_path: Path,
    event_type: str,
    source: str,
    actor: str,
    host: str,
    message: str,
    details: dict[str, Any] | None = None,
    commit_hash: str = "",
    ts_ms: int | None = None,
) -> dict[str, Any]:
    ts = ts_ms if ts_ms is not None else now_ms()
    detail_payload = details or {}
    if not isinstance(detail_payload, dict):
        raise ValueError("details must be a JSON object")

    conn = connect_db(db_path)
    try:
        ensure_schema(conn)
        maintenance_ts = now_ms()
        prune_result = maybe_prune_audit_prefix(
            conn,
            now_ts_ms=maintenance_ts,
            retention_days=DEFAULT_AUDIT_RETENTION_DAYS,
            prune_interval_ms=DEFAULT_AUDIT_PRUNE_INTERVAL_MS,
        )
        prev_row = conn.execute(
            "SELECT id, event_hash FROM ops_audit_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        prev_hash = str(prev_row[1]) if prev_row else ""

        hash_payload = {
            "ts_ms": ts,
            "event_type": event_type,
            "source": source,
            "actor": actor,
            "host": host,
            "commit_hash": commit_hash,
            "message": message,
            "details": detail_payload,
        }
        payload_json = canonical_json(hash_payload)
        event_hash = hashlib.sha256(f"{prev_hash}|{payload_json}".encode("utf-8")).hexdigest()
        details_json = json.dumps(detail_payload, sort_keys=True)

        cur = conn.execute(
            """
            INSERT INTO ops_audit_events(
              ts_ms, event_type, source, actor, host, commit_hash, message,
              details_json, prev_hash, event_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                event_type,
                source,
                actor,
                host,
                commit_hash,
                message,
                details_json,
                prev_hash,
                event_hash,
            ),
        )
        event_id = int(cur.lastrowid)

        _set_ops_status(
            conn,
            {
                "audit_last_event_id": str(event_id),
                "audit_last_event_ts_ms": str(ts),
                "audit_last_event_type": event_type,
                "audit_last_event_hash": event_hash,
                "audit_chain_status": "ok",
                "audit_chain_checked_ms": str(ts),
            },
            ts,
        )
        conn.commit()
        response = {
            "status": "ok",
            "event": {
                "id": event_id,
                "ts_ms": ts,
                "event_type": event_type,
                "source": source,
                "actor": actor,
                "host": host,
                "commit_hash": commit_hash,
                "message": message,
                "details": detail_payload,
                "prev_hash": prev_hash,
                "event_hash": event_hash,
            },
        }
        if prune_result is not None:
            response["prune"] = prune_result
        return response
    finally:
        conn.close()


def fetch_tail(*, db_path: Path, limit: int) -> dict[str, Any]:
    safe_limit = max(1, min(limit, MAX_TAIL_LIMIT))
    conn = connect_db(db_path)
    try:
        ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT id, ts_ms, event_type, source, actor, host, commit_hash, message,
                   details_json, prev_hash, event_hash
            FROM ops_audit_events
            ORDER BY id DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()
        events: list[dict[str, Any]] = []
        for row in reversed(rows):
            details = {}
            if row[8]:
                try:
                    details = json.loads(row[8])
                except json.JSONDecodeError:
                    details = {"raw": row[8]}
            events.append(
                {
                    "id": int(row[0]),
                    "ts_ms": int(row[1]),
                    "event_type": str(row[2]),
                    "source": str(row[3]),
                    "actor": str(row[4]),
                    "host": str(row[5]),
                    "commit_hash": str(row[6]),
                    "message": str(row[7]),
                    "details": details,
                    "prev_hash": str(row[9]),
                    "event_hash": str(row[10]),
                }
            )
        return {"status": "ok", "count": len(events), "events": events}
    finally:
        conn.close()


def verify_chain(*, db_path: Path) -> dict[str, Any]:
    conn = connect_db(db_path)
    try:
        ensure_schema(conn)
        anchor_prev_hash = _get_ops_status_value(conn, "audit_chain_anchor_prev_hash")
        rows = conn.execute(
            """
            SELECT id, ts_ms, event_type, source, actor, host, commit_hash, message,
                   details_json, prev_hash, event_hash
            FROM ops_audit_events
            ORDER BY id ASC
            """
        ).fetchall()

        prev_hash = anchor_prev_hash
        checked = 0
        for row in rows:
            event_id = int(row[0])
            ts_ms = int(row[1])
            event_type = str(row[2])
            source = str(row[3])
            actor = str(row[4])
            host = str(row[5])
            commit_hash = str(row[6])
            message = str(row[7])
            details_raw = str(row[8] or "{}")
            stored_prev_hash = str(row[9])
            stored_event_hash = str(row[10])

            try:
                details = json.loads(details_raw)
                if not isinstance(details, dict):
                    details = {"raw": details}
            except json.JSONDecodeError:
                details = {"raw": details_raw}

            expected_prev = prev_hash
            payload_json = canonical_json(
                {
                    "ts_ms": ts_ms,
                    "event_type": event_type,
                    "source": source,
                    "actor": actor,
                    "host": host,
                    "commit_hash": commit_hash,
                    "message": message,
                    "details": details,
                }
            )
            expected_hash = hashlib.sha256(f"{expected_prev}|{payload_json}".encode("utf-8")).hexdigest()
            checked += 1

            if stored_prev_hash != expected_prev or stored_event_hash != expected_hash:
                ts = now_ms()
                _set_ops_status(
                    conn,
                    {
                        "audit_chain_status": "fail",
                        "audit_chain_checked_ms": str(ts),
                        "audit_chain_last_error": f"chain mismatch at id={event_id}",
                    },
                    ts,
                )
                conn.commit()
                return {
                    "status": "fail",
                    "checked_events": checked,
                    "error": f"chain mismatch at id={event_id}",
                    "event_id": event_id,
                    "anchor_prev_hash": anchor_prev_hash,
                }

            prev_hash = stored_event_hash

        ts = now_ms()
        _set_ops_status(
            conn,
            {
                "audit_chain_status": "ok",
                "audit_chain_checked_ms": str(ts),
                "audit_chain_last_error": "",
            },
            ts,
        )
        conn.commit()
        return {"status": "ok", "checked_events": checked, "anchor_prev_hash": anchor_prev_hash}
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Append-only audit trail tooling.")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    sub = parser.add_subparsers(dest="command", required=True)

    log_cmd = sub.add_parser("log", help="Append an audit event")
    log_cmd.add_argument("--event-type", required=True)
    log_cmd.add_argument("--source", default=os.getenv("ML_AUDIT_SOURCE", "pivotquant"))
    log_cmd.add_argument("--actor", default=os.getenv("ML_AUDIT_ACTOR", os.getenv("USER", "unknown")))
    log_cmd.add_argument("--host", default=socket.gethostname())
    log_cmd.add_argument("--commit", default="")
    log_cmd.add_argument("--message", default="")
    log_cmd.add_argument("--details-json", default="")
    log_cmd.add_argument("--detail", action="append", default=[])
    log_cmd.add_argument("--ts-ms", type=int, default=0)
    log_cmd.add_argument("--dry-run", action="store_true")

    tail_cmd = sub.add_parser("tail", help="Read latest audit events")
    tail_cmd.add_argument("--limit", type=int, default=25)

    sub.add_parser("verify", help="Verify tamper-evident hash chain")

    prune_cmd = sub.add_parser("prune", help="Prune old audit rows while preserving hash-chain anchor")
    prune_cmd.add_argument("--retention-days", type=int, default=DEFAULT_AUDIT_RETENTION_DAYS)
    prune_cmd.add_argument("--now-ms", type=int, default=0)
    prune_cmd.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db).expanduser()

    if args.command == "log":
        details: dict[str, Any] = {}
        if args.details_json:
            parsed = json.loads(args.details_json)
            if not isinstance(parsed, dict):
                raise ValueError("--details-json must be a JSON object")
            details.update(parsed)
        for raw in args.detail:
            key, value = parse_key_value(raw)
            details[key] = value

        commit_hash = args.commit.strip() or detect_commit_hash(ROOT)
        payload = {
            "event_type": args.event_type.strip(),
            "source": args.source.strip(),
            "actor": args.actor.strip(),
            "host": args.host.strip(),
            "commit_hash": commit_hash,
            "message": args.message.strip(),
            "details": details,
            "ts_ms": int(args.ts_ms) if args.ts_ms else now_ms(),
        }
        if args.dry_run:
            print(json.dumps({"status": "dry_run", "event": payload}, indent=2))
            return 0

        result = append_event(
            db_path=db_path,
            event_type=payload["event_type"],
            source=payload["source"],
            actor=payload["actor"],
            host=payload["host"],
            commit_hash=payload["commit_hash"],
            message=payload["message"],
            details=payload["details"],
            ts_ms=int(payload["ts_ms"]),
        )
        print(json.dumps(result, indent=2))
        return 0

    if args.command == "tail":
        result = fetch_tail(db_path=db_path, limit=int(args.limit))
        print(json.dumps(result, indent=2))
        return 0

    if args.command == "verify":
        result = verify_chain(db_path=db_path)
        print(json.dumps(result, indent=2))
        return 0 if result.get("status") == "ok" else 1

    if args.command == "prune":
        result = prune_history(
            db_path=db_path,
            retention_days=int(args.retention_days),
            now_ts_ms=int(args.now_ms) if args.now_ms else None,
            dry_run=bool(args.dry_run),
        )
        print(json.dumps(result, indent=2))
        return 0

    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
