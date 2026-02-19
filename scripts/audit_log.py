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
        return {
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
        rows = conn.execute(
            """
            SELECT id, ts_ms, event_type, source, actor, host, commit_hash, message,
                   details_json, prev_hash, event_hash
            FROM ops_audit_events
            ORDER BY id ASC
            """
        ).fetchall()

        prev_hash = ""
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
        return {"status": "ok", "checked_events": checked}
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

    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
