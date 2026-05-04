"""Kill-list enforcement (RESEARCH_PROTOCOL §6).

Append-only record of permanently-killed candidates. There is no public
API to remove or modify an entry; ``record_kill`` for a candidate that
already exists on the list is a no-op that returns the existing entry.

The on-disk file is rewritten atomically via tempfile + ``os.replace``
so that a crash mid-write cannot corrupt the list. The file carries a
version field; an unrecognized version raises
:class:`KillListTamperingError` rather than silently re-initializing.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.research_protocol._paths import kill_list_path
from services.research_protocol.audit_logger import safe_emit_audit_event
from services.research_protocol.errors import (
    CandidateKilledError,
    KillListTamperingError,
)

KILL_LIST_VERSION = 1
KILL_STAGE_MIN = 1
KILL_STAGE_MAX = 6


@dataclass(frozen=True)
class KillEntry:
    candidate_id: str
    registration_hash: str
    killed_at: str          # ISO8601 UTC
    killed_at_stage: int
    kill_reason: str
    supporting_artifacts: tuple[str, ...]


def _read_kill_list() -> dict[str, Any]:
    path = kill_list_path()
    if not path.exists():
        return {"version": KILL_LIST_VERSION, "entries": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise KillListTamperingError(
            f"kill_list at {path} is not valid JSON: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise KillListTamperingError(
            f"kill_list at {path} must be a JSON object; got"
            f" {type(payload).__name__}"
        )
    if payload.get("version") != KILL_LIST_VERSION:
        raise KillListTamperingError(
            f"kill_list at {path} has version={payload.get('version')!r};"
            f" expected {KILL_LIST_VERSION}. Refusing to proceed."
        )
    entries = payload.get("entries")
    if not isinstance(entries, list):
        raise KillListTamperingError(
            f"kill_list at {path} entries field must be a list;"
            f" got {type(entries).__name__}"
        )
    return payload


def _write_kill_list_atomic(payload: dict[str, Any]) -> None:
    path = kill_list_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=".kill_list.", suffix=".json", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp, path)
    except Exception:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp)
        raise


def _entry_from_dict(entry: dict[str, Any]) -> KillEntry:
    return KillEntry(
        candidate_id=str(entry["candidate_id"]),
        registration_hash=str(entry["registration_hash"]),
        killed_at=str(entry["killed_at"]),
        killed_at_stage=int(entry["killed_at_stage"]),
        kill_reason=str(entry["kill_reason"]),
        supporting_artifacts=tuple(entry.get("supporting_artifacts") or ()),
    )


def list_killed() -> list[KillEntry]:
    payload = _read_kill_list()
    return [_entry_from_dict(e) for e in payload["entries"]]


def is_killed(candidate_id: str) -> bool:
    for entry in list_killed():
        if entry.candidate_id == candidate_id:
            return True
    return False


def assert_not_killed(candidate_id: str) -> None:
    """Raise :class:`CandidateKilledError` if the candidate is on the list."""
    for entry in list_killed():
        if entry.candidate_id == candidate_id:
            raise CandidateKilledError(
                f"candidate_id={candidate_id!r} is on the kill list"
                f" (stage={entry.killed_at_stage},"
                f" killed_at={entry.killed_at},"
                f" reason={entry.kill_reason!r}). Reviving killed candidates"
                " is prohibited; register a new candidate with a different"
                " mechanism (RESEARCH_PROTOCOL §6.2)."
            )


def record_kill(
    *,
    candidate_id: str,
    registration_hash: str,
    stage: int,
    reason: str,
    artifacts: list[str] | None = None,
) -> KillEntry:
    """Append a kill entry. Idempotent on candidate_id.

    If ``candidate_id`` is already on the list, returns the existing
    entry without writing. The list is append-only by API: there is no
    function to remove or rewrite entries.
    """
    if not isinstance(stage, int) or not (KILL_STAGE_MIN <= stage <= KILL_STAGE_MAX):
        raise KillListTamperingError(
            f"kill stage must be int in [{KILL_STAGE_MIN}, {KILL_STAGE_MAX}];"
            f" got {stage!r}"
        )
    if not isinstance(candidate_id, str) or not candidate_id:
        raise KillListTamperingError(
            f"candidate_id must be a non-empty string; got {candidate_id!r}"
        )
    if not isinstance(registration_hash, str) or len(registration_hash) != 64:
        raise KillListTamperingError(
            f"registration_hash must be a 64-char hex string;"
            f" got {registration_hash!r}"
        )
    if not isinstance(reason, str) or not reason.strip():
        raise KillListTamperingError(
            "kill reason must be a non-empty string"
        )
    payload = _read_kill_list()
    for existing in payload["entries"]:
        if existing["candidate_id"] == candidate_id:
            return _entry_from_dict(existing)
    entry = {
        "candidate_id": candidate_id,
        "registration_hash": registration_hash,
        "killed_at": datetime.now(timezone.utc).isoformat(),
        "killed_at_stage": int(stage),
        "kill_reason": reason,
        "supporting_artifacts": list(artifacts or ()),
    }
    payload["entries"].append(entry)
    _write_kill_list_atomic(payload)
    safe_emit_audit_event(
        event_type="candidate_killed",
        decision="record",
        candidate_id=candidate_id,
        protocol_stage=int(stage),
        reason=reason,
        registration_hash=registration_hash,
        metadata={
            "killed_at": entry["killed_at"],
            "supporting_artifacts": list(entry["supporting_artifacts"]),
        },
    )
    return _entry_from_dict(entry)
