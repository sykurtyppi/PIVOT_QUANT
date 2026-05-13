"""Audit logging + reproducibility fingerprints (RESEARCH_PROTOCOL §7).

Every protocol decision — registration loaded, registration rejected,
kill-list block, ladder block, statistical block, replication block,
protocol pass, stage result recorded, replication evidence recorded,
candidate killed — is appended as one JSON record per line to
``reports/research_protocol/audit_log.jsonl``.

The file is append-only. There is no public API to remove, clear, or
rewrite events. The on-disk format is JSON Lines so a corrupted line is
detectable on read; :class:`AuditLogTamperingError` is raised when any
line fails to parse, has missing required fields, or carries invalid
``event_type`` / ``decision`` / ``protocol_stage`` values.

Fingerprinting helpers produce deterministic SHA256 digests over:
  - file bytes (:func:`hash_file`)
  - dataset schema + content (:func:`hash_dataframe_schema_or_csv`)
  - the signal-defining slice of a registration
    (:func:`hash_signal_definition`)
  - a complete run fingerprint suitable for embedding in any artifact
    (:func:`build_run_fingerprint`)

Audit failures must NEVER mask a protocol decision. Callers wrap audit
emit calls inside a try/except around the original protocol error so
the original :class:`ProtocolViolationError` always propagates.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import subprocess
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

# pandas is imported lazily inside hash_dataframe_schema_or_csv so that
# environments that don't have pandas (e.g. the python-fast CI job) can still
# import this module and run all other protocol machinery without it.

from services.research_protocol._paths import audit_log_path, protocol_root
from services.research_protocol.errors import AuditLogTamperingError

AUDIT_LOG_VERSION = 1

EVENT_TYPES: frozenset[str] = frozenset({
    "registration_loaded",
    "registration_rejected",
    "kill_list_block",
    "ladder_block",
    "statistical_block",
    "replication_block",
    "trial_budget_block",
    "protocol_pass",
    "stage_result_recorded",
    "replication_evidence_recorded",
    "trial_recorded",
    "candidate_killed",
    # Serving-state observability (D2). These are operational, not
    # research-protocol decisions, but live in the same audit log so all
    # control-plane events are reviewable in one place. They have no
    # ``candidate_id`` (see CANDIDATE_OPTIONAL_EVENT_TYPES).
    "serving_state_changed",
    "predict_blocked_dormant",
})

DECISIONS: frozenset[str] = frozenset({"pass", "block", "record"})

REQUIRED_EVENT_FIELDS: tuple[str, ...] = (
    "event_id",
    "timestamp_utc",
    "event_type",
    "decision",
    "audit_log_version",
)

# Event types that may legitimately have a None candidate_id (e.g.,
# registration_rejected before the candidate is fully loaded). Other
# event types must carry a non-empty candidate_id.
CANDIDATE_OPTIONAL_EVENT_TYPES: frozenset[str] = frozenset({
    "registration_rejected",
    # D2 serving-state events have no candidate_id — they describe the
    # active manifest's live-serving status, not a single research candidate.
    "serving_state_changed",
    "predict_blocked_dormant",
})


# --------------------------------------------------------------------- #
# Data classes
# --------------------------------------------------------------------- #


@dataclass(frozen=True)
class AuditEvent:
    event_id: str
    timestamp_utc: str
    candidate_id: str | None
    event_type: str
    protocol_stage: int | None
    decision: str
    reason: str | None
    registration_hash: str | None
    dataset_hash: str | None
    signal_definition_hash: str | None
    report_path: str | None
    code_version: str | None
    metadata: dict[str, Any] = field(default_factory=dict)
    audit_log_version: int = AUDIT_LOG_VERSION


@dataclass(frozen=True)
class DatasetFingerprint:
    path: str
    sha256: str
    file_size_bytes: int
    row_count: int | None
    column_set_hash: str | None
    columns: tuple[str, ...] | None
    min_date: str | None
    max_date: str | None
    fingerprinted_at: str


# --------------------------------------------------------------------- #
# Time + git
# --------------------------------------------------------------------- #


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


_GIT_COMMIT_CACHE: dict[str, str | None] = {}


def detect_git_commit(*, cwd: Path | None = None) -> str | None:
    """Return the current git HEAD SHA, or None if git is unavailable.

    Cached per-cwd so repeated calls don't fork ``git rev-parse``.
    """
    key = str(Path(cwd).resolve()) if cwd else "_default_"
    if key in _GIT_COMMIT_CACHE:
        return _GIT_COMMIT_CACHE[key]
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            timeout=2,
        )
        if result.returncode == 0:
            sha = result.stdout.strip()
            _GIT_COMMIT_CACHE[key] = sha
            return sha
    except (OSError, subprocess.SubprocessError):
        pass
    _GIT_COMMIT_CACHE[key] = None
    return None


# --------------------------------------------------------------------- #
# JSONL append
# --------------------------------------------------------------------- #


_WRITE_LOCK = threading.Lock()


def _serialize_event(event: AuditEvent) -> str:
    payload = {
        "event_id": event.event_id,
        "timestamp_utc": event.timestamp_utc,
        "candidate_id": event.candidate_id,
        "event_type": event.event_type,
        "protocol_stage": event.protocol_stage,
        "decision": event.decision,
        "reason": event.reason,
        "registration_hash": event.registration_hash,
        "dataset_hash": event.dataset_hash,
        "signal_definition_hash": event.signal_definition_hash,
        "report_path": event.report_path,
        "code_version": event.code_version,
        "metadata": event.metadata,
        "audit_log_version": event.audit_log_version,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)


def _append_line(line: str) -> None:
    """Append a single line to the audit log under an OS-level file lock."""
    path = audit_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with _WRITE_LOCK:
        # Open in append-binary mode so writes hit the OS append-position
        # atomically per write(); also acquire an OS advisory lock to
        # serialize against other processes.
        with open(path, "ab") as fh:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
                fh.write(line.encode("utf-8") + b"\n")
                fh.flush()
                os.fsync(fh.fileno())
            finally:
                with contextlib.suppress(OSError):
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


# --------------------------------------------------------------------- #
# Public emit + load
# --------------------------------------------------------------------- #


def emit_audit_event(
    *,
    event_type: str,
    decision: str,
    candidate_id: str | None = None,
    protocol_stage: int | None = None,
    reason: str | None = None,
    registration_hash: str | None = None,
    dataset_hash: str | None = None,
    signal_definition_hash: str | None = None,
    report_path: str | None = None,
    code_version: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> AuditEvent:
    """Append a single event to the audit log JSONL and return it.

    Raises :class:`AuditLogTamperingError` only on input validation
    failure (invalid event_type / decision / protocol_stage). Filesystem
    failures propagate as the underlying ``OSError`` so callers can
    decide whether to suppress them — protocol-decision callers must
    always preserve the original :class:`ProtocolViolationError`.
    """
    if event_type not in EVENT_TYPES:
        raise AuditLogTamperingError(
            f"event_type must be one of {sorted(EVENT_TYPES)};"
            f" got {event_type!r}"
        )
    if decision not in DECISIONS:
        raise AuditLogTamperingError(
            f"decision must be one of {sorted(DECISIONS)};"
            f" got {decision!r}"
        )
    if protocol_stage is not None:
        if (
            not isinstance(protocol_stage, int)
            or isinstance(protocol_stage, bool)
            or not (0 <= protocol_stage <= 6)
        ):
            raise AuditLogTamperingError(
                f"protocol_stage must be int in [0, 6] or None;"
                f" got {protocol_stage!r}"
            )
    if event_type not in CANDIDATE_OPTIONAL_EVENT_TYPES:
        if not isinstance(candidate_id, str) or not candidate_id:
            raise AuditLogTamperingError(
                f"event_type={event_type!r} requires a non-empty"
                f" candidate_id; got {candidate_id!r}"
            )
    code_version = code_version if code_version is not None else detect_git_commit(
        cwd=protocol_root().parent if protocol_root().exists() else None
    )
    event = AuditEvent(
        event_id=uuid.uuid4().hex,
        timestamp_utc=_utc_now_iso(),
        candidate_id=candidate_id,
        event_type=event_type,
        protocol_stage=protocol_stage,
        decision=decision,
        reason=reason,
        registration_hash=registration_hash,
        dataset_hash=dataset_hash,
        signal_definition_hash=signal_definition_hash,
        report_path=report_path,
        code_version=code_version,
        metadata=dict(metadata or {}),
    )
    _append_line(_serialize_event(event))
    return event


def safe_emit_audit_event(**kwargs: Any) -> AuditEvent | None:
    """Emit an audit event, swallowing transient/filesystem failures.

    Re-raises :class:`AuditLogTamperingError` (developer-error: malformed
    event payload) so bugs surface in tests and CI, but never masks the
    underlying protocol decision with an OSError or similar. Returns the
    emitted event on success or None when a transient error was
    swallowed.
    """
    try:
        return emit_audit_event(**kwargs)
    except AuditLogTamperingError:
        raise
    except Exception:
        return None


def _validate_event_record(record: Any, *, line_index: int) -> AuditEvent:
    if not isinstance(record, dict):
        raise AuditLogTamperingError(
            f"audit log line {line_index} is not a JSON object;"
            f" got {type(record).__name__}"
        )
    missing = [k for k in REQUIRED_EVENT_FIELDS if k not in record]
    if missing:
        raise AuditLogTamperingError(
            f"audit log line {line_index} missing required keys: {missing}"
        )
    version = record.get("audit_log_version")
    if version != AUDIT_LOG_VERSION:
        raise AuditLogTamperingError(
            f"audit log line {line_index} has audit_log_version="
            f"{version!r}; expected {AUDIT_LOG_VERSION}"
        )
    if record["event_type"] not in EVENT_TYPES:
        raise AuditLogTamperingError(
            f"audit log line {line_index} event_type="
            f"{record['event_type']!r} not in {sorted(EVENT_TYPES)}"
        )
    if record["decision"] not in DECISIONS:
        raise AuditLogTamperingError(
            f"audit log line {line_index} decision={record['decision']!r}"
            f" not in {sorted(DECISIONS)}"
        )
    ts = record["timestamp_utc"]
    if not isinstance(ts, str) or not ts:
        raise AuditLogTamperingError(
            f"audit log line {line_index} timestamp_utc must be a non-empty"
            f" string; got {ts!r}"
        )
    try:
        parsed = datetime.fromisoformat(ts)
    except ValueError as exc:
        raise AuditLogTamperingError(
            f"audit log line {line_index} timestamp_utc {ts!r} is not"
            f" a valid ISO8601 string: {exc}"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(None):
        raise AuditLogTamperingError(
            f"audit log line {line_index} timestamp_utc {ts!r} is not in"
            " UTC (must carry +00:00 offset)"
        )
    stage = record.get("protocol_stage")
    if stage is not None and (
        not isinstance(stage, int)
        or isinstance(stage, bool)
        or not (0 <= stage <= 6)
    ):
        raise AuditLogTamperingError(
            f"audit log line {line_index} protocol_stage={stage!r}"
            " must be int in [0,6] or null"
        )
    eid = record["event_id"]
    if not isinstance(eid, str) or not eid:
        raise AuditLogTamperingError(
            f"audit log line {line_index} event_id must be a non-empty"
            f" string; got {eid!r}"
        )
    return AuditEvent(
        event_id=str(record["event_id"]),
        timestamp_utc=str(record["timestamp_utc"]),
        candidate_id=record.get("candidate_id"),
        event_type=str(record["event_type"]),
        protocol_stage=record.get("protocol_stage"),
        decision=str(record["decision"]),
        reason=record.get("reason"),
        registration_hash=record.get("registration_hash"),
        dataset_hash=record.get("dataset_hash"),
        signal_definition_hash=record.get("signal_definition_hash"),
        report_path=record.get("report_path"),
        code_version=record.get("code_version"),
        metadata=dict(record.get("metadata") or {}),
        audit_log_version=int(version),
    )


def load_audit_events(
    *,
    candidate_id: str | None = None,
    event_type: str | None = None,
) -> list[AuditEvent]:
    """Read and validate every event in the audit log.

    Raises :class:`AuditLogTamperingError` on any malformed line. With
    no filters, returns every event in chronological order.
    """
    path = audit_log_path()
    if not path.exists():
        return []
    events: list[AuditEvent] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line_index, raw in enumerate(fh, start=1):
            stripped = raw.rstrip("\n")
            if not stripped:
                continue
            try:
                record = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise AuditLogTamperingError(
                    f"audit log line {line_index} is not valid JSON: {exc}"
                ) from exc
            event = _validate_event_record(record, line_index=line_index)
            if candidate_id is not None and event.candidate_id != candidate_id:
                continue
            if event_type is not None and event.event_type != event_type:
                continue
            events.append(event)
    return events


# --------------------------------------------------------------------- #
# Fingerprint helpers
# --------------------------------------------------------------------- #


def hash_file(path: str | os.PathLike[str]) -> str:
    """SHA256 of the file at ``path``. Streams in 64 KiB chunks."""
    p = Path(path).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"file not found: {p}")
    if not p.is_file():
        raise IsADirectoryError(f"not a regular file: {p}")
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


_DATE_COLUMN_CANDIDATES: tuple[str, ...] = (
    "entry_date",
    "date",
    "observation_date",
    "trade_date",
)


def hash_dataframe_schema_or_csv(
    path: str | os.PathLike[str],
) -> DatasetFingerprint:
    """Fingerprint a parquet or CSV dataset for reproducibility.

    Returns:
      :class:`DatasetFingerprint` with SHA256 of file bytes, file size,
      row count, sorted column-set hash, columns, and (when a date-like
      column exists) the min/max date observed.
    """
    p = Path(path).expanduser().resolve()
    if not p.exists():
        raise FileNotFoundError(f"file not found: {p}")
    if not p.is_file():
        raise IsADirectoryError(f"not a regular file: {p}")
    sha = hash_file(p)
    size = p.stat().st_size
    suffix = p.suffix.lower()
    row_count: int | None = None
    columns: tuple[str, ...] | None = None
    column_set_hash: str | None = None
    min_date: str | None = None
    max_date: str | None = None
    df = None
    # pandas is lazy-imported here so callers in environments without pandas
    # (e.g. CI runners that only have scikit-learn) can still import this module
    # and exercise all other protocol machinery.
    if suffix in (".parquet", ".csv"):
        try:
            import pandas as pd  # noqa: PLC0415
            df = pd.read_parquet(p) if suffix == ".parquet" else pd.read_csv(p)
        except Exception:
            df = None
    if df is not None:
        row_count = int(len(df))
        cols_sorted = tuple(sorted(map(str, df.columns)))
        columns = cols_sorted
        column_set_hash = hashlib.sha256(
            json.dumps(list(cols_sorted), separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        date_col = next(
            (c for c in _DATE_COLUMN_CANDIDATES if c in df.columns),
            None,
        )
        if date_col is not None and len(df) > 0:
            try:
                import pandas as pd  # noqa: PLC0415,F811
                dates = pd.to_datetime(df[date_col], errors="coerce").dropna()
                if len(dates):
                    min_date = str(dates.min().date())
                    max_date = str(dates.max().date())
            except Exception:
                pass
    return DatasetFingerprint(
        path=str(p),
        sha256=sha,
        file_size_bytes=int(size),
        row_count=row_count,
        column_set_hash=column_set_hash,
        columns=columns,
        min_date=min_date,
        max_date=max_date,
        fingerprinted_at=_utc_now_iso(),
    )


def hash_signal_definition(registration_body: dict[str, Any]) -> str:
    """SHA256 over the signal-defining slice of a registration body.

    This isolates the *behaviour-determining* fields — features,
    thresholds, transformations, forbidden_changes, falsification,
    horizon, hypothesis mechanism + direction — so that audit events
    can reference a stable identifier for "the signal as specified."
    Distinct from :func:`registration.compute_registration_hash` which
    covers the entire pre-registration document including metadata.
    """
    if not isinstance(registration_body, dict):
        raise AuditLogTamperingError(
            f"registration_body must be a dict; got"
            f" {type(registration_body).__name__}"
        )
    hypothesis = registration_body.get("hypothesis") or {}
    canonical = {
        "hypothesis_mechanism": hypothesis.get("mechanism"),
        "predicted_direction": hypothesis.get("predicted_direction"),
        "features": registration_body.get("features"),
        "thresholds": registration_body.get("thresholds"),
        "transformations": registration_body.get("transformations"),
        "forbidden_changes": registration_body.get("forbidden_changes"),
        "falsification": registration_body.get("falsification"),
        "horizon_days": registration_body.get("horizon_days"),
    }
    canonical_str = json.dumps(
        canonical, sort_keys=True, separators=(",", ":"), default=str
    )
    return hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()


def build_run_fingerprint(
    *,
    registration_body: dict[str, Any],
    registration_hash: str,
    datasets: Iterable[str | os.PathLike[str]] | None = None,
    code_version: str | None = None,
) -> dict[str, Any]:
    """Build a fingerprint suitable for embedding in any artifact.

    Includes ``registration_hash``, ``signal_definition_hash``,
    per-dataset :class:`DatasetFingerprint` records,
    ``code_version`` (auto-detected via git when None), and
    ``fingerprinted_at`` UTC timestamp.
    """
    dataset_records: list[dict[str, Any]] = []
    for ds_path in datasets or ():
        fp = hash_dataframe_schema_or_csv(ds_path)
        dataset_records.append({
            "path": fp.path,
            "sha256": fp.sha256,
            "file_size_bytes": fp.file_size_bytes,
            "row_count": fp.row_count,
            "column_set_hash": fp.column_set_hash,
            "columns": list(fp.columns) if fp.columns is not None else None,
            "min_date": fp.min_date,
            "max_date": fp.max_date,
            "fingerprinted_at": fp.fingerprinted_at,
        })
    return {
        "registration_hash": registration_hash,
        "signal_definition_hash": hash_signal_definition(registration_body),
        "datasets": dataset_records,
        "code_version": (
            code_version if code_version is not None else detect_git_commit()
        ),
        "fingerprinted_at": _utc_now_iso(),
    }
