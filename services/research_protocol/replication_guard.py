"""Cross-period and cross-symbol replication enforcement (RESEARCH_PROTOCOL §5).

Stage 6 (paper observation) requires independent replication: at least
two distinct test periods AND at least two distinct symbols, OR an
explicit, documented cross-symbol exemption. Each piece of evidence
must independently pass the statistical guardrails (PR3): n_eff at
floor, bootstrap CI excluding zero, permutation p below alpha.

Evidence lives in
``reports/research_protocol/replication_evidence.json`` and is mutated
only through :func:`record_replication_result` and
:func:`record_cross_symbol_exemption`. There is no public API to remove,
downgrade, or rewrite an entry; the file is rewritten atomically via
tempfile + ``os.replace``. Statistical verdicts inside each evidence
record are recomputed on read so a tamperer cannot flip
``statistical_pass`` to True without the underlying inputs supporting it.

Rules enforced:
  - The dedup key is ``(period_id, symbol)``. Re-recording the same
    key with the same ``report_path`` AND the same recomputed verdict
    is idempotent. Anything else raises
    :class:`ReplicationViolationError`.
  - Failed evidence (``statistical_pass=False`` after recompute) is
    retained in the file but does not count toward replication
    readiness.
  - Missing/blank ``report_path`` raises.
  - The first record fixes the candidate's ``registration_hash``;
    subsequent records must match.
  - Granting a cross-symbol exemption is permanent (idempotent on the
    same reason; differing reasons raise).
  - :func:`assert_replication_ready` requires at least
    :data:`MIN_DISTINCT_PERIODS` distinct passing period_ids AND at
    least :data:`MIN_DISTINCT_SYMBOLS` distinct passing symbols (or a
    granted exemption).
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.research_protocol._paths import replication_state_path
from services.research_protocol.audit_logger import safe_emit_audit_event
from services.research_protocol.errors import (
    ReplicationViolationError,
    StatisticalViolationError,
)
from services.research_protocol.registration import (
    Registration,
    load_registration,
)
from services.research_protocol.statistical_guard import (
    StatisticalVerdict,
    verdict_from_dict,
    verdict_to_dict,
)

REPLICATION_VERSION = 1
MIN_DISTINCT_PERIODS = 2
MIN_DISTINCT_SYMBOLS = 2

REQUIRED_EVIDENCE_INPUT_KEYS: tuple[str, ...] = (
    "period_id",
    "train_start",
    "train_end",
    "test_start",
    "test_end",
    "symbol",
    "report_path",
    "statistical_validity",
)

_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9_.\-]{0,15}$")
_PERIOD_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.\-:+=]{1,64}$")


# --------------------------------------------------------------------- #
# Data classes
# --------------------------------------------------------------------- #


@dataclass(frozen=True)
class ReplicationEvidence:
    candidate_id: str
    period_id: str
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    symbol: str
    report_path: str
    statistical_validity: dict[str, Any]
    recorded_at: str
    registration_hash: str

    @property
    def passed(self) -> bool:
        return bool(self.statistical_validity.get("statistical_pass", False))


@dataclass(frozen=True)
class CrossSymbolExemption:
    granted: bool
    reason: str
    recorded_at: str


@dataclass(frozen=True)
class ReplicationStatus:
    candidate_id: str
    registration_hash: str | None
    total_evidence: int
    passing_evidence: int
    failing_evidence: int
    distinct_passing_periods: tuple[str, ...]
    distinct_passing_symbols: tuple[str, ...]
    cross_symbol_exemption: CrossSymbolExemption | None
    meets_minimum_periods: bool
    meets_minimum_symbols: bool
    replication_ready: bool
    blocking_reasons: tuple[str, ...]


# --------------------------------------------------------------------- #
# State file IO
# --------------------------------------------------------------------- #


def _empty_payload() -> dict[str, Any]:
    return {"version": REPLICATION_VERSION, "candidates": {}}


def _read_state() -> dict[str, Any]:
    path = replication_state_path()
    if not path.exists():
        return _empty_payload()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ReplicationViolationError(
            f"replication_evidence at {path} is not valid JSON: {exc}"
        ) from exc
    _assert_state_shape(payload, path)
    return payload


def _assert_state_shape(payload: Any, path: Path) -> None:
    if not isinstance(payload, dict):
        raise ReplicationViolationError(
            f"replication_evidence at {path} must be a JSON object;"
            f" got {type(payload).__name__}"
        )
    if payload.get("version") != REPLICATION_VERSION:
        raise ReplicationViolationError(
            f"replication_evidence at {path} has version="
            f"{payload.get('version')!r}; expected {REPLICATION_VERSION}."
            " Refusing to proceed."
        )
    candidates = payload.get("candidates")
    if not isinstance(candidates, dict):
        raise ReplicationViolationError(
            f"replication_evidence at {path} 'candidates' field must be a"
            f" dict; got {type(candidates).__name__}"
        )
    for candidate_id, body in candidates.items():
        if not isinstance(candidate_id, str) or not candidate_id:
            raise ReplicationViolationError(
                f"candidate id key must be a non-empty string; got"
                f" {candidate_id!r}"
            )
        if not isinstance(body, dict):
            raise ReplicationViolationError(
                f"candidate {candidate_id!r} entry must be a dict;"
                f" got {type(body).__name__}"
            )
        if not isinstance(body.get("registration_hash"), str):
            raise ReplicationViolationError(
                f"candidate {candidate_id!r} missing registration_hash"
            )
        evidence = body.get("evidence")
        if not isinstance(evidence, list):
            raise ReplicationViolationError(
                f"candidate {candidate_id!r} 'evidence' must be a list;"
                f" got {type(evidence).__name__}"
            )
        for entry in evidence:
            if not isinstance(entry, dict):
                raise ReplicationViolationError(
                    f"candidate {candidate_id!r} evidence entry must be"
                    f" a dict; got {type(entry).__name__}"
                )
            for key in REQUIRED_EVIDENCE_INPUT_KEYS:
                if key not in entry:
                    raise ReplicationViolationError(
                        f"candidate {candidate_id!r} evidence missing"
                        f" required key {key!r}"
                    )
        exemption = body.get("cross_symbol_exemption")
        if exemption is not None and not isinstance(exemption, dict):
            raise ReplicationViolationError(
                f"candidate {candidate_id!r} cross_symbol_exemption must"
                f" be a dict or absent; got {type(exemption).__name__}"
            )


def _write_state_atomic(payload: dict[str, Any]) -> None:
    path = replication_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=".replication_evidence.", suffix=".json", dir=str(path.parent)
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


# --------------------------------------------------------------------- #
# Input validation
# --------------------------------------------------------------------- #


def _require_str(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ReplicationViolationError(
            f"{field} must be a non-empty string; got {value!r}"
        )
    return value


def _require_date(value: Any, *, field: str) -> str:
    s = _require_str(value, field=field)
    if not _DATE_PATTERN.match(s):
        raise ReplicationViolationError(
            f"{field} must match YYYY-MM-DD; got {s!r}"
        )
    return s


def _require_period_id(value: Any) -> str:
    s = _require_str(value, field="period_id")
    if not _PERIOD_ID_PATTERN.match(s):
        raise ReplicationViolationError(
            f"period_id must match {_PERIOD_ID_PATTERN.pattern};"
            f" got {s!r}"
        )
    return s


def _require_symbol(value: Any) -> str:
    s = _require_str(value, field="symbol")
    if not _SYMBOL_PATTERN.match(s):
        raise ReplicationViolationError(
            f"symbol must match {_SYMBOL_PATTERN.pattern};"
            f" got {s!r}"
        )
    return s


def _require_window(train_start: str, train_end: str, test_start: str, test_end: str) -> None:
    if train_end < train_start:
        raise ReplicationViolationError(
            f"train_end {train_end!r} must be >= train_start {train_start!r}"
        )
    if test_end < test_start:
        raise ReplicationViolationError(
            f"test_end {test_end!r} must be >= test_start {test_start!r}"
        )
    if test_start < train_end:
        raise ReplicationViolationError(
            f"test_start {test_start!r} must be >= train_end {train_end!r}"
            " (train and test windows must not overlap)"
        )


def _recompute_verdict(
    *,
    candidate_id: str,
    period_id: str,
    symbol: str,
    statistical_validity: dict[str, Any],
) -> StatisticalVerdict:
    """Re-derive the verdict from inputs in the stat block.

    Wraps :func:`statistical_guard.verdict_from_dict` and converts
    failure to :class:`ReplicationViolationError` with context (which
    candidate / period / symbol the malformed block came from).
    """
    try:
        return verdict_from_dict(statistical_validity)
    except StatisticalViolationError as exc:
        raise ReplicationViolationError(
            f"candidate {candidate_id!r} period_id={period_id!r}"
            f" symbol={symbol!r} statistical_validity block invalid: {exc}"
        ) from exc


def _entry_to_evidence(
    candidate_id: str,
    registration_hash: str,
    entry: dict[str, Any],
) -> ReplicationEvidence:
    return ReplicationEvidence(
        candidate_id=candidate_id,
        period_id=str(entry["period_id"]),
        train_start=str(entry["train_start"]),
        train_end=str(entry["train_end"]),
        test_start=str(entry["test_start"]),
        test_end=str(entry["test_end"]),
        symbol=str(entry["symbol"]),
        report_path=str(entry["report_path"]),
        statistical_validity=dict(entry["statistical_validity"]),
        recorded_at=str(entry.get("recorded_at", "")),
        registration_hash=registration_hash,
    )


# --------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------- #


def load_replication_evidence(candidate_id: str) -> list[ReplicationEvidence]:
    """Return all recorded evidence for the candidate (passing and failing)."""
    payload = _read_state()
    body = payload["candidates"].get(candidate_id)
    if body is None:
        return []
    reg_hash = body["registration_hash"]
    return [_entry_to_evidence(candidate_id, reg_hash, e) for e in body["evidence"]]


def get_cross_symbol_exemption(candidate_id: str) -> CrossSymbolExemption | None:
    payload = _read_state()
    body = payload["candidates"].get(candidate_id)
    if body is None:
        return None
    raw = body.get("cross_symbol_exemption")
    if not isinstance(raw, dict):
        return None
    granted = bool(raw.get("granted", False))
    if not granted:
        return None
    return CrossSymbolExemption(
        granted=True,
        reason=str(raw.get("reason", "")),
        recorded_at=str(raw.get("recorded_at", "")),
    )


def record_replication_result(
    *,
    candidate_id: str,
    period_id: str,
    train_start: str,
    train_end: str,
    test_start: str,
    test_end: str,
    symbol: str,
    report_path: str,
    statistical_validity: dict[str, Any],
) -> ReplicationEvidence:
    """Append a piece of replication evidence for the candidate.

    The dedup key is ``(period_id, symbol)``. Re-recording the same key
    with identical ``report_path`` AND identical recomputed verdict is
    idempotent. Any other re-record raises
    :class:`ReplicationViolationError`.

    The supplied ``statistical_validity`` block is *recomputed* via
    :func:`statistical_guard.verdict_from_dict`, so a payload that
    claims ``statistical_pass=True`` while its inputs (n_obs, ci_*,
    permutation_p_value) imply False will be stored with
    ``statistical_pass=False``. Failed evidence is recorded but does
    not count toward readiness.
    """
    period_id = _require_period_id(period_id)
    symbol = _require_symbol(symbol)
    report_path = _require_str(report_path, field="report_path")
    train_start = _require_date(train_start, field="train_start")
    train_end = _require_date(train_end, field="train_end")
    test_start = _require_date(test_start, field="test_start")
    test_end = _require_date(test_end, field="test_end")
    _require_window(train_start, train_end, test_start, test_end)
    if not isinstance(statistical_validity, dict):
        raise ReplicationViolationError(
            f"statistical_validity must be a dict; got"
            f" {type(statistical_validity).__name__}"
        )

    registration: Registration = load_registration(candidate_id)
    expected_hash = registration.registration_hash
    verdict = _recompute_verdict(
        candidate_id=candidate_id,
        period_id=period_id,
        symbol=symbol,
        statistical_validity=statistical_validity,
    )
    canonical_block = verdict_to_dict(verdict)

    payload = _read_state()
    candidate_block = payload["candidates"].setdefault(
        candidate_id,
        {
            "registration_hash": expected_hash,
            "evidence": [],
            "cross_symbol_exemption": None,
        },
    )
    if candidate_block["registration_hash"] != expected_hash:
        raise ReplicationViolationError(
            f"candidate {candidate_id!r} prior evidence used"
            f" registration_hash {candidate_block['registration_hash']!r}"
            f" but the current registration hash is {expected_hash!r}."
            " The registration changed; this is treated as a new"
            " candidate. Register a new candidate_id to continue."
        )
    # Dedup: same (period_id, symbol) must match exactly.
    for existing in candidate_block["evidence"]:
        if (
            existing["period_id"] == period_id
            and existing["symbol"] == symbol
        ):
            same_path = existing["report_path"] == report_path
            same_window = (
                existing["train_start"] == train_start
                and existing["train_end"] == train_end
                and existing["test_start"] == test_start
                and existing["test_end"] == test_end
            )
            existing_verdict = _recompute_verdict(
                candidate_id=candidate_id,
                period_id=period_id,
                symbol=symbol,
                statistical_validity=existing["statistical_validity"],
            )
            same_verdict = (
                existing_verdict.statistical_pass == verdict.statistical_pass
                and existing_verdict.metrics_suppressed == verdict.metrics_suppressed
                and existing_verdict.n_eff == verdict.n_eff
            )
            if same_path and same_window and same_verdict:
                return _entry_to_evidence(candidate_id, expected_hash, existing)
            raise ReplicationViolationError(
                f"candidate {candidate_id!r} evidence for"
                f" period_id={period_id!r} symbol={symbol!r} already"
                f" recorded with different content; replication evidence"
                " is append-only. Use a distinct period_id for a new"
                " run, or record the original report_path verbatim."
            )

    entry = {
        "period_id": period_id,
        "train_start": train_start,
        "train_end": train_end,
        "test_start": test_start,
        "test_end": test_end,
        "symbol": symbol,
        "report_path": report_path,
        "statistical_validity": canonical_block,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
    candidate_block["evidence"].append(entry)
    _write_state_atomic(payload)
    safe_emit_audit_event(
        event_type="replication_evidence_recorded",
        decision="record",
        candidate_id=candidate_id,
        reason=(
            f"period_id={period_id} symbol={symbol}"
            f" statistical_pass={verdict.statistical_pass}"
        ),
        registration_hash=expected_hash,
        report_path=report_path,
        metadata={
            "period_id": period_id,
            "symbol": symbol,
            "train_start": train_start,
            "train_end": train_end,
            "test_start": test_start,
            "test_end": test_end,
            "statistical_pass": bool(verdict.statistical_pass),
            "metrics_suppressed": bool(verdict.metrics_suppressed),
            "n_eff": int(verdict.n_eff),
        },
    )
    return _entry_to_evidence(candidate_id, expected_hash, entry)


def record_cross_symbol_exemption(
    *,
    candidate_id: str,
    reason: str,
) -> CrossSymbolExemption:
    """Grant the cross-symbol replication exemption.

    Permanent and idempotent: re-granting with the same ``reason``
    returns the existing exemption; granting with a different reason
    raises. There is no public API to revoke an exemption.
    """
    reason = _require_str(reason, field="reason")
    if len(reason) < 16:
        raise ReplicationViolationError(
            "exemption reason must be at least 16 characters explaining"
            " why a single-symbol replication is acceptable"
        )

    registration = load_registration(candidate_id)
    expected_hash = registration.registration_hash

    payload = _read_state()
    candidate_block = payload["candidates"].setdefault(
        candidate_id,
        {
            "registration_hash": expected_hash,
            "evidence": [],
            "cross_symbol_exemption": None,
        },
    )
    if candidate_block["registration_hash"] != expected_hash:
        raise ReplicationViolationError(
            f"candidate {candidate_id!r} prior evidence used"
            f" registration_hash {candidate_block['registration_hash']!r}"
            f" but the current registration hash is {expected_hash!r}."
            " Register a new candidate_id to continue."
        )
    existing = candidate_block.get("cross_symbol_exemption")
    if isinstance(existing, dict) and existing.get("granted"):
        if existing.get("reason") == reason:
            return CrossSymbolExemption(
                granted=True,
                reason=reason,
                recorded_at=str(existing.get("recorded_at", "")),
            )
        raise ReplicationViolationError(
            f"candidate {candidate_id!r} already has a granted"
            f" cross-symbol exemption with reason"
            f" {existing.get('reason')!r}; reasons cannot be changed."
        )
    record = {
        "granted": True,
        "reason": reason,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
    candidate_block["cross_symbol_exemption"] = record
    _write_state_atomic(payload)
    return CrossSymbolExemption(
        granted=True,
        reason=reason,
        recorded_at=record["recorded_at"],
    )


def summarize_replication_status(candidate_id: str) -> ReplicationStatus:
    """Return a snapshot suitable for printing or for the guard layer."""
    payload = _read_state()
    body = payload["candidates"].get(candidate_id)
    if body is None:
        return ReplicationStatus(
            candidate_id=candidate_id,
            registration_hash=None,
            total_evidence=0,
            passing_evidence=0,
            failing_evidence=0,
            distinct_passing_periods=(),
            distinct_passing_symbols=(),
            cross_symbol_exemption=None,
            meets_minimum_periods=False,
            meets_minimum_symbols=False,
            replication_ready=False,
            blocking_reasons=(
                "no replication evidence recorded for this candidate",
            ),
        )

    reg_hash = body["registration_hash"]
    evidence = [
        _entry_to_evidence(candidate_id, reg_hash, e) for e in body["evidence"]
    ]
    passing: list[ReplicationEvidence] = []
    failing: list[ReplicationEvidence] = []
    for ev in evidence:
        # Recompute verdict every time — never trust the stored flag.
        verdict = _recompute_verdict(
            candidate_id=candidate_id,
            period_id=ev.period_id,
            symbol=ev.symbol,
            statistical_validity=ev.statistical_validity,
        )
        if verdict.statistical_pass:
            passing.append(ev)
        else:
            failing.append(ev)

    distinct_periods = tuple(sorted({ev.period_id for ev in passing}))
    distinct_symbols = tuple(sorted({ev.symbol for ev in passing}))
    exemption_raw = body.get("cross_symbol_exemption")
    exemption = (
        CrossSymbolExemption(
            granted=True,
            reason=str(exemption_raw.get("reason", "")),
            recorded_at=str(exemption_raw.get("recorded_at", "")),
        )
        if isinstance(exemption_raw, dict) and exemption_raw.get("granted")
        else None
    )

    meets_periods = len(distinct_periods) >= MIN_DISTINCT_PERIODS
    meets_symbols = (
        len(distinct_symbols) >= MIN_DISTINCT_SYMBOLS
        or exemption is not None
    )
    blocking: list[str] = []
    if not meets_periods:
        blocking.append(
            f"distinct passing periods={len(distinct_periods)}"
            f" < required {MIN_DISTINCT_PERIODS}"
            f" (have: {list(distinct_periods)})"
        )
    if not meets_symbols:
        blocking.append(
            f"distinct passing symbols={len(distinct_symbols)}"
            f" < required {MIN_DISTINCT_SYMBOLS}"
            f" (have: {list(distinct_symbols)});"
            " no cross-symbol exemption granted"
        )

    return ReplicationStatus(
        candidate_id=candidate_id,
        registration_hash=reg_hash,
        total_evidence=len(evidence),
        passing_evidence=len(passing),
        failing_evidence=len(failing),
        distinct_passing_periods=distinct_periods,
        distinct_passing_symbols=distinct_symbols,
        cross_symbol_exemption=exemption,
        meets_minimum_periods=meets_periods,
        meets_minimum_symbols=meets_symbols,
        replication_ready=meets_periods and meets_symbols,
        blocking_reasons=tuple(blocking),
    )


def assert_replication_ready(candidate_id: str) -> None:
    """Raise :class:`ReplicationViolationError` unless replication ready."""
    status = summarize_replication_status(candidate_id)
    if status.replication_ready:
        return
    reasons = "; ".join(status.blocking_reasons or ("(no reason recorded)",))
    raise ReplicationViolationError(
        f"candidate {candidate_id!r} is not replication-ready: {reasons}."
        " Stage 6 (paper observation) requires at least"
        f" {MIN_DISTINCT_PERIODS} distinct passing periods AND at least"
        f" {MIN_DISTINCT_SYMBOLS} distinct passing symbols (or a granted"
        " cross-symbol exemption). Failed evidence does not count."
    )
