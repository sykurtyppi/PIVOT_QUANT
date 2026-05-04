"""Researcher degrees-of-freedom + N_trials accounting (RESEARCH_PROTOCOL §8).

Every registration that enters the protocol counts as a trial. Trials
are partitioned by ``hypothesis_family`` and by the calendar quarter of
the registration's ``registration_timestamp``. The default budget is
:data:`MAX_TRIALS_PER_FAMILY_PER_QUARTER` (=3); the 4th trial in a given
family/quarter is recorded for audit history but blocks any
:func:`assert_protocol_compliant` call that names it.

Each trial carries a :data:`MODIFICATION_TYPES` classification computed
from the relationship between the new registration and its declared
``parent_candidate_id``:

  - ``new_hypothesis``      — no parent, or explicitly declared and
                              the parent is in a *different*
                              ``hypothesis_family``.
  - ``feature_change``      — feature names differ from the parent.
  - ``threshold_change``    — features identical, thresholds differ.
  - ``period_change``       — dataset windows differ (and symbol same).
  - ``symbol_change``       — datasets.symbol differs.
  - ``parameter_change``    — horizon_days, random_seed, or other
                              parameters differ; nothing structural.
  - ``revival_attempt``     — parent is on the kill list AND the new
                              registration is *not* pre-registered
                              before the parent's failure AND the user
                              has not declared a different
                              ``hypothesis_family``.

Revival attempts are blocked unconditionally — they are the canonical
data-snooping pattern that PR23–PR42 produced. Pre-registered alternates
(new registrations whose ``registration_timestamp`` precedes the
parent's ``killed_at``) are exempt because their existence cannot have
been informed by the failure.

State lives in ``reports/research_protocol/trial_budget_state.json`` and
is rewritten atomically. There is no public API to remove, downgrade,
or rewrite an entry; the on-disk file is version-checked and tampered
records raise :class:`TrialBudgetViolationError` on read.
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

from services.research_protocol._paths import trial_budget_state_path
from services.research_protocol.audit_logger import (
    hash_signal_definition,
    safe_emit_audit_event,
)
from services.research_protocol.errors import (
    CandidateKilledError,
    TrialBudgetViolationError,
)
from services.research_protocol.kill_list import is_killed, list_killed
from services.research_protocol.registration import (
    Registration,
    load_registration,
)

TRIAL_BUDGET_VERSION = 1
MAX_TRIALS_PER_FAMILY_PER_QUARTER = 3
DEFAULT_HYPOTHESIS_FAMILY = "unspecified"

MODIFICATION_TYPES: frozenset[str] = frozenset({
    "new_hypothesis",
    "parameter_change",
    "feature_change",
    "threshold_change",
    "period_change",
    "symbol_change",
    "revival_attempt",
})

ALLOWED_CLAIMED_MODIFICATION_TYPES: frozenset[str] = MODIFICATION_TYPES

_HYPOTHESIS_FAMILY_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


# --------------------------------------------------------------------- #
# Data classes
# --------------------------------------------------------------------- #


@dataclass(frozen=True)
class TrialEntry:
    candidate_id: str
    parent_candidate_id: str | None
    signal_definition_hash: str
    registration_hash: str
    created_at: str
    recorded_at: str
    status: str
    hypothesis_family: str
    modification_type: str


@dataclass(frozen=True)
class TrialBudgetSummary:
    hypothesis_family: str
    quarter: str
    trial_count: int
    quarter_budget: int
    budget_remaining: int
    in_budget_trials: tuple[TrialEntry, ...]
    over_budget_trials: tuple[TrialEntry, ...]
    revival_attempts: tuple[TrialEntry, ...]


# --------------------------------------------------------------------- #
# State file IO
# --------------------------------------------------------------------- #


def _empty_payload() -> dict[str, Any]:
    return {"version": TRIAL_BUDGET_VERSION, "trials": []}


def _read_state() -> dict[str, Any]:
    path = trial_budget_state_path()
    if not path.exists():
        return _empty_payload()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise TrialBudgetViolationError(
            f"trial_budget_state at {path} is not valid JSON: {exc}"
        ) from exc
    _assert_state_shape(payload, path)
    return payload


def _assert_state_shape(payload: Any, path: Path) -> None:
    if not isinstance(payload, dict):
        raise TrialBudgetViolationError(
            f"trial_budget_state at {path} must be a JSON object;"
            f" got {type(payload).__name__}"
        )
    if payload.get("version") != TRIAL_BUDGET_VERSION:
        raise TrialBudgetViolationError(
            f"trial_budget_state at {path} has version="
            f"{payload.get('version')!r}; expected"
            f" {TRIAL_BUDGET_VERSION}. Refusing to proceed."
        )
    trials = payload.get("trials")
    if not isinstance(trials, list):
        raise TrialBudgetViolationError(
            f"trial_budget_state at {path} 'trials' field must be a"
            f" list; got {type(trials).__name__}"
        )
    for entry in trials:
        if not isinstance(entry, dict):
            raise TrialBudgetViolationError(
                f"trial_budget_state at {path} entry must be a dict;"
                f" got {type(entry).__name__}"
            )
        for key in (
            "candidate_id",
            "registration_hash",
            "created_at",
            "hypothesis_family",
            "modification_type",
        ):
            if key not in entry:
                raise TrialBudgetViolationError(
                    f"trial_budget_state at {path} entry missing"
                    f" required key {key!r}"
                )
        if entry["modification_type"] not in MODIFICATION_TYPES:
            raise TrialBudgetViolationError(
                f"trial_budget_state at {path} entry has unknown"
                f" modification_type={entry['modification_type']!r};"
                f" expected one of {sorted(MODIFICATION_TYPES)}"
            )


def _write_state_atomic(payload: dict[str, Any]) -> None:
    path = trial_budget_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=".trial_budget_state.", suffix=".json", dir=str(path.parent)
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
# Quarter / time helpers
# --------------------------------------------------------------------- #


def quarter_for_timestamp(iso_timestamp: str) -> str:
    """Return ``YYYY-Q[1-4]`` for the calendar quarter of the timestamp."""
    if not isinstance(iso_timestamp, str) or not iso_timestamp:
        raise TrialBudgetViolationError(
            f"timestamp must be a non-empty ISO8601 string; got {iso_timestamp!r}"
        )
    # Accept the trailing-Z UTC convention as well as +00:00.
    parsed_str = iso_timestamp.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(parsed_str)
    except ValueError as exc:
        raise TrialBudgetViolationError(
            f"timestamp {iso_timestamp!r} is not a valid ISO8601 string: {exc}"
        ) from exc
    quarter = (dt.month - 1) // 3 + 1
    return f"{dt.year}-Q{quarter}"


def _to_trial_entry(record: dict[str, Any]) -> TrialEntry:
    return TrialEntry(
        candidate_id=str(record["candidate_id"]),
        parent_candidate_id=record.get("parent_candidate_id"),
        signal_definition_hash=str(record.get("signal_definition_hash", "")),
        registration_hash=str(record["registration_hash"]),
        created_at=str(record["created_at"]),
        recorded_at=str(record.get("recorded_at", "")),
        status=str(record.get("status", "registered")),
        hypothesis_family=str(record["hypothesis_family"]),
        modification_type=str(record["modification_type"]),
    )


# --------------------------------------------------------------------- #
# Classification
# --------------------------------------------------------------------- #


def _feature_signature(reg: Registration) -> tuple[str, ...]:
    features = reg.body.get("features") or []
    names: list[str] = []
    for f in features:
        if isinstance(f, dict):
            name = f.get("name")
            if isinstance(name, str):
                names.append(name)
    return tuple(sorted(names))


def _threshold_signature(reg: Registration) -> tuple[tuple[str, str, Any], ...]:
    thresholds = reg.body.get("thresholds") or []
    sig: list[tuple[str, str, Any]] = []
    for t in thresholds:
        if not isinstance(t, dict):
            continue
        name = str(t.get("name", ""))
        kind = str(t.get("kind", ""))
        # Capture whichever value-shaped field is present
        value = t.get("value", t.get("quantile", t.get("source")))
        sig.append((name, kind, value))
    return tuple(sorted(sig, key=lambda x: x[0]))


def _datasets_block(reg: Registration) -> dict[str, Any]:
    block = reg.body.get("datasets")
    return dict(block) if isinstance(block, dict) else {}


def _is_symbol_change(new_block: dict[str, Any], old_block: dict[str, Any]) -> bool:
    new_sym = new_block.get("symbol")
    old_sym = old_block.get("symbol")
    if isinstance(new_sym, str) and isinstance(old_sym, str):
        return new_sym != old_sym
    return False


def _kill_record(candidate_id: str) -> dict[str, Any] | None:
    for entry in list_killed():
        if entry.candidate_id == candidate_id:
            return {
                "killed_at": entry.killed_at,
                "killed_at_stage": entry.killed_at_stage,
                "kill_reason": entry.kill_reason,
            }
    return None


def _classify_diff(new_reg: Registration, prior_reg: Registration) -> str:
    new_features = _feature_signature(new_reg)
    old_features = _feature_signature(prior_reg)
    if new_features != old_features:
        return "feature_change"

    if _threshold_signature(new_reg) != _threshold_signature(prior_reg):
        return "threshold_change"

    new_ds = _datasets_block(new_reg)
    old_ds = _datasets_block(prior_reg)
    if new_ds != old_ds:
        if _is_symbol_change(new_ds, old_ds):
            return "symbol_change"
        return "period_change"

    if new_reg.body.get("horizon_days") != prior_reg.body.get("horizon_days"):
        return "parameter_change"
    if new_reg.body.get("random_seed") != prior_reg.body.get("random_seed"):
        return "parameter_change"
    if (
        (new_reg.body.get("transformations") or {})
        != (prior_reg.body.get("transformations") or {})
    ):
        return "parameter_change"

    return "parameter_change"


def classify_candidate_change(
    new_registration: Registration,
    prior_registration: Registration,
) -> str:
    """Return the :data:`MODIFICATION_TYPES` value describing the change.

    Rules:
      - If the prior is **not** on the kill list, the diff between
        registrations determines the type
        (feature/threshold/period/symbol/parameter).
      - If the prior is on the kill list **and** the new registration
        was created before the parent was killed, the diff is returned
        unchanged (legitimate pre-registered alternate).
      - Otherwise, the new registration is a ``revival_attempt`` unless
        the user has declared ``claimed_modification_type="new_hypothesis"``
        AND the new registration's ``hypothesis_family`` differs from
        the parent's ``hypothesis_family``.
    """
    diff_type = _classify_diff(new_registration, prior_registration)

    parent_kill = _kill_record(prior_registration.candidate_id)
    if parent_kill is None:
        return diff_type

    new_created = new_registration.body.get("registration_timestamp")
    parent_killed_at = parent_kill["killed_at"]
    if (
        isinstance(new_created, str)
        and isinstance(parent_killed_at, str)
        and new_created < parent_killed_at
    ):
        # Pre-registered before the parent's failure: legitimate alternate.
        return diff_type

    new_family = new_registration.body.get("hypothesis_family")
    parent_family = prior_registration.body.get("hypothesis_family")
    claim = new_registration.body.get("claimed_modification_type")
    if (
        claim == "new_hypothesis"
        and isinstance(new_family, str)
        and new_family
        and isinstance(parent_family, str)
        and parent_family
        and new_family != parent_family
    ):
        return "new_hypothesis"

    return "revival_attempt"


# --------------------------------------------------------------------- #
# Validators
# --------------------------------------------------------------------- #


def _resolve_hypothesis_family(reg: Registration) -> str:
    family = reg.body.get("hypothesis_family", DEFAULT_HYPOTHESIS_FAMILY)
    if not isinstance(family, str) or not family:
        raise TrialBudgetViolationError(
            f"candidate {reg.candidate_id!r} hypothesis_family must be a"
            f" non-empty string; got {family!r}"
        )
    if not _HYPOTHESIS_FAMILY_PATTERN.match(family):
        raise TrialBudgetViolationError(
            f"candidate {reg.candidate_id!r} hypothesis_family must match"
            f" {_HYPOTHESIS_FAMILY_PATTERN.pattern}; got {family!r}"
        )
    return family


def _resolve_claimed_modification(reg: Registration) -> str | None:
    claim = reg.body.get("claimed_modification_type")
    if claim is None:
        return None
    if claim not in ALLOWED_CLAIMED_MODIFICATION_TYPES:
        raise TrialBudgetViolationError(
            f"candidate {reg.candidate_id!r} claimed_modification_type="
            f"{claim!r} is not one of"
            f" {sorted(ALLOWED_CLAIMED_MODIFICATION_TYPES)}"
        )
    return claim


# --------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------- #


def load_trial_state() -> dict[str, Any]:
    """Return the full trial state. Strict version + schema check."""
    return _read_state()


def list_trials() -> list[TrialEntry]:
    """All recorded trials in insertion order."""
    payload = _read_state()
    return [_to_trial_entry(t) for t in payload["trials"]]


def get_trial(candidate_id: str) -> TrialEntry | None:
    for entry in list_trials():
        if entry.candidate_id == candidate_id:
            return entry
    return None


def record_trial(registration: Registration) -> TrialEntry:
    """Append a trial entry for the given registration. Idempotent on
    ``(candidate_id, registration_hash)``.

    Re-recording the same ``candidate_id`` with a different
    ``registration_hash`` raises :class:`TrialBudgetViolationError`
    (registration drift; treat as a new candidate).
    """
    if not isinstance(registration, Registration):
        raise TrialBudgetViolationError(
            f"registration must be a Registration; got"
            f" {type(registration).__name__}"
        )
    body = registration.body
    candidate_id = registration.candidate_id
    registration_hash = registration.registration_hash
    family = _resolve_hypothesis_family(registration)
    _resolve_claimed_modification(registration)  # validate; not used here
    parent_id = body.get("parent_candidate_id")
    if parent_id is not None and (
        not isinstance(parent_id, str) or not parent_id
    ):
        raise TrialBudgetViolationError(
            f"parent_candidate_id must be None or a non-empty string;"
            f" got {parent_id!r}"
        )

    payload = _read_state()
    for existing in payload["trials"]:
        if existing["candidate_id"] != candidate_id:
            continue
        if existing["registration_hash"] == registration_hash:
            return _to_trial_entry(existing)
        raise TrialBudgetViolationError(
            f"candidate {candidate_id!r} previously recorded with"
            f" registration_hash {existing['registration_hash']!r};"
            f" current hash {registration_hash!r} differs. The"
            " registration changed; register a new candidate_id rather"
            " than re-using the existing one."
        )

    if parent_id is not None:
        try:
            prior_registration = load_registration(parent_id)
        except Exception as exc:
            raise TrialBudgetViolationError(
                f"candidate {candidate_id!r} declares parent_candidate_id="
                f"{parent_id!r} but loading that registration failed: {exc}"
            ) from exc
        modification_type = classify_candidate_change(
            registration, prior_registration
        )
    else:
        modification_type = "new_hypothesis"

    created_at = body.get("registration_timestamp")
    if not isinstance(created_at, str) or not created_at:
        raise TrialBudgetViolationError(
            f"candidate {candidate_id!r} registration_timestamp must be a"
            f" non-empty string; got {created_at!r}"
        )
    # Validate the timestamp parses; quarter_for_timestamp will be called
    # for budget computation and would raise downstream otherwise.
    quarter_for_timestamp(created_at)

    entry = {
        "candidate_id": candidate_id,
        "parent_candidate_id": parent_id,
        "signal_definition_hash": hash_signal_definition(body),
        "registration_hash": registration_hash,
        "created_at": created_at,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "status": "registered",
        "hypothesis_family": family,
        "modification_type": modification_type,
    }
    payload["trials"].append(entry)
    _write_state_atomic(payload)
    safe_emit_audit_event(
        event_type="trial_recorded",
        decision="record",
        candidate_id=candidate_id,
        registration_hash=registration_hash,
        signal_definition_hash=entry["signal_definition_hash"],
        reason=f"modification_type={modification_type} family={family}",
        metadata={
            "modification_type": modification_type,
            "hypothesis_family": family,
            "parent_candidate_id": parent_id,
            "quarter": quarter_for_timestamp(created_at),
        },
    )
    return _to_trial_entry(entry)


def summarize_trial_budget(
    hypothesis_family: str,
    *,
    quarter: str | None = None,
    reference_timestamp: str | None = None,
) -> TrialBudgetSummary:
    """Return a snapshot for the family in the given quarter.

    ``quarter`` defaults to the quarter of ``reference_timestamp`` (or
    ``datetime.now()`` if neither is given).
    """
    if quarter is None:
        ref = reference_timestamp or datetime.now(timezone.utc).isoformat()
        quarter = quarter_for_timestamp(ref)
    payload = _read_state()
    family_quarter_trials = [
        _to_trial_entry(t)
        for t in payload["trials"]
        if t["hypothesis_family"] == hypothesis_family
        and quarter_for_timestamp(t["created_at"]) == quarter
    ]
    in_budget = tuple(
        t for i, t in enumerate(family_quarter_trials)
        if i < MAX_TRIALS_PER_FAMILY_PER_QUARTER
    )
    over_budget = tuple(
        t for i, t in enumerate(family_quarter_trials)
        if i >= MAX_TRIALS_PER_FAMILY_PER_QUARTER
    )
    revival_attempts = tuple(
        t for t in family_quarter_trials
        if t.modification_type == "revival_attempt"
    )
    remaining = max(
        0,
        MAX_TRIALS_PER_FAMILY_PER_QUARTER - len(family_quarter_trials),
    )
    return TrialBudgetSummary(
        hypothesis_family=hypothesis_family,
        quarter=quarter,
        trial_count=len(family_quarter_trials),
        quarter_budget=MAX_TRIALS_PER_FAMILY_PER_QUARTER,
        budget_remaining=remaining,
        in_budget_trials=in_budget,
        over_budget_trials=over_budget,
        revival_attempts=revival_attempts,
    )


def assert_trial_budget_available(candidate_id: str) -> None:
    """Raise :class:`TrialBudgetViolationError` if the candidate cannot
    use the protocol due to trial-budget rules.

    Conditions that raise:
      - The candidate is recorded as a ``revival_attempt``.
      - The candidate's position in its (family, quarter) bucket is
        beyond :data:`MAX_TRIALS_PER_FAMILY_PER_QUARTER`.

    A candidate that has not yet been recorded is allowed (the caller
    is expected to record_trial first; protocol_guard does this in
    sequence).
    """
    payload = _read_state()
    trial = next(
        (t for t in payload["trials"] if t["candidate_id"] == candidate_id),
        None,
    )
    if trial is None:
        return

    if trial["modification_type"] == "revival_attempt":
        raise TrialBudgetViolationError(
            f"candidate {candidate_id!r} is a revival_attempt of a"
            " killed candidate; revival is prohibited under"
            " RESEARCH_PROTOCOL §6.2 / §8."
            " Register a new candidate with a different mechanism and a"
            " distinct hypothesis_family."
        )
    if trial["modification_type"] not in MODIFICATION_TYPES:
        raise TrialBudgetViolationError(
            f"candidate {candidate_id!r} has unknown modification_type"
            f" {trial['modification_type']!r}; refusing to proceed."
        )

    family = trial["hypothesis_family"]
    quarter = quarter_for_timestamp(trial["created_at"])
    family_quarter_trials = [
        t for t in payload["trials"]
        if t["hypothesis_family"] == family
        and quarter_for_timestamp(t["created_at"]) == quarter
    ]
    # Position in insertion order determines whether this candidate is
    # within budget; the first MAX_TRIALS_PER_FAMILY_PER_QUARTER are
    # allowed, anything beyond is blocked.
    for idx, t in enumerate(family_quarter_trials):
        if t["candidate_id"] == candidate_id:
            if idx >= MAX_TRIALS_PER_FAMILY_PER_QUARTER:
                raise TrialBudgetViolationError(
                    f"trial budget exceeded for"
                    f" hypothesis_family={family!r} in quarter {quarter}:"
                    f" candidate {candidate_id!r} is the {idx + 1}th"
                    f" registration but the per-quarter limit is"
                    f" {MAX_TRIALS_PER_FAMILY_PER_QUARTER}."
                    " Wait for the next quarter or pre-register fewer"
                    " variants."
                )
            return
    # Defensive: trial existed but was filtered out by family/quarter
    # mismatch. This shouldn't happen but treat as no-op.
    return
