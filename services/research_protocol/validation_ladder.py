"""Validation-ladder enforcement (RESEARCH_PROTOCOL §3).

Enforces stage progression for the six-stage validation ladder. State
lives in ``reports/research_protocol/validation_ladder_state.json`` and
is mutated only through :func:`record_stage_result`. There is no public
API to remove, downgrade, or rewrite a recorded result; once a stage is
recorded, the entry is permanent for that ``candidate_id``. The on-disk
file is rewritten atomically via tempfile + ``os.replace`` so a crash
mid-write cannot corrupt the state.

Stage 0 (``stage_0_registered``) is the registration check itself —
implicit, satisfied by ``services.research_protocol.registration`` and
never written into the ladder state. Stages 1–6 are the runnable
ladder stages whose results are tracked here.

Rules enforced:
  - ``record_stage_result`` requires every prior stage in [1, stage-1]
    to be recorded ``status="pass"``; otherwise :class:`StageGateError`.
  - A failed stage permanently blocks all later stages for that
    ``candidate_id``. The only legitimate way to proceed is to register
    a new ``candidate_id``.
  - A stage already on record cannot be overwritten with a different
    ``status`` or ``report_path``; identical re-records are idempotent.
  - All records require a non-empty ``report_path`` and a metadata dict
    containing ``run_timestamp`` and ``dataset_identifier``.
  - The first record for a candidate fixes the candidate's
    ``registration_hash`` in the state file. Subsequent records must
    match — preventing the "edit registration, retry stage" failure
    mode.
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

from services.research_protocol._paths import validation_ladder_state_path
from services.research_protocol.audit_logger import safe_emit_audit_event
from services.research_protocol.errors import (
    StageGateError,
    StatisticalViolationError,
    ValidationLadderTamperingError,
)
from services.research_protocol.registration import (
    Registration,
    load_registration,
)
from services.research_protocol.statistical_guard import (
    STATISTICAL_VALIDITY_KEY,
    StatisticalVerdict,
    verdict_from_dict,
    verdict_to_dict,
)

VALIDATION_LADDER_VERSION = 1

STAGE_NAMES: dict[int, str] = {
    0: "stage_0_registered",
    1: "stage_1_in_sample_sanity",
    2: "stage_2_single_period_oos",
    3: "stage_3_cross_period",
    4: "stage_4_cross_symbol",
    5: "stage_5_robustness",
    6: "stage_6_paper_observation",
}
STAGE_MIN_RECORDABLE = 1
STAGE_MAX = 6
STATUS_PASS = "pass"
STATUS_FAIL = "fail"
ALLOWED_STATUSES: frozenset[str] = frozenset({STATUS_PASS, STATUS_FAIL})
REQUIRED_METADATA_KEYS: tuple[str, ...] = ("run_timestamp", "dataset_identifier")

# Stages that require a statistical_validity block in metadata
# (RESEARCH_PROTOCOL §4). Stage 1 is implementation sanity and does not
# require statistics.
STAGES_REQUIRING_STATISTICS: frozenset[int] = frozenset({2, 3, 4, 5, 6})


@dataclass(frozen=True)
class StageResult:
    candidate_id: str
    stage: int
    name: str
    status: str
    report_path: str
    metadata: dict[str, Any]
    recorded_at: str
    registration_hash: str


@dataclass(frozen=True)
class CandidateStageStatus:
    """Snapshot of a candidate's ladder state.

    ``stages`` is keyed by integer stage number and contains only
    explicitly-recorded results. ``highest_passed_stage`` is the largest
    stage with status=pass (or 0 if no recorded stages and registration
    is valid). ``has_failure`` is True iff any recorded stage is fail;
    in that case ``blocked_at_stage`` names the failed stage.
    """

    candidate_id: str
    registration_hash: str | None
    stages: dict[int, StageResult]
    highest_passed_stage: int
    has_failure: bool
    blocked_at_stage: int | None


# --------------------------------------------------------------------- #
# State file IO
# --------------------------------------------------------------------- #


def _empty_payload() -> dict[str, Any]:
    return {"version": VALIDATION_LADDER_VERSION, "candidates": {}}


def _read_state() -> dict[str, Any]:
    path = validation_ladder_state_path()
    if not path.exists():
        return _empty_payload()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValidationLadderTamperingError(
            f"validation_ladder_state at {path} is not valid JSON: {exc}"
        ) from exc
    _assert_state_shape(payload, path)
    return payload


def _assert_state_shape(payload: Any, path: Path) -> None:
    if not isinstance(payload, dict):
        raise ValidationLadderTamperingError(
            f"validation_ladder_state at {path} must be a JSON object;"
            f" got {type(payload).__name__}"
        )
    if payload.get("version") != VALIDATION_LADDER_VERSION:
        raise ValidationLadderTamperingError(
            f"validation_ladder_state at {path} has version="
            f"{payload.get('version')!r}; expected {VALIDATION_LADDER_VERSION}."
            " Refusing to proceed."
        )
    candidates = payload.get("candidates")
    if not isinstance(candidates, dict):
        raise ValidationLadderTamperingError(
            f"validation_ladder_state at {path} 'candidates' field must be a"
            f" dict; got {type(candidates).__name__}"
        )
    for candidate_id, body in candidates.items():
        if not isinstance(candidate_id, str) or not candidate_id:
            raise ValidationLadderTamperingError(
                f"candidate id key must be a non-empty string; got"
                f" {candidate_id!r}"
            )
        if not isinstance(body, dict):
            raise ValidationLadderTamperingError(
                f"candidate {candidate_id!r} entry must be a dict;"
                f" got {type(body).__name__}"
            )
        if not isinstance(body.get("registration_hash"), str):
            raise ValidationLadderTamperingError(
                f"candidate {candidate_id!r} missing registration_hash"
            )
        stages = body.get("stages")
        if not isinstance(stages, dict):
            raise ValidationLadderTamperingError(
                f"candidate {candidate_id!r} 'stages' must be a dict;"
                f" got {type(stages).__name__}"
            )
        for k, entry in stages.items():
            try:
                stage_int = int(k)
            except (TypeError, ValueError) as exc:
                raise ValidationLadderTamperingError(
                    f"candidate {candidate_id!r} has non-integer stage key {k!r}"
                ) from exc
            if stage_int not in STAGE_NAMES:
                raise ValidationLadderTamperingError(
                    f"candidate {candidate_id!r} has unknown stage {stage_int}"
                )
            if not isinstance(entry, dict):
                raise ValidationLadderTamperingError(
                    f"candidate {candidate_id!r} stage {stage_int} entry must"
                    f" be a dict; got {type(entry).__name__}"
                )
            if entry.get("status") not in ALLOWED_STATUSES:
                raise ValidationLadderTamperingError(
                    f"candidate {candidate_id!r} stage {stage_int} status"
                    f" {entry.get('status')!r} is not in"
                    f" {sorted(ALLOWED_STATUSES)}"
                )


def _write_state_atomic(payload: dict[str, Any]) -> None:
    path = validation_ladder_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=".validation_ladder_state.", suffix=".json", dir=str(path.parent)
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
# Public API
# --------------------------------------------------------------------- #


def load_validation_state() -> dict[str, Any]:
    """Return the full ladder state. Strict version check applied."""
    return _read_state()


def _stage_result_from_dict(
    candidate_id: str,
    registration_hash: str,
    raw: dict[str, Any],
) -> StageResult:
    stage = int(raw["stage"])
    return StageResult(
        candidate_id=candidate_id,
        stage=stage,
        name=str(raw.get("name") or STAGE_NAMES[stage]),
        status=str(raw["status"]),
        report_path=str(raw["report_path"]),
        metadata=dict(raw.get("metadata") or {}),
        recorded_at=str(raw["recorded_at"]),
        registration_hash=registration_hash,
    )


def get_candidate_stage_status(candidate_id: str) -> CandidateStageStatus:
    """Return the current ladder snapshot for a candidate.

    A candidate with no recorded stages has ``stages={}``,
    ``registration_hash=None``, ``highest_passed_stage=0`` (stage_0 is
    implicit), and ``has_failure=False``.
    """
    payload = _read_state()
    body = payload["candidates"].get(candidate_id)
    if body is None:
        return CandidateStageStatus(
            candidate_id=candidate_id,
            registration_hash=None,
            stages={},
            highest_passed_stage=0,
            has_failure=False,
            blocked_at_stage=None,
        )
    reg_hash = body["registration_hash"]
    stages: dict[int, StageResult] = {}
    for k, entry in body["stages"].items():
        result = _stage_result_from_dict(candidate_id, reg_hash, entry)
        stages[result.stage] = result
    has_failure = any(s.status == STATUS_FAIL for s in stages.values())
    blocked = next(
        (s.stage for s in sorted(stages.values(), key=lambda x: x.stage)
         if s.status == STATUS_FAIL),
        None,
    )
    passed_stages = [s.stage for s in stages.values() if s.status == STATUS_PASS]
    highest = max(passed_stages) if passed_stages else 0
    return CandidateStageStatus(
        candidate_id=candidate_id,
        registration_hash=reg_hash,
        stages=stages,
        highest_passed_stage=highest,
        has_failure=has_failure,
        blocked_at_stage=blocked,
    )


def assert_stage_allowed(candidate_id: str, requested_stage: int) -> None:
    """Raise :class:`StageGateError` if the candidate cannot run the stage.

    Stage 0 is allowed iff a valid registration exists. For stages
    >= 1, every prior recordable stage in [1, requested_stage-1] must
    already be recorded with ``status="pass"``; any missing prior stage
    or any prior failure raises.
    """
    if not isinstance(requested_stage, int) or isinstance(requested_stage, bool):
        raise StageGateError(
            f"requested_stage must be an int in [0, {STAGE_MAX}];"
            f" got {requested_stage!r}"
        )
    if requested_stage < 0 or requested_stage > STAGE_MAX:
        raise StageGateError(
            f"requested_stage {requested_stage} is outside [0, {STAGE_MAX}]"
        )
    # Stage 0 = registered. Ensure registration loads cleanly. Any
    # registration error propagates as RegistrationMissingError /
    # RegistrationInvalidError / RegistrationHashMismatchError, which
    # are also ProtocolViolationError subclasses.
    load_registration(candidate_id)
    if requested_stage == 0:
        return
    status = get_candidate_stage_status(candidate_id)
    if status.has_failure and status.blocked_at_stage is not None:
        if status.blocked_at_stage < requested_stage:
            raise StageGateError(
                f"candidate {candidate_id!r} has a failed stage"
                f" {status.blocked_at_stage}"
                f" ({STAGE_NAMES[status.blocked_at_stage]}); cannot proceed"
                f" to stage {requested_stage}"
                f" ({STAGE_NAMES[requested_stage]}). Register a new"
                " candidate_id to continue research."
            )
    missing = [
        prior
        for prior in range(STAGE_MIN_RECORDABLE, requested_stage)
        if status.stages.get(prior) is None
        or status.stages[prior].status != STATUS_PASS
    ]
    if missing:
        raise StageGateError(
            f"candidate {candidate_id!r} cannot run stage"
            f" {requested_stage} ({STAGE_NAMES[requested_stage]}): prior"
            f" stages {missing} are not recorded as pass. Run them in"
            " order; the ladder cannot be skipped."
        )


def _validate_metadata(metadata: dict[str, Any]) -> None:
    if not isinstance(metadata, dict):
        raise StageGateError(
            f"metadata must be a dict; got {type(metadata).__name__}"
        )
    missing = [k for k in REQUIRED_METADATA_KEYS if k not in metadata]
    if missing:
        raise StageGateError(
            f"metadata missing required keys: {missing}"
            f" (required: {list(REQUIRED_METADATA_KEYS)})"
        )
    for key in REQUIRED_METADATA_KEYS:
        value = metadata[key]
        if not isinstance(value, str) or not value.strip():
            raise StageGateError(
                f"metadata[{key!r}] must be a non-empty string; got {value!r}"
            )


def _validate_and_recompute_statistical_block(
    *,
    candidate_id: str,
    stage: int,
    metadata: dict[str, Any],
) -> StatisticalVerdict:
    """For stage 2+: require + recompute the statistical_validity block.

    The block must be present and a dict. We recompute the verdict from
    the inputs (n_obs, horizon_days, ci_*, permutation_p_value); the
    user's claimed ``statistical_pass`` and ``metrics_suppressed`` are
    cross-checked against the recomputation. Disagreement raises.

    Returns the recomputed (authoritative) verdict so the caller can
    persist it.
    """
    block = metadata.get(STATISTICAL_VALIDITY_KEY)
    if not isinstance(block, dict):
        raise StageGateError(
            f"stage {stage} requires"
            f" metadata[{STATISTICAL_VALIDITY_KEY!r}] to be a dict;"
            f" got {type(block).__name__}. Stages 2+ must include a"
            " statistical_validity block built via"
            " statistical_guard.evaluate_statistical_validity()."
        )
    if int(block.get("stage", -1)) != int(stage):
        raise StageGateError(
            f"metadata[{STATISTICAL_VALIDITY_KEY!r}].stage="
            f"{block.get('stage')!r} does not match recorded stage="
            f"{stage}"
        )
    try:
        verdict = verdict_from_dict(block)
    except StatisticalViolationError as exc:
        raise StageGateError(
            f"candidate {candidate_id!r} stage {stage}"
            f" statistical_validity block invalid: {exc}"
        ) from exc
    claimed_pass = block.get("statistical_pass")
    if claimed_pass is not None and bool(claimed_pass) != verdict.statistical_pass:
        raise StageGateError(
            f"candidate {candidate_id!r} stage {stage}"
            f" claimed statistical_pass={claimed_pass!r} disagrees"
            f" with recomputed verdict={verdict.statistical_pass!r};"
            " the block was edited or computed with stale rules."
        )
    claimed_suppressed = block.get("metrics_suppressed")
    if (
        claimed_suppressed is not None
        and bool(claimed_suppressed) != verdict.metrics_suppressed
    ):
        raise StageGateError(
            f"candidate {candidate_id!r} stage {stage}"
            f" claimed metrics_suppressed={claimed_suppressed!r}"
            f" disagrees with recomputed"
            f" verdict={verdict.metrics_suppressed!r}"
        )
    return verdict


def record_stage_result(
    *,
    candidate_id: str,
    stage: int,
    passed: bool,
    report_path: str,
    metadata: dict[str, Any],
    registration_hash: str | None = None,
) -> StageResult:
    """Persist a stage outcome. Append-only; idempotent on identical record.

    Args:
      candidate_id: registered candidate.
      stage: integer in [1, 6]. Stage 0 is implicit; recording it here
        is rejected.
      passed: True records ``status="pass"``; False records ``"fail"``.
      report_path: path to the artifact this stage produced (required,
        must be non-empty).
      metadata: dict with at least ``run_timestamp`` and
        ``dataset_identifier`` (both non-empty strings). Additional keys
        are preserved.
      registration_hash: optional override of the candidate's hash. When
        omitted, the hash is loaded from the registration on disk and
        checked for consistency with any prior records for this candidate.

    Returns:
      The stored :class:`StageResult`. If a record for this stage
      already exists with the same status and report_path, returns the
      existing entry without writing.

    Raises:
      :class:`StageGateError` for any rule violation: invalid stage,
      missing prior stages, fail→pass overwrite attempt, mismatched
      report_path on re-record, missing report_path, malformed metadata,
      or registration_hash mismatch.
      :class:`RegistrationMissingError` /
      :class:`RegistrationHashMismatchError` /
      :class:`RegistrationInvalidError` when the registration is bad.
    """
    if not isinstance(stage, int) or isinstance(stage, bool):
        raise StageGateError(f"stage must be an int; got {stage!r}")
    if stage < STAGE_MIN_RECORDABLE or stage > STAGE_MAX:
        raise StageGateError(
            f"stage {stage} is not recordable; recordable range is"
            f" [{STAGE_MIN_RECORDABLE}, {STAGE_MAX}]. Stage 0 is implicit"
            " via registration."
        )
    if not isinstance(report_path, str) or not report_path.strip():
        raise StageGateError("report_path must be a non-empty string")
    _validate_metadata(metadata)

    # Load (and validate) the registration before any state mutation.
    # This catches missing/edited/hash-mismatched registrations early.
    registration: Registration = load_registration(candidate_id)
    expected_hash = registration.registration_hash
    if registration_hash is not None and registration_hash != expected_hash:
        raise StageGateError(
            f"registration_hash override {registration_hash!r} does not"
            f" match loaded registration's hash {expected_hash!r}"
        )

    # Enforce stage-progression rules using the current state.
    assert_stage_allowed(candidate_id, stage)

    # Read state and check registration_hash drift BEFORE the stat block
    # check: a hash drift is the more fundamental problem (the candidate
    # is no longer the same one that was registered) and must be reported
    # first so the user can see they need a new candidate_id rather than
    # chasing a missing-stat-block error message.
    payload = _read_state()
    candidate_block = payload["candidates"].setdefault(
        candidate_id,
        {"registration_hash": expected_hash, "stages": {}},
    )
    if candidate_block["registration_hash"] != expected_hash:
        raise StageGateError(
            f"candidate {candidate_id!r} prior records used"
            f" registration_hash {candidate_block['registration_hash']!r}"
            f" but the current registration hash is {expected_hash!r}."
            " The registration changed; this is treated as a new"
            " candidate. Register a new candidate_id to continue."
        )

    # Stages 2+ require a statistical_validity block; recompute the
    # verdict to defeat tampering, and refuse to record passed=True
    # while the verdict itself fails.
    metadata_to_store = dict(metadata)
    if stage in STAGES_REQUIRING_STATISTICS:
        verdict = _validate_and_recompute_statistical_block(
            candidate_id=candidate_id,
            stage=stage,
            metadata=metadata_to_store,
        )
        if passed and not verdict.statistical_pass:
            reason_text = "; ".join(
                verdict.suppression_reasons or ("(no reason recorded)",)
            )
            raise StageGateError(
                f"refusing to record stage {stage} for candidate"
                f" {candidate_id!r} as pass: statistical verdict"
                f" failed ({reason_text}). Record passed=False or fix"
                " the underlying statistics."
            )
        # Persist the recomputed verdict so the stored block is canonical
        # (the user's claimed statistical_pass / metrics_suppressed
        # values may have been omitted; we always write the truth).
        metadata_to_store[STATISTICAL_VALIDITY_KEY] = verdict_to_dict(verdict)
    new_status = STATUS_PASS if passed else STATUS_FAIL
    existing = candidate_block["stages"].get(str(stage))
    if existing is not None:
        if (
            existing["status"] == new_status
            and existing["report_path"] == report_path
        ):
            # Idempotent re-record. Do not rewrite the file.
            return _stage_result_from_dict(
                candidate_id, expected_hash, existing
            )
        raise StageGateError(
            f"stage {stage} for candidate {candidate_id!r} is already"
            f" recorded as status={existing['status']!r},"
            f" report_path={existing['report_path']!r}; refusing to"
            f" overwrite with status={new_status!r},"
            f" report_path={report_path!r}. Stage results are"
            " append-only."
        )
    entry = {
        "stage": int(stage),
        "name": STAGE_NAMES[stage],
        "status": new_status,
        "report_path": report_path,
        "metadata": metadata_to_store,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }
    candidate_block["stages"][str(stage)] = entry
    _write_state_atomic(payload)
    audit_metadata: dict[str, Any] = {"status": new_status}
    if STATISTICAL_VALIDITY_KEY in metadata_to_store:
        block = metadata_to_store[STATISTICAL_VALIDITY_KEY]
        audit_metadata["statistical_pass"] = bool(block.get("statistical_pass"))
        audit_metadata["metrics_suppressed"] = bool(block.get("metrics_suppressed"))
        audit_metadata["n_eff"] = block.get("n_eff")
    safe_emit_audit_event(
        event_type="stage_result_recorded",
        decision="record",
        candidate_id=candidate_id,
        protocol_stage=int(stage),
        reason=f"stage_{stage} status={new_status}",
        registration_hash=expected_hash,
        report_path=report_path,
        metadata=audit_metadata,
    )
    return _stage_result_from_dict(candidate_id, expected_hash, entry)
