"""Single guard entry-point for protocol enforcement.

Every validation script must call :func:`assert_protocol_compliant` at
the top, before opening any dataset. The function returns the loaded
:class:`Registration` so callers can reference its hash in downstream
artifacts.

The guard is the unlock: a script that omits it will fail downstream
audit-logger checks (PR5) because it has no recorded registration hash.
"""

from __future__ import annotations

from services.research_protocol.audit_logger import safe_emit_audit_event
from services.research_protocol.errors import (
    CandidateKilledError,
    RegistrationHashMismatchError,
    RegistrationInvalidError,
    RegistrationMissingError,
    ReplicationViolationError,
    StageGateError,
    StatisticalViolationError,
    TrialBudgetViolationError,
)
from services.research_protocol.kill_list import assert_not_killed
from services.research_protocol.registration import (
    Registration,
    load_registration,
)
from services.research_protocol.replication_guard import assert_replication_ready
from services.research_protocol.statistical_guard import (
    STATISTICAL_VALIDITY_KEY,
    verdict_from_dict,
)
from services.research_protocol.trial_budget import (
    assert_trial_budget_available,
    record_trial,
)
from services.research_protocol.validation_ladder import (
    STAGES_REQUIRING_STATISTICS,
    assert_stage_allowed,
    get_candidate_stage_status,
)

REPLICATION_REQUIRED_STAGE = 6


def assert_protocol_compliant(
    candidate_id: str,
    *,
    requested_stage: int | None = None,
    enforce_statistical_validity: bool = True,
    enforce_replication: bool = True,
    enforce_trial_budget: bool = True,
) -> Registration:
    """Validate registration + trial budget + kill-list + ladder + replication.

    Order of checks:
      1. Registration exists at the canonical path.
      2. Registration schema is valid.
      3. Recorded ``registration_hash`` matches the recomputed hash.
      4. If ``enforce_trial_budget`` (default True): record the trial
         (idempotent on registration_hash), then assert the trial is
         not a ``revival_attempt`` and the per-family / per-quarter
         budget is not exceeded.
      5. Candidate is not on the kill list.
      6. If ``requested_stage`` is provided, the candidate is allowed to
         run that stage given the current ladder state.
      7. If ``enforce_statistical_validity`` and ``requested_stage >= 2``,
         every prior stage in [2, requested_stage-1] that is recorded
         must contain a ``statistical_validity`` block whose recomputed
         verdict has ``statistical_pass=True``.
      8. If ``enforce_replication`` and ``requested_stage == 6``, the
         candidate must satisfy replication readiness.

    Order matters: registration validity is reported first; the trial
    gate fires before the kill-list so that a revival_attempt of a
    killed parent is reported as a trial-budget block (not a kill-list
    block on the parent). The kill-list still fires when the *candidate
    itself* is on the list. Statistical and replication checks come
    after.

    Args:
      candidate_id: registered candidate.
      requested_stage: optional integer in [0, 6]. When provided, the
        ladder gate is enforced; omit for callers that only need
        registration + kill-list checks.
      enforce_statistical_validity: when True (default), prior stages'
        ``statistical_validity`` blocks are recomputed.
      enforce_replication: when True (default), stage-6 requests call
        :func:`replication_guard.assert_replication_ready`.
      enforce_trial_budget: when True (default), the trial gate runs.
        Set False only inside inspection tooling that needs to read
        protocol state without recording a trial.

    Returns:
      The loaded :class:`Registration`.

    Raises:
      :class:`ProtocolViolationError` subclasses on any failure. Callers
      must not silence these; either re-raise or convert to a recorded
      stage-failure.
    """
    try:
        registration = load_registration(candidate_id)
    except (
        RegistrationMissingError,
        RegistrationInvalidError,
        RegistrationHashMismatchError,
    ) as exc:
        safe_emit_audit_event(
            event_type="registration_rejected",
            decision="block",
            candidate_id=candidate_id,
            protocol_stage=requested_stage,
            reason=str(exc),
            metadata={"error_class": type(exc).__name__},
        )
        raise

    safe_emit_audit_event(
        event_type="registration_loaded",
        decision="pass",
        candidate_id=candidate_id,
        protocol_stage=requested_stage,
        registration_hash=registration.registration_hash,
    )

    if enforce_trial_budget:
        try:
            record_trial(registration)
            assert_trial_budget_available(candidate_id)
        except TrialBudgetViolationError as exc:
            safe_emit_audit_event(
                event_type="trial_budget_block",
                decision="block",
                candidate_id=candidate_id,
                protocol_stage=requested_stage,
                reason=str(exc),
                registration_hash=registration.registration_hash,
            )
            raise

    try:
        assert_not_killed(candidate_id)
    except CandidateKilledError as exc:
        safe_emit_audit_event(
            event_type="kill_list_block",
            decision="block",
            candidate_id=candidate_id,
            protocol_stage=requested_stage,
            reason=str(exc),
            registration_hash=registration.registration_hash,
        )
        raise

    if requested_stage is not None:
        try:
            assert_stage_allowed(candidate_id, requested_stage)
        except StageGateError as exc:
            safe_emit_audit_event(
                event_type="ladder_block",
                decision="block",
                candidate_id=candidate_id,
                protocol_stage=requested_stage,
                reason=str(exc),
                registration_hash=registration.registration_hash,
            )
            raise

        if enforce_statistical_validity and requested_stage >= 2:
            try:
                _assert_prior_stages_statistically_valid(
                    candidate_id, requested_stage
                )
            except StatisticalViolationError as exc:
                safe_emit_audit_event(
                    event_type="statistical_block",
                    decision="block",
                    candidate_id=candidate_id,
                    protocol_stage=requested_stage,
                    reason=str(exc),
                    registration_hash=registration.registration_hash,
                )
                raise

        if enforce_replication and requested_stage == REPLICATION_REQUIRED_STAGE:
            try:
                assert_replication_ready(candidate_id)
            except ReplicationViolationError as exc:
                safe_emit_audit_event(
                    event_type="replication_block",
                    decision="block",
                    candidate_id=candidate_id,
                    protocol_stage=requested_stage,
                    reason=str(exc),
                    registration_hash=registration.registration_hash,
                )
                raise

    safe_emit_audit_event(
        event_type="protocol_pass",
        decision="pass",
        candidate_id=candidate_id,
        protocol_stage=requested_stage,
        registration_hash=registration.registration_hash,
        metadata={
            "enforce_statistical_validity": bool(enforce_statistical_validity),
            "enforce_replication": bool(enforce_replication),
            "enforce_trial_budget": bool(enforce_trial_budget),
        },
    )
    return registration


def _assert_prior_stages_statistically_valid(
    candidate_id: str,
    requested_stage: int,
) -> None:
    """Defensive recompute of every recorded stat block in [2, stage-1].

    The ladder check has already enforced that every prior stage is
    recorded ``status="pass"``; this defends against a state file edit
    that flips ``statistical_pass`` to True while leaving ``status`` as
    the existing value.
    """
    status = get_candidate_stage_status(candidate_id)
    for prior in range(2, requested_stage):
        if prior not in STAGES_REQUIRING_STATISTICS:
            continue
        result = status.stages.get(prior)
        if result is None:
            # Ladder check should have already raised; defensive.
            raise StatisticalViolationError(
                f"prior stage {prior} for candidate {candidate_id!r}"
                " is not recorded; cannot proceed."
            )
        block = result.metadata.get(STATISTICAL_VALIDITY_KEY)
        if not isinstance(block, dict):
            raise StatisticalViolationError(
                f"prior stage {prior} for candidate {candidate_id!r}"
                " has no statistical_validity block; the ladder state"
                " was edited or written by a non-protocol path."
            )
        verdict = verdict_from_dict(block)  # raises on missing/invalid fields
        if not verdict.statistical_pass:
            reason_text = "; ".join(
                verdict.suppression_reasons or ("(no reason recorded)",)
            )
            raise StatisticalViolationError(
                f"prior stage {prior} for candidate {candidate_id!r}"
                f" has statistical_pass=False after recompute:"
                f" {reason_text}"
            )
