"""Protocol-violation error hierarchy.

These errors signal that the research protocol has been bypassed,
tampered with, or attempted-to-be-bypassed. Catch sites must either
re-raise or convert to a recorded stage-failure; silencing one of these
exceptions is itself a protocol violation.
"""

from __future__ import annotations


class ProtocolViolationError(RuntimeError):
    """Base for all enforcement failures.

    Subclasses must NEVER be silenced with a bare except. Every catch
    site must either re-raise or convert to a recorded stage-failure.
    """


class RegistrationMissingError(ProtocolViolationError):
    """No pre-registration document exists for the candidate."""


class RegistrationInvalidError(ProtocolViolationError):
    """The registration document failed schema validation."""


class RegistrationHashMismatchError(ProtocolViolationError):
    """The registration's recorded hash does not match its canonical contents.

    The document was edited after signing. Treat as a new candidate and
    re-register; do not attempt to repair the hash field by hand.
    """


class CandidateKilledError(ProtocolViolationError):
    """The candidate appears on the append-only kill list."""


class KillListTamperingError(ProtocolViolationError):
    """The kill list payload is malformed, has a wrong version, or
    otherwise fails integrity checks. Refuse to proceed."""


class StageGateError(ProtocolViolationError):
    """Validation-ladder progression rule violated.

    Raised when a stage is attempted out of order: prior stages missing,
    a prior stage marked fail, an attempt to record a stage outcome that
    would overwrite a prior fail with a pass, or a stage number outside
    the allowed range.
    """


class ValidationLadderTamperingError(ProtocolViolationError):
    """The validation-ladder state file is malformed, has a wrong version,
    or otherwise fails integrity checks. Refuse to proceed."""


class StatisticalViolationError(ProtocolViolationError):
    """A statistical-guard rule was violated.

    Raised by:
      - assert_minimum_sample when n_eff < threshold
      - assert_statistical_pass when the verdict is not pass
      - input validators inside statistical_guard helpers
      - protocol_guard's defensive check when a prior stage's
        statistical_validity block is missing or stat_pass is False
    """


class ReplicationViolationError(ProtocolViolationError):
    """A replication-evidence rule was violated.

    Raised by:
      - record_replication_result on duplicate (period_id, symbol) with
        differing report_path or differing recomputed verdict
      - record_replication_result / record_cross_symbol_exemption on
        registration_hash drift
      - record_cross_symbol_exemption on attempts to change an existing
        granted exemption's reason
      - assert_replication_ready when minimum-period or minimum-symbol
        requirements are not met for stage 6
      - load helpers when the on-disk replication state is malformed,
        version-mismatched, or otherwise tampered with
    """


class AuditLogTamperingError(ProtocolViolationError):
    """audit_log.jsonl has malformed lines, missing required event fields,
    invalid event_type / decision / protocol_stage values, or otherwise
    fails integrity checks. Refuse to proceed."""


class ProtocolCLIError(ProtocolViolationError):
    """A research-protocol CLI flag combination is invalid.

    Raised when ``--enforce-protocol`` is set but ``--candidate-id`` or
    ``--protocol-stage`` is missing, or when the supplied stage does not
    match the script's expected stage.
    """


class TrialBudgetViolationError(ProtocolViolationError):
    """A trial-budget rule was violated.

    Raised by:
      - record_trial when the candidate's registration_hash drifts from a
        prior trial entry under the same candidate_id
      - record_trial / classify_candidate_change on malformed inputs
      - assert_trial_budget_available when the per-family / per-quarter
        budget is exhausted, when the trial is a ``revival_attempt`` of
        a killed candidate, or when a stored entry has an unknown
        ``modification_type``
      - load helpers when the on-disk trial state is malformed,
        version-mismatched, or otherwise tampered with
    """
