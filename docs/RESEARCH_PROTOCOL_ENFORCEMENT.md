# Research Protocol Enforcement Architecture

**Status:** v7, last updated 2026-05-04. Companion to `docs/RESEARCH_PROTOCOL.md`.

**Goal:** convert the protocol into code that *blocks* non-compliance at
runtime. Not advisory. Not a linter. The default for any unregistered or
killed candidate is `RuntimeError`.

**Shipped:**
- **PR1 (§10):** `registration.py`, `kill_list.py`, `protocol_guard.py`, `errors.py`.
- **PR2 (§3):** `validation_ladder.py`; `protocol_guard.assert_protocol_compliant`
  accepts optional `requested_stage` and gates ladder progression.
- **PR3 (§4):** `statistical_guard.py`; ladder enforces a recomputed
  `statistical_validity` block on stage 2+ records; guard recomputes prior
  stages' verdicts before allowing the next stage.
- **PR4 (§5):** `replication_guard.py`; cross-period and cross-symbol
  evidence (or documented exemption) required before stage 6.
- **PR5 (§7):** `audit_logger.py`; every protocol decision is appended
  as one JSONL record; reproducibility fingerprints embed deterministic
  identifiers in artifacts.
- **PR6 (§8):** `trial_budget.py`; every registration consumes a trial,
  classified as new_hypothesis / parameter_change / feature_change /
  threshold_change / period_change / symbol_change / revival_attempt;
  default budget is 3 candidates per hypothesis_family per calendar
  quarter; `protocol_guard` records the trial after registration and
  blocks revival_attempts and over-budget candidates before any kill,
  ladder, statistical, or replication check.
- **PR7 (integration):** `cli_protocol.py` exposes
  `add_protocol_arguments` and `enforce_protocol_from_args`; the three
  primary research scripts —
  `scripts/run_ml_regime_validation.py` (stage 2),
  `scripts/run_ml_regime_validation_cross_period.py` (stage 3), and
  `scripts/run_model_ready_dataset_smoke.py` (stage 0/infra) — now
  accept `--candidate-id`, `--protocol-stage`, and the mutually
  exclusive `--enforce-protocol` / `--no-enforce-protocol` flags.
  Default behavior is unchanged (legacy non-enforcing); enforcement is
  opt-in.

**No remaining work.** All seven enforcement layers are shipped and
wired. The only follow-on changes would be (1) tightening the
permutation α to a Bonferroni-adjusted threshold using
`summarize_trial_budget(...).trial_count`, and (2) extending the audit
log with hash-chained signing — both documented in §11 as residual
surface.

**Posture:** assume the user (human or model) will try to bypass rules,
sometimes unintentionally — by editing a JSON file, by re-running a
killed candidate "just to check," by skipping ladder stages, by
silencing an exception. Every guard below is designed so that the
shortest path to "make it work" is to comply with the protocol, not to
disable the guard.

---

## 1. Module Layout

```
services/research_protocol/
  __init__.py              # public re-exports                    [PR1+PR2+PR3+PR4+PR5+PR6+PR7]
  _paths.py                # protocol root + dir resolution        [PR1+PR2+PR4+PR5+PR6]
  errors.py                # ProtocolViolationError hierarchy      [PR1+PR2+PR3+PR4+PR5+PR6+PR7]
  registration.py          # §1 / §7.2 — load, hash, validate     [PR1]
  kill_list.py             # §6 — append-only, atomic              [PR1+PR5]
  validation_ladder.py     # §3 — stage gating + stat-block req    [PR2+PR3+PR5]
  statistical_guard.py     # §4 — n_eff / bootstrap / permutation  [PR3]
  replication_guard.py     # §5 — cross-period + cross-symbol     [PR4+PR5]
  audit_logger.py          # §7 — JSONL audit + fingerprints       [PR5+PR6]
  trial_budget.py          # §8 — N_trials + DOF accounting        [PR6]
  cli_protocol.py          # CLI integration helper                [PR7]
  protocol_guard.py        # single entry-point                    [PR1+PR2+PR3+PR4+PR5+PR6]

scripts/                                                           [PR7 wired]
  run_model_ready_dataset_smoke.py            # PROTOCOL_STAGE = 0
  run_ml_regime_validation.py                 # PROTOCOL_STAGE = 2
  run_ml_regime_validation_cross_period.py    # PROTOCOL_STAGE = 3
```

Mapping to `RESEARCH_PROTOCOL.md`:

| Module | Protocol section(s) | PR | Purpose |
|---|---|---|---|
| `registration.py` | §1, §7.2 | PR1 | Frozen, hashed pre-registration as the only legal entry point. |
| `kill_list.py` | §6 | PR1 + PR5 | Append-only kill record; PR5 adds audit emit on `record_kill`. |
| `validation_ladder.py` | §3 + §4 | PR2 + PR3 + PR5 | Stage progression gate; PR3 enforces stat-block recompute on stage 2+; PR5 emits audit on `record_stage_result`. |
| `statistical_guard.py` | §4 | PR3 | n_eff floor, bootstrap CI, permutation test, suppression sanitizer. |
| `replication_guard.py` | §5 | PR4 + PR5 | Cross-period + cross-symbol evidence; PR5 emits audit on `record_replication_result`. |
| `audit_logger.py` | §7 | PR5 + PR6 | JSONL audit + fingerprinting; PR6 added `trial_recorded` and `trial_budget_block` event types. |
| `trial_budget.py` | §8 | PR6 | Trial accounting + classification + per-family/quarter budget; revival-attempt detection. |
| `protocol_guard.py` | §1+§3+§4+§5+§6+§7+§8 | PR1+PR2+PR3+PR4+PR5+PR6 | Single call site; PR6 fires the trial gate after registration and before kill/ladder/stat/replication. |

### Errors

```python
# services/research_protocol/errors.py
class ProtocolViolationError(RuntimeError):
    """Base for all enforcement failures.

    Subclasses must NEVER be silenced with a bare except. Every catch
    site must either re-raise or convert to a stage-failure record.
    """

class RegistrationMissingError(ProtocolViolationError): ...
class RegistrationInvalidError(ProtocolViolationError): ...
class RegistrationHashMismatchError(ProtocolViolationError): ...
class CandidateKilledError(ProtocolViolationError): ...
class KillListTamperingError(ProtocolViolationError): ...

# Skeleton: surface in PR2/PR3/PR4
class StageGateError(ProtocolViolationError): ...
class StatisticalSafeguardError(ProtocolViolationError): ...
class GovernanceError(ProtocolViolationError): ...
class TrialBudgetExceededError(GovernanceError): ...
```

---

## 2. Pre-Registration Enforcement (§1, §7.2)

A registration is a frozen JSON document at
`reports/research_protocol/registrations/{candidate_id}.json`. Its
`registration_hash` field equals `SHA256(canonical_json(payload \ {hash}))`.
Any drift between recorded hash and recomputed hash → `RegistrationHashMismatchError`.

### Required schema

```json
{
  "candidate_id": "kebab-case-id",
  "registration_hash": "<sha256 hex>",
  "registration_timestamp": "2026-05-04T18:00:00Z",
  "git_commit_sha": "<full sha>",
  "hypothesis": {
    "mechanism": "...",
    "predicted_direction": "long | short | hedge",
    "why_might_fail": "...",
    "citations": ["..."]
  },
  "features":   [ /* {name, input_columns, transformation, lookback_window_days, min_periods, missing_data_policy, clipping, scaling, leakage_guard} */ ],
  "thresholds": [ /* {name, kind: fixed|train_quantile|external, value|quantile|source, derivation_window} */ ],
  "transformations": {
    "allowed":  ["log", "z_score_train", "..."],
    "forbidden_unless_listed": ["..."]
  },
  "forbidden_changes": ["..."],
  "falsification": { /* per-stage pass/fail */ },
  "datasets": {
    "validation_dataset_pattern": "...",
    "holdout_dataset_pattern": "..."
  },
  "horizon_days": 5,
  "random_seed": 42,
  "stages_required": [1, 2, 3, 4, 5, 6]
}
```

### Public surface (skeleton)

```python
# services/research_protocol/registration.py

@dataclass(frozen=True)
class Registration:
    candidate_id: str
    registration_hash: str
    body: dict[str, Any]

def canonical_json(payload: dict) -> str: ...
def compute_registration_hash(payload: dict) -> str: ...
def assert_registration_valid(payload: dict) -> None: ...
def assert_registration_exists(candidate_id: str) -> Path: ...
def load_registration(candidate_id: str) -> Registration: ...
def registration_path(candidate_id: str) -> Path: ...
```

### Enforcement

- Validation entry-points must call `load_registration(candidate_id)`
  *before* opening any dataset. Without that call, the script reads no
  data — the call is the unlock.
- The hash field is canonical-JSON over the document **without** the
  hash field. Editing any other field and re-saving breaks the hash.
- Schema is closed: unknown keys at the top level are rejected (skeleton
  PR may relax to "warn"; first version of this code rejects).

This part **ships in §10**.

---

## 3. Validation Ladder Guard (§3) — shipped (PR2)

Single state file at
`reports/research_protocol/validation_ladder_state.json`. All mutations
go through `record_stage_result`; the file is rewritten atomically via
`tempfile + os.replace`. The module exposes **no** function to remove,
downgrade, reset, or overwrite a recorded stage; once written, an entry
is permanent for that `candidate_id`.

### Stage definitions

```python
STAGE_NAMES = {
    0: "stage_0_registered",            # implicit; satisfied by registration.py
    1: "stage_1_in_sample_sanity",
    2: "stage_2_single_period_oos",
    3: "stage_3_cross_period",
    4: "stage_4_cross_symbol",
    5: "stage_5_robustness",
    6: "stage_6_paper_observation",
}
```

Stage 0 is **not** written into the state file — registration *is* the
artifact for stage_0_registered. Stages 1–6 are the recordable ladder
stages.

### Public surface (shipped)

```python
# services/research_protocol/validation_ladder.py

@dataclass(frozen=True)
class StageResult:
    candidate_id: str
    stage: int
    name: str            # STAGE_NAMES[stage]
    status: str          # "pass" | "fail"
    report_path: str
    metadata: dict[str, Any]
    recorded_at: str     # ISO8601 UTC
    registration_hash: str

@dataclass(frozen=True)
class CandidateStageStatus:
    candidate_id: str
    registration_hash: str | None
    stages: dict[int, StageResult]
    highest_passed_stage: int  # 0 if nothing recorded but reg valid
    has_failure: bool
    blocked_at_stage: int | None

def load_validation_state() -> dict[str, Any]: ...

def get_candidate_stage_status(candidate_id: str) -> CandidateStageStatus: ...

def assert_stage_allowed(candidate_id: str, requested_stage: int) -> None:
    """Raise StageGateError if the candidate cannot run requested_stage.

    Stage 0 only requires a valid registration. For stages >= 1, every
    prior stage in [1, requested_stage-1] must already be recorded
    status='pass'; any missing prior or any prior failure raises.
    """

def record_stage_result(
    *,
    candidate_id: str,
    stage: int,                              # in [1, 6]; 0 rejected
    passed: bool,
    report_path: str,                        # required, non-empty
    metadata: dict[str, Any],                # must include run_timestamp + dataset_identifier
    registration_hash: str | None = None,    # optional override; must match registration on disk
) -> StageResult:
    """Persist a stage outcome. Append-only; idempotent on identical record.

    Same status + same report_path on a re-record returns the existing
    entry without writing. Any other re-record (status flip, different
    report_path) raises StageGateError. The candidate's
    registration_hash is fixed at first record; any later mismatch
    raises StageGateError ("registration changed; ... register a new
    candidate_id").
    """
```

### Rules enforced (with the test that proves each)

| Rule | Test |
|---|---|
| stage_0 requires valid registration | `test_stage_0_blocked_when_registration_missing` |
| stage_N can only run if all priors passed | `test_stage_2_blocked_before_stage_1_recorded`, `test_cannot_skip_stages` |
| failed prior stage blocks all later stages | `test_failed_stage_1_blocks_stage_2`, `test_failed_stage_3_blocks_stage_5` |
| cannot overwrite failed→passed | `test_cannot_overwrite_failed_stage_with_passed` |
| cannot overwrite passed→failed | `test_cannot_overwrite_passed_stage_with_failed` |
| cannot change report_path on re-record | `test_cannot_change_report_path_on_re_record` |
| report_path required | `test_record_requires_report_path` |
| metadata.run_timestamp required | `test_record_requires_run_timestamp_in_metadata` |
| metadata.dataset_identifier required | `test_record_requires_dataset_identifier_in_metadata` |
| stage 0 not recordable (registration is the artifact) | `test_record_stage_0_rejected` |
| registration_hash drift detected across records | `test_registration_hash_change_detected_across_records` |
| no public downgrade/removal API | `test_no_removal_or_downgrade_functions` |
| state file tampering raises (bad version, malformed JSON, unknown stage, invalid status) | `TestStateTampering` (six tests) |

### Integration with `protocol_guard`

```python
# services/research_protocol/protocol_guard.py — shipped

def assert_protocol_compliant(
    candidate_id: str,
    *,
    requested_stage: int | None = None,
) -> Registration:
    registration = load_registration(candidate_id)         # 1, 2, 3
    assert_not_killed(candidate_id)                        # 4
    if requested_stage is not None:
        assert_stage_allowed(candidate_id, requested_stage)  # 5
    return registration
```

Order is load-fixed: registration → kill list → ladder. A killed
candidate cannot diagnose ladder state by probing `requested_stage`
(test: `test_kill_list_blocks_before_ladder_check`). A
deleted-registration cannot bypass the kill list by re-creating ladder
state under the same `candidate_id` (test: same as PR1
`test_check_order_registration_first_then_kill_list`).

### Failure modes blocked (post-PR2)

- Running Stage 3 before Stage 2 has a recorded `pass` → `StageGateError`.
- Re-running a passed stage with a different `report_path` → `StageGateError`.
- Re-running a failed stage with `passed=True` → `StageGateError`.
- Recording stage 0 directly → `StageGateError` (registration is the artifact).
- Stage record without `run_timestamp` or `dataset_identifier` → `StageGateError`.
- Editing the state file's version, structure, or stage status by hand
  → `ValidationLadderTamperingError` on next `load_validation_state`.
- Re-registering the same `candidate_id` with a different hash and
  attempting another stage record → `StageGateError`.

---

## 4. Statistical Guardrails (§4) — ENFORCED (PR3)

Suppression-not-warning is the load-bearing rule. A metric below the
n_eff floor is **removed from the metrics dict** by
`suppress_metrics_if_invalid`; downstream consumers cannot see the
unreliable estimate at all.

### Public surface (shipped)

```python
# services/research_protocol/statistical_guard.py

# RESEARCH_PROTOCOL §4.2 — per-stage minimum n_eff
N_EFF_FLOOR_BY_STAGE = {2: 30, 3: 30, 4: 30, 5: 60, 6: 100}
DEFAULT_PERMUTATION_ALPHA = 0.05
STATISTICAL_VALIDITY_KEY = "statistical_validity"
SUPPRESSIBLE_METRIC_KEYS = frozenset({...})  # win_rate, mean_return, sharpe, etc.

@dataclass(frozen=True)
class StatisticalVerdict:
    stage: int
    n_obs: int
    horizon_days: int
    n_eff: int
    n_eff_floor: int
    ci_lower: float | None
    ci_upper: float | None
    permutation_p_value: float | None
    permutation_alpha: float
    statistical_pass: bool
    metrics_suppressed: bool
    suppression_reasons: tuple[str, ...]

def compute_effective_sample_size(returns, *, horizon_days) -> int: ...
def n_eff_floor_for_stage(stage: int) -> int: ...
def assert_minimum_sample(n_eff, *, threshold, stage=None) -> None: ...

def compute_bootstrap_ci(
    returns, *, method="block", block_size, iterations=10_000,
    confidence=0.95, rng_seed=0, statistic="mean",
) -> tuple[float, float]: ...

def run_permutation_test(
    *, signal_returns, baseline_returns,
    n_iter=1000, rng_seed=0, one_sided="greater",
) -> float: ...

def evaluate_statistical_validity(
    *, stage, n_obs, horizon_days,
    ci_lower, ci_upper, permutation_p_value,
    permutation_alpha=DEFAULT_PERMUTATION_ALPHA,
) -> StatisticalVerdict: ...

def assert_statistical_pass(verdict: StatisticalVerdict) -> None: ...

def verdict_to_dict(verdict) -> dict: ...
def verdict_from_dict(payload: dict) -> StatisticalVerdict:
    """Reconstruct by RECOMPUTING from the inputs; the payload's claimed
    statistical_pass / metrics_suppressed flags are not trusted."""

def suppress_metrics_if_invalid(metrics: dict, *, verdict) -> dict:
    """Remove SUPPRESSIBLE_METRIC_KEYS when verdict.metrics_suppressed
    is True. Audit fields are preserved verbatim. Verdict is mirrored
    at the top level (n_eff, ci_lower, ci_upper, permutation_p_value,
    metrics_suppressed, statistical_pass) and embedded under
    STATISTICAL_VALIDITY_KEY."""
```

### Three rules, all *enforced*

| Rule | Trigger | Effect on metrics | Effect on stage |
|---|---|---|---|
| n_eff floor (§4.2) | `n_eff < N_EFF_FLOOR_BY_STAGE[stage]` | **suppressed** (perf keys removed) | cannot pass |
| Bootstrap CI | `ci_lower <= 0 <= ci_upper` | not suppressed (sample size is fine) | cannot pass |
| Permutation p | `p_value >= alpha` (default 0.05) | not suppressed | cannot pass |

The suppression rule is the strongest: when n_eff is below floor, the
metric values themselves are unreadable garbage and reports must not
display them. The CI and p-value rules mark a result as statistically
insignificant but leave the underlying numbers in place for diagnostic
review.

### Rule → test mapping

| Rule | Test |
|---|---|
| `compute_effective_sample_size` is `n_obs // horizon_days` | `TestEffectiveSampleSize.test_basic_floor_division` |
| Per-stage n_eff floors match §4.2 | `TestNEffFloors.test_per_stage_floors_match_protocol` |
| `assert_minimum_sample` raises below threshold | `TestAssertMinimumSample.test_raises_when_below_threshold` |
| Bootstrap CI excludes 0 for clearly positive signal | `TestBootstrapCI.test_strong_positive_signal_excludes_zero` |
| Bootstrap CI includes 0 for zero-mean sample | `TestBootstrapCI.test_zero_mean_signal_includes_zero` |
| Bootstrap CI deterministic under seed | `TestBootstrapCI.test_deterministic_under_same_seed` |
| Permutation rejects strong-signal vs baseline at α=0.01 | `TestPermutationTest.test_strong_signal_yields_low_p` |
| Permutation does not reject identical distributions | `TestPermutationTest.test_identical_distributions_yields_high_p` |
| Phipson-Smyth floor on p-value | `TestPermutationTest.test_phipson_smyth_floor` |
| n_eff failure → `metrics_suppressed=True` | `TestEvaluateStatisticalValidity.test_n_eff_below_floor_suppresses_metrics` |
| CI failure → `statistical_pass=False`, no suppression | `TestEvaluateStatisticalValidity.test_ci_includes_zero_blocks` |
| p-value failure → `statistical_pass=False`, no suppression | `TestEvaluateStatisticalValidity.test_high_p_blocks` |
| Multiple failures recorded together | `TestEvaluateStatisticalValidity.test_multiple_failures_record_multiple_reasons` |
| Suppression preserves audit fields | `TestSuppressMetricsIfInvalid.test_suppression_removes_perf_metrics_but_preserves_audit` |
| CI failure does NOT remove perf metrics | `TestSuppressMetricsIfInvalid.test_ci_failure_does_not_remove_metrics` |
| Tampered `statistical_pass` flag is recomputed away | `TestVerdictRoundtripAndTamperingDetection.test_tampered_pass_field_is_recomputed` |
| Stage 2+ ladder record requires stat block | `TestStatisticalBlockEnforcement.test_stage_2_record_without_stat_block_rejected` |
| Stage 1 record does NOT require stat block | `TestStatisticalBlockEnforcement.test_stage_1_record_without_stat_block_succeeds` |
| `passed=True` rejected when verdict fails | `TestStatisticalBlockEnforcement.test_stage_2_passed_true_with_failing_stats_rejected` |
| `passed=False` accepted when verdict fails | `TestStatisticalBlockEnforcement.test_stage_2_passed_false_with_failing_stats_succeeds` |
| Stored block is canonical (recomputed) | `TestStatisticalBlockEnforcement.test_persisted_block_is_canonical` |
| Guard re-runs verdict for prior stage | `TestProtocolGuardStatisticalDefensiveCheck.test_stage_3_blocked_when_stage_2_stats_tampered` |
| Missing prior stat block on next stage → raise | `TestProtocolGuardStatisticalDefensiveCheck.test_stage_3_blocked_when_stage_2_stat_block_missing` |
| `enforce_statistical_validity=False` skips defensive recompute | `TestProtocolGuardStatisticalDefensiveCheck.test_enforce_statistical_validity_false_skips_recompute` |

### Output format (recorded in ladder state)

For every stage 2+ record, `metadata.statistical_validity` is the
recomputed canonical block:

```json
{
  "stage": 2,
  "n_obs": 250,
  "horizon_days": 5,
  "n_eff": 50,
  "n_eff_floor": 30,
  "ci_lower": 0.0012,
  "ci_upper": 0.0083,
  "permutation_p_value": 0.012,
  "permutation_alpha": 0.05,
  "statistical_pass": true,
  "metrics_suppressed": false,
  "suppression_reasons": []
}
```

`suppress_metrics_if_invalid` mirrors `n_eff`, `ci_lower`, `ci_upper`,
`permutation_p_value`, `metrics_suppressed`, and `statistical_pass` at
the top level of the metrics dict for at-a-glance reading; the embedded
verdict block under `statistical_validity` is the single source of
truth.

### Suppressed vs allowed example

**Allowed (n_eff=50, CI excludes 0, p=0.012):**

```python
metrics = {"win_rate": 0.62, "mean_return": 0.005}
verdict = evaluate_statistical_validity(
    stage=2, n_obs=250, horizon_days=5,
    ci_lower=0.0012, ci_upper=0.0083,
    permutation_p_value=0.012,
)
out = suppress_metrics_if_invalid(metrics, verdict=verdict)
# → {
#     "win_rate": 0.62,
#     "mean_return": 0.005,
#     "n_eff": 50, "ci_lower": 0.0012, "ci_upper": 0.0083,
#     "permutation_p_value": 0.012,
#     "metrics_suppressed": false, "statistical_pass": true,
#     "statistical_validity": {... full block ...}
# }
```

**Suppressed (n_eff=2 from n_obs=10, horizon=5):**

```python
metrics = {"win_rate": 0.7, "mean_return": 0.02,
           "filtered_test_win_rate": 0.81,
           "registration_hash": "ab" * 32, "candidate_id": "cand-001"}
verdict = evaluate_statistical_validity(
    stage=2, n_obs=10, horizon_days=5,
    ci_lower=0.01, ci_upper=0.02,           # would-be passing CI
    permutation_p_value=0.001,              # would-be passing p
)
out = suppress_metrics_if_invalid(metrics, verdict=verdict)
# → {
#     # win_rate, mean_return, filtered_test_win_rate REMOVED
#     "registration_hash": "ab...", "candidate_id": "cand-001",  # PRESERVED
#     "n_eff": 2, "ci_lower": 0.01, "ci_upper": 0.02,
#     "permutation_p_value": 0.001,
#     "metrics_suppressed": true, "statistical_pass": false,
#     "statistical_validity": {
#         ..., "suppression_reasons": ["n_eff=2 below stage_2_floor=30 ..."],
#     }
# }
```

The suppressed output cannot mislead a reader into thinking the 70%
win rate is real — the field is gone. The audit fields
(`registration_hash`, `candidate_id`) survive so the suppression event
remains traceable.

### Integration with the ladder + guard

- `validation_ladder.record_stage_result` requires `metadata.statistical_validity`
  for any stage in `STAGES_REQUIRING_STATISTICS = {2, 3, 4, 5, 6}`. The
  block's `statistical_pass` and `metrics_suppressed` flags are
  recomputed from the inputs; user-claimed flags that disagree with the
  recomputation raise `StageGateError`. Recording `passed=True` while
  the verdict fails raises `StageGateError`.
- `protocol_guard.assert_protocol_compliant(candidate_id, requested_stage=N)`
  with `N >= 2` recomputes every prior stage's verdict via
  `verdict_from_dict`. Any prior `statistical_pass=False` after
  recompute raises `StatisticalViolationError`. The check is on by
  default (`enforce_statistical_validity=True`); set False only inside
  inspection tooling that is not running validation.

---

## 5. Cross-Period & Cross-Symbol Enforcement (§5) — ENFORCED (PR4)

Stage 6 is the gate: paper observation cannot start until the candidate
has independent replication evidence on at least
:data:`MIN_DISTINCT_PERIODS` (=2) distinct test periods AND at least
:data:`MIN_DISTINCT_SYMBOLS` (=2) distinct symbols. A documented
cross-symbol exemption can substitute for the second symbol but not for
the second period — the period requirement is non-waivable.

Evidence is stored in `reports/research_protocol/replication_evidence.json`
(separate file from the ladder). The verdict in each evidence record is
*recomputed* on every read; a tamperer cannot flip
``statistical_pass`` to True without inputs that imply it.

### Public surface (shipped)

```python
# services/research_protocol/replication_guard.py

MIN_DISTINCT_PERIODS = 2
MIN_DISTINCT_SYMBOLS = 2
REPLICATION_VERSION = 1

@dataclass(frozen=True)
class ReplicationEvidence:
    candidate_id: str
    period_id: str
    train_start: str          # YYYY-MM-DD
    train_end: str
    test_start: str
    test_end: str
    symbol: str               # e.g. "SPY"
    report_path: str
    statistical_validity: dict
    recorded_at: str          # ISO8601 UTC
    registration_hash: str

@dataclass(frozen=True)
class CrossSymbolExemption:
    granted: bool
    reason: str               # >= 16 chars
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

def load_replication_evidence(candidate_id) -> list[ReplicationEvidence]: ...

def get_cross_symbol_exemption(candidate_id) -> CrossSymbolExemption | None: ...

def record_replication_result(
    *, candidate_id, period_id,
    train_start, train_end, test_start, test_end,
    symbol, report_path, statistical_validity,
) -> ReplicationEvidence:
    """Append-only; idempotent on identical (period_id, symbol, path,
    recomputed verdict). Failed verdicts are stored, not rejected.
    Train/test windows must not overlap."""

def record_cross_symbol_exemption(
    *, candidate_id, reason,
) -> CrossSymbolExemption:
    """Permanent; idempotent on same reason; differing reasons raise."""

def summarize_replication_status(candidate_id) -> ReplicationStatus: ...

def assert_replication_ready(candidate_id) -> None:
    """Raises ReplicationViolationError unless replication_ready."""
```

### Rules enforced (with the test that proves each)

| Rule | Test |
|---|---|
| One period only blocks stage 6 | `TestReplicationReadiness.test_one_period_only_blocks` |
| Two periods, same symbol → blocks | `TestReplicationReadiness.test_two_periods_same_symbol_blocks` |
| Two symbols, same period → blocks | `TestReplicationReadiness.test_two_symbols_same_period_blocks` |
| Two periods AND two symbols → passes | `TestReplicationReadiness.test_two_periods_two_symbols_passes` |
| Cross-symbol exemption + 2 periods + 1 symbol → passes | `TestReplicationReadiness.test_cross_symbol_exemption_with_two_periods_one_symbol` |
| Failed evidence retained but not counted | `TestReplicationReadiness.test_failed_evidence_retained_but_not_counted` |
| n_eff-suppressed evidence does not count | `TestReplicationReadiness.test_n_eff_suppressed_evidence_does_not_count` |
| Duplicate (period_id, symbol) idempotent | `TestRecordReplicationResult.test_idempotent_on_identical_repeat` |
| Same key, different report_path → raise | `TestRecordReplicationResult.test_duplicate_key_with_different_report_path_rejected` |
| Same key, different stat block → raise | `TestRecordReplicationResult.test_duplicate_key_with_different_status_rejected` |
| Missing/blank report_path rejected | `TestRecordReplicationResult.test_missing_report_path_rejected` |
| Tampered `statistical_pass=True` lie recomputed to False | `TestRecordReplicationResult.test_user_claimed_pass_recomputed_to_fail` |
| Train/test window overlap rejected | `TestRecordReplicationResult.test_test_window_overlapping_train_rejected` |
| Registration-hash drift rejected | `TestRecordReplicationResult.test_registration_hash_drift_detected` |
| Exemption permanent (idempotent on same reason) | `TestCrossSymbolExemption.test_exemption_idempotent_on_same_reason` |
| Exemption with different reason rejected | `TestCrossSymbolExemption.test_exemption_with_different_reason_rejected` |
| Short reason rejected (must be >= 16 chars) | `TestCrossSymbolExemption.test_short_reason_rejected` |
| State-file tampering raises | `TestStateTampering` (six tests) |
| No public removal/revoke API | `TestNoDowngradeAPI.test_no_removal_functions_in_module` |
| Stage 6 blocked when no replication recorded | `TestProtocolGuardReplicationCheck.test_stage_6_blocked_when_no_replication` |
| Stage 6 allowed when 2 periods × 2 symbols recorded | `TestProtocolGuardReplicationCheck.test_stage_6_allowed_with_two_periods_two_symbols` |
| Stage 6 allowed with exemption | `TestProtocolGuardReplicationCheck.test_stage_6_allowed_with_two_periods_one_symbol_plus_exemption` |
| Stage 5 does not check replication | `TestProtocolGuardReplicationCheck.test_stage_5_does_not_check_replication` |
| `enforce_replication=False` skips check | `TestProtocolGuardReplicationCheck.test_enforce_replication_false_skips_check` |
| Kill list blocks before replication check | `TestProtocolGuardReplicationCheck.test_kill_list_blocks_before_replication_check` |

### Example replication evidence JSON

```json
{
  "version": 1,
  "candidates": {
    "fomc-iv-crush-001": {
      "registration_hash": "9f...e3",
      "evidence": [
        {
          "period_id": "p2025",
          "train_start": "2023-01-03",
          "train_end": "2024-12-31",
          "test_start": "2025-01-02",
          "test_end": "2025-12-31",
          "symbol": "SPY",
          "report_path": "reports/.../stage_3_p2025_SPY.json",
          "statistical_validity": {
            "stage": 3, "n_obs": 250, "horizon_days": 5,
            "n_eff": 50, "n_eff_floor": 30,
            "ci_lower": 0.0012, "ci_upper": 0.0083,
            "permutation_p_value": 0.012, "permutation_alpha": 0.05,
            "statistical_pass": true,
            "metrics_suppressed": false,
            "suppression_reasons": []
          },
          "recorded_at": "2026-05-04T19:00:00+00:00"
        },
        {
          "period_id": "p2025",
          "train_start": "2023-01-03",
          "train_end": "2024-12-31",
          "test_start": "2025-01-02",
          "test_end": "2025-12-31",
          "symbol": "QQQ",
          "report_path": "reports/.../stage_4_p2025_QQQ.json",
          "statistical_validity": {... statistical_pass=true ...},
          "recorded_at": "2026-05-04T19:30:00+00:00"
        },
        {
          "period_id": "p2022",
          "train_start": "2020-01-02",
          "train_end": "2021-12-31",
          "test_start": "2022-01-03",
          "test_end": "2022-12-30",
          "symbol": "SPY",
          "report_path": "reports/.../stage_3_p2022_SPY.json",
          "statistical_validity": {... statistical_pass=true ...},
          "recorded_at": "2026-05-04T20:00:00+00:00"
        }
      ],
      "cross_symbol_exemption": null
    }
  }
}
```

### Example: blocked vs allowed stage 6

**Blocked — only one period, one symbol:**

```python
# After stages 1-5 pass cleanly, but only one replication piece recorded:
record_replication_result(
    candidate_id="fomc-iv-crush-001",
    period_id="p2025", symbol="SPY",
    train_start="2023-01-03", train_end="2024-12-31",
    test_start="2025-01-02", test_end="2025-12-31",
    report_path="reports/.../stage_3_p2025_SPY.json",
    statistical_validity={... passing block ...},
)

assert_protocol_compliant("fomc-iv-crush-001", requested_stage=6)
# raises ReplicationViolationError:
#   candidate 'fomc-iv-crush-001' is not replication-ready:
#     distinct passing periods=1 < required 2 (have: ['p2025']);
#     distinct passing symbols=1 < required 2 (have: ['SPY']);
#     no cross-symbol exemption granted.
#   Stage 6 (paper observation) requires at least 2 distinct passing
#   periods AND at least 2 distinct passing symbols (or a granted
#   cross-symbol exemption). Failed evidence does not count.
```

**Allowed — two periods × two symbols:**

```python
# (p2025, SPY), (p2025, QQQ), (p2022, SPY) all recorded with passing
# statistical_validity blocks
assert_protocol_compliant("fomc-iv-crush-001", requested_stage=6)
# returns Registration; the guard has verified:
#   1. registration is valid + matches recomputed hash
#   2. candidate is not on the kill list
#   3. ladder shows stages 1-5 recorded as pass
#   4. each prior stat block recomputes to statistical_pass=true
#   5. replication has 2 distinct passing periods (p2025, p2022)
#      AND 2 distinct passing symbols (SPY, QQQ)
```

**Allowed via exemption — two periods, one symbol, documented:**

```python
# (p2025, SPY), (p2022, SPY) — only SPY available
record_cross_symbol_exemption(
    candidate_id="fomc-iv-crush-001",
    reason="iVolatility coverage limited to SPY for the windows under test",
)
assert_protocol_compliant("fomc-iv-crush-001", requested_stage=6)
# returns Registration; cross_symbol_exemption.granted=True substitutes
# for the second symbol but the two-period requirement still holds.
```

### Failure modes blocked (post-PR4)

- Single-period claim of generalization → `ReplicationViolationError` at
  stage 6.
- Single-symbol claim of generalization (without an exemption) →
  `ReplicationViolationError` at stage 6.
- Re-recording the same `(period_id, symbol)` with a different result
  to "upgrade" the verdict → `ReplicationViolationError`.
- Tampering with the `statistical_pass` flag inside a stored evidence
  record → recomputation produces False, so the evidence does not count.
- Granting an exemption with a vague reason (< 16 chars) →
  `ReplicationViolationError`.
- Revoking or rewriting an exemption → no public API exists; attempts
  to add one would be detected by the `TestNoDowngradeAPI` contract test.

---

## 6. Kill-Switch Enforcement (§6)

Append-only file, atomic write, no delete API. **Ships in §10.**

```python
# services/research_protocol/kill_list.py — public surface

@dataclass(frozen=True)
class KillEntry:
    candidate_id: str
    registration_hash: str
    killed_at: str          # ISO8601 UTC
    killed_at_stage: int
    kill_reason: str
    supporting_artifacts: tuple[str, ...]

def list_killed() -> list[KillEntry]: ...
def is_killed(candidate_id: str) -> bool: ...
def assert_not_killed(candidate_id: str) -> None: ...

def record_kill(
    *,
    candidate_id: str,
    registration_hash: str,
    stage: int,
    reason: str,
    artifacts: list[str] | None = None,
) -> KillEntry:
    """Idempotent: re-killing returns the existing entry without a second
    append. Stage must be in [1,6]. The on-disk file is replaced atomically
    via tempfile + os.replace; partial writes cannot corrupt the list."""
```

### No-revival enforcement at runtime

Every validation entry-point routes through `protocol_guard.assert_protocol_compliant`,
which calls `assert_not_killed` after `load_registration`. A killed
candidate cannot reach any dataset:

```
script entry → protocol_guard → registration valid? → kill list clear? → run
                                                      └── raises CandidateKilledError
```

### Tampering detection (first version)

- The file is read with strict version check (`version: 1`); unrecognized
  payload → `KillListTamperingError`.
- Future hardening (PR-later): hash chain — each entry stores
  `prev_state_sha256`, validated on load. Out of scope for v1; the API
  surface is unchanged when the chain lands.

---

## 7. Researcher Degrees-of-Freedom Tracking (§8) — ENFORCED (PR6)

State lives at `reports/research_protocol/trial_budget_state.json`.
Every registration that enters the protocol records exactly one trial,
classified by the relationship between the new registration and its
declared `parent_candidate_id`. The default budget is
`MAX_TRIALS_PER_FAMILY_PER_QUARTER` (=3); the 4th registration in a
given `(hypothesis_family, calendar_quarter)` bucket is recorded for
audit history but blocks any subsequent
`assert_protocol_compliant(candidate_id)` call. Revival attempts of a
killed parent are blocked unconditionally — they are the canonical
data-snooping pattern that PR23–PR42 produced.

### Public surface (shipped)

```python
# services/research_protocol/trial_budget.py

TRIAL_BUDGET_VERSION = 1
MAX_TRIALS_PER_FAMILY_PER_QUARTER = 3
DEFAULT_HYPOTHESIS_FAMILY = "unspecified"

MODIFICATION_TYPES = frozenset({
    "new_hypothesis",
    "parameter_change",
    "feature_change",
    "threshold_change",
    "period_change",
    "symbol_change",
    "revival_attempt",
})

@dataclass(frozen=True)
class TrialEntry:
    candidate_id: str
    parent_candidate_id: str | None
    signal_definition_hash: str          # PR5's hash_signal_definition()
    registration_hash: str
    created_at: str                      # registration_timestamp from the JSON
    recorded_at: str                     # ISO8601 UTC at record time
    status: str                          # "registered"
    hypothesis_family: str
    modification_type: str               # one of MODIFICATION_TYPES

@dataclass(frozen=True)
class TrialBudgetSummary:
    hypothesis_family: str
    quarter: str                         # "YYYY-QN"
    trial_count: int
    quarter_budget: int                  # = MAX_TRIALS_PER_FAMILY_PER_QUARTER
    budget_remaining: int
    in_budget_trials: tuple[TrialEntry, ...]
    over_budget_trials: tuple[TrialEntry, ...]
    revival_attempts: tuple[TrialEntry, ...]

def record_trial(registration: Registration) -> TrialEntry:
    """Append a trial. Idempotent on (candidate_id, registration_hash).
    Re-recording the same candidate_id with a different hash raises."""

def classify_candidate_change(
    new_registration: Registration,
    prior_registration: Registration,
) -> str:
    """Return the modification_type. If the prior is on the kill list:
    pre-registered-before-failure → diff_type; otherwise revival_attempt
    unless claimed_modification_type='new_hypothesis' AND
    hypothesis_family differs from the parent's."""

def assert_trial_budget_available(candidate_id: str) -> None:
    """Raise TrialBudgetViolationError if the candidate is a
    revival_attempt, has unknown modification_type, or is the
    >MAX_TRIALS_PER_FAMILY_PER_QUARTER-th in its (family, quarter)."""

def summarize_trial_budget(
    hypothesis_family: str,
    *, quarter: str | None = None,
    reference_timestamp: str | None = None,
) -> TrialBudgetSummary: ...

def quarter_for_timestamp(iso_timestamp: str) -> str: ...
def load_trial_state() -> dict: ...
def list_trials() -> list[TrialEntry]: ...
def get_trial(candidate_id: str) -> TrialEntry | None: ...
```

### Optional registration fields

PR1's registration schema requires fixed top-level keys but tolerates
extra keys; PR6 uses three optional fields when present:

| Field | Effect |
|---|---|
| `hypothesis_family` | The bucket the trial counts against; matches `^[a-z][a-z0-9_]{0,63}$`. Defaults to `"unspecified"`. |
| `parent_candidate_id` | The prior registration this one derives from. Used by `classify_candidate_change`. |
| `claimed_modification_type` | One of `MODIFICATION_TYPES`. Required to bypass `revival_attempt` after a parent is killed (must also use a different `hypothesis_family`). |

These fields are part of the canonical-JSON registration payload, so
they are covered by `registration_hash` — they cannot be edited
post-signing.

### Classification algorithm

1. Compare features (set of `name` fields) → `feature_change`.
2. Compare thresholds (sorted by name) → `threshold_change`.
3. Compare `datasets` block → `symbol_change` if `datasets.symbol`
   differs, else `period_change`.
4. Compare `horizon_days`, `random_seed`, `transformations` →
   `parameter_change`.
5. Otherwise → `parameter_change` (catch-all).

If the parent is on the kill list:
- If the new registration's `registration_timestamp` precedes the
  parent's `killed_at` (lexicographic ISO8601), keep the diff_type —
  this is a *legitimate pre-registered alternate*.
- Else if `claimed_modification_type="new_hypothesis"` AND the new
  `hypothesis_family` is non-empty and differs from the parent's,
  classify as `new_hypothesis`.
- Else: `revival_attempt`.

### Rule → test mapping

| Rule | Test |
|---|---|
| First registration consumes one trial | `TestRecordTrial.test_first_registration_consumes_one_trial` |
| Repeated registration is idempotent | `TestRecordTrial.test_repeated_same_registration_idempotent` |
| Hash drift on same candidate_id rejected | `TestRecordTrial.test_recording_with_changed_registration_hash_rejected` |
| Default `hypothesis_family` when unspecified | `TestRecordTrial.test_default_hypothesis_family_when_unspecified` |
| Invalid family name rejected | `TestRecordTrial.test_invalid_hypothesis_family_rejected` |
| Invalid `claimed_modification_type` rejected | `TestRecordTrial.test_invalid_claimed_modification_type_rejected` |
| Unknown parent registration rejected | `TestRecordTrial.test_record_with_unknown_parent_raises` |
| `threshold_change` classified | `TestClassifyCandidateChange.test_threshold_change_classified` |
| `feature_change` classified | `TestClassifyCandidateChange.test_feature_change_classified` |
| `period_change` classified | `TestClassifyCandidateChange.test_period_change_classified` |
| `symbol_change` classified | `TestClassifyCandidateChange.test_symbol_change_classified` |
| `parameter_change` classified | `TestClassifyCandidateChange.test_parameter_change_classified` |
| Killed parent → `revival_attempt` | `TestClassifyCandidateChange.test_killed_parent_makes_revival_attempt` |
| Pre-registered alternate is not revival | `TestClassifyCandidateChange.test_pre_registered_before_failure_is_not_revival` |
| Materially different new hypothesis allowed | `TestClassifyCandidateChange.test_materially_different_new_hypothesis_allowed` |
| Same-family `new_hypothesis` claim still revival | `TestClassifyCandidateChange.test_claim_without_family_change_still_revival` |
| First three pass, fourth blocks | `TestAssertTrialBudgetAvailable.test_fourth_in_same_family_quarter_blocks` |
| Different quarter resets budget | `TestAssertTrialBudgetAvailable.test_different_quarter_resets_budget` |
| Different family resets budget | `TestAssertTrialBudgetAvailable.test_different_family_resets_budget` |
| `revival_attempt` blocks | `TestAssertTrialBudgetAvailable.test_revival_attempt_blocks` |
| Pre-registered alternate not blocked | `TestAssertTrialBudgetAvailable.test_pre_registered_alternate_not_blocked` |
| Tampered trial state raises | `TestTrialStateTampering.*` (six tests) |
| No public delete/reset/clear/purge API | `TestNoRemovalAPI.*` |
| Protocol guard records + gates | `TestProtocolGuardTrialBudget.*` (5 tests) |

### Example trial state JSON

```json
{
  "version": 1,
  "trials": [
    {
      "candidate_id": "fomc-iv-crush-001",
      "parent_candidate_id": null,
      "signal_definition_hash": "8e1b...",
      "registration_hash": "9f...e3",
      "created_at": "2026-04-15T10:00:00Z",
      "recorded_at": "2026-04-15T10:00:01.234567+00:00",
      "status": "registered",
      "hypothesis_family": "iv_crush",
      "modification_type": "new_hypothesis"
    },
    {
      "candidate_id": "fomc-iv-crush-002",
      "parent_candidate_id": "fomc-iv-crush-001",
      "signal_definition_hash": "f3c2...",
      "registration_hash": "ab12...",
      "created_at": "2026-04-20T10:00:00Z",
      "recorded_at": "2026-04-20T10:00:01.000000+00:00",
      "status": "registered",
      "hypothesis_family": "iv_crush",
      "modification_type": "threshold_change"
    },
    {
      "candidate_id": "fomc-iv-crush-003-revival",
      "parent_candidate_id": "fomc-iv-crush-001",
      "signal_definition_hash": "deadbeef...",
      "registration_hash": "cd34...",
      "created_at": "2099-12-31T10:00:00Z",
      "recorded_at": "2099-12-31T10:00:01.000000+00:00",
      "status": "registered",
      "hypothesis_family": "iv_crush",
      "modification_type": "revival_attempt"
    }
  ]
}
```

The third entry is recorded for audit history (so the attempt is
traceable) but `assert_trial_budget_available("fomc-iv-crush-003-revival")`
raises `TrialBudgetViolationError`.

### Example: allowed vs blocked trial flow

**Allowed — three sibling threshold variants in 2026-Q2:**

```python
# All registered before any failure; same family; same quarter.
for cand_id, value in [
    ("iv-crush-001", -0.05),  # new_hypothesis (no parent)
    ("iv-crush-002", -0.04),  # threshold_change vs 001
    ("iv-crush-003", -0.03),  # threshold_change vs 002 (or 001)
]:
    # ... write registration JSON with hypothesis_family="iv_crush",
    #     parent_candidate_id=<prior>, threshold value=value ...
    assert_protocol_compliant(cand_id)   # records trial + checks budget
# Three trials recorded in 2026-Q2 / iv_crush. Budget exhausted.
```

**Blocked — fourth variant in same quarter:**

```python
# fourth registration in the same family/quarter
assert_protocol_compliant("iv-crush-004")
# raises TrialBudgetViolationError:
#   trial budget exceeded for hypothesis_family='iv_crush' in quarter
#   2026-Q2: candidate 'iv-crush-004' is the 4th registration but the
#   per-quarter limit is 3. Wait for the next quarter or pre-register
#   fewer variants.
```

**Blocked — revival of a killed candidate:**

```python
# After iv-crush-001 is killed at stage 3, registering a derivative
# (same hypothesis_family, threshold tweak) is auto-classified as a
# revival_attempt:
assert_protocol_compliant("iv-crush-001-retry")
# raises TrialBudgetViolationError:
#   candidate 'iv-crush-001-retry' is a revival_attempt of a killed
#   candidate; revival is prohibited under RESEARCH_PROTOCOL §6.2 / §8.
```

**Allowed — genuinely new hypothesis after a kill:**

```python
# After iv-crush-001 is killed, register a candidate in a DIFFERENT
# hypothesis_family with claimed_modification_type='new_hypothesis':
# weekend_decay-001 with hypothesis_family="weekend_decay" and
#                       claimed_modification_type="new_hypothesis"
assert_protocol_compliant("weekend-decay-001")
# Returns Registration. Classified as new_hypothesis; counts against
# the weekend_decay quarterly budget, not the iv_crush one.
```

### How this would have counted PR23–PR42 correctly

The previous failed candidate was discovered after a sequence of
exploratory diagnostics — what `SNOOPING_METADATA.diagnostics_explored_count`
recorded as `6` was, in practice, an order-of-magnitude undercount of
the true number of researcher choices. Under PR6:

- Each of PR23–PR42's intermediate diagnostic *modules* (late-trend,
  candidate signal, fragility, paper eval, sensitivity, boundary purge)
  whose output was used to refine the next iteration would have
  required a registered trial under the same `hypothesis_family`
  (e.g., `"high_vol_trend_early"`). The 4th iteration in the same
  quarter would have triggered `TrialBudgetViolationError` and
  forced the researcher to stop before the candidate was finalized.
- PR42's final candidate would have been recorded as
  `modification_type="threshold_change"` against the prior iteration —
  with `parent_candidate_id` pointing at it. After the 2022 cross-period
  failure killed the candidate, any further iteration (parameter
  tweak, threshold sweep, regime-conditioned variant) would have been
  classified as `revival_attempt` and blocked.
- The seven-rule retrospective in `RESEARCH_PROTOCOL.md` §10 lists
  "honest `N_trials` accounting" as one of the rules that, alone,
  would have prevented the failure. PR6's append-only trial state
  file is exactly that mechanism: there is no public surface to
  remove or rewrite a trial, and tampered state raises on read.

---

## 8. Audit Logging + Reproducibility (§7) — ENFORCED (PR5)

Every protocol decision is recorded as one JSON Lines record in
`reports/research_protocol/audit_log.jsonl`. The file is append-only:
the module exposes no public function to remove, clear, reset,
truncate, or rotate it. Each record carries the deterministic
fingerprints needed to reproduce the decision: ``registration_hash``,
``signal_definition_hash``, ``dataset_hash``, ``code_version`` (auto-
detected via `git rev-parse HEAD` when not supplied).

The audit logger is wired into the existing entry points so every
protocol decision logs without explicit caller cooperation:

| Entry point | Event(s) emitted |
|---|---|
| `assert_protocol_compliant` (load) | `registration_rejected` (block) or `registration_loaded` (pass) |
| `assert_protocol_compliant` (kill check) | `kill_list_block` (block) |
| `assert_protocol_compliant` (ladder gate) | `ladder_block` (block) |
| `assert_protocol_compliant` (stat defensive recompute) | `statistical_block` (block) |
| `assert_protocol_compliant` (replication gate) | `replication_block` (block) |
| `assert_protocol_compliant` (success path) | `protocol_pass` (pass) |
| `record_stage_result` | `stage_result_recorded` (record) |
| `record_replication_result` | `replication_evidence_recorded` (record) |
| `record_kill` | `candidate_killed` (record) |

Audit failures must NEVER mask a protocol decision. All emit calls go
through :func:`safe_emit_audit_event`, which propagates
:class:`AuditLogTamperingError` (developer-error: malformed event
payload) but swallows transient OS errors so the underlying
:class:`ProtocolViolationError` always reaches the caller.

### Audit event schema

```python
@dataclass(frozen=True)
class AuditEvent:
    event_id: str                              # uuid4 hex; unique per emit
    timestamp_utc: str                         # ISO8601 UTC, e.g. "2026-05-04T19:00:00.123456+00:00"
    candidate_id: str | None                   # None only for registration_rejected
    event_type: str                            # one of EVENT_TYPES
    protocol_stage: int | None                 # None or 0..6
    decision: str                              # "pass" | "block" | "record"
    reason: str | None                         # human-readable reason for the decision
    registration_hash: str | None              # 64-char SHA256 hex
    dataset_hash: str | None                   # SHA256 of the dataset used (when applicable)
    signal_definition_hash: str | None         # see hash_signal_definition()
    report_path: str | None                    # artifact path produced by the decision
    code_version: str | None                   # git HEAD SHA, auto-detected
    metadata: dict[str, Any]                   # event-specific extra fields
    audit_log_version: int                     # = 1
```

### Allowed event types

```python
EVENT_TYPES = frozenset({
    "registration_loaded",
    "registration_rejected",
    "kill_list_block",
    "ladder_block",
    "statistical_block",
    "replication_block",
    "protocol_pass",
    "stage_result_recorded",
    "replication_evidence_recorded",
    "candidate_killed",
})

DECISIONS = frozenset({"pass", "block", "record"})
```

### Public surface (shipped)

```python
# services/research_protocol/audit_logger.py

def emit_audit_event(*, event_type, decision, candidate_id=None,
                     protocol_stage=None, reason=None,
                     registration_hash=None, dataset_hash=None,
                     signal_definition_hash=None, report_path=None,
                     code_version=None, metadata=None) -> AuditEvent: ...

def safe_emit_audit_event(**kwargs) -> AuditEvent | None:
    """Like emit_audit_event but swallows transient errors. Re-raises
    AuditLogTamperingError so developer bugs surface."""

def load_audit_events(*, candidate_id=None,
                      event_type=None) -> list[AuditEvent]:
    """Read + validate every line. Tampered lines raise
    AuditLogTamperingError."""

# Fingerprints
def hash_file(path) -> str: ...
def hash_dataframe_schema_or_csv(path) -> DatasetFingerprint: ...
def hash_signal_definition(registration_body: dict) -> str: ...
def build_run_fingerprint(*, registration_body, registration_hash,
                          datasets=None, code_version=None) -> dict: ...
def detect_git_commit(*, cwd=None) -> str | None: ...
```

### Rule → test mapping

| Rule | Test |
|---|---|
| Pass event logged | `TestEmitAuditEvent.test_pass_event_logged_with_required_fields` |
| Block event logged | `TestEmitAuditEvent.test_block_event_logged` |
| event_id unique across emits | `TestEmitAuditEvent.test_event_id_unique_across_emits` |
| Timestamp parses + carries UTC offset | `TestEmitAuditEvent.test_timestamp_is_utc_iso8601` |
| Invalid event_type rejected | `TestEmitAuditEvent.test_invalid_event_type_rejected` |
| Invalid decision rejected | `TestEmitAuditEvent.test_invalid_decision_rejected` |
| Invalid protocol_stage rejected | `TestEmitAuditEvent.test_invalid_protocol_stage_rejected` |
| Tampered (non-JSON) line raises | `TestTamperingDetection.test_invalid_json_line_rejected` |
| Tampered (missing fields) raises | `TestTamperingDetection.test_missing_required_keys_rejected` |
| Tampered (bad event_type) raises | `TestTamperingDetection.test_unknown_event_type_rejected` |
| Tampered (bad decision) raises | `TestTamperingDetection.test_unknown_decision_rejected` |
| Tampered (non-UTC timestamp) raises | `TestTamperingDetection.test_non_utc_timestamp_rejected` |
| Tampered (naive timestamp) raises | `TestTamperingDetection.test_naive_timestamp_rejected` |
| Wrong audit_log_version raises | `TestTamperingDetection.test_wrong_audit_log_version_rejected` |
| No remove/clear/reset API on module | `TestAppendOnlyAPI.test_no_removal_or_clear_functions` |
| `safe_emit_audit_event` propagates dev errors | `TestSafeEmit.test_safe_emit_propagates_input_validation_errors` |
| `hash_file`: identical content → same hash | `TestHashFile.test_identical_content_same_hash` |
| `hash_file`: different content → different hash | `TestHashFile.test_different_content_different_hash` |
| Dataset fingerprint stable for identical CSV | `TestHashDataframeSchemaOrCsv.test_identical_data_same_hashes` |
| Dataset fingerprint changes when row changes | `TestHashDataframeSchemaOrCsv.test_different_row_changes_sha` |
| Dataset fingerprint changes when columns change | `TestHashDataframeSchemaOrCsv.test_added_column_changes_column_set_hash` |
| Signal hash stable for identical body | `TestHashSignalDefinition.test_identical_body_identical_hash` |
| Signal hash changes when threshold changes | `TestHashSignalDefinition.test_threshold_change_changes_hash` |
| Signal hash ignores non-defining metadata | `TestHashSignalDefinition.test_irrelevant_metadata_does_not_change_signal_hash` |
| `build_run_fingerprint` stable for same inputs | `TestBuildRunFingerprint.test_fingerprint_is_stable_for_same_inputs` |
| `build_run_fingerprint` changes with dataset | `TestBuildRunFingerprint.test_fingerprint_changes_with_dataset_content` |
| `build_run_fingerprint` changes with signal | `TestBuildRunFingerprint.test_fingerprint_changes_with_signal_definition` |
| Missing-registration emits `registration_rejected` | `TestProtocolGuardAuditEmits.test_missing_registration_emits_registration_rejected` |
| Killed candidate emits `kill_list_block` | `TestProtocolGuardAuditEmits.test_killed_candidate_emits_kill_list_block` |
| Stage skip emits `ladder_block` | `TestProtocolGuardAuditEmits.test_ladder_block_emits_ladder_block_event` |
| Success emits `protocol_pass` | `TestProtocolGuardAuditEmits.test_protocol_pass_emits_protocol_pass` |
| Replication failure emits `replication_block` | `TestProtocolGuardAuditEmits.test_replication_block_emits_replication_block` |
| `record_kill` emits `candidate_killed` | `TestRecordEntryPointsEmitAuditEvents.test_record_kill_emits_candidate_killed` |
| `record_stage_result` emits `stage_result_recorded` | `TestRecordEntryPointsEmitAuditEvents.test_record_stage_result_emits_stage_result_recorded` |
| Stage 2+ stat metadata appears in audit event | `TestRecordEntryPointsEmitAuditEvents.test_record_stage_2_pass_includes_statistical_metadata` |
| `record_replication_result` emits the right event | `TestRecordEntryPointsEmitAuditEvents.test_record_replication_result_emits_replication_evidence_recorded` |

### Example audit log records

```jsonl
{"audit_log_version":1,"candidate_id":"fomc-iv-crush-001","code_version":"3a9f...","dataset_hash":null,"decision":"pass","event_id":"4b3a...","event_type":"registration_loaded","metadata":{},"protocol_stage":2,"reason":null,"registration_hash":"9f...e3","report_path":null,"signal_definition_hash":null,"timestamp_utc":"2026-05-04T19:00:00.123456+00:00"}
{"audit_log_version":1,"candidate_id":"fomc-iv-crush-001","code_version":"3a9f...","dataset_hash":null,"decision":"pass","event_id":"7c8e...","event_type":"protocol_pass","metadata":{"enforce_replication":true,"enforce_statistical_validity":true},"protocol_stage":2,"reason":null,"registration_hash":"9f...e3","report_path":null,"signal_definition_hash":null,"timestamp_utc":"2026-05-04T19:00:00.234567+00:00"}
{"audit_log_version":1,"candidate_id":"fomc-iv-crush-001","code_version":"3a9f...","dataset_hash":null,"decision":"record","event_id":"d2f1...","event_type":"stage_result_recorded","metadata":{"metrics_suppressed":false,"n_eff":50,"statistical_pass":true,"status":"pass"},"protocol_stage":2,"reason":"stage_2 status=pass","registration_hash":"9f...e3","report_path":"reports/.../stage_2_oos.json","signal_definition_hash":null,"timestamp_utc":"2026-05-04T19:00:01.345678+00:00"}
```

### Example: pass vs block event

**Pass — protocol gate cleared at stage 2:**

```python
assert_protocol_compliant("fomc-iv-crush-001", requested_stage=2)
# → returns Registration; emits two events:
#   1. registration_loaded   (decision=pass, protocol_stage=2)
#   2. protocol_pass         (decision=pass, protocol_stage=2,
#                              metadata={enforce_statistical_validity: true,
#                                        enforce_replication: true})
```

**Block — replication missing at stage 6:**

```python
assert_protocol_compliant("fomc-iv-crush-001", requested_stage=6)
# raises ReplicationViolationError; emits three events:
#   1. registration_loaded   (decision=pass, protocol_stage=6)
#   2. (ladder check passes, no event)
#   3. (statistical recompute passes, no event)
#   4. replication_block     (decision=block, protocol_stage=6,
#                              reason="distinct passing periods=1 < required 2 ...")
```

### Reproducibility guarantees

A consumer of any protocol artifact can reconstruct the decision context
deterministically from the audit trail:

1. **Registration provenance.** Every `protocol_pass` event carries the
   `registration_hash`. Combined with the registration JSON on disk
   (whose hash field equals `compute_registration_hash(payload)`), the
   exact pre-registration document is identifiable.
2. **Signal definition.** `hash_signal_definition(registration_body)`
   produces a SHA256 over the *behaviour-determining* slice of the
   registration (features + thresholds + transformations +
   forbidden_changes + falsification + horizon + hypothesis mechanism +
   direction). Distinct from `registration_hash`, which covers the entire
   document. Two candidates with identical signals but different
   metadata (timestamps, citations) produce the same
   `signal_definition_hash`.
3. **Dataset provenance.** `hash_dataframe_schema_or_csv` produces a
   stable `DatasetFingerprint` containing SHA256 of file bytes, file
   size, row count, sorted column-set hash, and (when a date-like column
   exists) the min/max date observed. Identical CSV content produces
   identical SHA256.
4. **Code provenance.** `detect_git_commit()` records the current
   `git rev-parse HEAD`. Auto-attached to every event when the caller
   does not pass `code_version` explicitly.
5. **Decision history.** `load_audit_events(candidate_id=...)` returns
   the chronological event sequence for any candidate. The append-only
   contract (no public removal API; tampered lines raise on read)
   ensures the history is faithful.
6. **Tamper detection.** Every read calls `_validate_event_record` on
   each line; missing/invalid fields, non-UTC timestamps, unknown event
   types, or wrong `audit_log_version` all raise
   :class:`AuditLogTamperingError` rather than silently filter.
7. **`build_run_fingerprint`** packages all of the above into a single
   dict that scripts can embed in any report file. Two runs with
   identical registration + identical datasets produce byte-identical
   fingerprints (modulo the wall-clock `fingerprinted_at` field).

---

## 9. Integration Plan — shipped (PR7)

Three primary research scripts now expose the protocol flags via a
shared helper:

| Script | Stage | Module-level constant |
|---|---|---|
| `scripts/run_model_ready_dataset_smoke.py` | 0 (infra) | `PROTOCOL_STAGE = 0` |
| `scripts/run_ml_regime_validation.py` | 2 (single OOS) | `PROTOCOL_STAGE = 2` |
| `scripts/run_ml_regime_validation_cross_period.py` | 3 (cross-period) | `PROTOCOL_STAGE = 3` |

Each script wires the helper at two points:

- `parse_args()` → `add_protocol_arguments(parser, expected_stage=PROTOCOL_STAGE)`
- top of `main()` → `enforce_protocol_from_args(args, expected_stage=PROTOCOL_STAGE)`

### Default behavior — non-enforcing

Without `--enforce-protocol`, scripts emit the diagnostic warning to
stderr and proceed with the legacy code path, unchanged from PR6:

```
$ .venv/bin/python scripts/run_ml_regime_validation.py
[protocol] WARNING: Protocol enforcement disabled; output is diagnostic only.
PivotQuant realized_vol_60d strict validation
...
```

### Enforced behavior

Adding `--enforce-protocol` turns on `assert_protocol_compliant` at the
top of `main()`. The candidate must already be pre-registered (a
`reports/research_protocol/registrations/<id>.json` file with a valid
hash) and not on the kill list, the trial must be in budget, and any
prior stages must have a passing recomputed statistical verdict.

```
$ .venv/bin/python scripts/run_ml_regime_validation.py \
    --candidate-id fomc-iv-crush-001 \
    --enforce-protocol \
    --train-years 2023 2024 \
    --test-year 2025
```

Failure modes (all raise before any analysis runs):

| Symptom | Error |
|---|---|
| no `--candidate-id` under enforcement | `ProtocolCLIError` |
| `--protocol-stage` mismatch with the script's expected stage | `ProtocolCLIError` |
| missing/edited registration JSON | `RegistrationMissingError` / `RegistrationInvalidError` / `RegistrationHashMismatchError` |
| trial budget exhausted (4th in family/quarter) | `TrialBudgetViolationError` |
| candidate is a `revival_attempt` of a killed parent | `TrialBudgetViolationError` |
| candidate on kill list | `CandidateKilledError` |
| stage 3+ requested before required priors recorded | `StageGateError` |
| prior stage's statistical verdict recomputes to fail | `StatisticalViolationError` |
| stage 6 without 2 periods × 2 symbols (or exemption) | `ReplicationViolationError` |

### Example commands

#### Non-enforcing legacy smoke (same as before PR7)

```bash
# Default — no enforcement, diagnostic-only warning emitted
.venv/bin/python scripts/run_model_ready_dataset_smoke.py \
    --analysis-start-date 2025-01-02 --analysis-end-date 2025-12-31
```

#### Enforced flows for each stage

```bash
# Stage 0 — dataset infrastructure under enforcement (registration + trial
# budget recorded; no validation logic gated)
.venv/bin/python scripts/run_model_ready_dataset_smoke.py \
    --analysis-start-date 2025-01-02 --analysis-end-date 2025-12-31 \
    --candidate-id fomc-iv-crush-001 \
    --enforce-protocol

# Stage 2 — single-period OOS validation
.venv/bin/python scripts/run_ml_regime_validation.py \
    --train-years 2023 2024 --test-year 2025 \
    --candidate-id fomc-iv-crush-001 \
    --enforce-protocol

# Stage 3 — cross-period validation (requires Stage 1 + Stage 2 passes
# in the validation_ladder_state.json before this can clear)
.venv/bin/python scripts/run_ml_regime_validation_cross_period.py \
    --train-years 2021 --test-year 2022 \
    --train-coverage-start 2021-04-05 --train-coverage-end 2021-12-31 \
    --train-is-partial \
    --candidate-id fomc-iv-crush-001 \
    --enforce-protocol
```

#### Stage-mismatch rejection

```bash
# Trying to run the cross-period script while claiming stage 2 fails:
$ .venv/bin/python scripts/run_ml_regime_validation_cross_period.py \
      --candidate-id fomc-iv-crush-001 \
      --protocol-stage 2 \
      --enforce-protocol
ProtocolCLIError: this script runs at protocol stage 3; got --protocol-stage=2.
Either use the matching stage flag or run a script that targets a different stage.
```

There is no path through any of the three scripts that:
- runs without a registration *when enforcement is on*,
- runs with a tampered registration,
- runs on a killed candidate,
- skips ladder stages,
- silently passes a cross-period failure,
- bypasses the trial budget,
- runs without a recorded audit-log entry per decision.

Each of those paths terminates in a `ProtocolViolationError` subclass
before any downstream artifact is written. The non-enforcing default
preserves backward compatibility for legacy smoke commands and prints
a clear warning that the output is diagnostic-only.

---

## 10. Implementation Inventory (PR1 + PR2 + PR3 + PR4 + PR5 + PR6 + PR7 shipped)

**Shipped surface as of PR7:**

| Module | PR | Purpose |
|---|---|---|
| `services/research_protocol/_paths.py` | PR1+PR2+PR4+PR5+PR6 | Protocol root + ladder/replication/audit-log/trial-budget paths; env-overridable. |
| `services/research_protocol/errors.py` | PR1+PR2+PR3+PR4+PR5+PR6 | `ProtocolViolationError` hierarchy: `Registration*`, `CandidateKilledError`, `StageGateError`, `ValidationLadderTamperingError`, `StatisticalViolationError`, `ReplicationViolationError`, `AuditLogTamperingError`, `TrialBudgetViolationError`. |
| `services/research_protocol/registration.py` | PR1 | Schema, canonical-JSON hash, load, validate. |
| `services/research_protocol/kill_list.py` | PR1+PR5 | Append-only kill list; atomic write; PR5 emits `candidate_killed`. |
| `services/research_protocol/validation_ladder.py` | PR2+PR3+PR5 | Stage progression gate; PR3 adds stat-block enforcement; PR5 emits `stage_result_recorded`. |
| `services/research_protocol/statistical_guard.py` | PR3 | n_eff floor, block-bootstrap CI, permutation test, verdict combinator, suppression sanitizer. |
| `services/research_protocol/replication_guard.py` | PR4+PR5 | Cross-period + cross-symbol evidence; exemption; PR5 emits `replication_evidence_recorded`. |
| `services/research_protocol/audit_logger.py` | PR5+PR6 | JSONL audit log + fingerprinting helpers + git-commit detection; PR6 adds `trial_recorded` / `trial_budget_block` to `EVENT_TYPES`. |
| `services/research_protocol/trial_budget.py` | PR6 | Trial accounting; classification (feature / threshold / parameter / period / symbol / revival_attempt / new_hypothesis); per-family/quarter budget; pre-registered-before-failure exception. |
| `services/research_protocol/cli_protocol.py` | PR7 | `add_protocol_arguments(parser, expected_stage)` + `enforce_protocol_from_args(args, expected_stage)`. Standard wiring helper for any script that runs at a known stage. |
| `services/research_protocol/protocol_guard.py` | PR1+PR2+PR3+PR4+PR5+PR6 | `assert_protocol_compliant(candidate_id, *, requested_stage=None, enforce_statistical_validity=True, enforce_replication=True, enforce_trial_budget=True)`; PR6 records the trial after registration and gates before kill-list. |
| `scripts/run_model_ready_dataset_smoke.py` | PR7 | Dataset infrastructure script; `PROTOCOL_STAGE = 0`. Calls `enforce_protocol_from_args` at top of `main()`. |
| `scripts/run_ml_regime_validation.py` | PR7 | Single-period OOS validation; `PROTOCOL_STAGE = 2`. |
| `scripts/run_ml_regime_validation_cross_period.py` | PR7 | Cross-period validation; `PROTOCOL_STAGE = 3`. |

Tests:

| Test file | PR | Coverage |
|---|---|---|
| `tests/.../test_registration.py` | PR1 | Schema, hash determinism, post-signing-edit detection, missing-file, schema validation (14). |
| `tests/.../test_kill_list.py` | PR1 | Append-only contract, idempotent re-kill, atomic write, invalid input rejection, version tampering, no-revival API contract (18). |
| `tests/.../test_validation_ladder.py` | PR2 + PR3 | `assert_stage_allowed`, `record_stage_result` (idempotency, no-overwrite, hash-drift detection), state tampering, no-downgrade API contract, stat-block enforcement on stage 2+ (54). |
| `tests/.../test_statistical_guard.py` | PR3 | n_eff, bootstrap CI determinism + behavior, permutation rejection + Phipson-Smyth floor + one-sided inversion, verdict combinator rule combinations, audit-field preservation, tampering detection via verdict round-trip (49). |
| `tests/.../test_replication_guard.py` | PR4 | Empty-state, append-only record + idempotency, dedup on `(period_id, symbol)`, validator regexes, train-test window non-overlap, hash drift detection, exemption permanence, state tampering, no-revocation API contract, all 9 spec readiness cases (39). |
| `tests/.../test_audit_logger.py` | PR5 | `emit_audit_event` (shape, uniqueness, UTC timestamp, validation), `safe_emit_audit_event` propagation rules, filters, tampering detection, no-removal API contract, fingerprint stability + change detection (44). |
| `tests/.../test_trial_budget.py` | PR6 | `quarter_for_timestamp` boundaries, `record_trial` (idempotency, hash-drift, default family, validator regexes), `classify_candidate_change` (all 7 modification types + pre-registered-before-failure + materially-different-new-hypothesis), `assert_trial_budget_available` (3 pass / 4 block / different quarter resets / different family resets / revival blocks / pre-registered alternate not blocked), `summarize_trial_budget`, state tampering (6 cases), no-removal API contract (47). |
| `tests/.../test_cli_protocol.py` | PR7 | `add_protocol_arguments` (defaults, expected_stage default, mutex), `enforce_protocol_from_args` (disabled/diagnostic warning, missing candidate_id, missing stage, mismatched stage, returns `Registration`, killed candidate raises, revival_attempt raises, invalid stage value raises), 3-script integration smoke (each script imports cleanly + exposes `PROTOCOL_STAGE`) (18). |
| `tests/.../test_protocol_guard.py` | PR1+PR2+PR3+PR4+PR5+PR6 | Missing reg / hash mismatch / killed / ladder block / stat block / replication block / trial budget block → raise; valid path returns Registration; check ordering; defensive recomputes; every decision path emits the matching audit event; **PR6:** trial recorded on first call, fourth blocks, revival blocks, opt-out works (38). |

Total: **317 tests** in `tests/unit/test_research_protocol/`, all passing.

### Usage

```python
from services.research_protocol.protocol_guard import assert_protocol_compliant
from services.research_protocol.statistical_guard import (
    compute_bootstrap_ci, run_permutation_test, evaluate_statistical_validity,
    suppress_metrics_if_invalid,
)
from services.research_protocol.validation_ladder import record_stage_result

# A complete Stage-2 entry-point:
def run_stage_2(candidate_id: str):
    registration = assert_protocol_compliant(
        candidate_id, requested_stage=2,
    )

    # ... compute filtered candidate forward returns ...
    forward_returns: list[float] = ...
    baseline_returns: list[float] = ...
    horizon_days = registration.horizon_days

    ci_lower, ci_upper = compute_bootstrap_ci(
        forward_returns, block_size=horizon_days,
        iterations=10_000, rng_seed=registration.random_seed,
    )
    p_value = run_permutation_test(
        signal_returns=forward_returns,
        baseline_returns=baseline_returns,
        n_iter=10_000, rng_seed=registration.random_seed,
    )
    verdict = evaluate_statistical_validity(
        stage=2, n_obs=len(forward_returns), horizon_days=horizon_days,
        ci_lower=ci_lower, ci_upper=ci_upper,
        permutation_p_value=p_value,
    )
    metrics = {
        "win_rate": ..., "mean_return": ..., "sharpe": ...,
    }
    metrics = suppress_metrics_if_invalid(metrics, verdict=verdict)
    # `metrics` is now safe to write to a report file.

    record_stage_result(
        candidate_id=candidate_id, stage=2,
        passed=verdict.statistical_pass,
        report_path="reports/.../stage_2_report.json",
        metadata={
            "run_timestamp": "...", "dataset_identifier": "...",
            "statistical_validity": {
                "stage": 2,
                "n_obs": len(forward_returns),
                "horizon_days": horizon_days,
                "ci_lower": ci_lower, "ci_upper": ci_upper,
                "permutation_p_value": p_value,
                "permutation_alpha": 0.05,
            },
        },
    )
```

The guard cannot be opt-out. Any script that omits it runs without a
recorded registration hash and is rejected by future audit-logger
checks (PR5). The guard is therefore both a runtime check *and* a
prerequisite for producing reproducible artifacts at all.

### Usage (post-§10, pre-integration)

```python
from services.research_protocol.protocol_guard import assert_protocol_compliant

# In any future validation script:
def main(candidate_id: str):
    registration = assert_protocol_compliant(candidate_id)
    # registration.registration_hash is referenced by every downstream artifact.
    ...
```

The guard cannot be opt-out. Any script that omits it runs without a
recorded registration hash and is rejected by future audit-logger checks
(PR5). The guard is therefore both a runtime check *and* a prerequisite
for producing reproducible artifacts at all.

---

## 11. Residual Surface After PR7

Now blocked by the runtime guards (PR1 + PR2 + PR3 + PR4 + PR5 + PR6):
- Running validation without a pre-registration.
- Editing a registration after signing without re-hashing.
- Re-evaluating a killed candidate.
- Skipping ladder stages.
- Overwriting a recorded stage result.
- Re-using a `candidate_id` after editing the registration.
- Recording a stage 2+ result without a `statistical_validity` block.
- Recording a stage 2+ result as `passed=True` while the recomputed
  verdict fails (n_eff floor, CI, or permutation).
- Tampering with the `statistical_pass` flag in a stored stat or
  evidence block — recomputed from inputs on every read.
- Displaying performance metrics from a sub-floor sample —
  `suppress_metrics_if_invalid` removes them.
- Reaching stage 6 with one period only.
- Reaching stage 6 with one symbol and no documented exemption.
- Double-counting evidence by re-recording the same `(period_id, symbol)`.
- Counting failed evidence toward replication readiness.
- Granting a vague (< 16 char) cross-symbol exemption.
- Revoking or rewriting an existing exemption.
- Producing a protocol artifact without an audit-log entry.
- Tampering with the audit log.
- Removing or clearing audit entries.
- **Registering a 4th candidate in the same `(hypothesis_family, quarter)`** —
  recorded for audit history but `assert_protocol_compliant` rejects it
  with `TrialBudgetViolationError`.
- **Reviving a killed candidate** under any of the seven modification
  types — auto-classified as `revival_attempt` and unconditionally
  blocked.
- **Skirting the budget by re-using the parent's `candidate_id`** with
  a tweaked registration — `record_trial` rejects on hash drift; a new
  candidate_id starts a new trial entry that counts against the budget.
- **Tampering with the trial state file** — version-checked,
  schema-validated, unknown `modification_type` values rejected.
- **Removing or rewriting trial entries** — there is no public API
  surface for it; contract tests forbid any module member matching
  remove/delete/clear/reset/purge/downgrade/rewrite.

Still **not** blocked (deferred to a separate integration PR):

- **Bonferroni-tightened α at Stage 5.** The permutation test currently
  uses α=0.05; tightening to `α / N_trials` would multiply
  `summarize_trial_budget(...).trial_count` into
  `evaluate_statistical_validity` automatically. Out of scope for PR6
  (statistical_guard remains pure / inputs-only).
- **Audit-event signing / external anchoring.** Append-only at the API,
  tampered files raise on read, but no hash chain across lines or
  cryptographic signature. A privileged user with filesystem access
  could rewrite history; the contract relies on filesystem ACLs.
- **Bonferroni-tightened α at Stage 5.** Currently α=0.05; tightening
  to `α / N_trials` would multiply
  `summarize_trial_budget(...).trial_count` into
  `evaluate_statistical_validity` automatically. Out of scope for the
  shipped enforcement layer.
- **Audit-event signing / external anchoring.** Append-only at the
  API; tampered files raise on read; but no hash chain across lines
  or cryptographic signature.

The "integration into existing scripts" residual gap from prior PRs is
**closed by PR7**. Three primary research scripts now expose the
enforcement flags and call `enforce_protocol_from_args` at the top of
`main()`. Default behavior is unchanged (legacy non-enforcing); users
opt in via `--enforce-protocol`.

These remaining gaps are documented so they cannot be silently relied on.
