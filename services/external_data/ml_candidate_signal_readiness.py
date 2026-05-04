"""Candidate signal readiness checklist for paper observation.

Aggregates results from late-trend removal validation, fragility diagnostics,
paper evaluation stability, and sensitivity analysis into a single
paper-observation readiness decision. No data processing, ML, tuning,
or live trading.

Governance classification:
  The signal was discovered after exploring multiple diagnostic passes
  (PR23–PR42). Without pre-registration or a multiple-testing adjustment,
  the best achievable status is 'exploratory_paper_candidate'. It can never
  be classified 'ready_for_live' until prospective validation, independent
  pre-registration, and a multiple-testing adjustment are performed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ReadinessConfig:
    live_trading_enabled: bool = False
    edge_claim: bool = False


# Number of distinct diagnostic modules examined before this candidate was
# selected (late-trend, candidate signal, fragility, paper eval, sensitivity,
# boundary purge). Used in the snooping-risk caveat.
DIAGNOSTICS_EXPLORED_COUNT: int = 6

# Snooping-risk metadata frozen at the time the signal was frozen for paper
# observation. These fields describe the discovery process and cannot be
# overridden by any post-hoc argument.
SNOOPING_METADATA: dict[str, Any] = {
    "diagnostics_explored_count": DIAGNOSTICS_EXPLORED_COUNT,
    "candidate_discovered_after_diagnostics": True,
    "pre_registered": False,
    "multiple_testing_adjustment_applied": False,
    "prospective_validation_required": True,
    "snooping_risk_note": (
        "Signal was selected after exploring multiple diagnostic modules."
        " Without pre-registration or a multiple-testing adjustment,"
        " in-sample positive results may be inflated."
        " Prospective paper observation is required before any edge claim."
    ),
}

# Permanent record of cross-period falsification.
#
# Recorded after the executed cross-period run on 2021_partial -> 2022 produced
# cross_period_validated=False with a sub-50% test win rate and negative test
# mean return. This is a frozen fact about the signal, not a per-run flag — it
# overrides any subsequent readiness criteria and blocks paper observation,
# live integration, and edge claims regardless of inputs. Repairing or tuning
# the signal in response to this falsification is explicitly prohibited.
FALSIFICATION_RECORD: dict[str, Any] = {
    "candidate_falsified": True,
    "falsification_period": "train=2021_partial; test=2022",
    "filtered_test_win_rate": 0.4117647058823529,
    "filtered_test_mean_return": -0.005174632785788908,
    "filtered_test_sample_size": 51,
    "baseline_period": "train=2023+2024; test=2025",
    "baseline_filtered_test_win_rate": 0.8088235294117647,
    "baseline_filtered_test_mean_return": 0.010125475505699285,
    "baseline_filtered_test_sample_size": 68,
    "reason": (
        "failed bear-regime cross-period validation: candidate flipped from"
        " 80.9% test win rate / +1.01% mean return on regime-favorable 2025"
        " to 41.2% test win rate / -0.52% mean return on 2022 bear regime"
    ),
    "falsification_run_artifact": (
        "reports/ml_diagnostics/spy_2021-2022_ml_regime_validation_cross_period.json"
    ),
    "cross_period_aggregate_artifact": (
        "reports/ml_diagnostics/ml_cross_period_validation.json"
    ),
    "tune_or_repair_prohibited": True,
    "tune_or_repair_note": (
        "Do NOT tune thresholds, change filters, retrain, or otherwise modify"
        " this signal in response to the 2022 result. That would be the"
        " multiple-testing / overfitting failure mode the snooping metadata"
        " was designed to gate. The honest move is to record the falsification"
        " and stop."
    ),
}

FROZEN_SIGNAL_DEFINITION: dict[str, Any] = {
    "signal_name": "high_vol_trend_early_candidate",
    "conditions": {
        "condition_1": {
            "feature": "realized_vol_60d",
            "operator": ">=",
            "threshold_label": "vol_split_value",
            "threshold_source": "median of realized_vol_60d across train period rows",
        },
        "condition_2": {
            "feature": "price_momentum_20d",
            "operator": ">",
            "threshold_value": 0,
            "threshold_source": "fixed zero; no derivation required",
        },
        "condition_3": {
            "feature": "distance_from_20d_mean",
            "operator": "<",
            "threshold_label": "maturity_threshold",
            "threshold_source": (
                "quantile(0.70) of distance_from_20d_mean"
                " in the high_vol_trend_positive train bucket"
            ),
        },
    },
    "logical_operator": "AND (all three conditions must hold)",
    "threshold_derivation": "train period only; test data is never used to derive thresholds",
    "freeze_note": (
        "Signal definition is frozen for paper observation."
        " Any change to conditions, features, or threshold derivation method"
        " requires a new full validation run before further paper observation."
    ),
    "live_trading_enabled": False,
    "governance_promotion_performed": False,
}


def _safe_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    return bool(value)


def _extract_criteria(
    *,
    late_trend_removal_validation: dict[str, Any],
    candidate_signal_diagnostics: dict[str, Any],
    overextension_fragility_diagnostics: dict[str, Any],
    paper_eval_diagnostics: dict[str, Any],
    sensitivity_diagnostics: dict[str, Any],
    boundary_purge_report: dict[str, Any] | None = None,
) -> dict[str, bool]:
    filtered_validated = _safe_bool(
        (late_trend_removal_validation.get("filtered_validation") or {}).get("validated"),
        default=False,
    )
    sample_size_safe = _safe_bool(
        (candidate_signal_diagnostics.get("test") or {}).get("sample_size_safe"),
        default=False,
    )
    overfiltering_risk = _safe_bool(
        (overextension_fragility_diagnostics.get("flags") or {}).get("overfiltering_risk"),
        default=True,
    )
    fragility_warning = _safe_bool(
        (overextension_fragility_diagnostics.get("flags") or {}).get("fragility_warning"),
        default=True,
    )
    stability_flag = _safe_bool(
        (
            (paper_eval_diagnostics.get("test") or {})
            .get("stability", {})
            .get("flags", {})
            .get("stability_flag")
        ),
        default=False,
    )
    concentration_warning = _safe_bool(
        (
            (paper_eval_diagnostics.get("test") or {})
            .get("stability", {})
            .get("flags", {})
            .get("concentration_warning")
        ),
        default=True,
    )
    threshold_robust = _safe_bool(
        sensitivity_diagnostics.get("threshold_robust"),
        default=False,
    )

    bpr = boundary_purge_report or {}
    boundary_label_overlap_detected = _safe_bool(
        bpr.get("boundary_label_overlap_detected"), default=False
    )
    boundary_purge_applied = _safe_bool(
        bpr.get("boundary_purge_applied"), default=False
    )
    # clean if: no overlap, OR overlap was detected AND purge was applied
    boundary_clean = not (boundary_label_overlap_detected and not boundary_purge_applied)

    return {
        "filtered_validated": filtered_validated,
        "sample_size_safe": sample_size_safe,
        "overfiltering_risk": overfiltering_risk,
        "fragility_warning": fragility_warning,
        "stability_flag": stability_flag,
        "concentration_warning": concentration_warning,
        "threshold_robust": threshold_robust,
        "boundary_clean": boundary_clean,
        "live_trading_enabled": False,
        "edge_claim": False,
    }


def _candidate_status(candidate_ready: bool) -> str:
    """Classify the candidate into one of four governance states.

    Possible values:
      falsified_cross_period — FALSIFICATION_RECORD.candidate_falsified is True;
        the signal failed cross-period generalization. Always returned when
        falsified, regardless of any individual run's criteria.
      exploratory_paper_candidate — all diagnostic criteria pass but the signal
        was discovered after exploratory diagnostics; paper observation only.
      blocked — one or more diagnostic criteria fail; no observation permitted.
      ready_for_live — never returned by this function; requires pre-registration,
        multiple-testing adjustment, and prospective validation (none done).
    """
    if FALSIFICATION_RECORD["candidate_falsified"]:
        return "falsified_cross_period"
    if not candidate_ready:
        return "blocked"
    return "exploratory_paper_candidate"


def _governance_flags(candidate_ready: bool) -> dict[str, bool]:
    """Derive integration permission flags from readiness, snooping metadata,
    and falsification record.

    live_integration_allowed is always False because SNOOPING_METADATA shows
    pre_registered=False, multiple_testing_adjustment_applied=False, and
    prospective_validation_required=True. It is also independently blocked by
    FALSIFICATION_RECORD.candidate_falsified=True.

    prospective_paper_observation_allowed is False whenever the signal is
    falsified, regardless of candidate_ready, because cross-period
    falsification overrides any single-period readiness.
    """
    falsified = bool(FALSIFICATION_RECORD["candidate_falsified"])
    live_integration_allowed = bool(
        SNOOPING_METADATA["pre_registered"]
        and SNOOPING_METADATA["multiple_testing_adjustment_applied"]
        and not SNOOPING_METADATA["prospective_validation_required"]
        and not falsified
    )
    return {
        "edge_claim_allowed": False,
        "live_integration_allowed": live_integration_allowed,
        "prospective_paper_observation_allowed": bool(candidate_ready and not falsified),
    }


def build_readiness_checklist(
    *,
    late_trend_removal_validation: dict[str, Any],
    candidate_signal_diagnostics: dict[str, Any],
    overextension_fragility_diagnostics: dict[str, Any],
    paper_eval_diagnostics: dict[str, Any],
    sensitivity_diagnostics: dict[str, Any],
    boundary_purge_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the candidate signal paper-observation readiness checklist.

    Inputs are pre-computed diagnostic dicts from build_regime_validation_report.
    No data processing is performed here.
    """
    config = ReadinessConfig()
    criteria = _extract_criteria(
        late_trend_removal_validation=late_trend_removal_validation,
        candidate_signal_diagnostics=candidate_signal_diagnostics,
        overextension_fragility_diagnostics=overextension_fragility_diagnostics,
        paper_eval_diagnostics=paper_eval_diagnostics,
        sensitivity_diagnostics=sensitivity_diagnostics,
        boundary_purge_report=boundary_purge_report,
    )

    criteria_pass = bool(
        criteria["filtered_validated"]
        and criteria["sample_size_safe"]
        and not criteria["overfiltering_risk"]
        and not criteria["fragility_warning"]
        and criteria["stability_flag"]
        and not criteria["concentration_warning"]
        and criteria["threshold_robust"]
        and criteria["boundary_clean"]
        and not criteria["live_trading_enabled"]
        and not criteria["edge_claim"]
    )

    falsified = bool(FALSIFICATION_RECORD["candidate_falsified"])
    candidate_ready = bool(criteria_pass and not falsified)

    gov_flags = _governance_flags(candidate_ready)

    return {
        "status": "ok",
        "purpose": "candidate signal readiness checklist for paper observation; no live trading",
        "data_level": "date",
        "candidate_ready_for_paper_observation": candidate_ready,
        "candidate_status": _candidate_status(candidate_ready),
        "governance_flags": gov_flags,
        "snooping_metadata": dict(SNOOPING_METADATA),
        "falsification_record": dict(FALSIFICATION_RECORD),
        "criteria_pass_pre_falsification": criteria_pass,
        "criteria": criteria,
        "criteria_descriptions": {
            "filtered_validated": (
                "late_trend_removal_validation: filtered signal beats all_rows"
                " on win rate and mean return in both train and test"
            ),
            "sample_size_safe": (
                "candidate_signal_diagnostics: all test-period quarters have >= 10 signal entries"
            ),
            "overfiltering_risk": (
                "overextension_fragility_diagnostics: false means late-trend filter"
                " removes <= 50% of baseline rows"
            ),
            "fragility_warning": (
                "overextension_fragility_diagnostics: false means positive return"
                " is not concentrated in a single quarter"
            ),
            "stability_flag": (
                "paper_eval_diagnostics: no mature-negative month, no concentration,"
                " and at least one mature month exists"
            ),
            "concentration_warning": (
                "paper_eval_diagnostics: false means no month holds >50% of entries"
                " or >50% of total positive return"
            ),
            "threshold_robust": (
                "sensitivity_diagnostics: reference quantile (0.70) has mean_return > 0"
                " and >= 4 of 6 other quantiles also have mean_return > 0"
            ),
            "boundary_clean": (
                "boundary_purge_report: true if no overlap was detected,"
                " or if overlap was detected and the purge was applied"
            ),
            "live_trading_enabled": "always false; no live trading performed",
            "edge_claim": "always false; no edge claim is made",
        },
        "readiness_decision_logic": (
            "true only if: filtered_validated AND sample_size_safe"
            " AND NOT overfiltering_risk AND NOT fragility_warning"
            " AND stability_flag AND NOT concentration_warning"
            " AND threshold_robust AND boundary_clean"
            " AND NOT live_trading_enabled AND NOT edge_claim"
        ),
        "frozen_signal_definition": FROZEN_SIGNAL_DEFINITION,
        "sample_size_caveats": [
            "paper eval train period may have low-sample quarters (sample_size_warning)",
            "test period monthly breakdown may have low_sample months (entries < 5)",
            "sample_size_safe evaluates test-period quarters only (threshold: >= 10 entries per quarter)",
            "signal observation in low-sample months should be interpreted with caution",
        ],
        "flags": {
            "live_trading_enabled": config.live_trading_enabled,
            "edge_claim": config.edge_claim,
        },
        "definitions": {
            "training_performed": False,
            "threshold_optimization_performed": False,
            "filter_changes_performed": False,
            "live_trading_enabled": False,
            "governance_promotion_performed": False,
        },
        "disclaimer": (
            "candidate has been falsified for cross-period generalization on the"
            " 2021_partial -> 2022 bear-regime run (test win rate 41.2%, mean"
            " return -0.52%); paper observation is blocked, no statistical edge"
            " claim is made or permitted, no live integration is permitted;"
            " signal was discovered after multiple diagnostic passes and is not"
            " pre-registered; tuning or repairing this signal in response to"
            " 2022 is explicitly prohibited; original 2023+2024 -> 2025"
            " diagnostics remain available for audit history"
        ),
    }
