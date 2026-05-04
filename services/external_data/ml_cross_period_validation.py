"""Cross-period aggregation of regime validation reports.

Combines two or more independent train/test validation runs and reports
``cross_period_validated`` only when every period independently produced a
validated, paper-observation-ready candidate. No model training, threshold
optimization, live trading, or edge claim is performed here.

This module is read-only over the input reports. It does not re-run validation,
re-derive thresholds, or modify any state.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _period_label(report: dict[str, Any]) -> str:
    train_years = list(map(str, report.get("train_years") or []))
    coverage = report.get("data_coverage") or {}
    if coverage.get("train_is_partial") and len(train_years) == 1:
        train_label = f"{train_years[0]}_partial"
    else:
        train_label = "+".join(train_years)
    test = str(report.get("test_year") or "")
    return f"train={train_label}; test={test}"


def _period_metrics(report: dict[str, Any]) -> dict[str, Any]:
    """Pull the headline metrics from a regime-validation report.

    Reports can be partial (status='fail' before diagnostics ran). Missing
    fields default to None so callers can serialize and inspect uniformly.
    """
    crc = report.get("candidate_readiness_checklist") or {}
    csd = report.get("candidate_signal_diagnostics") or {}
    ltr = report.get("late_trend_removal_validation") or {}
    bpr = report.get("boundary_purge_report") or {}

    def _stats(period_block: dict[str, Any]) -> dict[str, Any]:
        return {
            "sample_size": period_block.get("sample_size"),
            "win_rate_5d": period_block.get("win_rate_5d"),
            "mean_return_5d": period_block.get("mean_return_5d"),
        }

    baseline = ltr.get("baseline_validation") or {}
    filtered = ltr.get("filtered_validation") or {}
    impr = ltr.get("improvement_summary") or {}
    coverage = report.get("data_coverage") or {}

    return {
        "label": _period_label(report),
        "data_coverage": {
            "period_label": coverage.get("period_label") or _period_label(report),
            "train_coverage_start": coverage.get("train_coverage_start"),
            "train_coverage_end": coverage.get("train_coverage_end"),
            "train_is_partial": bool(coverage.get("train_is_partial", False)),
            "data_coverage_note": coverage.get("data_coverage_note"),
        },
        "status": report.get("status"),
        "validated": bool(report.get("validated")),
        "candidate_status": crc.get("candidate_status"),
        "candidate_ready_for_paper_observation": bool(
            crc.get("candidate_ready_for_paper_observation")
        ),
        "thresholds": (csd.get("thresholds") or {}),
        "baseline_high_vol_trend_positive": {
            "train": _stats(baseline.get("train") or {}),
            "test": _stats(baseline.get("test") or {}),
        },
        "filtered_early_trend": {
            "train": _stats(filtered.get("train") or {}),
            "test": _stats(filtered.get("test") or {}),
        },
        "rows_removed": {
            "train": impr.get("rows_removed_train"),
            "test": impr.get("rows_removed_test"),
        },
        "boundary_purge": {
            "boundary_label_overlap_detected": bpr.get("boundary_label_overlap_detected"),
            "boundary_purge_applied": bpr.get("boundary_purge_applied"),
            "rows_purged": bpr.get("rows_purged"),
        },
        "criteria": (crc.get("criteria") or {}),
    }


def aggregate_cross_period_validation(
    reports: list[dict[str, Any]],
) -> dict[str, Any]:
    """Aggregate two-or-more validation reports into a cross-period decision.

    cross_period_validated is True only if every input period independently
    has candidate_ready_for_paper_observation=True. With fewer than two
    periods this returns False — a single period is not "cross-period".
    """
    if not reports:
        return {
            "name": "ml_cross_period_validation",
            "status": "missing",
            "cross_period_validated": False,
            "periods": [],
            "reason": "no period reports provided",
            "definitions": _definitions(),
            "disclaimer": _disclaimer(),
        }

    period_metrics = [_period_metrics(r) for r in reports]
    n = len(period_metrics)
    all_paper_ready = all(p["candidate_ready_for_paper_observation"] for p in period_metrics)
    cross_period_validated = bool(n >= 2 and all_paper_ready)

    not_ready_labels = [
        p["label"] for p in period_metrics
        if not p["candidate_ready_for_paper_observation"]
    ]

    return {
        "name": "ml_cross_period_validation",
        "status": "ok",
        "cross_period_validated": cross_period_validated,
        "period_count": n,
        "periods": period_metrics,
        "agreement_summary": {
            "all_periods_paper_ready": all_paper_ready,
            "periods_not_ready": not_ready_labels,
            "minimum_periods_for_cross_period": 2,
        },
        "decision_logic": (
            "cross_period_validated is true only if at least two independent"
            " periods report candidate_ready_for_paper_observation=true; a"
            " single period does not constitute cross-period validation"
        ),
        "definitions": _definitions(),
        "disclaimer": _disclaimer(),
    }


def write_cross_period_report(
    report: dict[str, Any],
    *,
    reports_dir: Path | None = None,
    stem: str = "ml_cross_period_validation",
) -> Path:
    base = reports_dir or Path.cwd() / "reports" / "ml_diagnostics"
    base = base.expanduser().resolve()
    cwd = Path.cwd().resolve()
    if cwd not in [base, *base.parents]:
        raise ValueError(f"reports_dir must be inside the repo working directory: {base}")
    base.mkdir(parents=True, exist_ok=True)
    path = base / f"{stem}.json"
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    return path


def _definitions() -> dict[str, Any]:
    return {
        "training_performed": False,
        "threshold_optimization_performed": False,
        "filter_changes_performed": False,
        "live_trading_enabled": False,
        "governance_promotion_performed": False,
        "edge_claim": False,
    }


def _disclaimer() -> str:
    return (
        "no edge claim; cross-period aggregation is read-only over per-period"
        " reports; cross_period_validated does not authorize live integration"
    )
