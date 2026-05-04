"""Train/test boundary label contamination purge.

For a train entry_date E and horizon H, the label covers E through
E + H business days. If that label_date falls on or after the test
period start, the row's outcome uses test-period prices and must be
removed from train before any threshold derivation or performance
measurement.

Embargo: max evaluated horizon = 5 business days (forward_return_5d).

No ML, no threshold optimization, no live trading, no T9 mutation.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

MAX_EVALUATED_HORIZON_BDAYS: int = 5


def _test_start_date(test_frame: pd.DataFrame) -> str | None:
    """Earliest entry_date in the test frame as an ISO date string."""
    if test_frame.empty or "entry_date" not in test_frame.columns:
        return None
    dates = pd.to_datetime(test_frame["entry_date"], errors="coerce").dropna()
    return dates.min().date().isoformat() if not dates.empty else None


def _label_dates_series(
    train_frame: pd.DataFrame,
    *,
    horizon_bdays: int,
) -> pd.Series:
    """Compute label_date for each train row.

    Uses label_date_{H}d column when present (updated exports).
    Falls back to entry_date + H business days for existing exports
    that predate this change.
    """
    col = f"label_date_{horizon_bdays}d"
    if col in train_frame.columns:
        return pd.to_datetime(train_frame[col], errors="coerce")
    dates = pd.to_datetime(
        train_frame.get("entry_date", pd.Series(dtype="object")), errors="coerce"
    )
    return dates + pd.offsets.BDay(horizon_bdays)


def apply_boundary_purge(
    train_frame: pd.DataFrame,
    test_frame: pd.DataFrame,
    *,
    horizon_bdays: int = MAX_EVALUATED_HORIZON_BDAYS,
) -> dict[str, Any]:
    """Remove train rows whose forward label overlaps the test period.

    Returns a dict with two keys:
      - purged_frame  : pd.DataFrame  — train with contaminated rows removed
      - report        : dict          — purge statistics and boundary flags
    """
    rows_before = int(len(train_frame))
    test_start = _test_start_date(test_frame)

    if test_start is None or train_frame.empty or "entry_date" not in train_frame.columns:
        return {
            "purged_frame": train_frame.copy(),
            "report": {
                "status": "skipped",
                "reason": "test_start could not be determined or train frame is empty",
                "train_rows_before_purge": rows_before,
                "train_rows_after_purge": rows_before,
                "rows_purged": 0,
                "max_label_date_retained": None,
                "test_start": test_start,
                "embargo_horizon_bdays": horizon_bdays,
                "boundary_label_overlap_detected": False,
                "boundary_purge_applied": False,
                "definitions": _definitions(horizon_bdays),
                "disclaimer": "no edge claim; boundary purge is a data integrity fix only",
            },
        }

    test_start_ts = pd.Timestamp(test_start)
    label_dates = _label_dates_series(train_frame, horizon_bdays=horizon_bdays)
    overlap_mask = label_dates >= test_start_ts
    boundary_label_overlap_detected = bool(overlap_mask.any())

    if boundary_label_overlap_detected:
        keep_mask = ~overlap_mask
        purged_frame = train_frame.loc[keep_mask].copy().reset_index(drop=True)
        boundary_purge_applied = True
    else:
        purged_frame = train_frame.copy()
        boundary_purge_applied = False

    rows_after = int(len(purged_frame))
    rows_purged = rows_before - rows_after

    retained_label_dates = label_dates.loc[~overlap_mask].dropna()
    max_label_date_retained = (
        retained_label_dates.max().date().isoformat()
        if not retained_label_dates.empty
        else None
    )

    return {
        "purged_frame": purged_frame,
        "report": {
            "status": "ok",
            "train_rows_before_purge": rows_before,
            "train_rows_after_purge": rows_after,
            "rows_purged": rows_purged,
            "max_label_date_retained": max_label_date_retained,
            "test_start": test_start,
            "embargo_horizon_bdays": horizon_bdays,
            "boundary_label_overlap_detected": boundary_label_overlap_detected,
            "boundary_purge_applied": boundary_purge_applied,
            "definitions": _definitions(horizon_bdays),
            "disclaimer": "no edge claim; boundary purge is a data integrity fix only",
        },
    }


def _definitions(horizon_bdays: int) -> dict[str, str]:
    return {
        "label_date": (
            f"entry_date + {horizon_bdays} business days"
            f" (forward_return_{horizon_bdays}d horizon)"
        ),
        "boundary_label_overlap_detected": (
            "true if any train row has label_date >= test_start before purge"
        ),
        "boundary_purge_applied": (
            "true if contaminated rows were removed from the train frame"
        ),
        "embargo_horizon_bdays": (
            f"max evaluated horizon = {horizon_bdays} business days"
        ),
    }
