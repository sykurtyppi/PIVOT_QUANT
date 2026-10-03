#!/usr/bin/env python3
"""Shared leak-free forecast engine for the levels data product.

ONE source of truth for the hold-probability rule, imported by train_hold_model,
build_track_record and forecast_store so history and live are byte-identical.

Leak-freeness is enforced by a HORIZON TIME-EMBARGO: the forecast for a touch at
ts_i may only use outcomes that had actually RESOLVED before ts_i — i.e. prior
touches j with ts_j + horizon <= ts_i. (A reject@h label does not exist until h
minutes after the touch; using a not-yet-resolved prior outcome is look-ahead.)
Because ts is sorted and the horizon is constant, resolution times are monotonic,
so a FIFO deque matures pending outcomes in O(n).
"""
from __future__ import annotations

from collections import deque
import sys
from pathlib import Path

import numpy as np

SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
from label_eligibility import normalize_bar_interval  # noqa: E402

WINDOW = 400        # trailing window (matured events) for the adaptive rate
MIN_BUCKET = 30     # min matured in-bucket events before trusting a bucket rate
HORIZONS = [15, 30, 60]
BUCKETS = ["0", "1", "2+"]
MIN_PUBLISHABLE_N = 200  # a bucket's calibration/CI is only "publishable" above this


def _require_coverage_status(con) -> None:
    columns = {row[1] for row in con.execute("PRAGMA table_info(event_labels)")}
    if "coverage_status" not in columns:
        raise RuntimeError("event_labels.coverage_status is required")


def bucket(conf):
    return np.where(conf >= 2, "2+", np.where(conf >= 1, "1", "0"))


def wilson(k, n, z=1.96):
    if n == 0:
        return [None, None]
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * np.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return [round(float(c - h), 4), round(float(c + h), 4)]


def _matured_window_walk(ts, bkts, outcomes, horizon_min, window, min_bucket, bucket_aware):
    """Core leak-free walk. Returns per-event prediction.

    bucket_aware=True  -> trailing in-bucket rate (the model).
    bucket_aware=False -> trailing GLOBAL rate (the drift-adapted base-rate
                          baseline, using exactly the info the model is allowed).
    nan/None outcomes are forecast from the prior matured window but never
    enter history (unresolved touches don't pollute the rate).
    """
    n = len(bkts)
    pred = np.full(n, np.nan)
    histB = {b: [] for b in BUCKETS}
    histG: list[int] = []
    pend: deque = deque()  # (resolve_ts, bucket, outcome) FIFO, monotonic resolve_ts
    hms = horizon_min * 60_000
    for i in range(n):
        ti = int(ts[i])
        while pend and pend[0][0] <= ti:  # mature everything resolved before ti
            _, bb, oo = pend.popleft()
            histB[bb].append(oo)
            histG.append(oo)
        if bucket_aware:
            bw = histB[bkts[i]][-window:]
        else:
            bw = []
        gw = histG[-window:]
        if bucket_aware and len(bw) >= min_bucket:
            pred[i] = float(np.mean(bw))
        elif len(gw) >= min_bucket:
            pred[i] = float(np.mean(gw))
        o = outcomes[i]
        if o is not None and o == o:  # skip nan (unresolved)
            pend.append((ti + hms, str(bkts[i]), int(o)))
    return pred


def trailing_forecasts(ts, bkts, outcomes, horizon_min, window=WINDOW, min_bucket=MIN_BUCKET):
    """Leak-free P(hold) per event = embargoed trailing in-bucket hold rate."""
    return _matured_window_walk(ts, bkts, outcomes, horizon_min, window, min_bucket, True)


def trailing_base_rate(ts, outcomes, horizon_min, window=WINDOW, min_bucket=MIN_BUCKET):
    """Drift-adapted baseline = embargoed trailing GLOBAL hold rate per event.

    This is the fair Brier-skill reference: it tracks the same base-rate drift
    the model sees, so positive skill reflects the confluence tilt, not a stale
    constant graded on a population that drifted away from it.
    """
    dummy = np.array(["0"] * len(outcomes))
    return _matured_window_walk(ts, dummy, outcomes, horizon_min, window, min_bucket, False)


def reliability_curve(p, y, n_bins=10):
    """Fixed-width-bin reliability + ECE.

    Fixed [0,1] bins are robust for BOTH near-continuous trailing-rate forecasts
    (where quantile edges are fine but exact-value grouping degenerates into
    singletons) AND genuinely discrete forecasts (where quantile edges collapse).
    ECE = sum_b (n_b/N) * |mean(actual_b) - mean(pred_b)| over non-empty bins.
    """
    p = np.asarray(p, float)
    y = np.asarray(y, float)
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    idx = np.clip(np.digitize(p, edges[1:-1]), 0, n_bins - 1)
    rows, ece, N = [], 0.0, len(p)
    for b in range(n_bins):
        m = idx == b
        nn = int(m.sum())
        if nn == 0:
            continue
        pm, ym = float(p[m].mean()), float(y[m].mean())
        rows.append({"bin": [round(float(edges[b]), 2), round(float(edges[b + 1]), 2)],
                     "n": nn, "pred_p": round(pm, 4), "actual": round(ym, 4),
                     "gap": round(ym - pm, 4)})
        ece += (nn / N) * abs(ym - pm)
    return rows, round(float(ece), 4)


def et_dates_from_ms(ts_ms):
    """ET calendar dates for ms-epoch timestamps (the real trading day)."""
    import pandas as pd
    return pd.to_datetime(ts_ms, unit="ms", utc=True).dt.tz_convert("America/New_York").dt.date


def label_coverage(read_con, symbol, dq_min=0.9, horizons=HORIZONS):
    """Return label completeness against every touch that can now be matured.

    Eligibility mirrors ``scripts/build_labels.py``: the touch has a valid bar
    interval, at least one forward bar inside the horizon, and market data at or
    beyond the horizon endpoint. Any eligible touch without a label makes that
    horizon incomplete. This compares labels to the data actually available in
    the database rather than to wall-clock time, so weekends and feed downtime do
    not create false staleness alarms.
    """
    _require_coverage_status(read_con)
    raw_intervals = read_con.execute(
        """SELECT DISTINCT bar_interval_sec
           FROM touch_events
           WHERE symbol=? AND data_quality>=?
             AND confluence_count IS NOT NULL""",
        (symbol, dq_min),
    ).fetchall()
    valid_intervals = [
        (raw, normalized)
        for (raw,) in raw_intervals
        if (normalized := normalize_bar_interval(raw)) is not None
    ]

    coverage = {}
    for h in horizons:
        horizon_ms = int(h) * 60_000
        if not valid_intervals:
            coverage[int(h)] = {
                "complete": False,
                "status": "no_mature_events",
                "eligible_count": 0,
                "labeled_count": 0,
                "missing_count": 0,
                "latest_eligible_ts": None,
                "latest_labeled_ts": None,
            }
            continue
        interval_values = ", ".join("(?, ?)" for _ in valid_intervals)
        interval_params = [value for pair in valid_intervals for value in pair]
        row = read_con.execute(
            f"""WITH valid_intervals(raw_interval, normalized_interval) AS (
                   VALUES {interval_values}
               ), eligible AS (
                   SELECT te.event_id, te.ts_event
                   FROM touch_events te
                   JOIN valid_intervals vi
                     ON te.bar_interval_sec=vi.raw_interval
                   WHERE te.symbol=? AND te.data_quality>=?
                     AND te.confluence_count IS NOT NULL
                     AND EXISTS (
                         SELECT 1 FROM bar_data b
                         WHERE b.symbol=te.symbol
                           AND b.bar_interval_sec=vi.normalized_interval
                           AND b.ts>te.ts_event AND b.ts<=te.ts_event+?
                     )
                     AND (SELECT MAX(b.ts) FROM bar_data b
                          WHERE b.symbol=te.symbol
                            AND b.bar_interval_sec=vi.normalized_interval)
                         >= te.ts_event+?
               )
               SELECT COUNT(*) AS eligible_count,
                      COUNT(el.event_id) AS labeled_count,
                      MAX(eligible.ts_event) AS latest_eligible_ts,
                      MAX(CASE WHEN el.event_id IS NOT NULL
                               THEN eligible.ts_event END) AS latest_labeled_ts
               FROM eligible
               LEFT JOIN event_labels el
                 ON el.event_id=eligible.event_id AND el.horizon_min=?
                AND el.coverage_status='qualified'""",
            (*interval_params, symbol, dq_min, horizon_ms, horizon_ms, int(h)),
        ).fetchone()
        eligible = int(row[0] or 0)
        labeled = int(row[1] or 0)
        missing = eligible - labeled
        complete = bool(eligible > 0 and missing == 0)
        coverage[int(h)] = {
            "complete": complete,
            "status": ("current" if complete else
                       "no_mature_events" if eligible == 0 else "missing_labels"),
            "eligible_count": eligible,
            "labeled_count": labeled,
            "missing_count": missing,
            "latest_eligible_ts": int(row[2]) if row[2] is not None else None,
            "latest_labeled_ts": int(row[3]) if row[3] is not None else None,
        }
    return coverage


def current_rates(read_con, symbol, dq_min=0.9, window=WINDOW):
    """Live hold rates, failing closed unless every horizon is complete.

    Uses only matured outcomes and returns coverage metadata under ``_coverage``.
    If any configured horizon has missing labels (or no mature events), no rate is
    returned for any horizon: a partial snapshot must never look like a current,
    comparable 15m/30m/60m probability set.
    """
    import pandas as pd
    _require_coverage_status(read_con)
    coverage = label_coverage(read_con, symbol, dq_min)
    publishable = all(coverage[h]["complete"] for h in HORIZONS)
    out = {"_publishable": publishable, "_coverage": coverage}
    if not publishable:
        out.update({h: {} for h in HORIZONS})
        return out

    for h in HORIZONS:
        df = pd.read_sql_query(
            """SELECT te.confluence_count, el.reject
               FROM touch_events te JOIN event_labels el ON te.event_id=el.event_id
               WHERE te.symbol=? AND el.horizon_min=? AND te.data_quality>=?
                     AND el.coverage_status='qualified'
                     AND el.reject IS NOT NULL AND te.confluence_count IS NOT NULL
               ORDER BY te.ts_event, te.event_id""",
            read_con, params=(symbol, h, dq_min))
        if df.empty:
            out[h] = {}
            continue
        df["bkt"] = bucket(df.confluence_count.to_numpy())
        per = {}
        for b in BUCKETS:
            s = df[df.bkt == b].tail(window)
            k, n = int(s.reject.sum()), len(s)
            per[b] = {"rate": round(k / n, 4) if n >= MIN_BUCKET else None, "n": n,
                      "ci95": wilson(k, n) if n >= MIN_BUCKET else [None, None],
                      "publishable": bool(n >= MIN_PUBLISHABLE_N)}
        out[h] = per
    return out
