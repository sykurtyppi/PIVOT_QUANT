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

import numpy as np

WINDOW = 400        # trailing window (matured events) for the adaptive rate
MIN_BUCKET = 30     # min matured in-bucket events before trusting a bucket rate
HORIZONS = [15, 30, 60]
BUCKETS = ["0", "1", "2+"]
MIN_PUBLISHABLE_N = 200  # a bucket's calibration/CI is only "publishable" above this


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


def current_rates(read_con, symbol, dq_min=0.9, window=WINDOW):
    """Live, as-of-now publishable hold rates per (horizon, bucket).

    Uses ONLY matured (labeled) outcomes — the most recent `window` per bucket —
    so it is leak-free by construction (an outcome exists in event_labels only
    after it resolved). This is the P(hold) the intraday alert and the morning
    post quote. Returns {horizon: {bucket: {"rate", "n", "ci95"}}}.
    """
    import pandas as pd
    out = {}
    for h in HORIZONS:
        df = pd.read_sql_query(
            """SELECT te.confluence_count, el.reject
               FROM touch_events te JOIN event_labels el ON te.event_id=el.event_id
               WHERE te.symbol=? AND el.horizon_min=? AND te.data_quality>=?
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
