#!/usr/bin/env python3
"""Phase-0 (3/4): the live forecast store — pre-commit + score.

The going-forward loop behind the public track record. For every SPY touch the
collector logs, it pre-commits an immutable, leak-free P(hold) forecast (per
horizon), then scores committed forecasts against realized outcomes.

CORRECTNESS (post-audit):
  * Forecasts are horizon-embargoed (hold_engine.trailing_forecasts): the value
    written for a touch depends ONLY on outcomes that resolved before that touch,
    so a later cron run can never fold in since-resolved outcomes — the committed
    value equals what was knowable at touch time, regardless of when emit runs.
  * MATURITY GATE: a forecast is committed only once the touch's own horizon has
    elapsed (ts_event + horizon <= now). By then every pre-touch outcome the
    embargoed window needs is already labeled, so the first (and only, via
    INSERT OR IGNORE) committed value is the stable, complete one — never a thin
    placeholder that gets frozen.

  emit():  compute embargoed forecasts; commit matured, not-yet-committed ones.
  score(): join committed forecasts to event_labels; report calibration.

Reads the live DB strictly READ-ONLY (mode=ro); writes only the product DB.
v450's prediction_log / serving state are untouched.

    python scripts/levels_product/forecast_store.py emit
    python scripts/levels_product/forecast_store.py score
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "levels_product"))
from hold_engine import (  # noqa: E402
    BUCKETS, HORIZONS, MIN_PUBLISHABLE_N, bucket, et_dates_from_ms,
    reliability_curve, trailing_forecasts, wilson,
)

READ_DB = REPO / "data" / "pivot_events.sqlite"      # live trading DB — READ ONLY here
PRODUCT_DB = REPO / "data" / "levels_product.sqlite"  # product's own forward log
MODEL_VERSION = "conf_bucket_embargoed_v2"


def _read_con():
    return sqlite3.connect(f"file:{READ_DB}?mode=ro", uri=True)


def _product_con():
    return sqlite3.connect(PRODUCT_DB)


def ensure_table(con):
    con.execute(
        """CREATE TABLE IF NOT EXISTS level_hold_forecasts (
            event_id TEXT NOT NULL, symbol TEXT NOT NULL, horizon_min INTEGER NOT NULL,
            ts_event INTEGER NOT NULL, confluence_count INTEGER, conf_bucket TEXT,
            p_hold REAL NOT NULL, model_version TEXT NOT NULL, forecast_ts INTEGER NOT NULL,
            PRIMARY KEY (event_id, horizon_min)
        )""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_lhf_ts ON level_hold_forecasts(ts_event);")
    con.commit()


def emit(symbol, dq_min):
    rcon, pcon = _read_con(), _product_con()
    ensure_table(pcon)
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    committed = 0
    for h in HORIZONS:
        df = pd.read_sql_query(
            """SELECT te.event_id, te.ts_event, te.confluence_count, el.reject
               FROM touch_events te
               LEFT JOIN event_labels el ON te.event_id=el.event_id AND el.horizon_min=?
               WHERE te.symbol=? AND te.data_quality>=? AND te.confluence_count IS NOT NULL
               ORDER BY te.ts_event, te.event_id""",
            rcon, params=(h, symbol, dq_min))
        if df.empty:
            continue
        ts = df.ts_event.to_numpy()
        bk = bucket(df.confluence_count.to_numpy())
        outcomes = [None if pd.isna(v) else int(v) for v in df.reject]
        preds = trailing_forecasts(ts, bk, outcomes, h)
        mature_before = now - h * 60_000  # touch's own horizon must have elapsed
        for i, row in enumerate(df.itertuples()):
            p = preds[i]
            if not np.isfinite(p) or int(row.ts_event) > mature_before:
                continue
            cur = pcon.execute(
                """INSERT OR IGNORE INTO level_hold_forecasts
                   (event_id,symbol,horizon_min,ts_event,confluence_count,conf_bucket,
                    p_hold,model_version,forecast_ts)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (row.event_id, symbol, h, int(row.ts_event), int(row.confluence_count),
                 str(bk[i]), round(float(p), 4), MODEL_VERSION, now))
            committed += cur.rowcount
    pcon.commit()
    rcon.close(); pcon.close()
    return committed


def score(symbol, recent_days):
    rcon, pcon = _read_con(), _product_con()
    ensure_table(pcon)
    labels = pd.read_sql_query(
        "SELECT event_id, horizon_min, reject FROM event_labels WHERE reject IS NOT NULL", rcon)
    out = {"product": "levels_forecast_log_score", "symbol": symbol,
           "as_of_utc": datetime.now(timezone.utc).isoformat(),
           "model_version": MODEL_VERSION, "horizons": {}}
    for h in HORIZONS:
        fc = pd.read_sql_query(
            "SELECT event_id, ts_event, conf_bucket, p_hold FROM level_hold_forecasts WHERE symbol=? AND horizon_min=?",
            pcon, params=(symbol, h))
        lab = labels[labels.horizon_min == h][["event_id", "reject"]]
        df = fc.merge(lab, on="event_id", how="inner")
        if df.empty:
            out["horizons"][f"h{h}"] = {"n_scored": 0, "message": "no matured committed forecasts yet"}
            continue
        y, p = df.reject.to_numpy(int), df.p_hold.to_numpy(float)
        _, ece = reliability_curve(p, y)
        by = {}
        for bk in BUCKETS:
            s = df[df.conf_bucket == bk]
            k, nn = int(s.reject.sum()), len(s)
            by[bk] = {"n": nn, "pred_hold": round(float(s.p_hold.mean()), 4) if nn else None,
                      "actual_hold": round(k / nn, 4) if nn else None, "ci95": wilson(k, nn),
                      "publishable": bool(nn >= MIN_PUBLISHABLE_N)}
        df["d"] = et_dates_from_ms(df.ts_event)
        days = sorted(df.d.unique())[-recent_days:]
        rec = df[df.d.isin(days) & (df.conf_bucket != "0")]
        rk, rn = int(rec.reject.sum()), len(rec)
        out["horizons"][f"h{h}"] = {
            "n_scored": len(df), "ece": ece, "by_confluence_bucket": by,
            "rolling_scoreboard_alerted": {"window_trading_days": recent_days, "n": rn,
                                           "actual_hold_rate": round(rk / rn, 4) if rn else None,
                                           "ci95": wilson(rk, rn)}}
    rcon.close(); pcon.close()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["emit", "score"])
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--dq-min", type=float, default=0.9)
    ap.add_argument("--recent-days", type=int, default=30)
    args = ap.parse_args()
    if args.cmd == "emit":
        n = emit(args.symbol, args.dq_min)
        pcon = _product_con()
        total = pcon.execute("SELECT COUNT(*) FROM level_hold_forecasts WHERE symbol=?",
                             (args.symbol,)).fetchone()[0]
        pcon.close()
        print(f"committed {n} new matured forecasts (total {total} for {args.symbol})")
    else:
        import json
        print(json.dumps(score(args.symbol, args.recent_days), indent=2))


if __name__ == "__main__":
    main()
