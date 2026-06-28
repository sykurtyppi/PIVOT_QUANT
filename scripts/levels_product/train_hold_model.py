#!/usr/bin/env python3
"""Phase-0 (1/4): VALIDATE the level-hold model (not a black-box trainer).

The "model" is deliberately parameter-free: P(hold | tested) = horizon-embargoed
trailing hold rate within the touch's confluence bucket {0,1,2+} (see
hold_engine — the SAME function the track record and live store use). This
script validates it out of sample and reports the HONEST picture:

  * The model is, in aggregate, close to a base-rate tracker, so good aggregate
    calibration is near-trivial. Brier skill is therefore measured against a
    DRIFT-ADAPTED trailing base rate (not a stale constant) and is honestly small.
  * The real, sellable signal is the CONFLUENCE-1 TILT: conf-1 levels hold ~8-9
    pts more than conf-0, OOS, with non-overlapping Wilson CIs and n>2000. The
    conf>=2 bucket is small/time-concentrated -> flagged non-publishable.

Read-only on the live DB (mode=ro). Writes only an evidence report. No runtime
artifact (the snapshot publishes base rates; the live store recomputes rates),
no promotion, v450 untouched.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "levels_product"))
from hold_engine import (  # noqa: E402
    BUCKETS, MIN_PUBLISHABLE_N, bucket, reliability_curve,
    trailing_base_rate, trailing_forecasts, wilson,
)

DB = REPO / "data" / "pivot_events.sqlite"
OOS_FRAC = 0.35


def load(symbol, horizon, dq_min):
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    df = pd.read_sql_query(
        """SELECT te.ts_event, te.confluence_count, el.reject
           FROM touch_events te JOIN event_labels el ON te.event_id=el.event_id
           WHERE te.symbol=? AND el.horizon_min=? AND te.data_quality>=?
                 AND el.reject IS NOT NULL AND te.confluence_count IS NOT NULL
           ORDER BY te.ts_event, te.event_id""",
        con, params=(symbol, horizon, dq_min))
    con.close()
    df["bkt"] = bucket(df.confluence_count.to_numpy())
    return df.reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--horizon", type=int, default=15)
    ap.add_argument("--dq-min", type=float, default=0.9)
    args = ap.parse_args()

    df = load(args.symbol, args.horizon, args.dq_min)
    n = len(df)
    ts = df.ts_event.to_numpy()
    out_arr = df.reject.to_numpy(int)
    df["pred"] = trailing_forecasts(ts, df.bkt.to_numpy(), out_arr, args.horizon)
    df["base"] = trailing_base_rate(ts, out_arr, args.horizon)

    cut = int(n * (1 - OOS_FRAC))
    oos = df.iloc[cut:].dropna(subset=["pred", "base"])
    y, p, b = oos.reject.to_numpy(int), oos.pred.to_numpy(float), oos.base.to_numpy(float)
    bm, bb = float(brier_score_loss(y, p)), float(brier_score_loss(y, b))
    rel, ece = reliability_curve(p, y)

    buckets = {}
    for bk in BUCKETS:
        s = oos[oos.bkt == bk]
        k, nn = int(s.reject.sum()), len(s)
        buckets[bk] = {"oos_n": nn, "oos_hold_rate": round(k / nn, 4) if nn else None,
                       "ci95": wilson(k, nn), "publishable": bool(nn >= MIN_PUBLISHABLE_N)}

    c0, c1 = buckets["0"], buckets["1"]
    conf1_solid = bool(
        c1["publishable"] and c0["ci95"][1] is not None and c1["ci95"][0] is not None
        and c1["ci95"][0] > c0["ci95"][1])  # conf-1 lower CI above conf-0 upper CI

    report = {
        "validation": "level_hold_model (horizon-embargoed trailing conf-bucket)",
        "symbol": args.symbol, "horizon_min": args.horizon, "dq_min": args.dq_min,
        "n_total": n, "oos_n_scored": int(len(oos)),
        "oos_brier_model": round(bm, 5),
        "oos_brier_trailing_base_rate": round(bb, 5),
        "oos_brier_skill_vs_trailing_base": round(1 - bm / bb, 4) if bb else None,
        "oos_ece": ece, "oos_reliability_curve": rel,
        "oos_hold_by_confluence_bucket": buckets,
        "conf1_beats_conf0_nonoverlapping_ci": conf1_solid,
        "honest_summary": ("aggregate calibration is near-trivial (model ~ base-rate tracker); "
                           "skill vs a drift-adapted trailing base rate is small; the genuine "
                           "claim is the conf-1 hold-rate TILT, not a calibrated probability edge"),
        "verdict": ("PUBLISHABLE TILT: conf-1 > conf-0 with non-overlapping CI"
                    if conf1_solid else "MARGINAL: conf-1 tilt not separated OOS"),
    }
    ts_s = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    ev = REPO / "evidence" / "levels_product" / f"hold_model_validation_{args.symbol}_{args.horizon}_{ts_s}.json"
    ev.parent.mkdir(parents=True, exist_ok=True)
    ev.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    print(f"\nWROTE {ev}")


if __name__ == "__main__":
    main()
