#!/usr/bin/env python3
"""Phase-0 (4/4): the public TRACK RECORD — the credibility asset.

Replays the confluence hold model leak-free over all labeled SPY touches and
scores it against realized outcomes. Leak-freeness is enforced by a horizon
TIME-embargo (see hold_engine): each touch is forecast using ONLY outcomes that
had resolved before that touch — so this is an honest, as-of-touch-time record,
identical to what the live forward log accumulates, available over 14 months now.

HONEST FRAMING (post-audit): the model is, in aggregate, close to a base-rate
tracker, so good aggregate calibration is largely trivial. The REAL, sellable
signal is the CONFLUENCE-1 TILT — conf-1 levels hold ~8-9 pts more than conf-0,
OOS-stable, non-overlapping CIs. Brier skill is measured against a DRIFT-ADAPTED
trailing base rate (not a stale constant), so it isolates the tilt's value and
is honestly small. The conf>=2 bucket is small/time-concentrated and flagged
non-publishable. Read-only on the live DB (opened mode=ro).
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss

import sys
REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "levels_product"))
from hold_engine import (  # noqa: E402
    BUCKETS, HORIZONS, MIN_PUBLISHABLE_N, bucket, et_dates_from_ms,
    reliability_curve, trailing_base_rate, trailing_forecasts, wilson,
)

DB = REPO / "data" / "pivot_events.sqlite"


def _read_con():
    return sqlite3.connect(f"file:{DB}?mode=ro", uri=True)


def horizon_record(con, symbol, horizon, dq_min, recent_days):
    df = pd.read_sql_query(
        """SELECT te.event_id, te.ts_event, te.confluence_count, el.reject
           FROM touch_events te JOIN event_labels el ON te.event_id=el.event_id
           WHERE te.symbol=? AND el.horizon_min=? AND te.data_quality>=?
                 AND el.reject IS NOT NULL AND te.confluence_count IS NOT NULL
           ORDER BY te.ts_event, te.event_id""",
        con, params=(symbol, horizon, dq_min))
    if df.empty:
        return {"horizon_min": horizon, "n": 0, "message": "no labeled touches"}
    ts = df.ts_event.to_numpy()
    df["bkt"] = bucket(df.confluence_count.to_numpy())
    out = df.reject.to_numpy(int)
    df["pred"] = trailing_forecasts(ts, df.bkt.to_numpy(), out, horizon)
    df["base"] = trailing_base_rate(ts, out, horizon)

    sc = df.dropna(subset=["pred", "base"]).copy()
    y, p, b = sc.reject.to_numpy(int), sc.pred.to_numpy(float), sc.base.to_numpy(float)
    bm = float(brier_score_loss(y, p))
    bb = float(brier_score_loss(y, b))  # drift-adapted trailing base rate
    rel, ece = reliability_curve(p, y)

    by_bucket = {}
    for bk in BUCKETS:
        s = sc[sc.bkt == bk]
        k, nn = int(s.reject.sum()), len(s)
        by_bucket[bk] = {
            "n": nn, "pred_hold": round(float(s.pred.mean()), 4) if nn else None,
            "actual_hold": round(k / nn, 4) if nn else None, "ci95": wilson(k, nn),
            "publishable": bool(nn >= MIN_PUBLISHABLE_N),
        }

    sc["d"] = et_dates_from_ms(sc.ts_event)
    days = sorted(sc.d.unique())[-recent_days:]
    recent = sc[sc.d.isin(days) & (sc.bkt != "0")]
    rk, rn = int(recent.reject.sum()), len(recent)
    scoreboard = {"window_trading_days": recent_days, "n_alerted_touches": rn,
                  "actual_hold_rate": round(rk / rn, 4) if rn else None,
                  "ci95": wilson(rk, rn),
                  "avg_predicted": round(float(recent.pred.mean()), 4) if rn else None}

    return {
        "horizon_min": horizon, "n_scored": len(sc),
        "brier_model": round(bm, 5), "brier_trailing_base_rate": round(bb, 5),
        "brier_skill_vs_trailing_base": round(1 - bm / bb, 4) if bb else None,
        "ece": ece, "reliability_curve": rel,
        "by_confluence_bucket": by_bucket,
        "rolling_scoreboard_alerted_levels": scoreboard,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--dq-min", type=float, default=0.9)
    ap.add_argument("--recent-days", type=int, default=30)
    ap.add_argument("--out-dir", default=str(REPO / "evidence" / "levels_product"))
    args = ap.parse_args()

    con = _read_con()
    records = {f"h{h}": horizon_record(con, args.symbol, h, args.dq_min, args.recent_days) for h in HORIZONS}
    con.close()

    out = {
        "product": "levels_track_record", "symbol": args.symbol,
        "as_of_utc": datetime.utcnow().isoformat() + "Z",
        "method": "leak-free (horizon-embargoed) trailing confluence-bucket hold model",
        "honest_framing": ("aggregate calibration is near-trivial (model ~ base-rate tracker); "
                           "the sellable signal is the conf-1 tilt; skill is vs a drift-adapted "
                           "trailing base rate and is honestly small"),
        "horizons": records,
    }
    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    fp = out_dir / f"track_record_{args.symbol}.json"
    fp.write_text(json.dumps(out, indent=2))

    print(f"\n📊 SPY VALIDATED-LEVELS TRACK RECORD  (leak-free, horizon-embargoed, {args.symbol})\n")
    for h in HORIZONS:
        r = records[f"h{h}"]
        if not r.get("n_scored"):
            continue
        sb = r["rolling_scoreboard_alerted_levels"]; bb = r["by_confluence_bucket"]
        print(f"  ── {h}m  (n={r['n_scored']}, ECE={r['ece']}, skill-vs-trailing-base={r['brier_skill_vs_trailing_base']}) ──")
        for bk in BUCKETS:
            c = bb[bk]
            if c["n"]:
                lbl = {"0": "no confluence", "1": "1 confluence", "2+": "2+ confluence"}[bk]
                flag = "" if c["publishable"] else "  ⚠ not publishable (small n)"
                print(f"     {lbl:15s} n={c['n']:5d}  predicted {c['pred_hold']:.0%}  actual {c['actual_hold']:.0%}  CI{c['ci95']}{flag}")
        if sb["n_alerted_touches"]:
            print(f"     last {sb['window_trading_days']}d alerted (conf≥1): {sb['actual_hold_rate']:.0%} held "
                  f"(n={sb['n_alerted_touches']}, CI{sb['ci95']})")
        print()
    print(f"WROTE {fp}")


if __name__ == "__main__":
    main()
