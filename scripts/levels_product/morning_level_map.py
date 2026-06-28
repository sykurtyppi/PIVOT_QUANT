#!/usr/bin/env python3
"""Phase-0 (2/4): the morning SPY level map (the descriptive draw).

Each morning this emits today's S/R level map — the deterministic floor-trader
pivots computed from the prior completed RTH session — annotated with the
UNCONDITIONAL hold rate by horizon (the honest base rate; per Phase-0 validation
there is NO forecastable per-level edge at the open, so every level carries the
same base rate, NOT a differentiated probability). The differentiated edge is
the intraday confluence alert (see forecast_store.py), fired at the touch.

Pivots reuse scripts/backfill_events.calculate_pivots (the live source of truth)
so the published prices match what the system computes — zero drift.

Output: a JSON post-payload (levels sorted by price, distance from spot, base
rates) suitable for a Discord/newsletter template. Read-only on the DB.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
from backfill_events import calculate_pivots  # noqa: E402

DB = REPO / "data" / "pivot_events.sqlite"
ET = ZoneInfo("America/New_York")
HORIZONS = [15, 30, 60]
BASE_RATE_LOOKBACK_DAYS = 60  # recent window for the unconditional base rate


MIN_SESSION_BARS = 360  # a complete RTH 1-min session is ~390 bars; require most of it


def daily_rth_ohlc(con, symbol):
    b = pd.read_sql_query(
        "SELECT ts,open,high,low,close FROM bar_data WHERE symbol=? ORDER BY ts",
        con, params=(symbol,))
    t = pd.to_datetime(b.ts, unit="ms", utc=True).dt.tz_convert(ET)
    b["d"] = t.dt.date
    mins = t.dt.hour * 60 + t.dt.minute
    b = b[(mins >= 570) & (mins <= 960) & (b.high >= b.low) & (b.high > 0)]
    g = b.groupby("d")
    o = pd.DataFrame({"open": g.open.first(), "high": g.high.max(),
                      "low": g.low.min(), "close": g.close.last(),
                      "bars": g.size()}).reset_index()
    # Use the most recent COMPLETE session (by bar count), regardless of run hour:
    # floor-trader pivots must come from a finished session, never today's partial
    # bars. A morning run → prior session; an after-close run → today's session
    # (i.e. the upcoming day's levels). Today's partial session has < MIN bars and
    # is excluded automatically, so no explicit "exclude today" is needed.
    o = o[o.bars >= MIN_SESSION_BARS]
    return o.sort_values("d").reset_index(drop=True)


def recent_base_rates(con, symbol, anchor_ts_ms, dq_min=0.9):
    # window anchored to the prior session (NOT wall-clock) so the published
    # base rate is reproducible on reruns.
    cutoff = anchor_ts_ms - BASE_RATE_LOOKBACK_DAYS * 86_400_000
    df = pd.read_sql_query(
        """SELECT el.horizon_min, AVG(el.reject) rate, COUNT(*) n
           FROM touch_events te JOIN event_labels el ON te.event_id=el.event_id
           WHERE te.symbol=? AND te.data_quality>=? AND te.ts_event>=? AND te.ts_event<=?
                 AND el.reject IS NOT NULL
           GROUP BY el.horizon_min""",
        con, params=(symbol, dq_min, cutoff, anchor_ts_ms))
    return {int(r.horizon_min): {"hold_rate": round(float(r.rate), 4), "n": int(r.n)}
            for r in df.itertuples() if int(r.horizon_min) in HORIZONS}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--out-dir", default=str(REPO / "evidence" / "levels_product"))
    args = ap.parse_args()

    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    ohlc = daily_rth_ohlc(con, args.symbol)
    if len(ohlc) < 2:
        raise SystemExit("not enough completed sessions to compute pivots")
    prior = ohlc.iloc[-1]  # most recent COMPLETED session (current/partial day excluded)
    anchor_ts = int(pd.Timestamp(prior.d).tz_localize(ET).timestamp() * 1000) + 16 * 3_600_000
    spot = float(prior.close)
    levels = calculate_pivots(float(prior.high), float(prior.low), float(prior.close))
    base = recent_base_rates(con, args.symbol, anchor_ts)
    con.close()

    rows = []
    for label, price in levels.items():
        rows.append({
            "level_type": label, "price": round(float(price), 2),
            "distance_from_spot_bps": round((price - spot) / spot * 1e4, 1),
            "side": "resistance" if price >= spot else "support",
        })
    rows.sort(key=lambda r: -r["price"])

    payload = {
        "product": "morning_level_map", "symbol": args.symbol,
        "as_of_utc": datetime.utcnow().isoformat() + "Z",
        "prior_session_date": str(prior.d),
        "prior_session_ohlc": {"high": round(float(prior.high), 2), "low": round(float(prior.low), 2),
                               "close": round(float(prior.close), 2)},
        "reference_spot": round(spot, 2),
        "levels": rows,
        "unconditional_hold_rate_by_horizon": base,
        "disclaimer": ("Base rate is the same for every level — no validated per-level edge exists "
                       "at the open. Differentiated hold-probabilities are emitted intraday at the "
                       "touch (confluence alerts)."),
    }
    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    fp = out_dir / f"morning_level_map_{args.symbol}_{prior.d}.json"
    fp.write_text(json.dumps(payload, indent=2))

    # human-readable preview (the Discord/newsletter body)
    print(f"\n📍 SPY LEVEL MAP — {prior.d} session basis (spot ~{spot:.2f})\n")
    for r in rows:
        marker = "  ← spot" if abs(r["distance_from_spot_bps"]) < 5 else ""
        print(f"  {r['level_type']:4s} {r['price']:8.2f}  ({r['distance_from_spot_bps']:+6.1f} bps, {r['side']}){marker}")
    print("\n  Base hold rate (any level):", {h: f"{v['hold_rate']*100:.0f}%" for h, v in sorted(base.items())})
    print(f"\nWROTE {fp}")


if __name__ == "__main__":
    main()
