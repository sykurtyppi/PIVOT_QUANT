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
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(REPO / "scripts" / "levels_product"))
from backfill_events import calculate_pivots  # noqa: E402
from hold_engine import label_coverage  # noqa: E402
from level_snapshot import build_snapshot, write_snapshot  # noqa: E402
from trading_calendar import REGULAR_SESSION_OPEN_ET, is_trading_day, session_close_et  # noqa: E402

DB = REPO / "data" / "pivot_events.sqlite"
ET = ZoneInfo("America/New_York")
HORIZONS = [15, 30, 60]
BASE_RATE_LOOKBACK_DAYS = 60  # recent window for the unconditional base rate


MAX_MISSING_SESSION_BARS = 30


def daily_rth_ohlc(con, symbol):
    columns = {
        str(row[1]) for row in con.execute("PRAGMA table_info(bar_data)").fetchall()
    }
    interval_filter = " AND bar_interval_sec=60" if "bar_interval_sec" in columns else ""
    b = pd.read_sql_query(
        "SELECT ts,open,high,low,close FROM bar_data WHERE symbol=?"
        + interval_filter
        + " ORDER BY ts",
        con, params=(symbol,))
    b = b.drop_duplicates(subset=["ts"], keep="last")
    t = pd.to_datetime(b.ts, unit="ms", utc=True).dt.tz_convert(ET)
    b["d"] = t.dt.date
    mins = t.dt.hour * 60 + t.dt.minute
    b["minute_of_day"] = mins
    # RTH is half-open: 09:30 <= timestamp < the session close. The later
    # per-date completeness check applies the exchange-specific close; this
    # broad prefilter must therefore exclude the regular 16:00 boundary too.
    b = b[(mins >= 570) & (mins < 960) & (b.high >= b.low) & (b.high > 0)]
    g = b.groupby("d")
    o = pd.DataFrame({"open": g.open.first(), "high": g.high.max(),
                      "low": g.low.min(), "close": g.close.last(),
                      "bars": g.minute_of_day.nunique(),
                      "last_minute": g.minute_of_day.max()}).reset_index()
    # Use the most recent COMPLETE session, regardless of run hour. Reaching an
    # arbitrary bar-count floor is insufficient: a feed truncated before the
    # close must never be stamped as the official completed-session close.
    # floor-trader pivots must come from a finished session, never today's partial
    # bars. A morning run → prior session; an after-close run → today's session
    # (i.e. the upcoming day's levels). Account for exchange early closes while
    # tolerating at most 30 missing one-minute bars within the session.
    open_minute = REGULAR_SESSION_OPEN_ET.hour * 60 + REGULAR_SESSION_OPEN_ET.minute
    complete = []
    for row in o.itertuples():
        if not is_trading_day(row.d):
            complete.append(False)
            continue
        close = session_close_et(row.d)
        close_minute = close.hour * 60 + close.minute
        expected_bars = close_minute - open_minute
        complete.append(
            row.last_minute >= close_minute - 1
            and row.bars >= expected_bars - MAX_MISSING_SESSION_BARS
        )
    o = o[pd.Series(complete, index=o.index, dtype=bool)]
    return o.sort_values("d").reset_index(drop=True)


def recent_base_rates(con, symbol, anchor_ts_ms, dq_min=0.9):
    # window anchored to the prior session (NOT wall-clock) so the published
    # base rate is reproducible on reruns.
    cutoff = anchor_ts_ms - BASE_RATE_LOOKBACK_DAYS * 86_400_000
    df = pd.read_sql_query(
        """SELECT el.horizon_min, AVG(el.reject) rate, COUNT(*) n
           FROM touch_events te JOIN event_labels el ON te.event_id=el.event_id
           WHERE te.symbol=? AND te.data_quality>=? AND te.ts_event>=? AND te.ts_event<=?
                 AND el.coverage_status='qualified'
                 AND el.reject IS NOT NULL
           GROUP BY el.horizon_min""",
        con, params=(symbol, dq_min, cutoff, anchor_ts_ms))
    return {int(r.horizon_min): {"hold_rate": round(float(r.rate), 4), "n": int(r.n)}
            for r in df.itertuples() if int(r.horizon_min) in HORIZONS}


def hold_rate_snapshot(con, symbol, anchor_ts_ms, dq_min=0.9):
    """Return morning rates only when every configured horizon is complete."""
    coverage = label_coverage(con, symbol, dq_min, HORIZONS)
    publishable = all(coverage[h]["complete"] for h in HORIZONS)
    return {
        "publishable": publishable,
        "coverage": coverage,
        "rates": recent_base_rates(con, symbol, anchor_ts_ms, dq_min) if publishable else {},
    }


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
    hold_snapshot = hold_rate_snapshot(con, args.symbol, anchor_ts)
    base = hold_snapshot["rates"]
    con.close()

    payload = build_snapshot(
        symbol=args.symbol,
        source_session_date=prior.d,
        source_ohlc={"high": prior.high, "low": prior.low, "close": prior.close},
        generated_at_utc=datetime.now(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z"),
        raw_levels=levels,
        evidence={
            "hold_rates_publishable": hold_snapshot["publishable"],
            "label_coverage": hold_snapshot["coverage"],
            "unconditional_hold_rate_by_horizon": base,
            "disclaimer": (
                "Base rate is the same for every level — no validated per-level edge exists "
                "at the open. Differentiated hold-probabilities are emitted intraday at the "
                "touch (confluence alerts)."
            ),
        },
    )
    out_dir = Path(args.out_dir)
    fp, latest = write_snapshot(payload, out_dir)

    # human-readable preview (the Discord/newsletter body)
    print(f"\n📍 {args.symbol} LEVEL MAP — {payload['session_date']} session (spot ~{spot:.2f})\n")
    for r in payload["levels"]:
        marker = "  ← spot" if abs(r["distance_from_reference_bps"]) < 5 else ""
        print(f"  {r['label']:4s} {r['price']:8.2f}  "
              f"({r['distance_from_reference_bps']:+6.1f} bps, {r['side']}){marker}")
    if hold_snapshot["publishable"]:
        print("\n  Base hold rate (any level):", {h: f"{v['hold_rate']*100:.0f}%" for h, v in sorted(base.items())})
    else:
        print("\n  Base hold rate: UNAVAILABLE (label coverage incomplete)")
    print(f"\nWROTE {fp} (latest: {latest.name}, snapshot_id={payload['snapshot_id']})")


if __name__ == "__main__":
    main()
