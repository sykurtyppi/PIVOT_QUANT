#!/usr/bin/env python3
"""Daily free GEX vol-regime SIGNAL (context, not a trade signal).

Computes today's dealer-gamma snapshot from the free yfinance chain (gex_engine),
appends it to a history DB, and classifies today's GEX against its TRAILING
distribution — the validated regime axis (high GEX → vol suppressed/calm; low
GEX → vol amplified/elevated). Emits expected-move + walls (useful from day 1)
and the regime (calibrates over ~MIN_HISTORY days of live data).

What the backtest established (memory gamma-vol-regime-edge): the edge is the
RELATIVE GEX level, so we classify vs trailing history, not the absolute sign.

Writes ONLY its own product DB (data/gamma_signal.sqlite) + an evidence JSON.
    python scripts/gamma_signal/gex_signal.py
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "gamma_signal"))
from gex_engine import compute_gex, fetch_chain  # noqa: E402

DB = REPO / "data" / "gamma_signal.sqlite"
MIN_HISTORY = 20          # days of live history before the regime is trusted
TRAIL = 252               # trailing window for the regime distribution


def _con():
    con = sqlite3.connect(DB)
    con.execute("""CREATE TABLE IF NOT EXISTS gex_history (
        date TEXT PRIMARY KEY, symbol TEXT, spot REAL, gex_norm REAL, net_gex REAL,
        gross_gex REAL, atm_iv REAL, expected_move_pct REAL, call_wall REAL,
        put_wall REAL, pin REAL, n_contracts INTEGER, created_at INTEGER)""")
    con.commit()
    return con


def regime_from_quantile(pct):
    # higher GEX percentile = more long gamma = calmer
    if pct >= 0.60:
        return ("CALM / long-gamma",
                "Vol suppression likely. Contained range; fading extremes (mean-reversion) "
                "favored over chasing. Realized tends to come in BELOW the implied move.")
    if pct <= 0.40:
        return ("ELEVATED / short-gamma",
                "Vol amplification likely. Larger, trendier moves; momentum over fading. "
                "Realized tends to come in AT/ABOVE the implied move.")
    return ("NEUTRAL", "Mixed gamma regime — no strong vol-suppression or -amplification tilt.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--max-dte", type=int, default=60)
    ap.add_argument("--out-dir", default=str(REPO / "evidence" / "gamma_signal"))
    args = ap.parse_args()

    spot, chain = fetch_chain(args.symbol, args.max_dte)
    snap = compute_gex(spot, chain)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    con = _con()
    con.execute(
        """INSERT OR REPLACE INTO gex_history
           (date,symbol,spot,gex_norm,net_gex,gross_gex,atm_iv,expected_move_pct,
            call_wall,put_wall,pin,n_contracts,created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (today, args.symbol, snap["spot"], snap["gex_norm"], snap["net_gex"], snap["gross_gex"],
         snap["atm_iv"], snap["expected_move_pct_1d"], snap["call_wall"], snap["put_wall"],
         snap["pin"], snap["n_contracts"], now_ms))
    con.commit()

    hist = [r[0] for r in con.execute(
        "SELECT gex_norm FROM gex_history WHERE symbol=? AND gex_norm IS NOT NULL ORDER BY date",
        (args.symbol,)).fetchall()]
    con.close()
    hist = hist[-TRAIL:]
    n = len(hist)
    g = snap["gex_norm"]
    if n >= MIN_HISTORY and g is not None:
        pct = float((np.asarray(hist) <= g).mean())
        regime, read = regime_from_quantile(pct)
        regime_status = "live"
    else:
        pct, regime, read = None, "CALIBRATING", f"Regime needs {MIN_HISTORY} days of history (have {n})."
        regime_status = "calibrating"

    out = {
        "product": "gex_vol_regime_signal", "symbol": args.symbol, "date": today,
        "as_of_utc": datetime.now(timezone.utc).isoformat(),
        "spot": snap["spot"], "gex_norm": g, "gex_percentile_trailing": round(pct, 3) if pct is not None else None,
        "regime": regime, "regime_status": regime_status, "read": read,
        "expected_move_pct_1d": snap["expected_move_pct_1d"], "atm_iv": snap["atm_iv"],
        "call_wall": snap["call_wall"], "put_wall": snap["put_wall"], "pin": snap["pin"],
        "history_days": n, "sanity": snap["sanity"],
        "disclaimer": "Vol-regime CONTEXT (sizing/risk/regime), not a trade signal. Educational.",
    }
    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"gex_signal_{args.symbol}.json").write_text(json.dumps(out, indent=2))

    em = snap["expected_move_pct_1d"]
    band = f"+/-{em:.2f}%  (~{snap['spot']*em/100:.2f} pts)" if em else "n/a"
    print(f"\n{args.symbol} GAMMA VOL-REGIME — {today}   (spot {snap['spot']:.2f})\n")
    print(f"  Regime:         {regime}" + (f"   [pctile {pct:.0%}, {n}d hist]" if pct is not None else f"   [{read}]"))
    if regime_status == "live":
        print(f"  Read:           {read}")
    print(f"  Implied 1-day move: {band}   (ATM IV {snap['atm_iv']:.1%})")
    print(f"  Levels:         pin {snap['pin']:.0f}   call wall {snap['call_wall']:.0f}   put wall {snap['put_wall']:.0f}")
    print(f"  GEX:            {g:+.3f} (normalized; classified vs trailing history)")
    sane = snap["sanity"]
    print(f"  Data sanity:    {'OK' if sane['ok'] else 'CHECK'} (pin {sane['pin_dist_pct']}% from spot, {snap['n_contracts']} contracts)")
    print(f"\n  Context only — not a trade signal.\n")


if __name__ == "__main__":
    main()
