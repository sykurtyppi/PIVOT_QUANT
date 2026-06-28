#!/usr/bin/env python3
"""Free dealer-gamma (GEX) engine — the project's one validated edge, computed
from a FREE full SPY option chain (yfinance), no paid feed, no IBKR.

Backtested on full chains 2021-2025 (see memory gamma-vol-regime-edge): net GEX
forecasts next-day realized vol (~2x long-gamma vs short-gamma), beats vol
persistence (OOS R² 0.088→0.161) and times the implied-vs-realized gap. The
edge is on the RELATIVE GEX level (trailing quantile), NOT the absolute sign —
equity-index put-OI dominance biases the naive sign short, which is irrelevant
once classified against history.

This module ONLY computes a daily snapshot (fetch chain → BS gamma from IV →
net GEX + walls + pin + ATM-IV expected move). Regime classification (trailing
quantile) and history live in gex_signal.py. Pure compute; no DB writes.
"""
from __future__ import annotations

import datetime as dt
import time

import numpy as np
import pandas as pd
from scipy.stats import norm

RISK_FREE = 0.05
DEFAULT_MAX_DTE = 60          # near-term gamma window
ATM_BAND_PCT = 1.5           # |moneyness| <= 1.5% counts as ATM for expected-move IV


def _retry(fn, tries=3, pause=1.5):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 — yfinance/Yahoo is flaky; retry
            last = exc
            if i < tries - 1:
                time.sleep(pause)
    raise RuntimeError(f"yfinance call failed after {tries} tries: {last}")


def fetch_chain(symbol: str = "SPY", max_dte: int = DEFAULT_MAX_DTE, asof: dt.date | None = None):
    """Return (spot, chain_df[strike, oi, iv, is_call, dte]) from yfinance (free)."""
    import yfinance as yf
    t = yf.Ticker(symbol)
    hist = _retry(lambda: t.history(period="1d"))
    if hist.empty:
        raise RuntimeError(f"no spot for {symbol}")
    spot = float(hist["Close"].iloc[-1])
    today = asof or dt.date.today()
    exps = _retry(lambda: t.options)
    rows = []
    for e in exps:
        de = (dt.date.fromisoformat(e) - today).days
        if de < 0 or de > max_dte:
            continue
        oc = _retry(lambda e=e: t.option_chain(e))
        for df, is_call in ((oc.calls, True), (oc.puts, False)):
            d = df[["strike", "openInterest", "impliedVolatility"]].copy()
            d.columns = ["strike", "oi", "iv"]
            d["is_call"] = is_call
            d["dte"] = max(de, 1)
            rows.append(d)
    if not rows:
        raise RuntimeError("no expiries within max_dte")
    ch = pd.concat(rows, ignore_index=True)
    for c in ("strike", "oi", "iv"):
        ch[c] = pd.to_numeric(ch[c], errors="coerce")
    ch = ch.dropna(subset=["strike", "oi", "iv"])
    ch = ch[(ch.oi > 0) & (ch.iv > 0.01) & (ch.iv < 3.0)].reset_index(drop=True)
    return spot, ch


def bs_gamma(S, K, T, sigma, r=RISK_FREE):
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    return norm.pdf(d1) / (S * sigma * np.sqrt(T))


def compute_gex(spot: float, chain: pd.DataFrame) -> dict:
    """Net dealer GEX (call γ·OI − put γ·OI), walls, pin, ATM-IV expected move.

    Includes a SANITY block (pin near spot; walls bracket spot) — the same check
    that exposed the broken thin-snapshot feed.
    """
    S = float(spot)
    K = chain.strike.to_numpy(float)
    T = chain.dte.to_numpy(float) / 365.0
    sig = chain.iv.to_numpy(float)
    OI = chain.oi.to_numpy(float)
    is_call = chain.is_call.to_numpy(bool)
    gamma = bs_gamma(S, K, T, sig)
    goi = gamma * OI
    net = float((goi * np.where(is_call, 1.0, -1.0)).sum())
    gross = float(goi.sum())

    by_strike = pd.DataFrame({"strike": K, "goi": goi, "is_call": is_call})
    call_wall = float(by_strike[by_strike.is_call].groupby("strike").goi.sum().idxmax())
    put_wall = float(by_strike[~by_strike.is_call].groupby("strike").goi.sum().idxmax())
    pin = float(by_strike.groupby("strike").goi.sum().idxmax())

    atm = chain[(chain.strike - S).abs() / S * 100 <= ATM_BAND_PCT]
    atm_iv = float(atm.iv.mean()) if len(atm) else float("nan")
    exp_move_pct = atm_iv / np.sqrt(252) * 100 if atm_iv == atm_iv else None

    return {
        "spot": round(S, 2),
        "net_gex": net, "gross_gex": gross,
        "gex_norm": round(net / gross, 4) if gross else None,   # scale-free, in [-1,1]
        "call_wall": round(call_wall, 2), "put_wall": round(put_wall, 2), "pin": round(pin, 2),
        "atm_iv": round(atm_iv, 4) if atm_iv == atm_iv else None,
        "expected_move_pct_1d": round(exp_move_pct, 3) if exp_move_pct is not None else None,
        "n_contracts": int(len(chain)),
        "sanity": {
            "pin_near_spot": bool(abs(pin - S) / S * 100 <= 1.0),
            "pin_dist_pct": round(abs(pin - S) / S * 100, 2),
            "walls_bracket_spot": bool(put_wall < S < call_wall),
            "ok": bool(abs(pin - S) / S * 100 <= 1.5 and call_wall > S),
        },
    }


if __name__ == "__main__":
    import argparse, json
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--max-dte", type=int, default=DEFAULT_MAX_DTE)
    a = ap.parse_args()
    spot, ch = fetch_chain(a.symbol, a.max_dte)
    print(json.dumps(compute_gex(spot, ch), indent=2))
