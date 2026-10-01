#!/usr/bin/env python3
"""Step 1 of the level-behavior evidence layer (see
research/levels_evidence/prereg_level_behavior.md).

Point-in-time daily event table for SPY realized-volatility bands, plus the
unconditional touch / close-beyond base rates with Wilson 95% intervals.

Every session T's band is computed from data available only through the close of
T-1 (no lookahead), matching the live engine (src/math/MultiHorizonVolatilityLevels.js):
close-to-close log returns, sample std (ddof=1) over the trailing 20 returns,
band = prior_close * exp(±k*sigma), daily horizon (sessionsRemaining = 1).

Deterministic: reads a persisted data snapshot and recomputes identically.
Offline research artifact (Mac Mini). Writes:
  data/levels_evidence/spy_daily_snapshot.json   (raw OHLC snapshot, reproducibility)
  data/levels_evidence/daily_event_table.csv     (one row per evaluated session)
  research/levels_evidence/daily_touch_rates.json (machine-readable base rates)
  research/levels_evidence/daily_touch_rates.md   (human report)
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "levels_evidence"
OUT_DIR = ROOT / "research" / "levels_evidence"
SNAPSHOT = DATA_DIR / "spy_daily_snapshot.json"
EVENT_TABLE = DATA_DIR / "daily_event_table.csv"
REPORT_JSON = OUT_DIR / "daily_touch_rates.json"
REPORT_MD = OUT_DIR / "daily_touch_rates.md"

WINDOW = 20                 # trailing returns for sigma (matches the live engine's primary window)
PROXY = os.environ.get("MARKET_PROXY", "http://127.0.0.1:3000")
RANGE = os.environ.get("SPY_RANGE", "10y")


def sample_std(values: list[float]) -> float:
    n = len(values)
    if n < 2:
        return float("nan")
    mean = sum(values) / n
    return math.sqrt(sum((v - mean) ** 2 for v in values) / (n - 1))


def wilson(k: int, n: int, z: float = 1.959963984540054) -> tuple[float, float, float]:
    """Wilson score interval for a binomial proportion. Returns (p_hat, low, high)."""
    if n == 0:
        return (float("nan"), float("nan"), float("nan"))
    p = k / n
    denom = 1.0 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = (z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / denom
    return (p, centre - half, centre + half)


def fetch_or_load_snapshot() -> tuple[list[dict], str]:
    """Load the persisted snapshot, or fetch once from the market proxy and persist it."""
    if SNAPSHOT.exists():
        raw = SNAPSHOT.read_text(encoding="utf-8")
    else:
        url = f"{PROXY}/api/market?symbol=SPY&range={RANGE}&interval=1d"
        with urllib.request.urlopen(url, timeout=30) as resp:  # noqa: S310 (localhost proxy)
            payload = json.load(resp)
        candles = payload.get("candles") or []
        if len(candles) < 200:
            raise SystemExit(f"proxy returned only {len(candles)} candles; refusing to build")
        snap = {
            "symbol": "SPY", "range": RANGE, "interval": "1d",
            "source": "yahoo via market proxy", "candle_count": len(candles),
            "candles": [
                {"time": c["time"], "open": c["open"], "high": c["high"],
                 "low": c["low"], "close": c["close"]}
                for c in candles
            ],
        }
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        raw = json.dumps(snap, separators=(",", ":"), sort_keys=True)
        SNAPSHOT.write_text(raw, encoding="utf-8")
    data_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    candles = json.loads(raw)["candles"]
    # unique, sorted, valid daily bars
    seen = set()
    bars = []
    for c in candles:
        d = _date(c["time"])
        if d in seen:
            continue
        o, h, l, cl = c["open"], c["high"], c["low"], c["close"]
        if not all(isinstance(x, (int, float)) and x > 0 for x in (o, h, l, cl)):
            continue
        seen.add(d)
        bars.append({"date": d, "open": o, "high": h, "low": l, "close": cl})
    bars.sort(key=lambda b: b["date"])
    return bars, data_hash


def _date(epoch_sec: float) -> str:
    import datetime as dt
    return dt.datetime.utcfromtimestamp(int(epoch_sec)).strftime("%Y-%m-%d")


def build_events(bars: list[dict]) -> list[dict]:
    """One row per evaluated session T (needs >= WINDOW prior returns)."""
    closes = [b["close"] for b in bars]
    logret = [math.log(closes[i]) - math.log(closes[i - 1]) for i in range(1, len(closes))]
    # logret[k] corresponds to bars[k+1] (return into that session). For session index i,
    # the returns available through the close of T-1 are logret[0 .. i-2] (i.e. into bars[1..i-1]).
    events = []
    for i in range(len(bars)):
        # need WINDOW returns ending at the return INTO bars[i-1] => logret index i-2 down to i-1-WINDOW
        hi_idx = i - 1            # last usable return index (into bars[i-1]) is logret[i-2]; slice end exclusive
        if i - 1 - WINDOW < 0 or hi_idx > len(logret):
            continue
        window_returns = logret[i - 1 - WINDOW: i - 1]
        if len(window_returns) < WINDOW:
            continue
        sigma = sample_std(window_returns)
        if not math.isfinite(sigma) or sigma <= 0:
            continue
        anchor = bars[i - 1]["close"]           # prior session close
        u1, u2 = anchor * math.exp(sigma), anchor * math.exp(2 * sigma)
        l1, l2 = anchor * math.exp(-sigma), anchor * math.exp(-2 * sigma)
        T = bars[i]
        events.append({
            "date": T["date"], "anchor": anchor, "sigma": sigma,
            "u1": u1, "u2": u2, "l1": l1, "l2": l2,
            "high": T["high"], "low": T["low"], "close": T["close"],
            "touch_u1": int(T["high"] >= u1), "touch_u2": int(T["high"] >= u2),
            "touch_l1": int(T["low"] <= l1),  "touch_l2": int(T["low"] <= l2),
            "close_above_u1": int(T["close"] >= u1), "close_above_u2": int(T["close"] >= u2),
            "close_below_l1": int(T["close"] <= l1), "close_below_l2": int(T["close"] <= l2),
        })
    for e in events:
        e["touch_either_1sigma"] = int(e["touch_u1"] or e["touch_l1"])
        e["touch_either_2sigma"] = int(e["touch_u2"] or e["touch_l2"])
    return events


def aggregate(events: list[dict]) -> dict:
    n = len(events)
    metrics = [
        ("touch_u1", "Upper +1σ touched (session high)"),
        ("touch_l1", "Lower −1σ touched (session low)"),
        ("touch_either_1sigma", "Either ±1σ touched"),
        ("touch_u2", "Upper +2σ touched"),
        ("touch_l2", "Lower −2σ touched"),
        ("touch_either_2sigma", "Either ±2σ touched"),
        ("close_above_u1", "Close above +1σ"),
        ("close_below_l1", "Close below −1σ"),
        ("close_above_u2", "Close above +2σ"),
        ("close_below_l2", "Close below −2σ"),
    ]
    out = []
    for key, label in metrics:
        k = sum(e[key] for e in events)
        p, lo, hi = wilson(k, n)
        out.append({"metric": key, "label": label, "hits": k, "n": n,
                    "rate": p, "ci_low": lo, "ci_high": hi})
    return {"n_sessions": n, "metrics": out}


def main() -> int:
    bars, data_hash = fetch_or_load_snapshot()
    events = build_events(bars)
    if not events:
        raise SystemExit("no events produced")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    cols = ["date", "anchor", "sigma", "u1", "u2", "l1", "l2", "high", "low", "close",
            "touch_u1", "touch_u2", "touch_l1", "touch_l2",
            "close_above_u1", "close_above_u2", "close_below_l1", "close_below_l2",
            "touch_either_1sigma", "touch_either_2sigma"]
    with EVENT_TABLE.open("w", encoding="utf-8") as f:
        f.write(",".join(cols) + "\n")
        for e in events:
            f.write(",".join(
                (f"{e[c]:.6f}" if isinstance(e[c], float) else str(e[c])) for c in cols) + "\n")

    agg = aggregate(events)
    span = f"{events[0]['date']} .. {events[-1]['date']}"
    report = {
        "generated_from": "research/levels_evidence/prereg_level_behavior.md",
        "estimator": "realized_vol band prior_close*exp(+/-k*sigma), sigma=std(ddof=1) of trailing "
                     f"{WINDOW} close-to-close log returns, point-in-time (data < T)",
        "data_source": "SPY daily OHLC, yahoo via market proxy",
        "data_snapshot_sha256_16": data_hash,
        "session_span": span,
        **agg,
    }
    REPORT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")

    def pctci(m):
        return f"{m['rate']*100:5.2f}%  [{m['ci_low']*100:.2f}–{m['ci_high']*100:.2f}%]"
    lines = [
        "# SPY realized-vol band — unconditional base rates (point-in-time)",
        "",
        f"Estimator: {report['estimator']}",
        f"Data: {report['data_source']} · snapshot `{data_hash}` · sessions {span}",
        f"Evaluated sessions (n): **{agg['n_sessions']}**",
        "",
        "| Outcome | Rate | 95% CI (Wilson) | hits/n |",
        "|---|---:|---|---:|",
    ]
    for m in agg["metrics"]:
        lines.append(f"| {m['label']} | {m['rate']*100:.2f}% | "
                     f"{m['ci_low']*100:.2f}–{m['ci_high']*100:.2f}% | {m['hits']}/{m['n']} |")
    lines += [
        "",
        "Notes:",
        "- Point-in-time: each session's band uses only close-to-close returns through the prior close.",
        "- Touch = intraday high/low reached the level; close-beyond = the session closed past it.",
        "- These are unconditional base rates. Regime conditioning and out-of-sample calibration vs "
        "the unconditional / vol-bucket / driftless-Brownian baselines are later steps (see prereg §7).",
        "- Compare informally to the driftless-Brownian one-sided touch expectation: ±1σ ≈ 31.7%, ±2σ ≈ 4.6%.",
    ]
    REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"snapshot sha256[16]={data_hash}  sessions={agg['n_sessions']}  span={span}")
    print(f"wrote {EVENT_TABLE.relative_to(ROOT)}, {REPORT_MD.relative_to(ROOT)}, {REPORT_JSON.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
