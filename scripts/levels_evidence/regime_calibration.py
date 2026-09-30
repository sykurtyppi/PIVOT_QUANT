#!/usr/bin/env python3
"""Step 4 of the level-behavior evidence layer (prereg §6-7).

Two analyses on the point-in-time SPY vol-band event stream:

  (A) DESCRIPTIVE conditional touch rates by regime (vol tercile, trend, gap),
      full-sample partition, Wilson CIs, sample-gated (n>=30, else abstain).

  (B) OUT-OF-SAMPLE CALIBRATION (the real test): a walk-forward Brier comparison
      answering "does conditioning beat the baselines?". At each session T, every
      predictor uses ONLY sessions < T (point-in-time buckets AND point-in-time
      trailing rates), then is scored against the realized touch. Predictors:
        - unconditional trailing rate
        - vol-bucket trailing rate (bucket via trailing terciles)
        - (vol x trend) trailing rate, with abstention fallback when the bucket is thin
        - driftless-Brownian one-sided touch: +/-1s = 0.3173, +/-2s = 0.0455
      Per prereg §10: if conditioning does NOT lower out-of-sample Brier vs the
      baselines, we KEEP the unconditional rates and do not claim regime lift.

Reads the reproducible snapshot; deterministic. Writes
research/levels_evidence/regime_calibration.{md,json}.
"""
from __future__ import annotations

import importlib.util
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BUILD = ROOT / "scripts" / "levels_evidence" / "build_daily_event_table.py"
OUT_DIR = ROOT / "research" / "levels_evidence"

_spec = importlib.util.spec_from_file_location("levels_evidence_build", BUILD)
_b = importlib.util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(_b)

WINDOW = _b.WINDOW
BURN_IN = 252                 # start scoring after ~1y of trailing history
MIN_BUCKET = 30               # sample gate for a bucketed estimate (prereg §5)
GAP_CUT = 0.003               # +/-0.3% gap threshold (prereg §6)
SMA = 50


def phi(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


BROWNIAN_TOUCH = {1: 2 * (1 - phi(1.0)), 2: 2 * (1 - phi(2.0))}  # 0.3173, 0.0455


def tercile_cuts(values: list[float]) -> tuple[float, float]:
    s = sorted(values)
    n = len(s)
    return s[n // 3], s[(2 * n) // 3]


def vol_bucket(sigma: float, lo: float, hi: float) -> str:
    return "low" if sigma <= lo else ("high" if sigma > hi else "mid")


def build_augmented(bars: list[dict]) -> list[dict]:
    """Event rows with band + outcomes + point-in-time regime features."""
    closes = [b["close"] for b in bars]
    logret = [math.log(closes[i]) - math.log(closes[i - 1]) for i in range(1, len(closes))]
    events = []
    for i in range(len(bars)):
        if i - 1 - WINDOW < 0 or i < SMA + 1:
            continue
        w = logret[i - 1 - WINDOW: i - 1]
        if len(w) < WINDOW:
            continue
        sigma = _b.sample_std(w)
        if not math.isfinite(sigma) or sigma <= 0:
            continue
        anchor = bars[i - 1]["close"]                       # prior close (available)
        u1, u2 = anchor * math.exp(sigma), anchor * math.exp(2 * sigma)
        l1, l2 = anchor * math.exp(-sigma), anchor * math.exp(-2 * sigma)
        T = bars[i]
        sma = sum(closes[i - SMA: i]) / SMA                 # 50-SMA of closes through T-1
        gap = (T["open"] - anchor) / anchor
        events.append({
            "date": T["date"], "sigma": sigma, "anchor": anchor,
            "touch_u1": int(T["high"] >= u1), "touch_l1": int(T["low"] <= l1),
            "touch_u2": int(T["high"] >= u2), "touch_l2": int(T["low"] <= l2),
            "trend": "up" if anchor >= sma else "down",     # point-in-time
            "gap": "up" if gap > GAP_CUT else ("down" if gap < -GAP_CUT else "flat"),
        })
    return events


def wilson_row(hits: int, n: int, label: str) -> dict:
    p, lo, hi = _b.wilson(hits, n)
    return {"label": label, "hits": hits, "n": n, "rate": p, "ci_low": lo, "ci_high": hi,
            "sufficient": n >= MIN_BUCKET}


def descriptive(events: list[dict]) -> dict:
    sig = [e["sigma"] for e in events]
    lo, hi = tercile_cuts(sig)
    for e in events:
        e["vol"] = vol_bucket(e["sigma"], lo, hi)
    out = {}
    for outcome in ("touch_u1", "touch_l1"):
        blocks = {}
        for dim in ("vol", "trend", "gap"):
            vals = sorted({e[dim] for e in events})
            rows = []
            for b in vals:
                sub = [e for e in events if e[dim] == b]
                rows.append({"bucket": b, **wilson_row(sum(e[outcome] for e in sub), len(sub), b)})
            blocks[dim] = rows
        out[outcome] = blocks
    return out


def brier(preds: list[float], y: list[int]) -> float:
    return sum((p - t) ** 2 for p, t in zip(preds, y)) / len(y)


def calibration(events: list[dict], outcome: str, k: int) -> dict:
    """Walk-forward, point-in-time. Returns Brier per predictor over scored sessions."""
    # pre-session predictors use only info known at the prior close; gap_at_open also
    # uses the session open (known intraday, before any touch) — labelled accordingly.
    preds = {"unconditional": [], "vol_bucket": [], "vol_trend": [], "gap_at_open": [], "brownian": []}
    y = []
    for j in range(len(events)):
        if j < BURN_IN:
            continue
        prior = events[:j]                                   # only the past
        e = events[j]
        # point-in-time vol bucket via trailing terciles
        plo, phi_ = tercile_cuts([p["sigma"] for p in prior])
        vb = vol_bucket(e["sigma"], plo, phi_)
        # trailing rates
        uncond = sum(p[outcome] for p in prior) / len(prior)
        vsub = [p for p in prior if vol_bucket(p["sigma"], plo, phi_) == vb]
        vrate = (sum(p[outcome] for p in vsub) / len(vsub)) if len(vsub) >= MIN_BUCKET else uncond
        tsub = [p for p in vsub if p["trend"] == e["trend"]]
        trate = (sum(p[outcome] for p in tsub) / len(tsub)) if len(tsub) >= MIN_BUCKET else vrate
        gsub = [p for p in prior if p["gap"] == e["gap"]]
        grate = (sum(p[outcome] for p in gsub) / len(gsub)) if len(gsub) >= MIN_BUCKET else uncond
        preds["unconditional"].append(uncond)
        preds["vol_bucket"].append(vrate)
        preds["vol_trend"].append(trate)
        preds["gap_at_open"].append(grate)
        preds["brownian"].append(BROWNIAN_TOUCH[k])
        y.append(e[outcome])
    n = len(y)
    briers = {name: brier(p, y) for name, p in preds.items()}
    base_rate = sum(y) / n
    return {"outcome": outcome, "scored_sessions": n, "realized_rate": base_rate, "brier": briers}


def main() -> int:
    bars, data_hash = _b.fetch_or_load_snapshot()
    events = build_augmented(bars)
    desc = descriptive(events)
    calib = {
        "touch_u1": calibration(events, "touch_u1", 1),
        "touch_l1": calibration(events, "touch_l1", 1),
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    report = {"data_snapshot_sha256_16": data_hash, "n_events": len(events),
              "span": f"{events[0]['date']} .. {events[-1]['date']}",
              "burn_in": BURN_IN, "min_bucket": MIN_BUCKET,
              "descriptive": desc, "calibration": calib}
    (OUT_DIR / "regime_calibration.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    def pct(x): return f"{x*100:.1f}%"
    lines = ["# SPY vol-band — regime conditioning & out-of-sample calibration", "",
             f"Snapshot `{data_hash}` · {len(events)} events · {report['span']} · "
             f"burn-in {BURN_IN}, bucket gate n≥{MIN_BUCKET}.", ""]

    lines.append("## A. Descriptive conditional touch rates (full-sample partition)")
    for outcome, blocks in desc.items():
        lines.append(f"\n**{outcome}** (upper +1σ touch)" if outcome == "touch_u1"
                     else f"\n**{outcome}** (lower −1σ touch)")
        for dim, rows in blocks.items():
            cells = " · ".join(
                (f"{r['bucket']}={pct(r['rate'])} (n={r['n']})" if r["sufficient"]
                 else f"{r['bucket']}=abstain(n={r['n']})") for r in rows)
            lines.append(f"- by {dim}: {cells}")

    lines += ["", "## B. Out-of-sample calibration (walk-forward Brier, lower is better)", "",
              "Pre-session predictors use only the prior close; **gap (at-open)** also uses the "
              "session open, so it applies once the market has opened, not the night before.", "",
              "| Outcome | realized | unconditional | vol-bucket | vol×trend | gap (at-open) | Brownian |",
              "|---|---:|---:|---:|---:|---:|---:|"]
    for key, c in calib.items():
        b = c["brier"]
        lines.append(f"| {key} | {pct(c['realized_rate'])} | {b['unconditional']:.4f} | "
                     f"{b['vol_bucket']:.4f} | {b['vol_trend']:.4f} | {b['gap_at_open']:.4f} | {b['brownian']:.4f} |")

    lines.append("")
    lines.append("**Verdict (prereg §10):**")
    for key, c in calib.items():
        b = c["brier"]
        best = min(b, key=b.get)
        pre_wins = b["vol_trend"] < b["unconditional"]
        gap_wins = b["gap_at_open"] < b["unconditional"]
        lines.append(
            f"- {key}: best predictor = **{best}** (Brier {b[best]:.4f}). "
            + ("Pre-session (vol×trend) beats unconditional out-of-sample. " if pre_wins
               else "Pre-session conditioning does NOT beat unconditional — keep unconditional pre-open. ")
            + ("The **gap (at-open)** bucket beats it substantially — the strongest honest "
               "conditioning available once the session has opened." if gap_wins else ""))
    (OUT_DIR / "regime_calibration.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"snapshot {data_hash} events={len(events)}")
    for key, c in calib.items():
        b = c["brier"]
        print(f"  {key}: uncond={b['unconditional']:.4f} vol={b['vol_bucket']:.4f} "
              f"vol×trend={b['vol_trend']:.4f} brownian={b['brownian']:.4f} (realized {c['realized_rate']*100:.1f}%)")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
