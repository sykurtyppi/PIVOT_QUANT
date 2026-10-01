#!/usr/bin/env python3
"""Step 3 of the level-behavior evidence layer (prereg §3): intraday post-touch
behaviour of the realized-vol band levels, from 1-minute SPY bars.

FROZEN definitions (prereg §3), per session T and band level P in {U1,U2,L1,L2}
(upper = +1σ/+2σ, lower = −1σ/−2σ), computed from the session's RTH 1-min bars:
  - First touch minute t*: first bar with high>=P (upper) or low<=P (lower).
  - Time-to-touch: minutes from the 09:30 ET open bar to t* (None if never touched).
  - First side touched: of {U1, L1}, the one whose first touch is earlier.
  - Acceptance / Rejection / Undetermined @ N in {15,30,60} min after t*, with
    threshold R = 0.20% of P:
      * Acceptance@N: price trades >=R on the BEYOND side of P (away from the prior
        close) before >=R on the ANCHOR side.
      * Rejection@N: the ANCHOR side (back toward the prior close) first.
      * Undetermined@N: neither side reached within N minutes (or both in the same
        bar, where 1-min OHLC cannot order them).
  - MFE / MAE after touch over (t*, session close], in % of P.

Band levels are POINT-IN-TIME (daily data < T), reused from build_daily_event_table.
1-min bars come from a committed snapshot (reproducibility), or are extracted once
from data/pivot_events.sqlite bar_data opened strictly READ-ONLY. The v1 runtime
window is ~125 sessions (Apr–Sep 2026); per prereg §4/§5 the per-touch acceptance/
rejection rates will FREQUENTLY ABSTAIN (n < 100), which is expected and honest.

Deterministic; offline (Mac Mini). Writes research/levels_evidence/intraday_outcomes.json.
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import json
import os
import sqlite3
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
SNAPSHOT = ROOT / "data" / "levels_evidence" / "spy_intraday_snapshot.json"
OUT = ROOT / "research" / "levels_evidence" / "intraday_outcomes.json"
# Read-only source DB(s), only used when the snapshot must be (re)built. Env override
# (os.pathsep-separated for several sources, listed authoritative-first) lets the offline
# machine union the live runtime DB with the ~1.5y macbook_final salvage.
DB_PATHS = [Path(p) for p in os.environ.get(
    "LEVELS_INTRADAY_DB", str(ROOT / "data" / "pivot_events.sqlite")).split(os.pathsep) if p]

ET = ZoneInfo("America/New_York")   # RTH is defined in exchange local time (DST-correct)
R_FRAC = 0.002                  # 0.20% post-touch threshold (prereg §3, FROZEN)
HORIZONS_N = [15, 30, 60]       # minutes (FROZEN)
MIN_UNCOND = 100                # prereg §5 unconditional sample gate

_spec = importlib.util.spec_from_file_location(
    "levels_evidence_build", ROOT / "scripts" / "levels_evidence" / "build_daily_event_table.py")
_b = importlib.util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(_b)

# displayed band level -> (price key in the daily event, side)
LEVELS = [("u1", "upper"), ("u2", "upper"), ("l1", "lower"), ("l2", "lower")]


def _et(ts_ms: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(ts_ms / 1000, ET)


def _et_date(ts_ms: int) -> str:
    return _et(ts_ms).strftime("%Y-%m-%d")


def _is_rth(ts_ms: int) -> bool:
    """09:30–16:00 America/New_York, inclusive — DST-correct across EST/EDT."""
    t = _et(ts_ms)
    return (9, 30) <= (t.hour, t.minute) <= (16, 0)


def fetch_or_load_intraday_snapshot() -> tuple[list[dict], str]:
    """Load the committed 1-min snapshot, or extract once from the read-only DB."""
    import hashlib
    if SNAPSHOT.exists():
        raw = SNAPSHOT.read_text(encoding="utf-8")
    else:
        existing = [p for p in DB_PATHS if p.exists()]
        if not existing:
            raise SystemExit(
                f"no intraday snapshot and no source DB in {DB_PATHS}; set LEVELS_INTRADAY_DB")
        by_ts: dict[int, dict] = {}   # union across sources; first (authoritative) source wins per ts
        for db in existing:
            con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
            try:
                rows = con.execute(
                    "SELECT ts, open, high, low, close FROM bar_data "
                    "WHERE symbol='SPY' AND bar_interval_sec=60 AND (ts % 60000)=0 "
                    "ORDER BY ts").fetchall()
            finally:
                con.close()
            for r in rows:
                ts = int(r[0])
                if ts in by_ts or not all(isinstance(x, (int, float)) for x in r[1:]):
                    continue
                if not _is_rth(ts):
                    continue
                by_ts[ts] = {"ts": ts, "open": r[1], "high": r[2], "low": r[3], "close": r[4]}
        candles = [by_ts[t] for t in sorted(by_ts)]
        if len(candles) < 1000:
            raise SystemExit(f"only {len(candles)} intraday bars; refusing to build")
        snap = {"symbol": "SPY", "interval_sec": 60,
                "source": f"pivot_events.sqlite bar_data (union of {len(existing)} source(s))",
                "bar_count": len(candles), "candles": candles}
        SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
        raw = json.dumps(snap, separators=(",", ":"), sort_keys=True)
        SNAPSHOT.write_text(raw, encoding="utf-8")
    data_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    candles = json.loads(raw)["candles"]
    return candles, data_hash


def group_sessions(candles: list[dict]) -> dict:
    """ET session date -> ordered list of RTH minute bars (snapshot is already RTH-only)."""
    sessions: dict[str, list[dict]] = {}
    for c in candles:
        sessions.setdefault(_et_date(c["ts"]), []).append(c)
    for d in sessions:
        sessions[d].sort(key=lambda b: b["ts"])
    return sessions


def first_touch_idx(bars: list[dict], P: float, side: str) -> int | None:
    for i, b in enumerate(bars):
        if side == "upper" and b["high"] >= P:
            return i
        if side == "lower" and b["low"] <= P:
            return i
    return None


def accept_reject(bars: list[dict], t_idx: int, P: float, side: str, n_min: int) -> str:
    """Prereg §3: acceptance (beyond side first), rejection (anchor side first),
    or undetermined, within n_min minutes after the touch bar."""
    t_ts = bars[t_idx]["ts"]
    beyond_thr = P * (1 + R_FRAC) if side == "upper" else P * (1 - R_FRAC)
    anchor_thr = P * (1 - R_FRAC) if side == "upper" else P * (1 + R_FRAC)
    for b in bars[t_idx + 1:]:
        if (b["ts"] - t_ts) > n_min * 60000:
            break
        hit_beyond = (b["high"] >= beyond_thr) if side == "upper" else (b["low"] <= beyond_thr)
        hit_anchor = (b["low"] <= anchor_thr) if side == "upper" else (b["high"] >= anchor_thr)
        if hit_beyond and hit_anchor:
            return "undetermined"   # 1-min OHLC can't order a same-bar both-sides move
        if hit_beyond:
            return "acceptance"
        if hit_anchor:
            return "rejection"
    return "undetermined"


def first_side_of(tu: int | None, tl: int | None) -> str:
    """Which of {U1, L1} was touched first (prereg §3), by touch timestamp."""
    if tu is None and tl is None:
        return "neither"
    if tl is None or (tu is not None and tu < tl):
        return "u1_first"
    if tu is None or tl < tu:
        return "l1_first"
    return "neither"   # exact same-minute tie cannot be ordered


def mfe_mae(bars: list[dict], t_idx: int, P: float, side: str) -> tuple[float, float]:
    """Max beyond-side (MFE) and anchor-side (MAE) excursion from P after the touch,
    to session close, as fractions of P."""
    mfe = 0.0
    mae = 0.0
    for b in bars[t_idx + 1:]:
        if side == "upper":
            mfe = max(mfe, (b["high"] - P) / P)
            mae = max(mae, (P - b["low"]) / P)
        else:
            mfe = max(mfe, (P - b["low"]) / P)
            mae = max(mae, (b["high"] - P) / P)
    return mfe, mae


def _agg(hits: int, n: int) -> dict:
    p, lo, hi = _b.wilson(hits, n)
    return {"hits": hits, "n": n, "rate": p, "ci_low": lo, "ci_high": hi,
            "sufficient": n >= MIN_UNCOND}


def build() -> dict:
    candles, snap_hash = fetch_or_load_intraday_snapshot()
    sessions = group_sessions(candles)

    # point-in-time daily band levels, indexed by session date
    bars_daily, daily_hash = _b.fetch_or_load_snapshot()
    daily_by_date = {e["date"]: e for e in _b.build_events(bars_daily)}

    dates = sorted(d for d in sessions if d in daily_by_date)
    per_level: dict[str, dict] = {lid: {"touched": 0, "accept": {n: 0 for n in HORIZONS_N},
                                        "reject": {n: 0 for n in HORIZONS_N},
                                        "undet": {n: 0 for n in HORIZONS_N},
                                        "ttt": [], "mfe": [], "mae": []}
                                  for lid, _ in LEVELS}
    first_side = {"u1_first": 0, "l1_first": 0, "neither": 0}

    for d in dates:
        bars = sessions[d]
        ev = daily_by_date[d]
        touch_ts = {}
        for lid, side in LEVELS:
            P = ev[lid]
            ti = first_touch_idx(bars, P, side)
            if ti is None:
                continue
            acc = per_level[lid]
            acc["touched"] += 1
            acc["ttt"].append((bars[ti]["ts"] - bars[0]["ts"]) / 60000.0)
            f, a = mfe_mae(bars, ti, P, side)
            acc["mfe"].append(f)
            acc["mae"].append(a)
            touch_ts[lid] = bars[ti]["ts"]
            for n in HORIZONS_N:
                res = accept_reject(bars, ti, P, side, n)
                acc[{"acceptance": "accept", "rejection": "reject", "undetermined": "undet"}[res]][n] += 1
        # first side touched among {u1, l1}
        first_side[first_side_of(touch_ts.get("u1"), touch_ts.get("l1"))] += 1

    n_sessions = len(dates)

    def median(xs):
        if not xs:
            return None
        s = sorted(xs)
        m = len(s) // 2
        return s[m] if len(s) % 2 else (s[m - 1] + s[m]) / 2

    levels_out = {}
    for lid, _side in LEVELS:
        acc = per_level[lid]
        t = acc["touched"]
        horizons = {}
        for n in HORIZONS_N:
            horizons[str(n)] = {
                "acceptance": _agg(acc["accept"][n], t),
                "rejection": _agg(acc["reject"][n], t),
                "undetermined": _agg(acc["undet"][n], t),
            }
        levels_out[lid] = {
            "label": {"u1": "+1σ (upper)", "u2": "+2σ (upper)",
                      "l1": "−1σ (lower)", "l2": "−2σ (lower)"}[lid],
            "intraday_touch": _agg(t, n_sessions),
            "post_touch_by_N": horizons,
            "time_to_touch_min": {"median": median(acc["ttt"]), "n": t},
            "mfe_pct": {"median": median(acc["mfe"]), "n": t},
            "mae_pct": {"median": median(acc["mae"]), "n": t},
        }

    return {
        "generated_from": "research/levels_evidence/prereg_level_behavior.md §3",
        "intraday_snapshot_sha256_16": snap_hash,
        "daily_snapshot_sha256_16": daily_hash,
        "n_sessions": n_sessions,
        "session_span": f"{dates[0]} .. {dates[-1]}" if dates else "",
        "post_touch_threshold_pct": R_FRAC,
        "horizons_min": HORIZONS_N,
        "unconditional_min_n": MIN_UNCOND,
        "labelling": ("Intraday post-touch behaviour, point-in-time band levels. v1 runtime "
                      "window is small (~125 sessions); per-touch rates abstain when n < 100 "
                      "(prereg §4/§5). Historical, not a forecast."),
        "first_side_touched": {**first_side, "n": n_sessions},
        "levels": levels_out,
    }


def main() -> int:
    rep = build()
    OUT.write_text(json.dumps(rep, indent=2), encoding="utf-8")
    print(f"intraday snapshot {rep['intraday_snapshot_sha256_16']} · {rep['n_sessions']} sessions "
          f"({rep['session_span']}) · wrote {OUT.relative_to(ROOT)}")
    for lid in ("u1", "l1", "u2", "l2"):
        L = rep["levels"][lid]
        tt = L["intraday_touch"]
        a30 = L["post_touch_by_N"]["30"]["acceptance"]
        a30_txt = "(abstain, n<100)" if not a30["sufficient"] else f"{a30['rate'] * 100:.0f}%"
        print(f"  {lid} {L['label']}: touched {tt['hits']}/{tt['n']} "
              f"({tt['rate'] * 100:.0f}%) · accept@30 {a30['hits']}/{a30['n']} {a30_txt}")
    fs = rep["first_side_touched"]
    print(f"  first side of ±1σ: u1 {fs['u1_first']} · l1 {fs['l1_first']} · neither {fs['neither']} (n={fs['n']})")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
