#!/usr/bin/env python3
"""Phase-1: format + deliver the MORNING post (level map + track-record scoreboard).

Reads the two artifacts the daily job just wrote (morning_level_map_<sym>_<date>.json
and track_record_<sym>.json) and renders a Discord/newsletter message, then posts
it via notify.py (dry-runs to stdout if no webhook configured).

The message leads with the level map (the descriptive draw) and closes with the
rolling track record + the honest "most levels are coin-flips, confluence levels
aren't" framing. Read-only; no DB access.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import notify

REPO = Path(__file__).resolve().parents[2]
ART = REPO / "evidence" / "levels_product"


def _latest(symbol):
    maps = sorted(ART.glob(f"morning_level_map_{symbol}_*.json"))
    if not maps:
        raise SystemExit(f"no morning_level_map_{symbol}_*.json — run morning_level_map.py first")
    tr = ART / f"track_record_{symbol}.json"
    if not tr.exists():
        raise SystemExit(f"no {tr.name} — run build_track_record.py first")
    try:
        return json.loads(maps[-1].read_text()), json.loads(tr.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        raise SystemExit(f"could not read product artifacts ({type(exc).__name__}: {exc}) "
                         f"— regenerate via the daily job") from exc


def render(mp, tr) -> str:
    sym = mp["symbol"]
    spot = mp["reference_spot"]
    res = [r for r in mp["levels"] if r["side"] == "resistance"]
    sup = [r for r in mp["levels"] if r["side"] == "support"]
    L = [f"📍 **{sym} Levels — {mp['prior_session_date']} basis**  (spot ~{spot})", ""]
    L.append("**Resistance**")
    for r in res:
        L.append(f"`{r['level_type']:>3}`  {r['price']:>8.2f}   `{r['distance_from_spot_bps']:+6.0f} bps`")
    L.append("**Support**")
    for r in sup:
        L.append(f"`{r['level_type']:>3}`  {r['price']:>8.2f}   `{r['distance_from_spot_bps']:+6.0f} bps`")
    base = mp.get("unconditional_hold_rate_by_horizon", {})
    if base:
        bstr = " · ".join(f"{h}m {int(round(v['hold_rate']*100))}%" for h, v in sorted(base.items(), key=lambda x: int(x[0])))
        L += ["", f"Base hold rate (any level): {bstr}"]

    L += ["", "📊 **Track record** — last 30 trading days, confluence-alerted (conf≥1) levels"]
    for h in (15, 30, 60):
        rec = tr["horizons"].get(f"h{h}", {})
        sb = rec.get("rolling_scoreboard_alerted_levels") or rec.get("rolling_scoreboard_alerted")
        if sb and sb.get("actual_hold_rate") is not None:
            n = sb.get("n_alerted_touches", sb.get("n"))
            L.append(f"   {h}m: **{int(round(sb['actual_hold_rate']*100))}%** held  (n={n})")
    L += ["", "_Most S/R levels are roughly coin-flips; confluence levels hold meaningfully more. "
          "Educational, not financial advice._"]
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="SPY")
    args = ap.parse_args()
    mp, tr = _latest(args.symbol)
    msg = render(mp, tr)
    delivered = notify.post(msg)
    print(f"\n[{'delivered to webhook' if delivered else 'dry-run (no webhook set)'}]")


if __name__ == "__main__":
    main()
