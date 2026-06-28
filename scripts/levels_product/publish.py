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
import os
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
    """Clean, no-emoji, plain-text post for a human reader (single column so it
    degrades gracefully in proportional-font mail clients). Keeps the exporter's
    discipline (plain text, price+label) but uses real section headers and puts
    the track record — the product's proof — front and centre.
    """
    sym = mp["symbol"]
    spot = mp["reference_spot"]
    res = [r for r in mp["levels"] if r["side"] == "resistance"]  # already price-desc
    sup = [r for r in mp["levels"] if r["side"] == "support"]

    def level_line(r):
        return f"  {r['level_type']:<3} {r['price']:>8.2f}   {r['distance_from_spot_bps']:>+5.0f} bps"

    L = [f"{sym} LEVELS — {mp['prior_session_date']} session   (spot {spot:.2f})", ""]
    L.append("RESISTANCE")
    L += [level_line(r) for r in res]
    L += ["", "SUPPORT"]
    L += [level_line(r) for r in sup]

    tparts, n_alerted = [], None
    for h in (15, 30, 60):
        rec = tr["horizons"].get(f"h{h}", {})
        sb = rec.get("rolling_scoreboard_alerted_levels") or rec.get("rolling_scoreboard_alerted")
        if sb and sb.get("actual_hold_rate") is not None:
            tparts.append(f"{h}m {int(round(sb['actual_hold_rate'] * 100))}% held")
            n_alerted = sb.get("n_alerted_touches", sb.get("n"))
    if tparts:
        L += ["", "TRACK RECORD — last 30 trading days, confluence levels",
              "  " + "    ".join(tparts) + (f"   (n={n_alerted})" if n_alerted else "")]
    base = mp.get("unconditional_hold_rate_by_horizon", {})
    if base:
        bstr = " / ".join(f"{int(round(v['hold_rate'] * 100))}%"
                          for _, v in sorted(base.items(), key=lambda x: int(x[0])))
        L.append(f"  Base rate, any level (15m/30m/60m): {bstr}")

    L += ["",
          "Most S/R levels are roughly coin-flips; confluence levels hold meaningfully more.",
          "Educational, not financial advice."]
    return "\n".join(L)


def _resolve_channel(requested: str) -> str:
    """auto → email if SMTP configured, else webhook if set, else dry-run."""
    if requested != "auto":
        return requested
    notify._load_env_file()
    if (os.getenv("ML_REPORT_SMTP_HOST") or "").strip() and (os.getenv("ML_REPORT_EMAIL_TO") or "").strip():
        return "email"
    if (os.getenv(notify.WEBHOOK_ENV) or "").strip():
        return "webhook"
    return "dryrun"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--channel", default=os.getenv("LEVELS_CHANNEL", "auto"),
                    choices=["auto", "email", "webhook", "dryrun"])
    ap.add_argument("--dry-run", action="store_true", help="build but do not actually send")
    args = ap.parse_args()
    mp, tr = _latest(args.symbol)
    body = render(mp, tr)
    subject = f"SPY Levels — {mp['prior_session_date']} session basis"
    channel = _resolve_channel(args.channel)
    if channel == "email":
        ok = notify.email_post(subject, body, dry_run=args.dry_run)
    elif channel == "webhook":
        ok = False if args.dry_run else notify.post(body)
        if args.dry_run:
            print(body)
    else:  # dryrun
        print(body)
        ok = False
    print(f"\n[channel={channel} {'sent' if ok else 'dry-run/not-sent'}]")


if __name__ == "__main__":
    main()
