#!/usr/bin/env python3
"""Level-proximity alert watcher for SPY (the /levels habit loop).

Single-shot: run once per invocation (intended to be fired every ~1 min during
RTH by a launch agent). Computes today's key levels (Classic pivots + realized-vol
bands, matching the /levels page), reads the current price, and sends an alert when
price comes within a threshold of a level. De-duplicated with an armed/re-arm state
so it fires once per approach, not every minute.

Delivery is via scripts/levels_alerts/notify.py (Discord webhook if
LEVELS_PRODUCT_WEBHOOK_URL is set, otherwise a safe DRY RUN that only prints).
Nothing is sent until a webhook is configured. stdlib only.

Env:
  MARKET_PROXY            default http://127.0.0.1:3000
  ALERT_APPROACH_PCT      default 0.0015  (0.15%: fire when within this of a level)
  ALERT_REARM_PCT         default 0.0035  (0.35%: re-arm a level once price moves away)
  ALERT_WATCH             default "u1,u2,l1,l2,PP,R1,R2,S1,S2"  (levels to watch)
  LEVELS_ALERT_DRY_RUN    "1" to force dry-run even if a webhook is set
"""
from __future__ import annotations

import json
import math
import os
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))
import notify  # noqa: E402  (sibling module)

STATE_FILE = ROOT / "data" / "levels_alerts" / "state.json"
PROXY = os.environ.get("MARKET_PROXY", "http://127.0.0.1:3000")
APPROACH_PCT = float(os.environ.get("ALERT_APPROACH_PCT", "0.0015"))
REARM_PCT = float(os.environ.get("ALERT_REARM_PCT", "0.0035"))
WATCH = [s.strip() for s in os.environ.get("ALERT_WATCH", "u1,u2,l1,l2,PP,R1,R2,S1,S2").split(",") if s.strip()]
WINDOW = 20
TZ = "America/New_York"

# Pretty labels for messages
LABELS = {"u1": "+1σ band", "u2": "+2σ band", "l1": "−1σ band", "l2": "−2σ band",
          "PP": "pivot PP", "R1": "pivot R1", "R2": "pivot R2", "R3": "pivot R3",
          "S1": "pivot S1", "S2": "pivot S2", "S3": "pivot S3"}


def sample_std(values):
    n = len(values)
    if n < 2:
        return float("nan")
    mean = sum(values) / n
    return math.sqrt(sum((v - mean) ** 2 for v in values) / (n - 1))


def _et(epoch_sec):
    # Daily-bar session key. During RTH (13:30-20:00 UTC) the UTC date equals the ET
    # session date, and the watcher only runs while the market is open, so UTC date is
    # a safe session key here (no external tz dependency).
    import datetime as dt
    return dt.datetime.utcfromtimestamp(int(epoch_sec)).strftime("%Y-%m-%d")


def fetch_market():
    url = f"{PROXY}/api/market?symbol=SPY&range=1y&interval=1d"
    with urllib.request.urlopen(url, timeout=15) as resp:  # noqa: S310 (localhost proxy)
        return json.load(resp)


def market_is_open(data):
    state = str(data.get("marketState") or "").upper()
    if state == "REGULAR":
        return True
    if state in ("CLOSED", "PRE", "PREPRE", "POST", "POSTPOST"):
        return False
    # marketState missing: infer from a fresh quote on a not-complete session
    sess = data.get("session") or {}
    if sess.get("isLastSessionComplete"):
        return False
    qt = (data.get("meta") or {}).get("regularMarketTime")
    if not isinstance(qt, (int, float)):
        return False
    import time
    return (time.time() - qt) / 60.0 < 30.0


def compute_levels(data):
    candles = [c for c in (data.get("candles") or [])
               if all(isinstance(c.get(k), (int, float)) and c.get(k, 0) > 0
                      for k in ("open", "high", "low", "close")) and isinstance(c.get("time"), (int, float))]
    # de-dupe per session date, keep chronological
    seen, bars = set(), []
    for c in candles:
        d = _et(c["time"])
        if d in seen:
            continue
        seen.add(d)
        bars.append({"date": d, "high": c["high"], "low": c["low"], "close": c["close"]})
    bars.sort(key=lambda b: b["date"])
    if len(bars) < WINDOW + 2:
        return None, None
    today = _et_now()
    completed = [b for b in bars if b["date"] < today]
    prior = completed[-1] if completed else bars[-2]
    closes = [b["close"] for b in bars if b["date"] <= prior["date"]]
    logret = [math.log(closes[i]) - math.log(closes[i - 1]) for i in range(1, len(closes))]
    if len(logret) < WINDOW:
        return None, None
    sigma = sample_std(logret[-WINDOW:])
    if not math.isfinite(sigma) or sigma <= 0:
        return None, None
    a = prior["close"]
    h, low, c = prior["high"], prior["low"], prior["close"]
    pp = (h + low + c) / 3.0
    levels = {
        "u1": a * math.exp(sigma), "u2": a * math.exp(2 * sigma),
        "l1": a * math.exp(-sigma), "l2": a * math.exp(-2 * sigma),
        "PP": pp, "R1": 2 * pp - low, "R2": pp + (h - low), "R3": h + 2 * (pp - low),
        "S1": 2 * pp - h, "S2": pp - (h - low), "S3": low - 2 * (h - pp),
    }
    return {k: levels[k] for k in WATCH if k in levels}, prior["close"]


def _et_now():
    import datetime as dt
    # Approximate ET "today" for session comparison. Daily bars are keyed by UTC date
    # here; using UTC "today" keeps them consistent with compute's _et().
    return dt.datetime.utcnow().strftime("%Y-%m-%d")


def load_state():
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(STATE_FILE)


def evaluate(levels, price, lvl_state, approach=APPROACH_PCT, rearm=REARM_PCT):
    """Decide which levels to alert on, applying armed/re-arm de-duplication.

    Fires once when price enters the approach band of an armed level; re-arms the
    level once price moves beyond the re-arm band. Mutates lvl_state in place and
    returns a list of (key, message) to deliver. Pure (no I/O) so it is testable.
    """
    to_fire = []
    for key, lv in levels.items():
        dist = (price - lv) / lv
        ad = abs(dist)
        st = lvl_state.get(key, {"armed": True})
        if ad > rearm:
            st["armed"] = True
        elif ad <= approach and st.get("armed", True):
            side = "above" if dist >= 0 else "below"
            label = LABELS.get(key, key)
            msg = (f"\U0001F514 **SPY {price:.2f}** is approaching **{label}** at "
                   f"**{lv:.2f}** ({dist * 100:+.2f}%, price {side}).")
            st["armed"] = False
            to_fire.append((key, msg))
        lvl_state[key] = st
    return to_fire


def main():
    notify._load_env_file()  # source .env for the webhook/SMTP config
    try:
        data = fetch_market()
    except Exception as exc:
        print(f"[alert-watcher] market fetch failed: {exc}")
        return 0
    if not market_is_open(data):
        print("[alert-watcher] market not open — no alerts")
        return 0
    price = data.get("currentPrice")
    if not isinstance(price, (int, float)) or price <= 0:
        print("[alert-watcher] no current price")
        return 0
    levels, _prior = compute_levels(data)
    if not levels:
        print("[alert-watcher] could not compute levels (thin history)")
        return 0

    today = _et_now()
    state = load_state()
    if state.get("date") != today:
        state = {"date": today, "levels": {}}   # reset each session
    lvl_state = state["levels"]

    to_fire = evaluate(levels, price, lvl_state)
    dry = os.getenv("LEVELS_ALERT_DRY_RUN", "").strip() == "1"
    for _key, msg in to_fire:
        if dry:
            print("[DRY] " + msg)
        else:
            notify.post(msg)

    save_state(state)
    fired = [k for k, _ in to_fire]
    print(f"[alert-watcher] price {price:.2f} · checked {len(levels)} levels · fired {len(fired)}: {fired}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
