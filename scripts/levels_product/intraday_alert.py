#!/usr/bin/env python3
"""Phase-1: intraday confluence ALERT hook (the differentiated, validated signal).

Polls the live DB (READ-ONLY) for newly-logged conf≥1 SPY touches and emits a
P(hold) alert for each — the edge that only exists at the moment of the touch.
The hold probability is the live, matured-only trailing bucket rate
(hold_engine.current_rates), so it is leak-free.

State (last processed touch) lives in the PRODUCT DB. On first run it INITIALIZES
to the current max touch without alerting, so it never spam-fires on the
historical backfill — only on genuinely new touches going forward.

    python scripts/levels_product/intraday_alert.py        # poll once
Designed to be run on a short interval (e.g. every 1-2 min during RTH) by launchd.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "levels_product"))
import notify  # noqa: E402
from hold_engine import bucket, current_rates  # noqa: E402

READ_DB = REPO / "data" / "pivot_events.sqlite"
PRODUCT_DB = REPO / "data" / "levels_product.sqlite"


def _read_con():
    return sqlite3.connect(f"file:{READ_DB}?mode=ro", uri=True)


def _state_con():
    con = sqlite3.connect(PRODUCT_DB)
    con.execute("""CREATE TABLE IF NOT EXISTS alert_state (
        symbol TEXT PRIMARY KEY, last_ts INTEGER NOT NULL, last_event_id TEXT NOT NULL)""")
    con.commit()
    return con


def _max_touch(rcon, symbol, dq_min):
    row = rcon.execute(
        """SELECT ts_event, event_id FROM touch_events
           WHERE symbol=? AND data_quality>=? AND confluence_count IS NOT NULL
           ORDER BY ts_event DESC, event_id DESC LIMIT 1""",
        (symbol, dq_min)).fetchone()
    return (int(row[0]), row[1]) if row else None


def fmt_alert(row, rates):
    b = str(bucket(row.confluence_count))
    # touch_side semantics (scripts/build_labels.py): +1 = price above the level,
    # rejecting upward -> the level is acting as SUPPORT; -1 = price below,
    # rejecting downward -> RESISTANCE.
    side = "support" if row.touch_side == 1 else "resistance" if row.touch_side == -1 else "level"
    parts = []
    for h in (15, 30, 60):
        r = rates.get(h, {}).get(b, {})
        if r.get("rate") is not None:
            parts.append(f"{h}m **{int(round(r['rate']*100))}%**")
    odds = " · ".join(parts) if parts else "n/a (insufficient history)"
    conf = f"{int(row.confluence_count)}-level confluence" if row.confluence_count >= 1 else "no confluence"
    # quote the tier's all-history base rate; the morning post's "last 30d" number
    # is a separate recent-window view (labeled as such) — distinct on purpose.
    return (f"⚡ **SPY** touching `{row.level_type}` @ {row.level_price:.2f} ({side}) — "
            f"{conf}\n   Hold rate (this tier, full history): {odds}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--dq-min", type=float, default=0.9)
    ap.add_argument("--min-confluence", type=int, default=1)
    ap.add_argument("--max-alerts", type=int, default=12, help="safety cap per poll")
    args = ap.parse_args()

    rcon, scon = _read_con(), _state_con()
    st = scon.execute("SELECT last_ts, last_event_id FROM alert_state WHERE symbol=?",
                      (args.symbol,)).fetchone()
    if st is None:
        mx = _max_touch(rcon, args.symbol, args.dq_min)
        if mx:
            scon.execute("INSERT OR REPLACE INTO alert_state VALUES (?,?,?)", (args.symbol, mx[0], mx[1]))
            scon.commit()
            print(f"initialized alert state at ts={mx[0]} (no alerts on backfill)")
        else:
            print("no touches yet; nothing to initialize")
        rcon.close(); scon.close()
        return

    last_ts, last_id = int(st[0]), st[1]
    new = pd.read_sql_query(
        """SELECT ts_event, event_id, level_type, level_price, touch_side, confluence_count
           FROM touch_events
           WHERE symbol=? AND data_quality>=? AND confluence_count>=?
                 AND (ts_event > ? OR (ts_event = ? AND event_id > ?))
           ORDER BY ts_event, event_id""",
        rcon, params=(args.symbol, args.dq_min, args.min_confluence, last_ts, last_ts, last_id))
    if new.empty:
        print("no new confluence touches")
        rcon.close(); scon.close()
        return

    rates = current_rates(rcon, args.symbol, args.dq_min)
    fired = 0
    for row in new.itertuples():
        if fired >= args.max_alerts:
            print(f"hit max-alerts cap ({args.max_alerts}); {len(new)-fired} deferred to next poll")
            break
        notify.post(fmt_alert(row, rates))
        fired += 1
    # advance state ONLY past touches we actually processed. If nothing fired
    # (e.g. --max-alerts<=0), leave state untouched so no touch is silently
    # skipped — fired-1 is the last fired row (fired>0 guarantees it's valid).
    if fired == 0:
        print("fired 0 alert(s); state unchanged (nothing processed)")
    else:
        processed = new.iloc[fired - 1]
        scon.execute("INSERT OR REPLACE INTO alert_state VALUES (?,?,?)",
                     (args.symbol, int(processed.ts_event), processed.event_id))
        scon.commit()
        print(f"fired {fired} alert(s); state advanced to ts={int(processed.ts_event)}")
    rcon.close(); scon.close()


if __name__ == "__main__":
    main()
