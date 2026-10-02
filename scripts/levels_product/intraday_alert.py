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
import os
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
    if not rates.get("_publishable", False):
        stale = []
        for h in (15, 30, 60):
            status = rates.get("_coverage", {}).get(h, {})
            if status.get("complete"):
                continue
            missing = int(status.get("missing_count") or 0)
            stale.append(f"{h}m missing {missing}" if missing else f"{h}m unavailable")
        odds = "unavailable (labels incomplete: " + ", ".join(stale) + ")"
    else:
        parts = []
        for h in (15, 30, 60):
            r = rates.get(h, {}).get(b, {})
            if r.get("rate") is not None:
                parts.append(f"{h}m **{int(round(r['rate']*100))}%**")
        odds = " · ".join(parts) if parts else "n/a (insufficient in-tier sample)"
    conf = f"{int(row.confluence_count)}-level confluence" if row.confluence_count >= 1 else "no confluence"
    return (f"⚡ **SPY** touching `{row.level_type}` @ {row.level_price:.2f} ({side}) — "
            f"{conf}\n   Hold rate (latest 400 labeled touches per tier): {odds}")


def _checkpoint(scon, symbol, row):
    scon.execute("INSERT OR REPLACE INTO alert_state VALUES (?,?,?)",
                 (symbol, int(row.ts_event), row.event_id))
    scon.commit()


def deliver_alerts(new, rates, scon, symbol, max_alerts, *, dry_run=False):
    """Deliver rows in order and checkpoint only confirmed deliveries.

    A failed row terminates the batch so the failed event and every later event
    remain pending for the next poll. Dry-run is a pure preview: it sends and
    checkpoints nothing.
    """
    attempted = min(len(new), max(0, max_alerts))
    if attempted == 0:
        print("delivered 0 alert(s); state unchanged (nothing processed)")
        return 0

    if dry_run:
        for row in new.iloc[:attempted].itertuples():
            print(fmt_alert(row, rates))
        print(f"previewed {attempted} alert(s); state unchanged")
        if attempted < len(new):
            print(f"hit max-alerts cap ({max_alerts}); {len(new)-attempted} deferred")
        return 0

    delivered = 0
    for row in new.iloc[:attempted].itertuples():
        try:
            confirmed = notify.post(fmt_alert(row, rates))
        except Exception as exc:  # defensive: notifier contract is non-raising
            print(f"alert delivery raised {type(exc).__name__}; "
                  f"event_id={row.event_id} pending retry")
            return 1
        if not confirmed:
            print(f"alert delivery failed; event_id={row.event_id} pending retry "
                  f"(delivered={delivered})")
            return 1
        _checkpoint(scon, symbol, row)
        delivered += 1

    print(f"delivered {delivered} alert(s); "
          f"state advanced to ts={int(new.iloc[delivered - 1].ts_event)}")
    if attempted < len(new):
        print(f"hit max-alerts cap ({max_alerts}); {len(new)-attempted} deferred to next poll")
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--dq-min", type=float, default=0.9)
    ap.add_argument("--min-confluence", type=int, default=1)
    ap.add_argument("--max-alerts", type=int, default=12, help="safety cap per poll")
    ap.add_argument("--dry-run", action="store_true",
                    help="preview alerts without webhook delivery or cursor advancement")
    args = ap.parse_args(argv)

    if not args.dry_run and not (os.getenv(notify.WEBHOOK_ENV) or "").strip():
        print(f"configuration error: {notify.WEBHOOK_ENV} is required for delivery; "
              "use --dry-run to preview without sending")
        return 2

    rcon, scon = _read_con(), _state_con()
    st = scon.execute("SELECT last_ts, last_event_id FROM alert_state WHERE symbol=?",
                      (args.symbol,)).fetchone()
    if st is None:
        mx = _max_touch(rcon, args.symbol, args.dq_min)
        if mx and args.dry_run:
            print(f"dry-run: would initialize alert state at ts={mx[0]}; state unchanged")
        elif mx:
            scon.execute("INSERT OR REPLACE INTO alert_state VALUES (?,?,?)", (args.symbol, mx[0], mx[1]))
            scon.commit()
            print(f"initialized alert state at ts={mx[0]} (no alerts on backfill)")
        else:
            print("no touches yet; nothing to initialize")
        rcon.close(); scon.close()
        return 0

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
        return 0

    rates = current_rates(rcon, args.symbol, args.dq_min)
    rc = deliver_alerts(new, rates, scon, args.symbol, args.max_alerts,
                        dry_run=args.dry_run)
    rcon.close(); scon.close()
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
