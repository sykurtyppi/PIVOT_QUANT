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
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "levels_product"))
import notify  # noqa: E402
from hold_engine import bucket, current_rates  # noqa: E402
from level_snapshot import load_snapshot, validate_snapshot  # noqa: E402

READ_DB = REPO / "data" / "pivot_events.sqlite"
PRODUCT_DB = REPO / "data" / "levels_product.sqlite"
ART = REPO / "evidence" / "levels_product"
ET = ZoneInfo("America/New_York")


def _read_con():
    return sqlite3.connect(f"file:{READ_DB}?mode=ro", uri=True)


def _state_con():
    con = sqlite3.connect(PRODUCT_DB)
    _ensure_state_schema(con)
    return con


def _ensure_state_schema(con):
    con.execute("""CREATE TABLE IF NOT EXISTS alert_state (
        symbol TEXT PRIMARY KEY, last_ts INTEGER NOT NULL, last_event_id TEXT NOT NULL)""")
    con.execute("""CREATE TABLE IF NOT EXISTS alert_event_dispositions (
        symbol TEXT NOT NULL,
        event_id TEXT NOT NULL,
        ts_event INTEGER NOT NULL,
        session_date TEXT NOT NULL,
        snapshot_id TEXT,
        level_id TEXT,
        disposition TEXT NOT NULL,
        detail TEXT,
        created_at_utc TEXT NOT NULL,
        updated_at_utc TEXT NOT NULL,
        PRIMARY KEY (symbol, event_id))""")
    con.commit()


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
    reference = ""
    if getattr(row, "snapshot_id", None) and getattr(row, "level_id", None):
        reference = f"\n   Ref: {row.snapshot_id} / {row.level_id}"
    return (f"⚡ **SPY** touching `{row.level_type}` @ {row.level_price:.2f} ({side}) — "
            f"{conf}\n   Hold rate (latest 400 labeled touches per tier): {odds}{reference}")


def bind_snapshot_identity(row, snapshot):
    """Attach canonical snapshot/level IDs or fail closed on calculation drift."""
    validate_snapshot(snapshot)
    matches = [level for level in snapshot["levels"] if level["label"] == row.level_type]
    if len(matches) != 1 or abs(float(matches[0]["price"]) - float(row.level_price)) > 0.011:
        raise ValueError(
            f"touch {row.event_id} level {row.level_type}@{row.level_price:.4f} "
            f"does not match snapshot {snapshot['snapshot_id']}"
        )
    values = row._asdict() if hasattr(row, "_asdict") else vars(row)
    return SimpleNamespace(
        **values,
        snapshot_id=snapshot["snapshot_id"],
        level_id=matches[0]["level_id"],
    )


def _snapshot_for_event(symbol, ts_event, art_dir=ART):
    session_date = datetime.fromtimestamp(int(ts_event) / 1000, tz=ET).date().isoformat()
    path = art_dir / f"levels_snapshot_{symbol}_{session_date}.json"
    if not path.exists():
        raise ValueError(f"missing canonical levels snapshot for {symbol} session {session_date}")
    snapshot = load_snapshot(path)
    if snapshot["symbol"] != symbol or snapshot["session_date"] != session_date:
        raise ValueError(f"snapshot identity mismatch for {symbol} session {session_date}")
    return snapshot


def bind_snapshot_rows(new, symbol, art_dir=ART):
    """Bind every pending touch before any delivery or checkpoint mutation."""
    cache = {}
    bound = []
    for row in new.itertuples(index=False):
        session_date = datetime.fromtimestamp(int(row.ts_event) / 1000, tz=ET).date().isoformat()
        if session_date not in cache:
            cache[session_date] = _snapshot_for_event(symbol, row.ts_event, art_dir)
        bound.append(vars(bind_snapshot_identity(row, cache[session_date])))
    return pd.DataFrame(bound)


def _utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _session_date(ts_event):
    return datetime.fromtimestamp(int(ts_event) / 1000, tz=ET).date().isoformat()


def _saved_disposition(scon, symbol, event_id):
    return scon.execute(
        """SELECT snapshot_id, level_id, disposition, detail, session_date
           FROM alert_event_dispositions WHERE symbol=? AND event_id=?""",
        (symbol, event_id),
    ).fetchone()


def _record_disposition(scon, symbol, row, *, disposition, snapshot_id=None,
                        level_id=None, detail=None):
    now = _utc_now()
    scon.execute(
        """INSERT INTO alert_event_dispositions (
               symbol, event_id, ts_event, session_date, snapshot_id, level_id,
               disposition, detail, created_at_utc, updated_at_utc)
           VALUES (?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(symbol, event_id) DO UPDATE SET
               snapshot_id=excluded.snapshot_id,
               level_id=excluded.level_id,
               disposition=excluded.disposition,
               detail=excluded.detail,
               updated_at_utc=excluded.updated_at_utc""",
        (symbol, row.event_id, int(row.ts_event), _session_date(row.ts_event),
         snapshot_id, level_id, disposition, detail, now, now),
    )
    scon.commit()


def resolve_pending_rows(new, symbol, scon, persist=True, art_dir=ART):
    """Resolve rows once, then reuse their durable snapshot binding on retries.

    Returns the valid ordered prefix and the first contract-mismatch row, if any.
    The mismatch is durably recorded before it can be checkpointed as terminal.
    """
    cache = {}
    bound = []
    for row in new.itertuples(index=False):
        saved = _saved_disposition(scon, symbol, row.event_id)
        if saved:
            snapshot_id, level_id, disposition, detail, session_date = saved
            if disposition == "snapshot_mismatch":
                return pd.DataFrame(bound), SimpleNamespace(
                    **row._asdict(), session_date=session_date,
                    snapshot_id=snapshot_id, detail=detail,
                )
            if disposition in {"pending", "delivered"} and snapshot_id and level_id:
                bound.append({**row._asdict(), "snapshot_id": snapshot_id, "level_id": level_id})
                continue
            raise ValueError(
                f"unsupported saved disposition {disposition!r} for event {row.event_id}"
            )

        session_date = _session_date(row.ts_event)
        try:
            if session_date not in cache:
                cache[session_date] = _snapshot_for_event(symbol, row.ts_event, art_dir)
            snapshot = cache[session_date]
            resolved = bind_snapshot_identity(row, snapshot)
        except ValueError as exc:
            if persist:
                _record_disposition(
                    scon, symbol, row, disposition="snapshot_mismatch",
                    snapshot_id=(cache.get(session_date) or {}).get("snapshot_id"),
                    detail=str(exc),
                )
            return pd.DataFrame(bound), SimpleNamespace(
                **row._asdict(), session_date=session_date,
                snapshot_id=(cache.get(session_date) or {}).get("snapshot_id"),
                detail=str(exc),
            )

        values = vars(resolved)
        if persist:
            _record_disposition(
                scon, symbol, row, disposition="pending",
                snapshot_id=resolved.snapshot_id, level_id=resolved.level_id,
            )
        bound.append(values)
    return pd.DataFrame(bound), None


def _checkpoint(scon, symbol, row):
    _ensure_state_schema(scon)
    scon.execute("INSERT OR REPLACE INTO alert_state VALUES (?,?,?)",
                 (symbol, int(row.ts_event), row.event_id))
    scon.execute(
        """UPDATE alert_event_dispositions
           SET disposition='delivered', detail=NULL, updated_at_utc=?
           WHERE symbol=? AND event_id=?""",
        (_utc_now(), symbol, row.event_id),
    )
    scon.commit()


def _checkpoint_mismatch(scon, symbol, row):
    """Advance only after the rejected event has a durable audit disposition."""
    saved = _saved_disposition(scon, symbol, row.event_id)
    if not saved or saved[2] != "snapshot_mismatch":
        raise ValueError(f"event {row.event_id} lacks a snapshot_mismatch disposition")
    scon.execute("INSERT OR REPLACE INTO alert_state VALUES (?,?,?)",
                 (symbol, int(row.ts_event), row.event_id))
    scon.commit()


def deliver_alerts(new, rates, scon, symbol, max_alerts, *, dry_run=False, pending_count=None):
    """Deliver rows in order and checkpoint only confirmed deliveries.

    A failed row terminates the batch so the failed event and every later event
    remain pending for the next poll. Dry-run is a pure preview: it sends and
    checkpoints nothing.
    """
    total_pending = len(new) if pending_count is None else pending_count
    attempted = min(len(new), max(0, max_alerts))
    deferred = max(0, total_pending - attempted)
    if attempted == 0:
        print("delivered 0 alert(s); state unchanged (nothing processed)")
        return 0

    if dry_run:
        for row in new.iloc[:attempted].itertuples():
            print(fmt_alert(row, rates))
        print(f"previewed {attempted} alert(s); state unchanged")
        if deferred:
            print(f"hit max-alerts cap ({max_alerts}); {deferred} deferred")
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
    if deferred:
        print(f"hit max-alerts cap ({max_alerts}); {deferred} deferred to next poll")
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
    _ensure_state_schema(scon)
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

    pending_count = len(new)
    bounded = new.iloc[:max(0, args.max_alerts)].copy()
    try:
        bounded, mismatch = resolve_pending_rows(
            bounded, args.symbol, scon, not args.dry_run
        )
    except ValueError as exc:
        print(f"levels snapshot disposition error: {exc}; state unchanged")
        rcon.close(); scon.close()
        return 3

    if mismatch is not None and bounded.empty:
        if args.dry_run:
            print(f"levels snapshot contract error: {mismatch.detail}; state unchanged")
            rcon.close(); scon.close()
            return 3
        _checkpoint_mismatch(scon, args.symbol, mismatch)
        print(f"levels snapshot contract error: {mismatch.detail}; "
              f"recorded snapshot_mismatch for event_id={mismatch.event_id}")
        rcon.close(); scon.close()
        return 3

    rates = current_rates(rcon, args.symbol, args.dq_min)
    rc = deliver_alerts(bounded, rates, scon, args.symbol, args.max_alerts,
                        dry_run=args.dry_run,
                        pending_count=(len(bounded) if mismatch is not None else pending_count))
    if rc == 0 and mismatch is not None:
        if args.dry_run:
            print(f"levels snapshot contract error: {mismatch.detail}; state unchanged")
        else:
            _checkpoint_mismatch(scon, args.symbol, mismatch)
            print(f"levels snapshot contract error: {mismatch.detail}; "
                  f"recorded snapshot_mismatch for event_id={mismatch.event_id}")
        rc = 3
    rcon.close(); scon.close()
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
