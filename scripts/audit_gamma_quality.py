#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = os.getenv("PIVOT_DB", str(ROOT / "data" / "pivot_events.sqlite"))


@dataclass
class GateResult:
    name: str
    ok: bool
    actual: float
    threshold: float
    comparator: str

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "ok": self.ok,
            "actual": self.actual,
            "threshold": self.threshold,
            "comparator": self.comparator,
        }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit gamma snapshot + touch-event enrichment quality.")
    p.add_argument("--db", default=DEFAULT_DB, help="SQLite DB path")
    p.add_argument("--symbol", default="SPY", help="Underlying symbol (default: SPY)")
    p.add_argument("--start-date", default="", help="Inclusive YYYY-MM-DD (snapshot_date)")
    p.add_argument("--end-date", default="", help="Inclusive YYYY-MM-DD (snapshot_date)")
    p.add_argument(
        "--min-flip-pct",
        type=float,
        default=85.0,
        help="Minimum percent of snapshots with non-null gamma_flip",
    )
    p.add_argument(
        "--min-computed-days",
        type=int,
        default=150,
        help="Minimum number of snapshot days with computed_gamma_count > 0",
    )
    p.add_argument(
        "--min-touch-gamma-pct",
        type=float,
        default=25.0,
        help="Minimum percent of touch_events with non-null gamma_flip",
    )
    p.add_argument(
        "--enforce",
        action="store_true",
        help="Exit non-zero if any gate fails",
    )
    return p.parse_args()


def _date_predicate(column: str, start: str, end: str) -> tuple[str, list[object]]:
    clauses: list[str] = []
    params: list[object] = []
    if start:
        clauses.append(f"{column} >= ?")
        params.append(start)
    if end:
        clauses.append(f"{column} <= ?")
        params.append(end)
    if not clauses:
        return "", params
    return " AND " + " AND ".join(clauses), params


def _ms_epoch_date_predicate(ms_column: str, start: str, end: str) -> tuple[str, list[object]]:
    """Date predicate for columns stored as Unix millisecond timestamps."""
    clauses: list[str] = []
    params: list[object] = []
    expr = f"date({ms_column}/1000, 'unixepoch')"
    if start:
        clauses.append(f"{expr} >= ?")
        params.append(start)
    if end:
        clauses.append(f"{expr} <= ?")
        params.append(end)
    if not clauses:
        return "", params
    return " AND " + " AND ".join(clauses), params


def _single_row(conn: sqlite3.Connection, sql: str, params: list[object]) -> sqlite3.Row:
    row = conn.execute(sql, params).fetchone()
    if row is None:
        raise RuntimeError("Expected one row from aggregate query.")
    return row


def main() -> int:
    args = parse_args()
    conn = sqlite3.connect(args.db, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000;")

    snap_date_pred, snap_date_params = _date_predicate("snapshot_date", args.start_date, args.end_date)
    touch_date_pred, touch_date_params = _ms_epoch_date_predicate("ts_event", args.start_date, args.end_date)

    snap_sql = f"""
        SELECT
            COUNT(*) AS snaps,
            SUM(CASE WHEN gamma_flip IS NOT NULL THEN 1 ELSE 0 END) AS flip_nonnull,
            SUM(CASE WHEN with_greeks > 0 THEN 1 ELSE 0 END) AS provider_greeks_days,
            SUM(CASE WHEN with_iv > 0 THEN 1 ELSE 0 END) AS provider_iv_days,
            SUM(CASE WHEN CAST(json_extract(payload_json, '$.computed_gamma_count') AS INTEGER) > 0 THEN 1 ELSE 0 END) AS computed_days,
            AVG(CAST(json_extract(payload_json, '$.computed_gamma_count') AS REAL)) AS avg_computed_contracts
        FROM gamma_snapshots
        WHERE symbol = ?
        {snap_date_pred}
    """
    snap_row = _single_row(conn, snap_sql, [args.symbol.upper(), *snap_date_params])

    touch_sql = f"""
        SELECT
            COUNT(*) AS touch_rows,
            SUM(CASE WHEN gamma_flip IS NOT NULL THEN 1 ELSE 0 END) AS touch_gamma_nonnull,
            SUM(CASE WHEN gamma_confidence = 2 THEN 1 ELSE 0 END) AS conf2_rows,
            SUM(CASE WHEN gamma_confidence = 1 THEN 1 ELSE 0 END) AS conf1_rows,
            SUM(CASE WHEN gamma_confidence = 0 THEN 1 ELSE 0 END) AS conf0_rows
        FROM touch_events
        WHERE symbol = ?
        {touch_date_pred}
    """
    touch_row = _single_row(conn, touch_sql, [args.symbol.upper(), *touch_date_params])

    snaps = int(snap_row["snaps"] or 0)
    flip_nonnull = int(snap_row["flip_nonnull"] or 0)
    computed_days = int(snap_row["computed_days"] or 0)
    touch_rows = int(touch_row["touch_rows"] or 0)
    touch_gamma_nonnull = int(touch_row["touch_gamma_nonnull"] or 0)

    flip_pct = (100.0 * flip_nonnull / snaps) if snaps else 0.0
    touch_gamma_pct = (100.0 * touch_gamma_nonnull / touch_rows) if touch_rows else 0.0

    gates = [
        GateResult("flip_pct", flip_pct >= args.min_flip_pct, flip_pct, args.min_flip_pct, ">="),
        GateResult(
            "computed_days",
            float(computed_days) >= float(args.min_computed_days),
            float(computed_days),
            float(args.min_computed_days),
            ">=",
        ),
        GateResult(
            "touch_gamma_pct",
            touch_gamma_pct >= args.min_touch_gamma_pct,
            touch_gamma_pct,
            args.min_touch_gamma_pct,
            ">=",
        ),
    ]

    null_dates_sql = f"""
        SELECT snapshot_date
        FROM gamma_snapshots
        WHERE symbol = ?
          AND gamma_flip IS NULL
          {snap_date_pred}
        ORDER BY snapshot_date
        LIMIT 40
    """
    null_dates = [r[0] for r in conn.execute(null_dates_sql, [args.symbol.upper(), *snap_date_params]).fetchall()]

    payload = {
        "status": "ok",
        "db": args.db,
        "symbol": args.symbol.upper(),
        "window": {"start_date": args.start_date or None, "end_date": args.end_date or None},
        "gamma_snapshots": {
            "snaps": snaps,
            "flip_nonnull": flip_nonnull,
            "flip_pct": round(flip_pct, 3),
            "provider_greeks_days": int(snap_row["provider_greeks_days"] or 0),
            "provider_iv_days": int(snap_row["provider_iv_days"] or 0),
            "computed_days": computed_days,
            "avg_computed_contracts": round(float(snap_row["avg_computed_contracts"] or 0.0), 3),
            "sample_null_flip_dates": null_dates,
        },
        "touch_events": {
            "touch_rows": touch_rows,
            "touch_gamma_nonnull": touch_gamma_nonnull,
            "touch_gamma_pct": round(touch_gamma_pct, 3),
            "conf2_rows": int(touch_row["conf2_rows"] or 0),
            "conf1_rows": int(touch_row["conf1_rows"] or 0),
            "conf0_rows": int(touch_row["conf0_rows"] or 0),
        },
        "gates": [g.to_dict() for g in gates],
    }
    print(json.dumps(payload, indent=2))

    if args.enforce and any(not g.ok for g in gates):
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
