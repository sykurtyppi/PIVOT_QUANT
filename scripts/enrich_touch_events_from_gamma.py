#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")
IV_RV_HIGH_RATIO = float(os.getenv("GAMMA_IV_RV_HIGH_RATIO", "1.15"))
IV_RV_LOW_RATIO = float(os.getenv("GAMMA_IV_RV_LOW_RATIO", "0.85"))
CARRY_MAX_DAYS = int(os.getenv("GAMMA_CONTEXT_CARRY_MAX_DAYS", "1"))
CARRY_CONFIDENCE_DECAY_PER_DAY = int(os.getenv("GAMMA_CONTEXT_CARRY_CONFIDENCE_DECAY_PER_DAY", "20"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fill touch_events gamma/IV/OI fields from gamma_snapshots.")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite DB path")
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbols filter")
    parser.add_argument("--start-date", default="", help="Optional inclusive date filter (YYYY-MM-DD, UTC date)")
    parser.add_argument("--end-date", default="", help="Optional inclusive date filter (YYYY-MM-DD, UTC date)")
    parser.add_argument("--overwrite", action="store_true", default=False, help="Overwrite existing populated values")
    parser.add_argument("--dry-run", action="store_true", default=False, help="Do not write changes")
    return parser.parse_args()


def _to_float(value) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _to_int(value) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _gamma_confidence(
    gamma_flip: float | None,
    total_contracts: int | None,
    with_greeks: int | None,
    with_oi: int | None,
    used_open_interest: int | None,
) -> int | None:
    if gamma_flip is None:
        return None
    tc = max(0, total_contracts or 0)
    if tc == 0:
        return 1
    greek_cov = (with_greeks or 0) / tc
    oi_cov = (with_oi or 0) / tc
    if (used_open_interest or 0) and greek_cov >= 0.80 and oi_cov >= 0.60:
        return 3
    if greek_cov >= 0.50:
        return 2
    return 1


def _iv_rv_state(atm_iv: float | None, rv_30: float | None) -> int | None:
    if atm_iv is None or rv_30 is None or rv_30 <= 0:
        return None
    iv_pct = atm_iv * 100.0
    ratio = iv_pct / rv_30
    if ratio >= IV_RV_HIGH_RATIO:
        return 1
    if ratio <= IV_RV_LOW_RATIO:
        return -1
    return 0


def _maybe_set(current, new_value, overwrite: bool):
    # Even with overwrite enabled, keep existing values when replacement is null.
    if new_value is None:
        return current
    if overwrite:
        return new_value
    if current is None:
        return new_value
    return current


def _to_date(raw: str | None):
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        return None


def _carry_snapshot_row(
    conn: sqlite3.Connection,
    symbol: str,
    event_date: str,
    max_age_days: int,
) -> sqlite3.Row | None:
    d = _to_date(event_date)
    if d is None:
        return None
    lower = (d - timedelta(days=max_age_days)).strftime("%Y-%m-%d")
    return conn.execute(
        """
        SELECT
            snapshot_date,
            gamma_flip,
            atm_iv,
            oi_concentration_top5,
            zero_dte_share,
            total_contracts,
            with_greeks,
            with_oi,
            used_open_interest
        FROM gamma_snapshots
        WHERE symbol = ?
          AND snapshot_date < ?
          AND snapshot_date >= ?
          AND (gamma_flip IS NOT NULL OR atm_iv IS NOT NULL)
        ORDER BY snapshot_date DESC, ts_collected_ms DESC
        LIMIT 1
        """,
        (symbol, event_date, lower),
    ).fetchone()


def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "touch_events" not in tables:
            raise SystemExit("touch_events table not found")
        if "gamma_snapshots" not in tables:
            print(
                json.dumps(
                    {
                        "status": "no_data",
                        "db": str(db_path),
                        "message": "gamma_snapshots table not found; run migrate_db + collect_gamma_history first",
                    },
                    indent=2,
                )
            )
            return

        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

        where = []
        params: list[object] = []
        if symbols:
            where.append("te.symbol IN ({})".format(",".join(["?"] * len(symbols))))
            params.extend(symbols)
        if args.start_date:
            datetime.strptime(args.start_date, "%Y-%m-%d")
            where.append("date(te.ts_event/1000,'unixepoch') >= ?")
            params.append(args.start_date)
        if args.end_date:
            datetime.strptime(args.end_date, "%Y-%m-%d")
            where.append("date(te.ts_event/1000,'unixepoch') <= ?")
            params.append(args.end_date)
        where_clause = ("WHERE " + " AND ".join(where)) if where else ""

        rows = conn.execute(
            f"""
            SELECT
                te.rowid AS row_id,
                te.symbol,
                date(te.ts_event/1000,'unixepoch') AS event_date,
                te.touch_price,
                te.rv_30,
                te.gamma_flip AS cur_gamma_flip,
                te.gamma_mode AS cur_gamma_mode,
                te.gamma_flip_dist_bps AS cur_gamma_flip_dist_bps,
                te.gamma_confidence AS cur_gamma_confidence,
                te.oi_concentration_top5 AS cur_oi_concentration_top5,
                te.zero_dte_share AS cur_zero_dte_share,
                te.iv_rv_state AS cur_iv_rv_state,
                te.data_quality AS cur_data_quality,
                gs.gamma_flip AS snap_gamma_flip,
                gs.oi_concentration_top5 AS snap_oi_concentration_top5,
                gs.zero_dte_share AS snap_zero_dte_share,
                gs.atm_iv AS snap_atm_iv,
                gs.total_contracts AS snap_total_contracts,
                gs.with_greeks AS snap_with_greeks,
                gs.with_oi AS snap_with_oi,
                gs.used_open_interest AS snap_used_open_interest
            FROM touch_events te
            LEFT JOIN gamma_snapshots gs
              ON gs.rowid = (
                  SELECT gs2.rowid
                  FROM gamma_snapshots gs2
                  WHERE gs2.symbol = te.symbol
                    AND gs2.snapshot_date = date(te.ts_event/1000,'unixepoch')
                  ORDER BY gs2.ts_collected_ms DESC
                  LIMIT 1
              )
            {where_clause}
            """
            ,
            params,
        ).fetchall()

        updates: list[tuple] = []
        touched = 0
        unchanged = 0
        carry_cache: dict[tuple[str, str], sqlite3.Row | None] = {}

        for row in rows:
            symbol = str(row["symbol"] or "").upper()
            event_date = str(row["event_date"] or "")
            event_day = _to_date(event_date)
            touch_price = _to_float(row["touch_price"])
            rv_30 = _to_float(row["rv_30"])
            snap_gamma_flip = _to_float(row["snap_gamma_flip"])
            snap_oi = _to_float(row["snap_oi_concentration_top5"])
            snap_zero_dte = _to_float(row["snap_zero_dte_share"])
            snap_atm_iv = _to_float(row["snap_atm_iv"])
            snap_total = _to_int(row["snap_total_contracts"])
            snap_with_greeks = _to_int(row["snap_with_greeks"])
            snap_with_oi = _to_int(row["snap_with_oi"])
            snap_used_oi = _to_int(row["snap_used_open_interest"])

            effective_gamma_flip = snap_gamma_flip
            effective_atm_iv = snap_atm_iv
            effective_oi = snap_oi
            effective_zero_dte = snap_zero_dte
            confidence_total = snap_total
            confidence_with_greeks = snap_with_greeks
            confidence_with_oi = snap_with_oi
            confidence_used_oi = snap_used_oi
            gamma_from_carry = False
            carry_row: sqlite3.Row | None = None
            if CARRY_MAX_DAYS >= 0 and (effective_gamma_flip is None or effective_atm_iv is None):
                carry_key = (symbol, event_date)
                if carry_key not in carry_cache:
                    carry_cache[carry_key] = _carry_snapshot_row(
                        conn=conn,
                        symbol=symbol,
                        event_date=event_date,
                        max_age_days=CARRY_MAX_DAYS,
                    )
                carry_row = carry_cache[carry_key]
                if carry_row is not None:
                    carry_gamma = _to_float(carry_row["gamma_flip"])
                    carry_atm_iv = _to_float(carry_row["atm_iv"])
                    carry_oi = _to_float(carry_row["oi_concentration_top5"])
                    carry_zero_dte = _to_float(carry_row["zero_dte_share"])
                    carry_total = _to_int(carry_row["total_contracts"])
                    carry_with_greeks = _to_int(carry_row["with_greeks"])
                    carry_with_oi = _to_int(carry_row["with_oi"])
                    carry_used_oi = _to_int(carry_row["used_open_interest"])

                    if effective_gamma_flip is None and carry_gamma is not None:
                        effective_gamma_flip = carry_gamma
                        confidence_total = carry_total
                        confidence_with_greeks = carry_with_greeks
                        confidence_with_oi = carry_with_oi
                        confidence_used_oi = carry_used_oi
                        gamma_from_carry = True
                    if effective_atm_iv is None and carry_atm_iv is not None:
                        effective_atm_iv = carry_atm_iv
                    if effective_oi is None and carry_oi is not None:
                        effective_oi = carry_oi
                    if effective_zero_dte is None and carry_zero_dte is not None:
                        effective_zero_dte = carry_zero_dte

            new_gamma_mode = None
            new_gamma_flip_dist_bps = None
            if effective_gamma_flip is not None and effective_gamma_flip != 0 and touch_price is not None:
                new_gamma_mode = 1 if touch_price >= effective_gamma_flip else -1
                new_gamma_flip_dist_bps = (touch_price - effective_gamma_flip) / effective_gamma_flip * 1e4

            coverage = None
            if (snap_total or 0) > 0 and (snap_with_greeks is not None):
                coverage = float(snap_with_greeks) / float(snap_total)
            elif gamma_from_carry and (confidence_total or 0) > 0 and (confidence_with_greeks is not None):
                coverage = float(confidence_with_greeks) / float(confidence_total)

            gamma_confidence = _gamma_confidence(
                effective_gamma_flip,
                confidence_total,
                confidence_with_greeks,
                confidence_with_oi,
                confidence_used_oi,
            )
            if gamma_from_carry and gamma_confidence is not None and carry_row is not None and event_day is not None:
                carry_day = _to_date(carry_row["snapshot_date"])
                if carry_day is not None:
                    carry_age_days = max(0, (event_day - carry_day).days)
                    gamma_confidence = max(
                        0,
                        gamma_confidence - (carry_age_days * CARRY_CONFIDENCE_DECAY_PER_DAY),
                    )

            new_values = {
                "gamma_flip": _maybe_set(_to_float(row["cur_gamma_flip"]), effective_gamma_flip, args.overwrite),
                "gamma_mode": _maybe_set(_to_int(row["cur_gamma_mode"]), new_gamma_mode, args.overwrite),
                "gamma_flip_dist_bps": _maybe_set(
                    _to_float(row["cur_gamma_flip_dist_bps"]), new_gamma_flip_dist_bps, args.overwrite
                ),
                "gamma_confidence": _maybe_set(
                    _to_int(row["cur_gamma_confidence"]),
                    gamma_confidence,
                    args.overwrite,
                ),
                "oi_concentration_top5": _maybe_set(
                    _to_float(row["cur_oi_concentration_top5"]), effective_oi, args.overwrite
                ),
                "zero_dte_share": _maybe_set(_to_float(row["cur_zero_dte_share"]), effective_zero_dte, args.overwrite),
                "iv_rv_state": _maybe_set(
                    _to_int(row["cur_iv_rv_state"]),
                    _iv_rv_state(effective_atm_iv, rv_30),
                    args.overwrite,
                ),
                "data_quality": _maybe_set(_to_float(row["cur_data_quality"]), coverage, args.overwrite),
            }

            current_values = {
                "gamma_flip": _to_float(row["cur_gamma_flip"]),
                "gamma_mode": _to_int(row["cur_gamma_mode"]),
                "gamma_flip_dist_bps": _to_float(row["cur_gamma_flip_dist_bps"]),
                "gamma_confidence": _to_int(row["cur_gamma_confidence"]),
                "oi_concentration_top5": _to_float(row["cur_oi_concentration_top5"]),
                "zero_dte_share": _to_float(row["cur_zero_dte_share"]),
                "iv_rv_state": _to_int(row["cur_iv_rv_state"]),
                "data_quality": _to_float(row["cur_data_quality"]),
            }

            changed = any(
                (
                    (current_values[k] is None and new_values[k] is not None)
                    or (current_values[k] is not None and new_values[k] != current_values[k])
                )
                for k in new_values
            )
            if not changed:
                unchanged += 1
                continue

            touched += 1
            updates.append(
                (
                    new_values["gamma_flip"],
                    new_values["gamma_mode"],
                    new_values["gamma_flip_dist_bps"],
                    new_values["gamma_confidence"],
                    new_values["oi_concentration_top5"],
                    new_values["zero_dte_share"],
                    new_values["iv_rv_state"],
                    new_values["data_quality"],
                    row["row_id"],
                )
            )

        if updates and not args.dry_run:
            conn.executemany(
                """
                UPDATE touch_events
                SET
                    gamma_flip = ?,
                    gamma_mode = ?,
                    gamma_flip_dist_bps = ?,
                    gamma_confidence = ?,
                    oi_concentration_top5 = ?,
                    zero_dte_share = ?,
                    iv_rv_state = ?,
                    data_quality = ?
                WHERE rowid = ?
                """,
                updates,
            )
            conn.commit()

        print(
            json.dumps(
                {
                    "status": "ok",
                    "db": str(db_path),
                    "matched_rows": len(rows),
                    "updated_rows": touched,
                    "unchanged_rows": unchanged,
                    "dry_run": bool(args.dry_run),
                    "overwrite": bool(args.overwrite),
                    "symbols": symbols,
                    "start_date": args.start_date or None,
                    "end_date": args.end_date or None,
                },
                indent=2,
            )
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
