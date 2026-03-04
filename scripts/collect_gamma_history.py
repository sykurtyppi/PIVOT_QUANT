#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

MARKETDATA_APP_BASE = os.getenv("MARKETDATA_APP_BASE", "https://api.marketdata.app/v1").rstrip("/")
MARKETDATA_APP_TOKEN = os.getenv("MARKETDATA_APP_TOKEN", "").strip()
DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")
DEFAULT_SYMBOLS = os.getenv("LIVE_COLLECTOR_SYMBOLS", "SPY")
DEFAULT_RANGE_PCT = float(os.getenv("GAMMA_HISTORY_STRIKE_RANGE_PCT", "0.2"))
DEFAULT_MAX_STRIKES = int(os.getenv("GAMMA_HISTORY_MAX_STRIKES", "120"))
DEFAULT_TIMEOUT = int(os.getenv("GAMMA_HISTORY_HTTP_TIMEOUT_SEC", "60"))

try:
    from migrate_db import migrate_connection
except Exception:  # pragma: no cover
    migrate_connection = None  # type: ignore


def parse_yyyy_mm_dd(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def iter_trading_days(start_date: date, end_date: date) -> Iterable[date]:
    cur = start_date
    while cur <= end_date:
        if cur.weekday() < 5:
            yield cur
        cur += timedelta(days=1)


def fetch_marketdata_chain(
    symbol: str,
    snapshot_date: date,
    timeout_sec: int,
) -> dict:
    def _request(url: str) -> dict:
        req = Request(
            url,
            headers={
                "Authorization": f"Token {MARKETDATA_APP_TOKEN}",
                "User-Agent": "PivotQuantGammaHistory/1.0",
            },
        )
        with urlopen(req, timeout=timeout_sec) as resp:
            payload = json.loads(resp.read())
        if payload.get("s") != "ok":
            raise RuntimeError(
                f"marketdata.app chain error for {symbol} {snapshot_date}: "
                f"{payload.get('errmsg', payload.get('s', 'unknown'))}"
            )
        return payload

    params = {"date": snapshot_date.strftime("%Y-%m-%d")}
    url = f"{MARKETDATA_APP_BASE}/options/chain/{symbol.upper()}/?{urlencode(params)}"
    try:
        return _request(url)
    except HTTPError as exc:
        # Same-day date queries can fail with 400 for some accounts.
        # Fall back to the live chain endpoint so today's snapshot can still persist.
        if exc.code != 400:
            raise
        live_url = f"{MARKETDATA_APP_BASE}/options/chain/{symbol.upper()}/?expiration=all"
        return _request(live_url)


def _to_float(value) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _safe_pct(num: float, den: float) -> float | None:
    if den <= 0:
        return None
    return round((num / den) * 100.0, 4)


def summarize_chain(
    symbol: str,
    snapshot_date: date,
    chain: dict,
    strike_range_pct: float,
    max_strikes: int,
) -> dict:
    strikes = chain.get("strike") or []
    sides = chain.get("side") or []
    gammas = chain.get("gamma") or []
    ivs = chain.get("iv") or []
    ois = chain.get("openInterest") or []
    deltas = chain.get("delta") or []
    expiries = chain.get("expiration") or []
    underlyings = chain.get("underlyingPrice") or []

    if not strikes:
        raise RuntimeError(f"chain has no strike data for {symbol} {snapshot_date}")

    spot_candidates = [_to_float(v) for v in underlyings]
    spot_candidates = [v for v in spot_candidates if v is not None]
    if not spot_candidates:
        raise RuntimeError(f"chain has no underlyingPrice for {symbol} {snapshot_date}")
    spot = float(spot_candidates[0])

    low = spot * (1.0 - strike_range_pct)
    high = spot * (1.0 + strike_range_pct)

    gex_by_strike: dict[float, float] = {}
    oi_by_strike: dict[float, float] = {}
    total_contracts = 0
    with_greeks = 0
    with_iv = 0
    with_oi = 0
    oi_call = 0.0
    oi_put = 0.0
    zero_dte_oi = 0.0
    total_oi = 0.0
    iv_samples: list[tuple[float, str, float | None, float]] = []

    multiplier = 100.0
    target_date = snapshot_date

    for i in range(len(strikes)):
        strike = _to_float(strikes[i])
        if strike is None or strike < low or strike > high:
            continue

        total_contracts += 1
        side = str((sides[i] if i < len(sides) else "") or "").lower()
        gamma = _to_float(gammas[i] if i < len(gammas) else None)
        iv = _to_float(ivs[i] if i < len(ivs) else None)
        oi = _to_float(ois[i] if i < len(ois) else None)
        delta = _to_float(deltas[i] if i < len(deltas) else None)
        expiry_raw = expiries[i] if i < len(expiries) else None

        if iv is not None:
            with_iv += 1
            iv_samples.append((strike, side, delta, iv))

        if oi is not None:
            with_oi += 1
            total_oi += oi
            oi_by_strike[strike] = oi_by_strike.get(strike, 0.0) + oi
            if side == "call":
                oi_call += oi
            elif side == "put":
                oi_put += oi
            try:
                expiry_date = parse_yyyy_mm_dd(str(expiry_raw)[:10]) if expiry_raw else None
                if expiry_date == target_date:
                    zero_dte_oi += oi
            except Exception:
                pass

        if gamma is None:
            continue
        with_greeks += 1
        size = oi if oi is not None else 1.0
        gex = gamma * size * multiplier * (spot ** 2)
        if side == "put":
            gex = -gex
        gex_by_strike[strike] = gex_by_strike.get(strike, 0.0) + gex

    gamma_flip = None
    call_wall = None
    put_wall = None
    pin = None
    ordered = sorted(gex_by_strike.keys())
    if ordered:
        if len(ordered) > max_strikes:
            nearest_idx = min(range(len(ordered)), key=lambda j: abs(ordered[j] - spot))
            half = max_strikes // 2
            start = max(0, nearest_idx - half)
            end = min(len(ordered), start + max_strikes)
            keep = set(ordered[start:end])
            gex_by_strike = {k: v for k, v in gex_by_strike.items() if k in keep}
            ordered = sorted(gex_by_strike.keys())

        cumulative = 0.0
        last_sign: int | None = None
        for strike in ordered:
            cumulative += gex_by_strike[strike]
            sign = 1 if cumulative > 0 else -1 if cumulative < 0 else 0
            if last_sign is not None and sign != 0 and sign != last_sign:
                gamma_flip = strike
                break
            last_sign = sign
        if gamma_flip is None:
            gamma_flip = min(ordered, key=lambda s: abs(gex_by_strike[s]))

        call_wall = max(ordered, key=lambda s: gex_by_strike[s])
        put_wall = min(ordered, key=lambda s: gex_by_strike[s])
        pin = max(ordered, key=lambda s: abs(gex_by_strike[s]))

    atm_iv = None
    if iv_samples:
        atm_iv = min(iv_samples, key=lambda x: abs(x[0] - spot))[3]

    call_iv = None
    put_iv = None
    call_candidates = [s for s in iv_samples if s[1] == "call" and s[2] is not None]
    put_candidates = [s for s in iv_samples if s[1] == "put" and s[2] is not None]
    if call_candidates:
        call_iv = min(call_candidates, key=lambda x: abs(float(x[2]) - 0.25))[3]
    if put_candidates:
        put_iv = min(put_candidates, key=lambda x: abs(float(x[2]) + 0.25))[3]
    skew_25d = (put_iv - call_iv) if (put_iv is not None and call_iv is not None) else None

    top_oi = sum(sorted(oi_by_strike.values(), reverse=True)[:5]) if oi_by_strike else 0.0
    oi_concentration_top5 = _safe_pct(top_oi, total_oi)
    zero_dte_share = _safe_pct(zero_dte_oi, total_oi)

    return {
        "symbol": symbol.upper(),
        "snapshot_date": snapshot_date.strftime("%Y-%m-%d"),
        "source": "marketdata.app",
        "ts_collected_ms": int(time.time() * 1000),
        "spot": spot,
        "gamma_flip": gamma_flip,
        "call_wall": call_wall,
        "put_wall": put_wall,
        "pin": pin,
        "atm_iv": atm_iv,
        "skew_25d": skew_25d,
        "oi_call": oi_call if with_oi > 0 else None,
        "oi_put": oi_put if with_oi > 0 else None,
        "oi_concentration_top5": oi_concentration_top5,
        "zero_dte_share": zero_dte_share,
        "total_contracts": total_contracts,
        "with_greeks": with_greeks,
        "with_iv": with_iv,
        "with_oi": with_oi,
        "used_open_interest": 1 if with_oi > 0 else 0,
        "payload_json": json.dumps(
            {
                "symbol": symbol.upper(),
                "snapshot_date": snapshot_date.strftime("%Y-%m-%d"),
                "spot": spot,
                "contracts": len(strikes),
                "filtered_contracts": total_contracts,
            },
            separators=(",", ":"),
        ),
    }


def ensure_schema(conn: sqlite3.Connection) -> None:
    if migrate_connection is not None:
        migrate_connection(conn, verbose=False)
        return
    conn.execute(
        """CREATE TABLE IF NOT EXISTS gamma_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            snapshot_date TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'marketdata.app',
            ts_collected_ms INTEGER NOT NULL,
            spot REAL,
            gamma_flip REAL,
            call_wall REAL,
            put_wall REAL,
            pin REAL,
            atm_iv REAL,
            skew_25d REAL,
            oi_call REAL,
            oi_put REAL,
            oi_concentration_top5 REAL,
            zero_dte_share REAL,
            total_contracts INTEGER,
            with_greeks INTEGER,
            with_iv INTEGER,
            with_oi INTEGER,
            used_open_interest INTEGER DEFAULT 0,
            payload_json TEXT,
            UNIQUE(symbol, snapshot_date, source)
        );"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_gamma_snapshots_symbol_date "
        "ON gamma_snapshots(symbol, snapshot_date);"
    )
    conn.commit()


def upsert_snapshot(conn: sqlite3.Connection, snap: dict) -> None:
    conn.execute(
        """
        INSERT INTO gamma_snapshots (
            symbol, snapshot_date, source, ts_collected_ms,
            spot, gamma_flip, call_wall, put_wall, pin,
            atm_iv, skew_25d, oi_call, oi_put,
            oi_concentration_top5, zero_dte_share,
            total_contracts, with_greeks, with_iv, with_oi,
            used_open_interest, payload_json
        ) VALUES (
            :symbol, :snapshot_date, :source, :ts_collected_ms,
            :spot, :gamma_flip, :call_wall, :put_wall, :pin,
            :atm_iv, :skew_25d, :oi_call, :oi_put,
            :oi_concentration_top5, :zero_dte_share,
            :total_contracts, :with_greeks, :with_iv, :with_oi,
            :used_open_interest, :payload_json
        )
        ON CONFLICT(symbol, snapshot_date, source) DO UPDATE SET
            ts_collected_ms=excluded.ts_collected_ms,
            spot=excluded.spot,
            gamma_flip=excluded.gamma_flip,
            call_wall=excluded.call_wall,
            put_wall=excluded.put_wall,
            pin=excluded.pin,
            atm_iv=excluded.atm_iv,
            skew_25d=excluded.skew_25d,
            oi_call=excluded.oi_call,
            oi_put=excluded.oi_put,
            oi_concentration_top5=excluded.oi_concentration_top5,
            zero_dte_share=excluded.zero_dte_share,
            total_contracts=excluded.total_contracts,
            with_greeks=excluded.with_greeks,
            with_iv=excluded.with_iv,
            with_oi=excluded.with_oi,
            used_open_interest=excluded.used_open_interest,
            payload_json=excluded.payload_json
        """,
        snap,
    )


def row_exists(conn: sqlite3.Connection, symbol: str, snapshot_date: date, source: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM gamma_snapshots
        WHERE symbol = ? AND snapshot_date = ? AND source = ?
        LIMIT 1
        """,
        (symbol.upper(), snapshot_date.strftime("%Y-%m-%d"), source),
    ).fetchone()
    return row is not None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect marketdata.app gamma snapshots into SQLite.")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite DB path")
    parser.add_argument("--symbols", default=DEFAULT_SYMBOLS, help="Comma-separated symbols (default: LIVE_COLLECTOR_SYMBOLS)")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--date", help="Single snapshot date (YYYY-MM-DD)")
    parser.add_argument("--timeout-sec", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--strike-range-pct", type=float, default=DEFAULT_RANGE_PCT)
    parser.add_argument("--max-strikes", type=int, default=DEFAULT_MAX_STRIKES)
    parser.add_argument("--skip-existing", action="store_true", default=False)
    parser.add_argument("--dry-run", action="store_true", default=False)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not MARKETDATA_APP_TOKEN:
        raise SystemExit("MARKETDATA_APP_TOKEN is required")

    if args.date:
        start_date = parse_yyyy_mm_dd(args.date)
        end_date = start_date
    else:
        today = datetime.now(timezone.utc).date()
        start_date = parse_yyyy_mm_dd(args.start_date) if args.start_date else today
        end_date = parse_yyyy_mm_dd(args.end_date) if args.end_date else today
    if end_date < start_date:
        raise SystemExit("end-date must be >= start-date")

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        raise SystemExit("No symbols provided")

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        ensure_schema(conn)

        inserted = 0
        skipped = 0
        failed = 0
        attempted = 0

        for d in iter_trading_days(start_date, end_date):
            for symbol in symbols:
                attempted += 1
                if args.skip_existing and row_exists(conn, symbol, d, "marketdata.app"):
                    skipped += 1
                    continue
                try:
                    chain = fetch_marketdata_chain(symbol, d, timeout_sec=args.timeout_sec)
                    snap = summarize_chain(
                        symbol=symbol,
                        snapshot_date=d,
                        chain=chain,
                        strike_range_pct=args.strike_range_pct,
                        max_strikes=args.max_strikes,
                    )
                    if args.dry_run:
                        print(
                            json.dumps(
                                {
                                    "symbol": snap["symbol"],
                                    "snapshot_date": snap["snapshot_date"],
                                    "spot": snap["spot"],
                                    "gamma_flip": snap["gamma_flip"],
                                    "oi_concentration_top5": snap["oi_concentration_top5"],
                                    "zero_dte_share": snap["zero_dte_share"],
                                    "with_oi": snap["with_oi"],
                                },
                                separators=(",", ":"),
                            )
                        )
                    else:
                        upsert_snapshot(conn, snap)
                        inserted += 1
                except Exception as exc:
                    failed += 1
                    print(f"[WARN] {symbol} {d}: {exc}", file=sys.stderr)
            if not args.dry_run:
                conn.commit()

        summary = {
            "status": "ok",
            "db": str(db_path),
            "symbols": symbols,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "attempted": attempted,
            "inserted_or_updated": inserted,
            "skipped_existing": skipped,
            "failed": failed,
            "dry_run": bool(args.dry_run),
        }
        print(json.dumps(summary, indent=2))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
