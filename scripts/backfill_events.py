#!/usr/bin/env python3
import argparse
import json
import math
import os
import sqlite3
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterable
from urllib.request import Request, urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")
NY_TZ = ZoneInfo("America/New_York") if ZoneInfo else timezone.utc


def now_ms() -> int:
    return int(time.time() * 1000)


def ensure_bar_schema(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(bar_data)")
    cols = {row[1] for row in cur.fetchall()}
    if "bar_interval_sec" not in cols:
        conn.execute("ALTER TABLE bar_data ADD COLUMN bar_interval_sec INTEGER")
        conn.commit()


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS touch_events (
            event_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            ts_event INTEGER NOT NULL,
            session TEXT,
            level_type TEXT NOT NULL,
            level_price REAL NOT NULL,
            touch_price REAL NOT NULL,
            touch_side INTEGER,
            distance_bps REAL NOT NULL,
            is_first_touch_today INTEGER DEFAULT 0,
            touch_count_today INTEGER DEFAULT 1,
            confluence_count INTEGER DEFAULT 0,
            confluence_types TEXT,
            ema9 REAL,
            ema21 REAL,
            ema_state INTEGER,
            vwap REAL,
            vwap_dist_bps REAL,
            atr REAL,
            rv_30 REAL,
            rv_regime INTEGER,
            iv_rv_state INTEGER,
            gamma_mode INTEGER,
            gamma_flip REAL,
            gamma_flip_dist_bps REAL,
            gamma_confidence INTEGER,
            oi_concentration_top5 REAL,
            zero_dte_share REAL,
            data_quality REAL,
            bar_interval_sec INTEGER,
            source TEXT,
            created_at INTEGER NOT NULL
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bar_data (
            symbol TEXT NOT NULL,
            ts INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL,
            bar_interval_sec INTEGER,
            PRIMARY KEY (symbol, ts)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS event_labels (
            event_id TEXT NOT NULL,
            horizon_min INTEGER NOT NULL,
            return_bps REAL,
            mfe_bps REAL,
            mae_bps REAL,
            reject INTEGER,
            break INTEGER,
            resolution_min REAL,
            PRIMARY KEY (event_id, horizon_min),
            FOREIGN KEY (event_id) REFERENCES touch_events(event_id)
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_touch_symbol_ts ON touch_events(symbol, ts_event);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_touch_level_ts ON touch_events(level_type, ts_event);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bar_symbol_ts ON bar_data(symbol, ts);")
    conn.commit()


def fetch_json(url: str, timeout: int = 12) -> dict:
    req = Request(url, headers={"User-Agent": "PivotQuantBackfill/1.0"})
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return json.loads(raw.decode("utf-8"))


def fetch_market(symbol: str, interval: str, range_str: str, source: str) -> tuple[dict, str]:
    if source == "ibkr":
        url = f"http://127.0.0.1:5001/market?symbol={symbol}&range={range_str}&interval={interval}"
        return fetch_json(url), "IBKR"
    if source == "yahoo":
        url = (
            "http://127.0.0.1:3000/api/market"
            f"?source=yahoo&symbol={symbol}&range={range_str}&interval={interval}"
        )
        return fetch_json(url), "Yahoo"

    # auto
    try:
        data, src = fetch_market(symbol, interval, range_str, "ibkr")
        candles = data.get("candles") or []
        if candles:
            return data, src
    except Exception:
        pass

    data, src = fetch_market(symbol, interval, range_str, "yahoo")
    return data, src


def normalize_range_for_source(interval: str, range_str: str, source: str) -> str:
    if source not in ("yahoo", "auto"):
        return range_str
    if interval == "1m" and range_str not in ("1d", "5d", "7d"):
        print("Yahoo 1m data limited to ~7d. Clamping range to 7d.")
        return "7d"
    return range_str


def parse_candles(payload: dict) -> list[dict]:
    candles = payload.get("candles") or []
    normalized = []
    for bar in candles:
        try:
            ts = int(bar["time"])
            normalized.append(
                {
                    "time": ts,
                    "open": float(bar["open"]),
                    "high": float(bar["high"]),
                    "low": float(bar["low"]),
                    "close": float(bar["close"]),
                    "volume": float(bar.get("volume", 0) or 0),
                }
            )
        except Exception:
            continue
    normalized.sort(key=lambda b: b["time"])
    return normalized


def et_date(epoch_seconds: int):
    dt = datetime.fromtimestamp(epoch_seconds, tz=NY_TZ)
    return dt.date()


def build_daily_bars(candles: Iterable[dict]) -> list[dict]:
    grouped = defaultdict(list)
    for bar in candles:
        grouped[et_date(bar["time"])].append(bar)

    sessions = []
    for session_date in sorted(grouped.keys()):
        bars = grouped[session_date]
        bars.sort(key=lambda b: b["time"])
        open_ = bars[0]["open"]
        high = max(b["high"] for b in bars)
        low = min(b["low"] for b in bars)
        close = bars[-1]["close"]
        sessions.append(
            {
                "date": session_date,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "bars": bars,
            }
        )
    return sessions


def compute_atr(sessions: list[dict], window: int) -> dict:
    atr_by_date = {}
    trs = []
    prev_close = None
    for session in sessions:
        high = session["high"]
        low = session["low"]
        if prev_close is None:
            tr = high - low
        else:
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
        prev_close = session["close"]
        if len(trs) >= window:
            atr = sum(trs[-window:]) / window
            atr_by_date[session["date"]] = atr
    return atr_by_date


def calculate_pivots(high: float, low: float, close: float) -> dict:
    pivot = (high + low + close) / 3
    r1 = 2 * pivot - low
    s1 = 2 * pivot - high
    r2 = pivot + (high - low)
    s2 = pivot - (high - low)
    r3 = high + 2 * (pivot - low)
    s3 = low - 2 * (high - pivot)
    m1 = (s1 + pivot) / 2
    m2 = (pivot + r1) / 2
    m3 = (s2 + s1) / 2
    m4 = (r1 + r2) / 2
    return {
        "R3": r3,
        "R2": r2,
        "R1": r1,
        "M4": m4,
        "M2": m2,
        "PP": pivot,
        "M1": m1,
        "M3": m3,
        "S1": s1,
        "S2": s2,
        "S3": s3,
    }


def ema_update(prev: float | None, value: float, period: int) -> float:
    alpha = 2 / (period + 1)
    if prev is None:
        return value
    return (value - prev) * alpha + prev


def insert_bars(conn: sqlite3.Connection, symbol: str, candles: list[dict], interval_sec: int) -> int:
    ensure_bar_schema(conn)
    sql = """
        INSERT OR REPLACE INTO bar_data (symbol, ts, open, high, low, close, volume, bar_interval_sec)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    values = []
    for bar in candles:
        values.append(
            (
                symbol,
                int(bar["time"]) * 1000,
                bar["open"],
                bar["high"],
                bar["low"],
                bar["close"],
                bar.get("volume", 0),
                interval_sec,
            )
        )
    if not values:
        return 0
    conn.executemany(sql, values)
    return conn.total_changes


def insert_events(conn: sqlite3.Connection, events: list[dict]) -> int:
    if not events:
        return 0
    columns = [
        "event_id",
        "symbol",
        "ts_event",
        "session",
        "level_type",
        "level_price",
        "touch_price",
        "touch_side",
        "distance_bps",
        "is_first_touch_today",
        "touch_count_today",
        "confluence_count",
        "confluence_types",
        "ema9",
        "ema21",
        "ema_state",
        "vwap",
        "vwap_dist_bps",
        "atr",
        "rv_30",
        "rv_regime",
        "iv_rv_state",
        "gamma_mode",
        "gamma_flip",
        "gamma_flip_dist_bps",
        "gamma_confidence",
        "oi_concentration_top5",
        "zero_dte_share",
        "data_quality",
        "bar_interval_sec",
        "source",
        "created_at",
    ]
    placeholders = ", ".join(["?"] * len(columns))
    sql = f"INSERT OR IGNORE INTO touch_events ({', '.join(columns)}) VALUES ({placeholders})"
    values = []
    for ev in events:
        values.append([ev.get(col) for col in columns])
    conn.executemany(sql, values)
    return conn.total_changes


def build_events(
    symbol: str,
    sessions: list[dict],
    interval_sec: int,
    threshold_bps: float,
    cooldown_min: int,
    source: str,
    atr_by_date: dict,
):
    events = []
    ema9 = None
    ema21 = None
    ema_bar_count = 0  # Track how many bars have fed the EMA
    cooldown_ms = cooldown_min * 60 * 1000

    # Warm up EMAs using the first session's bars (don't generate events for it)
    if sessions:
        for bar in sessions[0]["bars"]:
            ema9 = ema_update(ema9, bar["close"], 9)
            ema21 = ema_update(ema21, bar["close"], 21)
            ema_bar_count += 1

    for idx in range(1, len(sessions)):
        base = sessions[idx - 1]
        session = sessions[idx]
        levels = calculate_pivots(base["high"], base["low"], base["close"])
        last_touch_ts = {}
        touch_counts = defaultdict(int)

        cumulative_vol = 0.0
        cumulative_vwap = 0.0

        for bar in session["bars"]:
            close = bar["close"]
            ema9 = ema_update(ema9, close, 9)
            ema21 = ema_update(ema21, close, 21)
            ema_bar_count += 1

            typical = (bar["high"] + bar["low"] + bar["close"]) / 3
            vol = bar.get("volume", 0) or 0
            cumulative_vol += vol
            cumulative_vwap += typical * vol
            vwap = cumulative_vwap / cumulative_vol if cumulative_vol > 0 else None

            for label, level_price in levels.items():
                dist_bps = abs((close - level_price) / level_price * 1e4)
                if dist_bps > threshold_bps:
                    continue

                ts_event = int(bar["time"]) * 1000
                last_ts = last_touch_ts.get(label)
                if last_ts and ts_event - last_ts < cooldown_ms:
                    continue

                confluence = [
                    other
                    for other, price in levels.items()
                    if other != label
                    and abs((close - price) / price * 1e4) <= threshold_bps
                ]

                ema_state = None
                ema9_out = ema9 if ema_bar_count >= 21 else None
                ema21_out = ema21 if ema_bar_count >= 21 else None
                if ema9_out is not None and ema21_out is not None:
                    ema_state = 1 if ema9_out > ema21_out else -1 if ema9_out < ema21_out else 0

                vwap_dist_bps = (
                    (close - vwap) / vwap * 1e4 if vwap is not None and vwap != 0 else None
                )

                touch_counts[label] += 1
                event = {
                    "event_id": str(uuid.uuid4()),
                    "symbol": symbol,
                    "ts_event": ts_event,
                    "session": "RTH",
                    "level_type": label,
                    "level_price": level_price,
                    "touch_price": close,
                    "touch_side": 1 if close >= level_price else -1,
                    "distance_bps": dist_bps,
                    "is_first_touch_today": 1 if touch_counts[label] == 1 else 0,
                    "touch_count_today": touch_counts[label],
                    "confluence_count": len(confluence),
                    "confluence_types": json.dumps(confluence),
                    "ema9": ema9_out,
                    "ema21": ema21_out,
                    "ema_state": ema_state,
                    "vwap": vwap,
                    "vwap_dist_bps": vwap_dist_bps,
                    "atr": atr_by_date.get(base["date"]),
                    "rv_30": None,
                    "rv_regime": None,
                    "iv_rv_state": None,
                    "gamma_mode": None,
                    "gamma_flip": None,
                    "gamma_flip_dist_bps": None,
                    "gamma_confidence": None,
                    "oi_concentration_top5": None,
                    "zero_dte_share": None,
                    "data_quality": None,
                    "bar_interval_sec": interval_sec,
                    "source": source,
                    "created_at": ts_event,
                }
                events.append(event)
                last_touch_ts[label] = ts_event

    return events


def run_build_labels(db_path: str, horizons: list[int]):
    import subprocess

    args = [sys.executable, "scripts/build_labels.py", "--incremental", "--db", db_path]
    if horizons:
        args.extend(["--horizons", *[str(h) for h in horizons]])
    subprocess.run(args, check=False, cwd=os.getcwd())


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill touch events and labels from intraday bars.")
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--symbols", default="SPX", help="Comma-separated symbols")
    parser.add_argument("--interval", default="1m")
    parser.add_argument("--range", dest="range_str", default="5d")
    parser.add_argument("--source", choices=["auto", "ibkr", "yahoo"], default="auto")
    parser.add_argument("--threshold-bps", type=float, default=10)
    parser.add_argument("--cooldown-min", type=int, default=10)
    parser.add_argument("--atr-window", type=int, default=14)
    parser.add_argument("--write-bars", action="store_true", default=True)
    parser.add_argument("--write-events", action="store_true", default=True)
    parser.add_argument("--label", action="store_true", default=True)
    parser.add_argument("--label-horizons", default="5,15,60")
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    interval_sec = int(args.interval.replace("m", "")) * 60 if args.interval.endswith("m") else 60

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    ensure_schema(conn)

    total_bars = 0
    total_events = 0

    range_str = normalize_range_for_source(args.interval, args.range_str, args.source)
    for symbol in symbols:
        payload, source = fetch_market(symbol, args.interval, range_str, args.source)
        candles = parse_candles(payload)
        if not candles:
            print(f"No candles for {symbol}. Skipping.")
            continue

        if args.write_bars:
            total_bars += insert_bars(conn, symbol, candles, interval_sec)

        sessions = build_daily_bars(candles)
        atr_by_date = compute_atr(sessions, args.atr_window)
        events = build_events(
            symbol=symbol,
            sessions=sessions,
            interval_sec=interval_sec,
            threshold_bps=args.threshold_bps,
            cooldown_min=args.cooldown_min,
            source=source,
            atr_by_date=atr_by_date,
        )

        if args.write_events and events:
            total_events += insert_events(conn, events)

    conn.commit()
    conn.close()

    print(f"Inserted bars: {total_bars}")
    print(f"Inserted events: {total_events}")

    if args.label:
        horizons = [int(h) for h in args.label_horizons.split(",") if h.strip().isdigit()]
        run_build_labels(args.db, horizons)


if __name__ == "__main__":
    main()
