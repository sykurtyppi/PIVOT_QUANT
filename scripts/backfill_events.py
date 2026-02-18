#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sqlite3
import sys
import time
import traceback
import hashlib
from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterable
from urllib.request import Request, urlopen

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("backfill")

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

try:
    from migrate_db import migrate_connection
except ImportError:  # pragma: no cover
    migrate_connection = None  # type: ignore

DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")
NY_TZ = ZoneInfo("America/New_York") if ZoneInfo else timezone.utc
GAMMA_BRIDGE_URL = os.getenv("GAMMA_BRIDGE_URL", "http://127.0.0.1:5001/gamma")
GAMMA_IV_RV_HIGH_RATIO = float(os.getenv("GAMMA_IV_RV_HIGH_RATIO", "1.15"))
GAMMA_IV_RV_LOW_RATIO = float(os.getenv("GAMMA_IV_RV_LOW_RATIO", "0.85"))


def now_ms() -> int:
    return int(time.time() * 1000)


def deterministic_event_id(
    symbol: str, ts_event: int, level_type: str, level_price: float, interval_sec: int
) -> str:
    """Generate a deterministic event ID from the natural key.

    Repeated backfills for the same touch produce the same ID, so
    INSERT OR IGNORE deduplicates automatically.
    """
    raw = f"{symbol}|{ts_event}|{level_type}|{level_price:.4f}|{interval_sec}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


def ensure_bar_schema(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(bar_data)")
    cols = {row[1] for row in cur.fetchall()}
    if "bar_interval_sec" not in cols:
        conn.execute("ALTER TABLE bar_data ADD COLUMN bar_interval_sec INTEGER")
        conn.commit()


def ensure_new_columns(conn: sqlite3.Connection) -> None:
    """Add new columns to existing touch_events table if missing."""
    cur = conn.execute("PRAGMA table_info(touch_events)")
    cols = {row[1] for row in cur.fetchall()}
    new_cols = {
        "vpoc": "REAL",
        "vpoc_dist_bps": "REAL",
        "volume_at_level": "REAL",
        "mtf_confluence": "INTEGER DEFAULT 0",
        "mtf_confluence_types": "TEXT",
        "weekly_pivot": "REAL",
        "monthly_pivot": "REAL",
        "level_age_days": "INTEGER DEFAULT 0",
        "hist_reject_rate": "REAL",
        "hist_break_rate": "REAL",
        "hist_sample_size": "INTEGER DEFAULT 0",
        # v3 features: regime, opening range, σ-bands
        "regime_type": "INTEGER",
        "overnight_gap_atr": "REAL",
        "or_high": "REAL",
        "or_low": "REAL",
        "or_size_atr": "REAL",
        "or_breakout": "INTEGER",
        "or_high_dist_bps": "REAL",
        "or_low_dist_bps": "REAL",
        "session_std": "REAL",
        "sigma_band_position": "REAL",
        "distance_to_upper_sigma_bps": "REAL",
        "distance_to_lower_sigma_bps": "REAL",
    }
    for col_name, col_type in new_cols.items():
        if col_name not in cols:
            conn.execute(f"ALTER TABLE touch_events ADD COLUMN {col_name} {col_type}")
    # Natural-key uniqueness: prevent logical duplicates from repeated backfills.
    # Safe to call repeatedly — IF NOT EXISTS is a no-op when the index already exists.
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_touch_natural_key "
        "ON touch_events(symbol, ts_event, level_type, level_price, bar_interval_sec);"
    )
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
            created_at INTEGER NOT NULL,
            vpoc REAL,
            vpoc_dist_bps REAL,
            volume_at_level REAL,
            mtf_confluence INTEGER DEFAULT 0,
            mtf_confluence_types TEXT,
            weekly_pivot REAL,
            monthly_pivot REAL,
            level_age_days INTEGER DEFAULT 0,
            hist_reject_rate REAL,
            hist_break_rate REAL,
            hist_sample_size INTEGER DEFAULT 0,
            regime_type INTEGER,
            overnight_gap_atr REAL,
            or_high REAL,
            or_low REAL,
            or_size_atr REAL,
            or_breakout INTEGER,
            or_high_dist_bps REAL,
            or_low_dist_bps REAL,
            session_std REAL,
            sigma_band_position REAL,
            distance_to_upper_sigma_bps REAL,
            distance_to_lower_sigma_bps REAL
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
            PRIMARY KEY (symbol, ts, bar_interval_sec)
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bar_symbol_interval ON bar_data(symbol, bar_interval_sec, ts);")
    conn.commit()


def fetch_json(url: str, timeout: int = 12, retries: int = 2) -> dict:
    req = Request(url, headers={"User-Agent": "PivotQuantBackfill/1.0"})
    last_err = None
    for attempt in range(1, retries + 2):
        try:
            with urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
            return json.loads(raw.decode("utf-8"))
        except Exception as exc:
            last_err = exc
            if attempt <= retries:
                wait = min(2 ** attempt, 8)
                log.warning("fetch_json attempt %d/%d failed (%s), retrying in %ds...", attempt, retries + 1, exc, wait)
                time.sleep(wait)
            else:
                raise ConnectionError(f"fetch_json failed after {retries + 1} attempts: {url}") from last_err


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
        log.warning("Yahoo 1m data limited to ~7d. Clamping range to 7d.")
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


def _to_float(value):
    try:
        if value is None:
            return None
        out = float(value)
        if math.isfinite(out):
            return out
    except (TypeError, ValueError):
        return None
    return None


def _parse_generated_at_ms(value: str | None) -> int | None:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        return int(datetime.fromisoformat(normalized).timestamp() * 1000)
    except ValueError:
        return None


def _derive_gamma_confidence(payload: dict) -> int | None:
    call_strength = _to_float((payload.get("callWall") or {}).get("strength"))
    put_strength = _to_float((payload.get("putWall") or {}).get("strength"))
    strengths = [s for s in (call_strength, put_strength) if s is not None]
    if not strengths:
        return None
    avg_strength = sum(strengths) / len(strengths)
    return max(0, min(100, int(round(avg_strength))))


def fetch_gamma_context(symbol: str, timeout: int = 2) -> dict | None:
    """Fetch one gamma snapshot from the local IBKR gamma bridge.

    Returns normalized numeric context used to enrich generated touch events.
    If bridge data is unavailable, returns None (non-fatal).
    """
    base_url = (os.getenv("GAMMA_BRIDGE_URL") or GAMMA_BRIDGE_URL).strip()
    sep = "&" if "?" in base_url else "?"
    url = f"{base_url}{sep}symbol={symbol}&expiry=front&limit=60"
    try:
        payload = fetch_json(url, timeout=timeout, retries=0)
    except Exception:
        return None

    gamma_flip = _to_float(payload.get("gammaFlip"))
    if gamma_flip is None:
        return None

    generated_at_ms = _parse_generated_at_ms(payload.get("generatedAt"))
    atm_iv_raw = _to_float((payload.get("stats") or {}).get("atmIV"))
    context = {
        "symbol": symbol.upper(),
        "gamma_flip": gamma_flip,
        "gamma_confidence": _derive_gamma_confidence(payload),
        "oi_concentration_top5": _to_float((payload.get("stats") or {}).get("oiConcentration")),
        "zero_dte_share": _to_float((payload.get("stats") or {}).get("zeroDteShare")),
        # IB returns impliedVol as a decimal (e.g. 0.22). Convert to % for IV/RV comparison.
        "atm_iv_pct": atm_iv_raw * 100 if atm_iv_raw is not None else None,
        "generated_at_ms": generated_at_ms,
        "generated_at_date_et": (
            datetime.fromtimestamp(generated_at_ms / 1000, tz=NY_TZ).date()
            if generated_at_ms is not None
            else None
        ),
    }
    return context


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
    # Use adaptive window: at least 2 TRs, up to requested window
    min_window = min(2, window)
    for session in sessions:
        high = session["high"]
        low = session["low"]
        if prev_close is None:
            tr = high - low
        else:
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
        prev_close = session["close"]
        effective_window = min(len(trs), window)
        if effective_window >= min_window:
            atr = sum(trs[-effective_window:]) / effective_window
            atr_by_date[session["date"]] = atr
    return atr_by_date


def compute_realized_volatility(sessions: list[dict], window: int = 30) -> dict:
    """Compute annualized realized volatility using close-to-close log returns.

    Returns dict mapping session date -> RV value.
    Also classifies into regime: 1=low, 2=normal, 3=high.
    Uses adaptive window: minimum 3 returns, up to requested window.
    """
    rv_by_date = {}
    rv_regime_by_date = {}

    if len(sessions) < 2:
        return rv_by_date, rv_regime_by_date

    min_returns = min(3, window)  # Need at least 3 returns for meaningful RV

    log_returns = []
    for i in range(1, len(sessions)):
        prev_close = sessions[i - 1]["close"]
        curr_close = sessions[i]["close"]
        if prev_close > 0 and curr_close > 0:
            lr = math.log(curr_close / prev_close)
            log_returns.append((sessions[i]["date"], lr))

    for i, (dt, _) in enumerate(log_returns):
        available = i + 1
        if available < min_returns:
            continue
        effective_window = min(available, window)
        returns_window = [r for _, r in log_returns[i + 1 - effective_window:i + 1]]
        n = len(returns_window)
        if n < 2:
            continue
        mean = sum(returns_window) / n
        variance = sum((r - mean) ** 2 for r in returns_window) / (n - 1)
        rv = math.sqrt(variance * 252) * 100  # annualized, in %
        rv_by_date[dt] = rv

        # Regime classification based on historical percentiles
        # Low < 12%, Normal 12-22%, High > 22% (approximate SPX ranges)
        if rv < 12:
            rv_regime_by_date[dt] = 1  # low vol
        elif rv < 22:
            rv_regime_by_date[dt] = 2  # normal
        else:
            rv_regime_by_date[dt] = 3  # high vol

    return rv_by_date, rv_regime_by_date


def compute_data_quality(event: dict) -> float:
    """Score data quality 0-1 based on how many key fields are populated.

    Core fields (weighted higher):
      touch_price, level_price, distance_bps, ema_state, vwap, atr
    Enhanced fields (weighted lower):
      vpoc, mtf_confluence, hist_reject_rate, volume_at_level
    """
    core_fields = ["touch_price", "level_price", "distance_bps", "ema_state", "vwap", "atr"]
    enhanced_fields = ["vpoc", "mtf_confluence", "hist_reject_rate", "volume_at_level",
                       "gamma_flip", "rv_30"]

    core_weight = 0.7
    enhanced_weight = 0.3

    core_present = sum(1 for f in core_fields if event.get(f) is not None)
    enhanced_present = sum(1 for f in enhanced_fields if event.get(f) is not None)

    core_score = core_present / len(core_fields) if core_fields else 1.0
    enhanced_score = enhanced_present / len(enhanced_fields) if enhanced_fields else 1.0

    return round(core_score * core_weight + enhanced_score * enhanced_weight, 3)


def compute_volume_profile(bars: list[dict], num_bins: int = 50) -> dict:
    """Build a volume-at-price profile from intraday bars.

    Returns dict with:
      vpoc: price level with highest volume (Volume Point of Control)
      profile: list of (price_mid, volume) tuples
      value_area_high: upper bound of 70% value area
      value_area_low: lower bound of 70% value area
    """
    if not bars:
        return {"vpoc": None, "profile": [], "value_area_high": None, "value_area_low": None}

    prices_with_vol = []
    for bar in bars:
        vol = bar.get("volume", 0) or 0
        if vol <= 0:
            continue
        typical = (bar["high"] + bar["low"] + bar["close"]) / 3
        prices_with_vol.append((typical, vol))

    if not prices_with_vol:
        return {"vpoc": None, "profile": [], "value_area_high": None, "value_area_low": None}

    all_prices = [p for p, _ in prices_with_vol]
    price_min = min(all_prices)
    price_max = max(all_prices)
    price_range = price_max - price_min

    if price_range < 1e-8:
        return {
            "vpoc": price_min,
            "profile": [(price_min, sum(v for _, v in prices_with_vol))],
            "value_area_high": price_min,
            "value_area_low": price_min,
        }

    bin_size = price_range / num_bins
    bins = [0.0] * num_bins
    bin_mids = [price_min + (i + 0.5) * bin_size for i in range(num_bins)]

    for price, vol in prices_with_vol:
        idx = min(int((price - price_min) / bin_size), num_bins - 1)
        bins[idx] += vol

    max_vol_idx = max(range(num_bins), key=lambda i: bins[i])
    vpoc = bin_mids[max_vol_idx]

    # Value Area: 70% of total volume centered on VPOC
    total_vol = sum(bins)
    target_vol = total_vol * 0.70
    va_vol = bins[max_vol_idx]
    lo_idx = max_vol_idx
    hi_idx = max_vol_idx

    while va_vol < target_vol and (lo_idx > 0 or hi_idx < num_bins - 1):
        expand_lo = bins[lo_idx - 1] if lo_idx > 0 else -1
        expand_hi = bins[hi_idx + 1] if hi_idx < num_bins - 1 else -1
        if expand_lo >= expand_hi:
            lo_idx -= 1
            va_vol += bins[lo_idx]
        else:
            hi_idx += 1
            va_vol += bins[hi_idx]

    profile = [(bin_mids[i], bins[i]) for i in range(num_bins) if bins[i] > 0]

    return {
        "vpoc": vpoc,
        "profile": profile,
        "value_area_high": bin_mids[hi_idx] + bin_size / 2,
        "value_area_low": bin_mids[lo_idx] - bin_size / 2,
    }


def volume_at_price(bars: list[dict], price: float, tolerance_bps: float = 10) -> float:
    """Sum volume within tolerance_bps of a given price level."""
    total = 0.0
    for bar in bars:
        vol = bar.get("volume", 0) or 0
        if vol <= 0:
            continue
        typical = (bar["high"] + bar["low"] + bar["close"]) / 3
        dist = abs((typical - price) / price * 1e4)
        if dist <= tolerance_bps:
            total += vol
    return total


def build_weekly_sessions(sessions: list[dict]) -> list[dict]:
    """Aggregate daily sessions into weekly OHLC."""
    weekly = []
    current_week = None
    week_sessions = []

    for session in sessions:
        iso_cal = session["date"].isocalendar()
        week_key = (iso_cal[0], iso_cal[1])  # (year, week_number)
        if current_week != week_key:
            if week_sessions:
                weekly.append({
                    "date": week_sessions[-1]["date"],
                    "open": week_sessions[0]["open"],
                    "high": max(s["high"] for s in week_sessions),
                    "low": min(s["low"] for s in week_sessions),
                    "close": week_sessions[-1]["close"],
                })
            current_week = week_key
            week_sessions = [session]
        else:
            week_sessions.append(session)

    if week_sessions:
        weekly.append({
            "date": week_sessions[-1]["date"],
            "open": week_sessions[0]["open"],
            "high": max(s["high"] for s in week_sessions),
            "low": min(s["low"] for s in week_sessions),
            "close": week_sessions[-1]["close"],
        })

    return weekly


def build_monthly_sessions(sessions: list[dict]) -> list[dict]:
    """Aggregate daily sessions into monthly OHLC."""
    monthly = []
    current_month = None
    month_sessions = []

    for session in sessions:
        month_key = (session["date"].year, session["date"].month)
        if current_month != month_key:
            if month_sessions:
                monthly.append({
                    "date": month_sessions[-1]["date"],
                    "open": month_sessions[0]["open"],
                    "high": max(s["high"] for s in month_sessions),
                    "low": min(s["low"] for s in month_sessions),
                    "close": month_sessions[-1]["close"],
                })
            current_month = month_key
            month_sessions = [session]
        else:
            month_sessions.append(session)

    if month_sessions:
        monthly.append({
            "date": month_sessions[-1]["date"],
            "open": month_sessions[0]["open"],
            "high": max(s["high"] for s in month_sessions),
            "low": min(s["low"] for s in month_sessions),
            "close": month_sessions[-1]["close"],
        })

    return monthly


def find_mtf_pivot_for_date(higher_tf_sessions: list[dict], target_date, calc_fn=None):
    """Given a list of higher-TF OHLC sessions, return the pivot set whose
    period ended before target_date (i.e., the 'prior completed' bar)."""
    if calc_fn is None:
        calc_fn = calculate_pivots
    candidate = None
    for session in higher_tf_sessions:
        if session["date"] < target_date:
            candidate = session
        else:
            break
    if candidate is None:
        return None
    return calc_fn(candidate["high"], candidate["low"], candidate["close"])


def compute_level_age(
    prior_sessions: list[dict],
    level_type: str,
    level_price: float,
    tolerance_bps: float = 15,
) -> int:
    """Count how many consecutive prior sessions had a pivot of the same type
    within tolerance_bps of the current level_price. This measures 'persistence'."""
    age = 0
    for session in reversed(prior_sessions):
        session_pivots = calculate_pivots(session["high"], session["low"], session["close"])
        if level_type in session_pivots:
            dist = abs((session_pivots[level_type] - level_price) / level_price * 1e4)
            if dist <= tolerance_bps:
                age += 1
            else:
                break
        else:
            break
    return age


def compute_historical_accuracy(
    conn: sqlite3.Connection,
    symbol: str,
    level_type: str,
    before_ts: int,
    horizon: int = 15,
) -> tuple[float | None, float | None, int]:
    """Look back at past labeled events for this level_type to compute
    historical reject/break rates. Returns (reject_rate, break_rate, sample_size)."""
    cur = conn.execute(
        """
        SELECT el.reject, el.break
        FROM touch_events te
        JOIN event_labels el ON te.event_id = el.event_id
        WHERE te.symbol = ? AND te.level_type = ? AND te.ts_event < ?
          AND el.horizon_min = ?
        ORDER BY te.ts_event DESC
        LIMIT 100
        """,
        (symbol, level_type, before_ts, horizon),
    )
    rows = cur.fetchall()
    if not rows:
        return None, None, 0

    sample_size = len(rows)
    reject_count = sum(1 for r in rows if r[0] == 1)
    break_count = sum(1 for r in rows if r[1] == 1)
    return reject_count / sample_size, break_count / sample_size, sample_size


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
    before = conn.total_changes
    conn.executemany(sql, values)
    return conn.total_changes - before


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
        "vpoc",
        "vpoc_dist_bps",
        "volume_at_level",
        "mtf_confluence",
        "mtf_confluence_types",
        "weekly_pivot",
        "monthly_pivot",
        "level_age_days",
        "hist_reject_rate",
        "hist_break_rate",
        "hist_sample_size",
        # v3 features
        "regime_type",
        "overnight_gap_atr",
        "or_high",
        "or_low",
        "or_size_atr",
        "or_breakout",
        "or_high_dist_bps",
        "or_low_dist_bps",
        "session_std",
        "sigma_band_position",
        "distance_to_upper_sigma_bps",
        "distance_to_lower_sigma_bps",
    ]
    placeholders = ", ".join(["?"] * len(columns))
    sql = f"INSERT OR IGNORE INTO touch_events ({', '.join(columns)}) VALUES ({placeholders})"
    values = []
    for ev in events:
        values.append([ev.get(col) for col in columns])
    before = conn.total_changes
    conn.executemany(sql, values)
    return conn.total_changes - before


def compute_opening_range(bars: list[dict], or_minutes: int = 30) -> dict:
    """Compute Opening Range High/Low from first N minutes of session bars.

    Returns dict with or_high, or_low, or_size.
    The first bar's timestamp is used as session open reference.
    """
    if not bars:
        return {"or_high": None, "or_low": None, "or_size": None}

    open_ts = bars[0]["time"]
    cutoff_ts = open_ts + or_minutes * 60

    or_bars = [b for b in bars if b["time"] < cutoff_ts]
    if not or_bars:
        return {"or_high": None, "or_low": None, "or_size": None}

    or_high = max(b["high"] for b in or_bars)
    or_low = min(b["low"] for b in or_bars)
    return {"or_high": or_high, "or_low": or_low, "or_size": or_high - or_low}


def classify_regime(
    session: dict,
    prior_session: dict | None,
    atr: float | None,
    ema9: float | None,
    ema21: float | None,
    or_data: dict,
) -> tuple[int, float | None, float | None]:
    """Classify the trading day regime using rule-based signals.

    Returns (regime_type, overnight_gap_atr, or_size_atr):
      regime_type: 1=trend_up, 2=trend_down, 3=range, 4=vol_expansion
      overnight_gap_atr: gap / ATR ratio (signed)
      or_size_atr: opening range / ATR ratio
    """
    # Default: range (3)
    regime = 3
    overnight_gap_atr = None
    or_size_atr = None

    if atr is None or atr <= 0:
        return regime, overnight_gap_atr, or_size_atr

    # Overnight gap signal
    if prior_session is not None:
        gap = session["open"] - prior_session["close"]
        overnight_gap_atr = gap / atr
    else:
        overnight_gap_atr = 0.0

    # Opening range signal
    or_size = or_data.get("or_size")
    if or_size is not None and or_size > 0:
        or_size_atr = or_size / atr
    else:
        or_size_atr = None

    # EMA trend signal
    ema_bullish = ema9 is not None and ema21 is not None and ema9 > ema21
    ema_bearish = ema9 is not None and ema21 is not None and ema9 < ema21

    # Scoring: combine signals for regime classification
    # Large gap (>0.5 ATR) + wide OR (>0.4 ATR) + EMA alignment → trend
    # Large OR (>0.7 ATR) alone → vol expansion
    # Small gap + narrow OR → range

    large_gap = abs(overnight_gap_atr or 0) > 0.5
    wide_or = (or_size_atr or 0) > 0.4
    very_wide_or = (or_size_atr or 0) > 0.7

    if very_wide_or and large_gap:
        regime = 4  # vol_expansion
    elif large_gap and wide_or and ema_bullish and (overnight_gap_atr or 0) > 0:
        regime = 1  # trend_up
    elif large_gap and wide_or and ema_bearish and (overnight_gap_atr or 0) < 0:
        regime = 2  # trend_down
    elif wide_or and ema_bullish:
        regime = 1  # trend_up (weaker signal)
    elif wide_or and ema_bearish:
        regime = 2  # trend_down (weaker signal)
    else:
        regime = 3  # range

    return regime, overnight_gap_atr, or_size_atr


def compute_session_std(bars: list[dict]) -> float | None:
    """Compute standard deviation of close prices within the session.

    Used for VWAP z-score normalization.
    """
    if len(bars) < 2:
        return None
    closes = [b["close"] for b in bars]
    n = len(closes)
    mean = sum(closes) / n
    variance = sum((c - mean) ** 2 for c in closes) / (n - 1)
    return math.sqrt(variance) if variance > 0 else None


def compute_sigma_bands(prior_close: float, atr: float | None, rv_daily: float | None) -> dict:
    """Compute expected daily range σ-bands.

    Uses RV if available, falls back to ATR-based estimate.
    Returns dict with upper_1sigma, lower_1sigma, upper_2sigma, lower_2sigma.
    """
    if prior_close <= 0:
        return {"upper_1sigma": None, "lower_1sigma": None,
                "upper_2sigma": None, "lower_2sigma": None}

    # Use RV (annualized %) to get daily σ
    daily_sigma = None
    if rv_daily is not None and rv_daily > 0:
        # rv_daily is annualized %, convert to daily dollar move
        daily_sigma = prior_close * (rv_daily / 100) / math.sqrt(252)
    elif atr is not None and atr > 0:
        # ATR approximates ~1.2σ of daily range
        daily_sigma = atr / 1.2

    if daily_sigma is None or daily_sigma <= 0:
        return {"upper_1sigma": None, "lower_1sigma": None,
                "upper_2sigma": None, "lower_2sigma": None}

    return {
        "upper_1sigma": prior_close + daily_sigma,
        "lower_1sigma": prior_close - daily_sigma,
        "upper_2sigma": prior_close + 2 * daily_sigma,
        "lower_2sigma": prior_close - 2 * daily_sigma,
    }


def compute_daily_emas(sessions: list[dict]) -> dict:
    """Compute daily EMA9 and EMA21 from session close prices.

    Returns dict mapping session date -> (ema9, ema21, ema_state).
    Uses prior session's close as the EMA input, matching the dashboard's
    daily chart overlay behavior (9-day and 21-day EMAs, not 9-bar intraday).
    """
    ema9 = None
    ema21 = None
    result = {}

    for session in sessions:
        close = session["close"]
        ema9 = ema_update(ema9, close, 9)
        ema21 = ema_update(ema21, close, 21)
        result[session["date"]] = (ema9, ema21)

    return result


def build_events(
    symbol: str,
    sessions: list[dict],
    interval_sec: int,
    threshold_bps: float,
    cooldown_min: int,
    source: str,
    atr_by_date: dict,
    conn: sqlite3.Connection | None = None,
    rv_by_date: dict | None = None,
    rv_regime_by_date: dict | None = None,
    gamma_context: dict | None = None,
):
    events = []
    cooldown_ms = cooldown_min * 60 * 1000

    # Build higher-timeframe OHLC for multi-TF confluence
    weekly_sessions = build_weekly_sessions(sessions)
    monthly_sessions = build_monthly_sessions(sessions)

    # Compute daily EMAs from session closes (matches dashboard daily chart)
    daily_emas = compute_daily_emas(sessions)
    latest_session_date = sessions[-1]["date"] if sessions else None

    for idx in range(1, len(sessions)):
        base = sessions[idx - 1]
        session = sessions[idx]
        levels = calculate_pivots(base["high"], base["low"], base["close"])
        last_touch_ts = {}
        touch_counts = defaultdict(int)

        cumulative_vol = 0.0
        cumulative_vwap = 0.0

        # Volume profile for the current session (computed incrementally)
        session_bars_so_far = []

        # Get higher-TF pivots for this session date
        weekly_pivots = find_mtf_pivot_for_date(weekly_sessions, session["date"])
        monthly_pivots = find_mtf_pivot_for_date(monthly_sessions, session["date"])

        # Daily EMAs: use prior session's close-based EMA (available before today opens)
        ema9_daily, ema21_daily = daily_emas.get(base["date"], (None, None))
        # Require at least 2 sessions so EMA has seen multiple closes
        ema_ready = ema9_daily is not None and ema21_daily is not None and idx >= 2

        # ── ATR for this session (from prior day) ──
        session_atr = atr_by_date.get(base["date"])

        # ── σ-bands from RV (use prior session's RV — no look-ahead) ──
        # RV on session["date"] includes today's close, which isn't known
        # until EOD. base["date"] RV is fully known at today's open.
        rv_for_session = rv_by_date.get(base["date"]) if rv_by_date else None
        rv_regime_for_session = rv_regime_by_date.get(base["date"]) if rv_regime_by_date else None
        sigma_bands = compute_sigma_bands(base["close"], session_atr, rv_for_session)
        gamma_context_date = gamma_context.get("generated_at_date_et") if gamma_context else None
        use_gamma_context = bool(
            gamma_context
            and (
                gamma_context_date is None
                or gamma_context_date == session["date"]
                or (
                    latest_session_date is not None
                    and gamma_context_date is not None
                    and session["date"] == latest_session_date
                    and gamma_context_date > latest_session_date
                    and (gamma_context_date - latest_session_date).days <= 3
                )
            )
        )

        for bar in session["bars"]:
            close = bar["close"]
            session_bars_so_far.append(bar)

            # ── Session std (expanding window, no look-ahead) ──
            session_std = compute_session_std(session_bars_so_far)

            typical = (bar["high"] + bar["low"] + bar["close"]) / 3
            vol = bar.get("volume", 0) or 0
            cumulative_vol += vol
            cumulative_vwap += typical * vol
            vwap = cumulative_vwap / cumulative_vol if cumulative_vol > 0 else None

            # ── Opening Range + regime features from bars seen so far (no look-ahead) ──
            # This matches live behavior: early-session events use partial OR;
            # post-OR events use completed OR.
            or_data = compute_opening_range(session_bars_so_far, or_minutes=30)
            regime_type, overnight_gap_atr, _ = classify_regime(
                session=session,
                prior_session=base,
                atr=session_atr,
                ema9=ema9_daily if ema_ready else None,
                ema21=ema21_daily if ema_ready else None,
                or_data=or_data,
            )
            or_high = or_data["or_high"]
            or_low = or_data["or_low"]
            or_size_atr_val = None
            if or_data["or_size"] is not None and session_atr and session_atr > 0:
                or_size_atr_val = or_data["or_size"] / session_atr

            or_breakout_val = 0
            or_high_dist = None
            or_low_dist = None
            if or_high is not None and or_low is not None:
                if close > or_high:
                    or_breakout_val = 1
                elif close < or_low:
                    or_breakout_val = -1
                if or_high != 0:
                    or_high_dist = (close - or_high) / or_high * 1e4
                if or_low != 0:
                    or_low_dist = (close - or_low) / or_low * 1e4

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
                ema9_out = ema9_daily if ema_ready else None
                ema21_out = ema21_daily if ema_ready else None
                if ema9_out is not None and ema21_out is not None:
                    ema_state = 1 if ema9_out > ema21_out else -1 if ema9_out < ema21_out else 0

                vwap_dist_bps = (
                    (close - vwap) / vwap * 1e4 if vwap is not None and vwap != 0 else None
                )

                # --- VPOC & Volume at Level ---
                vol_profile = compute_volume_profile(session_bars_so_far)
                vpoc = vol_profile["vpoc"]
                vpoc_dist_bps = None
                if vpoc is not None and vpoc != 0:
                    vpoc_dist_bps = (close - vpoc) / vpoc * 1e4
                vol_at_level = volume_at_price(session_bars_so_far, level_price, threshold_bps)

                iv_rv_state = None
                gamma_mode = None
                gamma_flip = None
                gamma_flip_dist_bps = None
                gamma_confidence = None
                oi_concentration_top5 = None
                zero_dte_share = None
                if use_gamma_context:
                    gamma_flip = gamma_context.get("gamma_flip")
                    gamma_confidence = gamma_context.get("gamma_confidence")
                    oi_concentration_top5 = gamma_context.get("oi_concentration_top5")
                    zero_dte_share = gamma_context.get("zero_dte_share")
                    if gamma_flip is not None and gamma_flip != 0:
                        gamma_mode = 1 if close >= gamma_flip else -1
                        gamma_flip_dist_bps = (close - gamma_flip) / gamma_flip * 1e4
                    atm_iv_pct = gamma_context.get("atm_iv_pct")
                    if (
                        atm_iv_pct is not None
                        and rv_for_session is not None
                        and rv_for_session > 0
                    ):
                        iv_rv_ratio = atm_iv_pct / rv_for_session
                        if iv_rv_ratio >= GAMMA_IV_RV_HIGH_RATIO:
                            iv_rv_state = 1
                        elif iv_rv_ratio <= GAMMA_IV_RV_LOW_RATIO:
                            iv_rv_state = -1
                        else:
                            iv_rv_state = 0

                # --- Multi-Timeframe Confluence ---
                mtf_matches = []
                for tf_name, tf_pivots in [("weekly", weekly_pivots), ("monthly", monthly_pivots)]:
                    if tf_pivots is None:
                        continue
                    for tf_label, tf_price in tf_pivots.items():
                        tf_dist = abs((level_price - tf_price) / tf_price * 1e4)
                        if tf_dist <= threshold_bps * 2:  # wider tolerance for HTF
                            mtf_matches.append(f"{tf_name}_{tf_label}")

                # Weekly/monthly pivot PP for reference
                wp = weekly_pivots.get("PP") if weekly_pivots else None
                mp = monthly_pivots.get("PP") if monthly_pivots else None

                # --- Level Age (persistence across sessions) ---
                prior_sessions = sessions[max(0, idx - 30):idx]
                level_age = compute_level_age(prior_sessions, label, level_price)

                # --- Historical Accuracy ---
                hist_reject_rate = None
                hist_break_rate = None
                hist_sample_size = 0
                if conn is not None:
                    hist_reject_rate, hist_break_rate, hist_sample_size = (
                        compute_historical_accuracy(conn, symbol, label, ts_event)
                    )

                touch_counts[label] += 1

                # ── σ-band position for this bar ──
                sigma_pos = None
                dist_upper_sigma = None
                dist_lower_sigma = None
                upper_1s = sigma_bands.get("upper_1sigma")
                lower_1s = sigma_bands.get("lower_1sigma")
                if upper_1s is not None and lower_1s is not None:
                    band_width = upper_1s - lower_1s
                    if band_width > 0:
                        midpoint = (upper_1s + lower_1s) / 2
                        sigma_pos = (close - midpoint) / (band_width / 2)
                    if upper_1s != 0:
                        dist_upper_sigma = (close - upper_1s) / upper_1s * 1e4
                    if lower_1s != 0:
                        dist_lower_sigma = (close - lower_1s) / lower_1s * 1e4

                event = {
                    "event_id": deterministic_event_id(
                        symbol, ts_event, label, level_price, interval_sec
                    ),
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
                    "rv_30": rv_for_session,
                    "rv_regime": rv_regime_for_session,
                    "iv_rv_state": iv_rv_state,
                    "gamma_mode": gamma_mode,
                    "gamma_flip": gamma_flip,
                    "gamma_flip_dist_bps": gamma_flip_dist_bps,
                    "gamma_confidence": gamma_confidence,
                    "oi_concentration_top5": oi_concentration_top5,
                    "zero_dte_share": zero_dte_share,
                    "data_quality": None,  # computed after event dict is built
                    "bar_interval_sec": interval_sec,
                    "source": source,
                    "created_at": ts_event,
                    "vpoc": vpoc,
                    "vpoc_dist_bps": vpoc_dist_bps,
                    "volume_at_level": vol_at_level,
                    "mtf_confluence": len(mtf_matches),
                    "mtf_confluence_types": json.dumps(mtf_matches) if mtf_matches else None,
                    "weekly_pivot": wp,
                    "monthly_pivot": mp,
                    "level_age_days": level_age,
                    "hist_reject_rate": hist_reject_rate,
                    "hist_break_rate": hist_break_rate,
                    "hist_sample_size": hist_sample_size,
                    # v3 features
                    "regime_type": regime_type,
                    "overnight_gap_atr": overnight_gap_atr,
                    "or_high": or_high,
                    "or_low": or_low,
                    "or_size_atr": or_size_atr_val,
                    "or_breakout": or_breakout_val,
                    "or_high_dist_bps": or_high_dist,
                    "or_low_dist_bps": or_low_dist,
                    "session_std": session_std,
                    "sigma_band_position": sigma_pos,
                    "distance_to_upper_sigma_bps": dist_upper_sigma,
                    "distance_to_lower_sigma_bps": dist_lower_sigma,
                }
                event["data_quality"] = compute_data_quality(event)
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
    parser.add_argument("--symbols", default="SPY", help="Comma-separated symbols")
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
    if migrate_connection is not None:
        migrate_connection(conn, verbose=False)
    else:
        ensure_schema(conn)
        ensure_new_columns(conn)

    total_bars = 0
    total_events = 0

    range_str = normalize_range_for_source(args.interval, args.range_str, args.source)
    failed_symbols = []

    for symbol in symbols:
        try:
            log.info("Processing %s (interval=%s, range=%s)", symbol, args.interval, range_str)
            payload, source = fetch_market(symbol, args.interval, range_str, args.source)
            candles = parse_candles(payload)
            if not candles:
                log.warning("No candles for %s. Skipping.", symbol)
                continue

            gamma_context = fetch_gamma_context(symbol)
            if gamma_context:
                log.info(
                    "%s: gamma context loaded (flip=%.2f, date=%s)",
                    symbol,
                    gamma_context["gamma_flip"],
                    gamma_context.get("generated_at_date_et"),
                )
            else:
                log.info("%s: gamma context unavailable, proceeding without gamma enrichment", symbol)

            if args.write_bars:
                n_bars = insert_bars(conn, symbol, candles, interval_sec)
                total_bars += n_bars
                log.info("%s: inserted %d bars", symbol, n_bars)

            sessions = build_daily_bars(candles)
            atr_by_date = compute_atr(sessions, args.atr_window)
            rv_by_date, rv_regime_by_date = compute_realized_volatility(sessions, window=30)
            events = build_events(
                symbol=symbol,
                sessions=sessions,
                interval_sec=interval_sec,
                threshold_bps=args.threshold_bps,
                cooldown_min=args.cooldown_min,
                source=source,
                atr_by_date=atr_by_date,
                conn=conn,
                rv_by_date=rv_by_date,
                rv_regime_by_date=rv_regime_by_date,
                gamma_context=gamma_context,
            )

            if args.write_events and events:
                n_events = insert_events(conn, events)
                total_events += n_events
                log.info("%s: inserted %d events", symbol, n_events)

            # Commit per symbol so partial progress is preserved
            conn.commit()

        except Exception:
            log.error("Failed processing %s:\n%s", symbol, traceback.format_exc())
            failed_symbols.append(symbol)
            # Rollback any uncommitted changes for this symbol
            conn.rollback()
            continue

    conn.close()

    log.info("Inserted bars: %d", total_bars)
    log.info("Inserted events: %d", total_events)

    if failed_symbols:
        log.warning("Failed symbols: %s", ", ".join(failed_symbols))

    if args.label:
        horizons = [int(h) for h in args.label_horizons.split(",") if h.strip().isdigit()]
        run_build_labels(args.db, horizons)

    if failed_symbols:
        sys.exit(1)


if __name__ == "__main__":
    main()
