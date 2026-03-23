#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import random
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

MARKETDATA_APP_BASE = os.getenv("MARKETDATA_APP_BASE", "https://api.marketdata.app/v1").rstrip("/")
MARKETDATA_APP_TOKEN = os.getenv("MARKETDATA_APP_TOKEN", "").strip()
DEFAULT_DB = os.getenv("PIVOT_DB", "data/pivot_events.sqlite")
DEFAULT_SYMBOLS = os.getenv("LIVE_COLLECTOR_SYMBOLS", "SPY")
DEFAULT_RANGE_PCT = float(os.getenv("GAMMA_HISTORY_STRIKE_RANGE_PCT", "0.2"))
DEFAULT_MAX_STRIKES = int(os.getenv("GAMMA_HISTORY_MAX_STRIKES", "120"))
GAMMA_HISTORY_EXPIRY_MODE = (os.getenv("GAMMA_HISTORY_EXPIRY_MODE", "90dte") or "90dte").strip().lower()
if GAMMA_HISTORY_EXPIRY_MODE == "quarterly":
    raise ValueError("GAMMA_HISTORY_EXPIRY_MODE=quarterly is no longer supported; use 90dte")
if GAMMA_HISTORY_EXPIRY_MODE not in {"0dte", "front", "monthly", "all", "90dte"}:
    raise ValueError(f"Unsupported GAMMA_HISTORY_EXPIRY_MODE={GAMMA_HISTORY_EXPIRY_MODE!r}")
# Use a wider fetch window than the target 90DTE tenor so the nearest
# structural expiry is still present when it lands slightly beyond 90DTE.
GAMMA_HISTORY_LIVE_DTE_DAYS = max(1, int(os.getenv("GAMMA_HISTORY_LIVE_DTE_DAYS", "120")))
DEFAULT_TIMEOUT = int(os.getenv("GAMMA_HISTORY_HTTP_TIMEOUT_SEC", "60"))
DEFAULT_HTTP_MAX_ATTEMPTS = max(1, int(os.getenv("GAMMA_HISTORY_HTTP_MAX_ATTEMPTS", "6")))
DEFAULT_HTTP_RETRY_BASE_SEC = max(0.0, float(os.getenv("GAMMA_HISTORY_HTTP_RETRY_BASE_SEC", "1.0")))
DEFAULT_HTTP_RETRY_MAX_SEC = max(
    DEFAULT_HTTP_RETRY_BASE_SEC,
    float(os.getenv("GAMMA_HISTORY_HTTP_RETRY_MAX_SEC", "30.0")),
)
DEFAULT_HTTP_RETRY_JITTER_SEC = max(0.0, float(os.getenv("GAMMA_HISTORY_HTTP_RETRY_JITTER_SEC", "0.25")))
GAMMA_HISTORY_SQLITE_SYNC = (os.getenv("GAMMA_HISTORY_SQLITE_SYNC", "FULL") or "FULL").strip().upper()
if GAMMA_HISTORY_SQLITE_SYNC not in {"OFF", "NORMAL", "FULL", "EXTRA"}:
    GAMMA_HISTORY_SQLITE_SYNC = "FULL"
GAMMA_HISTORY_WAL_AUTOCHECKPOINT = max(
    100,
    int(os.getenv("GAMMA_HISTORY_WAL_AUTOCHECKPOINT", "1000")),
)
TRANSIENT_HTTP_CODES = {429, 500, 502, 503, 504}
_MARKETDATA_ROW_FIELDS = (
    "strike",
    "side",
    "gamma",
    "iv",
    "openInterest",
    "delta",
    "expiration",
    "underlyingPrice",
    "bid",
    "ask",
    "last",
    "lastPrice",
    "mid",
    "mark",
    "close",
)
GAMMA_COMPUTE_FALLBACK = (
    (os.getenv("GAMMA_COMPUTE_FALLBACK", "false") or "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
GAMMA_COMPUTE_FALLBACK_SOLVE_IV = (
    (os.getenv("GAMMA_COMPUTE_FALLBACK_SOLVE_IV", "true") or "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
GAMMA_COMPUTE_FALLBACK_RISK_FREE_RATE = float(
    os.getenv("GAMMA_COMPUTE_FALLBACK_RISK_FREE_RATE", "0.045")
)
GAMMA_COMPUTE_FALLBACK_DIVIDEND_YIELD = float(
    os.getenv("GAMMA_COMPUTE_FALLBACK_DIVIDEND_YIELD", "0.0")
)
GAMMA_COMPUTE_FALLBACK_IV_MIN = max(
    1e-4,
    float(os.getenv("GAMMA_COMPUTE_FALLBACK_IV_MIN", "0.01")),
)
GAMMA_COMPUTE_FALLBACK_IV_MAX = max(
    0.25,
    float(os.getenv("GAMMA_COMPUTE_FALLBACK_IV_MAX", "5.0")),
)
GAMMA_COMPUTE_FALLBACK_MIN_DTE_DAYS = max(
    1,
    int(os.getenv("GAMMA_COMPUTE_FALLBACK_MIN_DTE_DAYS", "1")),
)
GAMMA_COMPUTE_FALLBACK_MAX_IV_ITERS = max(
    10,
    int(os.getenv("GAMMA_COMPUTE_FALLBACK_MAX_IV_ITERS", "80")),
)

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


def _parse_retry_after_seconds(raw: object) -> float | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        seconds = float(text)
        return max(0.0, seconds)
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = (dt.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds()
        return max(0.0, delta)
    except Exception:
        return None


def _retry_sleep_seconds(
    attempt_idx: int,
    retry_after_sec: float | None,
    retry_base_sec: float,
    retry_max_sec: float,
    retry_jitter_sec: float,
) -> float:
    exponential = retry_base_sec * (2 ** max(0, attempt_idx))
    wait = max(exponential, retry_after_sec or 0.0)
    wait = min(wait, retry_max_sec)
    if retry_jitter_sec > 0:
        wait += random.uniform(0.0, retry_jitter_sec)
    return max(0.0, wait)


def _marketdata_live_dte_queries(mode: str, base_dte_days: int) -> list[int]:
    safe_mode = _normalize_expiry_mode(mode)
    safe_base = max(1, int(base_dte_days))
    if safe_mode == "aggregate_90dte":
        queries: list[int] = []
        for candidate in (7, 14, 30, 45, 60, 75, 90):
            if candidate not in queries:
                queries.append(candidate)
        return queries
    if safe_mode != "90dte":
        return [safe_base]
    queries = []
    for candidate in (90, 75, 105, safe_base):
        safe_candidate = max(1, int(candidate))
        if safe_candidate not in queries:
            queries.append(safe_candidate)
    return queries


def _merge_marketdata_chain_payloads(payloads: list[dict]) -> dict:
    merged: dict[str, object] = {"s": "ok"}
    for field in _MARKETDATA_ROW_FIELDS:
        merged[field] = []
    seen_contracts: set[tuple[str, str, str]] = set()

    for payload in payloads or []:
        strikes = payload.get("strike") or []
        if not strikes:
            continue
        for idx in range(len(strikes)):
            expiry_series = payload.get("expiration") or []
            side_series = payload.get("side") or []
            expiry_raw = expiry_series[idx] if idx < len(expiry_series) else None
            side_raw = side_series[idx] if idx < len(side_series) else ""
            contract_key = (
                _normalize_expiry_yyyymmdd(expiry_raw) or str(expiry_raw or ""),
                str(strikes[idx]),
                str(side_raw).lower(),
            )
            if contract_key in seen_contracts:
                continue
            seen_contracts.add(contract_key)
            for field in _MARKETDATA_ROW_FIELDS:
                series = payload.get(field) or []
                series_list = series if isinstance(series, list) else []
                merged[field].append(series_list[idx] if idx < len(series_list) else None)
    return merged


def fetch_marketdata_chain(
    symbol: str,
    snapshot_date: date,
    timeout_sec: int,
    *,
    max_attempts: int = DEFAULT_HTTP_MAX_ATTEMPTS,
    retry_base_sec: float = DEFAULT_HTTP_RETRY_BASE_SEC,
    retry_max_sec: float = DEFAULT_HTTP_RETRY_MAX_SEC,
    retry_jitter_sec: float = DEFAULT_HTTP_RETRY_JITTER_SEC,
) -> dict:
    def _request(url: str) -> dict:
        for attempt in range(max_attempts):
            req = Request(
                url,
                headers={
                    "Authorization": f"Token {MARKETDATA_APP_TOKEN}",
                    "User-Agent": "PivotQuantGammaHistory/1.0",
                },
            )
            try:
                with urlopen(req, timeout=timeout_sec) as resp:
                    payload = json.loads(resp.read())
                if payload.get("s") != "ok":
                    raise RuntimeError(
                        f"marketdata.app chain error for {symbol} {snapshot_date}: "
                        f"{payload.get('errmsg', payload.get('s', 'unknown'))}"
                    )
                return payload
            except HTTPError as exc:
                if attempt + 1 >= max_attempts or exc.code not in TRANSIENT_HTTP_CODES:
                    raise
                retry_after = _parse_retry_after_seconds(
                    exc.headers.get("Retry-After") if getattr(exc, "headers", None) else None
                )
                wait = _retry_sleep_seconds(
                    attempt_idx=attempt,
                    retry_after_sec=retry_after,
                    retry_base_sec=retry_base_sec,
                    retry_max_sec=retry_max_sec,
                    retry_jitter_sec=retry_jitter_sec,
                )
                if wait > 0:
                    time.sleep(wait)
            except (URLError, TimeoutError):
                if attempt + 1 >= max_attempts:
                    raise
                wait = _retry_sleep_seconds(
                    attempt_idx=attempt,
                    retry_after_sec=None,
                    retry_base_sec=retry_base_sec,
                    retry_max_sec=retry_max_sec,
                    retry_jitter_sec=retry_jitter_sec,
                )
                if wait > 0:
                    time.sleep(wait)
        raise RuntimeError(f"marketdata.app retry loop exhausted for {symbol} {snapshot_date}")

    params = {"date": snapshot_date.strftime("%Y-%m-%d")}
    url = f"{MARKETDATA_APP_BASE}/options/chain/{symbol.upper()}/?{urlencode(params)}"
    try:
        return _request(url)
    except HTTPError as exc:
        # Same-day date queries can fail with 400 for some accounts.
        # Fall back to a DTE-filtered live chain endpoint so today's snapshot can
        # still persist without consuming full-chain credits. For structural
        # 90DTE mode we query a narrow DTE bracket and merge unique contracts so
        # the selector can choose the true nearest forward expiry.
        if exc.code != 400:
            raise
        dte_queries = _marketdata_live_dte_queries(GAMMA_HISTORY_EXPIRY_MODE, GAMMA_HISTORY_LIVE_DTE_DAYS)
        payloads: list[dict] = []
        errors: list[str] = []
        for dte_query in dte_queries:
            live_url = f"{MARKETDATA_APP_BASE}/options/chain/{symbol.upper()}/?dte={dte_query}"
            try:
                payloads.append(_request(live_url))
            except Exception as live_exc:
                errors.append(f"dte={dte_query}: {live_exc}")
        if not payloads:
            raise RuntimeError(
                f"marketdata.app live fallback failed for {symbol} {snapshot_date}: {'; '.join(errors) or 'no payloads'}"
            )
        merged = _merge_marketdata_chain_payloads(payloads) if len(payloads) > 1 else payloads[0]
        merged["dteQueries"] = dte_queries
        if errors:
            merged["partialFetchWarnings"] = errors
        return merged


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


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _to_date(raw: object) -> date | None:
    if raw is None:
        return None

    # Handle unix timestamps (seconds/ms) from some option-chain payloads.
    if isinstance(raw, (int, float)):
        try:
            ts = float(raw)
            if ts > 10_000_000_000:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, timezone.utc).date()
        except Exception:
            pass

    text = str(raw).strip()
    if not text:
        return None

    if text.isdigit():
        try:
            ts = float(text)
            if ts > 10_000_000_000:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, timezone.utc).date()
        except Exception:
            pass

    try:
        return parse_yyyy_mm_dd(text[:10])
    except Exception:
        return None


def _normalize_expiry_yyyymmdd(raw: object) -> str | None:
    expiry_date = _to_date(raw)
    return expiry_date.strftime("%Y%m%d") if expiry_date is not None else None


def _is_monthly_expiry(expiry_date: date) -> bool:
    return 15 <= expiry_date.day <= 21 and expiry_date.weekday() == 4


def _normalize_expiry_mode(mode: str) -> str:
    safe_mode = str(mode or "90dte").strip().lower()
    if safe_mode == "quarterly":
        raise ValueError("expiry_mode=quarterly is no longer supported; use 90dte")
    if safe_mode not in {"0dte", "front", "monthly", "all", "90dte", "aggregate_90dte"}:
        raise ValueError(f"Unsupported expiry_mode={safe_mode!r}")
    return safe_mode


def _pick_target_dte_expiry(expiries: list[str], today: date, target_days: int) -> str | None:
    candidates: list[tuple[int, int, str]] = []
    for exp in expiries:
        expiry_date = parse_yyyy_mm_dd(exp[:4] + "-" + exp[4:6] + "-" + exp[6:8])
        dte_days = (expiry_date - today).days
        if dte_days < 0:
            continue
        candidates.append((abs(dte_days - target_days), dte_days, exp))
    if candidates:
        candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        return candidates[0][2]
    return None


def _pick_chain_expiries(expiries: list[object], mode: str, today: date) -> set[str]:
    if _normalize_expiry_mode(str(mode or "")) == "all":
        return set()

    normalized: list[str] = []
    seen = set()
    for raw in expiries or []:
        compact = _normalize_expiry_yyyymmdd(raw)
        if compact is None or compact in seen:
            continue
        seen.add(compact)
        normalized.append(compact)
    if not normalized:
        return set()

    normalized.sort()
    today_compact = today.strftime("%Y%m%d")
    safe_mode = _normalize_expiry_mode(mode or "90dte")

    if safe_mode == "0dte":
        return {today_compact if today_compact in seen else normalized[0]}

    if safe_mode == "front":
        future = [exp for exp in normalized if exp != today_compact]
        return {future[0] if future else normalized[0]}

    if safe_mode == "90dte":
        target = _pick_target_dte_expiry(normalized, today, 90)
        return {target} if target else set()

    if safe_mode == "aggregate_90dte":
        # Include every forward expiry within the 90DTE structural window.
        result: set[str] = set()
        for exp in normalized:
            if exp < today_compact:
                continue
            exp_date = parse_yyyy_mm_dd(exp[:4] + "-" + exp[4:6] + "-" + exp[6:8])
            dte = (exp_date - today).days
            if 0 <= dte <= 90:
                result.add(exp)
        return result

    if safe_mode == "monthly":
        monthly = [
            exp for exp in normalized
            if (exp >= today_compact) and _is_monthly_expiry(parse_yyyy_mm_dd(exp[:4] + "-" + exp[4:6] + "-" + exp[6:8]))
        ]
        if monthly:
            return {monthly[0]}
        fallback = [
            exp for exp in normalized
            if _is_monthly_expiry(parse_yyyy_mm_dd(exp[:4] + "-" + exp[4:6] + "-" + exp[6:8]))
        ]
        return {fallback[0] if fallback else normalized[0]}

    future = [exp for exp in normalized if exp >= today_compact]
    return {future[0] if future else normalized[0]}


def _option_mid_price(
    idx: int,
    bids: list,
    asks: list,
    lasts: list,
    marks: list,
    closes: list,
) -> float | None:
    bid = _to_float(bids[idx] if idx < len(bids) else None)
    ask = _to_float(asks[idx] if idx < len(asks) else None)
    if bid is not None and ask is not None and bid > 0 and ask > 0 and ask >= bid:
        mid = (bid + ask) / 2.0
        if mid > 0:
            return mid
    for series in (marks, lasts, closes):
        px = _to_float(series[idx] if idx < len(series) else None)
        if px is not None and px > 0:
            return px
    return None


def _bsm_price(
    is_call: bool,
    s: float,
    k: float,
    t: float,
    r: float,
    q: float,
    sigma: float,
) -> float | None:
    if s <= 0 or k <= 0 or t <= 0 or sigma <= 0:
        return None
    sqrt_t = math.sqrt(t)
    d1 = (math.log(s / k) + (r - q + 0.5 * sigma * sigma) * t) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    disc_r = math.exp(-r * t)
    disc_q = math.exp(-q * t)
    if is_call:
        return s * disc_q * _norm_cdf(d1) - k * disc_r * _norm_cdf(d2)
    return k * disc_r * _norm_cdf(-d2) - s * disc_q * _norm_cdf(-d1)


def _solve_implied_vol(
    is_call: bool,
    target_price: float,
    s: float,
    k: float,
    t: float,
    r: float,
    q: float,
) -> float | None:
    if target_price <= 0 or s <= 0 or k <= 0 or t <= 0:
        return None
    intrinsic = max(0.0, s - k) if is_call else max(0.0, k - s)
    if target_price < intrinsic:
        return None

    lo = GAMMA_COMPUTE_FALLBACK_IV_MIN
    hi = GAMMA_COMPUTE_FALLBACK_IV_MAX
    px_lo = _bsm_price(is_call, s, k, t, r, q, lo)
    px_hi = _bsm_price(is_call, s, k, t, r, q, hi)
    if px_lo is None or px_hi is None:
        return None
    if target_price < px_lo or target_price > px_hi:
        return None

    for _ in range(GAMMA_COMPUTE_FALLBACK_MAX_IV_ITERS):
        mid = (lo + hi) / 2.0
        px_mid = _bsm_price(is_call, s, k, t, r, q, mid)
        if px_mid is None:
            return None
        err = px_mid - target_price
        if abs(err) < 1e-5:
            return mid
        if err > 0:
            hi = mid
        else:
            lo = mid
    return (lo + hi) / 2.0


def _bsm_gamma(
    s: float,
    k: float,
    t: float,
    r: float,
    q: float,
    sigma: float,
) -> float | None:
    if s <= 0 or k <= 0 or t <= 0 or sigma <= 0:
        return None
    sqrt_t = math.sqrt(t)
    d1 = (math.log(s / k) + (r - q + 0.5 * sigma * sigma) * t) / (sigma * sqrt_t)
    disc_q = math.exp(-q * t)
    gamma = (disc_q * _norm_pdf(d1)) / (s * sigma * sqrt_t)
    if not math.isfinite(gamma) or gamma <= 0:
        return None
    return gamma


def _derive_gamma_levels(
    gex_by_strike: dict[float, float],
    call_gex_by_strike: dict[float, float],
    put_gex_by_strike: dict[float, float],
) -> tuple[float | None, bool, str, float | None, float | None, float | None]:
    ordered = sorted(gex_by_strike.keys())
    if not ordered:
        return None, False, "crossing", None, None, None

    gamma_flip = None
    cumulative = 0.0
    last_sign: int | None = None
    is_true_crossing = False
    observed_signs: set[int] = set()
    for strike in ordered:
        cumulative += gex_by_strike[strike]
        sign = 1 if cumulative > 0 else -1 if cumulative < 0 else 0
        if sign != 0:
            observed_signs.add(sign)
        if last_sign is not None and sign != 0 and sign != last_sign:
            gamma_flip = strike
            is_true_crossing = True
            break
        last_sign = sign
    if gamma_flip is None:
        gamma_flip = min(ordered, key=lambda s: abs(gex_by_strike[s]))

    if is_true_crossing:
        gamma_regime = "crossing"
    elif observed_signs == {-1}:
        gamma_regime = "net_short"
    elif observed_signs == {1}:
        gamma_regime = "net_long"
    else:
        gamma_regime = "crossing"

    call_wall = max(call_gex_by_strike, key=call_gex_by_strike.get) if call_gex_by_strike else None
    put_wall = min(put_gex_by_strike, key=put_gex_by_strike.get) if put_gex_by_strike else None
    pin = max(ordered, key=lambda s: abs(gex_by_strike[s]))
    return gamma_flip, is_true_crossing, gamma_regime, call_wall, put_wall, pin


def summarize_chain(
    symbol: str,
    snapshot_date: date,
    chain: dict,
    strike_range_pct: float,
    max_strikes: int,
    *,
    expiry_mode: str = GAMMA_HISTORY_EXPIRY_MODE,
) -> dict:
    strikes = chain.get("strike") or []
    sides = chain.get("side") or []
    gammas = chain.get("gamma") or []
    ivs = chain.get("iv") or []
    ois = chain.get("openInterest") or []
    deltas = chain.get("delta") or []
    expiries = chain.get("expiration") or []
    underlyings = chain.get("underlyingPrice") or []
    bids = chain.get("bid") or []
    asks = chain.get("ask") or []
    lasts = chain.get("last") or chain.get("lastPrice") or []
    marks = chain.get("mid") or chain.get("mark") or []
    closes = chain.get("close") or []

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
    call_gex_by_strike: dict[float, float] = {}
    put_gex_by_strike: dict[float, float] = {}
    oi_by_strike: dict[float, float] = {}
    total_contracts = 0
    with_greeks = 0
    with_iv = 0
    with_oi = 0
    computed_greeks = 0
    computed_from_iv = 0
    computed_from_price = 0
    oi_call = 0.0
    oi_put = 0.0
    zero_dte_oi = 0.0
    total_oi = 0.0
    iv_samples: list[tuple[float, str, float | None, float]] = []
    selected_expiries = _pick_chain_expiries(expiries, expiry_mode, snapshot_date)
    if _normalize_expiry_mode(expiry_mode or GAMMA_HISTORY_EXPIRY_MODE) == "90dte" and not selected_expiries:
        raise ValueError("No valid forward 90DTE expiry available in options chain")

    multiplier = 100.0
    target_date = snapshot_date

    for i in range(len(strikes)):
        strike = _to_float(strikes[i])
        if strike is None or strike < low or strike > high:
            continue

        total_contracts += 1
        side = str((sides[i] if i < len(sides) else "") or "").lower()
        gamma_provider = _to_float(gammas[i] if i < len(gammas) else None)
        gamma = gamma_provider
        iv = _to_float(ivs[i] if i < len(ivs) else None)
        oi = _to_float(ois[i] if i < len(ois) else None)
        delta = _to_float(deltas[i] if i < len(deltas) else None)
        expiry_raw = expiries[i] if i < len(expiries) else None
        expiry_compact = _normalize_expiry_yyyymmdd(expiry_raw)
        if selected_expiries and expiry_compact not in selected_expiries:
            continue

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

        if gamma is not None:
            with_greeks += 1
        elif GAMMA_COMPUTE_FALLBACK and side in {"call", "put"}:
            expiry_date = _to_date(expiry_raw)
            if expiry_date is not None:
                dte_days = (expiry_date - snapshot_date).days
                if dte_days >= GAMMA_COMPUTE_FALLBACK_MIN_DTE_DAYS:
                    t = dte_days / 365.0
                    sigma = iv if (iv is not None and iv > 0) else None
                    sigma_source = "iv" if sigma is not None else None
                    if sigma is None and GAMMA_COMPUTE_FALLBACK_SOLVE_IV:
                        option_px = _option_mid_price(i, bids, asks, lasts, marks, closes)
                        if option_px is not None:
                            sigma = _solve_implied_vol(
                                is_call=(side == "call"),
                                target_price=option_px,
                                s=spot,
                                k=strike,
                                t=t,
                                r=GAMMA_COMPUTE_FALLBACK_RISK_FREE_RATE,
                                q=GAMMA_COMPUTE_FALLBACK_DIVIDEND_YIELD,
                            )
                            if sigma is not None:
                                sigma_source = "price"
                    if sigma is not None and sigma > 0:
                        gamma = _bsm_gamma(
                            s=spot,
                            k=strike,
                            t=t,
                            r=GAMMA_COMPUTE_FALLBACK_RISK_FREE_RATE,
                            q=GAMMA_COMPUTE_FALLBACK_DIVIDEND_YIELD,
                            sigma=sigma,
                        )
                        if gamma is not None:
                            computed_greeks += 1
                            if sigma_source == "iv":
                                computed_from_iv += 1
                            elif sigma_source == "price":
                                computed_from_price += 1

        if gamma is None:
            continue
        size = oi if oi is not None else 1.0
        gex = gamma * size * multiplier * (spot ** 2)
        if side == "put":
            gex = -gex
        gex_by_strike[strike] = gex_by_strike.get(strike, 0.0) + gex
        if side == "call":
            call_gex_by_strike[strike] = call_gex_by_strike.get(strike, 0.0) + gex
        elif side == "put":
            put_gex_by_strike[strike] = put_gex_by_strike.get(strike, 0.0) + gex

    gamma_flip = None
    gamma_flip_is_true_crossing = False
    gamma_regime = "crossing"
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
            call_gex_by_strike = {k: v for k, v in call_gex_by_strike.items() if k in keep}
            put_gex_by_strike = {k: v for k, v in put_gex_by_strike.items() if k in keep}
            ordered = sorted(gex_by_strike.keys())

        gamma_flip, gamma_flip_is_true_crossing, gamma_regime, call_wall, put_wall, pin = _derive_gamma_levels(
            gex_by_strike,
            call_gex_by_strike,
            put_gex_by_strike,
        )

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
        "gamma_flip_is_true_crossing": 1 if gamma_flip_is_true_crossing else 0,
        "gamma_regime": gamma_regime,
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
                "expiry_mode": _normalize_expiry_mode(expiry_mode or GAMMA_HISTORY_EXPIRY_MODE),
                "selected_expiries": sorted(selected_expiries),
                "dte_window_days": GAMMA_HISTORY_LIVE_DTE_DAYS,
                "spot": spot,
                "gamma_flip_is_true_crossing": bool(gamma_flip_is_true_crossing),
                "gamma_regime": gamma_regime,
                "contracts": len(strikes),
                "filtered_contracts": total_contracts,
                "compute_fallback_enabled": bool(GAMMA_COMPUTE_FALLBACK),
                "computed_gamma_count": computed_greeks,
                "computed_gamma_from_iv": computed_from_iv,
                "computed_gamma_from_price": computed_from_price,
                "provider_gamma_count": with_greeks,
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
    parser.add_argument("--http-max-attempts", type=int, default=DEFAULT_HTTP_MAX_ATTEMPTS)
    parser.add_argument("--http-retry-base-sec", type=float, default=DEFAULT_HTTP_RETRY_BASE_SEC)
    parser.add_argument("--http-retry-max-sec", type=float, default=DEFAULT_HTTP_RETRY_MAX_SEC)
    parser.add_argument("--http-retry-jitter-sec", type=float, default=DEFAULT_HTTP_RETRY_JITTER_SEC)
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
    max_attempts = max(1, int(args.http_max_attempts))
    retry_base_sec = max(0.0, float(args.http_retry_base_sec))
    retry_max_sec = max(retry_base_sec, float(args.http_retry_max_sec))
    retry_jitter_sec = max(0.0, float(args.http_retry_jitter_sec))

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute(f"PRAGMA synchronous={GAMMA_HISTORY_SQLITE_SYNC};")
        conn.execute(f"PRAGMA wal_autocheckpoint={GAMMA_HISTORY_WAL_AUTOCHECKPOINT};")
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
                    chain = fetch_marketdata_chain(
                        symbol,
                        d,
                        timeout_sec=args.timeout_sec,
                        max_attempts=max_attempts,
                        retry_base_sec=retry_base_sec,
                        retry_max_sec=retry_max_sec,
                        retry_jitter_sec=retry_jitter_sec,
                    )
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
