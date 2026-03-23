import json
import os
import threading
import time
import urllib.error
import urllib.request
from copy import deepcopy
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None

from ib_insync import IB, Index, Option, Stock, ContFuture

IB_HOST = os.getenv("IB_HOST", "127.0.0.1")
IB_PORT = int(os.getenv("IB_PORT", "4002"))
IB_CLIENT_ID = int(os.getenv("IB_CLIENT_ID", "12"))
IB_BRIDGE_BIND = os.getenv("IB_BRIDGE_BIND", "127.0.0.1")
IB_BRIDGE_PORT = int(os.getenv("IB_BRIDGE_PORT", "5001"))
IB_EXCHANGE = os.getenv("IB_EXCHANGE", "CBOE")
IB_MAX_STRIKES = int(os.getenv("IB_MAX_STRIKES", "60"))
IB_STRIKE_RANGE = float(os.getenv("IB_STRIKE_RANGE", "0.05"))  # +/- 5%
IB_EXPIRY_MODE = os.getenv("IB_EXPIRY_MODE", "quarterly")  # quarterly structural default
IB_MAX_EXPIRIES = int(os.getenv("IB_MAX_EXPIRIES", "1"))
IB_WEIGHT_0DTE = float(os.getenv("IB_WEIGHT_0DTE", "1.0"))
IB_WEIGHT_FRONT = float(os.getenv("IB_WEIGHT_FRONT", "0.6"))
IB_WEIGHT_MONTHLY = float(os.getenv("IB_WEIGHT_MONTHLY", "0.35"))
IB_WEIGHT_OTHER = float(os.getenv("IB_WEIGHT_OTHER", "0.2"))
IB_USE_RTH = os.getenv("IB_USE_RTH", "1") != "0"
IB_DATA_TYPE = os.getenv("IB_DATA_TYPE", "").strip()
IB_CONNECT_TIMEOUT_SEC = max(0.5, float(os.getenv("IB_CONNECT_TIMEOUT_SEC", "5.0")))
IB_CONNECT_RETRY_BACKOFF_SEC = max(1.0, float(os.getenv("IB_CONNECT_RETRY_BACKOFF_SEC", "5.0")))
MARKETDATA_APP_TOKEN = os.getenv("MARKETDATA_APP_TOKEN", "").strip()
MARKETDATA_APP_BASE = "https://api.marketdata.app/v1"
# Limit options chain to options expiring within this many days.
# Keeps ~4-6 near-term weekly expiries instead of the full chain (~20 expiries).
# Reduces per-call credit cost from ~8,000 → ~1,500 rows.
# Keep the fetch window wider than the target expiry family so the nearest
# quarterly contract is still present when it sits a few days beyond 90DTE.
MDA_GAMMA_DTE_DAYS = int(os.getenv("MDA_GAMMA_DTE_DAYS", "120"))
# Cache gamma results for this many seconds to avoid burning credits on every
# dashboard auto-refresh (default 60s). 1800s = 30 min; tune down to 300 if
# you want faster reaction to intraday gamma shifts.
MDA_GAMMA_CACHE_TTL_SEC = int(os.getenv("MDA_GAMMA_CACHE_TTL_SEC", "1800"))
# After an upstream marketdata.app failure (for example 429), suppress new
# upstream requests for this many seconds per symbol and serve stale cache when
# available to avoid a request loop.
MDA_GAMMA_ERROR_BACKOFF_SEC = int(os.getenv("MDA_GAMMA_ERROR_BACKOFF_SEC", "900"))
_DEFAULT_CORS_ORIGINS = "http://127.0.0.1:3000,http://localhost:3000"

# In-process gamma cache: symbol -> (payload_dict, expires_monotonic_sec)
_mda_gamma_cache: dict = {}
_mda_gamma_cache_lock = threading.Lock()
_mda_gamma_error_backoff_until: dict = {}
NY_TZ = ZoneInfo("America/New_York") if ZoneInfo else None

ib = IB()
ib_lock = threading.RLock()
_ib_connect_backoff_until_mono = 0.0


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso_z() -> str:
    return _utc_now().isoformat().replace("+00:00", "Z")


def _utc_today_yyyymmdd() -> str:
    return _utc_now().strftime("%Y%m%d")


def _parse_retry_after_seconds(raw_value) -> float | None:
    if raw_value is None:
        return None
    text = str(raw_value).strip()
    if not text:
        return None
    try:
        seconds = float(text)
        if seconds > 0:
            return seconds
    except Exception:
        try:
            dt = parsedate_to_datetime(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            seconds = (dt - _utc_now()).total_seconds()
            if seconds > 0:
                return seconds
        except Exception:
            return None
    return None


def _is_market_session_closed(now_utc: datetime) -> bool:
    if now_utc.weekday() >= 5:
        return True
    if NY_TZ is None:
        # Fallback without zoneinfo: conservative close estimate.
        return now_utc.hour >= 21
    now_et = now_utc.astimezone(NY_TZ)
    return now_et.hour > 16 or (now_et.hour == 16 and now_et.minute >= 0)


def _parse_allowed_origins() -> list[str]:
    origins = [
        origin.strip()
        for origin in os.getenv("ML_CORS_ORIGINS", _DEFAULT_CORS_ORIGINS).split(",")
        if origin.strip()
    ]
    return origins or ["http://127.0.0.1:3000"]


ALLOWED_ORIGINS = _parse_allowed_origins()


def _cors_origin(request_origin: str | None) -> str:
    if request_origin and request_origin in ALLOWED_ORIGINS:
        return request_origin
    return ALLOWED_ORIGINS[0]


def _is_finite(value):
    try:
        return value is not None and float(value) == float(value)
    except Exception:
        return False


def _safe_price(*values):
    for value in values:
        if _is_finite(value):
            return float(value)
    return None


def _request_market_data_type(data_type):
    try:
        ib.reqMarketDataType(data_type)
    except Exception:
        pass


def _fetch_ticker_price(contract):
    def _first_ticker_or_raise():
        tickers = ib.reqTickers(contract)
        if not tickers or tickers[0] is None:
            raise ValueError(f"No market data ticker returned for {contract}")
        return tickers[0]

    # Prefer explicit data type if provided.
    if IB_DATA_TYPE:
        try:
            _request_market_data_type(int(IB_DATA_TYPE))
        except ValueError:
            _request_market_data_type(1)
    else:
        _request_market_data_type(1)

    ticker = _first_ticker_or_raise()
    price = _safe_price(ticker.marketPrice(), ticker.last, ticker.close)
    if _is_finite(price):
        return price

    # Fallback to delayed if realtime unavailable.
    _request_market_data_type(3)
    ticker = _first_ticker_or_raise()
    price = _safe_price(ticker.marketPrice(), ticker.last, ticker.close)
    return price


def ensure_connected():
    global _ib_connect_backoff_until_mono
    with ib_lock:
        if ib.isConnected():
            return
        now_mono = time.monotonic()
        if _ib_connect_backoff_until_mono > now_mono:
            remaining = int(_ib_connect_backoff_until_mono - now_mono)
            raise RuntimeError(
                f"IBKR reconnect cooldown active ({remaining}s remaining)"
            )
        try:
            ib.connect(
                IB_HOST,
                IB_PORT,
                clientId=IB_CLIENT_ID,
                timeout=IB_CONNECT_TIMEOUT_SEC,
            )
        except Exception:
            _ib_connect_backoff_until_mono = now_mono + IB_CONNECT_RETRY_BACKOFF_SEC
            raise
        _ib_connect_backoff_until_mono = 0.0


def fetch_spot(symbol):
    ensure_connected()
    contract = get_underlying(symbol)
    ib.qualifyContracts(contract)
    spot = _fetch_ticker_price(contract)
    if not spot and symbol.upper() == "SPX":
        es_contract = get_es_fallback()
        ib.qualifyContracts(es_contract)
        spot = _fetch_ticker_price(es_contract)
    if not spot:
        raise ValueError("Spot price unavailable")
    return float(spot)


def _parse_expiry_yyyymmdd(exp):
    try:
        return datetime.strptime(str(exp), "%Y%m%d")
    except ValueError:
        return None


def _parse_expiry_any(exp):
    text = str(exp or "").strip()
    if not text:
        return None
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[:10], fmt)
        except ValueError:
            continue
    return None


def _normalize_expiry_yyyymmdd(exp):
    parsed = _parse_expiry_any(exp)
    return parsed.strftime("%Y%m%d") if parsed is not None else None


def _is_monthly_expiry(dt):
    return 15 <= dt.day <= 21 and dt.weekday() == 4


def _is_quarterly_expiry(dt):
    return _is_monthly_expiry(dt) and dt.month in (3, 6, 9, 12)


def pick_expiries(expirations, mode):
    exp_list = sorted(expirations)
    if not exp_list:
        return []

    today = _utc_today_yyyymmdd()

    if mode == "0dte":
        if today in exp_list:
            return [today]
        return [exp_list[0]]

    if mode == "front":
        for exp in exp_list:
            if exp != today:
                return [exp]
        return [exp_list[0]]

    if mode == "monthly":
        for exp in exp_list:
            dt = _parse_expiry_yyyymmdd(exp)
            if dt is None:
                continue
            if exp >= today and _is_monthly_expiry(dt):
                return [exp]
        for exp in exp_list:
            dt = _parse_expiry_yyyymmdd(exp)
            if dt is None:
                continue
            if _is_monthly_expiry(dt):
                return [exp]
        return [exp_list[0]]

    if mode == "quarterly":
        for exp in exp_list:
            dt = _parse_expiry_yyyymmdd(exp)
            if dt is None:
                continue
            if exp >= today and _is_quarterly_expiry(dt):
                return [exp]
        for exp in exp_list:
            dt = _parse_expiry_yyyymmdd(exp)
            if dt is None:
                continue
            if _is_quarterly_expiry(dt):
                return [exp]
        # Fallback to monthly/front behavior if no quarterly expiry is present.
        monthly = pick_expiries(exp_list, "monthly")
        return monthly if monthly else pick_expiries(exp_list, "front")

    if mode == "all":
        return exp_list[: max(1, IB_MAX_EXPIRIES)]

    # front-week default
    return [exp_list[0]]


_ETF_SYMBOLS = {"SPY", "QQQ", "IWM", "DIA", "XLF", "XLE", "XLK", "GLD", "TLT", "HYG", "EEM"}
_INDEX_SYMBOLS = {"SPX", "NDX", "RUT", "VIX", "DJX"}


def get_underlying(symbol):
    sym = symbol.upper()
    if sym in _INDEX_SYMBOLS:
        return Index(sym, IB_EXCHANGE, "USD")
    # ETFs (including all standard equity ETFs) use Stock contract
    return Stock(sym, "SMART", "USD")


def get_es_fallback():
    return ContFuture("ES", "CME")


def select_strikes(strikes, spot):
    strikes = sorted([float(s) for s in strikes])
    if not strikes:
        return []
    lower = spot * (1 - IB_STRIKE_RANGE)
    upper = spot * (1 + IB_STRIKE_RANGE)
    filtered = [s for s in strikes if lower <= s <= upper]
    if len(filtered) <= IB_MAX_STRIKES:
        return filtered

    # Take centered window around spot
    nearest_idx = min(range(len(filtered)), key=lambda i: abs(filtered[i] - spot))
    half = IB_MAX_STRIKES // 2
    start = max(0, nearest_idx - half)
    end = min(len(filtered), start + IB_MAX_STRIKES)
    return filtered[start:end]


def expiry_type(expiration, today, front_expiry):
    if expiration == today:
        return "0dte"
    if expiration == front_expiry:
        return "front"
    try:
        dt = datetime.strptime(expiration, "%Y%m%d")
        if 15 <= dt.day <= 21 and dt.weekday() == 4:
            return "monthly"
    except ValueError:
        pass
    return "other"


def compute_gamma_walls(symbol, expiry_mode, limit):
    ensure_connected()

    underlying = get_underlying(symbol)
    ib.qualifyContracts(underlying)

    spot = _fetch_ticker_price(underlying)
    if not spot:
        raise ValueError("Unable to determine underlying price")

    opt_params = ib.reqSecDefOptParams(underlying.symbol, "", underlying.secType, underlying.conId)
    if not opt_params:
        raise ValueError("No option chain available")

    # Prefer params matching exchange/trading class
    opt_param = None
    for param in opt_params:
        if param.exchange == IB_EXCHANGE:
            opt_param = param
            break
    if not opt_param:
        opt_param = opt_params[0]

    today = _utc_today_yyyymmdd()
    front_expiry = pick_expiries(opt_param.expirations, "front")[0]
    expiries = pick_expiries(opt_param.expirations, expiry_mode)
    if expiry_mode == "all":
        expiries = sorted(opt_param.expirations)[: max(1, IB_MAX_EXPIRIES)]
    strikes = select_strikes(opt_param.strikes, spot)
    strikes = strikes[:limit]

    contracts = []
    expiry_weights = {}
    for exp in expiries:
        e_type = expiry_type(exp, today, front_expiry)
        weight = {
            "0dte": IB_WEIGHT_0DTE,
            "front": IB_WEIGHT_FRONT,
            "monthly": IB_WEIGHT_MONTHLY,
            "other": IB_WEIGHT_OTHER,
        }.get(e_type, IB_WEIGHT_OTHER)
        expiry_weights[exp] = weight

        for strike in strikes:
            contracts.append(
                Option(
                    underlying.symbol,
                    exp,
                    strike,
                    "C",
                    opt_param.exchange or IB_EXCHANGE,
                    tradingClass=getattr(opt_param, "tradingClass", None),
                    currency="USD",
                    multiplier="100",
                )
            )
            contracts.append(
                Option(
                    underlying.symbol,
                    exp,
                    strike,
                    "P",
                    opt_param.exchange or IB_EXCHANGE,
                    tradingClass=getattr(opt_param, "tradingClass", None),
                    currency="USD",
                    multiplier="100",
                )
            )

    ib.qualifyContracts(*contracts)

    # Prefer realtime greeks, but gracefully fall back to delayed feeds when
    # the account lacks live options subscriptions (common on paper accounts).
    data_type_attempts = []
    if IB_DATA_TYPE:
        try:
            data_type_attempts.append(int(IB_DATA_TYPE))
        except ValueError:
            pass
    for candidate in (1, 3, 4):
        if candidate not in data_type_attempts:
            data_type_attempts.append(candidate)

    tickers = []
    used_data_type = None
    for data_type in data_type_attempts:
        _request_market_data_type(data_type)
        tickers = ib.reqTickers(*contracts)
        used_data_type = data_type
        if any((t.modelGreeks is not None or getattr(t, "delayedGreeks", None) is not None) for t in tickers):
            break

    gex_by_strike = {}
    max_abs = 0.0
    used_oi = False
    total_contracts = 0
    with_greeks = 0
    with_iv = 0
    with_oi = 0
    oi_call = 0.0
    oi_put = 0.0
    oi_by_strike = {}
    oi_by_expiry = {}
    iv_samples = []

    for t in tickers:
        total_contracts += 1
        greeks = t.modelGreeks or getattr(t, "delayedGreeks", None)
        if not greeks or greeks.gamma is None:
            continue

        with_greeks += 1
        strike = float(t.contract.strike)
        right = t.contract.right
        gamma = greeks.gamma
        multiplier = float(t.contract.multiplier or 100)
        expiry = t.contract.lastTradeDateOrContractMonth

        iv = getattr(greeks, "impliedVol", None)
        if iv is not None:
            with_iv += 1
            iv_samples.append((strike, right, greeks.delta, iv))

        oi = getattr(t, "openInterest", None)
        if oi is None:
            oi = getattr(t, "putOpenInterest", None)
        if oi is None:
            oi = getattr(t, "callOpenInterest", None)

        if oi is not None:
            used_oi = True
            with_oi += 1
        size = oi if oi is not None else (t.volume or 1)

        weight = expiry_weights.get(expiry, 1.0)
        gex = gamma * size * multiplier * (spot ** 2) * weight
        if right.upper() == "P":
            gex = -gex

        gex_by_strike[strike] = gex_by_strike.get(strike, 0.0) + gex
        max_abs = max(max_abs, abs(gex_by_strike[strike]))

        oi_by_strike[strike] = oi_by_strike.get(strike, 0.0) + (size or 0)
        oi_by_expiry[expiry] = oi_by_expiry.get(expiry, 0.0) + (size or 0)
        if right.upper() == "C":
            oi_call += size or 0
        else:
            oi_put += size or 0

    if not gex_by_strike:
        raise ValueError(
            f"No gamma data available from IBKR (market_data_type={used_data_type}). "
            "Check IBKR options market-data permissions or use delayed options greeks."
        )

    sorted_strikes = sorted(gex_by_strike.keys())
    cumulative = 0.0
    flip = None
    last_sign = None

    for strike in sorted_strikes:
        cumulative += gex_by_strike[strike]
        sign = 1 if cumulative > 0 else -1 if cumulative < 0 else 0
        if last_sign is not None and sign != last_sign and sign != 0:
            flip = strike
            break
        last_sign = sign

    if flip is None:
        flip = min(sorted_strikes, key=lambda s: abs(gex_by_strike[s]))

    call_wall = max(sorted_strikes, key=lambda s: gex_by_strike[s])
    put_wall = min(sorted_strikes, key=lambda s: gex_by_strike[s])
    pin = max(sorted_strikes, key=lambda s: abs(gex_by_strike[s]))

    def wall_payload(strike):
        gex = gex_by_strike[strike]
        strength = round(abs(gex) / max_abs * 100) if max_abs else 0
        return {"price": strike, "gex": gex, "strength": strength}

    total_oi = oi_call + oi_put
    top_strikes = sorted(oi_by_strike.items(), key=lambda kv: kv[1], reverse=True)[:5]
    top_oi = sum(v for _, v in top_strikes)
    oi_concentration = round((top_oi / total_oi) * 100, 2) if total_oi else 0
    zero_dte_oi = oi_by_expiry.get(today, 0.0)
    zero_dte_share = round((zero_dte_oi / total_oi) * 100, 2) if total_oi else 0

    atm_iv = None
    if iv_samples:
        atm = min(iv_samples, key=lambda x: abs(x[0] - spot))
        atm_iv = atm[3]

    call_iv = None
    put_iv = None
    call_candidates = [s for s in iv_samples if s[1] == "C" and s[2] is not None]
    put_candidates = [s for s in iv_samples if s[1] == "P" and s[2] is not None]
    if call_candidates:
        call_iv = min(call_candidates, key=lambda x: abs(x[2] - 0.25))[3]
    if put_candidates:
        put_iv = min(put_candidates, key=lambda x: abs(x[2] + 0.25))[3]
    skew = (put_iv - call_iv) if (put_iv is not None and call_iv is not None) else None

    return {
        "source": "IBKR",
        "symbol": symbol.upper(),
        "spot": spot,
        "expiryMode": expiry_mode,
        "generatedAt": _utc_iso_z(),
        "gammaFlip": flip,
        "callWall": wall_payload(call_wall),
        "putWall": wall_payload(put_wall),
        "pin": wall_payload(pin),
        "usedOpenInterest": used_oi,
        "stats": {
            "totalContracts": total_contracts,
            "withGreeks": with_greeks,
            "withIV": with_iv,
            "withOI": with_oi,
            "oiCall": oi_call,
            "oiPut": oi_put,
            "oiConcentration": oi_concentration,
            "zeroDteShare": zero_dte_share,
            "atmIV": atm_iv,
            "skew25d": skew,
            "expiries": expiries,
        },
    }


_EXPIRY_MODE_DTE = {
    "0dte":      0,   # today only
    "front":     7,   # current week (Mon/Wed/Fri SPY weeklies)
    "weekly":    7,
    "monthly":  45,   # nearest monthly expiry can sit >30D out
    "quarterly": 120, # nearest quarterly expiry can sit a bit beyond 90D
}


def _selected_marketdata_expiries(expiries, mode):
    if mode == "all":
        return set()

    normalized = []
    seen = set()
    for expiry in expiries or []:
        compact = _normalize_expiry_yyyymmdd(expiry)
        if compact is None or compact in seen:
            continue
        seen.add(compact)
        normalized.append(compact)

    selected = pick_expiries(normalized, mode or IB_EXPIRY_MODE)
    return set(selected)

def fetch_gamma_marketdata(symbol, strike_range=None, max_strikes=None, expiry_mode=None):
    """Compute gamma walls from marketdata.app options chain — fallback when IBKR
    returns Error 10089 (missing options market-data subscription).

    Returns the same payload shape as compute_gamma_walls() so the dashboard
    can consume it transparently.
    """
    if not MARKETDATA_APP_TOKEN:
        raise ValueError("MARKETDATA_APP_TOKEN not set — cannot use marketdata.app fallback")

    # Map expiry_mode → DTE parameter so we fetch the right expiry.
    # Default falls back to MDA_GAMMA_DTE_DAYS env var (default 30).
    mode = (expiry_mode or "").lower()
    dte_days = _EXPIRY_MODE_DTE.get(mode, MDA_GAMMA_DTE_DAYS)

    # Serve from in-process cache if still fresh. The full options chain for
    # SPY costs ~1,500 credits per call with DTE filtering; at the dashboard's
    # 60s auto-refresh that would exhaust a 100k daily quota in under an hour.
    # Include expiry_mode in the cache key so front/monthly/quarterly are independent.
    cache_key = f"{symbol.upper()}:{mode or 'default'}"
    now_mono = time.monotonic()
    stale_payload = None
    with _mda_gamma_cache_lock:
        cached_entry = _mda_gamma_cache.get(cache_key)
        if cached_entry is not None:
            if now_mono < cached_entry[1]:
                return deepcopy(cached_entry[0])
            stale_payload = deepcopy(cached_entry[0])
        cooldown_until = float(_mda_gamma_error_backoff_until.get(cache_key, 0.0) or 0.0)
        if cooldown_until > now_mono:
            remaining_sec = int(cooldown_until - now_mono)
            if stale_payload is not None:
                stale_payload["cacheStale"] = True
                stale_payload["cacheStaleReason"] = (
                    f"marketdata cooldown active ({remaining_sec}s remaining)"
                )
                stale_payload["cacheStaleAt"] = _utc_iso_z()
                return stale_payload
            raise ValueError(
                f"marketdata.app cooldown active for {remaining_sec}s; "
                "upstream call suppressed after recent error"
            )

    sr = strike_range or IB_STRIKE_RANGE
    ms = max_strikes or IB_MAX_STRIKES

    # Limit to a bounded DTE window to avoid fetching the full chain
    # (all expiries). We still apply exact expiry-family filtering below so
    # quarterly mode resolves to the nearest quarterly contract set rather than
    # aggregating every expiry inside the DTE window.
    url = f"{MARKETDATA_APP_BASE}/options/chain/{symbol.upper()}/?dte={dte_days}"
    req = urllib.request.Request(url, headers={"Authorization": f"Token {MARKETDATA_APP_TOKEN}"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
        if data.get("s") != "ok":
            raise ValueError(
                f"marketdata.app options chain error for {symbol}: "
                f"{data.get('errmsg', data.get('s', 'unknown'))}"
            )
        with _mda_gamma_cache_lock:
            _mda_gamma_error_backoff_until.pop(cache_key, None)
    except Exception as exc:
        # marketdata.app rejects dte=0; retry with dte=1 for intraday fallback
        if dte_days == 0 and isinstance(exc, urllib.error.HTTPError) and exc.code == 400:
            print(
                "[gamma_bridge] 0DTE unavailable from marketdata.app, falling back to dte=1",
                flush=True,
            )
            fallback_url = f"{MARKETDATA_APP_BASE}/options/chain/{symbol.upper()}/?dte=1"
            fallback_req = urllib.request.Request(
                fallback_url,
                headers={"Authorization": f"Token {MARKETDATA_APP_TOKEN}"}
            )
            with urllib.request.urlopen(fallback_req, timeout=30) as resp:
                data = json.loads(resp.read())
            if data.get("s") != "ok":
                raise ValueError(
                    f"marketdata.app options chain error for {symbol}: "
                    f"{data.get('errmsg', data.get('s', 'unknown'))}"
                )
            data["dteFallback"] = 1
            data["dteFallbackReason"] = "marketdata.app rejected dte=0; used dte=1"
        else:
            backoff_sec = MDA_GAMMA_ERROR_BACKOFF_SEC
            if isinstance(exc, urllib.error.HTTPError):
                retry_after = _parse_retry_after_seconds(exc.headers.get("Retry-After"))
                if retry_after is not None:
                    # Respect upstream cooldown guidance when present.
                    backoff_sec = max(1, int(retry_after))
            with _mda_gamma_cache_lock:
                _mda_gamma_error_backoff_until[cache_key] = time.monotonic() + max(1, backoff_sec)
            if stale_payload is not None:
                stale_payload["cacheStale"] = True
                stale_payload["cacheStaleReason"] = str(exc)
                stale_payload["cacheStaleAt"] = _utc_iso_z()
                return stale_payload
            raise

    # Extract arrays from the response
    strikes = data.get("strike", [])
    sides = data.get("side", [])
    gammas = data.get("gamma", [])
    ivs = data.get("iv", [])
    ois = data.get("openInterest", [])
    deltas = data.get("delta", [])
    expiries = data.get("expiration", [])
    underlyings = data.get("underlyingPrice", [])
    dte_fallback = bool(data.get("dteFallback"))
    dte_fallback_reason = data.get("dteFallbackReason")
    if dte_fallback_reason is not None:
        dte_fallback_reason = str(dte_fallback_reason)
    selected_expiries = _selected_marketdata_expiries(expiries, mode)

    if not strikes or not gammas:
        raise ValueError("marketdata.app returned options chain with no strike/gamma data")

    # Determine spot from the underlying prices in the chain
    spot_candidates = [p for p in underlyings if p is not None]
    if not spot_candidates:
        raise ValueError("marketdata.app returned no underlying price in options chain")
    spot = float(spot_candidates[0])

    # Filter strikes within range
    lower = spot * (1 - sr)
    upper = spot * (1 + sr)

    gex_by_strike = {}
    max_abs = 0.0
    total_contracts = 0
    with_greeks = 0
    with_iv = 0
    with_oi = 0
    nonzero_oi = 0
    oi_call = 0.0
    oi_put = 0.0
    oi_by_strike = {}
    zero_dte_oi = 0.0
    expiries_seen = set()
    iv_samples = []
    multiplier = 100.0
    today_utc = datetime.now(timezone.utc).date()

    for i in range(len(strikes)):
        strike = strikes[i]
        if strike is None:
            continue
        strike = float(strike)
        if strike < lower or strike > upper:
            continue

        total_contracts += 1
        side = (sides[i] or "").lower() if i < len(sides) else ""
        gamma = gammas[i] if i < len(gammas) else None
        iv = ivs[i] if i < len(ivs) else None
        oi = ois[i] if i < len(ois) else None
        delta = deltas[i] if i < len(deltas) else None
        expiry_raw = expiries[i] if i < len(expiries) else None
        expiry_compact = _normalize_expiry_yyyymmdd(expiry_raw)
        if selected_expiries and expiry_compact not in selected_expiries:
            continue

        if gamma is None:
            continue
        gamma = float(gamma)
        with_greeks += 1

        if iv is not None:
            with_iv += 1
            iv_samples.append((strike, side, delta, float(iv)))

        # Use OI as the contract size for GEX weighting. When the API returns
        # oi=0 (field present but value zero — common when the data provider
        # does not supply open interest), fall back to size=1.0 so the GEX
        # formula uses raw gamma weighting instead of collapsing to zero and
        # producing degenerate all-zero walls.
        raw_oi = float(oi) if oi is not None else None
        size = max(raw_oi, 1.0) if raw_oi is not None else 1.0
        if raw_oi is not None:
            with_oi += 1
            if raw_oi > 0:
                nonzero_oi += 1
            oi_by_strike[strike] = oi_by_strike.get(strike, 0.0) + size

        # GEX = gamma * OI * multiplier * spot^2
        # Puts contribute negative GEX
        gex = gamma * size * multiplier * (spot ** 2)
        if side == "put":
            gex = -gex

        gex_by_strike[strike] = gex_by_strike.get(strike, 0.0) + gex
        max_abs = max(max_abs, abs(gex_by_strike[strike]))

        if side == "call":
            oi_call += size
        else:
            oi_put += size
        if expiry_compact:
            expiries_seen.add(expiry_compact)
            if oi is not None:
                try:
                    if datetime.strptime(expiry_compact, "%Y%m%d").date() == today_utc:
                        zero_dte_oi += size
                except Exception:
                    pass

    if not gex_by_strike:
        raise ValueError("marketdata.app returned options chain but no usable gamma data")

    # Trim to max_strikes centered around spot
    sorted_all = sorted(gex_by_strike.keys())
    if len(sorted_all) > ms:
        nearest_idx = min(range(len(sorted_all)), key=lambda j: abs(sorted_all[j] - spot))
        half = ms // 2
        start = max(0, nearest_idx - half)
        end = min(len(sorted_all), start + ms)
        keep = set(sorted_all[start:end])
        gex_by_strike = {k: v for k, v in gex_by_strike.items() if k in keep}
        max_abs = max(abs(v) for v in gex_by_strike.values()) if gex_by_strike else 0.0

    # Compute gamma flip, call wall, put wall, pin
    sorted_strikes = sorted(gex_by_strike.keys())
    cumulative = 0.0
    flip = None
    last_sign = None

    for strike in sorted_strikes:
        cumulative += gex_by_strike[strike]
        sign = 1 if cumulative > 0 else -1 if cumulative < 0 else 0
        if last_sign is not None and sign != last_sign and sign != 0:
            flip = strike
            break
        last_sign = sign

    if flip is None:
        flip = min(sorted_strikes, key=lambda s: abs(gex_by_strike[s]))

    call_wall = max(sorted_strikes, key=lambda s: gex_by_strike[s])
    put_wall = min(sorted_strikes, key=lambda s: gex_by_strike[s])
    pin = max(sorted_strikes, key=lambda s: abs(gex_by_strike[s]))

    def wall_payload(strike):
        gex = gex_by_strike[strike]
        strength = round(abs(gex) / max_abs * 100) if max_abs else 0
        return {"price": strike, "gex": gex, "strength": strength}

    # IV analysis
    atm_iv = None
    if iv_samples:
        atm = min(iv_samples, key=lambda x: abs(x[0] - spot))
        atm_iv = atm[3]

    call_iv = None
    put_iv = None
    call_candidates = [s for s in iv_samples if s[1] == "call" and s[2] is not None]
    put_candidates = [s for s in iv_samples if s[1] == "put" and s[2] is not None]
    if call_candidates:
        call_iv = min(call_candidates, key=lambda x: abs(float(x[2]) - 0.25))[3]
    if put_candidates:
        put_iv = min(put_candidates, key=lambda x: abs(float(x[2]) + 0.25))[3]
    skew = (put_iv - call_iv) if (put_iv is not None and call_iv is not None) else None

    total_oi = oi_call + oi_put
    top_oi = sum(sorted(oi_by_strike.values(), reverse=True)[:5]) if oi_by_strike else 0.0
    oi_concentration = round((top_oi / total_oi) * 100, 2) if total_oi else None
    zero_dte_share = round((zero_dte_oi / total_oi) * 100, 2) if total_oi else None

    payload = {
        "source": "marketdata.app",
        "symbol": symbol.upper(),
        "spot": spot,
        "expiryMode": mode or "front",
        "dteFallback": dte_fallback,
        "dteFallbackReason": dte_fallback_reason,
        "generatedAt": _utc_iso_z(),
        "gammaFlip": flip,
        "callWall": wall_payload(call_wall),
        "putWall": wall_payload(put_wall),
        "pin": wall_payload(pin),
        "usedOpenInterest": nonzero_oi > 0,
        "gammaOnlyMode": nonzero_oi == 0,
        "stats": {
            "totalContracts": total_contracts,
            "withGreeks": with_greeks,
            "withIV": with_iv,
            "withOI": with_oi,
            "nonzeroOI": nonzero_oi,
            "oiCall": oi_call,
            "oiPut": oi_put,
            "oiConcentration": oi_concentration,
            "zeroDteShare": zero_dte_share,
            "atmIV": atm_iv,
            "skew25d": skew,
            "expiries": sorted(expiries_seen),
            "selectedExpiries": sorted(selected_expiries),
        },
    }

    # Store in cache so subsequent dashboard refreshes within the TTL window
    # don't burn API credits on a full chain re-download.
    with _mda_gamma_cache_lock:
        _mda_gamma_cache[cache_key] = (deepcopy(payload), time.monotonic() + MDA_GAMMA_CACHE_TTL_SEC)

    return payload


class GammaHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code, payload):
        body = json.dumps(payload).encode("utf-8")
        try:
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", _cors_origin(self.headers.get("Origin")))
            self.send_header("Vary", "Origin")
            self.end_headers()
            self.wfile.write(body)
            return True
        except (BrokenPipeError, ConnectionResetError):
            # Client closed the socket before we could write the response.
            # This is harmless; avoid noisy traceback spam.
            return False

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", _cors_origin(self.headers.get("Origin")))
        self.send_header("Vary", "Origin")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Access-Control-Max-Age", "600")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/gamma":
            params = parse_qs(parsed.query)
            symbol = params.get("symbol", ["SPX"])[0]
            expiry = params.get("expiry", [IB_EXPIRY_MODE])[0]
            limit = int(params.get("limit", [str(IB_MAX_STRIKES)])[0])
            source = params.get("source", ["auto"])[0]

            # Try IBKR first (unless explicitly requesting marketdata)
            ibkr_err = None
            if source != "marketdata":
                with ib_lock:
                    try:
                        payload = compute_gamma_walls(symbol, expiry, limit)
                        self._send_json(200, payload)
                        return
                    except Exception as exc:
                        ibkr_err = str(exc)

            # Fallback to marketdata.app when IBKR fails or when explicitly requested
            if MARKETDATA_APP_TOKEN:
                try:
                    payload = fetch_gamma_marketdata(symbol, expiry_mode=expiry)
                    if ibkr_err:
                        payload = dict(payload)
                        payload["ibkrFallbackReason"] = ibkr_err
                    self._send_json(200, payload)
                    return
                except Exception as mda_exc:
                    combined = f"IBKR: {ibkr_err or 'skipped'}; marketdata.app: {mda_exc}"
                    self._send_json(502, {"error": "Gamma fetch failed", "message": combined})
                    return

            # No fallback available
            self._send_json(502, {
                "error": "Gamma fetch failed",
                "message": ibkr_err or "No gamma source available",
            })
            return

        if parsed.path == "/spot":
            params = parse_qs(parsed.query)
            symbol = params.get("symbol", ["SPX"])[0]
            with ib_lock:
                try:
                    spot = fetch_spot(symbol)
                    payload = {
                        "symbol": symbol.upper(),
                        "spot": spot,
                        "source": "IBKR",
                        "generatedAt": _utc_iso_z(),
                    }
                    self._send_json(200, payload)
                except Exception as exc:
                    self._send_json(502, {"error": "Spot fetch failed", "message": str(exc)})
            return

        if parsed.path == "/market":
            params = parse_qs(parsed.query)
            symbol = params.get("symbol", ["SPX"])[0]
            interval = params.get("interval", ["1d"])[0]
            range_str = params.get("range", ["3mo"])[0]

            with ib_lock:
                try:
                    payload = fetch_ibkr_market(symbol, interval, range_str)
                    self._send_json(200, payload)
                except Exception as exc:
                    self._send_json(502, {"error": "Market fetch failed", "message": str(exc)})
            return

        self._send_json(404, {"error": "Not found"})
        return


def map_duration(range_str, interval):
    intraday = interval.endswith("m") or interval.endswith("h")
    if intraday:
        mapping = {
            "1d": "1 D",
            "5d": "5 D",
            "10d": "10 D",
            "1mo": "1 M",
            "3mo": "3 M",
        }
        return mapping.get(range_str, "5 D")

    mapping = {
        "1mo": "1 M",
        "3mo": "3 M",
        "6mo": "6 M",
        "1y": "1 Y",
    }
    return mapping.get(range_str, "3 M")


def map_bar_size(interval):
    if interval == "1m":
        return "1 min"
    if interval == "5m":
        return "5 mins"
    if interval == "15m":
        return "15 mins"
    if interval == "30m":
        return "30 mins"
    if interval in {"60m", "1h"}:
        return "1 hour"
    if interval == "1wk":
        return "1 week"
    if interval == "1mo":
        return "1 month"
    return "1 day"


def parse_bar_timestamp(raw_date):
    if isinstance(raw_date, (int, float)):
        ts = int(raw_date)
        if ts > 10_000_000_000:
            return ts // 1000
        return ts
    if isinstance(raw_date, datetime):
        return int(raw_date.timestamp())

    text = str(raw_date)
    try:
        return int(text)
    except ValueError:
        pass

    for fmt in ("%Y%m%d %H:%M:%S", "%Y%m%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt == "%Y%m%d":
                parsed = parsed.replace(tzinfo=timezone.utc)
            return int(parsed.timestamp())
        except ValueError:
            continue

    try:
        return int(datetime.fromisoformat(text).timestamp())
    except ValueError as exc:
        raise ValueError(f"Unrecognized bar date format: {raw_date}") from exc


def canonical_session_timestamp(ts):
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return int(datetime(dt.year, dt.month, dt.day, 12, 0, 0, tzinfo=timezone.utc).timestamp())


def fetch_ibkr_market(symbol, interval, range_str):
    ensure_connected()

    contract = get_underlying(symbol)
    ib.qualifyContracts(contract)

    bar_size = map_bar_size(interval)
    duration = map_duration(range_str, interval)

    def request_bars(target_contract):
        what_to_show = "MIDPOINT" if target_contract.secType == "IND" else "TRADES"
        data = ib.reqHistoricalData(
            target_contract,
            endDateTime="",
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow=what_to_show,
            useRTH=IB_USE_RTH,
            formatDate=2,
        )
        if not data and what_to_show != "MIDPOINT":
            data = ib.reqHistoricalData(
                target_contract,
                endDateTime="",
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow="MIDPOINT",
                useRTH=IB_USE_RTH,
                formatDate=2,
            )
        return data

    bars = request_bars(contract)
    source_symbol = symbol.upper()
    source_note = "IBKR"

    if not bars and symbol.upper() == "SPX":
        es_contract = get_es_fallback()
        ib.qualifyContracts(es_contract)
        bars = request_bars(es_contract)
        if bars:
            source_symbol = "ES"
            source_note = "IBKR (ES fallback)"

    if not bars:
        raise ValueError("No historical data returned from IBKR")

    daily_like_interval = interval in {"1d", "daily", "1wk", "1mo"}
    candles_by_key = {}
    for bar in bars:
        ts = parse_bar_timestamp(bar.date)
        if daily_like_interval:
            ts = canonical_session_timestamp(ts)
        candle = {
            "time": ts,
            "open": float(bar.open),
            "high": float(bar.high),
            "low": float(bar.low),
            "close": float(bar.close),
            "volume": float(bar.volume or 0),
        }
        if daily_like_interval:
            candle["sessionDate"] = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
            key = candle["sessionDate"]
        else:
            key = str(ts)
        candles_by_key[key] = candle

    candles = sorted(candles_by_key.values(), key=lambda c: c["time"])
    candles = [c for c in candles if _is_finite(c["open"]) and _is_finite(c["high"]) and _is_finite(c["low"]) and _is_finite(c["close"])]
    if not candles:
        raise ValueError("Historical data contains no valid candles")

    last = candles[-1]
    prev = candles[-2] if len(candles) > 1 else last

    spot = _fetch_ticker_price(contract)
    if not _is_finite(spot):
        spot = last["close"]
    if source_symbol == "ES":
        es_contract = get_es_fallback()
        ib.qualifyContracts(es_contract)
        es_spot = _fetch_ticker_price(es_contract)
        if _is_finite(es_spot):
            spot = es_spot

    now_utc = _utc_now()
    is_last_complete = _is_market_session_closed(now_utc)

    return {
        "symbol": symbol.upper(),
        "sourceSymbol": source_symbol,
        "currency": "USD",
        "exchangeName": IB_EXCHANGE,
        "marketState": "UNKNOWN",
        "currentPrice": float(spot),
        "previousClose": prev["close"],
        "candles": candles,
        "session": {
            "usedIndex": len(candles) - 1,
            "usedDate": last.get("sessionDate")
            or datetime.fromtimestamp(last["time"], tz=timezone.utc).strftime("%Y-%m-%d"),
            "isLastSessionComplete": is_last_complete,
            "timeZone": "America/New_York",
        },
        "dataSource": source_note,
        "asOf": _utc_iso_z(),
    }


def run_server():
    server = HTTPServer((IB_BRIDGE_BIND, IB_BRIDGE_PORT), GammaHandler)
    print(f"IBKR gamma bridge running at http://{IB_BRIDGE_BIND}:{IB_BRIDGE_PORT}/gamma")
    server.serve_forever()


if __name__ == "__main__":
    run_server()
