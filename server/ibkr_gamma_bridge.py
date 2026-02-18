import json
import os
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

from ib_insync import IB, Index, Option, Stock, ContFuture

IB_HOST = os.getenv("IB_HOST", "127.0.0.1")
IB_PORT = int(os.getenv("IB_PORT", "4002"))
IB_CLIENT_ID = int(os.getenv("IB_CLIENT_ID", "12"))
IB_BRIDGE_BIND = os.getenv("IB_BRIDGE_BIND", "127.0.0.1")
IB_BRIDGE_PORT = int(os.getenv("IB_BRIDGE_PORT", "5001"))
IB_EXCHANGE = os.getenv("IB_EXCHANGE", "CBOE")
IB_MAX_STRIKES = int(os.getenv("IB_MAX_STRIKES", "60"))
IB_STRIKE_RANGE = float(os.getenv("IB_STRIKE_RANGE", "0.05"))  # +/- 5%
IB_EXPIRY_MODE = os.getenv("IB_EXPIRY_MODE", "front")  # front, 0dte, monthly, all
IB_MAX_EXPIRIES = int(os.getenv("IB_MAX_EXPIRIES", "1"))
IB_WEIGHT_0DTE = float(os.getenv("IB_WEIGHT_0DTE", "1.0"))
IB_WEIGHT_FRONT = float(os.getenv("IB_WEIGHT_FRONT", "0.6"))
IB_WEIGHT_MONTHLY = float(os.getenv("IB_WEIGHT_MONTHLY", "0.35"))
IB_WEIGHT_OTHER = float(os.getenv("IB_WEIGHT_OTHER", "0.2"))
IB_USE_RTH = os.getenv("IB_USE_RTH", "1") != "0"
IB_DATA_TYPE = os.getenv("IB_DATA_TYPE", "").strip()

ib = IB()
ib_lock = threading.Lock()


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
    # Prefer explicit data type if provided.
    if IB_DATA_TYPE:
        try:
            _request_market_data_type(int(IB_DATA_TYPE))
        except ValueError:
            _request_market_data_type(1)
    else:
        _request_market_data_type(1)

    ticker = ib.reqTickers(contract)[0]
    price = _safe_price(ticker.marketPrice(), ticker.last, ticker.close)
    if _is_finite(price):
        return price

    # Fallback to delayed if realtime unavailable.
    _request_market_data_type(3)
    ticker = ib.reqTickers(contract)[0]
    price = _safe_price(ticker.marketPrice(), ticker.last, ticker.close)
    return price


def ensure_connected():
    if ib.isConnected():
        return
    ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID)


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


def pick_expiries(expirations, mode):
    exp_list = sorted(expirations)
    if not exp_list:
        return []

    today = datetime.utcnow().strftime("%Y%m%d")
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
            try:
                dt = datetime.strptime(exp, "%Y%m%d")
                if 15 <= dt.day <= 21 and dt.weekday() == 4:
                    return [exp]
            except ValueError:
                continue
        return [exp_list[0]]

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

    today = datetime.utcnow().strftime("%Y%m%d")
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
        "generatedAt": datetime.utcnow().isoformat() + "Z",
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


class GammaHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code, payload):
        body = json.dumps(payload).encode("utf-8")
        try:
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
            return True
        except (BrokenPipeError, ConnectionResetError):
            # Client closed the socket before we could write the response.
            # This is harmless; avoid noisy traceback spam.
            return False

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/gamma":
            params = parse_qs(parsed.query)
            symbol = params.get("symbol", ["SPX"])[0]
            expiry = params.get("expiry", [IB_EXPIRY_MODE])[0]
            limit = int(params.get("limit", [str(IB_MAX_STRIKES)])[0])

            with ib_lock:
                try:
                    payload = compute_gamma_walls(symbol, expiry, limit)
                    self._send_json(200, payload)
                except Exception as exc:
                    self._send_json(502, {"error": "Gamma fetch failed", "message": str(exc)})
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
                        "generatedAt": datetime.utcnow().isoformat() + "Z",
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
    if interval == "1h":
        return "1 hour"
    if interval == "1wk":
        return "1 week"
    if interval == "1mo":
        return "1 month"
    return "1 day"


def parse_bar_timestamp(raw_date):
    if isinstance(raw_date, (int, float)):
        return int(raw_date)
    if isinstance(raw_date, datetime):
        return int(raw_date.timestamp())

    text = str(raw_date)
    try:
        return int(text)
    except ValueError:
        pass

    for fmt in ("%Y%m%d %H:%M:%S", "%Y%m%d"):
        try:
            return int(datetime.strptime(text, fmt).timestamp())
        except ValueError:
            continue

    try:
        return int(datetime.fromisoformat(text).timestamp())
    except ValueError as exc:
        raise ValueError(f"Unrecognized bar date format: {raw_date}") from exc


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

    candles = []
    for bar in bars:
        ts = parse_bar_timestamp(bar.date)
        candles.append(
            {
                "time": ts,
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": float(bar.volume or 0),
            }
        )

    candles = [c for c in candles if c["open"] and c["high"] and c["low"] and c["close"]]
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

    # Determine if last session is complete: after 4 PM ET (21:00 UTC during
    # EST, conservative — during EDT this triggers 1 hour late, safe direction)
    now_utc = datetime.now(timezone.utc)
    is_last_complete = now_utc.hour >= 21 or now_utc.weekday() >= 5

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
            "usedDate": datetime.utcfromtimestamp(last["time"]).strftime("%Y-%m-%d"),
            "isLastSessionComplete": is_last_complete,
            "timeZone": "America/New_York",
        },
        "dataSource": source_note,
        "asOf": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def run_server():
    server = HTTPServer((IB_BRIDGE_BIND, IB_BRIDGE_PORT), GammaHandler)
    print(f"IBKR gamma bridge running at http://{IB_BRIDGE_BIND}:{IB_BRIDGE_PORT}/gamma")
    server.serve_forever()


if __name__ == "__main__":
    run_server()
