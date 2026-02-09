from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

FEATURE_VERSION = "v1"
NY_TZ = ZoneInfo("America/New_York") if ZoneInfo else timezone.utc


def _tod_bucket(hour: int) -> str:
    if hour < 10:
        return "open"
    if hour < 14:
        return "mid"
    if hour < 16:
        return "power"
    return "overnight"


def _level_family(level_type: str | None) -> str:
    if not level_type:
        return "pivot"
    if level_type.startswith("R"):
        return "resistance"
    if level_type.startswith("S"):
        return "support"
    if level_type == "GAMMA":
        return "gamma"
    return "pivot"


def build_feature_row(event: dict[str, Any]) -> dict[str, Any]:
    row = dict(event)
    ts_event = event.get("ts_event")
    if ts_event:
        dt = datetime.fromtimestamp(ts_event / 1000, tz=NY_TZ)
        row["event_date_et"] = dt.date()
        row["event_hour_et"] = dt.hour
        row["tod_bucket"] = _tod_bucket(dt.hour)
    else:
        row["event_date_et"] = None
        row["event_hour_et"] = None
        row["tod_bucket"] = None

    level_type = event.get("level_type")
    row["level_family"] = _level_family(level_type)

    ema9 = event.get("ema9")
    ema21 = event.get("ema21")
    if event.get("ema_state") is not None:
        row["ema_state_calc"] = event.get("ema_state")
    elif ema9 is not None and ema21 is not None:
        row["ema_state_calc"] = 1 if ema9 > ema21 else -1 if ema9 < ema21 else 0
    else:
        row["ema_state_calc"] = None

    vwap = event.get("vwap")
    touch_price = event.get("touch_price")
    if event.get("vwap_dist_bps") is not None:
        row["vwap_dist_bps_calc"] = event.get("vwap_dist_bps")
    elif vwap and touch_price:
        row["vwap_dist_bps_calc"] = (touch_price - vwap) / vwap * 1e4
    else:
        row["vwap_dist_bps_calc"] = None

    gamma_flip = event.get("gamma_flip")
    if event.get("gamma_flip_dist_bps") is not None:
        row["gamma_flip_dist_bps_calc"] = event.get("gamma_flip_dist_bps")
    elif gamma_flip and touch_price:
        row["gamma_flip_dist_bps_calc"] = (touch_price - gamma_flip) / gamma_flip * 1e4
    else:
        row["gamma_flip_dist_bps_calc"] = None

    return row


def required_keys() -> list[str]:
    return ["symbol", "ts_event", "level_type", "level_price", "touch_price", "distance_bps"]


def collect_missing(event: dict[str, Any]) -> list[str]:
    missing = []
    for key in required_keys():
        if event.get(key) is None:
            missing.append(key)
    return missing
