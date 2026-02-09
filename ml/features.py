from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

FEATURE_VERSION = "v2"
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

    # --- Week 2 Features: Volume Profile ---
    vpoc = event.get("vpoc")
    if event.get("vpoc_dist_bps") is not None:
        row["vpoc_dist_bps_calc"] = event.get("vpoc_dist_bps")
    elif vpoc and touch_price:
        row["vpoc_dist_bps_calc"] = (touch_price - vpoc) / vpoc * 1e4
    else:
        row["vpoc_dist_bps_calc"] = None

    row["volume_at_level"] = event.get("volume_at_level")

    # Relative volume: volume_at_level as % of session total (if available)
    # This normalizes across different days/symbols
    vol_at = event.get("volume_at_level")
    row["volume_at_level_relative"] = None  # Computed downstream if total available

    # --- Week 2 Features: Multi-Timeframe Confluence ---
    row["mtf_confluence"] = event.get("mtf_confluence", 0) or 0
    row["has_weekly_confluence"] = 0
    row["has_monthly_confluence"] = 0
    mtf_types = event.get("mtf_confluence_types")
    if mtf_types:
        try:
            import json
            types_list = json.loads(mtf_types) if isinstance(mtf_types, str) else mtf_types
            row["has_weekly_confluence"] = 1 if any("weekly" in t for t in types_list) else 0
            row["has_monthly_confluence"] = 1 if any("monthly" in t for t in types_list) else 0
        except (json.JSONDecodeError, TypeError):
            pass

    # Distance to weekly/monthly pivot PP
    weekly_pivot = event.get("weekly_pivot")
    monthly_pivot = event.get("monthly_pivot")
    if weekly_pivot and touch_price:
        row["weekly_pivot_dist_bps"] = (touch_price - weekly_pivot) / weekly_pivot * 1e4
    else:
        row["weekly_pivot_dist_bps"] = None

    if monthly_pivot and touch_price:
        row["monthly_pivot_dist_bps"] = (touch_price - monthly_pivot) / monthly_pivot * 1e4
    else:
        row["monthly_pivot_dist_bps"] = None

    # --- Week 2 Features: Level Aging ---
    row["level_age_days"] = event.get("level_age_days", 0) or 0
    row["is_persistent_level"] = 1 if row["level_age_days"] >= 3 else 0

    # --- Week 2 Features: Historical Accuracy ---
    row["hist_reject_rate"] = event.get("hist_reject_rate")
    row["hist_break_rate"] = event.get("hist_break_rate")
    row["hist_sample_size"] = event.get("hist_sample_size", 0) or 0
    row["has_history"] = 1 if row["hist_sample_size"] >= 10 else 0

    # Composite edge score: higher = more likely to reject
    # Only computed when we have sufficient history
    if row["hist_reject_rate"] is not None and row["hist_sample_size"] >= 10:
        row["hist_edge_score"] = row["hist_reject_rate"] - (row["hist_break_rate"] or 0)
    else:
        row["hist_edge_score"] = None

    return row


def required_keys() -> list[str]:
    return ["symbol", "ts_event", "level_type", "level_price", "touch_price", "distance_bps"]


def collect_missing(event: dict[str, Any]) -> list[str]:
    missing = []
    for key in required_keys():
        if event.get(key) is None:
            missing.append(key)
    return missing
