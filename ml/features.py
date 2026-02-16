from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

FEATURE_VERSION = "v3"
NY_TZ = ZoneInfo("America/New_York") if ZoneInfo else timezone.utc

# ── Features the RF should NEVER see (dead, leaked, or raw-price) ──
# These are explicitly dropped before training regardless of null status.
DROP_FEATURES = {
    # Raw prices → non-stationary, cause temporal leakage
    "touch_price",
    "level_price",
    "vwap",
    "ema9",
    "ema21",
    "vpoc",
    "gamma_flip",
    "weekly_pivot",
    "monthly_pivot",
    # Duplicate _calc columns (we keep the canonical version only)
    "vwap_dist_bps",       # keep vwap_dist_bps_calc
    "vpoc_dist_bps",       # keep vpoc_dist_bps_calc
    "gamma_flip_dist_bps", # keep gamma_flip_dist_bps_calc
    # Permanently dead (all-zero or all-null with 6 days of data)
    "volume_at_level_relative",
    "has_history",
    "hist_edge_score",
    # Constant or near-constant
    "bar_interval_sec",
    "source",
    "session",
    "symbol",
    # Dead without IBKR connection (100% null from Yahoo-only collection)
    "iv_rv_state",
    "gamma_confidence",
    "oi_concentration_top5",
    "zero_dte_share",
    # Redundant: perfectly anti-correlated with minutes_since_open
    "minutes_until_close",
    # Raw ATR in dollars — non-stationary, replaced by atr_bps
    "atr",
}


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

    # ── Time-of-day features (improved) ──
    if ts_event:
        dt = datetime.fromtimestamp(ts_event / 1000, tz=NY_TZ)
        row["event_date_et"] = dt.date()
        row["event_hour_et"] = dt.hour
        row["tod_bucket"] = _tod_bucket(dt.hour)
        # Minutes since market open (9:30 ET)
        open_minutes = 9 * 60 + 30
        current_minutes = dt.hour * 60 + dt.minute
        row["minutes_since_open"] = max(0, current_minutes - open_minutes)
        # Minutes until close (16:00 ET)
        close_minutes = 16 * 60
        row["minutes_until_close"] = max(0, close_minutes - current_minutes)
        # Boolean flags for high-impact periods
        row["is_first_30min"] = 1 if row["minutes_since_open"] <= 30 else 0
        row["is_last_30min"] = 1 if row["minutes_until_close"] <= 30 else 0
        row["is_lunch_hour"] = 1 if 12 <= dt.hour < 13 else 0
    else:
        row["event_date_et"] = None
        row["event_hour_et"] = None
        row["tod_bucket"] = None
        row["minutes_since_open"] = None
        row["minutes_until_close"] = None
        row["is_first_30min"] = None
        row["is_last_30min"] = None
        row["is_lunch_hour"] = None

    level_type = event.get("level_type")
    row["level_family"] = _level_family(level_type)

    # ── EMA features (normalized, not raw prices) ──
    ema9 = event.get("ema9")
    ema21 = event.get("ema21")
    if event.get("ema_state") is not None:
        row["ema_state_calc"] = event.get("ema_state")
    elif ema9 is not None and ema21 is not None:
        row["ema_state_calc"] = 1 if ema9 > ema21 else -1 if ema9 < ema21 else 0
    else:
        row["ema_state_calc"] = None

    # EMA spread in bps (replaces raw ema9/ema21)
    if ema9 is not None and ema21 is not None and ema21 != 0:
        row["ema_spread_bps"] = (ema9 - ema21) / ema21 * 1e4
    else:
        row["ema_spread_bps"] = None

    # Price relative to EMA21 in bps (replaces raw touch_price)
    touch_price = event.get("touch_price")
    if touch_price is not None and ema21 is not None and ema21 != 0:
        row["price_vs_ema21_bps"] = (touch_price - ema21) / ema21 * 1e4
    else:
        row["price_vs_ema21_bps"] = None

    # ── VWAP features ──
    vwap = event.get("vwap")
    if event.get("vwap_dist_bps") is not None:
        row["vwap_dist_bps_calc"] = event.get("vwap_dist_bps")
    elif vwap and touch_price:
        row["vwap_dist_bps_calc"] = (touch_price - vwap) / vwap * 1e4
    else:
        row["vwap_dist_bps_calc"] = None

    # VWAP z-score: distance normalized by session volatility
    session_std = event.get("session_std")
    if vwap is not None and touch_price is not None and session_std and session_std > 0:
        row["vwap_zscore"] = (touch_price - vwap) / session_std
    else:
        row["vwap_zscore"] = None

    # ── Gamma features ──
    gamma_flip = event.get("gamma_flip")
    if event.get("gamma_flip_dist_bps") is not None:
        row["gamma_flip_dist_bps_calc"] = event.get("gamma_flip_dist_bps")
    elif gamma_flip and touch_price:
        row["gamma_flip_dist_bps_calc"] = (touch_price - gamma_flip) / gamma_flip * 1e4
    else:
        row["gamma_flip_dist_bps_calc"] = None

    # ── Volume Profile ──
    vpoc = event.get("vpoc")
    if event.get("vpoc_dist_bps") is not None:
        row["vpoc_dist_bps_calc"] = event.get("vpoc_dist_bps")
    elif vpoc and touch_price:
        row["vpoc_dist_bps_calc"] = (touch_price - vpoc) / vpoc * 1e4
    else:
        row["vpoc_dist_bps_calc"] = None

    row["volume_at_level"] = event.get("volume_at_level")

    # ── Multi-Timeframe Confluence ──
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

    # Distance to weekly/monthly pivot PP (in bps, not raw price)
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

    # ── Level Aging ──
    row["level_age_days"] = event.get("level_age_days", 0) or 0
    row["is_persistent_level"] = 1 if row["level_age_days"] >= 3 else 0

    # ── Historical Accuracy ──
    row["hist_reject_rate"] = event.get("hist_reject_rate")
    row["hist_break_rate"] = event.get("hist_break_rate")
    row["hist_sample_size"] = event.get("hist_sample_size", 0) or 0

    # ── Regime features (new) ──
    row["regime_type"] = event.get("regime_type")           # 1=trend_up, 2=trend_down, 3=range, 4=vol_expansion
    row["overnight_gap_atr"] = event.get("overnight_gap_atr")  # gap / ATR ratio
    row["or_size_atr"] = event.get("or_size_atr")           # opening range / ATR ratio
    row["or_breakout"] = event.get("or_breakout")           # 1=above ORH, -1=below ORL, 0=inside

    # ── Opening Range features (new) ──
    row["or_high_dist_bps"] = event.get("or_high_dist_bps")  # distance from OR high in bps
    row["or_low_dist_bps"] = event.get("or_low_dist_bps")    # distance from OR low in bps

    # ── Expected range σ-bands (new) ──
    row["sigma_band_position"] = event.get("sigma_band_position")  # price position within ±1σ (-1 to 1)
    row["distance_to_upper_sigma"] = event.get("distance_to_upper_sigma_bps")  # bps to +1σ
    row["distance_to_lower_sigma"] = event.get("distance_to_lower_sigma_bps")  # bps to -1σ

    # ── ATR features (normalized to bps, not raw dollars) ──
    atr = event.get("atr")
    distance_bps = event.get("distance_bps")
    if atr and atr > 0 and touch_price and touch_price > 0:
        atr_bps = atr / touch_price * 1e4
        row["atr_bps"] = atr_bps
        row["distance_atr_ratio"] = distance_bps / atr_bps if atr_bps > 0 and distance_bps is not None else None
    else:
        row["atr_bps"] = None
        row["distance_atr_ratio"] = None

    return row


def drop_features() -> set[str]:
    """Return the set of feature names that should be excluded from training."""
    return DROP_FEATURES.copy()


def required_keys() -> list[str]:
    return ["symbol", "ts_event", "level_type", "level_price", "touch_price", "distance_bps"]


def collect_missing(event: dict[str, Any]) -> list[str]:
    missing = []
    for key in required_keys():
        if event.get(key) is None:
            missing.append(key)
    return missing
