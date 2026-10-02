"""Shared eligibility rules for touch-event label maturation."""
from __future__ import annotations


def normalize_bar_interval(bar_interval_sec) -> int | None:
    """Return a positive integral bar interval, otherwise ``None``.

    Touches without a deterministic bar grid must never be treated as labelable:
    doing so would mix bars from different intervals and create invalid targets.
    """
    if bar_interval_sec is None or isinstance(bar_interval_sec, bool):
        return None
    try:
        if isinstance(bar_interval_sec, float) and not bar_interval_sec.is_integer():
            return None
        interval = int(bar_interval_sec)
    except (TypeError, ValueError):
        return None
    return interval if interval > 0 else None
