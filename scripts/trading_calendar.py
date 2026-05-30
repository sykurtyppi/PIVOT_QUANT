#!/usr/bin/env python3
"""Single source of truth for the US equity (NYSE) trading calendar.

Before this module existed, the NYSE holiday list was hardcoded in at least
three places (generate_daily_ml_report.py, run_daily_report_send.sh, and an
implicit weekday-only check in collect_gamma_history.py), which risked drift
when one copy was updated and the others were not. All three now import from
here.

Public surface:
  - ``NYSE_HOLIDAYS``     — set[date], full-closure holidays (2025–2027).
  - ``is_trading_day(d)`` — True iff ``d`` is a weekday and not a full holiday.
  - ``roll_back_to_trading_day(d)`` — latest trading day at or before ``d``.
  - ``NYSE_HALF_DAYS``    — dict[date, time], early-close (1:00 PM ET) sessions.
  - ``is_half_day(d)``    — True iff ``d`` is a known NYSE early-close session.
  - ``session_close_et(d)`` — the regular or early close time for a trading day.

Half-day data is provided for the P2-5 staleness follow-up. It is NOT yet wired
into any staleness/session computation — doing so is a deliberate behavior
change to be handled separately.

Update annually. Source: https://www.nyse.com/markets/hours-calendars
"""

from __future__ import annotations

from datetime import date, time as dtime, timedelta

# Regular NYSE cash session (Eastern Time).
REGULAR_SESSION_OPEN_ET: dtime = dtime(9, 30)
REGULAR_SESSION_CLOSE_ET: dtime = dtime(16, 0)
# Early-close sessions end at 1:00 PM ET.
EARLY_CLOSE_ET: dtime = dtime(13, 0)


# NYSE full-closure holidays (update annually).
NYSE_HOLIDAYS: set[date] = {
    # 2025
    date(2025, 1, 1),   # New Year's Day
    date(2025, 1, 20),  # MLK Day
    date(2025, 2, 17),  # Presidents' Day
    date(2025, 4, 18),  # Good Friday
    date(2025, 5, 26),  # Memorial Day
    date(2025, 6, 19),  # Juneteenth
    date(2025, 7, 4),   # Independence Day
    date(2025, 9, 1),   # Labor Day
    date(2025, 11, 27), # Thanksgiving
    date(2025, 12, 25), # Christmas Day
    # 2026
    date(2026, 1, 1),   # New Year's Day
    date(2026, 1, 19),  # MLK Day
    date(2026, 2, 16),  # Presidents' Day
    date(2026, 4, 3),   # Good Friday
    date(2026, 5, 25),  # Memorial Day
    date(2026, 6, 19),  # Juneteenth
    date(2026, 7, 3),   # Independence Day (observed)
    date(2026, 9, 7),   # Labor Day
    date(2026, 11, 26), # Thanksgiving
    date(2026, 12, 25), # Christmas Day
    # 2027 (pre-loaded for EOY runs)
    date(2027, 1, 1),   # New Year's Day
    date(2027, 1, 18),  # MLK Day
    date(2027, 2, 15),  # Presidents' Day
    date(2027, 3, 26),  # Good Friday
    date(2027, 5, 31),  # Memorial Day
    date(2027, 6, 18),  # Juneteenth (observed)
    date(2027, 7, 5),   # Independence Day (observed)
    date(2027, 9, 6),   # Labor Day
    date(2027, 11, 25), # Thanksgiving
    date(2027, 12, 24), # Christmas Day (observed)
}


# NYSE early-close (1:00 PM ET) sessions. Conservative — only dates NYSE
# has historically/officially published as half days are included. When the
# day-before/after a holiday is itself a full closure (e.g. July 3, 2026), it
# belongs in NYSE_HOLIDAYS, not here.
NYSE_HALF_DAYS: dict[date, dtime] = {
    # 2025
    date(2025, 7, 3):   EARLY_CLOSE_ET,  # Day before Independence Day
    date(2025, 11, 28): EARLY_CLOSE_ET,  # Day after Thanksgiving
    date(2025, 12, 24): EARLY_CLOSE_ET,  # Christmas Eve
    # 2026
    date(2026, 11, 27): EARLY_CLOSE_ET,  # Day after Thanksgiving
    date(2026, 12, 24): EARLY_CLOSE_ET,  # Christmas Eve
    # 2027
    date(2027, 11, 26): EARLY_CLOSE_ET,  # Day after Thanksgiving
}


def is_trading_day(d: date) -> bool:
    """Return True iff ``d`` is a NYSE trading day.

    A trading day is a weekday (Mon–Fri) that is not a full-closure holiday.
    Half-days are still trading days (the market is open, just closing early).
    """
    return d.weekday() < 5 and d not in NYSE_HOLIDAYS


def roll_back_to_trading_day(d: date) -> date:
    """Return the latest NYSE trading day at or before ``d``."""
    day = d
    while not is_trading_day(day):
        day -= timedelta(days=1)
    return day


def is_half_day(d: date) -> bool:
    """Return True iff ``d`` is a known NYSE early-close (half) session."""
    return d in NYSE_HALF_DAYS


def session_close_et(d: date) -> dtime:
    """Return the close time (ET) for a trading day ``d``.

    Returns the early-close time on half-days, otherwise the regular close.
    The caller is responsible for confirming ``d`` is a trading day.
    """
    return NYSE_HALF_DAYS.get(d, REGULAR_SESSION_CLOSE_ET)
