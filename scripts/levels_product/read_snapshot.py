#!/usr/bin/env python3
"""Read-only canonical morning-map consumer; never opens a DB or .env.

Clock policy: before the exchange close (including pre-open), target today's
session; at/after close target the next session. Weekends/holidays target the
next session. Half-days and DST follow trading_calendar/America/New_York.
No snapshot from another target, or generated after the read clock, is usable.
Calendar coverage is intentionally limited to the existing 2025–2027 tables.

LEVELS_SNAPSHOT_DIR is a trusted process/operator override, NOT a query option.
Only a fixed symbol-derived latest filename and in-directory regular-file
symlink targets are accepted. File data, subprocess time and output are bounded.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import stat
import sys
from datetime import datetime
from pathlib import Path
from typing import cast

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'scripts'))
from level_snapshot import ET, UTC, load_snapshot, next_trading_day
from trading_calendar import is_trading_day, session_close_et

MAX_BYTES = 262144
SYMBOL = re.compile(r'[A-Z0-9][A-Z0-9._-]{0,31}')


def target_session(now: datetime):
    if now.tzinfo is None:
        raise ValueError('clock must be timezone aware')
    local = now.astimezone(ET)
    day = local.date()
    if not 2025 <= day.year <= 2027:
        raise ValueError('calendar coverage unavailable')
    if is_trading_day(day) and local.time().replace(tzinfo=None) < session_close_et(day):
        target = day
    else:
        target = next_trading_day(day)
    if target.year > 2027:
        raise ValueError('calendar coverage unavailable')
    return target


class _FrozenRead:
    """Path read interface for load_snapshot, using a single bounded file read.

    Avoid re-opening a pathname after validation (symlink swap/growth races).
    The existing Python contract validator remains the sole identity authority.
    """
    def __init__(self, text):
        self.text = text

    def read_text(self):
        return self.text

    def __str__(self):
        return '<bounded canonical artifact>'


def read_current_snapshot(symbol: str, *, root: Path | None = None,
                          now: datetime | None = None) -> dict:
    result = {'status': 'unavailable', 'symbol': symbol}

    def unavailable(reason):
        return {**result, 'reason': reason}

    if not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol):
        return unavailable('invalid_symbol')
    now = now or datetime.now(UTC)
    try:
        target = target_session(now)
    except ValueError:
        return unavailable('calendar_unavailable')
    result.update(target_session=target.isoformat(), checked_at_utc=now.astimezone(UTC).isoformat().replace('+00:00', 'Z'))
    root = Path(root if root is not None else os.environ.get('LEVELS_SNAPSHOT_DIR', str(REPO / 'evidence/levels_product')))
    try:
        root = root.resolve()
        path = (root / f'levels_snapshot_{symbol}_latest.json').resolve(strict=True)
        if path.parent != root:
            return unavailable('invalid')
        # O_NONBLOCK ensures a replaced FIFO cannot hang before the file check.
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode):
                return unavailable('invalid')
            if info.st_size > MAX_BYTES:
                return unavailable('oversized')
            data = handle.read(MAX_BYTES + 1)
        if len(data) > MAX_BYTES:
            return unavailable('oversized')
        # load_snapshot only calls read_text; adapt the already bounded read to
        # its Path interface without allowing a second, unbounded filesystem read.
        snapshot = load_snapshot(cast(Path, _FrozenRead(data.decode('utf-8'))))
    except FileNotFoundError:
        return unavailable('missing')
    except (OSError, ValueError, TypeError, KeyError, AttributeError, OverflowError, RecursionError):
        return unavailable('invalid')
    if snapshot['symbol'] != symbol:
        return unavailable('symbol_mismatch')
    generated = datetime.fromisoformat(snapshot['generated_at_utc'].replace('Z', '+00:00'))
    if snapshot['session_date'] > target.isoformat() or generated > now:
        return unavailable('future')
    if snapshot['session_date'] < target.isoformat():
        return unavailable('stale')
    return {**result, 'status': 'available', 'snapshot': snapshot}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--symbol', required=True)
    args = parser.parse_args()
    print(json.dumps(read_current_snapshot(args.symbol), allow_nan=False))


if __name__ == '__main__':
    main()
