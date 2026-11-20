"""Hermetic reader contract: synthetic snapshots and frozen exchange clocks."""
import copy
import json
import os
import sys
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / 'scripts'), str(ROOT / 'scripts/levels_product')]
from backfill_events import calculate_pivots
from level_snapshot import (
    build_snapshot, write_snapshot, validate_snapshot,
    _digest, _identity_core, _level_identity_core,
)
from read_snapshot import read_current_snapshot, target_session


def clock(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def fixture(source=date(2026, 11, 13), symbol='SPY'):
    return build_snapshot(symbol=symbol, source_session_date=source,
                          source_ohlc={'high': 605, 'low': 595, 'close': 601},
                          generated_at_utc=f'{source.isoformat()}T21:01:00Z',
                          raw_levels=calculate_pivots(605, 595, 601), evidence={})


class SnapshotReaderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = clock('2026-11-16T14:00:00Z')

    def read(self, **kwargs):
        return read_current_snapshot('SPY', root=self.root, now=kwargs.pop('now', self.now), **kwargs)

    def test_exact_validated_snapshot_and_identity(self):
        snapshot = fixture()
        write_snapshot(snapshot, self.root)
        result = self.read()
        self.assertEqual(result['status'], 'available')
        self.assertEqual(result['snapshot'], snapshot)
        self.assertEqual(result['target_session'], '2026-11-16')

    def test_clock_policy_preopen_intraday_close_weekend_holiday_early_close(self):
        for instant, expected in [
            ('2026-11-16T13:00:00Z', '2026-11-16'),
            ('2026-11-16T20:59:59Z', '2026-11-16'),
            ('2026-11-16T21:00:00Z', '2026-11-17'),
            ('2026-11-14T16:00:00Z', '2026-11-16'),
            ('2026-11-26T16:00:00Z', '2026-11-27'),
            ('2026-11-27T17:59:59Z', '2026-11-27'),
            ('2026-11-27T18:00:00Z', '2026-11-30'),
            ('2026-03-09T19:59:59Z', '2026-03-09'),
            ('2026-03-09T20:00:00Z', '2026-03-10'),
        ]:
            with self.subTest(instant=instant):
                self.assertEqual(target_session(clock(instant)).isoformat(), expected)

    def test_missing_invalid_stale_future_and_symbol_mismatch_fail_closed(self):
        self.assertEqual(self.read()['reason'], 'missing')
        path = self.root / 'levels_snapshot_SPY_latest.json'
        path.write_text('{}')
        self.assertEqual(self.read()['reason'], 'invalid')
        for snapshot, reason in [
            (fixture(date(2026, 11, 12)), 'stale'),
            (fixture(date(2026, 11, 16)), 'future'),
            (fixture(symbol='QQQ'), 'symbol_mismatch'),
        ]:
            path.write_text(json.dumps(snapshot))
            self.assertEqual(self.read()['reason'], reason)
            self.assertNotIn('snapshot', self.read())
        tampered = copy.deepcopy(fixture())
        tampered['levels'][0]['price'] += 1
        path.write_text(json.dumps(tampered))
        self.assertEqual(self.read()['reason'], 'invalid')

    def test_future_generation_even_for_current_target_is_unavailable(self):
        snapshot = fixture()
        snapshot['generated_at_utc'] = '2026-11-16T15:00:00Z'
        (self.root / 'levels_snapshot_SPY_latest.json').write_text(json.dumps(snapshot))
        self.assertEqual(self.read()['reason'], 'future')

    def test_paths_size_and_non_regular_files_are_rejected(self):
        for symbol in ['../SPY', 'spy', 'SPY/QQQ', '', 'SPY\n', 'A' * 33]:
            with self.subTest(symbol=symbol):
                self.assertEqual(read_current_snapshot(symbol, root=self.root, now=self.now)['reason'], 'invalid_symbol')
        path = self.root / 'levels_snapshot_SPY_latest.json'
        path.write_text(' ' * (262144 + 1))
        self.assertEqual(self.read()['reason'], 'oversized')
        path.unlink()
        with tempfile.TemporaryDirectory() as outside:
            other = Path(outside) / 'snapshot.json'
            other.write_text(json.dumps(fixture()))
            path.symlink_to(other)
            self.assertEqual(self.read()['reason'], 'invalid')
        path.unlink()
        path.mkdir()
        self.assertEqual(self.read()['reason'], 'invalid')
        path.rmdir()
        os.mkfifo(path)
        self.assertEqual(self.read()['reason'], 'invalid')
        path.unlink()
        for data in [b'[]', b'null', b'{', b'\xff']:
            path.write_bytes(data)
            self.assertEqual(self.read()['reason'], 'invalid')

    def test_self_consistently_hashed_malformed_numbers_fail_closed(self):
        # Re-sign every mutation: identity checking alone is not schema validation.
        cases = [
            ('string level price', ('levels', 0, 'price'), str),
            ('subcent level price', ('levels', 0, 'price'), lambda price: price + 0.001),
            ('boolean level price', ('levels', 0, 'price'), True),
            ('null level price', ('levels', 0, 'price'), None),
            ('string reference', ('reference_price',), '601.0'),
            ('boolean source high', ('source', 'ohlc', 'high'), True),
            ('string source high', ('source', 'ohlc', 'high'), '605.0'),
            ('string distance', ('levels', 0, 'distance_from_reference_bps'), '255.1'),
            ('object confluence', ('levels', 0, 'confluence_components'), {}),
        ]
        path = self.root / 'levels_snapshot_SPY_latest.json'
        for name, keys, value in cases:
            with self.subTest(case=name):
                snapshot = fixture()
                owner = snapshot
                for key in keys[:-1]:
                    owner = owner[key]
                owner[keys[-1]] = value(owner[keys[-1]]) if callable(value) else value
                for level in snapshot['levels']:
                    level['level_id'] = _digest('lvl', _level_identity_core(
                        symbol=snapshot['symbol'], session_date=snapshot['session_date'],
                        label=level['label'], price=level['price']))
                snapshot['snapshot_id'] = _digest('lvs', _identity_core(snapshot))
                path.write_text(json.dumps(snapshot))
                result = self.read()
                self.assertEqual(result['status'], 'unavailable')
                self.assertEqual(result.get('reason'), 'invalid')
                self.assertNotIn('snapshot', result)
                with self.assertRaises(ValueError):
                    validate_snapshot(snapshot)

    def test_nonfinite_numeric_fields_fail_closed(self):
        path = self.root / 'levels_snapshot_SPY_latest.json'
        for value in [float('nan'), float('inf'), -float('inf')]:
            for keys in [('reference_price',), ('source', 'ohlc', 'high'),
                         ('levels', 0, 'price'),
                         ('levels', 0, 'distance_from_reference_bps')]:
                with self.subTest(value=value, keys=keys):
                    snapshot = fixture()
                    owner = snapshot
                    for key in keys[:-1]:
                        owner = owner[key]
                    owner[keys[-1]] = value
                    path.write_text(json.dumps(snapshot))
                    self.assertEqual(self.read()['reason'], 'invalid')
                    with self.assertRaises(ValueError):
                        validate_snapshot(snapshot)

    def test_uncovered_calendar_year_fails_closed(self):
        self.assertEqual(self.read(now=clock('2028-01-03T15:00:00Z'))['reason'], 'calendar_unavailable')


if __name__ == '__main__':
    unittest.main()
