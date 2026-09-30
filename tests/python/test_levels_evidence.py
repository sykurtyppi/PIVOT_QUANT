"""Tests for the level-behavior evidence pipeline (step 1).

Guards the two things that make the base rates trustworthy: the Wilson interval
maths, and that each session's band is genuinely point-in-time (no lookahead) and
uses the anchor*exp(+/-k*sigma) form that matches the live engine.
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import math
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "levels_evidence" / "build_daily_event_table.py"

spec = importlib.util.spec_from_file_location("levels_evidence_build", SCRIPT)
mod = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(mod)

RC_SCRIPT = ROOT / "scripts" / "levels_evidence" / "regime_calibration.py"
_rc_spec = importlib.util.spec_from_file_location("regime_calibration", RC_SCRIPT)
rc = importlib.util.module_from_spec(_rc_spec)
assert _rc_spec and _rc_spec.loader
_rc_spec.loader.exec_module(rc)


def make_bars(closes, highs=None, lows=None):
    bars = []
    for i, c in enumerate(closes):
        d = (dt.date(2020, 1, 1) + dt.timedelta(days=i)).isoformat()
        bars.append({
            "date": d, "open": c,
            "high": highs[i] if highs else c,
            "low": lows[i] if lows else c,
            "close": c,
        })
    return bars


class WilsonTest(unittest.TestCase):
    def test_matches_published_touch_interval(self):
        # 679/2492 upper +1sigma touches -> 27.25% [25.54, 29.03]
        p, lo, hi = mod.wilson(679, 2492)
        self.assertAlmostEqual(p, 0.2725, places=4)
        self.assertAlmostEqual(lo, 0.2554, places=3)
        self.assertAlmostEqual(hi, 0.2903, places=3)
        self.assertLess(lo, p)
        self.assertGreater(hi, p)

    def test_zero_and_full(self):
        p, lo, _ = mod.wilson(0, 100)
        self.assertEqual(p, 0.0)
        self.assertGreaterEqual(lo, 0.0)
        p2, _, hi2 = mod.wilson(100, 100)
        self.assertEqual(p2, 1.0)
        self.assertLessEqual(hi2, 1.0)


class BandTest(unittest.TestCase):
    def _oscillating(self, n):
        # nonzero, well-defined sigma
        return [100.0 * math.exp(0.01 * (1 if i % 2 else -1)) for i in range(n)]

    def test_band_is_anchor_times_exp_k_sigma(self):
        ev = mod.build_events(make_bars(self._oscillating(30)))
        self.assertTrue(ev)
        e = ev[-1]
        self.assertAlmostEqual(e["u1"], e["anchor"] * math.exp(e["sigma"]), places=6)
        self.assertAlmostEqual(e["u2"], e["anchor"] * math.exp(2 * e["sigma"]), places=6)
        self.assertAlmostEqual(e["l1"], e["anchor"] * math.exp(-e["sigma"]), places=6)
        self.assertAlmostEqual(e["l2"], e["anchor"] * math.exp(-2 * e["sigma"]), places=6)

    def test_point_in_time_no_lookahead(self):
        closes = self._oscillating(30)
        base = mod.build_events(make_bars(closes))
        # Mutating the evaluated session's OWN high/low/close must not move its band,
        # because the band depends only on closes through the prior session.
        highs = [c for c in closes]
        lows = [c for c in closes]
        highs[-1] *= 2.0
        lows[-1] *= 0.5
        mutated = mod.build_events(make_bars(closes, highs=highs, lows=lows))
        self.assertAlmostEqual(base[-1]["u1"], mutated[-1]["u1"], places=9)
        self.assertAlmostEqual(base[-1]["l1"], mutated[-1]["l1"], places=9)
        self.assertAlmostEqual(base[-1]["sigma"], mutated[-1]["sigma"], places=9)
        # ...but the touch flag DOES respond to the session's own high (the doubled high touches up)
        self.assertEqual(mutated[-1]["touch_u1"], 1)
        self.assertEqual(mutated[-1]["touch_u2"], 1)

    def test_touch_and_close_flags(self):
        closes = self._oscillating(30)
        ev = mod.build_events(make_bars(closes))
        u1 = ev[-1]["u1"]
        # high just below u1 -> no touch; high just above -> touch (flag reads the raw high)
        highs = [c for c in closes]
        highs[-1] = u1 - 0.01
        self.assertEqual(mod.build_events(make_bars(closes, highs=highs))[-1]["touch_u1"], 0)
        highs[-1] = u1 + 0.01
        self.assertEqual(mod.build_events(make_bars(closes, highs=highs))[-1]["touch_u1"], 1)


class RegimeCalibrationTest(unittest.TestCase):
    def test_brownian_one_sided_touch_values(self):
        self.assertAlmostEqual(rc.BROWNIAN_TOUCH[1], 0.3173, places=3)
        self.assertAlmostEqual(rc.BROWNIAN_TOUCH[2], 0.0455, places=3)

    def test_brier(self):
        self.assertAlmostEqual(rc.brier([0.3, 0.3], [0, 1]), (0.09 + 0.49) / 2, places=9)
        self.assertEqual(rc.brier([1.0, 0.0], [1, 0]), 0.0)

    def test_tercile_cuts_monotone(self):
        lo, hi = rc.tercile_cuts([float(x) for x in range(9)])
        self.assertLessEqual(lo, hi)
        self.assertEqual(rc.vol_bucket(lo - 1, lo, hi), "low")
        self.assertEqual(rc.vol_bucket(hi + 1, lo, hi), "high")

    def test_features_are_point_in_time(self):
        # An earlier session's regime features must not change when a LATER bar is mutated.
        closes = [100.0 * math.exp(0.01 * (1 if i % 2 else -1)) for i in range(80)]
        bars = make_bars(closes)
        base = rc.build_augmented(bars)
        bars2 = [dict(b) for b in bars]
        bars2[-1]["close"] *= 1.5
        bars2[-1]["open"] *= 1.5
        bars2[-1]["high"] *= 1.5
        mutated = rc.build_augmented(bars2)
        # compare the first evaluated event (well before the mutated last bar)
        self.assertEqual(base[0]["trend"], mutated[0]["trend"])
        self.assertEqual(base[0]["gap"], mutated[0]["gap"])
        self.assertAlmostEqual(base[0]["sigma"], mutated[0]["sigma"], places=9)


if __name__ == "__main__":
    unittest.main()
