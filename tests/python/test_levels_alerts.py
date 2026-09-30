"""Tests for the level-proximity alert watcher's decision logic.

Focus on the part that matters for a good alert: firing once on approach, not
re-firing while price lingers, and re-arming after price moves away.
"""
from __future__ import annotations

import importlib.util
import math
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "levels_alerts" / "watcher.py"

spec = importlib.util.spec_from_file_location("levels_alerts_watcher", SCRIPT)
w = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(w)


class SampleStdTest(unittest.TestCase):
    def test_matches_known(self):
        self.assertAlmostEqual(w.sample_std([1, 2, 3, 4, 5]), math.sqrt(2.5), places=9)
        self.assertTrue(math.isnan(w.sample_std([1.0])))


class EvaluateTest(unittest.TestCase):
    def test_fires_once_then_dedups_then_rearms(self):
        levels = {"u1": 100.0}
        st = {}
        # within 0.15% of the level, armed -> fires
        fired = w.evaluate(levels, 99.9, st, approach=0.0015, rearm=0.0035)
        self.assertEqual([k for k, _ in fired], ["u1"])
        self.assertFalse(st["u1"]["armed"])
        # still within the approach band, now disarmed -> no repeat
        self.assertEqual(w.evaluate(levels, 99.9, st, 0.0015, 0.0035), [])
        # price moves well past the re-arm band -> re-arms, no alert
        self.assertEqual(w.evaluate(levels, 99.5, st, 0.0015, 0.0035), [])
        self.assertTrue(st["u1"]["armed"])
        # comes back into the approach band -> fires again
        fired2 = w.evaluate(levels, 100.05, st, 0.0015, 0.0035)
        self.assertEqual([k for k, _ in fired2], ["u1"])

    def test_far_level_never_fires(self):
        levels = {"S2": 90.0}
        st = {}
        self.assertEqual(w.evaluate(levels, 100.0, st, 0.0015, 0.0035), [])
        self.assertTrue(st["S2"]["armed"])  # stays armed until price approaches

    def test_message_shape(self):
        levels = {"u1": 100.0}
        fired = w.evaluate(levels, 100.05, {}, 0.0015, 0.0035)
        _, msg = fired[0]
        self.assertIn("SPY", msg)
        self.assertIn("approaching", msg)
        self.assertIn("\U0001F514", msg)


class SlackFormatTest(unittest.TestCase):
    def test_discord_bold_becomes_slack_bold(self):
        self.assertEqual(w.notify._slack_text("**SPY 768** near **+1σ**"),
                         "*SPY 768* near *+1σ*")
        self.assertEqual(w.notify._slack_text("no bold here"), "no bold here")


if __name__ == "__main__":
    unittest.main()
