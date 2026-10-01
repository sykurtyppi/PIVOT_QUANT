"""Tests for the intraday post-touch outcome engine (prereg §3).

The committed artifact's full reproduction needs the 1-min snapshot/DB (not committed
— it is 5.5 MB and regenerable from the runtime DB), so the engine's FROZEN definitions
are pinned here on synthetic bars, and the committed artifact's shape and honest
abstention are checked directly.
"""
from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "levels_evidence" / "build_intraday_outcomes.py"
ARTIFACT = ROOT / "research" / "levels_evidence" / "intraday_outcomes.json"
SNAPSHOT = ROOT / "data" / "levels_evidence" / "spy_intraday_snapshot.json"

_spec = importlib.util.spec_from_file_location("build_intraday_outcomes", SCRIPT)
m = importlib.util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(m)

MIN = 60000  # ms per minute


def bars(seq, start=1_000_000_000_000):
    """seq: list of (high, low) or (high, low, close); one bar per minute from start."""
    out = []
    for i, t in enumerate(seq):
        h, l = t[0], t[1]
        c = t[2] if len(t) > 2 else (h + l) / 2
        out.append({"ts": start + i * MIN, "open": c, "high": h, "low": l, "close": c})
    return out


class FirstTouchTest(unittest.TestCase):
    def test_upper_touch_at_first_high_ge_P(self):
        self.assertEqual(m.first_touch_idx(bars([(99, 98), (100, 99), (101, 100)]), 100.0, "upper"), 1)

    def test_lower_touch_at_first_low_le_P(self):
        # bar 0 stays above the level (low 100.5 > 100); bar 1's low reaches it
        self.assertEqual(m.first_touch_idx(bars([(101, 100.5), (100, 99), (99, 98)]), 100.0, "lower"), 1)

    def test_never_touched(self):
        self.assertIsNone(m.first_touch_idx(bars([(99, 98), (99, 98)]), 100.0, "upper"))


class AcceptRejectTest(unittest.TestCase):
    # P = 100, R = 0.2% -> upper beyond = 100.2, upper anchor = 99.8
    def test_upper_acceptance_beyond_first(self):
        b = bars([(100, 99.9), (100.3, 100.0)])
        self.assertEqual(m.accept_reject(b, 0, 100.0, "upper", 30), "acceptance")

    def test_upper_rejection_anchor_first(self):
        b = bars([(100, 99.9), (100.1, 99.7)])
        self.assertEqual(m.accept_reject(b, 0, 100.0, "upper", 30), "rejection")

    def test_undetermined_within_band(self):
        b = bars([(100, 99.9), (100.1, 99.9), (100.1, 99.85)])
        self.assertEqual(m.accept_reject(b, 0, 100.0, "upper", 30), "undetermined")

    def test_same_bar_both_is_undetermined(self):
        b = bars([(100, 99.9), (100.3, 99.7)])  # beyond AND anchor in one bar: unorderable
        self.assertEqual(m.accept_reject(b, 0, 100.0, "upper", 30), "undetermined")

    def test_beyond_after_N_does_not_count(self):
        seq = [(100, 99.9)] + [(100.0, 99.9)] * 30 + [(100.5, 100.0)]  # beyond only at minute 31
        self.assertEqual(m.accept_reject(bars(seq), 0, 100.0, "upper", 30), "undetermined")

    def test_lower_acceptance_and_rejection(self):
        # lower level P=100: beyond = down (99.8), anchor = up (100.2)
        self.assertEqual(m.accept_reject(bars([(100.1, 100), (100.0, 99.7)]), 0, 100.0, "lower", 30), "acceptance")
        self.assertEqual(m.accept_reject(bars([(100.1, 100), (100.3, 99.9)]), 0, 100.0, "lower", 30), "rejection")


class MfeMaeTest(unittest.TestCase):
    def test_upper_excursions(self):
        b = bars([(100, 99.9), (101, 99.5), (100.5, 99.0)])
        mfe, mae = m.mfe_mae(b, 0, 100.0, "upper")
        self.assertAlmostEqual(mfe, (101 - 100) / 100, places=9)
        self.assertAlmostEqual(mae, (100 - 99.0) / 100, places=9)


class FirstSideTest(unittest.TestCase):
    def test_ordering(self):
        self.assertEqual(m.first_side_of(10, 20), "u1_first")
        self.assertEqual(m.first_side_of(20, 10), "l1_first")
        self.assertEqual(m.first_side_of(10, None), "u1_first")
        self.assertEqual(m.first_side_of(None, 10), "l1_first")
        self.assertEqual(m.first_side_of(None, None), "neither")
        self.assertEqual(m.first_side_of(10, 10), "neither")


class ArtifactShapeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rep = json.loads(ARTIFACT.read_text(encoding="utf-8"))

    def test_levels_and_outcomes_present(self):
        self.assertEqual(set(self.rep["levels"]), {"u1", "u2", "l1", "l2"})
        for lid, L in self.rep["levels"].items():
            self.assertIn("intraday_touch", L)
            for N in ("15", "30", "60"):
                blk = L["post_touch_by_N"][N]
                for k in ("acceptance", "rejection", "undetermined"):
                    self.assertIn(k, blk)
                # the three post-touch outcomes share one touched-session base
                self.assertEqual(blk["acceptance"]["n"], blk["rejection"]["n"], f"{lid}.{N}")
                self.assertEqual(blk["acceptance"]["n"], blk["undetermined"]["n"], f"{lid}.{N}")

    def test_abstention_is_honest(self):
        for lid, L in self.rep["levels"].items():
            self.assertEqual(L["intraday_touch"]["n"], self.rep["n_sessions"], lid)
            a30 = L["post_touch_by_N"]["30"]["acceptance"]
            if a30["n"] < self.rep["unconditional_min_n"]:
                self.assertFalse(a30["sufficient"], f"{lid} should abstain at n={a30['n']}")

    def test_first_side_partitions_sessions(self):
        fs = self.rep["first_side_touched"]
        self.assertEqual(fs["u1_first"] + fs["l1_first"] + fs["neither"], fs["n"])
        self.assertEqual(fs["n"], self.rep["n_sessions"])

    def test_threshold_and_horizons_frozen(self):
        self.assertAlmostEqual(self.rep["post_touch_threshold_pct"], 0.002, places=9)
        self.assertEqual(self.rep["horizons_min"], [15, 30, 60])


@unittest.skipUnless(SNAPSHOT.exists(), "intraday snapshot not committed; regenerate from the runtime DB")
class ReproductionTest(unittest.TestCase):
    def test_rebuild_matches_committed(self):
        rep = m.build()
        committed = json.loads(ARTIFACT.read_text(encoding="utf-8"))
        self.assertEqual(rep["n_sessions"], committed["n_sessions"])
        for lid in committed["levels"]:
            self.assertEqual(rep["levels"][lid]["intraday_touch"]["hits"],
                             committed["levels"][lid]["intraday_touch"]["hits"], lid)


if __name__ == "__main__":
    unittest.main()
