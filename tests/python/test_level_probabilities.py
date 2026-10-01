"""Guards the per-level probability contract served to /levels (prereg §8).

The page renders exactly what this artifact carries, so the shape, the gap bucket
cut (must match the page's gapBucket()), the honest §7 verdict fields, and the
documented edge direction are all pinned here.
"""
from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BUILDER = ROOT / "scripts" / "levels_evidence" / "build_level_probabilities.py"
ARTIFACT = ROOT / "research" / "levels_evidence" / "level_probabilities.json"
SNAPSHOT = ROOT / "data" / "levels_evidence" / "spy_daily_snapshot.json"
DAILY = ROOT / "research" / "levels_evidence" / "daily_touch_rates.json"
FLOAT_TOL = 1e-9

_spec = importlib.util.spec_from_file_location("build_level_probabilities", BUILDER)
blp = importlib.util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(blp)


@unittest.skipUnless(SNAPSHOT.exists(), "reproducibility snapshot not present on this checkout")
class LevelProbabilitiesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rep = blp.build()

    def test_four_levels_each_with_touch_and_close(self):
        self.assertEqual(set(self.rep["levels"]), {"u1", "u2", "l1", "l2"})
        for lid, L in self.rep["levels"].items():
            for outcome in ("touch", "close"):
                self.assertIn(outcome, L, f"{lid} missing {outcome}")
                blk = L[outcome]
                for key in ("rate", "ci_low", "ci_high", "n"):
                    self.assertIn(key, blk["unconditional"], f"{lid}.{outcome}.uncond.{key}")
                self.assertEqual(set(blk["by_gap"]), {"up", "flat", "down"}, f"{lid}.{outcome}")
                for g, row in blk["by_gap"].items():
                    for key in ("rate", "ci_low", "ci_high", "n", "sufficient"):
                        self.assertIn(key, row, f"{lid}.{outcome}.{g}.{key}")
                for key in ("best_predictor", "gap_beats_unconditional", "gap_rel_improvement"):
                    self.assertIn(key, blk["calibration"], f"{lid}.{outcome}.{key}")

    def test_gap_side_matches_level(self):
        self.assertEqual(self.rep["levels"]["u1"]["gap_side"], "up")
        self.assertEqual(self.rep["levels"]["u2"]["gap_side"], "up")
        self.assertEqual(self.rep["levels"]["l1"]["gap_side"], "down")
        self.assertEqual(self.rep["levels"]["l2"]["gap_side"], "down")

    def test_gap_cut_matches_page_constant(self):
        # The page's gapBucket() uses a ±0.3% cut; artifact and page must agree or the
        # displayed bucket won't correspond to the computed rate.
        self.assertAlmostEqual(self.rep["gap_cut"], 0.003, places=9)

    def test_verdict_fields_self_consistent(self):
        for lid, L in self.rep["levels"].items():
            for outcome in ("touch", "close"):
                c = L[outcome]["calibration"]
                self.assertEqual(c["gap_beats_unconditional"],
                                 c["brier_gap_at_open"] < c["brier_unconditional"],
                                 f"{lid}.{outcome}")

    def test_gap_direction_edge(self):
        # Documented edge direction: a gap up lifts the upper +1σ touch well above
        # unconditional and a gap down pushes it below; symmetric for the lower band.
        u1t = self.rep["levels"]["u1"]["touch"]
        self.assertGreater(u1t["by_gap"]["up"]["rate"], u1t["unconditional"]["rate"])
        self.assertLess(u1t["by_gap"]["down"]["rate"], u1t["unconditional"]["rate"])
        l1t = self.rep["levels"]["l1"]["touch"]
        self.assertGreater(l1t["by_gap"]["down"]["rate"], l1t["unconditional"]["rate"])
        self.assertLess(l1t["by_gap"]["up"]["rate"], l1t["unconditional"]["rate"])

    def test_unconditional_matches_committed_daily(self):
        metrics = {m["metric"]: m for m in json.loads(DAILY.read_text())["metrics"]}
        for lid, L in self.rep["levels"].items():
            for outcome in ("touch", "close"):
                blk = L[outcome]
                m = metrics[blk["outcome"]]
                self.assertEqual(blk["unconditional"]["n"], m["n"], f"{lid}.{outcome}")
                self.assertAlmostEqual(blk["unconditional"]["rate"], m["rate"], delta=FLOAT_TOL)

    def test_committed_artifact_matches_rebuild(self):
        committed = json.loads(ARTIFACT.read_text())
        self.assertEqual(committed["data_snapshot_sha256_16"], self.rep["data_snapshot_sha256_16"])
        self.assertEqual(set(committed["levels"]), set(self.rep["levels"]))
        for lid, L in self.rep["levels"].items():
            for outcome in ("touch", "close"):
                cu = committed["levels"][lid][outcome]["unconditional"]
                ru = L[outcome]["unconditional"]
                self.assertEqual(cu["n"], ru["n"], f"{lid}.{outcome}")
                self.assertAlmostEqual(cu["rate"], ru["rate"], delta=FLOAT_TOL)


if __name__ == "__main__":
    unittest.main()
