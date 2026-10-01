"""Reproducibility + calibration-behavior guards for the level-behavior evidence layer.

Closes two gaps flagged in the PR #70 convergence review:
  (1) the committed evidence numbers must be reproducible from the committed data
      snapshot (now tracked via a .gitignore negation), so a silent pipeline change
      that altered the published base rates would be caught; and
  (2) regime_calibration.calibration()/descriptive() — the point-in-time, abstaining
      core the product's credibility rests on — must be exercised, not merely imported.

Point-in-time discipline and the n>=MIN_BUCKET abstention fallback (prereg §5, §7)
are asserted directly here.
"""
from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BUILD = ROOT / "scripts" / "levels_evidence" / "build_daily_event_table.py"
RC = ROOT / "scripts" / "levels_evidence" / "regime_calibration.py"
SNAPSHOT = ROOT / "data" / "levels_evidence" / "spy_daily_snapshot.json"
DAILY_JSON = ROOT / "research" / "levels_evidence" / "daily_touch_rates.json"
REGIME_JSON = ROOT / "research" / "levels_evidence" / "regime_calibration.json"

# Integer counts (hits, n, scored_sessions) must match EXACTLY. Rates/Brier are
# compared with a tolerance: the last bits of a float sum differ across numpy/Python
# builds even though the statistic is identical to ~15 significant figures.
FLOAT_TOL = 1e-9


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(m)
    return m


mod = _load("levels_evidence_build", BUILD)
rc = _load("regime_calibration", RC)


@unittest.skipUnless(SNAPSHOT.exists(), "reproducibility snapshot not present on this checkout")
class ReproducibilityTest(unittest.TestCase):
    """The committed report JSONs must recompute from the committed snapshot."""

    @classmethod
    def setUpClass(cls):
        cls.bars, cls.data_hash = mod.fetch_or_load_snapshot()
        cls.daily = json.loads(DAILY_JSON.read_text(encoding="utf-8"))
        cls.regime = json.loads(REGIME_JSON.read_text(encoding="utf-8"))

    def test_snapshot_hash_matches_both_reports(self):
        # The provenance hash stamped into the published artifacts must be the hash of
        # the committed snapshot — otherwise the numbers came from a different input.
        self.assertEqual(self.data_hash, self.daily["data_snapshot_sha256_16"])
        self.assertEqual(self.data_hash, self.regime["data_snapshot_sha256_16"])

    def test_daily_rates_reproduce(self):
        agg = mod.aggregate(mod.build_events(self.bars))
        self.assertEqual(agg["n_sessions"], self.daily["n_sessions"])
        committed = {m["metric"]: m for m in self.daily["metrics"]}
        self.assertEqual({m["metric"] for m in agg["metrics"]}, set(committed))
        for m in agg["metrics"]:
            c = committed[m["metric"]]
            self.assertEqual(m["hits"], c["hits"], m["metric"])     # exact
            self.assertEqual(m["n"], c["n"], m["metric"])           # exact
            self.assertAlmostEqual(m["rate"], c["rate"], delta=FLOAT_TOL, msg=m["metric"])

    def test_regime_descriptive_reproduces(self):
        events = rc.build_augmented(self.bars)
        self.assertEqual(len(events), self.regime["n_events"])
        desc = rc.descriptive(events)
        for outcome, blocks in self.regime["descriptive"].items():
            for dim, rows in blocks.items():
                got = {r["bucket"]: r for r in desc[outcome][dim]}
                for r in rows:
                    where = f"{outcome}.{dim}.{r['bucket']}"
                    g = got[r["bucket"]]
                    self.assertEqual(g["hits"], r["hits"], where)   # exact
                    self.assertEqual(g["n"], r["n"], where)         # exact
                    self.assertEqual(g["sufficient"], r["sufficient"], where)
                    self.assertAlmostEqual(g["rate"], r["rate"], delta=FLOAT_TOL, msg=where)

    def test_regime_calibration_brier_reproduces(self):
        events = rc.build_augmented(self.bars)
        self.assertTrue(self.regime["calibration"], "no calibration outcomes committed")
        for outcome, c in self.regime["calibration"].items():
            k = 2 if outcome.endswith("2") else 1
            got = rc.calibration(events, outcome, k)
            self.assertEqual(got["scored_sessions"], c["scored_sessions"], outcome)
            self.assertAlmostEqual(got["realized_rate"], c["realized_rate"], delta=FLOAT_TOL)
            for predictor, bval in c["brier"].items():
                self.assertAlmostEqual(got["brier"][predictor], bval, delta=FLOAT_TOL,
                                       msg=f"{outcome}.{predictor}")
            # The §7/§10 verdict fields the display layer relies on must be present and
            # self-consistent with the Brier scores.
            for field in ("best_predictor", "gap_beats_unconditional", "gap_rel_improvement"):
                self.assertIn(field, c, f"{outcome} missing {field}")
            self.assertEqual(got["best_predictor"], min(got["brier"], key=got["brier"].get), outcome)
            self.assertEqual(got["gap_beats_unconditional"],
                             got["brier"]["gap_at_open"] < got["brier"]["unconditional"], outcome)
        # Sanity: the documented honest edge actually holds in the committed numbers.
        u1 = self.regime["calibration"]["touch_u1"]["brier"]
        self.assertLess(u1["gap_at_open"], u1["unconditional"])


def _synth_events(n: int, gap_fn, outcome_fn, trend_fn=None) -> list[dict]:
    """Minimal event dicts carrying only the keys calibration() reads."""
    ev = []
    for i in range(n):
        ev.append({
            "sigma": 0.01 + (i % 5) * 1e-4,     # spread so trailing terciles are well-defined
            "trend": (trend_fn(i) if trend_fn else ("up" if i % 2 else "down")),
            "gap": gap_fn(i),
            "touch_u1": outcome_fn(i),
        })
    return ev


class CalibrationBehaviorTest(unittest.TestCase):
    """Directly exercise the point-in-time, abstaining core (prereg §5, §7)."""

    def test_scored_sessions_equals_events_minus_burn_in(self):
        ev = _synth_events(rc.BURN_IN + 40, lambda i: "flat", lambda i: i % 2)
        out = rc.calibration(ev, "touch_u1", 1)
        self.assertEqual(out["scored_sessions"], len(ev) - rc.BURN_IN)

    def test_gap_predictor_abstains_to_unconditional_when_bucket_thin(self):
        # A handful of "down" gaps, all inside the scored region, so that when each is
        # scored its prior "down" count is < MIN_BUCKET -> the gap predictor must fall
        # back to the unconditional rate rather than a fragile small-sample bucket rate.
        down_idx = {rc.BURN_IN + 8, rc.BURN_IN + 13, rc.BURN_IN + 18,
                    rc.BURN_IN + 23, rc.BURN_IN + 28}
        self.assertLess(len(down_idx), rc.MIN_BUCKET)
        ev = _synth_events(rc.BURN_IN + 40,
                           lambda i: "down" if i in down_idx else "flat",
                           lambda i: i % 3 == 0)
        out = rc.calibration(ev, "touch_u1", 1, return_preds=True)
        scored = ev[rc.BURN_IN:]
        thin_positions = [idx for idx, e in enumerate(scored) if e["gap"] == "down"]
        self.assertTrue(thin_positions, "test did not actually exercise the abstention path")
        for idx in thin_positions:
            self.assertEqual(out["preds"]["gap_at_open"][idx], out["preds"]["unconditional"][idx])

    def test_calibration_is_point_in_time(self):
        # Flipping a scored session's OWN outcome must not change any predictor (predictors
        # use prior = events[:j] only); only the realized label y may change. Guards against
        # a future events[:j+1] self-leak regression.
        ev = _synth_events(rc.BURN_IN + 10,
                           lambda i: ("up" if i % 2 else "down"),
                           lambda i: i % 2)
        base = rc.calibration(ev, "touch_u1", 1, return_preds=True)
        ev2 = [dict(e) for e in ev]
        ev2[-1]["touch_u1"] ^= 1
        flipped = rc.calibration(ev2, "touch_u1", 1, return_preds=True)
        self.assertEqual(base["preds"], flipped["preds"])   # predictors unchanged
        self.assertNotEqual(base["y"], flipped["y"])        # only the realized label changed


if __name__ == "__main__":
    unittest.main()
