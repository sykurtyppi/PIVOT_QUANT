"""Contract tests for the /levels phone page (daily_levels.html) and its server wiring.

Re-establishes the guard dropped in the PR #70 port (review finding #5), updated to
the honest standard: per-level historical rates MAY now be shown, but only as
expandable secondary context that carries a sample size, a Wilson CI, a point-in-time
label and 'not a forecast' framing — and the page must never show a normal-distribution
'68%' containment claim, an 'Expected move' label, or wire itself to the retired RF
break/reject ML surface.
"""
from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PAGE = (ROOT / "daily_levels.html").read_text(encoding="utf-8")
SERVER = (ROOT / "server" / "yahoo_proxy.js").read_text(encoding="utf-8")


class NoMisleadingProbabilityTest(unittest.TestCase):
    def test_no_normal_distribution_68_label(self):
        self.assertNotIn("68%", PAGE)
        self.assertNotIn("68.3%", PAGE)

    def test_bands_labelled_realized_vol_not_expected_move(self):
        self.assertNotIn("Expected move", PAGE)
        self.assertIn("realized-volatility", PAGE.lower())
        self.assertIn("close-to-close RV", PAGE)

    def test_reference_band_disclaimer_present(self):
        self.assertIn("Statistical reference bands", PAGE)

    def test_not_a_forecast_framing_present(self):
        self.assertIn("not a forecast", PAGE)
        self.assertIn("Historical", PAGE)


class HonestPerLevelContractTest(unittest.TestCase):
    def test_consumes_per_level_artifact(self):
        self.assertIn("/app/levels/level_probabilities.json", PAGE)

    def test_sample_size_and_ci_shown(self):
        self.assertIn("n=", PAGE)
        self.assertIn("Wilson 95% CI", PAGE)
        self.assertIn("ci_low", PAGE)
        self.assertIn("ci_high", PAGE)

    def test_honors_oos_verdict_and_abstention(self):
        # The page may upgrade to the gap-conditioned rate only where it beats
        # unconditional out-of-sample and the bucket is sufficiently sampled.
        self.assertIn("gap_beats_unconditional", PAGE)
        self.assertIn("gap_rel_improvement", PAGE)
        self.assertIn("sufficient", PAGE)

    def test_gap_cut_matches_evidence_layer(self):
        # gapBucket() must use the same ±0.3% cut the artifact was built with.
        self.assertIn("0.003", PAGE)

    def test_does_not_wire_page_to_retired_ml(self):
        # The retired RF reject/break surface is /api/levels; the phone page must not touch it.
        self.assertNotIn("/api/levels", PAGE)
        self.assertNotIn("hist_reject_rate", PAGE)
        self.assertNotIn("manifest_active", PAGE)


class IntradaySurfaceTest(unittest.TestCase):
    def test_consumes_intraday_artifact(self):
        self.assertIn("/app/levels/intraday_outcomes.json", PAGE)

    def test_intraday_honest_labels_and_gate(self):
        self.assertIn("insufficient data", PAGE)   # abstention is surfaced, not hidden
        self.assertIn("post_touch_by_N", PAGE)      # reads the frozen §3 structure
        self.assertIn("sufficient", PAGE)           # honors the n>=100 gate

    def test_server_serves_intraday_route(self):
        self.assertIn("'/app/levels/intraday_outcomes.json'", SERVER)
        self.assertIn("INTRADAY_OUTCOMES_FILE", SERVER)


class ServerRouteTest(unittest.TestCase):
    def test_level_probabilities_route_served(self):
        self.assertIn("'/app/levels/level_probabilities.json'", SERVER)
        self.assertIn("LEVEL_PROBS_FILE", SERVER)

    def test_existing_evidence_routes_intact(self):
        self.assertIn("'/app/levels/regime_rates.json'", SERVER)
        self.assertIn("'/app/levels/touch_rates.json'", SERVER)


if __name__ == "__main__":
    unittest.main()
