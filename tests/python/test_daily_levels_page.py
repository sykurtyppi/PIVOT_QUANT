"""Contract tests for the /levels daily-levels page.

These lock in the trust/correctness fixes so they cannot silently regress:
market state + quote freshness, the NYSE calendar wiring, the realized-volatility
labelling (not "expected move"), and the absence of any naked probability label.
Behavioural correctness of the calendar and the volatility engine is covered by
their own jest suites (tests/forecast/nyseCalendar.test.js,
tests/MultiHorizonVolatilityLevels.test.js).
"""
from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PAGE = ROOT / "daily_levels.html"
STATIC_ROUTES = ROOT / "server" / "routes" / "static.js"


class DailyLevelsPageContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = PAGE.read_text(encoding="utf-8")
        cls.routes = STATIC_ROUTES.read_text(encoding="utf-8")

    # --- #3 realized-volatility labelling (not "expected move") ---
    def test_realized_volatility_label_not_expected_move(self):
        self.assertIn("Today’s realized-volatility bands", self.html)
        self.assertIn("close-to-close RV", self.html)
        self.assertNotIn("Expected move", self.html)

    def test_reference_band_disclaimer_present(self):
        self.assertIn(
            "Statistical reference bands — not price targets or guaranteed containment ranges.",
            self.html,
        )

    # --- no naked probability label on levels ---
    def test_no_naked_probability_label(self):
        # A generic "68%" / "68.3%" containment claim would be misleading.
        self.assertNotIn("68%", self.html)
        self.assertNotIn("68.3%", self.html)
        self.assertNotIn("probability", self.html.lower())

    # --- #1 market state + quote freshness ---
    def test_market_state_and_quote_freshness_wired(self):
        self.assertIn("data.marketState", self.html)
        self.assertIn("regularMarketTime", self.html)  # quote-as-of timestamp
        self.assertIn("isLastSessionComplete", self.html)
        self.assertIn("Yahoo, delayed", self.html)  # explicit source + delay
        # price label switches Last/Now rather than always claiming "Now"
        self.assertIn("(state === 'REGULAR') ? 'Now' : 'Last'", self.html)

    def test_uses_exchange_timezone_not_utc_for_session_dates(self):
        self.assertIn("America/New_York", self.html)
        self.assertIn("en-CA", self.html)  # YYYY-MM-DD key in the exchange tz

    # --- #2 NYSE calendar wiring (weekend/holiday safe) ---
    def test_nyse_calendar_wired(self):
        self.assertIn("/app/levels/nyse_calendar.js", self.html)
        self.assertIn("isTradingSession", self.html)
        self.assertIn("nextTradingSession", self.html)
        self.assertIn("holidaysInRange", self.html)
        # asOf falls forward to the next trading session when today is closed
        self.assertIn(
            "isTradingSession(todayKey) ? todayKey : nextTradingSession(todayKey)",
            self.html,
        )

    def test_calendar_module_route_registered(self):
        self.assertIn("'/app/levels/nyse_calendar.js'", self.routes)
        self.assertIn("NYSE_CALENDAR_JS", self.routes)

    # --- #5 minor trust fixes ---
    def test_no_negative_zero_percent(self):
        # pct() collapses values that round to zero to "0.00%" (never "-0.00%")
        self.assertIn("if (r === 0) return '0.00%';", self.html)
        self.assertIn("Unchanged", self.html)

    def test_levels_coloured_by_position_relative_to_price(self):
        # colour encodes above/below the current price, not resistance/support
        self.assertIn("r.value >= price ? 'above' : 'below'", self.html)

    def test_horizon_cards_have_upper_lower_labels_and_anchor(self):
        self.assertIn("Upper +1σ", self.html)
        self.assertIn("Lower −1σ", self.html)
        self.assertIn("anchor ", self.html)

    # --- #4 nearest-level hero card ---
    def test_nearest_level_card_present(self):
        self.assertIn('id="near-above"', self.html)
        self.assertIn('id="near-below"', self.html)
        self.assertIn("Nearest ${side}", self.html)
        self.assertIn("function renderNearest", self.html)

    def test_nearest_card_shows_distance_family_and_confluence(self):
        self.assertIn("function confluenceFor", self.html)          # cross-family confluence
        self.assertIn("confluence must come from an independent family", self.html)
        self.assertIn('class="fam"', self.html)                     # level family shown
        self.assertIn("Confluence", self.html)

    # --- evidence layer: historical base rates surfaced as SECONDARY, honest context ---
    def test_historical_behaviour_drawer_present(self):
        self.assertIn('id="evidence-daily"', self.html)
        self.assertIn("Historical behaviour", self.html)
        self.assertIn("/app/levels/touch_rates.json", self.html)   # canonical, from the pipeline
        self.assertIn("function renderEvidence", self.html)

    def test_evidence_is_labelled_as_base_rates_not_prediction(self):
        self.assertIn("Base rates, not a prediction for today.", self.html)
        self.assertIn("touch = intraday high/low reached the level", self.html)
        self.assertIn("Reached +1σ before close", self.html)
        self.assertIn("Closed inside ±1σ", self.html)     # keep touch vs close-inside distinct

    def test_touch_rates_route_registered(self):
        self.assertIn("'/app/levels/touch_rates.json'", self.routes)
        self.assertIn("TOUCH_RATES_FILE", self.routes)

    # --- gap-conditioned "today's setup" line (the out-of-sample edge, surfaced honestly) ---
    def test_gap_setup_line_present(self):
        self.assertIn('id="rv-setup"', self.html)
        self.assertIn("/app/levels/regime_rates.json", self.html)
        self.assertIn("function renderSetup", self.html)
        self.assertIn("function gapBucket", self.html)

    def test_gap_setup_uses_open_and_is_labelled_historical(self):
        self.assertIn("todayBar.open", self.html)                 # gap needs the session open
        self.assertIn("Historical, not a forecast.", self.html)   # not a prediction
        self.assertIn("Comparable ", self.html)                   # framed as comparable sessions

    def test_regime_rates_route_registered(self):
        self.assertIn("'/app/levels/regime_rates.json'", self.routes)
        self.assertIn("REGIME_RATES_FILE", self.routes)


if __name__ == "__main__":
    unittest.main()
