import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from services.external_data.historical_walk_forward import (
    WalkForwardRegimeConfig,
    WalkForwardRuleConfig,
    _assign_regime_buckets,
    _compute_realized_vol,
    build_historical_walk_forward_from_t9,
    build_historical_walk_forward_report,
)


def _duckdb_available() -> bool:
    return shutil.which("duckdb") is not None


@unittest.skipUnless(_duckdb_available(), "duckdb CLI is required for parquet fixture tests")
class TestHistoricalWalkForwardFromT9(unittest.TestCase):
    def _write_parquet(self, path: Path, select_sql: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["duckdb", "-c", f"COPY ({select_sql}) TO '{path}' (FORMAT PARQUET)"],
            check=True,
            capture_output=True,
            text=True,
        )

    def _build_fake_t9(self, root: Path) -> None:
        daily_path = (
            root
            / "market_data"
            / "normalized"
            / "underlyings"
            / "daily_ohlcv"
            / "underlying_symbol=SPY"
            / "year=2024"
            / "month=01"
            / "spy_daily_ohlcv_yahoo_2024-01.parquet"
        )
        option_path = (
            root
            / "market_data"
            / "research"
            / "options_features_eod"
            / "underlying_symbol=SPY"
            / "year=2024"
            / "month=01"
            / "spy_options_features_eod_2024-01.parquet"
        )
        self._write_parquet(
            daily_path,
            """
            SELECT * FROM (
              VALUES
                (DATE '2024-01-02', 'SPY', 4700000::BIGINT, 4720000::BIGINT, 4690000::BIGINT, 4710000::BIGINT, 1000::BIGINT, 'yahoo'),
                (DATE '2024-01-03', 'SPY', 4710000::BIGINT, 4730000::BIGINT, 4700000::BIGINT, 4720000::BIGINT, 1200::BIGINT, 'yahoo'),
                (DATE '2024-01-04', 'SPY', 4720000::BIGINT, 4740000::BIGINT, 4710000::BIGINT, 4730000::BIGINT, 1300::BIGINT, 'yahoo'),
                (DATE '2024-01-05', 'SPY', 4730000::BIGINT, 4750000::BIGINT, 4720000::BIGINT, 4740000::BIGINT, 1400::BIGINT, 'yahoo'),
                (DATE '2024-01-08', 'SPY', 4740000::BIGINT, 4760000::BIGINT, 4730000::BIGINT, 4750000::BIGINT, 1500::BIGINT, 'yahoo')
            ) AS t(trade_date, underlying_symbol, open_10000, high_10000, low_10000, close_10000, volume, vendor)
            """,
        )
        self._write_parquet(
            option_path,
            """
            SELECT * FROM (
              VALUES
                (DATE '2024-01-02', DATE '2024-01-19', 'SPY', 'C', 471.0, 1.10, 1.20, 1.15, 42::BIGINT, 100::BIGINT, 0.22),
                (DATE '2024-01-03', DATE '2024-01-19', 'SPY', 'P', 472.0, 1.30, 1.40, 1.35, 84::BIGINT, 200::BIGINT, 0.25),
                (DATE '2024-01-04', DATE '2024-01-19', 'SPY', 'C', 473.0, 1.50, 1.60, 1.55, 21::BIGINT, 80::BIGINT, 0.21),
                (DATE '2024-01-05', DATE '2024-01-19', 'SPY', 'P', 474.0, 1.70, 1.80, 1.75, 31::BIGINT, 90::BIGINT, 0.23)
            ) AS t(trade_date, expiry, underlying_symbol, call_put, strike, bid, ask, mid, volume, open_interest, iv)
            """,
        )

    def test_builds_windows_from_temp_parquet_without_training(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._build_fake_t9(root)
            wf = build_historical_walk_forward_from_t9(
                root=root,
                symbol="SPY",
                start_date="2024-01-02",
                end_date="2024-01-10",
                max_files=5,
                daily_source="yahoo",
                horizons=["1d"],
                train_window=2,
                test_window=2,
                step=2,
            )

        self.assertEqual(wf.report["status"], "pass")
        self.assertFalse(wf.report["training_performed"])
        self.assertEqual(wf.report["window_count"], 2)
        self.assertEqual(wf.report["leakage_checks"]["status"], "pass")
        self.assertEqual(wf.report["windows"][0]["train_trading_days"], 2)
        self.assertEqual(wf.report["windows"][0]["test_trading_days"], 2)


class TestHistoricalWalkForwardReport(unittest.TestCase):
    """Tests using pure in-memory fixtures; no T9 or duckdb required."""

    def _daily(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"date": "2024-01-02"},
                {"date": "2024-01-03"},
                {"date": "2024-01-04"},
                {"date": "2024-01-05"},
                {"date": "2024-01-08"},
                {"date": "2024-01-09"},
            ]
        )

    def _labels(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "observation_date": "2024-01-02",
                    "label_date": "2024-01-03",
                    "horizon": "1d",
                    "forward_return": 0.01,
                    "underlying_symbol": "SPY",
                    "expiration": "2024-01-19",
                    "strike": 471.0,
                    "option_type": "C",
                },
                {
                    "observation_date": "2024-01-03",
                    "label_date": "2024-01-04",
                    "horizon": "1d",
                    "forward_return": -0.01,
                    "underlying_symbol": "SPY",
                    "expiration": "2024-01-19",
                    "strike": 472.0,
                    "option_type": "P",
                },
                {
                    "observation_date": "2024-01-08",
                    "label_date": "2024-01-09",
                    "horizon": "1d",
                    "forward_return": 0.02,
                    "underlying_symbol": "SPY",
                    "expiration": "2024-01-19",
                    "strike": 471.0,
                    "option_type": "C",
                },
            ]
        )

    def _options(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "date": "2024-01-02",
                    "underlying_symbol": "SPY",
                    "expiration": "2024-01-19",
                    "strike": 471.0,
                    "option_type": "C",
                    "volume": 42,
                    "open_interest": 100,
                    "moneyness": 0.002,
                    "relative_spread": 0.08,
                    "bid": 1.1,
                    "ask": 1.2,
                    "mid": 1.15,
                },
                {
                    "date": "2024-01-03",
                    "underlying_symbol": "SPY",
                    "expiration": "2024-01-19",
                    "strike": 472.0,
                    "option_type": "P",
                    "volume": 5,
                    "open_interest": 50,
                    "moneyness": -0.002,
                    "relative_spread": 0.07,
                    "bid": 1.3,
                    "ask": 1.4,
                    "mid": 1.35,
                },
                {
                    "date": "2024-01-08",
                    "underlying_symbol": "SPY",
                    "expiration": "2024-01-19",
                    "strike": 471.0,
                    "option_type": "C",
                    "volume": 84,
                    "open_interest": 200,
                    "moneyness": 0.001,
                    "relative_spread": 0.06,
                    "bid": 1.5,
                    "ask": 1.6,
                    "mid": 1.55,
                },
            ]
        )

    # ---- multi-window fixtures for cross-window tests ----

    def _daily_multi(self) -> pd.DataFrame:
        # 10 trading days -> exactly 4 windows with train=2, test=2, step=2
        # Formula: windows = floor((10-2)/2) = 4; start_index=8 -> 8+2=10 not <10 -> stops
        dates = [
            "2024-01-02", "2024-01-03",  # train wf_001
            "2024-01-04", "2024-01-05",  # test wf_001 / train wf_002
            "2024-01-08", "2024-01-09",  # test wf_002 / train wf_003
            "2024-01-10", "2024-01-11",  # test wf_003 / train wf_004
            "2024-01-12", "2024-01-15",  # test wf_004
        ]
        return pd.DataFrame({"date": dates})

    def _labels_multi(self) -> pd.DataFrame:
        # Observation dates fall in test windows; label_date = next calendar day
        rows = [
            # wf_001 test: Jan 4-5 -> returns 0.02, 0.01 -> mean=0.015 win=100%
            {"observation_date": "2024-01-04", "label_date": "2024-01-05", "horizon": "1d",
             "forward_return": 0.02, "underlying_symbol": "SPY", "expiration": "2024-02-16",
             "strike": 470.0, "option_type": "C"},
            {"observation_date": "2024-01-05", "label_date": "2024-01-06", "horizon": "1d",
             "forward_return": 0.01, "underlying_symbol": "SPY", "expiration": "2024-02-16",
             "strike": 470.0, "option_type": "C"},
            # wf_002 test: Jan 8-9 -> returns -0.01, -0.02 -> mean=-0.015 win=0%
            {"observation_date": "2024-01-08", "label_date": "2024-01-09", "horizon": "1d",
             "forward_return": -0.01, "underlying_symbol": "SPY", "expiration": "2024-02-16",
             "strike": 470.0, "option_type": "C"},
            {"observation_date": "2024-01-09", "label_date": "2024-01-10", "horizon": "1d",
             "forward_return": -0.02, "underlying_symbol": "SPY", "expiration": "2024-02-16",
             "strike": 470.0, "option_type": "C"},
            # wf_003 test: Jan 10-11 -> returns 0.03, 0.04 -> mean=0.035 win=100%
            {"observation_date": "2024-01-10", "label_date": "2024-01-11", "horizon": "1d",
             "forward_return": 0.03, "underlying_symbol": "SPY", "expiration": "2024-02-16",
             "strike": 470.0, "option_type": "C"},
            {"observation_date": "2024-01-11", "label_date": "2024-01-12", "horizon": "1d",
             "forward_return": 0.04, "underlying_symbol": "SPY", "expiration": "2024-02-16",
             "strike": 470.0, "option_type": "C"},
            # wf_004 test: Jan 12-15 -> returns 0.05, 0.06 -> mean=0.055 win=100%
            {"observation_date": "2024-01-12", "label_date": "2024-01-13", "horizon": "1d",
             "forward_return": 0.05, "underlying_symbol": "SPY", "expiration": "2024-02-16",
             "strike": 470.0, "option_type": "C"},
            {"observation_date": "2024-01-15", "label_date": "2024-01-16", "horizon": "1d",
             "forward_return": 0.06, "underlying_symbol": "SPY", "expiration": "2024-02-16",
             "strike": 470.0, "option_type": "C"},
        ]
        return pd.DataFrame(rows)

    def _options_multi(self) -> pd.DataFrame:
        """Option rows matching every observation_date in _labels_multi; oi=200 passes min_oi=100."""
        obs_dates = ["2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09",
                     "2024-01-10", "2024-01-11", "2024-01-12", "2024-01-15"]
        return pd.DataFrame([
            {
                "date": d,
                "underlying_symbol": "SPY",
                "expiration": "2024-02-16",
                "strike": 470.0,
                "option_type": "C",
                "volume": 50,
                "open_interest": 200,
                "moneyness": 0.001,
                "relative_spread": 0.06,
                "bid": 1.5,
                "ask": 1.6,
                "mid": 1.55,
            }
            for d in obs_dates
        ])

    # ---- original PR-7 tests ----

    def test_includes_zero_row_windows_and_counts(self):
        wf = build_historical_walk_forward_report(
            model_ready_daily_features=self._daily(),
            label_candidates=self._labels(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-09",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
        )

        self.assertEqual(wf.report["status"], "pass")
        self.assertEqual(wf.report["window_count"], 2)
        self.assertEqual(wf.report["zero_row_window_count"], 1)
        self.assertEqual(wf.report["windows"][0]["test_row_count"], 0)
        self.assertEqual(wf.report["windows"][1]["test_row_count"], 1)
        self.assertEqual(wf.report["leakage_checks"]["status"], "pass")

    def test_leakage_check_fails_when_label_date_is_not_future(self):
        labels = self._labels()
        labels.loc[0, "label_date"] = "2024-01-02"
        wf = build_historical_walk_forward_report(
            model_ready_daily_features=self._daily(),
            label_candidates=labels,
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-09",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
        )

        self.assertEqual(wf.report["status"], "fail")
        self.assertEqual(wf.report["leakage_checks"]["status"], "fail")

    def test_rejects_non_positive_window_config(self):
        with self.assertRaises(ValueError):
            build_historical_walk_forward_report(
                model_ready_daily_features=self._daily(),
                label_candidates=self._labels(),
                symbol="SPY",
                start_date="2024-01-02",
                end_date="2024-01-09",
                horizons=["1d"],
                train_window=0,
                test_window=2,
                step=2,
            )

    # ---- PR-8 rule-baseline tests ----

    def test_rule_baseline_selects_expected_rows(self):
        rule_config = WalkForwardRuleConfig(min_open_interest=100, min_volume=1)
        wf = build_historical_walk_forward_report(
            model_ready_daily_features=self._daily(),
            label_candidates=self._labels(),
            option_context_features=self._options(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-09",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
            rule_config=rule_config,
        )

        self.assertTrue(wf.report["rule_baseline_applied"])
        windows = wf.report["windows"]
        # wf_001 test: Jan 4-5 — no matching option rows -> non_evaluable
        rb_001 = windows[0]["rule_baseline"]
        self.assertTrue(rb_001["non_evaluable"])
        # wf_002 test: Jan 8-9 — option row oi=200 passes min_oi=100
        rb_002 = windows[1]["rule_baseline"]
        self.assertFalse(rb_002["non_evaluable"])
        self.assertEqual(rb_002["selected_rows"], 1)
        self.assertIsNotNone(rb_002["forward_return"]["mean"])

    def test_zero_row_window_marked_non_evaluable(self):
        rule_config = WalkForwardRuleConfig(min_open_interest=9999)
        wf = build_historical_walk_forward_report(
            model_ready_daily_features=self._daily(),
            label_candidates=self._labels(),
            option_context_features=self._options(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-09",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
            rule_config=rule_config,
        )

        for window in wf.report["windows"]:
            rb = window.get("rule_baseline", {})
            self.assertTrue(rb["non_evaluable"])
            self.assertIsNotNone(rb["non_evaluable_reason"])

    def test_training_and_threshold_flags_remain_false(self):
        rule_config = WalkForwardRuleConfig(min_open_interest=1, min_volume=1)
        wf = build_historical_walk_forward_report(
            model_ready_daily_features=self._daily(),
            label_candidates=self._labels(),
            option_context_features=self._options(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-09",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
            rule_config=rule_config,
        )

        self.assertFalse(wf.report["training_performed"])
        self.assertFalse(wf.report["threshold_optimization_performed"])

    def test_leakage_checks_pass_with_rule_baseline(self):
        rule_config = WalkForwardRuleConfig(min_open_interest=1, min_volume=1)
        wf = build_historical_walk_forward_report(
            model_ready_daily_features=self._daily(),
            label_candidates=self._labels(),
            option_context_features=self._options(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-09",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
            rule_config=rule_config,
        )

        self.assertEqual(wf.report["leakage_checks"]["status"], "pass")
        self.assertTrue(wf.report["leakage_checks"]["train_end_before_test_start"])
        self.assertTrue(wf.report["leakage_checks"]["no_test_dates_inside_train"])
        self.assertTrue(wf.report["leakage_checks"]["labels_have_future_dates"])

    def test_option_type_filter_call_only(self):
        rule_config = WalkForwardRuleConfig(option_type="C", min_open_interest=1, min_volume=1)
        wf = build_historical_walk_forward_report(
            model_ready_daily_features=self._daily(),
            label_candidates=self._labels(),
            option_context_features=self._options(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-09",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
            rule_config=rule_config,
        )

        windows = wf.report["windows"]
        rb_002 = windows[1].get("rule_baseline", {})
        by_type = rb_002.get("counts_by_option_type", {})
        self.assertNotIn("P", by_type)

    def test_option_type_alias_call_maps_to_c(self):
        """CLI passes 'call'; filter should match rows with option_type='C'."""
        rule_config = WalkForwardRuleConfig(option_type="call", min_open_interest=1, min_volume=1)
        wf = build_historical_walk_forward_report(
            model_ready_daily_features=self._daily(),
            label_candidates=self._labels(),
            option_context_features=self._options(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-09",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
            rule_config=rule_config,
        )

        # wf_002 has a C row (2024-01-08, oi=200) — should be selected
        rb_002 = wf.report["windows"][1]["rule_baseline"]
        self.assertFalse(rb_002["non_evaluable"])
        self.assertEqual(rb_002["selected_rows"], 1)

    # ---- PR-9 cross-window aggregation tests ----

    def _wf_multi(self, rule_config: WalkForwardRuleConfig | None = None) -> dict:
        return build_historical_walk_forward_report(
            model_ready_daily_features=self._daily_multi(),
            label_candidates=self._labels_multi(),
            option_context_features=self._options_multi(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-17",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
            rule_config=rule_config or WalkForwardRuleConfig(min_open_interest=100, min_volume=1),
        ).report

    def test_cross_window_summary_present_in_report(self):
        report = self._wf_multi()
        self.assertIn("cross_window_summary", report)
        cws = report["cross_window_summary"]
        self.assertIn("total_windows", cws)
        self.assertIn("zero_row_window_fraction", cws)

    def test_cross_window_aggregation_evaluable_windows(self):
        report = self._wf_multi()
        cws = report["cross_window_summary"]
        # 4 windows, all have option rows that pass oi=100 -> all evaluable
        self.assertEqual(cws["total_windows"], 4)
        self.assertEqual(cws["evaluable_windows"], 4)
        self.assertEqual(cws["non_evaluable_windows"], 0)
        self.assertEqual(cws["total_selected_rows"], 8)

    def test_cross_window_zero_row_fraction(self):
        # Make last window have no labels by using very narrow date range
        report = build_historical_walk_forward_report(
            model_ready_daily_features=self._daily_multi(),
            label_candidates=self._labels_multi().iloc[:6],  # only first 3 windows have labels
            option_context_features=self._options_multi(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-17",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
            rule_config=WalkForwardRuleConfig(min_open_interest=100, min_volume=1),
        ).report

        cws = report["cross_window_summary"]
        # wf_004 test window (Jan 12-15) has no labels -> zero_row_window
        self.assertGreater(cws["zero_row_window_count"], 0)
        self.assertGreater(cws["zero_row_window_fraction"], 0.0)
        self.assertLessEqual(cws["zero_row_window_fraction"], 1.0)

    def test_cross_window_best_worst_window(self):
        report = self._wf_multi()
        cws = report["cross_window_summary"]
        # wf_001 mean=0.015, wf_002 mean=-0.015, wf_003 mean=0.035, wf_004 mean=0.055
        self.assertIsNotNone(cws.get("best_window"))
        self.assertIsNotNone(cws.get("worst_window"))
        best = cws["best_window"]
        worst = cws["worst_window"]
        self.assertGreater(best["mean_return"], worst["mean_return"])
        # wf_004 is best (mean=0.055), wf_002 is worst (mean=-0.015)
        self.assertEqual(best["window_id"], "wf_004")
        self.assertEqual(worst["window_id"], "wf_002")

    def test_cross_window_by_horizon(self):
        report = self._wf_multi()
        cws = report["cross_window_summary"]
        by_horizon = cws.get("by_horizon", {})
        self.assertIn("1d", by_horizon)
        stats = by_horizon["1d"]
        self.assertEqual(stats["selected_rows"], 8)
        # Weighted mean across all 8 rows: (0.02+0.01-0.01-0.02+0.03+0.04+0.05+0.06)/8 = 0.0225
        self.assertAlmostEqual(stats["mean_return"], 0.0225, places=6)
        # win_rate: 6/8 positive = 0.75
        self.assertAlmostEqual(stats["win_rate"], 0.75, places=6)

    def test_cross_window_window_mean_returns_list(self):
        report = self._wf_multi()
        cws = report["cross_window_summary"]
        means = cws.get("window_mean_returns", [])
        self.assertEqual(len(means), 4)
        # Check order: wf_001=0.015, wf_002=-0.015, wf_003=0.035, wf_004=0.055
        self.assertAlmostEqual(means[0], 0.015, places=6)
        self.assertAlmostEqual(means[1], -0.015, places=6)
        self.assertAlmostEqual(means[2], 0.035, places=6)
        self.assertAlmostEqual(means[3], 0.055, places=6)

    def test_cross_window_no_training_threshold_flags(self):
        report = self._wf_multi()
        self.assertFalse(report["training_performed"])
        self.assertFalse(report["threshold_optimization_performed"])

    def test_cross_window_summary_without_rule_baseline(self):
        # Without rule config, cross_window_summary has basic fields but no horizon/best/worst
        report = build_historical_walk_forward_report(
            model_ready_daily_features=self._daily_multi(),
            label_candidates=self._labels_multi(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-17",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
        ).report

        cws = report["cross_window_summary"]
        self.assertIn("total_windows", cws)
        self.assertIn("zero_row_window_fraction", cws)
        self.assertNotIn("evaluable_windows", cws)
        self.assertNotIn("by_horizon", cws)


class TestRegimeClassification(unittest.TestCase):
    """Tests for regime signal computation and bucket assignment."""

    def _daily_stable(self) -> pd.DataFrame:
        """10 dates with very stable closes -> low realized vol in all windows."""
        return pd.DataFrame([
            {"date": "2024-01-02", "close": 100.00},
            {"date": "2024-01-03", "close": 100.10},
            {"date": "2024-01-04", "close": 100.05},
            {"date": "2024-01-05", "close": 100.08},
            {"date": "2024-01-08", "close": 100.12},
            {"date": "2024-01-09", "close": 100.03},
            {"date": "2024-01-10", "close": 100.07},
            {"date": "2024-01-11", "close": 100.09},
            {"date": "2024-01-12", "close": 100.06},
            {"date": "2024-01-15", "close": 100.11},
        ])

    def _daily_vol_pattern(self) -> pd.DataFrame:
        """10 dates where vol clearly increases across windows.

        Window train_end vols (lookback=2):
          wf_001 train_end=Jan 3:  1 return available -> insufficient_history
          wf_002 train_end=Jan 5:  low vol (tiny moves Jan 4-5)
          wf_003 train_end=Jan 9:  high vol (large moves Jan 8-9)
          wf_004 train_end=Jan 11: mid vol (moderate moves Jan 10-11)
        """
        return pd.DataFrame([
            {"date": "2024-01-02", "close": 100.00},
            {"date": "2024-01-03", "close": 100.10},  # +0.001
            {"date": "2024-01-04", "close": 101.00},  # +0.009
            {"date": "2024-01-05", "close":  99.00},  # -0.020
            {"date": "2024-01-08", "close": 103.00},  # +0.040
            {"date": "2024-01-09", "close":  95.00},  # -0.078
            {"date": "2024-01-10", "close":  97.00},  # +0.021
            {"date": "2024-01-11", "close":  95.00},  # -0.021
            {"date": "2024-01-12", "close":  95.50},  # +0.005
            {"date": "2024-01-15", "close":  95.20},  # -0.003
        ])

    def _daily_multi(self) -> pd.DataFrame:
        dates = [
            "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
            "2024-01-08", "2024-01-09", "2024-01-10", "2024-01-11",
            "2024-01-12", "2024-01-15",
        ]
        return pd.DataFrame({"date": dates})

    def _labels_multi(self) -> pd.DataFrame:
        rows = [
            {"observation_date": "2024-01-04", "label_date": "2024-01-05", "horizon": "1d",
             "forward_return": 0.02, "underlying_symbol": "SPY", "expiration": "2024-02-16",
             "strike": 470.0, "option_type": "C"},
            {"observation_date": "2024-01-05", "label_date": "2024-01-06", "horizon": "1d",
             "forward_return": 0.01, "underlying_symbol": "SPY", "expiration": "2024-02-16",
             "strike": 470.0, "option_type": "C"},
            {"observation_date": "2024-01-08", "label_date": "2024-01-09", "horizon": "1d",
             "forward_return": -0.01, "underlying_symbol": "SPY", "expiration": "2024-02-16",
             "strike": 470.0, "option_type": "C"},
            {"observation_date": "2024-01-09", "label_date": "2024-01-10", "horizon": "1d",
             "forward_return": -0.02, "underlying_symbol": "SPY", "expiration": "2024-02-16",
             "strike": 470.0, "option_type": "C"},
            {"observation_date": "2024-01-10", "label_date": "2024-01-11", "horizon": "1d",
             "forward_return": 0.03, "underlying_symbol": "SPY", "expiration": "2024-02-16",
             "strike": 470.0, "option_type": "C"},
            {"observation_date": "2024-01-11", "label_date": "2024-01-12", "horizon": "1d",
             "forward_return": 0.04, "underlying_symbol": "SPY", "expiration": "2024-02-16",
             "strike": 470.0, "option_type": "C"},
            {"observation_date": "2024-01-12", "label_date": "2024-01-13", "horizon": "1d",
             "forward_return": 0.05, "underlying_symbol": "SPY", "expiration": "2024-02-16",
             "strike": 470.0, "option_type": "C"},
            {"observation_date": "2024-01-15", "label_date": "2024-01-16", "horizon": "1d",
             "forward_return": 0.06, "underlying_symbol": "SPY", "expiration": "2024-02-16",
             "strike": 470.0, "option_type": "C"},
        ]
        return pd.DataFrame(rows)

    def _options_multi(self) -> pd.DataFrame:
        obs_dates = ["2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09",
                     "2024-01-10", "2024-01-11", "2024-01-12", "2024-01-15"]
        return pd.DataFrame([
            {"date": d, "underlying_symbol": "SPY", "expiration": "2024-02-16",
             "strike": 470.0, "option_type": "C", "volume": 50, "open_interest": 200,
             "moneyness": 0.001, "relative_spread": 0.06, "bid": 1.5, "ask": 1.6, "mid": 1.55}
            for d in obs_dates
        ])

    # ---- _compute_realized_vol unit tests ----

    def test_realized_vol_returns_none_when_insufficient_history(self):
        # Only Jan 2-3 available before train_end; lookback=2 needs 2 returns, have 1
        daily = self._daily_vol_pattern()
        vol = _compute_realized_vol(daily, "2024-01-03", lookback_days=2)
        self.assertIsNone(vol)

    def test_realized_vol_returns_float_when_enough_history(self):
        daily = self._daily_vol_pattern()
        vol = _compute_realized_vol(daily, "2024-01-05", lookback_days=2)
        self.assertIsNotNone(vol)
        self.assertIsInstance(vol, float)
        self.assertGreater(vol, 0.0)

    def test_realized_vol_uses_only_dates_up_to_train_end(self):
        # Jan 2-5 in frame; train_end=Jan 3 -> can only use Jan 2-3 -> insufficient
        daily = self._daily_vol_pattern()
        vol_jan3 = _compute_realized_vol(daily, "2024-01-03", lookback_days=2)
        # Jan 8-9 are highly volatile; Jan 9 train_end with lookback=2 should yield high vol
        vol_jan9 = _compute_realized_vol(daily, "2024-01-09", lookback_days=2)
        # vol_jan3 is None (only 1 return in window); vol_jan9 is large
        self.assertIsNone(vol_jan3)
        self.assertIsNotNone(vol_jan9)
        # wf_003 vol should be clearly higher than wf_002 vol (stable Jan 4-5 vs volatile Jan 8-9)
        vol_jan5 = _compute_realized_vol(daily, "2024-01-05", lookback_days=2)
        self.assertGreater(vol_jan9, vol_jan5)

    def test_realized_vol_empty_frame_returns_none(self):
        self.assertIsNone(_compute_realized_vol(pd.DataFrame(), "2024-01-03", lookback_days=2))

    def test_realized_vol_no_close_or_return_column_returns_none(self):
        daily = pd.DataFrame([{"date": "2024-01-02"}, {"date": "2024-01-03"}])
        self.assertIsNone(_compute_realized_vol(daily, "2024-01-03", lookback_days=1))

    # ---- _assign_regime_buckets unit tests ----

    def test_bucket_assignment_all_none_gives_insufficient_history(self):
        buckets = _assign_regime_buckets([None, None, None], n_buckets=3)
        self.assertEqual(buckets, ["insufficient_history"] * 3)

    def test_bucket_assignment_three_distinct_vols(self):
        # Low=0.1, Mid=0.3, High=0.9; thresholds at 33rd and 67th pct
        vols = [0.1, 0.3, 0.9]
        buckets = _assign_regime_buckets(vols, n_buckets=3)
        self.assertEqual(buckets, ["low_vol", "mid_vol", "high_vol"])

    def test_bucket_assignment_deterministic(self):
        vols = [0.15, 0.35, 0.75, None]
        b1 = _assign_regime_buckets(vols, n_buckets=3)
        b2 = _assign_regime_buckets(vols, n_buckets=3)
        self.assertEqual(b1, b2)

    def test_bucket_assignment_two_buckets(self):
        vols = [0.1, 0.9]
        buckets = _assign_regime_buckets(vols, n_buckets=2)
        self.assertIn("low_vol", buckets)
        self.assertIn("high_vol", buckets)

    def test_bucket_assignment_mixed_none_and_valid(self):
        vols = [None, 0.1, 0.9]
        buckets = _assign_regime_buckets(vols, n_buckets=3)
        self.assertEqual(buckets[0], "insufficient_history")
        self.assertIn(buckets[1], ["low_vol", "mid_vol", "high_vol"])
        self.assertIn(buckets[2], ["low_vol", "mid_vol", "high_vol"])

    # ---- regime annotation in build_historical_walk_forward_report ----

    def test_regime_bucket_per_window_with_vol_pattern(self):
        """With lookback=2: wf_001 insufficient, wf_002 low, wf_003 high, wf_004 mid."""
        regime_config = WalkForwardRegimeConfig(
            signal="realized_vol_20d", n_buckets=3, lookback_days=2
        )
        rule_config = WalkForwardRuleConfig(min_open_interest=100, min_volume=1)
        report = build_historical_walk_forward_report(
            model_ready_daily_features=self._daily_vol_pattern(),
            label_candidates=self._labels_multi(),
            option_context_features=self._options_multi(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-15",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
            rule_config=rule_config,
            regime_config=regime_config,
        ).report

        self.assertTrue(report["regime_applied"])
        windows = report["windows"]
        self.assertEqual(windows[0]["regime"]["bucket"], "insufficient_history")
        # wf_002 vol is lower than wf_003 vol -> low_vol
        self.assertEqual(windows[1]["regime"]["bucket"], "low_vol")
        # wf_003 has highest vol -> high_vol
        self.assertEqual(windows[2]["regime"]["bucket"], "high_vol")
        # wf_004 vol is mid -> mid_vol
        self.assertEqual(windows[3]["regime"]["bucket"], "mid_vol")

    def test_by_regime_summary_in_cross_window(self):
        regime_config = WalkForwardRegimeConfig(
            signal="realized_vol_20d", n_buckets=3, lookback_days=2
        )
        rule_config = WalkForwardRuleConfig(min_open_interest=100, min_volume=1)
        report = build_historical_walk_forward_report(
            model_ready_daily_features=self._daily_vol_pattern(),
            label_candidates=self._labels_multi(),
            option_context_features=self._options_multi(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-15",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
            rule_config=rule_config,
            regime_config=regime_config,
        ).report

        cws = report["cross_window_summary"]
        self.assertIn("by_regime", cws)
        by_regime = cws["by_regime"]
        # Each bucket that has windows appears in by_regime
        for bucket in by_regime:
            rs = by_regime[bucket]
            self.assertIn("total_windows", rs)
            self.assertIn("evaluable_windows", rs)
            self.assertIn("total_selected_rows", rs)
            self.assertIn("by_horizon", rs)

    def test_regime_not_in_report_when_signal_is_none(self):
        report = build_historical_walk_forward_report(
            model_ready_daily_features=self._daily_vol_pattern(),
            label_candidates=self._labels_multi(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-15",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
        ).report

        self.assertFalse(report["regime_applied"])
        for w in report["windows"]:
            self.assertNotIn("regime", w)
        self.assertNotIn("by_regime", report.get("cross_window_summary", {}))

    def test_training_threshold_flags_remain_false_with_regime(self):
        regime_config = WalkForwardRegimeConfig(signal="realized_vol_20d", n_buckets=3, lookback_days=2)
        rule_config = WalkForwardRuleConfig(min_open_interest=1, min_volume=1)
        report = build_historical_walk_forward_report(
            model_ready_daily_features=self._daily_vol_pattern(),
            label_candidates=self._labels_multi(),
            option_context_features=self._options_multi(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-15",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
            rule_config=rule_config,
            regime_config=regime_config,
        ).report

        self.assertFalse(report["training_performed"])
        self.assertFalse(report["threshold_optimization_performed"])

    def test_leakage_checks_pass_with_regime(self):
        regime_config = WalkForwardRegimeConfig(signal="realized_vol_20d", n_buckets=3, lookback_days=2)
        report = build_historical_walk_forward_report(
            model_ready_daily_features=self._daily_vol_pattern(),
            label_candidates=self._labels_multi(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-15",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
            regime_config=regime_config,
        ).report

        self.assertEqual(report["leakage_checks"]["status"], "pass")

    def test_insufficient_history_when_lookback_exceeds_available_dates(self):
        """All windows get insufficient_history when lookback_days > available dates."""
        regime_config = WalkForwardRegimeConfig(
            signal="realized_vol_20d", n_buckets=3, lookback_days=50
        )
        report = build_historical_walk_forward_report(
            model_ready_daily_features=self._daily_vol_pattern(),
            label_candidates=self._labels_multi(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-15",
            horizons=["1d"],
            train_window=2,
            test_window=2,
            step=2,
            regime_config=regime_config,
        ).report

        for w in report["windows"]:
            self.assertEqual(w["regime"]["bucket"], "insufficient_history")
            self.assertIsNone(w["regime"]["train_end_realized_vol"])
