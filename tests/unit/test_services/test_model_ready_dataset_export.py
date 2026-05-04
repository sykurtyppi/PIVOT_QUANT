import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from services.external_data.model_ready_dataset_export import (
    EXPORT_COLUMNS,
    FEATURE_DATA_COLUMNS,
    LABEL_COLUMNS,
    LABEL_DATE_COLUMNS,
    build_model_ready_dataset_export,
    write_model_ready_dataset_export,
)


class TestModelReadyDatasetExport(unittest.TestCase):
    def _features(self) -> pd.DataFrame:
        return pd.DataFrame([self._feature_row("2024-01-02")])

    def _feature_row(self, day: str, *, iv_rank=0.5) -> dict:
        return {
            "symbol": "SPY",
            "date": day,
            "underlying_price": 471.0,
            "price_momentum_5d": 0.01,
            "price_momentum_20d": 0.02,
            "volume_ratio_10d": 1.1,
            "iv_rank": iv_rank,
            "iv_percentile": 50.0,
            "iv30_rv30_ratio": 1.2,
            "vol_term_structure_slope": 0.1,
            "rsi_14": 55.0,
            "bb_position": 0.6,
            "vix_level": 15.0,
            "volume": 1_000_000,
            "realized_vol_30d": 0.18,
            "realized_vol_60d": 0.2,
        }

    def _labels(self, horizons=None, observation_date="2024-01-02") -> pd.DataFrame:
        horizons = horizons or ["1d", "5d", "21d"]
        return pd.DataFrame(
            [
                {
                    "observation_date": observation_date,
                    "label_date": "2024-01-03",
                    "horizon": horizon,
                    "forward_return": 0.01,
                }
                for horizon in horizons
            ]
        )

    def _compatibility_report(self, *, status="pass") -> dict:
        return {
            "status": status,
            "schema_source": "test_schema",
            "warnings": [],
            "no_lookahead_notes": {"price_momentum_5d": "test note"},
        }

    def test_stable_feature_column_order_and_label_separation(self):
        export = build_model_ready_dataset_export(
            compatibility_rows=self._features(),
            compatibility_report=self._compatibility_report(),
            label_candidates=self._labels(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-30",
            requested_horizons=["1d", "5d", "21d"],
        )

        self.assertEqual(list(export.dataset.columns), EXPORT_COLUMNS + LABEL_DATE_COLUMNS + ["missing_required_label_count"])
        self.assertEqual(list(export.dataset.columns[:2]), ["symbol", "entry_date"])
        for label in LABEL_COLUMNS:
            self.assertIn(label, export.metadata["label_columns"])
            self.assertNotIn(label, export.metadata["feature_columns"])

    def test_missing_required_feature_rows_are_dropped_by_default(self):
        features = self._features()
        features.loc[0, "iv_rank"] = pd.NA
        export = build_model_ready_dataset_export(
            compatibility_rows=features,
            compatibility_report=self._compatibility_report(status="warn"),
            label_candidates=self._labels(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-30",
            requested_horizons=["1d", "5d", "21d"],
        )

        self.assertEqual(len(export.dataset), 0)
        self.assertEqual(export.metadata["drop_reasons"]["missing_required_features"], 1)
        self.assertEqual(export.metadata["status"], "fail")

    def test_missing_required_feature_rows_can_be_flagged(self):
        features = self._features()
        features.loc[0, "iv_rank"] = pd.NA
        export = build_model_ready_dataset_export(
            compatibility_rows=features,
            compatibility_report=self._compatibility_report(status="warn"),
            label_candidates=self._labels(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-31",
            requested_horizons=["1d", "5d", "21d"],
            missing_feature_policy="flag",
        )

        self.assertEqual(len(export.dataset), 1)
        self.assertIn("missing_required_feature_count", export.dataset.columns)
        self.assertEqual(int(export.dataset.loc[0, "missing_required_feature_count"]), 1)

    def test_metadata_sidecar_schema_and_csv_fallback(self):
        export = build_model_ready_dataset_export(
            compatibility_rows=self._features(),
            compatibility_report=self._compatibility_report(),
            label_candidates=self._labels(["1d"]),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-31",
            requested_horizons=["1d", "5d", "21d"],
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            with patch.object(Path, "cwd", return_value=root):
                paths = write_model_ready_dataset_export(export, reports_dir=root / "reports")
            self.assertTrue(paths["dataset"].exists())
            self.assertTrue(paths["metadata"].exists())
            self.assertEqual(paths["dataset"].suffix, ".csv")
            self.assertEqual(export.metadata["export_format"], "csv")
            for key in ["rows", "drop_reasons", "feature_columns", "label_columns", "null_rates"]:
                self.assertIn(key, export.metadata)
            self.assertIn("fully_labeled_row_count", export.metadata)

    def test_no_future_or_leaky_columns_included(self):
        features = self._features()
        features["forward_return_1d"] = 0.02

        with self.assertRaises(ValueError):
            build_model_ready_dataset_export(
                compatibility_rows=features,
                compatibility_report=self._compatibility_report(),
                label_candidates=self._labels(),
                symbol="SPY",
                start_date="2024-01-02",
                end_date="2024-01-31",
                requested_horizons=["1d", "5d", "21d"],
            )

    def test_feature_data_columns_are_exported_without_extra_columns(self):
        export = build_model_ready_dataset_export(
            compatibility_rows=self._features(),
            compatibility_report=self._compatibility_report(),
            label_candidates=self._labels(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-31",
            requested_horizons=["1d", "5d", "21d"],
        )

        for column in FEATURE_DATA_COLUMNS:
            self.assertIn(column, export.dataset.columns)
        self.assertNotIn("date", export.dataset.columns)
        self.assertNotIn("future_underlying_close", export.dataset.columns)

    def test_exported_rows_are_restricted_to_analysis_window(self):
        features = pd.DataFrame(
            [
                self._feature_row("2023-12-29"),
                self._feature_row("2024-01-02"),
                self._feature_row("2024-01-03"),
                self._feature_row("2024-02-01"),
            ]
        )
        labels = pd.concat(
            [
                self._labels(observation_date="2024-01-02"),
                self._labels(observation_date="2024-01-03"),
                self._labels(observation_date="2024-02-01"),
            ],
            ignore_index=True,
        )

        export = build_model_ready_dataset_export(
            compatibility_rows=features,
            compatibility_report=self._compatibility_report(),
            label_candidates=labels,
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-30",
            read_start_date="2023-10-04",
            read_end_date="2024-03-16",
            feature_lookback_days=90,
            label_lookahead_days=45,
            requested_horizons=["1d", "5d", "21d"],
        )

        self.assertEqual(export.dataset["entry_date"].tolist(), ["2024-01-02", "2024-01-03"])
        self.assertTrue(export.metadata["leakage_checks"]["exported_rows_inside_analysis_window"])
        self.assertEqual(export.metadata["windows"]["read_start_date"], "2023-10-04")
        self.assertEqual(export.metadata["windows"]["analysis_start_date"], "2024-01-02")

    def test_label_lookahead_can_supply_future_labels_without_exporting_future_rows(self):
        dates = pd.bdate_range("2024-01-30", periods=23)
        features = pd.DataFrame(
            [
                {**self._feature_row(day.strftime("%Y-%m-%d")), "underlying_price": 100.0 + index}
                for index, day in enumerate(dates)
            ]
        )
        labels = pd.DataFrame(
            [
                {
                    "observation_date": "2024-01-30",
                    "label_date": "2024-02-28",
                    "horizon": "21d",
                    "forward_return": 0.03,
                }
            ]
        )

        export = build_model_ready_dataset_export(
            compatibility_rows=features,
            compatibility_report=self._compatibility_report(),
            label_candidates=labels,
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-30",
            read_start_date="2023-10-04",
            read_end_date="2024-03-16",
            feature_lookback_days=90,
            label_lookahead_days=45,
            requested_horizons=["1d", "5d", "21d"],
        )

        self.assertEqual(export.dataset["entry_date"].tolist(), ["2024-01-30"])
        self.assertAlmostEqual(float(export.dataset.loc[0, "forward_return_21d"]), 0.03)
        self.assertTrue(pd.notna(export.dataset.loc[0, "forward_volatility_21d"]))
        self.assertNotIn(dates[-1].strftime("%Y-%m-%d"), export.dataset["entry_date"].tolist())
        self.assertTrue(export.metadata["leakage_checks"]["future_label_columns_excluded_from_features"])

    def test_metadata_records_window_semantics(self):
        export = build_model_ready_dataset_export(
            compatibility_rows=self._features(),
            compatibility_report=self._compatibility_report(),
            label_candidates=self._labels(),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-31",
            read_start_date="2023-10-04",
            read_end_date="2024-03-16",
            feature_lookback_days=90,
            label_lookahead_days=45,
            requested_horizons=["1d", "5d", "21d"],
        )

        self.assertEqual(export.metadata["read_start_date"], "2023-10-04")
        self.assertEqual(export.metadata["analysis_end_date"], "2024-01-31")
        self.assertEqual(export.metadata["config"]["feature_lookback_days"], 90)
        self.assertEqual(export.metadata["config"]["label_lookahead_days"], 45)
        self.assertTrue(export.metadata["leakage_checks"]["read_window_contains_feature_lookback"])

    def test_analysis_rows_not_dropped_when_lookback_rows_are_outside_analysis_window(self):
        features = pd.DataFrame(
            [
                self._feature_row("2023-11-01", iv_rank=pd.NA),
                self._feature_row("2023-12-01", iv_rank=pd.NA),
                self._feature_row("2024-01-02", iv_rank=0.7),
            ]
        )

        export = build_model_ready_dataset_export(
            compatibility_rows=features,
            compatibility_report=self._compatibility_report(status="warn"),
            label_candidates=self._labels(observation_date="2024-01-02"),
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-01-31",
            read_start_date="2023-10-04",
            read_end_date="2024-03-16",
            feature_lookback_days=90,
            label_lookahead_days=45,
            requested_horizons=["1d", "5d", "21d"],
        )

        self.assertEqual(export.metadata["rows"]["read_input"], 3)
        self.assertEqual(export.metadata["rows"]["input"], 1)
        self.assertEqual(export.metadata["rows"]["exported"], 1)
        self.assertEqual(export.metadata["drop_reasons"]["missing_required_features"], 0)

    def test_multimonth_metadata_contains_month_summary_and_stable_schema(self):
        features = pd.DataFrame(
            [
                self._feature_row("2024-01-02"),
                self._feature_row("2024-01-31"),
                self._feature_row("2024-02-01"),
                self._feature_row("2024-02-29"),
                self._feature_row("2024-03-01"),
            ]
        )
        labels = pd.concat(
            [
                self._labels(observation_date="2024-01-02"),
                self._labels(observation_date="2024-01-31"),
                self._labels(observation_date="2024-02-01"),
                self._labels(observation_date="2024-02-29"),
                self._labels(observation_date="2024-03-01"),
            ],
            ignore_index=True,
        )

        export = build_model_ready_dataset_export(
            compatibility_rows=features,
            compatibility_report=self._compatibility_report(),
            label_candidates=labels,
            symbol="SPY",
            start_date="2024-01-02",
            end_date="2024-03-29",
            read_start_date="2023-09-04",
            read_end_date="2024-05-13",
            feature_lookback_days=120,
            label_lookahead_days=45,
            requested_horizons=["1d", "5d", "21d"],
        )

        self.assertEqual(export.metadata["monthly_summary"]["2024-01"]["analysis_rows"], 2)
        self.assertEqual(export.metadata["monthly_summary"]["2024-02"]["analysis_rows"], 2)
        self.assertEqual(export.metadata["monthly_summary"]["2024-03"]["analysis_rows"], 1)
        self.assertTrue(export.metadata["schema_stability_checks"]["column_order_matches_expected"])
        self.assertTrue(export.metadata["schema_stability_checks"]["label_columns_stable"])

    def test_month_boundary_rows_do_not_create_leakage_failures(self):
        features = pd.DataFrame(
            [
                self._feature_row("2024-01-31"),
                self._feature_row("2024-02-01"),
            ]
        )
        labels = pd.concat(
            [
                self._labels(observation_date="2024-01-31"),
                self._labels(observation_date="2024-02-01"),
            ],
            ignore_index=True,
        )

        export = build_model_ready_dataset_export(
            compatibility_rows=features,
            compatibility_report=self._compatibility_report(),
            label_candidates=labels,
            symbol="SPY",
            start_date="2024-01-31",
            end_date="2024-02-01",
            read_start_date="2023-09-04",
            read_end_date="2024-03-16",
            feature_lookback_days=120,
            label_lookahead_days=45,
            requested_horizons=["1d", "5d", "21d"],
        )

        self.assertEqual(export.dataset["entry_date"].tolist(), ["2024-01-31", "2024-02-01"])
        self.assertTrue(export.metadata["leakage_checks"]["exported_rows_inside_analysis_window"])
        self.assertEqual(export.metadata["monthly_summary"]["2024-01"]["exported_rows"], 1)
        self.assertEqual(export.metadata["monthly_summary"]["2024-02"]["exported_rows"], 1)

    def test_one_year_metadata_contains_stable_monthly_summary(self):
        month_starts = [pd.Timestamp("2023-01-03"), *pd.date_range("2023-02-01", "2023-12-01", freq="MS")]
        features = pd.DataFrame(
            [
                self._feature_row(day.strftime("%Y-%m-%d"))
                for day in month_starts
            ]
        )
        labels = pd.concat(
            [
                self._labels(observation_date=day.strftime("%Y-%m-%d"))
                for day in month_starts
            ],
            ignore_index=True,
        )

        export = build_model_ready_dataset_export(
            compatibility_rows=features,
            compatibility_report=self._compatibility_report(),
            label_candidates=labels,
            symbol="SPY",
            start_date="2023-01-03",
            end_date="2023-12-29",
            read_start_date="2022-09-05",
            read_end_date="2024-02-12",
            feature_lookback_days=120,
            label_lookahead_days=45,
            requested_horizons=["1d", "5d", "21d"],
        )

        self.assertEqual(export.metadata["windows"]["analysis_start_date"], "2023-01-03")
        self.assertEqual(export.metadata["windows"]["analysis_end_date"], "2023-12-29")
        self.assertEqual(export.metadata["config"]["feature_lookback_days"], 120)
        self.assertEqual(export.metadata["config"]["label_lookahead_days"], 45)
        self.assertEqual(len(export.metadata["monthly_summary"]), 12)
        self.assertIn("2023-01", export.metadata["monthly_summary"])
        self.assertIn("2023-12", export.metadata["monthly_summary"])
        self.assertTrue(export.metadata["schema_stability_checks"]["column_order_matches_expected"])
        self.assertTrue(export.metadata["leakage_checks"]["read_window_contains_feature_lookback"])
        self.assertTrue(export.metadata["leakage_checks"]["read_window_contains_label_lookahead"])


if __name__ == "__main__":
    unittest.main()
