import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from services.external_data.ml_signal_diagnostics import (
    run_ml_signal_diagnostics_on_frame,
    write_ml_signal_diagnostics_report,
)
from services.external_data.model_ready_dataset_export import FEATURE_DATA_COLUMNS, LABEL_COLUMNS


class TestMLSignalDiagnostics(unittest.TestCase):
    def _dataset(self, rows: int = 50) -> pd.DataFrame:
        dates = pd.bdate_range("2023-01-03", periods=rows)
        data = {
            "symbol": ["SPY"] * rows,
            "entry_date": dates.strftime("%Y-%m-%d"),
        }
        for index, column in enumerate(FEATURE_DATA_COLUMNS):
            data[column] = [float(row + index + 1) for row in range(rows)]
        data["price_momentum_5d"] = [float(row) for row in range(rows)]
        data["forward_return_1d"] = [0.01 if row % 2 else -0.01 for row in range(rows)]
        data["forward_return_5d"] = [-0.02 if row < rows // 2 else 0.02 for row in range(rows)]
        data["forward_return_21d"] = [0.03 if row % 3 else -0.03 for row in range(rows)]
        data["forward_volatility_21d"] = [0.1 + row * 0.001 for row in range(rows)]
        return pd.DataFrame(data)

    def _metadata(self) -> dict:
        return {
            "status": "warn",
            "symbol": "SPY",
            "analysis_start_date": "2023-01-03",
            "analysis_end_date": "2023-03-14",
        }

    def _model_diagnostics(self) -> dict:
        return {
            "diagnostic_summary": {
                "model_underperformed_naive_accuracy": True,
                "predicted_mostly_one_class_window_count": 2,
                "coefficient_direction_stability": {
                    "price_momentum_5d": {
                        "stability_rate": 1.0,
                    },
                    "volume_ratio_10d": {
                        "stability_rate": 0.5,
                    },
                },
            },
            "walk_forward": {
                "windows": [
                    {
                        "window_id": 1,
                        "diagnostics": {
                            "probability_distribution": {
                                "p25": 0.45,
                                "p75": 0.55,
                            }
                        },
                    }
                ]
            },
        }

    def test_feature_bucket_computation_correctness(self):
        report = run_ml_signal_diagnostics_on_frame(
            dataset=self._dataset(50),
            dataset_metadata=self._metadata(),
            model_diagnostics=self._model_diagnostics(),
            bucket_count=5,
            stability_window_rows=25,
            rolling_window_rows=10,
        ).report

        buckets = report["feature_bucket_tables"]["price_momentum_5d"]
        self.assertEqual(len(buckets), 5)
        self.assertEqual([bucket["rows"] for bucket in buckets], [10, 10, 10, 10, 10])
        self.assertEqual(buckets[0]["p_target_positive"], 0.0)
        self.assertEqual(buckets[-1]["p_target_positive"], 1.0)
        price_row = next(row for row in report["feature_signal_table"] if row["feature"] == "price_momentum_5d")
        self.assertEqual(price_row["bucket_positive_rate_separation"], 1.0)

    def test_no_label_leakage(self):
        report = run_ml_signal_diagnostics_on_frame(
            dataset=self._dataset(30),
            dataset_metadata=self._metadata(),
            model_diagnostics=self._model_diagnostics(),
        ).report

        for label in LABEL_COLUMNS:
            self.assertNotIn(label, report["feature_columns"])
        self.assertTrue(report["leakage_checks"]["labels_excluded_from_features"])
        self.assertTrue(report["leakage_checks"]["no_training_performed"])
        self.assertTrue(report["leakage_checks"]["no_threshold_optimization"])

    def test_report_schema_stable(self):
        report = run_ml_signal_diagnostics_on_frame(
            dataset=self._dataset(40),
            dataset_metadata=self._metadata(),
            model_diagnostics=self._model_diagnostics(),
        ).report

        for key in [
            "target_distribution",
            "feature_signal_table",
            "feature_bucket_tables",
            "feature_stability_flags",
            "signal_strength_summary",
            "model_collapse_diagnosis",
            "explicit_warning",
        ]:
            self.assertIn(key, report)
        self.assertEqual(report["explicit_warning"], "no edge claim")
        self.assertIn("overall_signal_strength", report["signal_strength_summary"])
        self.assertIn("why_model_predicted_mostly_one_class", report["model_collapse_diagnosis"])

    def test_constant_features_are_handled(self):
        data = self._dataset(30)
        data["volume_ratio_10d"] = 1.0
        report = run_ml_signal_diagnostics_on_frame(
            dataset=data,
            dataset_metadata=self._metadata(),
            model_diagnostics=self._model_diagnostics(),
        ).report

        row = next(row for row in report["feature_signal_table"] if row["feature"] == "volume_ratio_10d")
        self.assertTrue(row["constant"])
        self.assertIsNone(row["pearson_corr_target_positive"])
        self.assertEqual(report["feature_bucket_tables"]["volume_ratio_10d"][0]["bucket"], "constant")

    def test_missing_values_are_reported(self):
        data = self._dataset(30)
        data.loc[:14, "price_momentum_20d"] = pd.NA
        report = run_ml_signal_diagnostics_on_frame(
            dataset=data,
            dataset_metadata=self._metadata(),
            model_diagnostics=self._model_diagnostics(),
        ).report

        row = next(row for row in report["feature_signal_table"] if row["feature"] == "price_momentum_20d")
        self.assertGreater(row["missing_rate"], 0.0)
        self.assertGreater(row["rows"], 0)

    def test_signal_diagnostics_can_write_report(self):
        result = run_ml_signal_diagnostics_on_frame(
            dataset=self._dataset(30),
            dataset_metadata=self._metadata(),
            model_diagnostics=self._model_diagnostics(),
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            with patch.object(Path, "cwd", return_value=root):
                path = write_ml_signal_diagnostics_report(result.report, reports_dir=root / "reports")
            self.assertTrue(path.exists())
            self.assertEqual(path.suffix, ".json")


if __name__ == "__main__":
    unittest.main()
