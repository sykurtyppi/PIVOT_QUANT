import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from services.external_data.ml_training_smoke import (
    run_ml_walk_forward_smoke_on_frame,
    run_ml_training_smoke_on_frame,
    write_ml_training_smoke_report,
)
from services.external_data.model_ready_dataset_export import FEATURE_DATA_COLUMNS, LABEL_COLUMNS


class TestMLTrainingSmoke(unittest.TestCase):
    def _dataset(self, rows: int = 20) -> pd.DataFrame:
        dates = pd.bdate_range("2024-01-02", periods=rows)
        data = {
            "symbol": ["SPY"] * rows,
            "entry_date": dates.strftime("%Y-%m-%d"),
        }
        for index, column in enumerate(FEATURE_DATA_COLUMNS):
            data[column] = [float(i + index + 1) for i in range(rows)]
        data["forward_return_1d"] = [0.01 if i % 2 else -0.01 for i in range(rows)]
        data["forward_return_5d"] = [0.02 if i % 3 else -0.02 for i in range(rows)]
        data["forward_return_21d"] = [0.03 if i % 4 else -0.03 for i in range(rows)]
        data["forward_volatility_21d"] = [0.12 + (i * 0.001) for i in range(rows)]
        data["missing_required_label_count"] = [0] * rows
        return pd.DataFrame(data)

    def _metadata(self) -> dict:
        return {
            "status": "warn",
            "symbol": "SPY",
            "analysis_start_date": "2024-01-02",
            "analysis_end_date": "2024-03-29",
            "schema_source": "test",
        }

    def test_chronological_split_enforced(self):
        result = run_ml_training_smoke_on_frame(
            dataset=self._dataset(20),
            dataset_metadata=self._metadata(),
            train_fraction=0.7,
        ).report

        self.assertEqual(result["split"]["method"], "chronological")
        self.assertFalse(result["split"]["shuffled"])
        self.assertTrue(result["split"]["test_strictly_after_train"])
        self.assertLess(result["split"]["train_end"], result["split"]["test_start"])

    def test_labels_excluded_from_features(self):
        result = run_ml_training_smoke_on_frame(
            dataset=self._dataset(20),
            dataset_metadata=self._metadata(),
        ).report

        for label in LABEL_COLUMNS:
            self.assertNotIn(label, result["feature_columns"])
        self.assertTrue(result["leakage_checks"]["labels_excluded_from_features"])

    def test_tiny_sample_warning_appears(self):
        result = run_ml_training_smoke_on_frame(
            dataset=self._dataset(20),
            dataset_metadata=self._metadata(),
        ).report

        self.assertEqual(result["status"], "warn")
        self.assertTrue(any("tiny sample warning" in warning for warning in result["warnings"]))

    def test_report_schema_stable(self):
        result = run_ml_training_smoke_on_frame(
            dataset=self._dataset(20),
            dataset_metadata=self._metadata(),
        ).report

        for key in [
            "name",
            "status",
            "target_name",
            "feature_columns",
            "label_columns",
            "split",
            "class_balance",
            "metrics",
            "naive_baseline",
            "leakage_checks",
            "dataset",
            "warnings",
        ]:
            self.assertIn(key, result)

    def test_model_smoke_can_write_report_on_synthetic_data(self):
        result = run_ml_training_smoke_on_frame(
            dataset=self._dataset(20),
            dataset_metadata=self._metadata(),
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            with patch.object(Path, "cwd", return_value=root):
                path = write_ml_training_smoke_report(result.report, reports_dir=root / "reports")
            self.assertTrue(path.exists())
            self.assertEqual(path.suffix, ".json")

    def test_single_split_and_walk_forward_report_paths_do_not_collide(self):
        single_split = run_ml_training_smoke_on_frame(
            dataset=self._dataset(40),
            dataset_metadata=self._metadata(),
        )
        walk_forward = run_ml_walk_forward_smoke_on_frame(
            dataset=self._dataset(40),
            dataset_metadata=self._metadata(),
            train_window_rows=15,
            test_window_rows=5,
            step_rows=5,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            with patch.object(Path, "cwd", return_value=root):
                single_path = write_ml_training_smoke_report(single_split.report, reports_dir=root / "reports")
                walk_forward_path = write_ml_training_smoke_report(walk_forward.report, reports_dir=root / "reports")

            self.assertTrue(single_path.exists())
            self.assertTrue(walk_forward_path.exists())
            self.assertNotEqual(single_path.name, walk_forward_path.name)

    def test_walk_forward_uses_chronological_non_shuffled_windows(self):
        result = run_ml_walk_forward_smoke_on_frame(
            dataset=self._dataset(40),
            dataset_metadata=self._metadata(),
            train_window_rows=15,
            test_window_rows=5,
            step_rows=5,
        ).report

        self.assertEqual(result["walk_forward"]["method"], "chronological_rolling_windows")
        self.assertTrue(result["leakage_checks"]["no_shuffled_splits"])
        for window in result["walk_forward"]["windows"]:
            self.assertFalse(window["leakage_checks"]["shuffled"])
            self.assertTrue(window["leakage_checks"]["test_strictly_after_train"])

    def test_walk_forward_excludes_labels_from_features(self):
        result = run_ml_walk_forward_smoke_on_frame(
            dataset=self._dataset(40),
            dataset_metadata=self._metadata(),
            train_window_rows=15,
            test_window_rows=5,
            step_rows=5,
        ).report

        for label in LABEL_COLUMNS:
            self.assertNotIn(label, result["feature_columns"])
        self.assertTrue(result["leakage_checks"]["labels_excluded_from_features"])

    def test_walk_forward_handles_non_evaluable_windows(self):
        data = self._dataset(20)
        data["forward_return_5d"] = 0.01
        result = run_ml_walk_forward_smoke_on_frame(
            dataset=data,
            dataset_metadata=self._metadata(),
            train_window_rows=10,
            test_window_rows=5,
            step_rows=5,
        ).report

        self.assertEqual(result["status"], "fail")
        self.assertGreater(result["walk_forward"]["non_evaluable_window_count"], 0)
        self.assertEqual(result["walk_forward"]["evaluable_window_count"], 0)

    def test_walk_forward_report_schema_stable(self):
        result = run_ml_walk_forward_smoke_on_frame(
            dataset=self._dataset(40),
            dataset_metadata=self._metadata(),
            train_window_rows=15,
            test_window_rows=5,
            step_rows=5,
        ).report

        for key in [
            "name",
            "status",
            "target_name",
            "feature_columns",
            "walk_forward",
            "aggregate",
            "leakage_checks",
            "dataset",
            "warnings",
        ]:
            self.assertIn(key, result)
        self.assertIn("average_metrics", result["aggregate"])
        self.assertIn("average_naive_baseline_metrics", result["aggregate"])

    def test_walk_forward_diagnostic_report_schema_stable(self):
        result = run_ml_walk_forward_smoke_on_frame(
            dataset=self._dataset(40),
            dataset_metadata=self._metadata(),
            train_window_rows=15,
            test_window_rows=5,
            step_rows=5,
        ).report

        self.assertIn("diagnostic_summary", result)
        for key in [
            "why_model_underperformed",
            "model_underperformed_naive_accuracy",
            "predicted_mostly_one_class_window_count",
            "class_imbalance_dominated",
            "low_variance_feature_count",
            "coefficient_direction_stability",
        ]:
            self.assertIn(key, result["diagnostic_summary"])

        first_window = result["walk_forward"]["windows"][0]
        self.assertIn("confusion_matrix", first_window)
        self.assertIn("diagnostics", first_window)
        diagnostics = first_window["diagnostics"]
        for key in [
            "class_imbalance",
            "prediction_distribution",
            "probability_distribution",
            "feature_null_rates",
            "feature_variance",
            "coefficients",
            "naive_baseline_class_choice",
            "confusion_matrix",
            "model_vs_naive",
        ]:
            self.assertIn(key, diagnostics)
        self.assertIn("class_choice", first_window["naive_baseline"])
        self.assertIn("by_feature", diagnostics["coefficients"])
        self.assertIn("top_positive", diagnostics["coefficients"])
        self.assertIn("top_negative", diagnostics["coefficients"])


if __name__ == "__main__":
    unittest.main()
