import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from services.external_data.ml_regime_target_diagnostics import (
    run_ml_regime_target_diagnostics_on_frame,
    write_ml_regime_target_diagnostics_report,
)
from services.external_data.model_ready_dataset_export import FEATURE_DATA_COLUMNS


class TestMLRegimeTargetDiagnostics(unittest.TestCase):
    def _dataset(self, rows: int = 80) -> pd.DataFrame:
        dates = pd.bdate_range("2023-01-03", periods=rows)
        data = {
            "symbol": ["SPY"] * rows,
            "entry_date": dates.strftime("%Y-%m-%d"),
        }
        for index, column in enumerate(FEATURE_DATA_COLUMNS):
            data[column] = [float(row + index + 1) for row in range(rows)]
        data["realized_vol_60d"] = [0.10 if row < rows // 2 else 0.30 for row in range(rows)]
        data["vix_level"] = [15.0 if row < rows // 2 else 25.0 for row in range(rows)]
        data["price_momentum_20d"] = [-0.02 if row < rows // 2 else 0.02 for row in range(rows)]
        data["iv30_rv30_ratio"] = [0.8 if row < rows // 2 else 1.4 for row in range(rows)]
        data["vol_term_structure_slope"] = [-0.05 if row < rows // 2 else 0.05 for row in range(rows)]
        data["forward_return_1d"] = [0.01 if row % 2 else -0.01 for row in range(rows)]
        data["forward_return_5d"] = [-0.02 if row < rows // 2 else 0.02 for row in range(rows)]
        data["forward_return_21d"] = [0.03 if row >= rows // 3 else -0.03 for row in range(rows)]
        data["forward_volatility_21d"] = [0.12 + row * 0.001 for row in range(rows)]
        return pd.DataFrame(data)

    def _metadata(self) -> dict:
        return {
            "status": "warn",
            "symbol": "SPY",
            "analysis_start_date": "2023-01-03",
            "analysis_end_date": "2023-04-24",
        }

    def test_regime_bucket_computation(self):
        report = run_ml_regime_target_diagnostics_on_frame(
            dataset=self._dataset(),
            dataset_metadata=self._metadata(),
            stability_window_rows=20,
            rolling_window_rows=20,
        ).report

        regimes = {
            (row["regime_feature"], row["regime"]): row
            for row in report["regime_table"]
        }
        self.assertIn(("realized_vol_60d", "high"), regimes)
        self.assertIn(("realized_vol_60d", "low"), regimes)
        self.assertEqual(regimes[("realized_vol_60d", "high")]["target_positive_rate"], 1.0)
        self.assertEqual(regimes[("realized_vol_60d", "low")]["target_positive_rate"], 0.0)
        self.assertIn("feature_correlation_stability", regimes[("realized_vol_60d", "high")])
        self.assertIn("bucket_separation_stability", regimes[("realized_vol_60d", "high")])

    def test_target_comparison_schema(self):
        report = run_ml_regime_target_diagnostics_on_frame(
            dataset=self._dataset(),
            dataset_metadata=self._metadata(),
        ).report

        targets = {row["target"]: row for row in report["target_comparison_table"]}
        for target in [
            "forward_return_1d_positive",
            "forward_return_5d_positive",
            "forward_return_21d_positive",
            "forward_return_5d_abs_move",
            "forward_volatility_21d",
        ]:
            self.assertIn(target, targets)
            self.assertIn("feature_correlation_stability", targets[target])
            self.assertIn("bucket_separation_stability", targets[target])
            self.assertIn("rolling_metric", targets[target])
        self.assertIn("recommendation", report)
        self.assertIn(report["recommendation"]["action"], ["keep_target", "change_target", "needs_more_data"])

    def test_overlap_warning_is_explicit(self):
        report = run_ml_regime_target_diagnostics_on_frame(
            dataset=self._dataset(),
            dataset_metadata=self._metadata(),
        ).report

        self.assertTrue(report["overlap_warning"]["forward_return_5d"])
        self.assertTrue(report["overlap_warning"]["forward_return_21d"])
        self.assertTrue(any("5d and 21d forward labels overlap" in warning for warning in report["warnings"]))
        targets = {row["target"]: row for row in report["target_comparison_table"]}
        self.assertTrue(targets["forward_return_5d_positive"]["overlap_warning"])
        self.assertTrue(targets["forward_return_21d_positive"]["overlap_warning"])
        self.assertFalse(targets["forward_return_1d_positive"]["overlap_warning"])

    def test_missing_regime_feature_handling(self):
        data = self._dataset().drop(columns=["vix_level"])
        report = run_ml_regime_target_diagnostics_on_frame(
            dataset=data,
            dataset_metadata=self._metadata(),
        ).report

        self.assertIn("vix_level", report["missing_regime_features"])
        self.assertTrue(any("missing regime feature" in warning for warning in report["warnings"]))

    def test_regime_target_diagnostics_can_write_report(self):
        result = run_ml_regime_target_diagnostics_on_frame(
            dataset=self._dataset(),
            dataset_metadata=self._metadata(),
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            with patch.object(Path, "cwd", return_value=root):
                path = write_ml_regime_target_diagnostics_report(result.report, reports_dir=root / "reports")
            self.assertTrue(path.exists())
            self.assertEqual(path.suffix, ".json")


if __name__ == "__main__":
    unittest.main()
