import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from services.external_data.ml_regime_benchmark import (
    build_regime_benchmark_report,
    write_ml_regime_benchmark_report,
)


class TestMLRegimeBenchmark(unittest.TestCase):
    def _frame(self, rows: int = 40) -> pd.DataFrame:
        dates = pd.bdate_range("2023-01-03", periods=rows)
        midpoint = rows // 2
        return pd.DataFrame(
            {
                "symbol": ["SPY"] * rows,
                "entry_date": dates.strftime("%Y-%m-%d"),
                "realized_vol_60d": [0.1 if index < midpoint else 0.3 for index in range(rows)],
                "forward_return_1d": [-0.01 if index % 2 else 0.01 for index in range(rows)],
                "forward_return_5d": [-0.02 if index < midpoint else 0.02 for index in range(rows)],
                "forward_return_21d": [-0.03 if index < midpoint else 0.03 for index in range(rows)],
                "forward_volatility_21d": [0.12 + index * 0.001 for index in range(rows)],
            }
        )

    def _year_frame(self, year: str, frame: pd.DataFrame | None = None) -> dict:
        return {
            "year": year,
            "status": "ok",
            "dataset_path": f"/tmp/{year}.csv",
            "metadata_path": f"/tmp/{year}.metadata.json",
            "metadata": {
                "analysis_start_date": f"{year}-01-03",
                "analysis_end_date": f"{year}-12-29",
            },
            "frame": frame if frame is not None else self._frame(),
        }

    def test_high_low_regime_split(self):
        report = build_regime_benchmark_report(
            symbol="SPY",
            year_frames=[self._year_frame("2023")],
            small_sample_threshold=1,
        ).report

        year = report["year_reports"][0]
        self.assertEqual(year["benchmarks"]["realized_vol_60d_high"]["rows"], 20)
        self.assertEqual(year["benchmarks"]["realized_vol_60d_low"]["rows"], 20)
        self.assertEqual(year["benchmarks"]["realized_vol_60d_high"]["positive_rate"], 1.0)
        self.assertEqual(year["benchmarks"]["realized_vol_60d_low"]["positive_rate"], 0.0)

    def test_benchmark_comparison_math(self):
        report = build_regime_benchmark_report(
            symbol="SPY",
            year_frames=[self._year_frame("2023")],
            small_sample_threshold=1,
        ).report
        comparisons = report["year_reports"][0]["comparisons_to_all_rows"]

        self.assertEqual(comparisons["realized_vol_60d_high"]["positive_rate_delta"], 0.5)
        self.assertEqual(comparisons["realized_vol_60d_low"]["positive_rate_delta"], -0.5)
        self.assertAlmostEqual(comparisons["realized_vol_60d_high"]["mean_forward_return_5d_delta"], 0.02)
        self.assertAlmostEqual(comparisons["realized_vol_60d_low"]["mean_forward_return_5d_delta"], -0.02)

    def test_missing_realized_vol_handling(self):
        frame = self._frame().drop(columns=["realized_vol_60d"])
        report = build_regime_benchmark_report(
            symbol="SPY",
            year_frames=[self._year_frame("2023", frame=frame)],
        ).report

        self.assertEqual(report["year_reports"][0]["status"], "missing_regime_feature")
        self.assertIn("realized_vol_60d", report["year_reports"][0]["missing_columns"])
        self.assertTrue(any("missing required benchmark column" in warning for warning in report["warnings"]))

    def test_no_edge_claim_language_and_write_report(self):
        result = build_regime_benchmark_report(
            symbol="SPY",
            year_frames=[self._year_frame("2023"), self._year_frame("2024")],
            small_sample_threshold=1,
        )

        self.assertFalse(result.report["performance_claim"])
        self.assertFalse(result.report["training_performed"])
        self.assertFalse(result.report["threshold_optimization_performed"])
        self.assertEqual(result.report["explicit_warning"], "no edge claim")
        self.assertTrue(any("no model training" in warning for warning in result.report["warnings"]))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            with patch.object(Path, "cwd", return_value=root):
                path = write_ml_regime_benchmark_report(result.report, reports_dir=root / "reports")
            self.assertTrue(path.exists())
            self.assertEqual(path.suffix, ".json")


if __name__ == "__main__":
    unittest.main()
