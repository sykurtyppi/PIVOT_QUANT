import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from services.external_data.ml_multiyear_diagnostics import (
    build_multiyear_diagnostics_report,
    write_ml_multiyear_diagnostics_report,
)


class TestMLMultiyearDiagnostics(unittest.TestCase):
    def _year_report(self, year: str, *, vol_diff: float = 0.20, feature_sign: str = "positive") -> dict:
        return {
            "year": year,
            "status": "ok",
            "analysis_start_date": f"{year}-01-02",
            "analysis_end_date": f"{year}-12-31",
            "actual_start_date": f"{year}-01-02",
            "actual_end_date": f"{year}-12-29",
            "export": {
                "status": "warn",
                "rows": {"read_input": 300, "input": 250, "exported": 250, "dropped": 0},
                "fully_labeled_row_count": 250,
                "drop_reasons": {"missing_required_features": 0, "missing_required_labels": 0},
            },
            "target_positive_rates": {
                "forward_return_1d_positive": 0.52,
                "forward_return_5d_positive": 0.60,
                "forward_return_21d_positive": 0.65,
            },
            "regime_segment_differences": [
                {
                    "regime_feature": "realized_vol_60d",
                    "left_regime": "high",
                    "right_regime": "low",
                    "positive_rate_difference": vol_diff,
                    "mean_forward_return_5d_difference": 0.01,
                }
            ],
            "strongest_descriptive_features": {
                "forward_return_5d_positive": ["realized_vol_60d"],
                "forward_return_21d_positive": ["realized_vol_60d", "iv30_rv30_ratio"],
            },
            "feature_signs": {
                "realized_vol_60d": feature_sign,
                "iv30_rv30_ratio": "negative",
            },
            "target_comparison_results": [
                self._target_result("forward_return_5d_positive", stable_bucket=4, unstable_bucket=5, overlap=True),
                self._target_result("forward_return_21d_positive", stable_bucket=7, unstable_bucket=2, overlap=True),
            ],
            "per_year_recommendation": {"action": "needs_more_data"},
            "regime_sensitive_segment_count": 1,
            "warnings": ["diagnostics only"],
        }

    def _target_result(self, target: str, *, stable_bucket: int, unstable_bucket: int, overlap: bool) -> dict:
        return {
            "target": target,
            "status": "ok",
            "overlap_warning": overlap,
            "feature_correlation_stability": {
                "stable_feature_count": stable_bucket,
                "unstable_feature_count": unstable_bucket,
            },
            "bucket_separation_stability": {
                "stable_feature_count": stable_bucket,
                "unstable_feature_count": unstable_bucket,
            },
        }

    def test_multiyear_report_schema(self):
        report = build_multiyear_diagnostics_report(
            symbol="SPY",
            year_reports=[self._year_report("2023"), self._year_report("2024")],
            config={"max_files": 120},
        ).report

        for key in [
            "year_reports",
            "cross_year_stability",
            "final_recommendation",
            "warnings",
            "explicit_warning",
        ]:
            self.assertIn(key, report)
        self.assertEqual(report["successful_year_count"], 2)
        self.assertEqual(report["explicit_warning"], "no edge claim")
        self.assertIn(report["final_recommendation"]["action"], ["keep_5d_target", "consider_21d_target", "use_regime_conditioned_targets", "needs_more_data"])

    def test_missing_year_handling(self):
        missing = {
            "year": "2025",
            "status": "missing",
            "analysis_start_date": "2025-01-02",
            "analysis_end_date": "2025-12-31",
            "reason": "no rows",
        }
        report = build_multiyear_diagnostics_report(
            symbol="SPY",
            year_reports=[self._year_report("2023"), missing],
        ).report

        self.assertEqual(report["successful_year_count"], 1)
        self.assertEqual(report["missing_years"], ["2025"])
        self.assertTrue(any("missing or non-evaluable year" in warning for warning in report["warnings"]))
        self.assertEqual(report["final_recommendation"]["action"], "needs_more_data")

    def test_cross_year_stability_classification(self):
        report = build_multiyear_diagnostics_report(
            symbol="SPY",
            year_reports=[self._year_report("2023"), self._year_report("2024")],
        ).report

        regimes = {row["regime_feature"]: row for row in report["cross_year_stability"]["persisting_regimes"]}
        self.assertTrue(regimes["realized_vol_60d"]["persistent"])
        target_21d = report["cross_year_stability"]["target_21d_stability"]
        self.assertEqual(target_21d["status"], "ok")
        self.assertTrue(target_21d["target_21d_remains_more_stable"])

    def test_feature_sign_flips_across_years(self):
        report = build_multiyear_diagnostics_report(
            symbol="SPY",
            year_reports=[
                self._year_report("2023", feature_sign="positive"),
                self._year_report("2024", feature_sign="negative"),
            ],
        ).report

        flips = {
            row["feature"]: row
            for row in report["cross_year_stability"]["feature_sign_flips_across_years"]
        }
        self.assertTrue(flips["realized_vol_60d"]["flipped"])

    def test_no_edge_claim_language_and_write_report(self):
        result = build_multiyear_diagnostics_report(
            symbol="SPY",
            year_reports=[self._year_report("2023"), self._year_report("2024")],
        )

        self.assertFalse(result.report["performance_claim"])
        self.assertEqual(result.report["explicit_warning"], "no edge claim")
        self.assertTrue(any("no model training" in warning for warning in result.report["warnings"]))
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            with patch.object(Path, "cwd", return_value=root):
                path = write_ml_multiyear_diagnostics_report(result.report, reports_dir=root / "reports")
            self.assertTrue(path.exists())
            self.assertEqual(path.suffix, ".json")


if __name__ == "__main__":
    unittest.main()
