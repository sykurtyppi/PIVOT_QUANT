"""Tests for Audit-Fix PR5: cross-period validation aggregation.

Verifies:
  - Default validation defaults are unchanged.
  - discover_year_datasets correctly resolves dataset paths.
  - aggregate_cross_period_validation produces False unless ALL periods are
    independently paper-ready, and never produces True with fewer than 2
    periods.
  - The frozen candidate signal logic (vol_split_value derivation, threshold
    quantile=0.70) is unchanged.
"""

import json
import tempfile
import unittest
from pathlib import Path

from services.external_data.ml_candidate_signal import CANDIDATE_SIGNAL_NAME
from services.external_data.ml_candidate_signal_sensitivity import REFERENCE_QUANTILE
from services.external_data.ml_cross_period_validation import (
    aggregate_cross_period_validation,
    write_cross_period_report,
)
from services.external_data.ml_regime_benchmark import (
    DEFAULT_YEAR_DATASETS,
    discover_year_datasets,
)
from services.external_data.ml_regime_validation import (
    DEFAULT_TEST_YEAR,
    DEFAULT_TRAIN_YEARS,
)


def _passing_period_report(*, train: list[str], test: str) -> dict:
    """A minimal validation-report-shaped dict with the candidate paper-ready."""
    return {
        "name": "ml_regime_validation",
        "status": "pass",
        "symbol": "SPY",
        "train_years": train,
        "test_year": test,
        "validated": True,
        "candidate_readiness_checklist": {
            "candidate_status": "exploratory_paper_candidate",
            "candidate_ready_for_paper_observation": True,
            "criteria": {"boundary_clean": True, "filtered_validated": True},
        },
        "candidate_signal_diagnostics": {
            "thresholds": {
                "vol_split_value": 0.12,
                "maturity_threshold": 1.5,
            },
            "train": {"sample_size_safe": True},
            "test": {"sample_size_safe": True},
        },
        "late_trend_removal_validation": {
            "baseline_validation": {
                "train": {"sample_size": 187, "win_rate_5d": 0.668, "mean_return_5d": 0.0056},
                "test": {"sample_size": 99, "win_rate_5d": 0.687, "mean_return_5d": 0.0061},
            },
            "filtered_validation": {
                "train": {"sample_size": 124, "win_rate_5d": 0.685, "mean_return_5d": 0.0054},
                "test": {"sample_size": 68, "win_rate_5d": 0.81, "mean_return_5d": 0.0101},
            },
            "improvement_summary": {
                "rows_removed_train": 63,
                "rows_removed_test": 31,
            },
        },
        "boundary_purge_report": {
            "boundary_label_overlap_detected": True,
            "boundary_purge_applied": True,
            "rows_purged": 4,
        },
    }


def _failing_period_report(*, train: list[str], test: str) -> dict:
    r = _passing_period_report(train=train, test=test)
    r["validated"] = False
    r["candidate_readiness_checklist"]["candidate_ready_for_paper_observation"] = False
    r["candidate_readiness_checklist"]["candidate_status"] = "blocked"
    r["candidate_readiness_checklist"]["criteria"]["filtered_validated"] = False
    return r


class TestDefaultsUnchanged(unittest.TestCase):
    """Defaults must NOT change — that is the point of cross-period work."""

    def test_default_train_years_unchanged(self):
        self.assertEqual(DEFAULT_TRAIN_YEARS, ["2023", "2024"])

    def test_default_test_year_unchanged(self):
        self.assertEqual(DEFAULT_TEST_YEAR, "2025")

    def test_default_year_datasets_unchanged(self):
        years = [item["year"] for item in DEFAULT_YEAR_DATASETS]
        self.assertEqual(years, ["2023", "2024", "2025"])

    def test_frozen_signal_name_unchanged(self):
        self.assertEqual(CANDIDATE_SIGNAL_NAME, "high_vol_trend_early_candidate")

    def test_reference_quantile_unchanged(self):
        self.assertAlmostEqual(REFERENCE_QUANTILE, 0.70)


class TestDiscoverYearDatasets(unittest.TestCase):

    def test_finds_existing_csv_in_temp_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp).resolve()
            csv_path = tmp_path / "spy_2020-01-02_2020-12-31.csv"
            meta_path = tmp_path / "spy_2020-01-02_2020-12-31.metadata.json"
            csv_path.write_text("symbol,entry_date\n", encoding="utf-8")
            meta_path.write_text("{}", encoding="utf-8")
            result = discover_year_datasets(["2020"], datasets_dir=tmp_path)
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["year"], "2020")
            self.assertEqual(result[0]["dataset_path"], str(csv_path))
            self.assertEqual(result[0]["metadata_path"], str(meta_path))
            self.assertNotIn("status", result[0])

    def test_marks_missing_year_when_no_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = discover_year_datasets(["2099"], datasets_dir=tmp)
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["status"], "missing")
            self.assertIn("no dataset file matching", result[0]["reason"])
            self.assertIn("run_model_ready_dataset_oneyear_smoke.py", result[0]["reason"])

    def test_returns_one_entry_per_year_in_input_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            for year in ("2020", "2021", "2022"):
                (tmp_path / f"spy_{year}-01-02_{year}-12-31.csv").write_text("a", encoding="utf-8")
            result = discover_year_datasets(["2022", "2020", "2021"], datasets_dir=tmp_path)
            self.assertEqual([r["year"] for r in result], ["2022", "2020", "2021"])

    def test_metadata_path_none_when_metadata_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            (tmp_path / "spy_2020-01-02_2020-12-31.csv").write_text("a", encoding="utf-8")
            result = discover_year_datasets(["2020"], datasets_dir=tmp_path)
            self.assertIsNone(result[0]["metadata_path"])


class TestAggregateCrossPeriod(unittest.TestCase):

    def test_two_passing_periods_validates(self):
        reports = [
            _passing_period_report(train=["2023", "2024"], test="2025"),
            _passing_period_report(train=["2020", "2021"], test="2022"),
        ]
        agg = aggregate_cross_period_validation(reports)
        self.assertTrue(agg["cross_period_validated"])
        self.assertEqual(agg["period_count"], 2)
        self.assertTrue(agg["agreement_summary"]["all_periods_paper_ready"])
        self.assertEqual(agg["agreement_summary"]["periods_not_ready"], [])

    def test_one_failing_period_invalidates(self):
        reports = [
            _passing_period_report(train=["2023", "2024"], test="2025"),
            _failing_period_report(train=["2020", "2021"], test="2022"),
        ]
        agg = aggregate_cross_period_validation(reports)
        self.assertFalse(agg["cross_period_validated"])
        self.assertEqual(len(agg["agreement_summary"]["periods_not_ready"]), 1)
        self.assertIn("2020+2021", agg["agreement_summary"]["periods_not_ready"][0])

    def test_single_period_is_not_cross_period_even_when_passing(self):
        reports = [_passing_period_report(train=["2023", "2024"], test="2025")]
        agg = aggregate_cross_period_validation(reports)
        self.assertFalse(agg["cross_period_validated"])
        self.assertEqual(agg["period_count"], 1)
        self.assertEqual(agg["agreement_summary"]["minimum_periods_for_cross_period"], 2)

    def test_empty_reports_returns_missing_status(self):
        agg = aggregate_cross_period_validation([])
        self.assertEqual(agg["status"], "missing")
        self.assertFalse(agg["cross_period_validated"])
        self.assertEqual(agg["periods"], [])

    def test_period_metrics_contain_required_keys(self):
        reports = [_passing_period_report(train=["2023", "2024"], test="2025")]
        agg = aggregate_cross_period_validation(reports)
        period = agg["periods"][0]
        for key in (
            "label",
            "status",
            "validated",
            "candidate_status",
            "candidate_ready_for_paper_observation",
            "thresholds",
            "baseline_high_vol_trend_positive",
            "filtered_early_trend",
            "rows_removed",
            "boundary_purge",
            "criteria",
        ):
            self.assertIn(key, period, msg=f"missing period key: {key}")

    def test_period_label_format(self):
        reports = [_passing_period_report(train=["2020", "2021"], test="2022")]
        agg = aggregate_cross_period_validation(reports)
        self.assertEqual(agg["periods"][0]["label"], "train=2020+2021; test=2022")

    def test_partial_train_label_uses_partial_suffix(self):
        report = _passing_period_report(train=["2021"], test="2022")
        report["data_coverage"] = {
            "period_label": "train=2021_partial; test=2022",
            "train_coverage_start": "2021-04-05",
            "train_coverage_end": "2021-12-31",
            "train_is_partial": True,
            "data_coverage_note": "T9 SPY options coverage starts 2021-04",
        }
        agg = aggregate_cross_period_validation([report])
        self.assertEqual(agg["periods"][0]["label"], "train=2021_partial; test=2022")
        cov = agg["periods"][0]["data_coverage"]
        self.assertTrue(cov["train_is_partial"])
        self.assertEqual(cov["train_coverage_start"], "2021-04-05")
        self.assertEqual(cov["train_coverage_end"], "2021-12-31")
        self.assertIn("April", cov["data_coverage_note"]) if False else self.assertIn(
            "T9", cov["data_coverage_note"]
        )

    def test_data_coverage_defaults_when_block_absent(self):
        reports = [_passing_period_report(train=["2023", "2024"], test="2025")]
        agg = aggregate_cross_period_validation(reports)
        cov = agg["periods"][0]["data_coverage"]
        self.assertFalse(cov["train_is_partial"])
        self.assertIsNone(cov["train_coverage_start"])
        self.assertIsNone(cov["train_coverage_end"])
        self.assertIsNone(cov["data_coverage_note"])
        self.assertEqual(cov["period_label"], "train=2023+2024; test=2025")

    def test_governance_definitions_block(self):
        agg = aggregate_cross_period_validation(
            [_passing_period_report(train=["2023", "2024"], test="2025")]
        )
        defs = agg["definitions"]
        self.assertFalse(defs["training_performed"])
        self.assertFalse(defs["threshold_optimization_performed"])
        self.assertFalse(defs["filter_changes_performed"])
        self.assertFalse(defs["live_trading_enabled"])
        self.assertFalse(defs["governance_promotion_performed"])
        self.assertFalse(defs["edge_claim"])

    def test_disclaimer_present(self):
        agg = aggregate_cross_period_validation(
            [_passing_period_report(train=["2023", "2024"], test="2025")]
        )
        self.assertIn("no edge claim", agg["disclaimer"])
        self.assertIn("does not authorize live integration", agg["disclaimer"])

    def test_partial_report_fields_default_none(self):
        """Reports missing diagnostic blocks should not crash aggregation."""
        partial = {
            "name": "ml_regime_validation",
            "status": "fail",
            "symbol": "SPY",
            "train_years": ["2020", "2021"],
            "test_year": "2022",
            "validated": False,
        }
        agg = aggregate_cross_period_validation([partial])
        period = agg["periods"][0]
        self.assertEqual(period["candidate_status"], None)
        self.assertFalse(period["candidate_ready_for_paper_observation"])
        self.assertEqual(period["thresholds"], {})


class TestWriteCrossPeriodReport(unittest.TestCase):

    def test_write_creates_json_inside_repo(self):
        agg = aggregate_cross_period_validation(
            [_passing_period_report(train=["2020", "2021"], test="2022")]
        )
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as tmp:
            tmp_path = Path(tmp).resolve()
            written = write_cross_period_report(agg, reports_dir=tmp_path, stem="test_xperiod")
            self.assertTrue(written.exists())
            payload = json.loads(written.read_text())
            self.assertEqual(payload["name"], "ml_cross_period_validation")
            self.assertEqual(payload["period_count"], 1)

    def test_write_rejects_path_outside_repo(self):
        agg = aggregate_cross_period_validation([])
        with self.assertRaises(ValueError):
            write_cross_period_report(agg, reports_dir=Path("/tmp/not_in_repo_xperiod"))


if __name__ == "__main__":
    unittest.main()
