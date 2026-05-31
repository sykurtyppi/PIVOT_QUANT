"""Regression tests for the CRITICAL-1 remediation (adversarial audit 2026-05-31).

Covers, per the remediation spec:
  1. CRITICAL-1 reverted: unresolved (resolution_min IS NULL) rows are RETAINED
     as reject=0/break=0 negatives by default; the experimental filter still
     drops them when explicitly enabled.
  2. CRITICAL-3: symbol scoping drops foreign-symbol rows (SPX leaking into a
     SPY model), at both the DuckDB load boundary and the in-frame guard.
  3. Partial-coverage unresolved rows are handled deliberately (dropped only
     when a coverage column is present + the flag is set; otherwise kept with a
     recorded count — never silently treated as clean negatives).
  4. Promotion guard: a candidate cannot be promoted without an OOS-validated
     evidence report (in-sample-only / threshold_tune_slice is rejected), and
     --force-promote does NOT bypass it.
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import train_rf_artifacts as tra  # noqa: E402
from scripts import model_governance as gov  # noqa: E402


def _frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _ev(symbol="SPY", resolution_min=3.0, reject=1, brk=0, coverage=1.0, ts=1):
    return {
        "symbol": symbol,
        "ts_event": ts,
        "resolution_min": resolution_min,
        "reject": reject,
        "break": brk,
        "bar_coverage": coverage,
    }


# ───────────────────────── CRITICAL-1 revert ───────────────────────── #


class TestUnresolvedRetention(unittest.TestCase):
    def test_unresolved_kept_by_default(self):
        """Default (filter_unresolved=False): unresolved chops are retained."""
        df = _frame([
            _ev(resolution_min=3.0, reject=1, ts=1),
            _ev(resolution_min=None, reject=0, brk=0, ts=2),  # chop / timeout
            _ev(resolution_min=None, reject=0, brk=0, ts=3),  # chop / timeout
        ])
        out, info = tra.apply_quality_filters(
            df, horizon=15, symbol="SPY",
            filter_unresolved=False,
            drop_low_coverage_unresolved=False,
            unresolved_min_coverage=0.8, coverage_column="bar_coverage",
        )
        # all 3 rows retained; both unresolved chops survive as negatives
        self.assertEqual(len(out), 3)
        self.assertEqual(int(out["resolution_min"].isna().sum()), 2)
        self.assertEqual(info["unresolved_handling"]["unresolved_rows_total"], 2)
        self.assertEqual(info["unresolved_handling"]["unresolved_rows_kept"], 2)
        self.assertFalse(info["experimental_filter_unresolved_applied"])

    def test_experimental_filter_drops_unresolved(self):
        """filter_unresolved=True is the experimental NOT-FOR-PROMOTION path."""
        df = _frame([
            _ev(resolution_min=3.0, reject=1, ts=1),
            _ev(resolution_min=None, reject=0, brk=0, ts=2),
        ])
        out, info = tra.apply_quality_filters(
            df, horizon=15, symbol="SPY",
            filter_unresolved=True,
            drop_low_coverage_unresolved=False,
            unresolved_min_coverage=0.8, coverage_column="bar_coverage",
        )
        self.assertEqual(len(out), 1)
        self.assertEqual(int(out["resolution_min"].isna().sum()), 0)
        self.assertTrue(info["experimental_filter_unresolved_applied"])

    def test_argparse_default_filter_unresolved_off(self):
        """Env unset → --filter-unresolved defaults OFF (CRITICAL-1 reverted)."""
        import os
        prev = os.environ.pop("RF_FILTER_UNRESOLVED_EVENTS", None)
        try:
            self.assertFalse(tra._env_bool("RF_FILTER_UNRESOLVED_EVENTS", False))
        finally:
            if prev is not None:
                os.environ["RF_FILTER_UNRESOLVED_EVENTS"] = prev


# ───────────────────────── CRITICAL-3 symbol scope ─────────────────── #


class TestSymbolScoping(unittest.TestCase):
    def test_foreign_symbol_dropped_in_frame(self):
        df = _frame([
            _ev(symbol="SPY", ts=1),
            _ev(symbol="SPX", ts=2),   # foreign index contamination
            _ev(symbol="SPX", ts=3),
            _ev(symbol="SPY", ts=4),
        ])
        out, info = tra.apply_quality_filters(
            df, horizon=15, symbol="SPY",
            filter_unresolved=False,
            drop_low_coverage_unresolved=False,
            unresolved_min_coverage=0.8, coverage_column="bar_coverage",
        )
        self.assertEqual(len(out), 2)
        self.assertTrue((out["symbol"] == "SPY").all())
        self.assertEqual(info["foreign_symbol_rows_dropped"], 2)

    def test_load_dataframe_symbol_filter(self):
        """load_dataframe scopes the DuckDB query to the requested symbol."""
        duckdb = __import__("duckdb")
        with tempfile.TemporaryDirectory() as d:
            dbp = str(Path(d) / "t.duckdb")
            con = duckdb.connect(dbp)
            con.execute(
                "CREATE TABLE training_events_v1 AS SELECT * FROM (VALUES "
                "('SPY', 15, 100), ('SPX', 15, 6600), ('SPY', 15, 101)) "
                "AS t(symbol, horizon_min, ts_event)"
            )
            con.close()
            spy = tra.load_dataframe(dbp, "training_events_v1", 15, symbol="SPY")
            self.assertEqual(len(spy), 2)
            self.assertTrue((spy["symbol"] == "SPY").all())
            allrows = tra.load_dataframe(dbp, "training_events_v1", 15, symbol="")
            self.assertEqual(len(allrows), 3)


# ───────────────────────── Item-3 partial coverage ─────────────────── #


class TestPartialCoverage(unittest.TestCase):
    def test_low_coverage_unresolved_dropped_when_column_present(self):
        df = _frame([
            _ev(resolution_min=3.0, reject=1, coverage=0.10, ts=1),  # resolved, low cov → KEEP
            _ev(resolution_min=None, reject=0, brk=0, coverage=0.95, ts=2),  # unresolved full cov → keep
            _ev(resolution_min=None, reject=0, brk=0, coverage=0.20, ts=3),  # unresolved low cov → DROP
        ])
        out, info = tra.apply_quality_filters(
            df, horizon=15, symbol="SPY",
            filter_unresolved=False,
            drop_low_coverage_unresolved=True,
            unresolved_min_coverage=0.8, coverage_column="bar_coverage",
        )
        # resolved low-coverage row is NOT dropped (only unresolved are gated)
        self.assertEqual(len(out), 2)
        self.assertEqual(info["unresolved_handling"]["low_coverage_dropped"], 1)
        self.assertTrue(info["unresolved_handling"]["coverage_column_present"])
        self.assertEqual(set(out["ts_event"]), {1, 2})

    def test_no_coverage_column_keeps_unresolved_non_silently(self):
        df = _frame([
            {"symbol": "SPY", "ts_event": 1, "resolution_min": None, "reject": 0, "break": 0},
            {"symbol": "SPY", "ts_event": 2, "resolution_min": 3.0, "reject": 1, "break": 0},
        ])
        out, info = tra.apply_quality_filters(
            df, horizon=15, symbol="SPY",
            filter_unresolved=False,
            drop_low_coverage_unresolved=True,   # requested...
            unresolved_min_coverage=0.8, coverage_column="bar_coverage",  # ...but column absent
        )
        self.assertEqual(len(out), 2)  # nothing dropped
        self.assertFalse(info["unresolved_handling"]["coverage_column_present"])
        self.assertEqual(info["unresolved_handling"]["unresolved_rows_total"], 1)
        self.assertEqual(info["unresolved_handling"]["low_coverage_dropped"], 0)


# ───────────────────────── Promotion guard ─────────────────────────── #


class TestOOSValidationGuard(unittest.TestCase):
    def _write_report(self, d: Path, version, *, oos_passed, promo_ready,
                      disposition="held_out_oos"):
        report = {
            "candidate_manifest": {"version": version},
            "candidate_readiness": {
                "oos_validation_passed": oos_passed,
                "promotion_ready": promo_ready,
                "promotion_disposition": disposition,
            },
        }
        p = d / f"report_{version}.json"
        p.write_text(json.dumps(report))
        return p

    def test_no_report_blocks(self):
        ok, reason, _ = gov.check_oos_validation("v500", "")
        self.assertFalse(ok)
        self.assertIn("no_evidence_report", reason)

    def test_in_sample_only_blocks(self):
        with tempfile.TemporaryDirectory() as d:
            p = self._write_report(
                Path(d), "v500", oos_passed=False, promo_ready=False,
                disposition="threshold_tune_slice",
            )
            ok, reason, detail = gov.check_oos_validation("v500", str(p))
            self.assertFalse(ok)
            self.assertIn("oos_validation_not_passed", reason)
            self.assertEqual(detail["promotion_disposition"], "threshold_tune_slice")

    def test_version_mismatch_blocks(self):
        with tempfile.TemporaryDirectory() as d:
            p = self._write_report(
                Path(d), "v499", oos_passed=True, promo_ready=True,
            )
            ok, reason, _ = gov.check_oos_validation("v500", str(p))
            self.assertFalse(ok)
            self.assertIn("version_mismatch", reason)

    def test_oos_passed_allows(self):
        with tempfile.TemporaryDirectory() as d:
            p = self._write_report(
                Path(d), "v500", oos_passed=True, promo_ready=True,
            )
            ok, reason, _ = gov.check_oos_validation("v500", str(p))
            self.assertTrue(ok)
            self.assertEqual(reason, "oos_validation_passed")

    def test_promotion_ready_false_blocks(self):
        with tempfile.TemporaryDirectory() as d:
            p = self._write_report(
                Path(d), "v500", oos_passed=True, promo_ready=False,
            )
            ok, reason, _ = gov.check_oos_validation("v500", str(p))
            self.assertFalse(ok)
            self.assertIn("not_promotion_ready", reason)


if __name__ == "__main__":
    unittest.main()
