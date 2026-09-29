from __future__ import annotations

import importlib.util
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

import duckdb
TESTS_PYTHON_DIR = Path(__file__).resolve().parent
if str(TESTS_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_PYTHON_DIR))
from helpers.asgi_contract import request_json


ROOT = Path(__file__).resolve().parents[2]
API_PATH = ROOT / "server" / "model_api.py"
REVIEW_STORE_MODULE_PATH = ROOT / "server" / "model_review_store.py"


def load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def create_research_marts(duckdb_path: Path) -> None:
    con = duckdb.connect(str(duckdb_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS pq_research")
        con.execute(
            """
            CREATE TABLE pq_research.mart_slice_expectancy_daily (
                symbol VARCHAR,
                event_date_et DATE,
                horizon_min INTEGER,
                regime_bucket VARCHAR,
                level_family VARCHAR,
                tod_bucket VARCHAR,
                atr_zone VARCHAR,
                confluence_bucket VARCHAR,
                rows_n INTEGER,
                avg_reject_net_bps DOUBLE,
                win_rate_reject DOUBLE,
                avg_break_net_bps DOUBLE,
                win_rate_break DOUBLE,
                avg_mfe_bps DOUBLE,
                avg_mae_bps DOUBLE,
                std_reject_net_bps DOUBLE,
                p05_reject_net_bps DOUBLE,
                p50_reject_net_bps DOUBLE,
                p95_reject_net_bps DOUBLE
            )
            """
        )
        rows = [
            ("SPY", "2026-03-13", 15, "compression", "resistance", "open", "near", "stacked", 120, 1.20, 0.57, 0.0, 0.0, 8.0, -5.5, 0.0, 0.0, 0.0, 0.0),
            ("SPY", "2026-03-14", 15, "compression", "resistance", "open", "near", "stacked", 100, 0.80, 0.54, 0.0, 0.0, 7.2, -5.1, 0.0, 0.0, 0.0, 0.0),
            ("SPY", "2026-03-15", 15, "compression", "resistance", "open", "near", "stacked", 80, -0.40, 0.46, 0.0, 0.0, 6.1, -6.8, 0.0, 0.0, 0.0, 0.0),
            ("SPY", "2026-03-13", 15, "expansion", "resistance", "open", "near", "stacked", 80, 0.10, 0.50, 0.0, 0.0, 6.7, -6.2, 0.0, 0.0, 0.0, 0.0),
            ("SPY", "2026-03-14", 15, "expansion", "resistance", "open", "near", "stacked", 90, -0.20, 0.48, 0.0, 0.0, 6.4, -6.5, 0.0, 0.0, 0.0, 0.0),
            ("SPY", "2026-03-15", 15, "expansion", "resistance", "open", "near", "stacked", 95, -0.60, 0.45, 0.0, 0.0, 5.8, -7.2, 0.0, 0.0, 0.0, 0.0),
            ("SPY", "2026-03-13", 60, "expansion", "resistance", "open", "near", "stacked", 90, 2.10, 0.61, 0.0, 0.0, 12.0, -8.0, 0.0, 0.0, 0.0, 0.0),
            ("SPY", "2026-03-14", 60, "expansion", "resistance", "open", "near", "stacked", 95, 1.70, 0.59, 0.0, 0.0, 11.4, -8.3, 0.0, 0.0, 0.0, 0.0),
            ("SPY", "2026-03-15", 60, "expansion", "resistance", "open", "near", "stacked", 88, 1.10, 0.55, 0.0, 0.0, 10.9, -8.7, 0.0, 0.0, 0.0, 0.0),
            ("SPY", "2026-03-13", 60, "compression", "resistance", "open", "near", "stacked", 90, 0.60, 0.52, 0.0, 0.0, 10.0, -9.0, 0.0, 0.0, 0.0, 0.0),
            ("SPY", "2026-03-14", 60, "compression", "resistance", "open", "near", "stacked", 85, 0.40, 0.51, 0.0, 0.0, 9.7, -9.2, 0.0, 0.0, 0.0, 0.0),
            ("SPY", "2026-03-15", 60, "compression", "resistance", "open", "near", "stacked", 82, 0.10, 0.49, 0.0, 0.0, 9.4, -9.4, 0.0, 0.0, 0.0, 0.0),
            ("QQQ", "2026-03-13", 15, "compression", "resistance", "open", "near", "stacked", 110, 1.50, 0.59, -0.30, 0.48, 8.6, -5.0, 0.0, 0.0, 0.0, 0.0),
            ("QQQ", "2026-03-14", 15, "compression", "resistance", "open", "near", "stacked", 104, 1.10, 0.56, -0.10, 0.49, 8.1, -5.4, 0.0, 0.0, 0.0, 0.0),
            ("QQQ", "2026-03-15", 15, "compression", "resistance", "open", "near", "stacked", 92, 0.60, 0.52, 0.20, 0.51, 7.8, -5.7, 0.0, 0.0, 0.0, 0.0),
            ("QQQ", "2026-03-13", 60, "expansion", "resistance", "open", "near", "stacked", 102, 1.20, 0.56, -0.40, 0.46, 12.4, -7.8, 0.0, 0.0, 0.0, 0.0),
            ("QQQ", "2026-03-14", 60, "expansion", "resistance", "open", "near", "stacked", 99, 0.90, 0.53, -0.10, 0.48, 11.9, -8.1, 0.0, 0.0, 0.0, 0.0),
            ("QQQ", "2026-03-15", 60, "expansion", "resistance", "open", "near", "stacked", 95, 0.70, 0.51, 0.10, 0.52, 11.3, -8.4, 0.0, 0.0, 0.0, 0.0),
        ]
        con.executemany(
            "INSERT INTO pq_research.mart_slice_expectancy_daily VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    finally:
        con.close()


class ModelApiTest(unittest.TestCase):
    def test_endpoint_contracts_cover_key_model_api_surfaces(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            duckdb_path = tmp / "research.duckdb"
            create_research_marts(duckdb_path)

            explicit_dir = tmp / "explicit_family"
            explicit_metadata = explicit_dir / "metadata_runtime" / "metadata_t9_explicit.json"
            write_json(
                explicit_metadata,
                {
                    "version": "t9_explicit",
                    "feature_version": "fv_explicit",
                    "trained_end_ts": 1712966400000,
                    "tune_date_range": {
                        "min_event_date_et": "2026-03-13",
                        "max_event_date_et": "2026-03-25",
                    },
                    "calibration": {"reject": {"15": "isotonic", "60": "isotonic"}},
                    "thresholds": {"reject": {"15": 0.84, "60": 0.91}},
                    "thresholds_meta": {
                        "reject": {
                            "15": {
                                "guard_applied": True,
                                "guard_reason": "non_positive_utility(-22.500000<=0.000000)",
                                "objective": "utility_bps",
                                "precision": 0.82,
                                "recall": 0.19,
                                "signals": 222,
                                "selected_utility_avg": -1.11,
                                "selected_utility_sum": -246.42,
                                "selected_tp_count": 180,
                                "selected_fp_count": 42,
                                "tune_prob_utility_corr_all": 0.03,
                                "tune_prob_utility_corr_pos": -0.08,
                            },
                            "60": {
                                "guard_applied": False,
                                "guard_reason": None,
                                "objective": "utility_bps",
                                "precision": 0.77,
                                "recall": 0.11,
                                "signals": 74,
                                "selected_utility_avg": 1.92,
                                "selected_utility_sum": 142.08,
                                "selected_tp_count": 51,
                                "selected_fp_count": 23,
                                "tune_prob_utility_corr_all": 0.12,
                                "tune_prob_utility_corr_pos": 0.19,
                            },
                        }
                    },
                    "stats": {
                        "15": {
                            "reject": {
                                "sample_size": 1200,
                                "reject_rate": 0.44,
                                "mfe_bps_reject": 10.0,
                                "mfe_bps_reject_other": 4.0,
                                "mae_bps_reject": -5.0,
                                "mae_bps_reject_other": -8.5,
                                "by_regime": {
                                    "compression": {
                                        "sample_size": 700,
                                        "sample_share": 0.58,
                                        "reject_rate": 0.47,
                                        "mfe_bps_reject": 12.0,
                                        "mfe_bps_reject_other": 4.0,
                                        "mae_bps_reject": -4.2,
                                        "mae_bps_reject_other": -8.8,
                                    }
                                },
                            }
                        },
                        "60": {
                            "reject": {
                                "sample_size": 980,
                                "reject_rate": 0.61,
                                "mfe_bps_reject": 17.0,
                                "mfe_bps_reject_other": 5.0,
                                "mae_bps_reject": -8.0,
                                "mae_bps_reject_other": -16.0,
                                "by_regime": {
                                    "expansion": {
                                        "sample_size": 620,
                                        "sample_share": 0.63,
                                        "reject_rate": 0.66,
                                        "mfe_bps_reject": 18.5,
                                        "mfe_bps_reject_other": 5.2,
                                        "mae_bps_reject": -7.6,
                                        "mae_bps_reject_other": -16.8,
                                    }
                                },
                            }
                        },
                    },
                    "model_context": {
                        "symbols": ["QQQ"],
                        "default_symbol": "QQQ",
                        "targets": ["reject"],
                        "cost_model_version": "rt_cost_v1",
                        "trade_cost_bps": 1.3,
                        "training_view": "training_events_v1",
                        "source_duckdb_path": str(duckdb_path),
                    },
                },
            )
            write_json(explicit_dir / "manifest_runtime_latest.json", {"version": "t9_explicit"})

            legacy_dir = tmp / "legacy_family"
            legacy_metadata = legacy_dir / "metadata_runtime" / "metadata_t9_legacy.json"
            write_json(
                legacy_metadata,
                {
                    "version": "t9_legacy",
                    "feature_version": "fv_legacy",
                    "trained_end_ts": 1712800000000,
                    "tune_date_range": {
                        "min_event_date_et": "2026-03-13",
                        "max_event_date_et": "2026-03-15",
                    },
                    "thresholds": {"reject": {"15": 0.71}},
                    "thresholds_meta": {
                        "reject": {
                            "15": {
                                "guard_applied": False,
                                "guard_reason": None,
                                "objective": "utility_bps",
                                "precision": 0.7,
                                "recall": 0.1,
                                "signals": 20,
                                "selected_utility_avg": 0.5,
                                "selected_utility_sum": 10.0,
                                "selected_tp_count": 14,
                                "selected_fp_count": 6,
                                "tune_prob_utility_corr_all": 0.04,
                                "tune_prob_utility_corr_pos": 0.08,
                            }
                        }
                    },
                    "stats": {
                        "15": {
                            "reject": {
                                "sample_size": 640,
                                "reject_rate": 0.39,
                                "mfe_bps_reject": 7.0,
                                "mfe_bps_reject_other": 4.2,
                                "mae_bps_reject": -6.8,
                                "mae_bps_reject_other": -8.1,
                            }
                        }
                    },
                },
            )
            write_json(legacy_dir / "manifest_runtime_latest.json", {"version": "t9_legacy"})

            prev_root = os.environ.get("MODEL_REGISTRY_ROOT")
            prev_roots = os.environ.get("MODEL_REGISTRY_ROOTS")
            prev_duckdb = os.environ.get("DUCKDB_PATH")
            prev_review_log = os.environ.get("MODEL_REVIEW_LOG_PATH")
            prev_review_store = os.environ.get("MODEL_REVIEW_DB_PATH")
            prev_review_backend = os.environ.get("MODEL_REVIEW_BACKEND")
            prev_allow_legacy = os.environ.get("ALLOW_LEGACY_MODEL_METADATA")
            try:
                os.environ["MODEL_REGISTRY_ROOT"] = str(tmp)
                os.environ.pop("MODEL_REGISTRY_ROOTS", None)
                os.environ["DUCKDB_PATH"] = str(duckdb_path)
                os.environ["MODEL_REVIEW_LOG_PATH"] = str(tmp / "review_log.jsonl")
                os.environ["MODEL_REVIEW_DB_PATH"] = str(tmp / "review_store.sqlite")
                os.environ["MODEL_REVIEW_BACKEND"] = "sqlite"
                os.environ["ALLOW_LEGACY_MODEL_METADATA"] = "1"
                api_module = load_module("model_api_endpoint_contract_module", API_PATH)
                health_resp = request_json(api_module.app, "GET", "/health")
                self.assertEqual(health_resp.status_code, 200)
                health_payload = health_resp.json()
                self.assertEqual(health_payload["status"], "ok")
                self.assertEqual(health_payload["models_found"], 2)

                registry_resp = request_json(api_module.app, "GET", "/registry", params={"limit": 10})
                self.assertEqual(registry_resp.status_code, 200)
                registry_payload = registry_resp.json()
                self.assertEqual(len(registry_payload["models"]), 2)
                explicit_row = next(row for row in registry_payload["models"] if row["version"] == "t9_explicit")
                legacy_row = next(row for row in registry_payload["models"] if row["version"] == "t9_legacy")

                baseline_resp = request_json(api_module.app, "GET", "/baseline-compare", params={"id": explicit_row["id"]})
                self.assertEqual(baseline_resp.status_code, 200)
                baseline_payload = baseline_resp.json()
                self.assertEqual(baseline_payload["model"]["version"], "t9_explicit")
                self.assertEqual(
                    baseline_payload["baseline_compare"]["summary"]["metadata_mode"],
                    "explicit",
                )
                self.assertFalse(
                    baseline_payload["baseline_compare"]["summary"]["requires_model_context_backfill"]
                )
                self.assertIn("source_duckdb_matches", baseline_payload["baseline_compare"]["summary"])
                self.assertIn("cost_model_matches", baseline_payload["baseline_compare"]["summary"])

                decision_resp = request_json(api_module.app, "GET", "/decision", params={"id": explicit_row["id"]})
                self.assertEqual(decision_resp.status_code, 200)
                decision_payload = decision_resp.json()
                self.assertEqual(decision_payload["decision"]["summary"]["metadata_mode"], "explicit")
                self.assertIn("evaluation_symbol", decision_payload["decision"]["summary"])
                self.assertIn("evaluation_target", decision_payload["decision"]["summary"])

                record_review_resp = request_json(
                    api_module.app,
                    "POST",
                    "/review-log",
                    json_body={
                        "id": explicit_row["id"],
                        "reviewer": "endpoint_contract",
                        "note": "Endpoint contract review entry.",
                    },
                )
                self.assertEqual(record_review_resp.status_code, 200)
                record_review_payload = record_review_resp.json()
                self.assertEqual(record_review_payload["status"], "ok")
                self.assertEqual(record_review_payload["review"]["reviewer"], "endpoint_contract")

                review_log_resp = request_json(
                    api_module.app,
                    "GET",
                    "/review-log",
                    params={"id": explicit_row["id"], "limit": 10},
                )
                self.assertEqual(review_log_resp.status_code, 200)
                review_log_payload = review_log_resp.json()
                self.assertEqual(review_log_payload["review_backend"], "sqlite")
                self.assertGreaterEqual(len(review_log_payload["reviews"]), 1)
                self.assertIn("review_store_path", review_log_payload)

                governance_summary_resp = request_json(api_module.app, "GET", "/governance-summary")
                self.assertEqual(governance_summary_resp.status_code, 200)
                governance_summary_payload = governance_summary_resp.json()
                self.assertIn("review_entries", governance_summary_payload)
                self.assertIn("latest_registry_model", governance_summary_payload)

                governance_workspace_resp = request_json(
                    api_module.app,
                    "GET",
                    "/governance-workspace",
                    params={"limit": 10},
                )
                self.assertEqual(governance_workspace_resp.status_code, 200)
                governance_workspace_payload = governance_workspace_resp.json()
                self.assertIn("summary", governance_workspace_payload)
                self.assertIn("queue", governance_workspace_payload)
                self.assertIn("queue_total_count", governance_workspace_payload["summary"])
                self.assertIn("queue_page_count", governance_workspace_payload["summary"])
                self.assertIn("queue_limit", governance_workspace_payload["summary"])

                legacy_review_log_resp = request_json(
                    api_module.app,
                    "GET",
                    "/review-log",
                    params={"id": legacy_row["id"], "limit": 10},
                )
                self.assertEqual(legacy_review_log_resp.status_code, 200)
                self.assertEqual(legacy_review_log_resp.json()["review_backend"], "sqlite")

                os.environ["ALLOW_LEGACY_MODEL_METADATA"] = "0"
                legacy_disabled_module = load_module("model_api_endpoint_contract_legacy_disabled_module", API_PATH)
                legacy_baseline_resp = request_json(
                    legacy_disabled_module.app,
                    "GET",
                    "/baseline-compare",
                    params={"id": legacy_row["id"]},
                )
                self.assertEqual(legacy_baseline_resp.status_code, 422)
                self.assertIn("missing model_context", legacy_baseline_resp.json()["detail"])
            finally:
                if prev_root is None:
                    os.environ.pop("MODEL_REGISTRY_ROOT", None)
                else:
                    os.environ["MODEL_REGISTRY_ROOT"] = prev_root
                if prev_roots is None:
                    os.environ.pop("MODEL_REGISTRY_ROOTS", None)
                else:
                    os.environ["MODEL_REGISTRY_ROOTS"] = prev_roots
                if prev_duckdb is None:
                    os.environ.pop("DUCKDB_PATH", None)
                else:
                    os.environ["DUCKDB_PATH"] = prev_duckdb
                if prev_review_log is None:
                    os.environ.pop("MODEL_REVIEW_LOG_PATH", None)
                else:
                    os.environ["MODEL_REVIEW_LOG_PATH"] = prev_review_log
                if prev_review_store is None:
                    os.environ.pop("MODEL_REVIEW_DB_PATH", None)
                else:
                    os.environ["MODEL_REVIEW_DB_PATH"] = prev_review_store
                if prev_review_backend is None:
                    os.environ.pop("MODEL_REVIEW_BACKEND", None)
                else:
                    os.environ["MODEL_REVIEW_BACKEND"] = prev_review_backend
                if prev_allow_legacy is None:
                    os.environ.pop("ALLOW_LEGACY_MODEL_METADATA", None)
                else:
                    os.environ["ALLOW_LEGACY_MODEL_METADATA"] = prev_allow_legacy

    def test_registry_and_diagnostics_contracts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            duckdb_path = tmp / "research.duckdb"
            create_research_marts(duckdb_path)

            alpha_dir = tmp / "alpha_family"
            alpha_metadata = alpha_dir / "metadata_runtime" / "metadata_t9_alpha_guarded.json"
            write_json(
                alpha_metadata,
                {
                    "version": "t9_alpha_guarded",
                    "feature_version": "fv_alpha",
                    "trained_end_ts": 1712966400000,
                    "tune_date_range": {
                        "min_event_date_et": "2026-03-13",
                        "max_event_date_et": "2026-03-25",
                    },
                    "calibration": {"reject": {"15": "isotonic", "60": "isotonic"}},
                    "stats": {
                        "15": {
                            "reject": {
                                "sample_size": 1200,
                                "reject_rate": 0.44,
                                "mfe_bps_reject": 10.0,
                                "mfe_bps_reject_other": 4.0,
                                "mae_bps_reject": -5.0,
                                "mae_bps_reject_other": -8.5,
                                "by_regime": {
                                    "compression": {
                                        "sample_size": 700,
                                        "sample_share": 0.58,
                                        "reject_rate": 0.47,
                                        "mfe_bps_reject": 12.0,
                                        "mfe_bps_reject_other": 4.0,
                                        "mae_bps_reject": -4.2,
                                        "mae_bps_reject_other": -8.8,
                                    }
                                },
                            }
                        },
                        "60": {
                            "reject": {
                                "sample_size": 980,
                                "reject_rate": 0.61,
                                "mfe_bps_reject": 17.0,
                                "mfe_bps_reject_other": 5.0,
                                "mae_bps_reject": -8.0,
                                "mae_bps_reject_other": -16.0,
                                "by_regime": {
                                    "expansion": {
                                        "sample_size": 620,
                                        "sample_share": 0.63,
                                        "reject_rate": 0.66,
                                        "mfe_bps_reject": 18.5,
                                        "mfe_bps_reject_other": 5.2,
                                        "mae_bps_reject": -7.6,
                                        "mae_bps_reject_other": -16.8,
                                    }
                                },
                            }
                        },
                    },
                    "shadow_policies": {
                        "model_side_margin_v1": {
                            "policy_name": "model_side_margin_v1",
                            "horizon": 60,
                            "scope": "regime_active_only",
                            "percentile_cutoff": 0.7,
                            "reject": {
                                "status": "ok",
                                "reason": "",
                                "horizon": 60,
                                "emitted_rows": 31,
                                "emitted_positive_rate": 0.68,
                                "emitted_utility_avg": 1.24,
                                "emitted_utility_sum": 38.44,
                            },
                        }
                    },
                    "thresholds": {"reject": {"15": 0.84, "60": 0.91}},
                    "model_context": {
                        "symbols": ["QQQ"],
                        "default_symbol": "QQQ",
                        "targets": ["reject"],
                        "cost_model_version": "rt_cost_v1",
                        "trade_cost_bps": 1.3,
                        "training_view": "training_events_v1",
                        "source_duckdb_path": str(duckdb_path),
                    },
                    "thresholds_meta": {
                        "reject": {
                            "15": {
                                "guard_applied": True,
                                "guard_reason": "non_positive_utility(-22.500000<=0.000000)",
                                "objective": "utility_bps",
                                "precision": 0.82,
                                "recall": 0.19,
                                "signals": 222,
                                "selected_utility_avg": -1.11,
                                "selected_utility_sum": -246.42,
                                "selected_tp_count": 180,
                                "selected_fp_count": 42,
                                "tune_prob_utility_corr_all": 0.03,
                                "tune_prob_utility_corr_pos": -0.08,
                                "top_candidates": [{"threshold": 0.84, "score": -246.42}],
                            },
                            "60": {
                                "guard_applied": False,
                                "guard_reason": None,
                                "objective": "utility_bps",
                                "precision": 0.77,
                                "recall": 0.11,
                                "signals": 74,
                                "selected_utility_avg": 1.92,
                                "selected_utility_sum": 142.08,
                                "selected_tp_count": 51,
                                "selected_fp_count": 23,
                                "tune_prob_utility_corr_all": 0.12,
                                "tune_prob_utility_corr_pos": 0.19,
                                "top_candidates": [{"threshold": 0.91, "score": 142.08}],
                            },
                        }
                    },
                },
            )
            write_json(alpha_dir / "manifest_runtime_latest.json", {"version": "t9_alpha_guarded"})

            beta_dir = tmp / "beta_family"
            beta_metadata = beta_dir / "metadata_runtime" / "metadata_t9_beta_blocked.json"
            write_json(
                beta_metadata,
                {
                    "version": "t9_beta_blocked",
                    "feature_version": "fv_beta",
                    "trained_end_ts": 1712876400000,
                    "stats": {
                        "15": {
                            "reject": {
                                "sample_size": 640,
                                "reject_rate": 0.39,
                                "mfe_bps_reject": 7.0,
                                "mfe_bps_reject_other": 4.2,
                                "mae_bps_reject": -6.8,
                                "mae_bps_reject_other": -8.1,
                            }
                        }
                    },
                    "tune_date_range": {
                        "min_event_date_et": "2026-02-01",
                        "max_event_date_et": "2026-02-28",
                    },
                    "calibration": {"reject": {"15": "sigmoid"}},
                    "thresholds": {"reject": {"15": 1.0}},
                    "thresholds_meta": {
                        "reject": {
                            "15": {
                                "guard_applied": True,
                                "guard_reason": "non_positive_utility(-44.000000<=0.000000)",
                                "objective": "utility_bps",
                                "precision": 0.66,
                                "recall": 0.07,
                                "signals": 18,
                                "selected_utility_avg": -2.44,
                                "selected_utility_sum": -44.0,
                                "selected_tp_count": 11,
                                "selected_fp_count": 7,
                                "tune_prob_utility_corr_all": -0.02,
                                "tune_prob_utility_corr_pos": -0.14,
                                "top_candidates": [{"threshold": 0.88, "score": -44.0}],
                            }
                        }
                    },
                },
            )
            write_json(beta_dir / "manifest_runtime_latest.json", {"version": "t9_beta_blocked"})

            prev_root = os.environ.get("MODEL_REGISTRY_ROOT")
            prev_roots = os.environ.get("MODEL_REGISTRY_ROOTS")
            prev_duckdb = os.environ.get("DUCKDB_PATH")
            prev_review_log = os.environ.get("MODEL_REVIEW_LOG_PATH")
            prev_review_store = os.environ.get("MODEL_REVIEW_DB_PATH")
            prev_review_backend = os.environ.get("MODEL_REVIEW_BACKEND")
            prev_allow_legacy = os.environ.get("ALLOW_LEGACY_MODEL_METADATA")
            try:
                os.environ["MODEL_REGISTRY_ROOT"] = str(tmp)
                os.environ.pop("MODEL_REGISTRY_ROOTS", None)
                os.environ["DUCKDB_PATH"] = str(duckdb_path)
                os.environ["MODEL_REVIEW_LOG_PATH"] = str(tmp / "review_log.jsonl")
                os.environ["MODEL_REVIEW_DB_PATH"] = str(tmp / "review_store.sqlite")
                os.environ["MODEL_REVIEW_BACKEND"] = "sqlite"
                os.environ["ALLOW_LEGACY_MODEL_METADATA"] = "1"
                api_module = load_module("model_api_test_module", API_PATH)

                health = api_module.health()
                self.assertEqual(health["status"], "ok")
                self.assertEqual(health["models_found"], 2)

                registry = api_module.registry(limit=10)
                self.assertEqual(len(registry["models"]), 2)
                self.assertEqual(registry["models"][0]["version"], "t9_alpha_guarded")
                self.assertEqual(registry["models"][0]["status"], "guarded")
                self.assertTrue(registry["models"][0]["is_family_latest"])
                self.assertEqual(registry["models"][1]["status"], "blocked")

                diagnostics = api_module.diagnostics(id=registry["models"][0]["id"])
                self.assertEqual(diagnostics["model"]["version"], "t9_alpha_guarded")
                self.assertEqual(diagnostics["metadata"]["feature_version"], "fv_alpha")
                self.assertEqual(len(diagnostics["horizon_details"]), 2)
                self.assertEqual(diagnostics["horizon_details"][0]["horizon"], 15)
                self.assertTrue(diagnostics["horizon_details"][0]["guard_applied"])
                self.assertEqual(diagnostics["horizon_details"][1]["horizon"], 60)
                self.assertFalse(diagnostics["horizon_details"][1]["guard_applied"])
                self.assertTrue(diagnostics["raw_paths"]["manifest_path"].endswith("manifest_runtime_latest.json"))

                benchmarks = api_module.benchmarks(id=registry["models"][0]["id"])
                self.assertEqual(benchmarks["model"]["version"], "t9_alpha_guarded")
                self.assertEqual(benchmarks["benchmarks"]["summary"]["registry_rank"], 1)
                self.assertEqual(len(benchmarks["benchmarks"]["horizon_rows"]), 2)
                self.assertEqual(benchmarks["benchmarks"]["horizon_rows"][0]["target"], "reject")
                self.assertEqual(benchmarks["benchmarks"]["horizon_rows"][0]["best_regime_by_separation"], "compression")
                self.assertEqual(benchmarks["benchmarks"]["research_handoff"]["date_from"], "2026-03-13")
                self.assertEqual(benchmarks["benchmarks"]["research_handoff"]["preferred_horizon"], 60)
                self.assertEqual(len(benchmarks["benchmarks"]["shadow_rows"]), 1)

                baseline_compare = api_module.baseline_compare(id=registry["models"][0]["id"])
                self.assertEqual(baseline_compare["model"]["version"], "t9_alpha_guarded")
                self.assertEqual(baseline_compare["baseline_compare"]["summary"]["symbol"], "QQQ")
                self.assertEqual(baseline_compare["baseline_compare"]["summary"]["metadata_mode"], "explicit")
                self.assertFalse(baseline_compare["baseline_compare"]["summary"]["requires_model_context_backfill"])
                self.assertEqual(baseline_compare["baseline_compare"]["summary"]["preferred_target"], "reject")
                self.assertTrue(baseline_compare["baseline_compare"]["summary"]["source_duckdb_matches"])
                self.assertTrue(baseline_compare["baseline_compare"]["summary"]["cost_model_matches"])
                self.assertTrue(baseline_compare["baseline_compare"]["summary"]["lineage_match"]["source_duckdb_matches"])
                self.assertEqual(baseline_compare["baseline_compare"]["summary"]["preferred_horizon"], 60)
                self.assertEqual(len(baseline_compare["baseline_compare"]["rows"]), 4)
                preferred_rows = [row for row in baseline_compare["baseline_compare"]["rows"] if row["horizon"] == 60]
                self.assertEqual(len(preferred_rows), 2)
                regime_row = next(row for row in preferred_rows if row["baseline_name"] == "best_regime_window")
                self.assertEqual(regime_row["regime_bucket"], "expansion")
                self.assertGreater(regime_row["avg_reject_net_bps"], 0.5)
                self.assertEqual(regime_row["baseline_metric_name"], "avg_reject_net_bps")
                self.assertEqual(regime_row["baseline_avg_net_bps"], regime_row["avg_reject_net_bps"])
                self.assertTrue(baseline_compare["baseline_compare"]["lineage"]["duckdb_path"].endswith("research.duckdb"))

                decision = api_module.decision(id=registry["models"][0]["id"])
                self.assertEqual(decision["model"]["version"], "t9_alpha_guarded")
                self.assertEqual(decision["decision"]["summary"]["verdict"], "challenger_pass")
                self.assertEqual(decision["decision"]["summary"]["metadata_mode"], "explicit")
                self.assertFalse(decision["decision"]["summary"]["requires_model_context_backfill"])
                self.assertEqual(decision["decision"]["summary"]["evaluation_symbol"], "QQQ")
                self.assertEqual(decision["decision"]["summary"]["evaluation_target"], "reject")
                self.assertTrue(decision["decision"]["summary"]["source_duckdb_matches"])
                self.assertTrue(decision["decision"]["summary"]["cost_model_matches"])
                self.assertEqual(decision["decision"]["summary"]["preferred_horizon"], 60)
                self.assertEqual(decision["decision"]["summary"]["peer_rank"], "1/1")
                self.assertEqual(len(decision["decision"]["rules"]), 7)
                self.assertTrue(all(rule["passed"] for rule in decision["decision"]["rules"]))

                review_record = api_module.record_review(
                    api_module.ReviewLogRequest(
                        id=registry["models"][0]["id"],
                        reviewer="unit_test",
                        note="Committee pass recorded during fixture validation.",
                    )
                )
                self.assertEqual(review_record["status"], "ok")
                self.assertEqual(review_record["review"]["decision_summary"]["verdict"], "challenger_pass")
                self.assertTrue(review_record["review"]["entry_hash"])

                review_log = api_module.review_log(id=registry["models"][0]["id"], limit=10)
                self.assertEqual(len(review_log["reviews"]), 1)
                self.assertEqual(review_log["reviews"][0]["reviewer"], "unit_test")
                self.assertEqual(review_log["reviews"][0]["note"], "Committee pass recorded during fixture validation.")
                self.assertEqual(review_log["review_backend"], "sqlite")
                self.assertTrue(review_log["review_log_path"].endswith("review_log.jsonl"))
                self.assertTrue(review_log["review_store_path"].endswith("review_store.sqlite"))

                beta_review = api_module.record_review(
                    api_module.ReviewLogRequest(
                        id=registry["models"][1]["id"],
                        reviewer="beta_reviewer",
                        note="Blocked candidate recorded for comparison.",
                    )
                )
                self.assertEqual(beta_review["review"]["decision_summary"]["verdict"], "blocked")
                self.assertEqual(beta_review["review"]["decision_summary"]["metadata_mode"], "legacy_inferred")
                self.assertTrue(beta_review["review"]["decision_summary"]["requires_model_context_backfill"])

                filtered_log = api_module.review_log(verdict="challenger_pass", limit=10)
                self.assertEqual(len(filtered_log["reviews"]), 1)
                self.assertEqual(filtered_log["reviews"][0]["reviewer"], "unit_test")

                reviewer_log = api_module.review_log(reviewer="beta_", limit=10)
                self.assertEqual(len(reviewer_log["reviews"]), 1)
                self.assertEqual(reviewer_log["reviews"][0]["reviewer"], "beta_reviewer")

                compare_payload = api_module.review_compare(
                    review_id_a=review_record["review"]["review_id"],
                    review_id_b=beta_review["review"]["review_id"],
                )
                self.assertFalse(compare_payload["summary"]["same_model"])
                self.assertTrue(compare_payload["summary"]["verdict_changed"])
                self.assertEqual(compare_payload["rows"][0]["metric"], "Verdict")
                self.assertEqual(compare_payload["rows"][0]["delta"], "changed")

                governance_summary = api_module.governance_summary()
                self.assertEqual(governance_summary["review_entries"], 2)
                self.assertEqual(governance_summary["reviewed_models"], 2)
                self.assertEqual(governance_summary["verdict_counts"]["challenger_pass"], 1)
                self.assertEqual(governance_summary["verdict_counts"]["blocked"], 1)
                self.assertTrue(governance_summary["latest_registry_model"])
                self.assertIn("latest_registry_reviewed", governance_summary)

                governance_workspace = api_module.governance_workspace(limit=10)
                self.assertEqual(governance_workspace["summary"]["queue_count"], 2)
                self.assertEqual(governance_workspace["summary"]["queue_total_count"], 2)
                self.assertEqual(governance_workspace["summary"]["queue_page_count"], 2)
                self.assertEqual(governance_workspace["summary"]["queue_limit"], 10)
                self.assertEqual(governance_workspace["summary"]["challenger_pass_count"], 1)
                self.assertEqual(governance_workspace["summary"]["blocked_count"], 1)
                self.assertEqual(len(governance_workspace["queue"]), 2)
                self.assertEqual(governance_workspace["queue"][0]["version"], "t9_alpha_guarded")
                self.assertEqual(governance_workspace["queue"][0]["review_state"], "challenger_pass")
                self.assertEqual(governance_workspace["queue"][0]["metadata_mode"], "explicit")
                self.assertFalse(governance_workspace["queue"][0]["requires_model_context_backfill"])
                self.assertEqual(governance_workspace["queue"][0]["evaluation_symbol"], "QQQ")
                self.assertEqual(governance_workspace["queue"][0]["evaluation_target"], "reject")
                self.assertTrue(governance_workspace["queue"][0]["source_duckdb_matches"])
                self.assertTrue(governance_workspace["queue"][0]["cost_model_matches"])
                self.assertEqual(governance_workspace["queue"][1]["review_state"], "blocked")
                self.assertEqual(governance_workspace["queue"][1]["metadata_mode"], "legacy_inferred")
                self.assertTrue(governance_workspace["queue"][1]["requires_model_context_backfill"])
                self.assertEqual(len(governance_workspace["latest_reviews"]), 2)
                self.assertTrue(governance_workspace["lineage"]["review_log_path"].endswith("review_log.jsonl"))
                self.assertTrue(governance_workspace["lineage"]["review_store_path"].endswith("review_store.sqlite"))
                self.assertEqual(governance_workspace["lineage"]["review_backend"], "sqlite")
            finally:
                if prev_root is None:
                    os.environ.pop("MODEL_REGISTRY_ROOT", None)
                else:
                    os.environ["MODEL_REGISTRY_ROOT"] = prev_root
                if prev_roots is None:
                    os.environ.pop("MODEL_REGISTRY_ROOTS", None)
                else:
                    os.environ["MODEL_REGISTRY_ROOTS"] = prev_roots
                if prev_duckdb is None:
                    os.environ.pop("DUCKDB_PATH", None)
                else:
                    os.environ["DUCKDB_PATH"] = prev_duckdb
                if prev_review_log is None:
                    os.environ.pop("MODEL_REVIEW_LOG_PATH", None)
                else:
                    os.environ["MODEL_REVIEW_LOG_PATH"] = prev_review_log
                if prev_review_store is None:
                    os.environ.pop("MODEL_REVIEW_DB_PATH", None)
                else:
                    os.environ["MODEL_REVIEW_DB_PATH"] = prev_review_store
                if prev_review_backend is None:
                    os.environ.pop("MODEL_REVIEW_BACKEND", None)
                else:
                    os.environ["MODEL_REVIEW_BACKEND"] = prev_review_backend
                if prev_allow_legacy is None:
                    os.environ.pop("ALLOW_LEGACY_MODEL_METADATA", None)
                else:
                    os.environ["ALLOW_LEGACY_MODEL_METADATA"] = prev_allow_legacy

    def test_governance_workspace_counts_are_global_before_pagination(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            duckdb_path = tmp / "research.duckdb"
            create_research_marts(duckdb_path)

            def write_model(
                family: str,
                version: str,
                trained_end_ts: int,
                utility_avg: float,
                *,
                model_context: dict | None,
            ) -> Path:
                model_dir = tmp / family
                metadata_path = model_dir / "metadata_runtime" / f"metadata_{version}.json"
                payload = {
                    "version": version,
                    "feature_version": "fv",
                    "trained_end_ts": trained_end_ts,
                    "tune_date_range": {
                        "min_event_date_et": "2026-03-13",
                        "max_event_date_et": "2026-03-15",
                    },
                    "calibration": {"reject": {"15": "isotonic"}},
                    "thresholds": {"reject": {"15": 0.84}},
                    "thresholds_meta": {
                        "reject": {
                            "15": {
                                "guard_applied": utility_avg <= 0,
                                "guard_reason": None if utility_avg > 0 else "non_positive_utility",
                                "objective": "utility_bps",
                                "precision": 0.82,
                                "recall": 0.19,
                                "signals": 25,
                                "selected_utility_avg": utility_avg,
                                "selected_utility_sum": utility_avg * 25,
                                "selected_tp_count": 18,
                                "selected_fp_count": 7,
                                "tune_prob_utility_corr_all": 0.12 if utility_avg > 0 else -0.03,
                                "tune_prob_utility_corr_pos": 0.18 if utility_avg > 0 else -0.05,
                                "trade_cost_bps": 1.3,
                            }
                        }
                    },
                    "stats": {
                        "15": {
                            "reject": {
                                "sample_size": 120,
                                "reject_rate": 0.44,
                                "mfe_bps_reject": 10.0,
                                "mfe_bps_reject_other": 4.0,
                                "mae_bps_reject": -5.0,
                                "mae_bps_reject_other": -8.5,
                                "by_regime": {
                                    "compression": {
                                        "sample_size": 80,
                                        "sample_share": 0.67,
                                        "reject_rate": 0.47,
                                        "mfe_bps_reject": 12.0,
                                        "mfe_bps_reject_other": 4.0,
                                        "mae_bps_reject": -4.2,
                                        "mae_bps_reject_other": -8.8,
                                    }
                                },
                            }
                        }
                    },
                    "shadow_policies": {
                        "model_side_margin_v1": {
                            "policy_name": "model_side_margin_v1",
                            "horizon": 15,
                            "scope": "regime_active_only",
                            "percentile_cutoff": 0.7,
                            "reject": {
                                "status": "ok" if utility_avg > 0 else "blocked",
                                "reason": "",
                                "horizon": 15,
                                "emitted_rows": 12,
                                "emitted_positive_rate": 0.6,
                                "emitted_utility_avg": 1.1 if utility_avg > 0 else -0.8,
                                "emitted_utility_sum": 13.2 if utility_avg > 0 else -9.6,
                            },
                        }
                    },
                }
                if model_context is not None:
                    payload["model_context"] = model_context
                write_json(metadata_path, payload)
                write_json(model_dir / "manifest_runtime_latest.json", {"version": version})
                return metadata_path

            explicit_context = {
                "symbols": ["SPY"],
                "default_symbol": "SPY",
                "targets": ["reject"],
                "cost_model_version": "rt_cost_v1",
                "trade_cost_bps": 1.3,
                "training_view": "training_events_v1",
                "source_duckdb_path": str(duckdb_path),
            }

            latest_metadata = write_model(
                "latest_blocked",
                "t9_latest_blocked",
                1714000000000,
                -1.5,
                model_context=explicit_context,
            )
            pending_metadata = write_model(
                "older_pending",
                "t9_older_pending",
                1713000000000,
                1.8,
                model_context=explicit_context,
            )
            reviewed_metadata = write_model(
                "reviewed_pass",
                "t9_reviewed_pass",
                1712000000000,
                1.4,
                model_context=explicit_context,
            )

            prev_root = os.environ.get("MODEL_REGISTRY_ROOT")
            prev_roots = os.environ.get("MODEL_REGISTRY_ROOTS")
            prev_duckdb = os.environ.get("DUCKDB_PATH")
            prev_review_log = os.environ.get("MODEL_REVIEW_LOG_PATH")
            prev_review_store = os.environ.get("MODEL_REVIEW_DB_PATH")
            prev_review_backend = os.environ.get("MODEL_REVIEW_BACKEND")
            prev_allow_legacy = os.environ.get("ALLOW_LEGACY_MODEL_METADATA")
            try:
                os.environ["MODEL_REGISTRY_ROOT"] = str(tmp)
                os.environ.pop("MODEL_REGISTRY_ROOTS", None)
                os.environ["DUCKDB_PATH"] = str(duckdb_path)
                os.environ["MODEL_REVIEW_LOG_PATH"] = str(tmp / "review_log.jsonl")
                os.environ["MODEL_REVIEW_DB_PATH"] = str(tmp / "review_store.sqlite")
                os.environ["MODEL_REVIEW_BACKEND"] = "sqlite"
                os.environ["ALLOW_LEGACY_MODEL_METADATA"] = "1"
                api_module = load_module("model_api_governance_page_test_module", API_PATH)

                registry = api_module.registry(limit=10)
                ids_by_version = {row["version"]: row["id"] for row in registry["models"]}

                blocked_review = api_module.record_review(
                    api_module.ReviewLogRequest(
                        id=ids_by_version["t9_latest_blocked"],
                        reviewer="committee",
                        note="Reviewed blocked candidate.",
                    )
                )
                self.assertEqual(blocked_review["review"]["decision_summary"]["verdict"], "blocked")

                review_record = api_module.record_review(
                    api_module.ReviewLogRequest(
                        id=ids_by_version["t9_reviewed_pass"],
                        reviewer="committee",
                        note="Reviewed pass candidate.",
                    )
                )
                self.assertEqual(review_record["review"]["decision_summary"]["verdict"], "challenger_pass")

                workspace = api_module.governance_workspace(limit=1)
                summary = workspace["summary"]
                self.assertEqual(summary["queue_count"], 3)
                self.assertEqual(summary["queue_total_count"], 3)
                self.assertEqual(summary["queue_page_count"], 1)
                self.assertEqual(summary["queue_limit"], 1)
                self.assertEqual(summary["pending_review_count"], 1)
                self.assertEqual(summary["challenger_pass_count"], 1)
                self.assertEqual(summary["blocked_count"], 1)
                self.assertEqual(len(workspace["queue"]), 1)
                self.assertEqual(workspace["queue"][0]["version"], "t9_older_pending")
                self.assertEqual(workspace["queue"][0]["review_state"], "pending_review")
                self.assertEqual(summary["latest_registry_version"], "t9_latest_blocked")
                self.assertEqual(summary["latest_registry_review_state"], "blocked")
            finally:
                if prev_root is None:
                    os.environ.pop("MODEL_REGISTRY_ROOT", None)
                else:
                    os.environ["MODEL_REGISTRY_ROOT"] = prev_root
                if prev_roots is None:
                    os.environ.pop("MODEL_REGISTRY_ROOTS", None)
                else:
                    os.environ["MODEL_REGISTRY_ROOTS"] = prev_roots
                if prev_duckdb is None:
                    os.environ.pop("DUCKDB_PATH", None)
                else:
                    os.environ["DUCKDB_PATH"] = prev_duckdb
                if prev_review_log is None:
                    os.environ.pop("MODEL_REVIEW_LOG_PATH", None)
                else:
                    os.environ["MODEL_REVIEW_LOG_PATH"] = prev_review_log
                if prev_review_store is None:
                    os.environ.pop("MODEL_REVIEW_DB_PATH", None)
                else:
                    os.environ["MODEL_REVIEW_DB_PATH"] = prev_review_store
                if prev_review_backend is None:
                    os.environ.pop("MODEL_REVIEW_BACKEND", None)
                else:
                    os.environ["MODEL_REVIEW_BACKEND"] = prev_review_backend
                if prev_allow_legacy is None:
                    os.environ.pop("ALLOW_LEGACY_MODEL_METADATA", None)
                else:
                    os.environ["ALLOW_LEGACY_MODEL_METADATA"] = prev_allow_legacy

    def test_legacy_metadata_can_be_disabled_explicitly(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            duckdb_path = tmp / "research.duckdb"
            create_research_marts(duckdb_path)

            legacy_dir = tmp / "legacy_family"
            legacy_metadata = legacy_dir / "metadata_runtime" / "metadata_t9_legacy.json"
            write_json(
                legacy_metadata,
                {
                    "version": "t9_legacy",
                    "feature_version": "fv_legacy",
                    "trained_end_ts": 1712800000000,
                    "tune_date_range": {
                        "min_event_date_et": "2026-03-13",
                        "max_event_date_et": "2026-03-15",
                    },
                    "thresholds": {"reject": {"15": 0.71}},
                    "thresholds_meta": {
                        "reject": {
                            "15": {
                                "guard_applied": False,
                                "guard_reason": None,
                                "objective": "utility_bps",
                                "precision": 0.7,
                                "recall": 0.1,
                                "signals": 20,
                                "selected_utility_avg": 0.5,
                                "selected_utility_sum": 10.0,
                                "selected_tp_count": 14,
                                "selected_fp_count": 6,
                                "tune_prob_utility_corr_all": 0.04,
                                "tune_prob_utility_corr_pos": 0.08,
                            }
                        }
                    },
                },
            )
            write_json(legacy_dir / "manifest_runtime_latest.json", {"version": "t9_legacy"})

            prev_root = os.environ.get("MODEL_REGISTRY_ROOT")
            prev_roots = os.environ.get("MODEL_REGISTRY_ROOTS")
            prev_duckdb = os.environ.get("DUCKDB_PATH")
            prev_review_log = os.environ.get("MODEL_REVIEW_LOG_PATH")
            prev_review_store = os.environ.get("MODEL_REVIEW_DB_PATH")
            prev_review_backend = os.environ.get("MODEL_REVIEW_BACKEND")
            prev_allow_legacy = os.environ.get("ALLOW_LEGACY_MODEL_METADATA")
            try:
                os.environ["MODEL_REGISTRY_ROOT"] = str(tmp)
                os.environ.pop("MODEL_REGISTRY_ROOTS", None)
                os.environ["DUCKDB_PATH"] = str(duckdb_path)
                os.environ["MODEL_REVIEW_LOG_PATH"] = str(tmp / "review_log.jsonl")
                os.environ["MODEL_REVIEW_DB_PATH"] = str(tmp / "review_store.sqlite")
                os.environ["MODEL_REVIEW_BACKEND"] = "sqlite"
                os.environ["ALLOW_LEGACY_MODEL_METADATA"] = "0"
                api_module = load_module("model_api_legacy_disabled_test_module", API_PATH)

                registry = api_module.registry(limit=10)
                self.assertEqual(len(registry["models"]), 1)

                with self.assertRaises(api_module.HTTPException) as exc_info:
                    api_module.baseline_compare(id=registry["models"][0]["id"])
                self.assertEqual(exc_info.exception.status_code, 422)
                self.assertIn("missing model_context", str(exc_info.exception.detail))
            finally:
                if prev_root is None:
                    os.environ.pop("MODEL_REGISTRY_ROOT", None)
                else:
                    os.environ["MODEL_REGISTRY_ROOT"] = prev_root
                if prev_roots is None:
                    os.environ.pop("MODEL_REGISTRY_ROOTS", None)
                else:
                    os.environ["MODEL_REGISTRY_ROOTS"] = prev_roots
                if prev_duckdb is None:
                    os.environ.pop("DUCKDB_PATH", None)
                else:
                    os.environ["DUCKDB_PATH"] = prev_duckdb
                if prev_review_log is None:
                    os.environ.pop("MODEL_REVIEW_LOG_PATH", None)
                else:
                    os.environ["MODEL_REVIEW_LOG_PATH"] = prev_review_log
                if prev_review_store is None:
                    os.environ.pop("MODEL_REVIEW_DB_PATH", None)
                else:
                    os.environ["MODEL_REVIEW_DB_PATH"] = prev_review_store
                if prev_review_backend is None:
                    os.environ.pop("MODEL_REVIEW_BACKEND", None)
                else:
                    os.environ["MODEL_REVIEW_BACKEND"] = prev_review_backend
                if prev_allow_legacy is None:
                    os.environ.pop("ALLOW_LEGACY_MODEL_METADATA", None)
                else:
                    os.environ["ALLOW_LEGACY_MODEL_METADATA"] = prev_allow_legacy

    def test_registry_snapshot_invalidates_on_metadata_and_manifest_changes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            duckdb_path = tmp / "research.duckdb"
            create_research_marts(duckdb_path)

            alpha_dir = tmp / "alpha_family"
            metadata_path = alpha_dir / "metadata_runtime" / "metadata_t9_alpha_guarded.json"
            manifest_path = alpha_dir / "manifest_runtime_latest.json"
            write_json(
                metadata_path,
                {
                    "version": "t9_alpha_guarded",
                    "feature_version": "fv_alpha",
                    "trained_end_ts": 1712966400000,
                    "tune_date_range": {
                        "min_event_date_et": "2026-03-13",
                        "max_event_date_et": "2026-03-25",
                    },
                    "thresholds": {"reject": {"15": 0.84}},
                    "thresholds_meta": {
                        "reject": {
                            "15": {
                                "guard_applied": False,
                                "guard_reason": None,
                                "objective": "utility_bps",
                                "precision": 0.82,
                                "recall": 0.19,
                                "signals": 222,
                                "selected_utility_avg": 1.11,
                                "selected_utility_sum": 246.42,
                                "selected_tp_count": 180,
                                "selected_fp_count": 42,
                                "tune_prob_utility_corr_all": 0.03,
                                "tune_prob_utility_corr_pos": 0.08,
                                "trade_cost_bps": 1.3,
                            }
                        }
                    },
                    "model_context": {
                        "symbols": ["SPY"],
                        "default_symbol": "SPY",
                        "targets": ["reject"],
                        "cost_model_version": "rt_cost_v1",
                        "trade_cost_bps": 1.3,
                        "training_view": "training_events_v1",
                        "source_duckdb_path": str(duckdb_path),
                    },
                },
            )
            write_json(manifest_path, {"version": "t9_alpha_guarded"})

            prev_root = os.environ.get("MODEL_REGISTRY_ROOT")
            prev_roots = os.environ.get("MODEL_REGISTRY_ROOTS")
            prev_duckdb = os.environ.get("DUCKDB_PATH")
            prev_review_log = os.environ.get("MODEL_REVIEW_LOG_PATH")
            prev_review_store = os.environ.get("MODEL_REVIEW_DB_PATH")
            prev_review_backend = os.environ.get("MODEL_REVIEW_BACKEND")
            prev_allow_legacy = os.environ.get("ALLOW_LEGACY_MODEL_METADATA")
            try:
                os.environ["MODEL_REGISTRY_ROOT"] = str(tmp)
                os.environ.pop("MODEL_REGISTRY_ROOTS", None)
                os.environ["DUCKDB_PATH"] = str(duckdb_path)
                os.environ["MODEL_REVIEW_LOG_PATH"] = str(tmp / "review_log.jsonl")
                os.environ["MODEL_REVIEW_DB_PATH"] = str(tmp / "review_store.sqlite")
                os.environ["MODEL_REVIEW_BACKEND"] = "sqlite"
                os.environ["ALLOW_LEGACY_MODEL_METADATA"] = "1"
                api_module = load_module("model_api_snapshot_test_module", API_PATH)

                registry_before = api_module.registry(limit=10)
                self.assertEqual(registry_before["models"][0]["version"], "t9_alpha_guarded")
                self.assertTrue(registry_before["models"][0]["is_family_latest"])

                time.sleep(0.01)
                write_json(
                    metadata_path,
                    {
                        **json.loads(metadata_path.read_text(encoding="utf-8")),
                        "version": "t9_alpha_guarded_v2",
                    },
                )

                registry_after_metadata = api_module.registry(limit=10)
                self.assertEqual(registry_after_metadata["models"][0]["version"], "t9_alpha_guarded_v2")
                self.assertFalse(registry_after_metadata["models"][0]["is_family_latest"])

                time.sleep(0.01)
                write_json(manifest_path, {"version": "t9_alpha_guarded_v2"})

                registry_after_manifest = api_module.registry(limit=10)
                self.assertEqual(registry_after_manifest["models"][0]["version"], "t9_alpha_guarded_v2")
                self.assertTrue(registry_after_manifest["models"][0]["is_family_latest"])
            finally:
                if prev_root is None:
                    os.environ.pop("MODEL_REGISTRY_ROOT", None)
                else:
                    os.environ["MODEL_REGISTRY_ROOT"] = prev_root
                if prev_roots is None:
                    os.environ.pop("MODEL_REGISTRY_ROOTS", None)
                else:
                    os.environ["MODEL_REGISTRY_ROOTS"] = prev_roots
                if prev_duckdb is None:
                    os.environ.pop("DUCKDB_PATH", None)
                else:
                    os.environ["DUCKDB_PATH"] = prev_duckdb
                if prev_review_log is None:
                    os.environ.pop("MODEL_REVIEW_LOG_PATH", None)
                else:
                    os.environ["MODEL_REVIEW_LOG_PATH"] = prev_review_log
                if prev_review_store is None:
                    os.environ.pop("MODEL_REVIEW_DB_PATH", None)
                else:
                    os.environ["MODEL_REVIEW_DB_PATH"] = prev_review_store
                if prev_review_backend is None:
                    os.environ.pop("MODEL_REVIEW_BACKEND", None)
                else:
                    os.environ["MODEL_REVIEW_BACKEND"] = prev_review_backend
                if prev_allow_legacy is None:
                    os.environ.pop("ALLOW_LEGACY_MODEL_METADATA", None)
                else:
                    os.environ["ALLOW_LEGACY_MODEL_METADATA"] = prev_allow_legacy

    def test_break_target_uses_break_baseline_metrics(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            duckdb_path = tmp / "research.duckdb"
            create_research_marts(duckdb_path)

            break_dir = tmp / "break_family"
            break_metadata = break_dir / "metadata_runtime" / "metadata_t9_break_candidate.json"
            write_json(
                break_metadata,
                {
                    "version": "t9_break_candidate",
                    "feature_version": "fv_break",
                    "trained_end_ts": 1712990000000,
                    "tune_date_range": {
                        "min_event_date_et": "2026-03-13",
                        "max_event_date_et": "2026-03-15",
                    },
                    "calibration": {"break": {"60": "isotonic"}},
                    "thresholds": {"break": {"60": 0.41}},
                    "stats": {
                        "60": {
                            "break": {
                                "sample_size": 420,
                                "break_rate": 0.43,
                                "mfe_bps_break": 8.9,
                                "mfe_bps_break_other": 6.2,
                                "mae_bps_break": -5.4,
                                "mae_bps_break_other": -7.8,
                                "by_regime": {
                                    "expansion": {
                                        "sample_size": 210,
                                        "sample_share": 0.5,
                                        "break_rate": 0.48,
                                        "mfe_bps_break": 9.7,
                                        "mfe_bps_break_other": 5.8,
                                        "mae_bps_break": -4.9,
                                        "mae_bps_break_other": -8.1
                                    }
                                }
                            }
                        }
                    },
                    "model_context": {
                        "symbols": ["QQQ"],
                        "default_symbol": "QQQ",
                        "targets": ["break"],
                        "cost_model_version": "rt_cost_v1",
                        "trade_cost_bps": 1.3,
                        "training_view": "training_events_v1",
                        "source_duckdb_path": str(duckdb_path),
                    },
                    "thresholds_meta": {
                        "break": {
                            "60": {
                                "guard_applied": False,
                                "guard_reason": None,
                                "objective": "utility_bps",
                                "precision": 0.71,
                                "recall": 0.16,
                                "signals": 44,
                                "selected_utility_avg": 0.35,
                                "selected_utility_sum": 15.4,
                                "selected_tp_count": 29,
                                "selected_fp_count": 15,
                                "tune_prob_utility_corr_all": 0.06,
                                "tune_prob_utility_corr_pos": 0.09,
                            }
                        }
                    },
                },
            )
            write_json(break_dir / "manifest_runtime_latest.json", {"version": "t9_break_candidate"})

            prev_root = os.environ.get("MODEL_REGISTRY_ROOT")
            prev_roots = os.environ.get("MODEL_REGISTRY_ROOTS")
            prev_duckdb = os.environ.get("DUCKDB_PATH")
            prev_review_log = os.environ.get("MODEL_REVIEW_LOG_PATH")
            prev_review_store = os.environ.get("MODEL_REVIEW_DB_PATH")
            prev_review_backend = os.environ.get("MODEL_REVIEW_BACKEND")
            prev_allow_legacy = os.environ.get("ALLOW_LEGACY_MODEL_METADATA")
            try:
                os.environ["MODEL_REGISTRY_ROOT"] = str(tmp)
                os.environ.pop("MODEL_REGISTRY_ROOTS", None)
                os.environ["DUCKDB_PATH"] = str(duckdb_path)
                os.environ["MODEL_REVIEW_LOG_PATH"] = str(tmp / "review_log.jsonl")
                os.environ["MODEL_REVIEW_DB_PATH"] = str(tmp / "review_store.sqlite")
                os.environ["MODEL_REVIEW_BACKEND"] = "sqlite"
                os.environ["ALLOW_LEGACY_MODEL_METADATA"] = "1"
                api_module = load_module("model_api_break_target_test_module", API_PATH)

                registry = api_module.registry(limit=10)
                baseline_compare = api_module.baseline_compare(id=registry["models"][0]["id"])
                summary = baseline_compare["baseline_compare"]["summary"]
                rows = baseline_compare["baseline_compare"]["rows"]

                self.assertEqual(summary["symbol"], "QQQ")
                self.assertEqual(summary["preferred_target"], "break")
                self.assertFalse(summary["requires_model_context_backfill"])
                self.assertTrue(summary["source_duckdb_matches"])
                self.assertTrue(summary["cost_model_matches"])
                self.assertIsNone(summary["preferred_all_regimes_avg_reject_net_bps"])
                self.assertGreater(summary["preferred_all_regimes_avg_net_bps"], -0.5)
                self.assertLess(summary["preferred_regime_avg_net_bps"], 0.0)
                self.assertEqual(rows[0]["baseline_metric_name"], "avg_break_net_bps")

                decision = api_module.decision(id=registry["models"][0]["id"])
                self.assertEqual(decision["decision"]["summary"]["evaluation_target"], "break")
                self.assertEqual(decision["decision"]["summary"]["evaluation_symbol"], "QQQ")
                self.assertFalse(decision["decision"]["summary"]["requires_model_context_backfill"])

                first_review = api_module.record_review(
                    api_module.ReviewLogRequest(
                        id=registry["models"][0]["id"],
                        reviewer="break_one",
                        note="First break review.",
                    )
                )
                second_review = api_module.record_review(
                    api_module.ReviewLogRequest(
                        id=registry["models"][0]["id"],
                        reviewer="break_two",
                        note="Second break review.",
                    )
                )
                compare_payload = api_module.review_compare(
                    review_id_a=first_review["review"]["review_id"],
                    review_id_b=second_review["review"]["review_id"],
                )
                preferred_baseline_row = next(
                    row for row in compare_payload["rows"] if row["metric"] == "Preferred Regime Baseline"
                )
                self.assertEqual(
                    preferred_baseline_row["value_a"],
                    first_review["review"]["baseline_summary"]["preferred_regime_avg_net_bps"],
                )
                self.assertEqual(
                    preferred_baseline_row["value_b"],
                    second_review["review"]["baseline_summary"]["preferred_regime_avg_net_bps"],
                )
            finally:
                if prev_root is None:
                    os.environ.pop("MODEL_REGISTRY_ROOT", None)
                else:
                    os.environ["MODEL_REGISTRY_ROOT"] = prev_root
                if prev_roots is None:
                    os.environ.pop("MODEL_REGISTRY_ROOTS", None)
                else:
                    os.environ["MODEL_REGISTRY_ROOTS"] = prev_roots
                if prev_duckdb is None:
                    os.environ.pop("DUCKDB_PATH", None)
                else:
                    os.environ["DUCKDB_PATH"] = prev_duckdb
                if prev_review_log is None:
                    os.environ.pop("MODEL_REVIEW_LOG_PATH", None)
                else:
                    os.environ["MODEL_REVIEW_LOG_PATH"] = prev_review_log
                if prev_review_store is None:
                    os.environ.pop("MODEL_REVIEW_DB_PATH", None)
                else:
                    os.environ["MODEL_REVIEW_DB_PATH"] = prev_review_store
                if prev_review_backend is None:
                    os.environ.pop("MODEL_REVIEW_BACKEND", None)
                else:
                    os.environ["MODEL_REVIEW_BACKEND"] = prev_review_backend
                if prev_allow_legacy is None:
                    os.environ.pop("ALLOW_LEGACY_MODEL_METADATA", None)
                else:
                    os.environ["ALLOW_LEGACY_MODEL_METADATA"] = prev_allow_legacy

    def test_sqlite_review_store_migrates_legacy_jsonl_and_verifies_chain(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            review_log_path = tmp / "review_log.jsonl"
            review_store_path = tmp / "review_store.sqlite"
            store_module = load_module("model_review_store_test_module", REVIEW_STORE_MODULE_PATH)

            entry_a = {
                "recorded_at_utc": "2026-04-13T00:00:00Z",
                "reviewer": "reviewer_a",
                "note": "First review",
                "model": {"id": "alpha", "version": "alpha_v1"},
                "decision_summary": {"verdict": "challenger_pass"},
                "benchmark_summary": {"registry_rank": 1},
                "baseline_summary": {"preferred_regime_avg_reject_net_bps": 0.5},
                "research_handoff": {},
                "raw_paths": {},
                "lineage": {},
                "prev_hash": "",
            }
            entry_a_hash = store_module.hashlib.sha256(store_module.canonical_json(entry_a).encode("utf-8")).hexdigest()
            entry_a_full = {**entry_a, "review_id": "r1", "entry_hash": entry_a_hash}

            entry_b = {
                "recorded_at_utc": "2026-04-13T01:00:00Z",
                "reviewer": "reviewer_b",
                "note": "Second review",
                "model": {"id": "beta", "version": "beta_v1"},
                "decision_summary": {"verdict": "blocked"},
                "benchmark_summary": {"registry_rank": 2},
                "baseline_summary": {"preferred_regime_avg_reject_net_bps": -0.2},
                "research_handoff": {},
                "raw_paths": {},
                "lineage": {},
                "prev_hash": entry_a_hash,
            }
            entry_b_hash = store_module.hashlib.sha256(store_module.canonical_json(entry_b).encode("utf-8")).hexdigest()
            entry_b_full = {**entry_b, "review_id": "r2", "entry_hash": entry_b_hash}

            review_log_path.write_text(
                json.dumps(entry_a_full, ensure_ascii=True) + "\n" + json.dumps(entry_b_full, ensure_ascii=True) + "\n",
                encoding="utf-8",
            )

            migration = store_module.migrate_jsonl_to_sqlite(review_log_path, review_store_path)
            self.assertEqual(migration["source_entries"], 2)
            self.assertEqual(migration["imported"], 2)
            self.assertEqual(migration["store_entries"], 2)

            verification = store_module.verify_hash_chain(review_store_path)
            self.assertTrue(verification["valid"])
            self.assertEqual(verification["count"], 2)

            reviews = store_module.list_reviews(review_store_path)
            self.assertEqual(len(reviews), 2)
            self.assertEqual(reviews[0]["review_id"], "r1")
            self.assertEqual(reviews[1]["review_id"], "r2")

    def test_sqlite_backend_bootstraps_legacy_jsonl_history(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            duckdb_path = tmp / "research.duckdb"
            create_research_marts(duckdb_path)

            alpha_dir = tmp / "alpha_family"
            alpha_metadata = alpha_dir / "metadata_runtime" / "metadata_t9_alpha_guarded.json"
            write_json(
                alpha_metadata,
                {
                    "version": "t9_alpha_guarded",
                    "feature_version": "fv_alpha",
                    "trained_end_ts": 1712966400000,
                    "tune_date_range": {
                        "min_event_date_et": "2026-03-13",
                        "max_event_date_et": "2026-03-25",
                    },
                    "thresholds": {"reject": {"60": 0.91}},
                    "model_context": {
                        "symbols": ["QQQ"],
                        "default_symbol": "QQQ",
                        "targets": ["reject"],
                        "cost_model_version": "rt_cost_v1",
                        "trade_cost_bps": 1.3,
                        "training_view": "training_events_v1",
                        "source_duckdb_path": str(duckdb_path),
                    },
                    "thresholds_meta": {
                        "reject": {
                            "60": {
                                "guard_applied": False,
                                "guard_reason": None,
                                "objective": "utility_bps",
                                "precision": 0.77,
                                "recall": 0.11,
                                "signals": 74,
                                "selected_utility_avg": 1.92,
                                "selected_utility_sum": 142.08,
                                "selected_tp_count": 51,
                                "selected_fp_count": 23,
                                "tune_prob_utility_corr_all": 0.12,
                                "tune_prob_utility_corr_pos": 0.19,
                            }
                        }
                    },
                },
            )
            write_json(alpha_dir / "manifest_runtime_latest.json", {"version": "t9_alpha_guarded"})

            review_log_path = tmp / "review_log.jsonl"
            legacy_entry_body = {
                "recorded_at_utc": "2026-04-13T00:00:00Z",
                "reviewer": "legacy_reviewer",
                "note": "Legacy review before sqlite migration.",
                "model": {"id": "alpha-placeholder", "version": "legacy_v1"},
                "decision_summary": {"verdict": "research_only"},
                "benchmark_summary": {"registry_rank": 1},
                "baseline_summary": {"preferred_regime_avg_reject_net_bps": 0.1},
                "research_handoff": {},
                "raw_paths": {},
                "lineage": {},
                "prev_hash": "",
            }
            store_module = load_module("model_review_store_bootstrap_test_module", REVIEW_STORE_MODULE_PATH)
            legacy_hash = store_module.hashlib.sha256(store_module.canonical_json(legacy_entry_body).encode("utf-8")).hexdigest()
            review_log_path.write_text(
                json.dumps({**legacy_entry_body, "review_id": "legacy_r1", "entry_hash": legacy_hash}, ensure_ascii=True) + "\n",
                encoding="utf-8",
            )

            prev_root = os.environ.get("MODEL_REGISTRY_ROOT")
            prev_roots = os.environ.get("MODEL_REGISTRY_ROOTS")
            prev_duckdb = os.environ.get("DUCKDB_PATH")
            prev_review_log = os.environ.get("MODEL_REVIEW_LOG_PATH")
            prev_review_store = os.environ.get("MODEL_REVIEW_DB_PATH")
            prev_review_backend = os.environ.get("MODEL_REVIEW_BACKEND")
            prev_allow_legacy = os.environ.get("ALLOW_LEGACY_MODEL_METADATA")
            try:
                os.environ["MODEL_REGISTRY_ROOT"] = str(tmp)
                os.environ.pop("MODEL_REGISTRY_ROOTS", None)
                os.environ["DUCKDB_PATH"] = str(duckdb_path)
                os.environ["MODEL_REVIEW_LOG_PATH"] = str(review_log_path)
                os.environ["MODEL_REVIEW_DB_PATH"] = str(tmp / "review_store.sqlite")
                os.environ["MODEL_REVIEW_BACKEND"] = "sqlite"
                os.environ["ALLOW_LEGACY_MODEL_METADATA"] = "1"
                api_module = load_module("model_api_bootstrap_test_module", API_PATH)

                log_payload = api_module.review_log(limit=10)
                self.assertEqual(log_payload["review_backend"], "sqlite")
                self.assertEqual(len(log_payload["reviews"]), 1)
                self.assertEqual(log_payload["reviews"][0]["review_id"], "legacy_r1")
                self.assertTrue(Path(log_payload["review_store_path"]).exists())
            finally:
                if prev_root is None:
                    os.environ.pop("MODEL_REGISTRY_ROOT", None)
                else:
                    os.environ["MODEL_REGISTRY_ROOT"] = prev_root
                if prev_roots is None:
                    os.environ.pop("MODEL_REGISTRY_ROOTS", None)
                else:
                    os.environ["MODEL_REGISTRY_ROOTS"] = prev_roots
                if prev_duckdb is None:
                    os.environ.pop("DUCKDB_PATH", None)
                else:
                    os.environ["DUCKDB_PATH"] = prev_duckdb
                if prev_review_log is None:
                    os.environ.pop("MODEL_REVIEW_LOG_PATH", None)
                else:
                    os.environ["MODEL_REVIEW_LOG_PATH"] = prev_review_log
                if prev_review_store is None:
                    os.environ.pop("MODEL_REVIEW_DB_PATH", None)
                else:
                    os.environ["MODEL_REVIEW_DB_PATH"] = prev_review_store
                if prev_review_backend is None:
                    os.environ.pop("MODEL_REVIEW_BACKEND", None)
                else:
                    os.environ["MODEL_REVIEW_BACKEND"] = prev_review_backend
                if prev_allow_legacy is None:
                    os.environ.pop("ALLOW_LEGACY_MODEL_METADATA", None)
                else:
                    os.environ["ALLOW_LEGACY_MODEL_METADATA"] = prev_allow_legacy


if __name__ == "__main__":
    unittest.main()
