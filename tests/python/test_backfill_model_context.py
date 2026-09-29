from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = ROOT / "scripts" / "backfill_model_context.py"


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


class BackfillModelContextTest(unittest.TestCase):
    def test_backfill_adds_model_context_to_metadata_and_manifest(self):
        module = load_module("backfill_model_context_test_module", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            registry_root = tmp / "registry"
            artifact_dir = registry_root / "full"
            metadata_path = artifact_dir / "metadata_runtime" / "metadata_t9_full_reject_1560.json"
            manifest_path = artifact_dir / "manifest_runtime_latest.json"
            write_json(
                metadata_path,
                {
                    "version": "t9_full_reject_1560",
                    "thresholds": {"reject": {"15": 0.88}},
                    "thresholds_meta": {"reject": {"15": {"trade_cost_bps": 1.3}}},
                    "stats": {"15": {"reject": {"sample_size": 10}}},
                },
            )
            write_json(
                manifest_path,
                {
                    "version": "t9_full_reject_1560",
                    "thresholds": {"reject": {"15": 0.88}},
                    "thresholds_meta": {"reject": {"15": {"trade_cost_bps": 1.3}}},
                },
            )
            lineage_path = tmp / "last_build.json"
            write_json(
                lineage_path,
                {
                    "duckdb_path": str(tmp / "research.duckdb"),
                    "source_db": str(tmp / "t9_pivot_events_spy_ivol_expanded.sqlite"),
                    "cost_model": {"version": "rt_cost_v1", "trade_cost_bps": 1.3},
                    "symbols": ["SPX", "SPY"],
                },
            )
            cost_models_path = tmp / "cost_models.json"
            write_json(
                cost_models_path,
                {
                    "rt_cost_v1": {
                        "trade_cost_bps": 1.3,
                        "spread_bps": 0.8,
                        "slippage_bps": 0.4,
                        "commission_bps": 0.1,
                    }
                },
            )

            summary = module.backfill_registry(
                registry_root=registry_root,
                lineage_path=lineage_path,
                cost_models_path=cost_models_path,
                default_symbol="SPY",
                training_view="training_events_v1",
                write=True,
            )

            self.assertEqual(summary["failed"], [])
            self.assertEqual(len(summary["updated"]), 1)
            metadata_payload = json.loads(metadata_path.read_text(encoding="utf-8"))
            manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            for payload in (metadata_payload, manifest_payload):
                self.assertEqual(payload["model_context"]["symbols"], ["SPY"])
                self.assertEqual(payload["model_context"]["default_symbol"], "SPY")
                self.assertEqual(payload["model_context"]["targets"], ["reject"])
                self.assertEqual(payload["model_context"]["cost_model_version"], "rt_cost_v1")
                self.assertEqual(payload["model_context"]["trade_cost_bps"], 1.3)
                self.assertEqual(payload["model_context"]["training_view"], "training_events_v1")
                self.assertEqual(payload["model_context"]["source_duckdb_path"], str(tmp / "research.duckdb"))

    def test_backfill_fails_on_inconsistent_trade_costs(self):
        module = load_module("backfill_model_context_test_module_mismatch", SCRIPT_PATH)
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            registry_root = tmp / "registry"
            artifact_dir = registry_root / "full"
            metadata_path = artifact_dir / "metadata_runtime" / "metadata_t9_full_reject_1560.json"
            manifest_path = artifact_dir / "manifest_runtime_latest.json"
            legacy_payload = {
                "version": "t9_full_reject_1560",
                "thresholds_meta": {
                    "reject": {
                        "15": {"trade_cost_bps": 1.3},
                        "60": {"trade_cost_bps": 1.6},
                    }
                },
                "thresholds": {"reject": {"15": 0.88, "60": 0.92}},
            }
            write_json(metadata_path, legacy_payload)
            write_json(manifest_path, legacy_payload)
            lineage_path = tmp / "last_build.json"
            write_json(
                lineage_path,
                {
                    "duckdb_path": str(tmp / "research.duckdb"),
                    "source_db": str(tmp / "t9_pivot_events_spy_ivol_expanded.sqlite"),
                    "cost_model": {"version": "rt_cost_v1", "trade_cost_bps": 1.3},
                },
            )
            cost_models_path = tmp / "cost_models.json"
            write_json(
                cost_models_path,
                {
                    "rt_cost_v1": {
                        "trade_cost_bps": 1.3,
                        "spread_bps": 0.8,
                        "slippage_bps": 0.4,
                        "commission_bps": 0.1,
                    }
                },
            )

            summary = module.backfill_registry(
                registry_root=registry_root,
                lineage_path=lineage_path,
                cost_models_path=cost_models_path,
                default_symbol="SPY",
                training_view="training_events_v1",
                write=True,
            )

            self.assertEqual(summary["updated"], [])
            self.assertEqual(len(summary["failed"]), 1)
            self.assertIn("Inconsistent trade_cost_bps values", summary["failed"][0]["error"])
            metadata_payload = json.loads(metadata_path.read_text(encoding="utf-8"))
            self.assertNotIn("model_context", metadata_payload)


if __name__ == "__main__":
    unittest.main()
