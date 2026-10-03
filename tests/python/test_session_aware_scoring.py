from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import time
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

import numpy as np


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def load_module(module_name: str, module_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to load module spec for {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


ET = ZoneInfo("America/New_York")


def event_ts(year: int, month: int, day: int, hour: int, minute: int) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=ET).timestamp() * 1000)


class CountingModel:
    classes_ = np.array([0, 1])

    def __init__(self, probability: float) -> None:
        self.probability = probability
        self.calls = 0

    def predict_proba(self, _df):
        self.calls += 1
        return np.array([[1.0 - self.probability, self.probability]], dtype=float)


class TestSessionAwareScoring(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.ml_server = load_module(
            "ml_server_session_aware_runtime", ROOT / "server" / "ml_server.py"
        )

    def setUp(self) -> None:
        ml_server = self.ml_server
        ml_server.collect_missing = lambda _event: []
        ml_server.ML_SHADOW_HORIZONS = set()
        ml_server._SCORE_LOAD_SHED_LOCAL.disable_analogs = True
        self.models: dict[tuple[str, int], CountingModel] = {}
        registry_models = {"reject": {}, "break": {}}
        for target in ("reject", "break"):
            for horizon in (5, 15, 30, 60):
                model = CountingModel(0.8 if target == "reject" else 0.1)
                self.models[(target, horizon)] = model
                registry_models[target][horizon] = {
                    "feature_columns": ["event_hour_et"],
                    "pipeline": model,
                    "calibration": "sigmoid",
                }
        ml_server.registry.models = registry_models
        ml_server.registry.thresholds = {
            "reject": {horizon: 0.5 for horizon in (5, 15, 30, 60)},
            "break": {horizon: 0.5 for horizon in (5, 15, 30, 60)},
        }
        ml_server.registry.manifest = {
            "version": "session-aware-test",
            "trained_end_ts": int(time.time() * 1000),
        }

    def assert_model_calls(self, expected_horizons: set[int]) -> None:
        for target in ("reject", "break"):
            for horizon in (5, 15, 30, 60):
                expected = 1 if horizon in expected_horizons else 0
                self.assertEqual(
                    self.models[(target, horizon)].calls,
                    expected,
                    msg=f"unexpected {target} {horizon}m predict_proba calls",
                )

    def assert_invalid_timestamp_abstention(self, event: dict) -> None:
        ml_server = self.ml_server
        ml_server._SCORE_LOAD_SHED_LOCAL.disable_analogs = False
        with (
            mock.patch.object(ml_server.registry, "snapshot", wraps=ml_server.registry.snapshot) as snapshot,
            mock.patch.object(
                ml_server.analog_engine,
                "score_event",
                wraps=ml_server.analog_engine.score_event,
            ) as analog_score,
        ):
            result = ml_server._score_event(event)

        snapshot.assert_not_called()
        analog_score.assert_not_called()
        self.assert_model_calls(set())
        self.assertEqual(result["status"], "abstained")
        self.assertTrue(result["abstain"])
        self.assertIsNone(result["best_horizon"])
        self.assertEqual(
            result["abstain_reason"], "invalid_or_missing_event_timestamp"
        )
        self.assertEqual(result["scores"], {})
        self.assertEqual(result["signals"], {})
        feasibility = result["session_feasibility"]
        self.assertEqual(feasibility["status"], "abstained")
        self.assertEqual(
            feasibility["reason"], "invalid_or_missing_event_timestamp"
        )
        self.assertIsNone(feasibility["session_close_et"])
        for horizon in (5, 15, 30, 60):
            payload = feasibility["horizons"][str(horizon)]
            self.assertEqual(payload["status"], "abstained")
            self.assertEqual(
                payload["reason"], "invalid_or_missing_event_timestamp"
            )
            self.assertNotIn("horizon_end_et", payload)

    def test_score_route_abstains_before_registry_access_for_invalid_timestamp(self):
        ml_server = self.ml_server

        class Request:
            headers: dict[str, str] = {}

            async def body(self) -> bytes:
                return json.dumps({"event": {"ts_event": "not-a-timestamp"}}).encode()

        with (
            mock.patch.object(ml_server.registry, "snapshot") as snapshot,
            mock.patch.object(ml_server.serving_state, "is_active", return_value=True),
        ):
            loop = asyncio.new_event_loop()
            try:
                response = loop.run_until_complete(ml_server.score(Request()))
            finally:
                loop.close()

        snapshot.assert_not_called()
        result = json.loads(response.body)
        self.assertEqual(result["status"], "abstained")
        self.assertEqual(
            result["abstain_reason"], "invalid_or_missing_event_timestamp"
        )
        self.assertEqual(result["scores"], {})
        self.assertEqual(result["signals"], {})

    def test_mixed_batch_does_not_rescore_or_log_invalid_timestamp(self):
        ml_server = self.ml_server
        invalid_event = {"event_id": "invalid", "ts_event": "not-a-timestamp"}
        valid_event = {
            "event_id": "valid",
            "ts_event": event_ts(2026, 10, 2, 10, 0),
        }

        class Request:
            headers: dict[str, str] = {}

            async def body(self) -> bytes:
                return json.dumps({"events": [invalid_event, valid_event]}).encode()

        call_order: list[str] = []
        original_score_event = ml_server._score_event

        def tracked_score_event(event):
            call_order.append(f"score:{event['event_id']}")
            return original_score_event(event)

        def tracked_snapshot():
            call_order.append("snapshot")
            return {
                "manifest": ml_server.registry.manifest,
                "models": ml_server.registry.models,
            }

        with (
            mock.patch.object(ml_server, "_score_event", side_effect=tracked_score_event),
            mock.patch.object(ml_server.registry, "snapshot", side_effect=tracked_snapshot),
            mock.patch.object(ml_server.serving_state, "is_active", return_value=True),
            mock.patch.object(ml_server, "_enqueue_prediction") as enqueue_prediction,
        ):
            loop = asyncio.new_event_loop()
            try:
                response = loop.run_until_complete(ml_server.score(Request()))
            finally:
                loop.close()

        result = json.loads(response.body)
        self.assertEqual(call_order.count("score:invalid"), 1)
        self.assertLess(call_order.index("score:invalid"), call_order.index("snapshot"))
        enqueue_prediction.assert_called_once()
        self.assertEqual(enqueue_prediction.call_args.args[0], valid_event)
        self.assertEqual(
            result["results"][0]["abstain_reason"],
            "invalid_or_missing_event_timestamp",
        )
        self.assertFalse(result["results"][1]["abstain"])

    def test_mixed_batch_preserves_invalid_abstention_when_models_unavailable(self):
        ml_server = self.ml_server
        invalid_event = {"event_id": "invalid", "ts_event": "not-a-timestamp"}
        valid_event = {
            "event_id": "valid",
            "ts_event": event_ts(2026, 10, 2, 10, 0),
        }

        class Request:
            headers: dict[str, str] = {}

            async def body(self) -> bytes:
                return json.dumps({"events": [invalid_event, valid_event]}).encode()

        with (
            mock.patch.object(
                ml_server.registry,
                "snapshot",
                return_value={"manifest": None, "models": {}},
            ),
            mock.patch.object(ml_server, "_enqueue_prediction") as enqueue_prediction,
        ):
            loop = asyncio.new_event_loop()
            try:
                response = loop.run_until_complete(ml_server.score(Request()))
            finally:
                loop.close()

        self.assertEqual(response.status_code, 503)
        result = json.loads(response.body)
        self.assertEqual(result["detail"], "Models not loaded. Train artifacts first.")
        self.assertEqual(
            result["results"][0]["abstain_reason"],
            "invalid_or_missing_event_timestamp",
        )
        self.assertEqual(result["results"][1]["status"], "unavailable")
        enqueue_prediction.assert_not_called()

    def test_mixed_batch_dormant_audit_counts_only_blocked_valid_events(self):
        ml_server = self.ml_server
        invalid_event = {"event_id": "invalid", "ts_event": "not-a-timestamp"}
        valid_event = {
            "event_id": "valid",
            "ts_event": event_ts(2026, 10, 2, 10, 0),
        }

        class Request:
            headers: dict[str, str] = {}

            async def body(self) -> bytes:
                return json.dumps({"events": [invalid_event, valid_event]}).encode()

        with (
            mock.patch.object(ml_server.serving_state, "is_active", return_value=False),
            mock.patch.object(
                ml_server.serving_state_observability,
                "record_dormant_block",
                return_value=(1, True),
            ),
            mock.patch.object(ml_server, "_emit_predict_blocked_dormant_event") as emit,
        ):
            loop = asyncio.new_event_loop()
            try:
                response = loop.run_until_complete(ml_server.score(Request()))
            finally:
                loop.close()

        result = json.loads(response.body)
        self.assertEqual(
            result["results"][0]["abstain_reason"],
            "invalid_or_missing_event_timestamp",
        )
        self.assertEqual(
            result["results"][1]["blocked_reason"], "serving_dormant"
        )
        self.assertEqual(emit.call_args.kwargs["event_count"], 1)

    def test_batch_helper_preflights_invalid_timestamp_without_enqueue(self):
        ml_server = self.ml_server
        invalid_event = {"event_id": "invalid", "ts_event": "not-a-timestamp"}
        valid_event = {
            "event_id": "valid",
            "ts_event": event_ts(2026, 10, 2, 10, 0),
        }

        with mock.patch.object(ml_server, "_enqueue_prediction") as enqueue_prediction:
            results = ml_server._score_events_batch([invalid_event, valid_event])

        self.assertEqual(
            results[0]["abstain_reason"], "invalid_or_missing_event_timestamp"
        )
        enqueue_prediction.assert_called_once()
        self.assertEqual(enqueue_prediction.call_args.args[0], valid_event)

    def test_missing_timestamp_abstains_without_scoring_any_model(self) -> None:
        self.assert_invalid_timestamp_abstention({"event_id": "missing-ts"})

    def test_malformed_timestamp_abstains_without_scoring_any_model(self) -> None:
        for ts_event in ("not-a-timestamp", "1700000000000", 1_700_000_000):
            with self.subTest(ts_event=ts_event):
                self.assert_invalid_timestamp_abstention(
                    {"event_id": "malformed-ts", "ts_event": ts_event}
                )

    def test_nonfinite_and_out_of_range_timestamps_abstain_before_downstream_calls(self) -> None:
        for ts_event in (
            float("nan"),
            float("inf"),
            float("-inf"),
            10**30,
            253_339_628_400_000,
        ):
            with self.subTest(ts_event=ts_event):
                self.assert_invalid_timestamp_abstention(
                    {"event_id": "invalid-ts", "ts_event": ts_event}
                )

    def test_regular_near_close_scores_only_horizon_ending_by_close(self) -> None:
        result = self.ml_server._score_event(
            {"event_id": "regular-near-close", "ts_event": event_ts(2026, 10, 2, 15, 50)}
        )

        self.assert_model_calls({5})
        self.assertEqual(result["best_horizon"], 5)
        self.assertFalse(result["abstain"])
        self.assertEqual(result["session_feasibility"]["status"], "partial")
        self.assertEqual(
            result["session_feasibility"]["horizons"]["5"]["status"], "scoreable"
        )
        for horizon in (15, 30, 60):
            payload = result["session_feasibility"]["horizons"][str(horizon)]
            self.assertEqual(payload["status"], "abstained")
            self.assertEqual(payload["reason"], "horizon_past_session_close")
            self.assertNotIn(f"prob_reject_{horizon}m", result["scores"])
            self.assertNotIn(f"prob_break_{horizon}m", result["scores"])
            self.assertNotIn(f"signal_{horizon}m", result["signals"])

    def test_known_2026_early_close_suppresses_horizons_past_one_pm(self) -> None:
        result = self.ml_server._score_event(
            {"event_id": "early-close", "ts_event": event_ts(2026, 11, 27, 12, 50)}
        )

        self.assert_model_calls({5})
        self.assertEqual(result["session_feasibility"]["session_close_et"], "13:00:00")
        self.assertEqual(result["best_horizon"], 5)
        for horizon in (15, 30, 60):
            self.assertEqual(
                result["session_feasibility"]["horizons"][str(horizon)]["reason"],
                "horizon_past_session_close",
            )

    def test_after_hours_event_abstains_without_scoring_any_model(self) -> None:
        result = self.ml_server._score_event(
            {"event_id": "after-hours", "ts_event": event_ts(2026, 10, 2, 16, 1)}
        )

        self.assert_model_calls(set())
        self.assertEqual(result["status"], "abstained")
        self.assertTrue(result["abstain"])
        self.assertIsNone(result["best_horizon"])
        self.assertEqual(result["abstain_reason"], "event_outside_regular_session")
        self.assertEqual(result["session_feasibility"]["status"], "abstained")
        self.assertEqual(result["session_feasibility"]["reason"], "event_outside_regular_session")

    def test_in_session_event_with_no_feasible_horizon_abstains_explicitly(self) -> None:
        result = self.ml_server._score_event(
            {"event_id": "no-feasible", "ts_event": event_ts(2026, 10, 2, 15, 58)}
        )

        self.assert_model_calls(set())
        self.assertEqual(result["status"], "abstained")
        self.assertTrue(result["abstain"])
        self.assertIsNone(result["best_horizon"])
        self.assertEqual(result["abstain_reason"], "no_session_feasible_horizons")
        self.assertEqual(result["session_feasibility"]["status"], "abstained")
        self.assertEqual(result["session_feasibility"]["reason"], "no_session_feasible_horizons")
        for horizon in (5, 15, 30, 60):
            self.assertEqual(
                result["session_feasibility"]["horizons"][str(horizon)]["reason"],
                "horizon_past_session_close",
            )


if __name__ == "__main__":
    unittest.main()
