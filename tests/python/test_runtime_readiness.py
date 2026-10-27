from __future__ import annotations

import importlib.util
import json
import math
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[2]


def load_runtime_readiness():
    name = "runtime_readiness_test"
    sys.modules.pop(name, None)
    spec = importlib.util.spec_from_file_location(
        name, REPO_ROOT / "scripts" / "runtime_readiness.py"
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load runtime_readiness")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class RuntimeReadinessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = load_runtime_readiness()

    def test_ml_health_rejects_degraded_runtime_even_with_status_field(self) -> None:
        errors = self.module.validate_ml_health(
            {
                "status": "degraded",
                "runtime_identity": {"governance_verified": False},
                "models": {},
                "startup_error": "manifest mismatch",
            }
        )
        self.assertIn("ML status is degraded", errors)
        self.assertIn("ML governance identity is not verified", errors)
        self.assertIn("ML runtime has no loaded models", errors)

    @staticmethod
    def _ready_ml_health(**overrides: object) -> dict[str, object]:
        payload: dict[str, object] = {
            "status": "ok",
            "runtime_identity": {"governance_verified": True},
            "models": {"reject": [5, 15, 30, 60]},
            "serving_state": {
                "state": "active",
                "source": "file",
                "load_error": None,
            },
        }
        payload.update(overrides)
        return payload

    def test_ml_health_rejects_stale_active_model(self) -> None:
        errors = self.module.validate_ml_health(
            self._ready_ml_health(status="stale")
        )
        self.assertEqual(errors, ["ML active model is stale"])

    def test_ml_health_distinguishes_intentionally_dormant_model(self) -> None:
        errors = self.module.validate_ml_health(
            self._ready_ml_health(
                status="stale",
                serving_state={
                    "state": "dormant_audit_fail",
                    "source": "file",
                    "load_error": None,
                },
            )
        )
        self.assertEqual(
            errors,
            ["ML serving is intentionally dormant: dormant_audit_fail"],
        )

    def test_ml_health_rejects_missing_serving_state_file(self) -> None:
        errors = self.module.validate_ml_health(
            self._ready_ml_health(
                serving_state={
                    "state": "active",
                    "source": "default_missing_file",
                    "load_error": None,
                }
            )
        )
        self.assertEqual(errors, ["ML serving state file is missing"])

    def test_ml_health_rejects_missing_serving_state_payload(self) -> None:
        health = self._ready_ml_health()
        del health["serving_state"]
        self.assertEqual(
            self.module.validate_ml_health(health),
            ["ML serving state is missing"],
        )

    def test_ml_health_rejects_untrusted_serving_state_source(self) -> None:
        errors = self.module.validate_ml_health(
            self._ready_ml_health(
                serving_state={
                    "state": "active",
                    "source": "",
                    "load_error": None,
                }
            )
        )
        self.assertEqual(errors, ["ML serving state source is invalid: missing"])

    def test_ml_health_rejects_invalid_serving_state_file(self) -> None:
        errors = self.module.validate_ml_health(
            self._ready_ml_health(
                serving_state={
                    "state": "dormant_data_quality",
                    "source": "invalid_file",
                    "load_error": "schema_invalid: state_invalid",
                }
            )
        )
        self.assertEqual(errors, ["ML serving state file is invalid"])

    def test_ml_health_accepts_fresh_active_governed_runtime(self) -> None:
        self.assertEqual(
            self.module.validate_ml_health(self._ready_ml_health()),
            [],
        )

    def test_alert_freshness_requires_recent_semantic_checks(self) -> None:
        now_ts = 1_800_000_000
        self.assertEqual(
            self.module.validate_alert_freshness(
                {
                    "services": {
                        "ml": {"last_check_ts": now_ts - 30},
                        "collector": {"last_check_ts": now_ts - 45},
                    }
                },
                now_ts=now_ts,
                max_age_sec=120,
            ),
            [],
        )

    def test_alert_freshness_rejects_stale_service_checks(self) -> None:
        now_ts = 1_800_000_000
        errors = self.module.validate_alert_freshness(
            {
                "services": {
                    "ml": {"last_check_ts": now_ts - 121},
                    "collector": {"last_check_ts": now_ts - 10},
                }
            },
            now_ts=now_ts,
            max_age_sec=120,
        )
        self.assertEqual(errors, ["alert checks are stale for: ml"])

    def test_alert_freshness_rejects_missing_or_invalid_state(self) -> None:
        self.assertEqual(
            self.module.validate_alert_freshness(
                {}, now_ts=1_800_000_000, max_age_sec=120
            ),
            ["alert state has no service checks"],
        )
        self.assertEqual(
            self.module.validate_alert_freshness(
                {"services": {"ml": {"last_check_ts": "bad"}}},
                now_ts=1_800_000_000,
                max_age_sec=120,
            ),
            [
                "alert check timestamp is invalid for: ml",
                "alert check is missing for: collector",
            ],
        )
        self.assertEqual(
            self.module.validate_alert_freshness(
                {"services": {"ml": {"last_check_ts": float("inf")}}},
                now_ts=1_800_000_000,
                max_age_sec=120,
            ),
            [
                "alert check timestamp is invalid for: ml",
                "alert check is missing for: collector",
            ],
        )

    def test_run_checks_includes_alert_freshness_and_scores_only_ready_ml(self) -> None:
        now_ts = 1_800_000_000
        with tempfile.TemporaryDirectory() as tmp_dir:
            alert_state_path = Path(tmp_dir) / "health_alert_state.json"
            alert_state_path.write_text(
                json.dumps(
                    {
                        "services": {
                            "ml": {"last_check_ts": now_ts - 121},
                            "collector": {"last_check_ts": now_ts - 10},
                        }
                    }
                ),
                encoding="utf-8",
            )
            responses = [
                self._ready_ml_health(
                    runtime_identity={
                        "governance_verified": True,
                        "active_version": "v001",
                    }
                ),
                {"status": "ok"},
                {
                    "status": "ok",
                    "model_version": "v001",
                    "scores": {"prob_reject_5m": 0.4},
                    "signals": {"signal_5m": "no_edge"},
                },
            ]
            with mock.patch.object(
                self.module, "_request_json", side_effect=responses
            ) as request_json:
                errors = self.module.run_checks(
                    ml_base_url="http://127.0.0.1:5003",
                    collector_url="http://127.0.0.1:5004/health",
                    timeout=1.0,
                    alert_state_path=alert_state_path,
                    alert_max_age_sec=120,
                    now_ts=now_ts,
                )

        self.assertEqual(errors, ["alert checks are stale for: ml"])
        self.assertEqual(request_json.call_count, 3)

    def test_run_checks_does_not_score_intentionally_dormant_ml(self) -> None:
        now_ts = 1_800_000_000
        with tempfile.TemporaryDirectory() as tmp_dir:
            alert_state_path = Path(tmp_dir) / "health_alert_state.json"
            alert_state_path.write_text(
                json.dumps(
                    {
                        "services": {
                            "ml": {"last_check_ts": now_ts - 10},
                            "collector": {"last_check_ts": now_ts - 10},
                        }
                    }
                ),
                encoding="utf-8",
            )
            responses = [
                self._ready_ml_health(
                    status="stale",
                    serving_state={
                        "state": "dormant_audit_fail",
                        "source": "file",
                        "load_error": None,
                    },
                ),
                {"status": "ok"},
            ]
            with mock.patch.object(
                self.module, "_request_json", side_effect=responses
            ) as request_json:
                errors = self.module.run_checks(
                    ml_base_url="http://127.0.0.1:5003",
                    collector_url="http://127.0.0.1:5004/health",
                    timeout=1.0,
                    alert_state_path=alert_state_path,
                    alert_max_age_sec=120,
                    now_ts=now_ts,
                )

        self.assertEqual(
            errors, ["ML serving is intentionally dormant: dormant_audit_fail"]
        )
        self.assertEqual(request_json.call_count, 2)

    def test_parse_args_reads_alert_readiness_environment(self) -> None:
        with mock.patch.dict(
            "os.environ",
            {
                "ML_READINESS_ALERT_STATE_FILE": "/tmp/alert-state.json",
                "ML_READINESS_ALERT_MAX_AGE_SEC": "240",
            },
            clear=False,
        ):
            args = self.module.parse_args([])
        self.assertEqual(args.alert_state_path, Path("/tmp/alert-state.json"))
        self.assertEqual(args.alert_max_age_sec, 240)

    def test_collector_health_requires_ok_status(self) -> None:
        self.assertEqual(
            self.module.validate_collector_health({"status": "degraded"}),
            ["collector status is degraded"],
        )
        self.assertEqual(self.module.validate_collector_health({"status": "ok"}), [])

    def test_score_response_requires_successful_scoring_shape(self) -> None:
        self.assertEqual(
            self.module.validate_score_response(
                {
                    "status": "ok",
                    "model_version": "v001",
                    "scores": {"prob_reject_5m": 0.4},
                    "signals": {"signal_5m": "no_edge"},
                },
                expected_model_version="v001",
            ),
            [],
        )

    def test_score_response_rejects_non_ok_status(self) -> None:
        for status in ("degraded", "abstained", "busy", "unavailable"):
            with self.subTest(status=status):
                errors = self.module.validate_score_response(
                    {
                        "status": status,
                        "model_version": "v001",
                        "scores": {"prob_reject_5m": 0.4},
                        "signals": {"signal_5m": "no_edge"},
                    },
                    expected_model_version="v001",
                )
                self.assertIn(f"ML score status is {status}", errors)

    def test_score_response_requires_matching_governed_model_version(self) -> None:
        payload = {
            "status": "ok",
            "scores": {"prob_reject_5m": 0.4},
            "signals": {"signal_5m": "no_edge"},
        }
        self.assertIn(
            "ML score model version does not match governed active version",
            self.module.validate_score_response(
                payload, expected_model_version="v001"
            ),
        )
        payload["model_version"] = "v999"
        self.assertIn(
            "ML score model version does not match governed active version",
            self.module.validate_score_response(
                payload, expected_model_version="v001"
            ),
        )

    def test_score_response_requires_finite_probability_and_signal(self) -> None:
        for probability in (None, math.nan, math.inf, -math.inf):
            with self.subTest(probability=probability):
                errors = self.module.validate_score_response(
                    {
                        "status": "ok",
                        "model_version": "v001",
                        "scores": {"prob_reject_5m": probability},
                        "signals": {},
                    },
                    expected_model_version="v001",
                )
                self.assertIn("ML score response has no finite probability", errors)
                self.assertIn("ML score response has no signals", errors)

    def test_score_response_requires_matching_probability_and_signal_contract(self) -> None:
        invalid_payloads = (
            {
                "scores": {"not_a_probability": 0.25},
                "signals": {"not_a_signal": False},
            },
            {
                "scores": {"prob_reject_5m": 0.25},
                "signals": {"signal_15m": "reject"},
            },
            {
                "scores": {"prob_reject_5m": 0.25},
                "signals": {"signal_5m": "invalid"},
            },
        )
        for partial_payload in invalid_payloads:
            with self.subTest(partial_payload=partial_payload):
                errors = self.module.validate_score_response(
                    {
                        "status": "ok",
                        "model_version": "v001",
                        **partial_payload,
                    },
                    expected_model_version="v001",
                )
                self.assertIn("ML score response has no complete scored horizon", errors)

    def test_authenticated_score_probe_rejects_non_loopback_url(self) -> None:
        with self.assertRaisesRegex(ValueError, "loopback"):
            self.module._require_loopback_url("https://example.com/score")
        self.module._require_loopback_url("http://127.0.0.1:5003/score")
        self.module._require_loopback_url("http://localhost:5003/score")

    def test_authenticated_score_probe_rejects_redirect_without_forwarding_token(self) -> None:
        received_tokens: list[str | None] = []

        class TargetHandler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                received_tokens.append(self.headers.get("X-ML-Write-Token"))
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status":"ok"}')

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        target = ThreadingHTTPServer(("127.0.0.1", 0), TargetHandler)
        target_thread = threading.Thread(target=target.serve_forever, daemon=True)
        target_thread.start()

        target_url = f"http://127.0.0.1:{target.server_port}/score"

        class RedirectHandler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                self.send_response(302)
                self.send_header("Location", target_url)
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        redirect = ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler)
        redirect_thread = threading.Thread(target=redirect.serve_forever, daemon=True)
        redirect_thread.start()
        try:
            with self.assertRaisesRegex(RuntimeError, "HTTP 302"):
                self.module._request_json(
                    f"http://127.0.0.1:{redirect.server_port}/score",
                    timeout=2,
                    payload={"event": {}},
                    token="test-secret",
                )
            self.assertEqual(received_tokens, [])
        finally:
            redirect.shutdown()
            target.shutdown()
            redirect.server_close()
            target.server_close()
            redirect_thread.join(timeout=2)
            target_thread.join(timeout=2)

    def test_score_response_rejects_missing_shape(self) -> None:
        self.assertEqual(
            self.module.validate_score_response(
                {"detail": "ML runtime governance/readiness failure"},
                expected_model_version="v001",
            ),
            [
                "ML score status is missing",
                "ML score model version does not match governed active version",
                "ML score response has no finite probability",
                "ML score response has no signals",
                "ML score response has no complete scored horizon",
            ],
        )

    def test_readiness_probe_event_is_valid_and_not_persisted(self) -> None:
        event = self.module.build_readiness_event()
        self.assertNotIn("event_id", event)
        self.assertEqual(event["symbol"], "SPY")
        self.assertIsInstance(event["ts_event"], int)
        self.assertGreater(event["level_price"], 0)
        self.assertEqual(event["level_price"], event["touch_price"])


if __name__ == "__main__":
    unittest.main()