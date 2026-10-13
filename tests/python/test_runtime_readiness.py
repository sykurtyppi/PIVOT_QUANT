from __future__ import annotations

import importlib.util
import math
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


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

    def test_ml_health_accepts_governed_stale_runtime_with_models(self) -> None:
        self.assertEqual(
            self.module.validate_ml_health(
                {
                    "status": "stale",
                    "runtime_identity": {"governance_verified": True},
                    "models": {"reject": [5, 15, 30, 60]},
                }
            ),
            [],
        )

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