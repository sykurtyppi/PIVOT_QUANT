#!/usr/bin/env python3
"""Fail-closed runtime readiness checks for the live scoring stack."""

from __future__ import annotations

import argparse
import ipaddress
import json
import math
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

ROOT_DIR = Path(__file__).resolve().parents[1]
SCORE_KEY_PATTERN = re.compile(r"^prob_(?:reject|break)_(\d+)m$")
VALID_SIGNAL_VALUES = {"break", "reject", "no_edge"}


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        del req, fp, code, msg, headers, newurl
        return None


def validate_ml_health(payload: object) -> list[str]:
    if not isinstance(payload, dict):
        return ["ML health response is not a JSON object"]

    errors: list[str] = []
    status = str(payload.get("status") or "missing")
    if status not in {"ok", "stale"}:
        errors.append(f"ML status is {status}")

    runtime_identity = payload.get("runtime_identity")
    if not isinstance(runtime_identity, dict) or not runtime_identity.get(
        "governance_verified"
    ):
        errors.append("ML governance identity is not verified")

    models = payload.get("models")
    has_models = isinstance(models, dict) and any(
        isinstance(horizons, (list, dict)) and bool(horizons)
        for horizons in models.values()
    )
    if not has_models:
        errors.append("ML runtime has no loaded models")
    return errors


def validate_collector_health(payload: object) -> list[str]:
    if not isinstance(payload, dict):
        return ["collector health response is not a JSON object"]
    status = str(payload.get("status") or "missing")
    return [] if status == "ok" else [f"collector status is {status}"]


def validate_score_response(
    payload: object, *, expected_model_version: str
) -> list[str]:
    if not isinstance(payload, dict):
        return ["ML score response is not a JSON object"]

    errors: list[str] = []
    status = str(payload.get("status") or "missing")
    if status != "ok":
        errors.append(f"ML score status is {status}")

    model_version = str(payload.get("model_version") or "").strip()
    if not expected_model_version or model_version != expected_model_version:
        errors.append("ML score model version does not match governed active version")

    scores = payload.get("scores")
    signals = payload.get("signals")
    has_finite_probability = isinstance(scores, dict) and any(
        str(name).startswith("prob_")
        and isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and 0.0 <= float(value) <= 1.0
        for name, value in scores.items()
    )
    if not has_finite_probability:
        errors.append("ML score response has no finite probability")

    has_signal = isinstance(signals, dict) and any(
        str(name).startswith("signal_") and bool(str(value).strip())
        for name, value in signals.items()
        if value is not None
    )
    if not has_signal:
        errors.append("ML score response has no signals")

    complete_horizon = False
    if isinstance(scores, dict) and isinstance(signals, dict):
        for name, value in scores.items():
            match = SCORE_KEY_PATTERN.fullmatch(str(name))
            if not match:
                continue
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(float(value))
                or not 0.0 <= float(value) <= 1.0
            ):
                continue
            signal = signals.get(f"signal_{match.group(1)}m")
            if signal in VALID_SIGNAL_VALUES:
                complete_horizon = True
                break
    if not complete_horizon:
        errors.append("ML score response has no complete scored horizon")
    return errors


def build_readiness_event() -> dict[str, Any]:
    # A known regular-session timestamp makes the probe exercise model inference
    # even when readiness is checked overnight or on a weekend. Omitting event_id
    # prevents the synthetic probe from entering prediction_log.
    ts_event = int(
        datetime(2026, 10, 9, 14, 0, tzinfo=timezone.utc).timestamp() * 1000
    )
    return {
        "symbol": "SPY",
        "ts_event": ts_event,
        "level_type": "R1",
        "level_price": 100.0,
        "touch_price": 100.0,
        "distance_bps": 0.0,
    }


def _env_file_value(key: str) -> str:
    env_path = ROOT_DIR / ".env"
    if not env_path.is_file():
        return ""
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        prefix = f"{key}="
        if not line.startswith(prefix):
            continue
        value = line[len(prefix) :].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        return value
    return ""


def _require_loopback_url(url: str) -> None:
    parsed = urlsplit(url)
    hostname = parsed.hostname
    if parsed.scheme not in {"http", "https"} or not hostname:
        raise ValueError("authenticated readiness score URL must be loopback")
    if hostname == "localhost":
        return
    try:
        if ipaddress.ip_address(hostname).is_loopback:
            return
    except ValueError:
        pass
    raise ValueError("authenticated readiness score URL must be loopback")


def _request_json(
    url: str,
    *,
    timeout: float,
    payload: dict[str, Any] | None = None,
    token: str = "",
) -> object:
    headers = {"Accept": "application/json"}
    body = None
    method = "GET"
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
        method = "POST"
    if token:
        headers["X-ML-Write-Token"] = token
    request = Request(url=url, data=body, headers=headers, method=method)
    opener = build_opener(_NoRedirectHandler())
    try:
        with opener.open(request, timeout=timeout) as response:  # noqa: S310
            raw = response.read()
    except HTTPError as exc:
        try:
            raw = exc.read()
        except OSError:
            raw = b""
        detail = raw.decode("utf-8", errors="replace") if raw else "no response body"
        raise RuntimeError(f"HTTP {exc.code} from {url}: {detail}") from exc
    except (URLError, OSError, TimeoutError) as exc:
        raise RuntimeError(f"request failed for {url}: {type(exc).__name__}: {exc}") from exc
    try:
        return json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid JSON from {url}") from exc


def run_checks(*, ml_base_url: str, collector_url: str, timeout: float) -> list[str]:
    errors: list[str] = []
    token = os.getenv("ML_WRITE_AUTH_TOKEN") or _env_file_value("ML_WRITE_AUTH_TOKEN")
    active_version = ""

    try:
        ml_health = _request_json(f"{ml_base_url.rstrip('/')}/health", timeout=timeout)
        errors.extend(validate_ml_health(ml_health))
        if isinstance(ml_health, dict):
            runtime_identity = ml_health.get("runtime_identity")
            if isinstance(runtime_identity, dict):
                active_version = str(
                    runtime_identity.get("active_version") or ""
                ).strip()
    except RuntimeError as exc:
        errors.append(str(exc))

    try:
        collector_health = _request_json(collector_url, timeout=timeout)
        errors.extend(validate_collector_health(collector_health))
    except RuntimeError as exc:
        errors.append(str(exc))

    try:
        score_url = f"{ml_base_url.rstrip('/')}/score"
        _require_loopback_url(score_url)
        score = _request_json(
            score_url,
            timeout=timeout,
            payload={"event": build_readiness_event()},
            token=token,
        )
        errors.extend(
            validate_score_response(score, expected_model_version=active_version)
        )
    except (RuntimeError, ValueError) as exc:
        errors.append(str(exc))

    return errors


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ml-base-url", default="http://127.0.0.1:5003")
    parser.add_argument(
        "--collector-url", default="http://127.0.0.1:5004/health"
    )
    parser.add_argument("--timeout", type=float, default=5.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    errors = run_checks(
        ml_base_url=args.ml_base_url,
        collector_url=args.collector_url,
        timeout=max(0.1, args.timeout),
    )
    if errors:
        for error in errors:
            print(f"[ERR] {error}", file=sys.stderr)
        return 1
    print("[OK] ML governance, loaded models, collector, and live scoring verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
