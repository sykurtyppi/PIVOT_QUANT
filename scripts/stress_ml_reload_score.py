#!/usr/bin/env python3
"""Concurrent stress harness for ML /score and /reload endpoints.

Usage examples:
  python scripts/stress_ml_reload_score.py --duration-sec 20
  python scripts/stress_ml_reload_score.py --base-url http://127.0.0.1:5003 --score-workers 6
  python scripts/stress_ml_reload_score.py --self-test
"""

from __future__ import annotations

import argparse
import json
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_BASE_URL = "http://127.0.0.1:5003"
SCORE_BACKPRESSURE_CODES = {429}
RELOAD_BACKPRESSURE_CODES = {409, 429}


@dataclass
class StressStats:
    score_ok: int = 0
    score_backpressure: int = 0
    score_fail: int = 0
    reload_ok: int = 0
    reload_backpressure: int = 0
    reload_fail: int = 0
    score_latencies_ms: list[float] = field(default_factory=list)
    reload_latencies_ms: list[float] = field(default_factory=list)
    last_error: str | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)

    def record_score(
        self,
        *,
        outcome: str,
        latency_ms: float,
        error: str | None = None,
    ) -> None:
        with self.lock:
            if outcome == "ok":
                self.score_ok += 1
                self.score_latencies_ms.append(float(latency_ms))
            elif outcome == "backpressure":
                self.score_backpressure += 1
            else:
                self.score_fail += 1
                self.last_error = error or self.last_error

    def record_reload(
        self,
        *,
        outcome: str,
        latency_ms: float,
        error: str | None = None,
    ) -> None:
        with self.lock:
            if outcome == "ok":
                self.reload_ok += 1
                self.reload_latencies_ms.append(float(latency_ms))
            elif outcome == "backpressure":
                self.reload_backpressure += 1
            else:
                self.reload_fail += 1
                self.last_error = error or self.last_error

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            score_latencies = list(self.score_latencies_ms)
            reload_latencies = list(self.reload_latencies_ms)
            return {
                "score_ok": self.score_ok,
                "score_backpressure": self.score_backpressure,
                "score_fail": self.score_fail,
                "reload_ok": self.reload_ok,
                "reload_backpressure": self.reload_backpressure,
                "reload_fail": self.reload_fail,
                "score_latency_ms": summarize_latencies(score_latencies),
                "reload_latency_ms": summarize_latencies(reload_latencies),
                "last_error": self.last_error,
            }


def summarize_latencies(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "min": None, "p50": None, "p95": None, "max": None, "mean": None}
    ordered = sorted(values)
    count = len(ordered)
    mean = sum(ordered) / count
    return {
        "count": count,
        "min": round(ordered[0], 3),
        "p50": round(_percentile(ordered, 50.0), 3),
        "p95": round(_percentile(ordered, 95.0), 3),
        "max": round(ordered[-1], 3),
        "mean": round(mean, 3),
    }


def _percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    rank = (pct / 100.0) * (len(sorted_values) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = rank - lo
    return float(sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac)


def _post_json_status(
    url: str, payload: dict[str, Any], timeout_sec: float
) -> tuple[int, float, str | None]:
    body = json.dumps(payload).encode("utf-8")
    req = Request(
        url=url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    start = time.perf_counter()
    try:
        with urlopen(req, timeout=timeout_sec) as resp:  # noqa: S310
            raw = resp.read()
            body_text = raw.decode("utf-8", errors="replace") if raw else None
            return int(resp.status), (time.perf_counter() - start) * 1000.0, body_text
    except HTTPError as exc:
        raw = exc.read()
        body_text = raw.decode("utf-8", errors="replace") if raw else None
        return int(exc.code), (time.perf_counter() - start) * 1000.0, body_text


def _wait_for_health(
    *,
    base_url: str,
    ready_timeout_sec: float,
    ready_poll_ms: int,
    timeout_sec: float,
) -> tuple[bool, float, str | None]:
    health_url = f"{base_url.rstrip('/')}/health"
    started = time.perf_counter()
    deadline = started + max(0.1, ready_timeout_sec)
    poll_sec = max(0.01, ready_poll_ms / 1000.0)
    last_error: str | None = None

    while time.perf_counter() < deadline:
        req = Request(url=health_url, method="GET")
        try:
            with urlopen(req, timeout=max(0.1, timeout_sec)):  # noqa: S310
                return True, (time.perf_counter() - started), None
        except (HTTPError, URLError, OSError, TimeoutError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            time.sleep(poll_sec)
    return False, (time.perf_counter() - started), last_error


def _make_event(worker_id: int, seq: int) -> dict[str, Any]:
    ts_ms = int(time.time() * 1000)
    return {
        "event_id": f"stress_{worker_id}_{seq}_{ts_ms}",
        "symbol": "SPY",
        "ts_event": ts_ms,
        "level_type": "R1",
        "level_price": 100.0,
        "touch_price": 100.0,
        "distance_bps": 0.0,
    }


def _score_worker(
    *,
    worker_id: int,
    score_url: str,
    timeout_sec: float,
    score_interval_ms: int,
    error_backoff_ms: int,
    stop_event: threading.Event,
    stats: StressStats,
) -> None:
    seq = 0
    interval_sec = max(0.0, score_interval_ms / 1000.0)
    error_backoff_sec = max(0.0, error_backoff_ms / 1000.0)
    while not stop_event.is_set():
        seq += 1
        payload = {"event": _make_event(worker_id, seq)}
        try:
            status_code, latency_ms, body = _post_json_status(score_url, payload, timeout_sec)
            if status_code == 200:
                stats.record_score(outcome="ok", latency_ms=latency_ms)
            elif status_code in SCORE_BACKPRESSURE_CODES:
                stats.record_score(outcome="backpressure", latency_ms=latency_ms)
            else:
                stats.record_score(
                    outcome="fail",
                    latency_ms=latency_ms,
                    error=f"HTTP {status_code}: {body or 'no body'}",
                )
                if error_backoff_sec > 0:
                    stop_event.wait(error_backoff_sec)
        except (HTTPError, URLError, OSError, TimeoutError) as exc:
            stats.record_score(
                outcome="fail",
                latency_ms=0.0,
                error=f"{type(exc).__name__}: {exc}",
            )
            if error_backoff_sec > 0:
                stop_event.wait(error_backoff_sec)
        if interval_sec > 0:
            stop_event.wait(interval_sec)


def _reload_worker(
    *,
    reload_url: str,
    timeout_sec: float,
    interval_ms: int,
    stop_event: threading.Event,
    stats: StressStats,
) -> None:
    interval_sec = max(0.01, interval_ms / 1000.0)
    while not stop_event.is_set():
        try:
            status_code, latency_ms, body = _post_json_status(reload_url, {}, timeout_sec)
            if status_code == 200:
                stats.record_reload(outcome="ok", latency_ms=latency_ms)
            elif status_code in RELOAD_BACKPRESSURE_CODES:
                stats.record_reload(outcome="backpressure", latency_ms=latency_ms)
            else:
                stats.record_reload(
                    outcome="fail",
                    latency_ms=latency_ms,
                    error=f"HTTP {status_code}: {body or 'no body'}",
                )
        except (HTTPError, URLError, OSError, TimeoutError) as exc:
            stats.record_reload(
                outcome="fail",
                latency_ms=0.0,
                error=f"{type(exc).__name__}: {exc}",
            )
        stop_event.wait(interval_sec)


def run_stress(
    *,
    base_url: str,
    duration_sec: float,
    ready_timeout_sec: float,
    ready_poll_ms: int,
    score_workers: int,
    score_interval_ms: int,
    score_error_backoff_ms: int,
    reload_interval_ms: int,
    timeout_sec: float,
    fail_on_error: bool,
) -> dict[str, Any]:
    stop_event = threading.Event()
    stats = StressStats()
    score_url = f"{base_url.rstrip('/')}/score"
    reload_url = f"{base_url.rstrip('/')}/reload"
    ready_ok, ready_wait_sec, ready_error = _wait_for_health(
        base_url=base_url,
        ready_timeout_sec=ready_timeout_sec,
        ready_poll_ms=ready_poll_ms,
        timeout_sec=timeout_sec,
    )
    if not ready_ok:
        summary = stats.snapshot()
        summary["base_url"] = base_url
        summary["duration_sec"] = 0.0
        summary["ready"] = False
        summary["ready_wait_sec"] = round(ready_wait_sec, 3)
        summary["ready_timeout_sec"] = ready_timeout_sec
        summary["score_workers"] = score_workers
        summary["score_interval_ms"] = score_interval_ms
        summary["score_error_backoff_ms"] = score_error_backoff_ms
        summary["reload_interval_ms"] = reload_interval_ms
        summary["timeout_sec"] = timeout_sec
        summary["fail_on_error"] = bool(fail_on_error)
        summary["last_error"] = ready_error or "health check timeout"
        summary["ok"] = False
        return summary

    threads: list[threading.Thread] = []
    for worker_id in range(max(1, score_workers)):
        thread = threading.Thread(
            target=_score_worker,
            kwargs={
                "worker_id": worker_id,
                "score_url": score_url,
                "timeout_sec": timeout_sec,
                "score_interval_ms": score_interval_ms,
                "error_backoff_ms": score_error_backoff_ms,
                "stop_event": stop_event,
                "stats": stats,
            },
            daemon=True,
        )
        threads.append(thread)

    reload_thread = threading.Thread(
        target=_reload_worker,
        kwargs={
            "reload_url": reload_url,
            "timeout_sec": timeout_sec,
            "interval_ms": reload_interval_ms,
            "stop_event": stop_event,
            "stats": stats,
        },
        daemon=True,
    )
    threads.append(reload_thread)

    started_at = time.time()
    for thread in threads:
        thread.start()

    time.sleep(max(0.1, duration_sec))
    stop_event.set()

    for thread in threads:
        thread.join(timeout=max(1.0, timeout_sec * 2))

    finished_at = time.time()
    summary = stats.snapshot()
    summary["base_url"] = base_url
    summary["duration_sec"] = round(finished_at - started_at, 3)
    summary["ready"] = True
    summary["ready_wait_sec"] = round(ready_wait_sec, 3)
    summary["ready_timeout_sec"] = ready_timeout_sec
    summary["score_workers"] = score_workers
    summary["score_interval_ms"] = score_interval_ms
    summary["score_error_backoff_ms"] = score_error_backoff_ms
    summary["reload_interval_ms"] = reload_interval_ms
    summary["timeout_sec"] = timeout_sec
    summary["fail_on_error"] = bool(fail_on_error)
    summary["ok"] = not (summary["score_fail"] or summary["reload_fail"])
    return summary


class _SelfTestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _json_response(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:  # noqa: N802
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        if content_length > 0:
            _ = self.rfile.read(content_length)
        if self.path == "/score":
            self._json_response(200, {"status": "ok", "scores": {"prob_reject_5m": 0.5}})
            return
        if self.path == "/reload":
            self._json_response(200, {"status": "ok"})
            return
        self._json_response(404, {"status": "error", "message": "not found"})

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self._json_response(200, {"status": "ok"})
            return
        self._json_response(404, {"status": "error", "message": "not found"})

    def log_message(self, _format: str, *_args: Any) -> None:
        return


def run_self_test(args: argparse.Namespace) -> int:
    try:
        server = ThreadingHTTPServer(("127.0.0.1", 0), _SelfTestHandler)
    except PermissionError as exc:
        print(
            json.dumps(
                {
                    "mode": "self_test",
                    "status": "skipped",
                    "reason": f"socket bind not permitted: {exc}",
                },
                indent=2,
            )
        )
        return 0
    server.daemon_threads = True
    host, port = server.server_address
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        summary = run_stress(
            base_url=f"http://{host}:{port}",
            duration_sec=max(0.5, float(args.duration_sec)),
            ready_timeout_sec=max(0.1, float(args.ready_timeout_sec)),
            ready_poll_ms=max(10, int(args.ready_poll_ms)),
            score_workers=max(1, int(args.score_workers)),
            score_interval_ms=max(0, int(args.score_interval_ms)),
            score_error_backoff_ms=max(0, int(args.score_error_backoff_ms)),
            reload_interval_ms=max(10, int(args.reload_interval_ms)),
            timeout_sec=max(0.1, float(args.timeout_sec)),
            fail_on_error=True,
        )
        print(json.dumps({"mode": "self_test", **summary}, indent=2))
        return 0 if bool(summary.get("ok")) else 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stress-test concurrent ML /score and /reload requests."
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="ML server base URL")
    parser.add_argument("--duration-sec", type=float, default=20.0, help="Stress duration in seconds")
    parser.add_argument(
        "--ready-timeout-sec",
        type=float,
        default=45.0,
        help="Wait this long for /health before starting load.",
    )
    parser.add_argument(
        "--ready-poll-ms",
        type=int,
        default=250,
        help="Polling interval while waiting for /health readiness.",
    )
    parser.add_argument("--score-workers", type=int, default=4, help="Concurrent /score workers")
    parser.add_argument(
        "--score-interval-ms",
        type=int,
        default=5,
        help="Delay between score requests per worker (milliseconds).",
    )
    parser.add_argument(
        "--score-error-backoff-ms",
        type=int,
        default=25,
        help="Extra delay after score failure before retry (milliseconds).",
    )
    parser.add_argument(
        "--reload-interval-ms",
        type=int,
        default=300,
        help="Interval between /reload calls in milliseconds",
    )
    parser.add_argument("--timeout-sec", type=float, default=2.5, help="HTTP timeout per request")
    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Return non-zero exit code if any /score or /reload call fails",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run against an in-process fake server (CI-safe, no external deps).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        return run_self_test(args)

    summary = run_stress(
        base_url=str(args.base_url),
        duration_sec=max(0.5, float(args.duration_sec)),
        ready_timeout_sec=max(0.1, float(args.ready_timeout_sec)),
        ready_poll_ms=max(10, int(args.ready_poll_ms)),
        score_workers=max(1, int(args.score_workers)),
        score_interval_ms=max(0, int(args.score_interval_ms)),
        score_error_backoff_ms=max(0, int(args.score_error_backoff_ms)),
        reload_interval_ms=max(10, int(args.reload_interval_ms)),
        timeout_sec=max(0.1, float(args.timeout_sec)),
        fail_on_error=bool(args.fail_on_error),
    )
    print(json.dumps(summary, indent=2))
    if bool(args.fail_on_error) and not bool(summary.get("ok")):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
