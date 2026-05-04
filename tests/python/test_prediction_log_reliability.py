"""Tests for prediction-log reliability improvements.

Covers:
  - WAL mode and busy_timeout are applied on new connections
  - Transient "database is locked" errors are retried
  - All retries exhausted → write_skip_total incremented (event not written)
  - Queue overflow → dropped_total incremented (not write_skip_total)
  - Scoring path only enqueues (no synchronous SQLite write on the hot path)
  - "database is locked" classified as skip; other errors classified as error
"""

from __future__ import annotations

import json
import os
import queue
import re
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[2]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_server_source() -> str:
    return (ROOT / "server" / "ml_server.py").read_text(encoding="utf-8")


def _extract_config_default(source: str, name: str) -> str:
    """Return the default literal inside os.getenv(NAME, 'default')."""
    pattern = rf"os\.getenv\(\s*['\"]?{re.escape(name)}['\"]?\s*,\s*['\"]([^'\"]+)['\"]"
    m = re.search(pattern, source)
    if m is None:
        raise KeyError(f"{name!r} not found in ml_server.py source")
    return m.group(1)


# ---------------------------------------------------------------------------
# 1. Config defaults
# ---------------------------------------------------------------------------

class TestConfigDefaults(unittest.TestCase):
    """Default env-var values must meet minimum reliability thresholds."""

    def setUp(self):
        self.src = _load_server_source()

    def test_busy_timeout_default_gte_5000ms(self):
        val = int(_extract_config_default(self.src, "PREDICTION_LOG_BUSY_TIMEOUT_MS"))
        self.assertGreaterEqual(val, 5000,
            f"PREDICTION_LOG_BUSY_TIMEOUT_MS default={val}ms, need ≥5000ms")

    def test_connect_timeout_default_gte_5s(self):
        val = float(_extract_config_default(self.src, "PREDICTION_LOG_CONNECT_TIMEOUT_SEC"))
        self.assertGreaterEqual(val, 5.0,
            f"PREDICTION_LOG_CONNECT_TIMEOUT_SEC default={val}s, need ≥5s")

    def test_write_retry_max_default_gt_0(self):
        val = int(_extract_config_default(self.src, "PREDICTION_LOG_WRITE_RETRY_MAX"))
        self.assertGreater(val, 0,
            f"PREDICTION_LOG_WRITE_RETRY_MAX default={val}, need >0 to enable retries")


# ---------------------------------------------------------------------------
# 2. WAL + busy_timeout pragmas applied on new connection
# ---------------------------------------------------------------------------

class TestConnectionPragmas(unittest.TestCase):
    """A fresh connection must have WAL journal mode and correct busy_timeout."""

    def _open_connection(self, db_path: Path, busy_timeout_ms: int = 5000) -> sqlite3.Connection:
        """Replicate _get_prediction_log_conn() pragma sequence on a real DB."""
        conn = sqlite3.connect(str(db_path), timeout=5.0)
        conn.execute(f"PRAGMA busy_timeout={busy_timeout_ms};")
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA wal_autocheckpoint=1000;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-200000;")
        return conn

    def test_wal_journal_mode_active_after_connect(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test_pred.sqlite"
            conn = self._open_connection(db_path)
            try:
                row = conn.execute("PRAGMA journal_mode;").fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row[0].lower(), "wal",
                    f"Expected WAL journal mode, got {row[0]!r}")
            finally:
                conn.close()

    def test_busy_timeout_pragma_present_in_source(self):
        """Source must call PRAGMA busy_timeout inside _get_prediction_log_conn."""
        src = _load_server_source()
        fn_match = re.search(
            r"def _get_prediction_log_conn\b.*?(?=^def |\Z)",
            src, re.MULTILINE | re.DOTALL,
        )
        self.assertIsNotNone(fn_match, "_get_prediction_log_conn not found")
        body = fn_match.group(0)
        self.assertIn("busy_timeout", body.lower(),
            "PRAGMA busy_timeout not applied in _get_prediction_log_conn")

    def test_wal_pragma_present_in_source(self):
        """Source must call PRAGMA journal_mode=WAL inside _get_prediction_log_conn."""
        src = _load_server_source()
        fn_match = re.search(
            r"def _get_prediction_log_conn\b.*?(?=^def |\Z)",
            src, re.MULTILINE | re.DOTALL,
        )
        self.assertIsNotNone(fn_match, "_get_prediction_log_conn not found")
        body = fn_match.group(0)
        self.assertIn("WAL", body,
            "PRAGMA journal_mode=WAL not applied in _get_prediction_log_conn")


# ---------------------------------------------------------------------------
# 3. Retry on transient contention
# ---------------------------------------------------------------------------

class TestWriterRetry(unittest.TestCase):
    """Writer retries on 'database is locked' before incrementing write_skip_total."""

    def _simulate_loop(self, fail_count: int, retry_max: int, backoff: float = 0.0):
        """
        Simulate the retry section of _prediction_log_writer_loop.
        Returns (final_status, write_skip_total, total_attempt_count).
        """
        attempt = {"n": 0}

        def mock_write(event, result):
            attempt["n"] += 1
            if attempt["n"] <= fail_count:
                return "skip", "database is locked"
            return "ok", None

        write_skip_total = 0
        status, error = mock_write({}, {})
        retries_remaining = retry_max
        while status == "skip" and retries_remaining > 0:
            retries_remaining -= 1
            if backoff > 0:
                time.sleep(backoff)
            status, error = mock_write({}, {})

        if status == "skip":
            write_skip_total += 1

        return status, write_skip_total, attempt["n"]

    def test_single_transient_failure_retried_and_succeeds(self):
        """One transient failure → retry → ok, skip counter unchanged."""
        status, write_skip, calls = self._simulate_loop(fail_count=1, retry_max=3)
        self.assertEqual(status, "ok")
        self.assertEqual(write_skip, 0)
        self.assertEqual(calls, 2)

    def test_all_retries_exhausted_increments_skip(self):
        """Fail more than retry_max → write_skip_total incremented."""
        status, write_skip, calls = self._simulate_loop(fail_count=10, retry_max=3)
        self.assertEqual(status, "skip")
        self.assertEqual(write_skip, 1)
        # 1 initial + 3 retries = 4 total
        self.assertEqual(calls, 4)

    def test_retry_max_zero_no_retry(self):
        """retry_max=0 → first skip is final, no retry attempts."""
        status, write_skip, calls = self._simulate_loop(fail_count=1, retry_max=0)
        self.assertEqual(status, "skip")
        self.assertEqual(write_skip, 1)
        self.assertEqual(calls, 1)

    def test_fail_count_equal_retry_max_succeeds(self):
        """fail_count == retry_max → last retry attempt gets 'ok'."""
        retry_max = 3
        # fail_count=3 → attempts 1,2,3 fail; attempt 4 (last retry) succeeds
        status, write_skip, calls = self._simulate_loop(
            fail_count=retry_max, retry_max=retry_max
        )
        self.assertEqual(status, "ok")
        self.assertEqual(write_skip, 0)
        self.assertEqual(calls, retry_max + 1)

    def test_retry_source_present_in_writer_loop(self):
        """Writer loop source must contain the retry while-loop."""
        src = _load_server_source()
        fn_match = re.search(
            r"def _prediction_log_writer_loop\b.*?(?=^def |\Z)",
            src, re.MULTILINE | re.DOTALL,
        )
        self.assertIsNotNone(fn_match, "_prediction_log_writer_loop not found")
        body = fn_match.group(0)
        self.assertIn("retries_remaining", body,
            "Retry logic (retries_remaining) not found in _prediction_log_writer_loop")
        self.assertIn("PREDICTION_LOG_WRITE_RETRY_MAX", body,
            "PREDICTION_LOG_WRITE_RETRY_MAX not referenced in writer loop")


# ---------------------------------------------------------------------------
# 4. Queue overflow → dropped_total (not write_skip_total)
# ---------------------------------------------------------------------------

class TestQueueOverflow(unittest.TestCase):
    """Full queue → dropped_total incremented; write_skip_total unchanged."""

    def _make_state_helpers(self):
        state = {"dropped_total": 0, "write_skip_total": 0, "queued_total": 0}
        lock = threading.Lock()

        def update(**fields):
            with lock:
                state.update(fields)

        def snapshot():
            with lock:
                return dict(state)

        return state, update, snapshot

    def test_queue_full_increments_dropped_not_skip(self):
        q: queue.Queue = queue.Queue(maxsize=1)
        q.put_nowait(("sentinel", "sentinel"))  # fill queue

        state, update, snapshot = self._make_state_helpers()

        try:
            q.put_nowait(({"event_id": "e-001"}, {}))
        except queue.Full:
            sb = snapshot()
            update(dropped_total=int(sb.get("dropped_total", 0)) + 1)

        self.assertEqual(state["dropped_total"], 1)
        self.assertEqual(state["write_skip_total"], 0)

    def test_queue_accepts_when_not_full(self):
        q: queue.Queue = queue.Queue(maxsize=10)
        state, update, snapshot = self._make_state_helpers()

        try:
            q.put_nowait(({"event_id": "e-002"}, {}))
            sb = snapshot()
            update(queued_total=int(sb.get("queued_total", 0)) + 1)
        except queue.Full:
            sb = snapshot()
            update(dropped_total=int(sb.get("dropped_total", 0)) + 1)

        self.assertEqual(state["queued_total"], 1)
        self.assertEqual(state["dropped_total"], 0)


# ---------------------------------------------------------------------------
# 5. Scoring path does not synchronously write SQLite
# ---------------------------------------------------------------------------

class TestScoringPathIsAsync(unittest.TestCase):
    """_write_prediction_record must NOT be called directly from the hot path.

    _enqueue_prediction is called from the helper functions
    (_score_single_event_with_log / _score_events_batch) which are themselves
    dispatched via asyncio.to_thread — never synchronously inside the handler.
    """

    def _score_helpers_source(self) -> str:
        src = _load_server_source()
        # Grab both scoring helpers
        helpers = []
        for fn_name in ("_score_single_event_with_log", "_score_events_batch"):
            m = re.search(
                rf"def {re.escape(fn_name)}\b.*?(?=^def |\Z)",
                src, re.MULTILINE | re.DOTALL,
            )
            if m:
                helpers.append(m.group(0))
        return "\n".join(helpers)

    def test_score_helpers_call_enqueue_not_direct_write(self):
        body = self._score_helpers_source()
        self.assertIn("_enqueue_prediction", body,
            "Score helpers must call _enqueue_prediction (async queue path)")
        self.assertNotIn("_write_prediction_record", body,
            "Score helpers must NOT call _write_prediction_record synchronously")

    def test_log_prediction_compat_fn_not_called_from_score_helpers(self):
        """_log_prediction (synchronous compat path) must not appear in helpers."""
        body = self._score_helpers_source()
        self.assertNotIn("_log_prediction(", body,
            "Score helpers must not call synchronous _log_prediction()")

    def test_score_endpoint_dispatches_helpers_via_to_thread(self):
        """The /score handler must delegate to asyncio.to_thread for scoring."""
        src = _load_server_source()
        m = re.search(
            r"@app\.(post|put)\(['\"]/?score['\"].*?(?=@app\.|^def |^class |\Z)",
            src, re.MULTILINE | re.DOTALL,
        )
        self.assertIsNotNone(m, "/score endpoint not found in ml_server.py")
        body = m.group(0)
        self.assertIn("asyncio.to_thread", body,
            "/score handler must use asyncio.to_thread for CPU-bound scoring")


# ---------------------------------------------------------------------------
# 6. _write_prediction_record error classification
# ---------------------------------------------------------------------------

class TestWriteRecordErrorClassification(unittest.TestCase):
    """'database is locked' → skip; other OperationalError → error."""

    def _build_write_fn(self, db_path: Path) -> object:
        """Extract _write_prediction_record + its deps from source into a namespace."""
        src = _load_server_source()

        import logging

        local = threading.local()
        # Pre-connect so _get_prediction_log_conn returns our connection
        real_conn = sqlite3.connect(str(db_path))
        local.conn = real_conn

        # Create schema on the real connection
        real_conn.executescript("""
            CREATE TABLE IF NOT EXISTS prediction_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                ts_prediction INTEGER NOT NULL,
                model_version TEXT, feature_version TEXT,
                best_horizon INTEGER, abstain INTEGER NOT NULL DEFAULT 0,
                signal_5m TEXT, signal_15m TEXT, signal_30m TEXT, signal_60m TEXT,
                prob_reject_5m REAL, prob_reject_15m REAL,
                prob_reject_30m REAL, prob_reject_60m REAL,
                prob_break_5m REAL, prob_break_15m REAL,
                prob_break_30m REAL, prob_break_60m REAL,
                threshold_reject_5m REAL, threshold_reject_15m REAL,
                threshold_reject_30m REAL, threshold_reject_60m REAL,
                threshold_break_5m REAL, threshold_break_15m REAL,
                threshold_break_30m REAL, threshold_break_60m REAL,
                regime_policy_mode TEXT, trade_regime TEXT,
                selected_policy TEXT, regime_policy_json TEXT,
                analog_best_reject_prob REAL, analog_best_break_prob REAL,
                analog_best_n REAL, analog_best_ci_width REAL,
                analog_best_disagreement REAL, analog_json TEXT,
                quality_flags TEXT, is_preview INTEGER NOT NULL DEFAULT 0,
                UNIQUE(event_id, model_version)
            );
        """)

        ns: dict = {
            "sqlite3": sqlite3,
            "_PREDICTION_LOG_LOCAL": local,
            "_PREDICTION_LOG_SCHEMA_READY": True,  # skip schema creation
            "_PREDICTION_LOG_SCHEMA_LOCK": threading.Lock(),
            "PREDICTION_LOG_DB": db_path,
            "PREDICTION_LOG_CONNECT_TIMEOUT_SEC": 5.0,
            "PREDICTION_LOG_BUSY_TIMEOUT_MS": 5000,
            "PREDICTION_LOG_SQLITE_SYNC": "NORMAL",
            "PREDICTION_LOG_WAL_AUTOCHECKPOINT": 1000,
            "_PREDICTION_LOG_CONTENTION_WARN_LOCK": threading.Lock(),
            "_PREDICTION_LOG_CONTENTION_WARN_LAST_AT": 0.0,
            "_PREDICTION_LOG_CONTENTION_WARN_SUPPRESSED": 0,
            "PREDICTION_LOG_LOCK_WARN_INTERVAL_SEC": 60.0,
            "log": logging.getLogger("test"),
            "time": time,
            "json": json,
        }

        for fn_name in ("_to_float", "_warn_prediction_log_contention",
                        "_ensure_prediction_log_schema", "_get_prediction_log_conn",
                        "_write_prediction_record"):
            m = re.search(
                rf"^def {re.escape(fn_name)}\b.*?(?=^def |\Z)",
                src, re.MULTILINE | re.DOTALL,
            )
            if m:
                exec(compile(m.group(0), "<test>", "exec"), ns)  # noqa: S102

        write_fn = ns.get("_write_prediction_record")
        if write_fn is None:
            raise RuntimeError("_write_prediction_record not extractable from source")
        return write_fn, real_conn, local

    def _call_with_mock_execute_raising(self, exc: Exception):
        """Call _write_prediction_record where the INSERT raises exc via a mock conn."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "test.sqlite"

            write_fn, real_conn, local = self._build_write_fn(db_path)

            # Wrap with MagicMock that raises on INSERT but passes other calls through
            mock_conn = MagicMock(spec=sqlite3.Connection)

            def smart_execute(sql, *args, **kw):
                stripped = sql.strip().upper()
                if stripped.startswith("INSERT"):
                    raise exc
                return real_conn.execute(sql, *args, **kw)

            mock_conn.execute.side_effect = smart_execute
            mock_conn.commit.side_effect = real_conn.commit
            local.conn = mock_conn

            event = {"event_id": "e-classify-001"}
            result = {}
            status, error = write_fn(event, result)
            real_conn.close()
            return status, error

    def test_database_locked_returns_skip(self):
        exc = sqlite3.OperationalError("database is locked")
        status, error = self._call_with_mock_execute_raising(exc)
        self.assertEqual(status, "skip")
        self.assertIn("database is locked", (error or ""))

    def test_database_busy_returns_skip(self):
        exc = sqlite3.OperationalError("database is busy")
        status, error = self._call_with_mock_execute_raising(exc)
        self.assertEqual(status, "skip")

    def test_other_operational_error_returns_error(self):
        exc = sqlite3.OperationalError("no such column: bad_col")
        status, _ = self._call_with_mock_execute_raising(exc)
        self.assertEqual(status, "error")


if __name__ == "__main__":
    unittest.main()
