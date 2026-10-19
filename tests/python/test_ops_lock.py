"""Tests for scripts/ops_lock.py — the shared fcntl lock guarding the backup
and restore-drill ops scripts.

Covers the concurrency contract: exclusive acquisition, the owner/pid payload,
parent-directory creation, truncation of stale content, fail-with-TimeoutError
when the lock is held (immediate at timeout 0 / negative, and after the retry
loop for a positive timeout), and release on both normal exit and exception.

Stdlib-only (fcntl + tempfile); runs without the heavy ML dependencies.
"""
from __future__ import annotations

import fcntl
import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.ops_lock import hold_lock  # noqa: E402


class HoldLockTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = Path(self._tmp.name)

    def _external_hold(self, path: Path):
        """Grab the flock from a separate open file description, as another
        process would (flock is per-description, so this blocks hold_lock)."""
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = path.open("a+", encoding="utf-8")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

        def _release():
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()

        self.addCleanup(_release)
        return handle

    def test_acquires_and_writes_owner_payload(self) -> None:
        lock = self.tmp / "ops.lock"
        with hold_lock(lock, 5, "nightly_backup"):
            payload = json.loads(lock.read_text(encoding="utf-8"))
        self.assertEqual(payload["owner"], "nightly_backup")
        self.assertEqual(payload["pid"], os.getpid())
        self.assertTrue(payload["acquired_at"].endswith("UTC"))

    def test_creates_missing_parent_directories(self) -> None:
        lock = self.tmp / "nested" / "deep" / "ops.lock"
        self.assertFalse(lock.parent.exists())
        with hold_lock(lock, 5, "owner"):
            self.assertTrue(lock.parent.is_dir())

    def test_truncates_preexisting_content(self) -> None:
        lock = self.tmp / "ops.lock"
        lock.write_text("x" * 500 + " stale-not-json", encoding="utf-8")
        with hold_lock(lock, 5, "owner"):
            text = lock.read_text(encoding="utf-8")
        # Clean JSON with exactly the three keys — no leftover stale bytes.
        payload = json.loads(text)
        self.assertEqual(set(payload), {"owner", "pid", "acquired_at"})

    def test_is_exclusive_while_held(self) -> None:
        lock = self.tmp / "ops.lock"
        with hold_lock(lock, 5, "owner"):
            other = lock.open("a+", encoding="utf-8")
            try:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(other.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                other.close()

    def test_timeout_zero_raises_immediately_when_busy(self) -> None:
        lock = self.tmp / "ops.lock"
        self._external_hold(lock)
        with self.assertRaises(TimeoutError) as ctx:
            with hold_lock(lock, 0, "second"):
                pass  # pragma: no cover - acquisition must not succeed
        self.assertIn(str(lock), str(ctx.exception))
        self.assertIn("0s", str(ctx.exception))

    def test_negative_timeout_is_clamped_to_zero(self) -> None:
        lock = self.tmp / "ops.lock"
        self._external_hold(lock)
        start = time.monotonic()
        with self.assertRaises(TimeoutError):
            with hold_lock(lock, -5, "second"):
                pass  # pragma: no cover
        self.assertLess(time.monotonic() - start, 0.5)  # no waiting

    def test_timeout_after_retry_loop(self) -> None:
        lock = self.tmp / "ops.lock"
        self._external_hold(lock)
        start = time.monotonic()
        with self.assertRaises(TimeoutError):
            with hold_lock(lock, 1, "second"):
                pass  # pragma: no cover
        # One BlockingIOError, a ~1s sleep, then the deadline trips.
        self.assertGreaterEqual(time.monotonic() - start, 0.9)

    def test_lock_released_after_exception_in_body(self) -> None:
        lock = self.tmp / "ops.lock"
        with self.assertRaises(ValueError):
            with hold_lock(lock, 5, "owner"):
                raise ValueError("boom")
        # finally released it, so a fresh acquisition with no wait succeeds.
        with hold_lock(lock, 0, "after"):
            payload = json.loads(lock.read_text(encoding="utf-8"))
        self.assertEqual(payload["owner"], "after")

    def test_sequential_reacquire(self) -> None:
        lock = self.tmp / "ops.lock"
        with hold_lock(lock, 5, "first"):
            pass
        with hold_lock(lock, 0, "second"):  # immediate — nothing holds it now
            payload = json.loads(lock.read_text(encoding="utf-8"))
        self.assertEqual(payload["owner"], "second")


if __name__ == "__main__":
    unittest.main(verbosity=2)
