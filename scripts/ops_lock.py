#!/usr/bin/env python3
"""Shared file lock helper for ops resilience scripts."""

from __future__ import annotations

import fcntl
import json
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator


@contextmanager
def hold_lock(lock_path: Path, timeout_sec: int, owner: str) -> Iterator[None]:
    """Acquire an exclusive lock with timeout and release on exit."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+", encoding="utf-8")
    start = time.monotonic()
    wait = max(0, int(timeout_sec))
    try:
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() - start >= wait:
                    raise TimeoutError(f"lock busy for {wait}s: {lock_path}")
                time.sleep(1.0)

        payload = {
            "owner": owner,
            "pid": os.getpid(),
            "acquired_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        }
        handle.seek(0)
        handle.truncate(0)
        handle.write(json.dumps(payload))
        handle.flush()

        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
