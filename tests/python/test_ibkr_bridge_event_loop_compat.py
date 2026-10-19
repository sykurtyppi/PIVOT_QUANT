"""Compatibility checks for importing the IBKR bridge on modern Python."""
from __future__ import annotations

import os
import subprocess
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


class IbkrBridgeEventLoopCompatibilityTest(unittest.TestCase):
    def test_import_bootstraps_event_loop_when_runtime_has_none(self) -> None:
        """eventkit still expects a current loop at import time on Python 3.13+."""
        script = """
import asyncio
asyncio.set_event_loop(None)
import server.ibkr_gamma_bridge
loop = asyncio.get_event_loop_policy().get_event_loop()
assert not loop.is_closed()
loop.close()
"""
        env = os.environ.copy()
        env["IB_EXPIRY_MODE"] = "90dte"
        proc = subprocess.run(
            [sys.executable, "-c", script],
            cwd=REPO_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, msg=f"{proc.stdout}\n{proc.stderr}")


if __name__ == "__main__":
    unittest.main()
