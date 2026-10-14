"""Installer contracts for the canonical levels-product launch agents."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "scripts" / "levels_product" / "install_levels_product_agents.sh"
LEGACY_LABEL = "com.pivotquant.levels-alerts"


class LevelsProductAgentInstallerTest(unittest.TestCase):
    def _run_installer(
        self,
        action: str,
        *,
        webhook: str | None = None,
        isolated_project_without_python: bool = False,
    ) -> tuple[subprocess.CompletedProcess[str], Path, str]:
        td = tempfile.TemporaryDirectory(prefix="pq_levels_agents_")
        self.addCleanup(td.cleanup)
        home = Path(td.name)
        fake_bin = home / "bin"
        fake_bin.mkdir()
        launchctl_log = home / "launchctl.log"
        fake_launchctl = fake_bin / "launchctl"
        fake_launchctl.write_text(
            "#!/usr/bin/env bash\n"
            "printf '%s\\n' \"$*\" >> \"${FAKE_LAUNCHCTL_LOG}\"\n"
            "exit 0\n",
            encoding="utf-8",
        )
        fake_launchctl.chmod(0o755)

        launch_agents = home / "Library" / "LaunchAgents"
        launch_agents.mkdir(parents=True)
        (launch_agents / f"{LEGACY_LABEL}.plist").write_text("legacy", encoding="utf-8")
        for label in ("com.pivotquant.levels_daily", "com.pivotquant.levels_intraday"):
            (launch_agents / f"{label}.plist").write_text("canonical", encoding="utf-8")

        installer = INSTALLER
        cwd = ROOT
        if isolated_project_without_python:
            cwd = home / "project"
            installer = cwd / "scripts" / "levels_product" / INSTALLER.name
            installer.parent.mkdir(parents=True)
            shutil.copy2(INSTALLER, installer)

        env = os.environ.copy()
        env.pop("LEVELS_PRODUCT_WEBHOOK_URL", None)
        env.pop("PYTHON_BIN", None)
        env.update(
            {
                "HOME": str(home),
                "PATH": f"{fake_bin}:{env['PATH']}",
                "FAKE_LAUNCHCTL_LOG": str(launchctl_log),
            }
        )
        if not isolated_project_without_python:
            # Installation needs a Python path in the generated intraday plist.
            # Supply the test runner explicitly rather than inheriting an
            # operator's PYTHON_BIN or depending on a repository-local venv.
            env["PYTHON_BIN"] = sys.executable
        if webhook is not None:
            env["LEVELS_PRODUCT_WEBHOOK_URL"] = webhook
        result = subprocess.run(
            ["bash", str(installer), action],
            cwd=cwd,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        log = launchctl_log.read_text(encoding="utf-8") if launchctl_log.exists() else ""
        return result, launch_agents, log

    def test_install_migrates_away_from_legacy_levels_alert_agent(self) -> None:
        result, launch_agents, log = self._run_installer("install-daily")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"bootout gui/{os.getuid()}/{LEGACY_LABEL}", log)
        self.assertFalse((launch_agents / f"{LEGACY_LABEL}.plist").exists())
        self.assertIn("removed legacy levels-alert agent", result.stdout)

    def test_install_without_webhook_makes_intraday_agent_explicitly_dry_run(self) -> None:
        result, launch_agents, _ = self._run_installer("install")

        self.assertEqual(result.returncode, 0, result.stderr)
        intraday_plist = (
            launch_agents / "com.pivotquant.levels_intraday.plist"
        ).read_text(encoding="utf-8")
        daily_plist = (
            launch_agents / "com.pivotquant.levels_daily.plist"
        ).read_text(encoding="utf-8")
        self.assertIn("<string>--dry-run</string>", intraday_plist)
        self.assertIn("<key>LEVELS_CHANNEL</key><string>dryrun</string>", daily_plist)
        self.assertIn("agents dry-run", result.stdout)

    def test_install_with_webhook_succeeds_without_intraday_dry_run(self) -> None:
        result, launch_agents, _ = self._run_installer(
            "install", webhook="https://example.invalid/levels"
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        intraday_plist = (
            launch_agents / "com.pivotquant.levels_intraday.plist"
        ).read_text(encoding="utf-8")
        daily_plist = (
            launch_agents / "com.pivotquant.levels_daily.plist"
        ).read_text(encoding="utf-8")
        self.assertNotIn("<string>--dry-run</string>", intraday_plist)
        self.assertIn("<key>LEVELS_CHANNEL</key><string>webhook</string>", daily_plist)

    def test_whitespace_only_webhook_is_treated_as_unconfigured(self) -> None:
        result, launch_agents, _ = self._run_installer("install", webhook="   ")

        self.assertEqual(result.returncode, 0, result.stderr)
        intraday_plist = (
            launch_agents / "com.pivotquant.levels_intraday.plist"
        ).read_text(encoding="utf-8")
        daily_plist = (
            launch_agents / "com.pivotquant.levels_daily.plist"
        ).read_text(encoding="utf-8")
        self.assertIn("<string>--dry-run</string>", intraday_plist)
        self.assertIn("<key>LEVELS_CHANNEL</key><string>dryrun</string>", daily_plist)

    def test_uninstall_removes_legacy_and_canonical_agents(self) -> None:
        result, launch_agents, log = self._run_installer(
            "uninstall", isolated_project_without_python=True
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"bootout gui/{os.getuid()}/{LEGACY_LABEL}", log)
        self.assertFalse((launch_agents / f"{LEGACY_LABEL}.plist").exists())
        self.assertFalse((launch_agents / "com.pivotquant.levels_daily.plist").exists())
        self.assertFalse((launch_agents / "com.pivotquant.levels_intraday.plist").exists())
        self.assertIn("uninstalled levels-product agents", result.stdout)


if __name__ == "__main__":
    unittest.main()
