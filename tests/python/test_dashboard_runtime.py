"""Exercise real dashboard entrypoint and launcher only in isolated fixtures.

The launcher copy changes only fixed service port literals. All listeners bind
loopback ephemeral ports; no operator services, environment, or data are used.
"""
import contextlib
import http.server
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
NODE = os.environ.get("NODE_BIN") or shutil.which("node") or "node"
NODE22 = os.environ.get("NODE22_BIN") or shutil.which("node22") or ""


def unused_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@contextlib.contextmanager
def listener(port=0, body=None):
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(json.dumps(body or {"status": "ok"}).encode())

        def log_message(self, format, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_port
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def responds(port):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=.2):
            return True
    except (OSError, urllib.error.URLError):
        return False


class DashboardRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="dashboard-runtime-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / "server").mkdir()
        (self.root / "scripts").mkdir()
        for name in ("yahoo_proxy.js", "level_converter.js", "node_runtime.js"):
            source = ROOT / "server" / name
            if source.exists():
                shutil.copy2(source, self.root / "server" / name)
        shutil.copy2(ROOT / "package.json", self.root / "package.json")
        # Do not inherit operator database paths, service URLs, or Node options.
        self.env = dict(PATH=os.environ.get("PATH", ""), HOME=str(self.root),
                        TMPDIR=str(self.root), HOST="127.0.0.1", NODE_BIN=NODE,
                        LIVE_COLLECTOR_ENABLED="0", STARTUP_TIMEOUT_SEC="3",
                        MONITOR_INTERVAL_SEC="1")
        # Explicit interpreter stub: migrations cannot touch any real database.
        (self.root / "scripts/_pybin.sh").write_text("PYTHON_BIN=/usr/bin/true\n")
        self.children = []
        self.logs = []
        self.addCleanup(self.stop_children)

    def stop_children(self):
        for proc in reversed(self.children):
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=3)
        for log in self.logs:
            log.close()

    def spawn(self, args, env=None):
        log = open(self.root / f"child-{len(self.children)}.log", "w+")
        self.logs.append(log)
        proc = subprocess.Popen(args, cwd=self.root, env=env or self.env,
                                stdout=log, stderr=subprocess.STDOUT)
        self.children.append(proc)
        return proc, log

    def output(self, log):
        log.flush()
        return Path(log.name).read_text()

    def await_text(self, proc, log, text, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            output = self.output(log)
            if text in output:
                return output
            if proc.poll() is not None:
                break
            time.sleep(.05)
        self.fail(f"Missing {text!r}; exit={proc.poll()}; output={self.output(log)}")

    def test_actual_node22_entrypoint_rejected_before_listen(self):
        if not NODE22 or not Path(NODE22).is_file():
            self.skipTest("Set NODE22_BIN for actual unsupported-runtime regression")
        port = unused_port()
        proc, log = self.spawn([NODE22, "server/yahoo_proxy.js"],
                               dict(self.env, PORT=str(port)))
        try:
            code = proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            self.fail(f"Unsupported Node entrypoint stayed alive; listening={responds(port)}")
        self.assertNotEqual(code, 0)
        self.assertIn(">=24.22.1 <25", self.output(log))
        self.assertFalse(responds(port))

    def test_entrypoint_policy_boundaries(self):
        # Override only the version string in a child, exercising the real entrypoint.
        for version in ("24.22.0", "24.21.99", "23.99.99", "25.0.0", "26.10.0"):
            with self.subTest(version=version):
                port = unused_port()
                preload = self.root / "version.cjs"
                preload.write_text(f"Object.defineProperty(process.versions, 'node', {{value: '{version}'}});\n")
                proc, log = self.spawn([NODE, "--require", str(preload), "server/yahoo_proxy.js"],
                                       dict(self.env, PORT=str(port)))
                try:
                    code = proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    self.fail(f"Version {version} not rejected before listen")
                self.assertNotEqual(code, 0)
                self.assertIn(">=24.22.1 <25", self.output(log))
                self.assertFalse(responds(port))

    def test_entrypoint_allows_supported_patch_and_minor_versions(self):
        for version in ("24.22.1", "24.22.2", "24.23.0"):
            with self.subTest(version=version):
                port = unused_port()
                preload = self.root / "supported-version.cjs"
                preload.write_text(f"Object.defineProperty(process.versions, 'node', {{value: '{version}'}});\n")
                proc, log = self.spawn([NODE, "--require", str(preload), "server/yahoo_proxy.js"],
                                       dict(self.env, PORT=str(port)))
                deadline = time.monotonic() + 3
                while not responds(port) and proc.poll() is None and time.monotonic() < deadline:
                    time.sleep(.05)
                self.assertTrue(responds(port), self.output(log))
                proc.terminate()
                proc.wait(timeout=5)

    def test_entrypoint_reads_repo_policy_and_fails_closed_for_unknown_format(self):
        package = json.loads((self.root / "package.json").read_text())
        for policy in (">=24.99.0 <25", "unrecognized-range"):
            with self.subTest(policy=policy):
                package["engines"]["node"] = policy
                (self.root / "package.json").write_text(json.dumps(package))
                port = unused_port()
                proc, log = self.spawn([NODE, "server/yahoo_proxy.js"],
                                       dict(self.env, PORT=str(port)))
                self.assertNotEqual(proc.wait(timeout=3), 0)
                self.assertIn(policy, self.output(log))
                self.assertFalse(responds(port))

    @contextlib.contextmanager
    def launcher(self, dashboard_port, wrapper=False):
        with contextlib.ExitStack() as stack:
            ports = {str(p): stack.enter_context(listener()) for p in (5001, 5002, 5003, 5004)}
            ports["3000"] = dashboard_port
            source = (ROOT / "server/run_all.sh").read_text()
            for old, new in ports.items():
                source = source.replace(old, str(new))
            (self.root / "server/run_all.sh").write_text(source)
            env = dict(self.env, PORT=str(dashboard_port))
            if wrapper:
                script = self.root / "node-wrapper"
                script.write_text(f'''#!/bin/bash
if [[ "${{1:-}}" == "server/yahoo_proxy.js" ]]; then
  touch "{self.root}/launch-requested"
  while [[ ! -f "{self.root}/launch-allowed" ]]; do sleep .05; done
fi
exec "{NODE}" "$@"
''')
                script.chmod(0o755)
                env["NODE_BIN"] = str(script)
            proc, log = self.spawn(["bash", "server/run_all.sh"], env)
            try:
                yield proc, log, ports
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    proc.wait(timeout=8)

    def assert_refused(self, proc, log, port):
        try:
            code = proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            self.fail(f"Launcher adopted occupied dashboard: {self.output(log)}")
        output = self.output(log)
        self.assertNotEqual(code, 0, output)
        self.assertNotIn("All services ready.", output)
        self.assertNotIn("dashboard ready on port", output)
        self.assertTrue(responds(port), "Pre-existing listener was disturbed")
        self.assertIn("dashboard", output)

    def test_launcher_refuses_old_unrelated_and_no_metadata_listeners(self):
        for body in ({"node_version": "v22.23.3"}, {"node_version": "v24.22.1"},
                     {"service": "unrelated"}, {"status": "ok"}):
            with self.subTest(body=body), listener(body=body) as port:
                with self.launcher(port) as (proc, log, _ports):
                    self.assert_refused(proc, log, port)
                    self.assertIn("restart", self.output(log).lower())
                    self.assertNotIn("Running DB migrations", self.output(log))

    def test_launcher_refuses_actual_node22_listener_without_killing_it(self):
        if not NODE22 or not Path(NODE22).is_file():
            self.skipTest("Set NODE22_BIN for actual old listener regression")
        port = unused_port()
        old, old_log = self.spawn([NODE22, "-e", "require('http').createServer((q,s)=>s.end('{}')).listen(" + str(port) + ", '127.0.0.1', ()=>console.log('bound'))"])
        self.await_text(old, old_log, "bound")
        with self.launcher(port) as (proc, log, _ports):
            self.assert_refused(proc, log, port)
            self.assertIsNone(old.poll())

    def test_actual_node22_launcher_rejected_before_migrations(self):
        if not NODE22 or not Path(NODE22).is_file():
            self.skipTest("Set NODE22_BIN for actual unsupported-runtime regression")
        self.env["NODE_BIN"] = NODE22
        with self.launcher(unused_port()) as (proc, log, _ports):
            self.assertNotEqual(proc.wait(timeout=3), 0)
            self.assertIn(">=24.22.1 <25", self.output(log))
            self.assertNotIn("Running DB migrations", self.output(log))

    def test_supported_launcher_startup_and_shutdown_owns_only_dashboard(self):
        port = unused_port()
        with self.launcher(port) as (proc, log, ports):
            self.await_text(proc, log, "All services ready.")
            self.assertTrue(responds(port))
            proc.terminate()
            self.assertEqual(proc.wait(timeout=8), 0)
            deadline = time.monotonic() + 7
            while responds(port) and time.monotonic() < deadline:
                time.sleep(.05)
            self.assertFalse(responds(port), "Owned dashboard survived shutdown")
            for service in ("5001", "5002", "5003", "5004"):
                self.assertTrue(responds(ports[service]), "Unowned service disturbed")
            self.assertFalse((self.root / "logs/run_all.lock").exists())

    def test_occupied_race_never_declares_foreign_listener_ready(self):
        port = unused_port()
        with self.launcher(port, wrapper=True) as (proc, log, _ports):
            deadline = time.monotonic() + 5
            while not (self.root / "launch-requested").exists() and time.monotonic() < deadline:
                time.sleep(.05)
            self.assertTrue((self.root / "launch-requested").exists(), self.output(log))
            with listener(port=port):
                (self.root / "launch-allowed").touch()
                self.assert_refused(proc, log, port)

    def test_shell_syntax(self):
        subprocess.run(["bash", "-n", str(ROOT / "server/run_all.sh")], check=True)


if __name__ == "__main__":
    unittest.main()
