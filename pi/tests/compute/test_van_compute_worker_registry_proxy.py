"""Worker wiring of the registry proxy, and the shipped Mac profile with it."""

from __future__ import annotations

import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import unittest

from unittest import mock

from macbook.scripts.van_compute_installer import constants as installer_constants
from macbook.scripts.van_compute_installer.runners import LocalRunner
from macbook.scripts.van_compute_installer.sandbox_proxy import (
    probe_proxy, run_proxy_probe,
)
from pi.tests.compute.van_compute_test_fixtures import (
    IPV6_LOOPBACK_SKIP,
    ipv6_loopback_available,
)
from van_compute import engine, registry_proxy, worker

REPO = Path(__file__).resolve().parents[3]

# Reaches the registry the way pip and npm do: through HTTPS_PROXY.
TUNNEL_JOB = """import json, os, socket, sys
from pathlib import Path
from urllib.parse import urlsplit

from van_compute.registry_proxy import client_hello_record

proxy = urlsplit(os.environ["HTTPS_PROXY"])
report = {"https_proxy": os.environ.get("https_proxy"), "no_proxy": "NO_PROXY" in os.environ}
with socket.create_connection((proxy.hostname, proxy.port), timeout=10) as connection:
    connection.sendall(b"CONNECT pypi.org:443 HTTP/1.1\\r\\nHost: pypi.org:443\\r\\n\\r\\n")
    head = b""
    while b"\\r\\n\\r\\n" not in head:
        head += connection.recv(4096)
    report["status"] = head.split(b"\\r\\n", 1)[0].decode()
    hello = client_hello_record("pypi.org")
    connection.sendall(hello)
    connection.shutdown(socket.SHUT_WR)
    echoed = b""
    while chunk := connection.recv(65536):
        echoed += chunk
    report["relayed"] = echoed == hello
try:
    socket.create_connection(("1.1.1.1", 443), timeout=5).close()
    report["direct"] = "allowed"
except OSError as exc:
    report["direct"] = exc.errno
print(json.dumps(report))
"""


def repo_manifest(job_id):
    return {
        "id": job_id,
        "task": "tunnel",
        "arguments": [],
        "inputs": [],
        "execution": {
            "profile": "python-script",
            "family": "python",
            "argv": ["{source:tunnel.py}"],
            "outputs": [],
            "datasets": [],
            "minimum_inputs": 0,
            "maximum_inputs": 0,
            "input_values": False,
        },
    }


class WorkerProxyWiringTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name).resolve()

    def run_job(self, **options):
        source = self.base / f"job-{len(list(self.base.iterdir()))}" / "source"
        source.mkdir(parents=True)
        (source / "tunnel.py").write_text("", encoding="utf-8")
        captured = {}

        def passthrough(command, **kwargs):
            captured.update(kwargs)
            return ["/usr/bin/true"]

        with mock.patch.object(worker, "sandbox_command", side_effect=passthrough), \
                mock.patch.object(worker, "process_group_resources", return_value=(0, 1)):
            worker.execute_job(
                repo_manifest("20260101T000000Z-00000001"),
                source_root=source, input_paths=[], input_values=[],
                result_root=source.parent / "result", python=sys.executable,
                timeout=30, nice=0, maximum_file_size=1024 * 1024,
                sandbox_profile=self.base / "sandbox.sb", **options,
            )
        return captured

    def test_job_gets_proxy_variables_and_port_only_with_a_running_proxy(self):
        captured = self.run_job(registry_proxy_port=4321)
        self.assertEqual(captured["proxy_port"], 4321)
        self.assertEqual(captured["environment"]["HTTPS_PROXY"], "http://127.0.0.1:4321")
        self.assertEqual(captured["environment"]["https_proxy"], "http://127.0.0.1:4321")
        for absent in ("HTTP_PROXY", "http_proxy", "NO_PROXY", "no_proxy"):
            self.assertNotIn(absent, captured["environment"])
        captured = self.run_job()
        self.assertIsNone(captured["proxy_port"])
        self.assertFalse({"HTTPS_PROXY", "https_proxy"} & set(captured["environment"]))

    def main_arguments(self, *extra):
        arguments = [
            "--worker", "test",
            "--work-root", str(self.base / "jobs"),
            "--task-cache-root", str(self.base / "task-caches"),
            *extra,
        ]
        for flag in ("--python", "--sqlite3", "--rg", "--jadx"):
            arguments += [flag, sys.executable]
        return arguments

    def observe_proxy(self, observed):
        def record(config, *_args, **_kwargs):
            port = config.registry_proxy_port
            observed.append(port)
            if port is not None:
                socket.create_connection(("127.0.0.1", port), timeout=2).close()
            return {"ok": True}
        return record

    @unittest.skipUnless(ipv6_loopback_available(), IPV6_LOOPBACK_SKIP)
    def test_main_runs_the_proxy_around_run_once_only_with_a_sandbox_profile(self):
        profile = self.base / "sandbox.sb"
        profile.write_text("(version 1)\n", encoding="utf-8")
        observed = []
        with mock.patch.object(worker, "run_once", side_effect=self.observe_proxy(observed)), \
                contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(worker.main(self.main_arguments("--run-once")), 0)
            self.assertEqual(worker.main(self.main_arguments(
                "--run-once", "--sandbox-profile", str(profile)
            )), 0)
        self.assertIsNone(observed[0])
        self.assertIsInstance(observed[1], int)
        with self.assertRaises(ConnectionRefusedError):
            socket.create_connection(("127.0.0.1", observed[1]), timeout=2).close()

    @unittest.skipUnless(ipv6_loopback_available(), IPV6_LOOPBACK_SKIP)
    def test_main_keeps_the_proxy_up_for_the_persistent_scheduler(self):
        profile = self.base / "sandbox.sb"
        profile.write_text("(version 1)\n", encoding="utf-8")
        observed = []
        with mock.patch.object(worker, "run_scheduler", side_effect=self.observe_proxy(observed)), \
                mock.patch.object(worker.SSHMultiplexer, "close"), \
                mock.patch.object(worker.signal, "signal"), \
                contextlib.redirect_stdout(io.StringIO()), \
                contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(worker.main(self.main_arguments(
                "--serve", "--sandbox-profile", str(profile)
            )), 0)
        self.assertIsInstance(observed[0], int)

    def test_a_proxy_that_cannot_bind_fails_worker_startup(self):
        profile = self.base / "sandbox.sb"
        profile.write_text("(version 1)\n", encoding="utf-8")
        error = io.StringIO()
        with mock.patch.object(
            registry_proxy, "bind_dual_stack", side_effect=OSError("no loopback")
        ), mock.patch.object(worker, "run_once") as run_once, \
                contextlib.redirect_stderr(error):
            status = worker.main(self.main_arguments(
                "--run-once", "--sandbox-profile", str(profile)
            ))
        self.assertEqual(status, 2)
        self.assertIn("no loopback", error.getvalue())
        run_once.assert_not_called()

    def test_profile_without_a_proxy_port_is_refused(self):
        profile = self.base / "sandbox.sb"
        profile.write_text("(version 1)\n", encoding="utf-8")
        for port in (None, 0, 70000):
            with self.subTest(port=port), self.assertRaisesRegex(
                RuntimeError, "registry proxy port"
            ):
                engine._mac_sandbox_command(
                    ["/usr/bin/true"], profile=profile, job_root=self.base,
                    source_root=self.base, result_root=self.base,
                    environment={"HOME": "h", "TMPDIR": "t"}, datasets={},
                    worker_root=self.base, error_type=RuntimeError, proxy_port=port,
                )


def homebrew_interpreter():
    interpreter = os.path.realpath(getattr(sys, "_base_executable", sys.executable))
    return interpreter if interpreter.startswith("/opt/homebrew/") else None


@unittest.skipUnless(
    sys.platform == "darwin" and Path("/usr/bin/sandbox-exec").exists(),
    "the Seatbelt profile only exists on macOS",
)
class MacSandboxRegistryProxyTests(unittest.TestCase):
    def setUp(self):
        trial = subprocess.run(
            ["/usr/bin/sandbox-exec", "-p", "(version 1)(allow default)", "/usr/bin/true"],
            capture_output=True,
        )
        if trial.returncode != 0:
            self.skipTest("sandbox-exec cannot be applied from this process")
        # The shipped profile reads /opt/homebrew but not an arbitrary venv.
        self.interpreter = homebrew_interpreter()
        if self.interpreter is None:
            self.skipTest("needs a Homebrew Python the profile can execute")
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name).resolve()
        self.profile = self.base / "sandbox.sb"
        self.profile.write_text(installer_constants.SANDBOX_PROFILE, encoding="utf-8")

    def staged_release(self):
        """A release-shaped tree the profile's WORKER_ROOT grant covers."""
        staging = self.base / "release"
        (staging / "venv/bin").mkdir(parents=True)
        (staging / "venv/bin/python").symlink_to(self.interpreter)
        shutil.copytree(
            REPO / "van_compute", staging / "app/van_compute",
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        return staging

    def test_installer_proxy_probe_passes_under_the_shipped_profile(self):
        staging = self.staged_release()
        job = self.base / "job"
        job.mkdir()
        local = LocalRunner()
        with probe_proxy(local, staging) as ports:
            prefix = engine._mac_sandbox_command(
                [], profile=self.profile, job_root=job, source_root=job,
                result_root=job, environment={"HOME": str(job), "TMPDIR": f"{job}/"},
                datasets={}, worker_root=staging, error_type=RuntimeError,
                proxy_port=ports.proxy,
            )
            run_proxy_probe(local, staging, job, prefix + ["/usr/bin/env", "-i"], ports)

    def test_sandboxed_job_reaches_a_registry_only_through_the_worker_proxy(self):
        upstream = socket.create_server(("127.0.0.1", 0))
        self.addCleanup(upstream.close)

        def echo_once():
            connection, _peer = upstream.accept()
            with connection:
                while data := connection.recv(65536):
                    connection.sendall(data)

        threading.Thread(target=echo_once, daemon=True).start()
        port = upstream.getsockname()[1]
        proxy = registry_proxy.RegistryProxy(
            resolver=lambda host, _port: [("127.0.0.1", port)], log=lambda _message: None
        )
        proxy.start()
        self.addCleanup(proxy.stop)
        source = self.base / "job" / "source"
        source.mkdir(parents=True)
        (source / "tunnel.py").write_text(TUNNEL_JOB, encoding="utf-8")
        shutil.copytree(
            REPO / "van_compute", source / "van_compute",
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        result = source.parent / "result"
        with mock.patch.object(worker, "process_group_resources", return_value=(0, 1)):
            exit_code, _execution = worker.execute_job(
                repo_manifest("20260101T000000Z-00000002"),
                source_root=source, input_paths=[], input_values=[], result_root=result,
                python=self.interpreter, timeout=60, nice=0, maximum_file_size=1024 * 1024,
                sandbox_profile=self.profile, registry_proxy_port=proxy.port,
            )
        stderr = (result / "stderr.txt").read_text(encoding="utf-8")
        self.assertEqual(exit_code, 0, stderr)
        report = json.loads((result / "stdout.txt").read_text(encoding="utf-8"))
        self.assertEqual(report["status"], "HTTP/1.1 200 Connection Established")
        self.assertTrue(report["relayed"])
        self.assertEqual(report["https_proxy"], f"http://127.0.0.1:{proxy.port}")
        self.assertFalse(report["no_proxy"])
        self.assertIn(report["direct"], (1, 13))  # EPERM or EACCES


if __name__ == "__main__":
    unittest.main()
