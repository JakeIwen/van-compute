from pathlib import Path
from types import SimpleNamespace
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from van_compute import broker, engine, worker


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class SharedEngineTests(unittest.TestCase):
    def wait(self, host, **overrides):
        values = dict(
            host=host,
            error_type=RuntimeError,
            timeout=1,
            should_stop=lambda: False,
            work_path=Path("/fake/work"),
            minimum_free_bytes=100,
            free_space_reader=lambda _path: 200,
            maximum_memory=1000,
            maximum_processes=10,
            resource_reader=lambda _pid: (0, 0),
            poll=lambda _process: (0, "usage"),
            clock=Clock(),
        )
        values.update(overrides)
        return engine.wait_for_process(SimpleNamespace(pid=77), **values)

    def test_pi_always_samples_disk_after_fast_exit_mac_does_not(self):
        disk = mock.Mock(return_value=99)
        pi = self.wait(engine.Host.PI, free_space_reader=disk)
        self.assertEqual(pi.exit_code, 137)
        self.assertEqual(pi.minimum_filesystem_free_bytes, 99)
        self.assertIn("execution safety threshold", pi.resource_limit)
        disk.reset_mock()
        mac = self.wait(engine.Host.MAC, free_space_reader=disk)
        self.assertEqual(mac.exit_code, 0)
        self.assertIsNone(mac.minimum_filesystem_free_bytes)
        disk.assert_not_called()

    def test_mac_watchdog_order_rss_precedes_count_and_disk(self):
        disk = mock.Mock()
        with mock.patch.object(engine.os, "killpg") as kill, mock.patch.object(
            engine.os, "wait4", return_value=(77, 0, "usage")
        ):
            result = self.wait(
                engine.Host.MAC,
                poll=lambda _p: None,
                resource_reader=lambda _p: (1001, 11),
                free_space_reader=disk,
            )
        self.assertEqual(result.exit_code, 137)
        self.assertEqual(result.resource_limit, "process-group RSS exceeded 1000 bytes")
        self.assertEqual(result.peak_process_count, 11)
        disk.assert_not_called()
        kill.assert_called_once_with(77, signal.SIGKILL)

    def test_empty_mac_watchdog_error_keeps_legacy_graceful_exit(self):
        with mock.patch.object(engine.os, "killpg") as kill, mock.patch.object(
            engine.os, "wait4", return_value=(77, 0, "usage")
        ):
            result = self.wait(
                engine.Host.MAC,
                poll=lambda _p: None,
                resource_reader=mock.Mock(side_effect=RuntimeError("")),
            )
        self.assertEqual(result.exit_code, 143)
        self.assertEqual(result.resource_monitor_error, "")
        self.assertEqual(
            kill.call_args_list,
            [mock.call(77, signal.SIGTERM), mock.call(77, signal.SIGKILL)],
        )

    def test_timeout_and_interruption_preserve_escalation_codes(self):
        for host in engine.Host:
            for interrupted, code in ((False, 124), (True, 143)):
                with self.subTest(
                    host=host, interrupted=interrupted
                ), mock.patch.object(engine.os, "killpg") as kill, mock.patch.object(
                    engine.os, "wait4", return_value=(77, 0, "usage")
                ):
                    result = self.wait(
                        host, poll=lambda _p: None, should_stop=lambda: interrupted
                    )
                self.assertEqual(result.exit_code, code)
                self.assertEqual(result.interrupted, interrupted)
                self.assertEqual(result.timed_out, not interrupted)
                self.assertEqual(
                    kill.call_args_list,
                    [mock.call(77, signal.SIGTERM), mock.call(77, signal.SIGKILL)],
                )

    def test_host_specific_process_group_permission_policy(self):
        with mock.patch.object(
            engine.os, "killpg", side_effect=PermissionError("denied")
        ):
            self.assertTrue(worker.process_group_exists(77))
            with self.assertRaisesRegex(
                broker.BrokerError, "cannot inspect analysis process group 77"
            ):
                broker._process_group_exists(77)

    def test_descendant_cleanup_has_host_specific_return_and_errors(self):
        for host in engine.Host:
            with self.subTest(host=host), mock.patch.object(
                engine.os, "killpg"
            ) as kill:
                with self.assertRaisesRegex(
                    RuntimeError, "analysis process group.*survived SIGKILL"
                ):
                    engine.terminate_group(
                        77,
                        exists=lambda _p: True,
                        error_type=RuntimeError,
                        host=host,
                        clock=Clock(),
                        terminate_grace=0.1,
                        kill_grace=0.1,
                    )
            self.assertEqual(
                kill.call_args_list,
                [mock.call(77, signal.SIGTERM), mock.call(77, signal.SIGKILL)],
            )

    def test_sandbox_interfaces_keep_host_path_and_runtime_mapping(self):
        root = Path("/private/job")
        sandbox: engine.Sandbox = engine.BubblewrapSandbox(
            sys.executable,
            root / "source",
            root / "inputs",
            root / "result",
            root / "home",
            root / "tmp",
            root / "cache",
            "job",
            RuntimeError,
        )
        self.assertEqual(
            sandbox.command_path(root / "source/tool.py", "source"),
            Path("/job/source/tool.py"),
        )
        self.assertEqual(
            sandbox.executable("/home/pi/venv/bin/python3", "python"),
            (
                "/job/runtime/python/bin/python3",
                [(Path("/home/pi/venv"), "/job/runtime/python")],
            ),
        )
        wrapped = sandbox.wrap(["/usr/bin/python3", "-m", "analysis"])
        self.assertIn("--unshare-net", wrapped)
        self.assertIn("--clearenv", wrapped)
        self.assertEqual(wrapped[-4:], ["--", "/usr/bin/python3", "-m", "analysis"])
        mac: engine.Sandbox = engine.MacSandbox(
            None,
            root,
            root / "source",
            root / "result",
            {},
            {},
            root / "worker",
            RuntimeError,
        )
        self.assertEqual(
            mac.command_path(root / "source/tool.py", "source"), root / "source/tool.py"
        )
        self.assertEqual(
            mac.wrap(["/bin/example", "one arg"]), ["/bin/example", "one arg"]
        )

    def test_shared_launch_preserves_redirects_environment_session_and_cwd(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            process = object()
            with mock.patch.object(
                engine.subprocess, "Popen", return_value=process
            ) as launch:
                result = engine.run_child(
                    ["/fake/python", "analysis"],
                    cwd=root,
                    environment={"HOME": "private"},
                    result_root=root,
                    supervise=lambda p: (p, "waited"),
                )
            self.assertEqual(result, (process, "waited"))
            self.assertTrue(launch.call_args.kwargs["start_new_session"])
            self.assertEqual(launch.call_args.kwargs["env"], {"HOME": "private"})
            self.assertEqual(launch.call_args.kwargs["cwd"], root)
            self.assertEqual(launch.call_args.kwargs["stdin"], subprocess.DEVNULL)
            self.assertEqual(
                sorted(p.name for p in root.iterdir()), ["stderr.txt", "stdout.txt"]
            )

    def test_result_component_guard_applies_to_both_hosts(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            (root / "outside").mkdir()
            (root / "link").symlink_to(root / "outside", target_is_directory=True)
            for check, error in (
                (broker._real_declared_output, broker.BrokerError),
                (worker.real_declared_output, worker.WorkerError),
            ):
                with self.subTest(check=check), self.assertRaisesRegex(
                    error, "symlinked path component"
                ):
                    check(root, Path("link/result.json"))


if __name__ == "__main__":
    unittest.main()
