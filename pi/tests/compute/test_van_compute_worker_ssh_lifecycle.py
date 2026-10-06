import io
from pathlib import Path
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock

from van_compute import worker


class LiveProcess:
    def __init__(self):
        self.stderr = io.BytesIO()
        self.terminated = False
        self.killed = False

    def poll(self):
        return 0 if self.terminated or self.killed else None

    def terminate(self):
        self.terminated = True

    def wait(self, timeout=None):
        return self.poll()

    def kill(self):
        self.killed = True


class SharedSSHLifecycleTests(unittest.TestCase):
    def multiplexer(self, root):
        control_path = Path(root) / "control.sock"
        control_path.write_bytes(b"")
        multiplexer = worker.SSHMultiplexer("fake", control_path)
        process = LiveProcess()
        multiplexer._process = process
        return multiplexer, process

    def test_caller_failure_does_not_stop_live_sibling_transfer(self):
        failures = (
            ("timeout", lambda: subprocess.TimeoutExpired("ssh", 1)),
            ("oserror", lambda: OSError("caller failed")),
        )
        for transfer in ("put-result", "stream"):
            for failure_name, make_failure in failures:
                with self.subTest(transfer=transfer, failure=failure_name):
                    self._assert_caller_failure_preserves_transfer(
                        transfer, make_failure
                    )

    def _assert_caller_failure_preserves_transfer(self, transfer, make_failure):
        with tempfile.TemporaryDirectory() as name:
            multiplexer, process = self.multiplexer(name)
            sibling = worker.RemoteQueue(
                "fake", "/fake/queue", "mac.00", multiplexer=multiplexer
            )
            caller = worker.RemoteQueue(
                "fake", "/fake/queue", "mac.01", multiplexer=multiplexer
            )
            transfer_started = threading.Event()
            release_transfer = threading.Event()
            errors = []
            destination = Path(name) / "download"
            source = Path(name) / "result"
            source.write_bytes(b"result")

            def transport(arguments, **kwargs):
                if "-O" in arguments:
                    self.assertEqual(arguments[arguments.index("-O") + 1], "check")
                    return subprocess.CompletedProcess(arguments, 0, b"", b"")
                remote_command = arguments[-1]
                if " put-result " in remote_command or " stream " in remote_command:
                    transfer_started.set()
                    if not release_transfer.wait(2):
                        raise AssertionError("test transfer was not released")
                    output = kwargs.get("stdout")
                    if output is not None:
                        output.write(b"payload")
                    return subprocess.CompletedProcess(arguments, 0, b"{}", b"")
                if " heartbeat " in remote_command:
                    raise make_failure()
                raise AssertionError(f"unexpected remote command: {remote_command}")

            def run_transfer():
                try:
                    if transfer == "put-result":
                        sibling.put_result("job", "result", source)
                    else:
                        sibling.stream_to_file(
                            ["worker", "stream", "job"], destination
                        )
                except Exception as exc:
                    errors.append(exc)

            with mock.patch.object(worker.subprocess, "run", side_effect=transport):
                thread = threading.Thread(target=run_transfer)
                thread.start()
                try:
                    self.assertTrue(transfer_started.wait(1))
                    with self.assertRaises(worker.TransportUnavailable):
                        caller.heartbeat()
                    self.assertIs(multiplexer._process, process)
                    self.assertFalse(process.terminated)
                    self.assertFalse(process.killed)
                    self.assertTrue(thread.is_alive())
                finally:
                    release_transfer.set()
                    thread.join(2)

            self.assertFalse(thread.is_alive())
            self.assertEqual(errors, [])
            if transfer == "stream":
                self.assertEqual(destination.read_bytes(), b"payload")

    def test_failed_master_check_invalidates_transport(self):
        with tempfile.TemporaryDirectory() as name:
            multiplexer, process = self.multiplexer(name)
            remote = worker.RemoteQueue(
                "fake", "/fake/queue", "mac.00", multiplexer=multiplexer
            )
            checks = 0

            def transport(arguments, **_kwargs):
                nonlocal checks
                if "-O" in arguments:
                    operation = arguments[arguments.index("-O") + 1]
                    if operation == "check":
                        checks += 1
                        return subprocess.CompletedProcess(
                            arguments, 0 if checks == 1 else 1, b"", b""
                        )
                    self.assertEqual(operation, "exit")
                    return subprocess.CompletedProcess(arguments, 0, b"", b"")
                raise OSError("caller failed")

            with mock.patch.object(worker.subprocess, "run", side_effect=transport):
                with self.assertRaises(worker.TransportUnavailable):
                    remote.heartbeat()

            self.assertEqual(checks, 2)
            self.assertTrue(process.terminated)
            self.assertIsNone(multiplexer._process)
            self.assertFalse(multiplexer.control_path.exists())

    def test_exit_255_still_invalidates_transport(self):
        with tempfile.TemporaryDirectory() as name:
            multiplexer, process = self.multiplexer(name)
            remote = worker.RemoteQueue(
                "fake", "/fake/queue", "mac.00", multiplexer=multiplexer
            )

            def transport(arguments, **_kwargs):
                if "-O" in arguments:
                    operation = arguments[arguments.index("-O") + 1]
                    return subprocess.CompletedProcess(arguments, 0, b"", b"")
                return subprocess.CompletedProcess(arguments, 255, "", "offline")

            with mock.patch.object(worker.subprocess, "run", side_effect=transport):
                with self.assertRaisesRegex(worker.TransportUnavailable, "offline"):
                    remote.heartbeat()

            self.assertTrue(process.terminated)
            self.assertIsNone(multiplexer._process)
            self.assertFalse(multiplexer.control_path.exists())

    def test_master_check_and_lock_wait_are_bounded(self):
        with tempfile.TemporaryDirectory() as name:
            multiplexer, process = self.multiplexer(name)
            with mock.patch.object(worker.time, "monotonic", return_value=100.0), \
                    mock.patch.object(
                        worker.subprocess,
                        "run",
                        return_value=subprocess.CompletedProcess([], 0),
                    ) as run:
                self.assertTrue(multiplexer._check_locked(200.0))
                self.assertTrue(multiplexer._check_locked(103.0))
            self.assertEqual(
                [call.kwargs["timeout"] for call in run.call_args_list],
                [worker.CONTROL_MASTER_CHECK_TIMEOUT, 2.0],
            )

            multiplexer._lock.acquire()
            started = time.monotonic()
            try:
                with self.assertRaises(subprocess.TimeoutExpired):
                    multiplexer.invalidate_if_unhealthy(time.monotonic() + 0.02)
            finally:
                multiplexer._lock.release()
            self.assertLess(time.monotonic() - started, 0.5)
            self.assertIs(multiplexer._process, process)
            self.assertFalse(process.terminated)

    def test_interrupted_existing_master_check_does_not_stop_master(self):
        for interruption in (
            worker.WorkerShutdown("stop requested"),
            subprocess.TimeoutExpired("check", 0),
        ):
            with self.subTest(interruption=type(interruption).__name__), \
                    tempfile.TemporaryDirectory() as name:
                multiplexer, process = self.multiplexer(name)
                with mock.patch.object(
                    multiplexer, "_check_locked", side_effect=interruption
                ):
                    with self.assertRaises(type(interruption)):
                        multiplexer.ensure(time.monotonic() + 1)
                self.assertIs(multiplexer._process, process)
                self.assertFalse(process.terminated)
                self.assertTrue(multiplexer.control_path.exists())


if __name__ == "__main__":
    unittest.main()
