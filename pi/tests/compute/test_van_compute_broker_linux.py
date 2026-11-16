from pathlib import Path
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from van_compute.can_activity import probe_can_activity
from pi.tests.compute.van_compute_test_fixtures import BrokerHarness


@unittest.skipUnless(
    sys.platform.startswith("linux"),
    "Pi broker descendant cleanup requires Linux process-group inspection; "
    "macOS agent sandboxes can refuse orphan-group probes",
)
class LinuxDescendantCleanupTests(BrokerHarness, unittest.TestCase):
    def test_background_descendant_is_terminated_before_results_publish(self):
        submitted = self.submit(arguments=["--background"])
        result = self.run_once()
        self.assertTrue(result["ok"], result)
        stdout = (
            self.root / "done" / submitted["id"] / "result" / "stdout.txt"
        ).read_text(encoding="utf-8")
        child_pid = int(stdout.split("background=", 1)[1].splitlines()[0])
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                os.kill(child_pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.05)
        else:
            try:
                os.kill(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.fail("background child survived result publication")


HOLDER = """
import fcntl, sys, time
handle = open(sys.argv[1], "a+")
mode = sys.argv[2]
if mode == "posix":
    fcntl.lockf(handle, fcntl.LOCK_EX)
else:
    fcntl.flock(handle, {"ex": fcntl.LOCK_EX, "sh": fcntl.LOCK_SH}[mode])
print("ready", flush=True)
time.sleep(60)
"""


@unittest.skipUnless(
    sys.platform.startswith("linux"),
    "reads the Linux kernel's /proc/locks table; no equivalent on macOS",
)
class LinuxCanLockProbeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.lock_dir = Path(self.temporary.name)
        self.lock = self.lock_dir / "active-diagnostics-can0.lock"
        self.lock.write_text("")

    def hold(self, mode):
        process = subprocess.Popen(
            [sys.executable, "-c", HOLDER, str(self.lock), mode],
            stdout=subprocess.PIPE,
            text=True,
        )
        self.addCleanup(process.wait)
        self.addCleanup(process.kill)
        self.assertEqual(process.stdout.readline().strip(), "ready")
        return process

    def test_exclusive_flock_holder_is_seen_and_release_is_seen(self):
        self.assertTrue(probe_can_activity(self.lock_dir).quiet)
        process = self.hold("ex")
        result = probe_can_activity(self.lock_dir)
        self.assertTrue(result.active, result)
        self.assertEqual(result.held_locks, (self.lock.name,))
        process.kill()
        process.wait()
        self.assertTrue(probe_can_activity(self.lock_dir).quiet)

    def test_shared_flock_holder_is_ignored(self):
        self.hold("sh")
        result = probe_can_activity(self.lock_dir)
        self.assertTrue(result.quiet, result)

    def test_posix_write_lock_is_seen(self):
        self.hold("posix")
        result = probe_can_activity(self.lock_dir)
        self.assertTrue(result.active, result)

    def test_probe_does_not_make_a_senders_nonblocking_lock_fail(self):
        import fcntl

        probe_can_activity(self.lock_dir)
        with self.lock.open("a+") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)


@unittest.skipUnless(
    sys.platform.startswith("linux"),
    "Pi broker descendant cleanup requires Linux process-group inspection; "
    "macOS agent sandboxes can refuse orphan-group probes",
)
class LinuxCancelCleanupTests(BrokerHarness, unittest.TestCase):
    def test_a_cancelled_jobs_group_and_descendant_are_gone_before_it_is_final(self):
        import contextlib
        import io
        import json
        import threading

        from van_compute import job_cancel
        from van_compute.job_cleanup_marker import read_group_start
        from pi.tests.compute.test_van_compute_broker_cancel import SLEEPER

        marker = Path(self.temporary.name) / "sleeper-started"
        (self.source / "tools" / "local_job.py").write_text(
            SLEEPER.format(marker=str(marker)), encoding="utf-8"
        )
        job_id = self.submit()["id"]

        def cancel_when_started():
            deadline = time.monotonic() + 20
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(0.05)
            job_cancel.cancel_job(self.root, job_id)

        launched = {}
        original = job_cancel.finalize_cancelled

        def finalize(*args, **kwargs):
            group = launched["group"]
            try:
                os.killpg(group, 0)
                alive = read_group_start(group) == launched["leader_start"]
            except ProcessLookupError:
                alive = False
            launched["alive_at_finalize"] = alive
            return original(*args, **kwargs)

        thread = threading.Thread(target=cancel_when_started)
        thread.start()
        output = io.StringIO()
        original_write = output.write

        def capture(text):
            # The broker's launch line (pgid and leader start) is the proof.
            if '"job-launched"' in text:
                payload = json.loads(text)
                launched.update(group=payload["group"], leader_start=payload["leader_start"])
            return original_write(text)

        output.write = capture
        with contextlib.redirect_stdout(output), \
                mock.patch.object(job_cancel, "finalize_cancelled", finalize):
            result = self.run_once()
        thread.join()
        self.assertEqual(result["action"], "cancelled", result)
        self.assertIsNotNone(launched.get("leader_start"))
        self.assertFalse(launched["alive_at_finalize"])
        self.assertTrue((self.root / "cancelled" / job_id).is_dir())
        descendant = int(marker.read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(descendant, 0)
