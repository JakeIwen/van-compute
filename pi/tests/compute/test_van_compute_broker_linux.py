from pathlib import Path
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest

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
