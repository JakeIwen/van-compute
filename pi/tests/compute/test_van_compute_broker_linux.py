import os
import signal
import sys
import time
import unittest

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
