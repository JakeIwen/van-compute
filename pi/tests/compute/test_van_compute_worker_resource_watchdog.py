"""Mac resource watchdog: tolerance for a slow `ps` process-table sample."""

import subprocess
import unittest
from unittest import mock

from van_compute import worker


class ResourceWatchdogPsTimeoutTests(unittest.TestCase):
    def _sample_process_table(self, clock: list[float], outcomes: list[object]):
        """Run one reader call at clock[0]; each outcome is stdout or an exception."""
        def fake_run(*_args, **_kwargs):
            outcome = outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            if isinstance(outcome, subprocess.CompletedProcess):
                return outcome
            return subprocess.CompletedProcess([], 0, outcome, "")

        with mock.patch.object(worker.time, "monotonic", side_effect=lambda: clock[0]), \
                mock.patch.object(worker.subprocess, "run", side_effect=fake_run) as run:
            return worker.process_group_resources(101), run.call_count

    def _reset_process_table(self):
        worker._PROCESS_TABLE_GROUPS = {}
        worker._PROCESS_TABLE_SAMPLED_AT = 0.0
        worker._PROCESS_TABLE_TIMED_OUT_AT = None
        self.addCleanup(self._clear_process_table)

    def _clear_process_table(self):
        worker._PROCESS_TABLE_GROUPS = {}
        worker._PROCESS_TABLE_SAMPLED_AT = 0.0
        worker._PROCESS_TABLE_TIMED_OUT_AT = None

    def test_resource_watchdog_tolerates_ps_timeout_with_recent_sample(self):
        self._reset_process_table()
        clock = [1000.0]
        timeout = subprocess.TimeoutExpired(["/bin/ps"], worker.PROCESS_TABLE_TIMEOUT)
        self.assertEqual(self._sample_process_table(clock, ["101 100\n"]), ((100 * 1024, 1), 1))

        clock[0] = 1002.0
        self.assertEqual(self._sample_process_table(clock, [timeout]), ((100 * 1024, 1), 1))

        # Callers right after a timeout reuse the table instead of queueing
        # another five-second ps on the shared lock.
        clock[0] = 1002.5
        self.assertEqual(self._sample_process_table(clock, []), ((100 * 1024, 1), 0))

        clock[0] = 1004.0
        self.assertEqual(self._sample_process_table(clock, ["101 400\n"]), ((400 * 1024, 1), 1))
        self.assertIsNone(worker._PROCESS_TABLE_TIMED_OUT_AT)

    def test_resource_watchdog_fails_closed_once_sample_is_too_old(self):
        self._reset_process_table()
        clock = [1000.0]
        timeout = subprocess.TimeoutExpired(["/bin/ps"], worker.PROCESS_TABLE_TIMEOUT)
        self._sample_process_table(clock, ["101 100\n"])

        clock[0] = 1000.0 + worker.PROCESS_TABLE_STALE_LIMIT + 1
        with self.assertRaisesRegex(worker.WorkerError, "resource watchdog could not run ps"):
            self._sample_process_table(clock, [timeout])

    def test_resource_watchdog_fails_closed_on_timeout_without_prior_sample(self):
        self._reset_process_table()
        timeout = subprocess.TimeoutExpired(["/bin/ps"], worker.PROCESS_TABLE_TIMEOUT)
        with self.assertRaisesRegex(worker.WorkerError, "resource watchdog could not run ps"):
            self._sample_process_table([1000.0], [timeout])

    def test_resource_watchdog_fails_closed_on_ps_error_despite_recent_sample(self):
        self._reset_process_table()
        clock = [1000.0]
        self._sample_process_table(clock, ["101 100\n"])
        clock[0] = 1002.0
        failed = subprocess.CompletedProcess([], 1, "", "ps: broken")
        with self.assertRaisesRegex(worker.WorkerError, "ps: broken"):
            self._sample_process_table(clock, [failed])
        with self.assertRaisesRegex(worker.WorkerError, "could not run ps"):
            self._sample_process_table(clock, [OSError("no ps")])


if __name__ == "__main__":
    unittest.main()
