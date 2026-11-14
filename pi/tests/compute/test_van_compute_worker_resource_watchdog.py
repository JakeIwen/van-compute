"""Mac resource watchdog: tolerance for a slow `ps` process-table sample."""

import subprocess
import unittest
from unittest import mock

from van_compute import worker


class SlowPs:
    """A `ps` that times out after consuming the real timeout on the fake clock."""


SLOW_PS = SlowPs()


class ResourceWatchdogPsTimeoutTests(unittest.TestCase):
    def setUp(self):
        self._clear_process_table()
        self.addCleanup(self._clear_process_table)
        self.clock = [1000.0]

    def _clear_process_table(self):
        worker._PROCESS_TABLE_GROUPS = {}
        worker._PROCESS_TABLE_SAMPLED_AT = 0.0
        worker._PROCESS_TABLE_TIMED_OUT_AT = None
        worker._PROCESS_TABLE_TIMEOUT_ERROR = ""

    def sample(self, outcomes: list[object], group_id: int = 101):
        """Read one group at the fake clock; outcomes are stdout, SLOW_PS or exceptions."""
        def fake_run(*_args, **kwargs):
            outcome = outcomes.pop(0)
            if outcome is SLOW_PS:
                self.clock[0] += kwargs["timeout"]
                raise subprocess.TimeoutExpired(["/bin/ps"], kwargs["timeout"])
            if isinstance(outcome, BaseException):
                raise outcome
            if isinstance(outcome, subprocess.CompletedProcess):
                return outcome
            return subprocess.CompletedProcess([], 0, outcome, "")

        with mock.patch.object(worker.time, "monotonic", side_effect=lambda: self.clock[0]), \
                mock.patch.object(worker.subprocess, "run", side_effect=fake_run) as run:
            return worker.process_group_resources(group_id), run.call_count

    def at_age(self, seconds: float):
        self.clock[0] = 1000.0 + seconds

    def test_slow_ps_reuses_recent_table_then_recovers(self):
        self.assertEqual(self.sample(["101 100\n"]), ((100 * 1024, 1), 1))

        self.at_age(2)
        self.assertEqual(self.sample([SLOW_PS]), ((100 * 1024, 1), 1))

        # Callers just after a timeout share it instead of queueing another
        # five-second ps on the shared lock.
        self.assertEqual(self.sample([]), ((100 * 1024, 1), 0))

        self.at_age(9)
        self.assertEqual(self.sample(["101 400\n"]), ((400 * 1024, 1), 1))
        self.assertIsNone(worker._PROCESS_TABLE_TIMED_OUT_AT)

    def test_unseen_group_reads_empty_while_table_is_reused(self):
        self.sample(["101 100\n"])
        self.at_age(2)
        self.assertEqual(self.sample([SLOW_PS], group_id=202), ((0, 0), 1))

    def test_timeout_that_crosses_the_stale_limit_fails(self):
        self.sample(["101 100\n"])
        self.at_age(worker.PROCESS_TABLE_STALE_LIMIT - 3)
        with self.assertRaisesRegex(worker.WorkerError, "could not run ps"):
            self.sample([SLOW_PS])

    def test_repeated_timeouts_cannot_extend_tolerance(self):
        self.sample(["101 100\n"])
        self.at_age(1)
        self.assertEqual(self.sample([SLOW_PS])[0], (100 * 1024, 1))  # age 6
        self.at_age(7)
        self.assertEqual(self.sample([SLOW_PS])[0], (100 * 1024, 1))  # age 12
        self.at_age(12.5)
        self.assertEqual(self.sample([]), ((100 * 1024, 1), 0))
        self.at_age(13)
        with self.assertRaisesRegex(worker.WorkerError, "could not run ps"):
            self.sample([SLOW_PS])  # age 18

    def test_waiting_callers_share_a_failed_probe(self):
        self.sample(["101 100\n"])
        self.at_age(13)
        with self.assertRaisesRegex(worker.WorkerError, "could not run ps"):
            self.sample([SLOW_PS])
        # The next nine slots get the same failure without nine more probes.
        for _ in range(9):
            with self.assertRaisesRegex(worker.WorkerError, "timed out after 5"):
                self.sample([])

    def test_timeout_without_prior_sample_fails(self):
        with self.assertRaisesRegex(worker.WorkerError, "could not run ps"):
            self.sample([SLOW_PS])

    def test_ps_errors_fail_despite_recent_table(self):
        self.sample(["101 100\n"])
        self.at_age(2)
        failed = subprocess.CompletedProcess([], 1, "", "ps: broken")
        with self.assertRaisesRegex(worker.WorkerError, "ps: broken"):
            self.sample([failed])
        with self.assertRaisesRegex(worker.WorkerError, "could not run ps"):
            self.sample([OSError("no ps")])


if __name__ == "__main__":
    unittest.main()
