"""Mac resource watchdog: a slow `ps` is an unknown reading, not a breach."""

import subprocess
import sys
import unittest
from unittest import mock

from van_compute import engine_process, worker


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
        worker._PROCESS_TABLE_BLIND_SINCE = None
        worker._PROCESS_TABLE_TIMEOUTS = 0
        worker._PROCESS_TABLE_RETRY_AT = 0.0
        worker._PROCESS_TABLE_TIMEOUT_ERROR = ""

    def sample(self, outcomes, group_id: int = 101):
        """Read one group at the fake clock.

        Outcomes are a list (or a callable returning the next one) of stdout,
        SLOW_PS or exceptions.
        """
        def fake_run(*_args, **kwargs):
            outcome = outcomes() if callable(outcomes) else outcomes.pop(0)
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
        self.assertIsNone(worker._PROCESS_TABLE_BLIND_SINCE)
        self.assertEqual(worker._PROCESS_TABLE_TIMEOUTS, 0)

    def test_unseen_group_reads_empty_while_table_is_reused(self):
        self.sample(["101 100\n"])
        self.at_age(2)
        self.assertEqual(self.sample([SLOW_PS], group_id=202), ((0, 0), 1))

    def test_timeout_without_prior_sample_reads_empty(self):
        self.assertEqual(self.sample([SLOW_PS]), ((0, 0), 1))

    def test_intermittent_timeouts_never_fail_a_healthy_job(self):
        # Twenty minutes of a ps that times out three probes in four.
        probes = []

        def flaky_ps():
            probes.append(self.clock[0])
            return "101 100\n" if len(probes) % 4 == 0 else SLOW_PS

        self.sample(["101 100\n"])
        while self.clock[0] < 1000.0 + 1200:
            self.assertEqual(self.sample(flaky_ps)[0], (100 * 1024, 1))
            self.clock[0] += 1
        self.assertGreater(len(probes), 40)

    def test_sustained_timeouts_fail_only_past_the_blind_limit(self):
        self.sample(["101 100\n"])
        probes = 0
        with self.assertRaisesRegex(worker.WorkerError, "no process-table reading for"):
            while True:
                self.assertLessEqual(
                    self.clock[0] - 1000.0, worker.PROCESS_TABLE_BLIND_LIMIT + 10
                )
                reading, calls = self.sample([SLOW_PS])
                probes += calls
                if calls == 0:
                    self.clock[0] += 1
                self.assertEqual(reading, (100 * 1024, 1))
        self.assertGreaterEqual(
            self.clock[0] - 1000.0, worker.PROCESS_TABLE_BLIND_LIMIT
        )
        # Exponential backoff keeps a loaded Mac from running ps every second.
        self.assertLess(probes, 15)

    def test_backoff_doubles_then_caps_at_retry_max(self):
        self.sample(["101 100\n"])
        self.at_age(1)
        delays = []
        for _ in range(8):
            self.assertEqual(self.sample([SLOW_PS])[1], 1)
            delays.append(worker._PROCESS_TABLE_RETRY_AT - self.clock[0])
            self.clock[0] = worker._PROCESS_TABLE_RETRY_AT - 0.5
            self.assertEqual(self.sample([])[1], 0)  # still backing off
            self.clock[0] += 0.5
        self.assertEqual(delays, [1, 2, 4, 8, 16, 30, 30, 30])

    def test_idle_gap_before_an_outage_does_not_count_as_blind(self):
        self.sample(["101 100\n"])
        self.at_age(3600)  # the worker sat idle for an hour
        self.assertEqual(self.sample([SLOW_PS], group_id=202), ((0, 0), 1))
        self.assertEqual(worker._PROCESS_TABLE_BLIND_SINCE, 1000.0 + 3600)

    def test_waiting_callers_share_a_failed_probe(self):
        self.sample(["101 100\n"])
        self.at_age(10)
        worker._PROCESS_TABLE_BLIND_SINCE = 10.0 + 1000.0 - worker.PROCESS_TABLE_BLIND_LIMIT + 2
        worker._PROCESS_TABLE_TIMEOUTS = 4
        with self.assertRaisesRegex(worker.WorkerError, "could not run ps"):
            self.sample([SLOW_PS])
        # The next nine slots get the same failure without nine more probes.
        for _ in range(9):
            with self.assertRaisesRegex(worker.WorkerError, "timed out after 5"):
                self.sample([])

    def test_ps_errors_fail_despite_recent_table(self):
        self.sample(["101 100\n"])
        self.at_age(2)
        failed = subprocess.CompletedProcess([], 1, "", "ps: broken")
        with self.assertRaisesRegex(worker.WorkerError, "ps: broken"):
            self.sample([failed])
        with self.assertRaisesRegex(worker.WorkerError, "could not run ps"):
            self.sample([OSError("no ps")])


class ResourceWatchdogJobTests(unittest.TestCase):
    def setUp(self):
        ResourceWatchdogPsTimeoutTests._clear_process_table(self)
        self.addCleanup(ResourceWatchdogPsTimeoutTests._clear_process_table, self)

    def test_job_survives_a_ps_that_always_times_out(self):
        def timing_out(*_args, **kwargs):
            raise subprocess.TimeoutExpired(["/bin/ps"], kwargs["timeout"])

        process = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(1.5)"],
            start_new_session=True,
        )
        with mock.patch.object(worker.subprocess, "run", side_effect=timing_out):
            outcome = engine_process.wait_for_process(
                process,
                host=engine_process.Host.MAC,
                error_type=worker.WorkerError,
                timeout=30,
                should_stop=lambda: False,
                work_path=None,
                minimum_free_bytes=0,
                free_space_reader=lambda _path: 0,
                maximum_memory=1 << 40,
                maximum_processes=1000,
                resource_reader=worker.process_group_resources,
            )
        self.assertEqual(outcome.exit_code, 0)
        self.assertIsNone(outcome.resource_monitor_error)


if __name__ == "__main__":
    unittest.main()
