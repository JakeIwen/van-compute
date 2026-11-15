import os
import signal
from types import SimpleNamespace
import unittest
from unittest import mock

from van_compute import engine_process, pi_cleanup_hold
from van_compute.pi_cleanup_hold import CleanupHold


class Unconfirmed(RuntimeError):
    pass


class HoldFixture:
    """A CleanupHold on a fake clock whose group dies after `alive_checks`."""

    def __init__(self, alive_checks=0, stop_after_sleeps=None, **overrides):
        self.now = 0.0
        self.sleeps = 0
        self.checks = 0
        self.alive_checks = alive_checks
        self.stop_after_sleeps = stop_after_sleeps
        self.logged = []
        self.marks = 0
        self.unmarks = 0
        self.marker = False
        self.mark_failures = 0
        self.unmark_failures = 0
        options = dict(
            job="job",
            group_exists=self.group_exists,
            service_stopping=self.stopping,
            mark=self.mark,
            unmark=self.unmark,
            unconfirmed_error=Unconfirmed,
            kill=lambda _group: None,
            reap=lambda _process: None,
            log=self.logged.append,
            sleep=self.sleep,
            clock=lambda: self.now,
        )
        options.update(overrides)
        self.hold = CleanupHold(**options)
        self.process = SimpleNamespace(pid=4242, returncode=None)

    def group_exists(self, _group):
        self.checks += 1
        return self.checks <= self.alive_checks

    def stopping(self):
        return (
            self.stop_after_sleeps is not None
            and self.sleeps >= self.stop_after_sleeps
        )

    def sleep(self, seconds):
        self.now += seconds
        self.sleeps += 1

    def mark(self, _group):
        self.marks += 1
        if self.marks <= self.mark_failures:
            raise OSError("queue busy")
        self.marker = True

    def unmark(self):
        self.unmarks += 1
        if self.unmarks <= self.unmark_failures:
            raise OSError("queue busy")
        self.marker = False


class NeverBlocksTests(unittest.TestCase):
    def test_the_first_attempt_never_makes_a_blocking_wait(self):
        calls = []

        def wait4(pid, options):
            calls.append(options)
            if not options & os.WNOHANG:
                raise AssertionError("blocking wait4 on a possibly stuck leader")
            return (0, 0, None)

        fixture = HoldFixture(
            alive_checks=10**6, stop_after_sleeps=60, reap=engine_process.wait4_nohang
        )
        with mock.patch.object(engine_process.os, "wait4", wait4):
            with self.assertRaises(Unconfirmed):
                fixture.hold.clean_up(fixture.process)
        self.assertTrue(calls)
        self.assertTrue(fixture.marker, "the marker is written before the stop")

    def test_a_cleanup_that_works_in_the_first_attempt_writes_no_marker(self):
        fixture = HoldFixture(alive_checks=2)
        fixture.hold.clean_up(fixture.process)
        self.assertEqual(fixture.marks, 0)


class BestEffortLoggingTests(unittest.TestCase):
    def test_a_logger_that_raises_never_ends_the_hold(self):
        def broken(_line):
            raise BrokenPipeError("stdout closed")

        fixture = HoldFixture(alive_checks=30, log=broken)
        fixture.hold.clean_up(fixture.process)  # held, then confirmed gone
        self.assertGreater(fixture.sleeps, 20)
        self.assertFalse(fixture.marker)


class MarkerPersistenceTests(unittest.TestCase):
    def test_a_transient_write_failure_is_retried_on_a_later_iteration(self):
        fixture = HoldFixture(alive_checks=40, stop_after_sleeps=35)
        fixture.mark_failures = 3
        with self.assertRaises(Unconfirmed):
            fixture.hold.clean_up(fixture.process)
        self.assertTrue(fixture.marker)
        self.assertEqual(fixture.marks, 4)

    def test_a_marker_that_never_persists_is_retried_at_stop_and_reported(self):
        fixture = HoldFixture(alive_checks=10**6, stop_after_sleeps=25)
        fixture.mark_failures = 10**6
        with self.assertRaises(Unconfirmed):
            fixture.hold.clean_up(fixture.process)
        self.assertFalse(fixture.marker)
        iterations = fixture.marks
        self.assertGreater(iterations, 2)
        self.assertTrue(
            any("could not write" in str(line.get("reason")) for line in fixture.logged)
        )

    def test_a_transient_clear_failure_is_retried_before_continuing(self):
        fixture = HoldFixture(alive_checks=25)
        fixture.unmark_failures = 3
        fixture.hold.clean_up(fixture.process)
        self.assertEqual(fixture.unmarks, 4)
        self.assertFalse(fixture.marker)

    def test_a_clear_that_keeps_failing_ends_in_unconfirmed_on_stop(self):
        fixture = HoldFixture(alive_checks=22, stop_after_sleeps=30)
        fixture.unmark_failures = 10**6
        with self.assertRaises(Unconfirmed):
            fixture.hold.clean_up(fixture.process)
        self.assertTrue(fixture.marker, "the job stays marked")


class UntilDoneTests(unittest.TestCase):
    def test_a_busy_discard_is_logged_when_it_starts_and_every_minute(self):
        fixture = HoldFixture()
        answers = iter(["busy"] * 130)
        fixture.hold.until_done(
            "cache-discard", lambda: next(answers, None), entry="ns/cache"
        )
        lines = [l for l in fixture.logged if l.get("phase") == "cache-discard"]
        self.assertEqual(len(lines), 3)  # at 0 s, ~60 s and ~120 s
        self.assertEqual(lines[0]["reason"], "busy")
        self.assertEqual(lines[0]["entry"], "ns/cache")
        self.assertEqual(lines[0]["action"], "cleanup-hold")
        self.assertEqual(lines[0]["job"], "job")

    def test_an_error_is_the_logged_reason_and_a_stop_ends_the_retry(self):
        fixture = HoldFixture(stop_after_sleeps=3)

        def failing():
            raise OSError("disk gone")

        with self.assertRaises(Unconfirmed):
            fixture.hold.until_done("cache-discard", failing)
        self.assertIn("disk gone", fixture.logged[0]["reason"])


class PreviousGroupTests(unittest.TestCase):
    """A marked job's group, found again by a later broker run."""

    def hold_previous(self, manifest, *, boot="boot-1", alive_checks=0,
                      identify=None, **overrides):
        fixture = HoldFixture(alive_checks=alive_checks, **overrides)
        if identify is not None:
            fixture.hold.identify = identify
        fixture.hold.hold_previous_group(
            {"cleanup_group": 4242, "cleanup_boot_id": "boot-1", **manifest},
            lambda: boot,
        )
        return fixture

    def test_a_missing_recorded_boot_id_still_holds_a_live_group(self):
        fixture = self.hold_previous(
            {"cleanup_boot_id": None}, alive_checks=3
        )
        self.assertGreaterEqual(fixture.sleeps, 1, "held instead of releasing")

    def test_an_unreadable_current_boot_id_still_holds_a_live_group(self):
        fixture = self.hold_previous({}, boot=None, alive_checks=3)
        self.assertGreaterEqual(fixture.sleeps, 1)

    def test_a_missing_boot_id_with_a_stop_is_unconfirmed_not_released(self):
        with self.assertRaises(Unconfirmed):
            self.hold_previous(
                {"cleanup_boot_id": None}, boot=None, alive_checks=10**6,
                stop_after_sleeps=1,
            )

    def test_a_different_boot_id_proves_a_reboot(self):
        fixture = self.hold_previous({}, boot="boot-2", alive_checks=10**6)
        self.assertEqual(fixture.sleeps, 0)
        self.assertEqual(fixture.checks, 0)

    def test_a_reused_group_id_is_not_held_or_killed(self):
        kills = []
        fixture = self.hold_previous(
            {"cleanup_group_start": "100"}, alive_checks=10**6,
            identify=lambda _group: "999", kill=kills.append,
        )
        self.assertEqual((fixture.sleeps, kills), (0, []))

    def test_the_same_leader_start_time_is_still_the_old_group(self):
        fixture = self.hold_previous(
            {"cleanup_group_start": "100"}, alive_checks=3,
            identify=lambda _group: "100",
        )
        self.assertGreaterEqual(fixture.sleeps, 1)

    def test_a_leader_gone_with_members_left_is_still_the_old_group(self):
        fixture = self.hold_previous(
            {"cleanup_group_start": "100"}, alive_checks=3,
            identify=lambda _group: None,
        )
        self.assertGreaterEqual(fixture.sleeps, 1)

    def test_a_marker_without_a_start_time_is_trusted_as_before(self):
        fixture = self.hold_previous(
            {}, alive_checks=3, identify=lambda _group: "999",
        )
        self.assertGreaterEqual(fixture.sleeps, 1)

    def test_a_group_id_reused_during_the_hold_ends_it_without_more_kills(self):
        kills = []
        starts = iter(["100", "100", "999"])
        fixture = self.hold_previous(
            {"cleanup_group_start": "100"}, alive_checks=10**6,
            identify=lambda _group: next(starts, "999"), kill=kills.append,
        )
        self.assertLessEqual(len(kills), 1)


class LeaderIdentityTests(unittest.TestCase):
    def test_the_leader_is_identified_before_it_is_killed_and_marked(self):
        order = []
        fixture = HoldFixture(
            alive_checks=40,
            identify=lambda group: order.append("identify") or "777",
            kill=lambda _group: order.append("kill"),
        )
        marked = []
        fixture.hold.mark = lambda group: marked.append(fixture.hold.identity)
        fixture.hold.clean_up(fixture.process)
        self.assertEqual(order[0], "identify")
        self.assertEqual(marked, ["777"])


class StopSignalTests(unittest.TestCase):
    def test_handlers_set_a_flag_and_are_restored(self):
        before = signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)
        with pi_cleanup_hold.stop_flag_signals() as stopping:
            self.assertFalse(stopping())
            os.kill(os.getpid(), signal.SIGINT)
            self.assertTrue(stopping())
        self.assertEqual(
            (signal.getsignal(signal.SIGINT), signal.getsignal(signal.SIGTERM)),
            before,
        )


if __name__ == "__main__":
    unittest.main()
