"""Pi-local cancellation: the stop latch, restart recovery and marked recovery."""

import dataclasses
import datetime as dt
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import types
import unittest
from unittest import mock

from van_compute import (
    broker, job_cancel, job_cleanup_marker, pi_cancel_recovery, pi_cleanup_hold, pi_marked_recovery,
    queue,
)
from van_compute.can_activity import CanProbe
from van_compute.pi_can_stop import CANCEL_REQUESTED, CAN_TRANSMIT, StopLatch
from van_compute.pi_cleanup_hold import CleanupHold

from pi.tests.compute.test_van_compute_cancel import CancelHarness
from pi.tests.compute.van_compute_test_fixtures import GOOD_HEALTH

SLEEPER = """#!/usr/bin/env python3
import subprocess, sys, time
from pathlib import Path
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
Path({marker!r}).write_text(str(child.pid))
time.sleep(60)
"""


ORPHAN = (
    "import subprocess, sys; "
    "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], "
    "start_new_session=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, "
    "stderr=subprocess.DEVNULL); print(child.pid)"
)


def orphan_group():
    started = subprocess.run(
        [sys.executable, "-c", ORPHAN], capture_output=True, text=True, check=True,
    )
    return int(started.stdout.strip())


def kill_group_quietly(group):
    try:
        os.killpg(group, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def _group_gone(group):
    try:
        os.killpg(group, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


class StopLatchCancelTests(unittest.TestCase):
    def test_cancel_and_can_both_discard_whichever_latches_first(self):
        active = CanProbe(True, ("can0",), None)
        latch = StopLatch(lambda: False, lambda: active, cancel_requested=lambda: True)
        self.assertTrue(latch.should_stop())
        self.assertTrue(latch.discards_attempt)
        self.assertEqual(latch.cause, CAN_TRANSMIT)  # CAN is checked first: fast kill

    def test_the_watch_is_rate_limited_and_errs_toward_not_requested(self):
        now = [0.0]
        calls = []

        def requested():
            calls.append(now[0])
            raise OSError("unreadable")

        latch = StopLatch(lambda: False, None, clock=lambda: now[0], cancel_requested=requested)
        self.assertFalse(latch.should_stop())
        now[0] = 0.5
        self.assertFalse(latch.should_stop())
        now[0] = 1.5
        self.assertFalse(latch.should_stop())
        self.assertEqual(calls, [0.0, 1.5])


class BrokerCancelTests(CancelHarness, unittest.TestCase):
    def sleeper_job(self):
        self.marker = Path(self.temporary.name) / "sleeper-started"
        (self.source / "tools" / "local_job.py").write_text(
            SLEEPER.format(marker=str(self.marker)), encoding="utf-8"
        )
        return self.submit()

    def cancel_when_started(self, job_id, results):
        deadline = time.monotonic() + 20
        while not self.marker.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        results.append(job_cancel.cancel_job(self.root, job_id)[0])

    def test_a_running_local_job_is_stopped_discarded_and_cancelled(self):
        job = self.sleeper_job()
        results = []
        thread = threading.Thread(target=self.cancel_when_started, args=(job["id"], results))
        thread.start()
        started = time.monotonic()
        result = self.run_once()
        thread.join()
        self.assertEqual(results, ["requested"])
        self.assertEqual(result["action"], "cancelled", result)
        self.assertLess(time.monotonic() - started, 30)
        final = self.assert_final(job["id"], "running")
        self.assertEqual(final["attempt_history"][-1]["reason"], CANCEL_REQUESTED)
        self.assertNotIn("results", final)
        descendant = int(self.marker.read_text())
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                os.kill(descendant, 0)
            except ProcessLookupError:
                break
            time.sleep(0.05)
        else:
            os.kill(descendant, signal.SIGKILL)
            self.fail("the cancelled job's descendant survived")
        self.assertEqual(list(self.work.iterdir()), [])

    def test_a_request_seen_before_launch_launches_nothing(self):
        job = self.submit()
        claimed = self.claim_local()
        job_cancel.cancel_job(self.root, job["id"])
        broker.prepare_work_root(self.args.work_root)
        with mock.patch.object(broker.engine, "run_child", side_effect=AssertionError("launched")):
            result = broker.execute_claimed_job(self.args, self.root, claimed)
        self.assertEqual(result["action"], "cancelled")
        self.assert_final(job["id"], "running")


class RestartRecoveryTests(CancelHarness, unittest.TestCase):
    def requested_local_job(self):
        job = self.submit()
        claimed = self.claim_local()
        job_cancel.cancel_job(self.root, job["id"])
        return job, claimed

    def run_once_with(self, health):
        with mock.patch.object(broker.engine, "run_child", side_effect=AssertionError("launched")), \
                mock.patch.object(broker, "bubblewrap_self_test", return_value=(True, "t")):
            return broker.run_once(self.args, health_reader=lambda: health)

    def test_restart_cancels_without_any_gate(self):
        unhealthy = dataclasses.replace(GOOD_HEALTH, memory_available_bytes=1, temperature_c=95.0)
        can_active = dataclasses.replace(
            GOOD_HEALTH, can_active=True, can_held_locks=("can0",), can_quiet_seconds=0.0,
        )
        for name, health, low_space in (
            ("unhealthy", unhealthy, False),
            ("can-active", can_active, False),
            ("low-space", GOOD_HEALTH, True),
        ):
            with self.subTest(case=name):
                self.tearDown()
                self.setUp()
                if low_space:
                    self.args.min_work_free_bytes = 1 << 62
                job, _claimed = self.requested_local_job()
                # An unmarked job holds no group, and a bare health reader
                # brings no startup snapshot: the uncontained case, finalized
                # at once.  The survivor evidence is tested in
                # test_van_compute_cgroup_survivors.
                with mock.patch.object(pi_marked_recovery, "build_hold") as build_hold:
                    result = self.run_once_with(health)
                build_hold.assert_not_called()
                self.assertEqual(result["action"], "cancelled", result)
                final = self.assert_final(job["id"], "running")
                self.assertEqual(final["attempt_history"][-1]["reason"], "cancel-recovery")

    def mark_with_live_group(self, job_id, lease_token):
        # A previous broker's group is not this process's child (it would
        # linger here as an unreaped zombie), so start it through a parent
        # that exits at once.
        group = orphan_group()
        self.addCleanup(kill_group_quietly, group)
        job_cleanup_marker.mark_cleanup_unconfirmed(
            self.root, job_id, lease_token, worker=broker.LOCAL_WORKER,
            now=dt.datetime.now(dt.timezone.utc), group=group, boot_id=None,
        )
        return group

    def test_a_marked_requested_job_holds_its_old_group_then_cancels(self):
        job = self.submit()
        claimed = self.claim_local()
        group = self.mark_with_live_group(job["id"], claimed["lease_token"])
        job_cancel.cancel_job(self.root, job["id"])
        self.args.min_work_free_bytes = 1 << 62  # the failure-publication wait is skipped
        result = self.run_once_with(GOOD_HEALTH)
        self.assertEqual(result["action"], "cancelled", result)
        self.assertTrue(_group_gone(group))
        self.assert_final(job["id"], "running")

    def test_a_request_during_marked_recovery_ends_it_without_publishing(self):
        for moment in ("after-hold", "during-space-wait"):
            with self.subTest(moment=moment):
                self.tearDown()
                self.setUp()
                job = self.submit()
                claimed = self.claim_local()
                self.mark_with_live_group(job["id"], claimed["lease_token"])
                self.args.min_work_free_bytes = 1 << 62
                original = pi_marked_recovery.hold_and_discard

                def hold_then_cancel(*args, **kwargs):
                    original(*args, **kwargs)
                    if moment == "after-hold":
                        job_cancel.cancel_job(self.root, job["id"])
                    else:
                        threading.Timer(
                            0.3, job_cancel.cancel_job, args=(self.root, job["id"])
                        ).start()

                with mock.patch.object(pi_marked_recovery, "hold_and_discard", hold_then_cancel), \
                        mock.patch.object(pi_cleanup_hold, "HOLD_INTERVAL_SECONDS", 0.05):
                    result = self.run_once_with(GOOD_HEALTH)
                self.assertEqual(result["action"], "cancelled", result)
                final = self.assert_final(job["id"], "running")
                self.assertNotIn("results", final)

    def test_recovery_reports_a_job_it_no_longer_owns(self):
        job, claimed = self.requested_local_job()
        stale = dict(queue.manifest_for(self.root, job["id"]), lease_token="f" * 32)
        result = pi_cancel_recovery.finish(
            broker._recovery_tools(), self.args, self.root, stale, lambda: False,
        )
        self.assertEqual(result["action"], "deferred")
        self.assertEqual(self.located(job["id"]), ["running"])


LIMITS = types.SimpleNamespace(timeout=30, min_work_free_bytes=0, max_result_bytes=0)


class LaunchLogTests(unittest.TestCase):
    def hold(self, log):
        return CleanupHold(
            job="20260101T000000Z-00000001", group_exists=broker._process_group_exists,
            service_stopping=lambda: False, mark=lambda _g: None, unmark=lambda: None,
            unconfirmed_error=broker.TerminationUnconfirmed, log=log,
            sleep=lambda _s: None,
        )

    def test_a_throwing_logger_still_reaches_supervision_and_cleanup(self):
        for error in (BrokenPipeError, RuntimeError):
            with self.subTest(error=error.__name__):
                logged = []

                def log(payload):
                    logged.append(payload)
                    raise error("log sink closed")

                hold = self.hold(log)
                process = subprocess.Popen(
                    [sys.executable, "-c", "import time; time.sleep(60)"],
                    start_new_session=True,
                )
                self.addCleanup(lambda: process.poll() is None and process.kill())
                with mock.patch.object(broker, "_wait_for_process", side_effect=OSError("wait failed")):
                    with self.assertRaisesRegex(OSError, "wait failed"):
                        broker._supervise_child(process, LIMITS, Path("."), mock.Mock(), hold)
                self.assertEqual(logged[0]["action"], "job-launched")
                self.assertEqual(logged[0]["group"], process.pid)
                process.wait(5)
                self.assertTrue(_group_gone(process.pid))


if __name__ == "__main__":
    unittest.main()
