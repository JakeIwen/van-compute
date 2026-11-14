import contextlib
import datetime as dt
import fcntl
import functools
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import threading
import time
import unittest
from unittest import mock

from van_compute import (
    broker, engine_process, job_cleanup_marker, job_requeue, pi_can_stop,
    pi_cleanup_hold, queue,
)
from van_compute.can_activity import CanProbe
from van_compute.pi_can_stop import (
    CAN_PROBE_FAILED, CAN_TRANSMIT, SERVICE_STOP, StopLatch,
)
from van_compute.pi_health_monitor import HealthMonitor

from pi.tests.compute.test_van_compute_pi_health import (
    FakeClock, FixedRates, host_readers,
)
from pi.tests.compute.van_compute_test_fixtures import BrokerHarness, GOOD_HEALTH

QUIET = CanProbe(False, (), None)
HELD = CanProbe(True, ("active-diagnostics-can0.lock",), None)


class LatchClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class StopLatchTests(unittest.TestCase):
    def latch(self, service=lambda: False, observe=lambda: QUIET):
        clock = LatchClock()
        return StopLatch(service, observe, clock=clock), clock

    def test_can_is_checked_at_most_every_half_second(self):
        calls = []
        latch, clock = self.latch(observe=lambda: calls.append(1) or QUIET)
        for _ in range(10):  # the wait loop calls every 0.1 s
            self.assertFalse(latch.should_stop())
            clock.now += 0.1
        self.assertEqual(len(calls), 2)

    def test_first_cause_wins_service_then_can(self):
        stop = {"on": False}
        probe = {"value": QUIET}
        latch, clock = self.latch(lambda: stop["on"], lambda: probe["value"])
        self.assertFalse(latch.should_stop())
        stop["on"] = True
        clock.now += 0.1
        self.assertTrue(latch.should_stop())
        probe["value"] = HELD
        clock.now += 1.0
        self.assertTrue(latch.should_stop())
        self.assertEqual(latch.cause, SERVICE_STOP)
        self.assertFalse(latch.can_cause)
        self.assertEqual(latch.terminate_grace(), 10.0)
        self.assertEqual(latch.group_grace(), 2.0)

    def test_first_cause_wins_can_then_service(self):
        stop = {"on": False}
        probe = {"value": QUIET}
        latch, clock = self.latch(lambda: stop["on"], lambda: probe["value"])
        probe["value"] = HELD
        self.assertTrue(latch.should_stop())
        stop["on"] = True
        clock.now += 1.0
        self.assertTrue(latch.should_stop())
        self.assertEqual(latch.cause, CAN_TRANSMIT)
        self.assertEqual(latch.detail, "active-diagnostics-can0.lock")
        self.assertEqual(latch.terminate_grace(), 1.0)
        self.assertEqual(latch.group_grace(), 0.0)

    def test_a_simultaneous_can_and_service_stop_takes_the_faster_kill(self):
        latch, _clock = self.latch(lambda: True, lambda: HELD)
        self.assertTrue(latch.should_stop())
        self.assertEqual(latch.cause, CAN_TRANSMIT)

    def test_probe_exception_latches_probe_failed_and_never_escapes(self):
        def boom():
            raise OSError("proc gone")

        latch, _clock = self.latch(observe=boom)
        self.assertTrue(latch.should_stop())
        self.assertEqual(latch.cause, CAN_PROBE_FAILED)
        self.assertTrue(latch.can_cause)
        self.assertIn("proc gone", latch.detail)

    def test_probe_error_result_latches_probe_failed(self):
        latch, _clock = self.latch(observe=lambda: CanProbe(False, (), "no dir"))
        self.assertTrue(latch.should_stop())
        self.assertEqual(latch.cause, CAN_PROBE_FAILED)

    def test_clear_to_launch_checks_immediately(self):
        latch, _clock = self.latch(observe=lambda: HELD)
        self.assertFalse(latch.clear_to_launch())
        quiet, _clock = self.latch()
        self.assertTrue(quiet.clear_to_launch())

    def test_without_an_observer_only_the_service_stop_applies(self):
        latch = StopLatch(lambda: False, None)
        self.assertFalse(latch.should_stop())
        self.assertTrue(latch.clear_to_launch())


class CanStopTests(BrokerHarness, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.token = f"--hang={secrets.token_hex(8)}"
        self.calls = 0
        self.seen_active_at = None
        self.dead_at = None
        self.seen_pids = []

    def active_after_launch(self, _directory=None):
        """Quiet until the job is running, then a held lock."""
        self.calls += 1
        if self.hang_pids():
            if self.seen_active_at is None:
                self.seen_active_at = time.monotonic()
                self.seen_pids = self.hang_pids()
                self.assertEqual(len(self.seen_pids), 2)
                self.watcher = threading.Thread(target=self.watch_until_dead)
                self.watcher.start()
            return HELD
        return QUIET

    def monitor(self, probe):
        return HealthMonitor(
            Path("/unused"), probe=probe, sampler=FixedRates()
        )

    def run_with(self, probe, **keywords):
        monitor = self.monitor(probe)
        with mock.patch.object(
            broker, "bubblewrap_self_test", return_value=(True, "test")
        ), mock.patch.object(broker, "bubblewrap_command", side_effect=self.no_sandbox):
            return broker.run_once(
                self.args,
                health_reader=lambda: GOOD_HEALTH,
                monitor=monitor,
                **keywords,
            )

    def submit_hanging(self):
        return self.submit(arguments=[self.token])

    def hang_pids(self):
        """Pids of the job and its descendant (both carry the token)."""
        listing = subprocess.run(
            ["ps", "-axo", "pid=,command="], capture_output=True, text=True
        ).stdout
        return [
            int(line.split(None, 1)[0])
            for line in listing.splitlines()
            if self.token in line and "ps -axo" not in line
        ]

    def watch_until_dead(self):
        """Record when every seen process has exited (a zombie counts).

        The stop bound is about the processes, not run_once: requeue I/O and
        this test's own `ps` scans add time on a slow Pi that a transmitter
        never competes with.
        """
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            states = [
                subprocess.run(
                    ["ps", "-o", "stat=", "-p", str(pid)],
                    capture_output=True, text=True,
                ).stdout.strip()
                for pid in self.seen_pids
            ]
            if all(not state or state.startswith("Z") for state in states):
                self.dead_at = time.monotonic()
                return
            time.sleep(0.02)

    def assert_gone(self, pids):
        deadline = time.monotonic() + 3
        for pid in pids:
            while True:
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    break
                if time.monotonic() > deadline:
                    os.kill(pid, 9)
                    self.fail(f"process {pid} survived")
                time.sleep(0.05)

    def queued_manifest(self, job_id):
        return queue.load_json(self.root / "queued" / job_id / "manifest.json")

    def test_running_job_is_stopped_fast_and_requeued_untouched(self):
        submitted = self.submit_hanging()
        with mock.patch.object(
            broker, "_finish_local_job"
        ) as finish, mock.patch.object(
            broker, "_put_local_result"
        ) as upload, mock.patch.object(
            broker, "_record_local_execution_once"
        ) as telemetry:
            result = self.run_with(self.active_after_launch)
        self.watcher.join()
        self.assertIsNotNone(self.dead_at, "job processes outlived the watch")
        stopped_after = self.dead_at - self.seen_active_at

        self.assertEqual(
            result,
            {"action": "requeued", "reason": "pi-can-transmit", "job_id": submitted["id"]},
        )
        # 0.5 s watch + 1 s SIGTERM grace (the job and a descendant ignore
        # SIGTERM); the usual grace would be 10 s.
        self.assertLess(stopped_after, 2.5)
        self.assert_gone(self.seen_pids)
        finish.assert_not_called()
        upload.assert_not_called()
        telemetry.assert_not_called()
        self.assertEqual(list((self.root / "missed").glob("*.json")), [])
        self.assertFalse((self.root / "running" / submitted["id"]).exists())
        self.assertFalse((self.root / "done" / submitted["id"]).exists())
        manifest = self.queued_manifest(submitted["id"])
        self.assertEqual(manifest["state"], "queued")
        for field in ("worker", "lease_token", "placement", "started_at"):
            self.assertNotIn(field, manifest)
        self.assertEqual(manifest["requeue_reason"], "pi-can-transmit")
        history = manifest["attempt_history"]
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["reason"], "pi-can-transmit")
        self.assertEqual(history[0]["worker"], broker.LOCAL_WORKER)
        self.assertEqual(history[0]["attempt"], 1)
        self.assertEqual(
            sorted(p.name for p in (self.root / "queued" / submitted["id"] / "result").iterdir()),
            [],
        )
        self.assertEqual(list(self.work.iterdir()), [])

    def test_a_later_claim_gets_a_new_lease_and_the_old_token_is_rejected(self):
        submitted = self.submit_hanging()
        with mock.patch.object(
            broker, "execute_claimed_job", wraps=broker.execute_claimed_job
        ) as execute:
            self.run_with(self.active_after_launch)
        old_token = execute.call_args.args[2]["lease_token"]

        root = queue.safe_root(self.root)
        claimed, _reason = broker.claim_local_job(
            root, grace_seconds=0.0, remote_max_age=45.0
        )
        self.assertEqual(claimed["id"], submitted["id"])
        self.assertEqual(claimed["attempt"], 2)
        self.assertNotEqual(claimed["lease_token"], old_token)
        upload = self.source / "upload.txt"
        upload.write_text("late")
        with upload.open("rb") as handle, self.assertRaises(queue.QueueError):
            queue.put_result(
                root, submitted["id"], broker.LOCAL_WORKER, "late.txt", handle,
                lease_token=old_token,
            )

    def test_a_job_claimed_by_someone_else_is_left_untouched(self):
        submitted = self.submit_hanging()
        other = secrets.token_hex(16)
        manifest_path = self.root / "running" / submitted["id"] / "manifest.json"

        def steal_then_report(directory=None):
            probe = self.active_after_launch()
            if probe.active and queue.load_json(manifest_path)["lease_token"] != other:
                stolen = queue.load_json(manifest_path)
                stolen["lease_token"] = other
                queue.atomic_json(manifest_path, stolen)
            return probe

        result = self.run_with(steal_then_report)

        self.assertEqual(result["action"], "deferred")
        self.assertIn("ownership-lost", result["reason"])
        self.assertTrue((self.root / "running" / submitted["id"]).is_dir())
        self.assertFalse((self.root / "queued" / submitted["id"]).exists())
        kept = queue.load_json(manifest_path)
        self.assertEqual(kept["lease_token"], other)
        self.assertNotIn("attempt_history", kept)
        self.assert_gone(self.seen_pids)

    def test_probe_exception_stops_and_requeues_as_probe_failed(self):
        submitted = self.submit_hanging()

        def flaky(directory=None):
            if self.hang_pids():
                self.seen_pids = self.hang_pids()
                raise RuntimeError("lock table unreadable")
            return QUIET

        result = self.run_with(flaky)
        self.assertEqual(result["action"], "requeued")
        self.assertEqual(result["reason"], "can-probe-failed")
        self.assertEqual(
            self.queued_manifest(submitted["id"])["attempt_history"][-1]["reason"],
            "can-probe-failed",
        )
        self.assert_gone(self.seen_pids)

    def test_service_stop_still_fails_the_job_as_before(self):
        submitted = self.submit_hanging()
        with mock.patch.object(pi_can_stop, "DEFAULT_TERMINATE_GRACE", 0.5):
            result = self.run_with(
                lambda _dir=None: QUIET, should_stop=lambda: bool(self.hang_pids())
            )
        self.assertEqual(result["action"], "executed")
        self.assertFalse(result["ok"])
        self.assertTrue(result["execution"]["interrupted"])
        self.assertFalse((self.root / "queued" / submitted["id"]).exists())
        self.assertTrue((self.root / "failed" / submitted["id"]).is_dir())
        self.assert_gone(self.seen_pids)

    def test_can_before_the_child_is_launched_requeues_without_starting_it(self):
        submitted = self.submit_hanging()
        # Calls: run_once's own observation, the check before staging, then the
        # check inside the cache scope; the third finds a lock.
        def late(directory=None):
            self.calls += 1
            return HELD if self.calls >= 3 else QUIET

        result = self.run_with(late)
        self.assertEqual(result["action"], "requeued")
        self.assertEqual(result["reason"], "pi-can-transmit")
        self.assertEqual(self.hang_pids(), [], "a child was started")
        self.assertTrue((self.root / "queued" / submitted["id"]).is_dir())
        self.assertEqual(list(self.work.iterdir()), [])

    def test_can_before_staging_requeues_without_touching_the_work_root(self):
        submitted = self.submit_hanging()

        def held_after_gate(directory=None):
            self.calls += 1
            return HELD if self.calls >= 2 else QUIET

        result = self.run_with(held_after_gate)
        self.assertEqual(result["action"], "requeued", result)
        self.assertEqual(list(self.work.iterdir()), [])
        self.assertEqual(self.hang_pids(), [])
        self.assertTrue((self.root / "queued" / submitted["id"]).is_dir())

    def test_an_exception_while_waiting_kills_and_reaps_the_group(self):
        self.submit_hanging()
        cleanup_seen = []

        def failing_wait(process, **_keywords):
            deadline = time.monotonic() + 5
            while len(self.hang_pids()) < 2 and time.monotonic() < deadline:
                time.sleep(0.02)
            cleanup_seen.append(self.hang_pids())
            raise broker.BrokerError("watchdog crashed")

        with mock.patch.object(broker, "_wait_for_process", failing_wait):
            result = self.run_with(lambda _dir=None: QUIET)
        # The failure is published as today, but only after the child and its
        # descendant are gone.
        self.assertEqual(result["action"], "executed")
        self.assertFalse(result["ok"])
        self.assert_gone(cleanup_seen[0])

    def run_gated_by_monitor(self, clock, monitor):
        """run_once on the monitor's own snapshot, so the quiet requirement applies."""
        with host_readers(), mock.patch.object(
            broker, "bubblewrap_self_test", return_value=(True, "test")
        ), mock.patch.object(broker, "bubblewrap_command", side_effect=self.no_sandbox):
            return broker.run_once(self.args, monitor=monitor)

    def warm_monitor(self):
        clock = FakeClock(0.0)
        monitor = HealthMonitor(
            Path("/unused"),
            probe=lambda _dir=None: QUIET,
            sampler=FixedRates(),
            clock=clock,
            sleep=clock.sleep,
        )
        for second in range(0, 66, 5):  # 65 s of quiet in 5 s steps
            clock.now = float(second)
            monitor.observe_can()
        return clock, monitor

    def test_a_quiet_chain_that_survives_staging_still_runs(self):
        self.submit()
        clock, monitor = self.warm_monitor()
        result = self.run_gated_by_monitor(clock, monitor)
        self.assertEqual(result["action"], "executed", result)

    def test_slow_staging_that_breaks_the_quiet_chain_requeues_before_launch(self):
        submitted = self.submit_hanging()
        clock, monitor = self.warm_monitor()
        real_copy = broker._copy_sources

        def slow_copy(*arguments, **keywords):
            real_copy(*arguments, **keywords)
            clock.now += 20.0  # longer than the tracker's 15 s observation gap

        with mock.patch.object(broker, "_copy_sources", slow_copy):
            result = self.run_gated_by_monitor(clock, monitor)
        self.assertEqual(result["action"], "requeued", result)
        self.assertEqual(result["reason"], pi_can_stop.CAN_NOT_QUIET)
        self.assertEqual(self.hang_pids(), [], "a child was started")
        self.assertTrue((self.root / "queued" / submitted["id"]).is_dir())

    def test_slow_cache_acquisition_that_breaks_the_quiet_chain_requeues(self):
        submitted = self.submit_hanging()
        clock, monitor = self.warm_monitor()
        real_job_cache = broker.task_cache.job_cache

        def slow_job_cache(*arguments, **keywords):
            directory, scope = real_job_cache(*arguments, **keywords)

            @contextlib.contextmanager
            def waiting():
                clock.now += 20.0  # waiting for the cache lock
                with scope:
                    yield

            return directory, waiting()

        with mock.patch.object(broker.task_cache, "job_cache", slow_job_cache):
            result = self.run_gated_by_monitor(clock, monitor)
        self.assertEqual(result["action"], "requeued", result)
        self.assertEqual(result["reason"], pi_can_stop.CAN_NOT_QUIET)
        self.assertEqual(self.hang_pids(), [], "a child was started")
        self.assertTrue((self.root / "queued" / submitted["id"]).is_dir())

    def failing_wait_with_scope_events(self, events):
        """Patch the wait to crash once the job runs, and record cache-scope exit."""
        real_job_cache = broker.task_cache.job_cache

        class Scope:
            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                events.append("scope-exit")

        def recording_job_cache(*arguments, **keywords):
            directory, _scope = real_job_cache(*arguments, **keywords)
            return directory, Scope()

        def failing_wait(process, **_keywords):
            deadline = time.monotonic() + 5
            while len(self.hang_pids()) < 2 and time.monotonic() < deadline:
                time.sleep(0.02)
            self.seen_pids = self.hang_pids()
            raise broker.BrokerError("watchdog crashed")

        return (
            mock.patch.object(broker.task_cache, "job_cache", recording_job_cache),
            mock.patch.object(broker, "_wait_for_process", failing_wait),
        )

    def test_cleanup_confirms_the_group_is_gone_before_the_cache_is_released(self):
        self.submit_hanging()
        events = []
        answers = iter([True, True, False])

        def group_exists(group):
            answer = next(answers, False)
            events.append(f"exists:{answer}")
            return answer

        cache, wait = self.failing_wait_with_scope_events(events)
        with cache, wait, mock.patch.object(
            broker, "_process_group_exists", group_exists
        ):
            result = self.run_with(lambda _dir=None: QUIET)
        self.assertEqual(result["action"], "executed")
        self.assertFalse(result["ok"])
        self.assertEqual(
            events, ["exists:True", "exists:True", "exists:False", "scope-exit"]
        )
        self.assert_gone(self.seen_pids)

    def hold_scenario(self, group_alive_for_sleeps, stop_at_sleep=None):
        """Run a job whose wait crashes, with a cleanup the group survives for a
        while.  Returns (result_or_exception, observations taken during holds)."""
        submitted = self.submit_hanging()
        lock_path = Path(self.temporary.name) / "cache.lock"
        events, seen = [], []
        sleeps = {"count": 0}
        stopping = {"on": False}

        class LockScope:
            def __enter__(inner):
                inner.handle = lock_path.open("w")
                fcntl.flock(inner.handle, fcntl.LOCK_SH)

            def __exit__(inner, *_exc):
                events.append("scope-exit")
                inner.handle.close()

        real_job_cache = broker.task_cache.job_cache

        def locking_job_cache(*arguments, **keywords):
            directory, _scope = real_job_cache(*arguments, **keywords)
            return directory, LockScope()

        def failing_wait(process, **_keywords):
            deadline = time.monotonic() + 5
            while len(self.hang_pids()) < 2 and time.monotonic() < deadline:
                time.sleep(0.02)
            self.seen_pids = self.hang_pids()
            raise broker.BrokerError("watchdog crashed")

        def hold_sleep(_seconds):
            sleeps["count"] += 1
            probe = lock_path.open("w")
            try:
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
                other_holder_got_the_lock = True
            except BlockingIOError:
                other_holder_got_the_lock = False
            finally:
                probe.close()
            running = self.root / "running" / submitted["id"]
            seen.append({
                "other_holder_got_the_lock": other_holder_got_the_lock,
                "work_dirs": len(list(self.work.iterdir())),
                "marker": queue.load_json(running / "manifest.json").get(
                    "cleanup_unconfirmed"
                ),
                "failed": (self.root / "failed" / submitted["id"]).exists(),
            })
            if stop_at_sleep is not None and sleeps["count"] >= stop_at_sleep:
                stopping["on"] = True

        def group_exists(_group):
            if stop_at_sleep is not None:
                return True
            return sleeps["count"] < group_alive_for_sleeps

        hold = functools.partial(
            pi_cleanup_hold.CleanupHold, sleep=hold_sleep, log=lambda _line: None
        )
        patches = [
            mock.patch.object(broker.task_cache, "job_cache", locking_job_cache),
            mock.patch.object(broker, "_wait_for_process", failing_wait),
            mock.patch.object(broker, "_process_group_exists", group_exists),
            mock.patch.object(broker, "CleanupHold", hold),
            mock.patch.object(engine_process, "GROUP_GONE_TIMEOUT", 0.1),
        ]
        with contextlib.ExitStack() as stack:
            for patch in patches:
                stack.enter_context(patch)
            try:
                outcome = self.run_with(
                    lambda _dir=None: QUIET, should_stop=lambda: stopping["on"]
                )
            except broker.TerminationUnconfirmed as exc:
                outcome = exc
        return submitted, outcome, seen, events

    def test_a_group_that_will_not_die_is_held_with_cache_lock_and_work_dir(self):
        submitted, result, seen, events = self.hold_scenario(group_alive_for_sleeps=3)
        self.assertGreaterEqual(len(seen), 3)
        for observation in seen:
            self.assertFalse(observation["other_holder_got_the_lock"])
            self.assertEqual(observation["work_dirs"], 1)
            self.assertIsNotNone(observation["marker"])
            self.assertFalse(observation["failed"])
        # Once confirmed gone, the original error takes the normal failure path.
        self.assertEqual(result["action"], "executed")
        self.assertFalse(result["ok"])
        self.assertEqual(events, ["scope-exit"])
        self.assertTrue((self.root / "failed" / submitted["id"]).is_dir())
        self.assertNotIn(
            "cleanup_unconfirmed",
            queue.load_json(self.root / "failed" / submitted["id"] / "manifest.json"),
        )
        self.assertNotIn(
            job_cleanup_marker.REASON,
            [
                entry.get("reason")
                for entry in queue.load_json(
                    self.root / "failed" / submitted["id"] / "manifest.json"
                ).get("attempt_history", [])
            ],
        )
        self.assert_gone(self.seen_pids)

    def test_a_failing_kill_and_reap_goes_to_the_hold_not_to_publication(self):
        with mock.patch.object(
            pi_cleanup_hold, "kill_and_reap_group",
            side_effect=PermissionError("denied"),
        ):
            submitted, result, seen, _events = self.hold_scenario(
                group_alive_for_sleeps=2
            )
        self.assertGreaterEqual(len(seen), 2)
        self.assertFalse(any(item["failed"] for item in seen))
        self.assertFalse(any(item["other_holder_got_the_lock"] for item in seen))
        self.assertEqual(result["action"], "executed")
        self.assertTrue((self.root / "failed" / submitted["id"]).is_dir())
        # The faked "gone" answer ended the hold before the kill landed, so the
        # unreaped leader is still a zombie; reap it before checking.
        for pid in self.seen_pids:
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass
        self.assert_gone(self.seen_pids)

    def test_a_stop_during_the_hold_publishes_and_requeues_nothing(self):
        submitted, error, seen, events = self.hold_scenario(
            group_alive_for_sleeps=0, stop_at_sleep=2
        )
        self.assertIsInstance(error, broker.TerminationUnconfirmed)
        self.assertEqual(events, ["scope-exit"])
        running = self.root / "running" / submitted["id"]
        self.assertTrue(running.is_dir())
        self.assertFalse((self.root / "queued" / submitted["id"]).exists())
        self.assertFalse((self.root / "failed" / submitted["id"]).exists())
        manifest = queue.load_json(running / "manifest.json")
        self.assertIsNotNone(manifest.get("cleanup_unconfirmed"))
        self.assertEqual(
            manifest["attempt_history"][-1]["reason"], job_cleanup_marker.REASON
        )

    def test_serve_exits_cleanly_when_a_stop_ends_a_cleanup_hold(self):
        with mock.patch.object(
            broker, "run_once", side_effect=broker.TerminationUnconfirmed("stopped")
        ):
            broker.serve(self.args, mock.Mock())  # returns instead of raising

    def claim_and_mark(self, marked):
        submitted = self.submit() if not marked else self.submit_hanging()
        manifest, reason = broker.claim_local_job(
            self.root, grace_seconds=0, remote_max_age=45,
            eligibility=broker.local_eligibility,
        )
        self.assertIsNotNone(manifest, reason)
        if marked:
            job_cleanup_marker.mark_cleanup_unconfirmed(
                self.root, submitted["id"], manifest["lease_token"],
                worker=broker.LOCAL_WORKER, now=dt.datetime.now(dt.timezone.utc),
            )
        return submitted

    def test_a_marked_job_is_failed_on_resume_without_running_again(self):
        submitted = self.claim_and_mark(True)
        result = self.run_with(lambda _dir=None: QUIET)
        self.assertEqual(result["action"], "executed")
        self.assertFalse(result["ok"])
        self.assertEqual(self.hang_pids(), [], "the job was run again")
        self.assertTrue((self.root / "failed" / submitted["id"]).is_dir())
        self.assertIn("previous broker instance", json.dumps(result))

    def test_an_unmarked_running_job_still_resumes(self):
        submitted = self.claim_and_mark(False)
        result = self.run_with(lambda _dir=None: QUIET)
        self.assertEqual(result["action"], "executed")
        self.assertTrue(result["ok"], result)

    def test_marker_needs_the_exact_lease_token(self):
        submitted = self.submit()
        broker.claim_local_job(
            self.root, grace_seconds=0, remote_max_age=45,
            eligibility=broker.local_eligibility,
        )
        with self.assertRaises(queue.QueueError):
            job_cleanup_marker.mark_cleanup_unconfirmed(
                self.root, submitted["id"], secrets.token_hex(16),
                worker=broker.LOCAL_WORKER, now=dt.datetime.now(dt.timezone.utc),
            )

    def test_an_already_reaped_leader_with_running_descendants_is_still_cleaned_up(self):
        leader = subprocess.Popen(
            [
                sys.executable, "-c",
                "import subprocess; subprocess.Popen(['sleep', '30'])",
            ],
            start_new_session=True,
        )
        leader.wait()  # reaped, so kill_and_reap_group has no child to wait for
        group = leader.pid
        self.addCleanup(self.kill_group, group)
        self.assertTrue(engine_process.process_group_exists(
            group, host=engine_process.Host.PI, error_type=broker.BrokerError
        ))
        with mock.patch.object(
            broker, "_wait_for_process", side_effect=broker.BrokerError("crash")
        ), self.assertRaises(broker.BrokerError):
            broker._supervise_child(
                leader, self.args, self.work, StopLatch(lambda: False, None),
                pi_cleanup_hold.CleanupHold(
                    job="job",
                    group_exists=broker._process_group_exists,
                    service_stopping=lambda: False,
                    mark=lambda: None,
                    unmark=lambda: None,
                    unconfirmed_error=broker.TerminationUnconfirmed,
                ),
            )
        # Gone for good: not even a zombie answers (macOS says EPERM for those).
        with self.assertRaises(ProcessLookupError):
            os.killpg(group, 0)

    @staticmethod
    def kill_group(group):
        try:
            os.killpg(group, 9)
        except ProcessLookupError:
            pass


class RequeueOwnershipTests(BrokerHarness, unittest.TestCase):
    def claim(self):
        submitted = self.submit()
        manifest, reason = broker.claim_local_job(
            self.root, grace_seconds=0, remote_max_age=45,
            eligibility=broker.local_eligibility,
        )
        self.assertIsNotNone(manifest, reason)
        return submitted["id"], manifest["lease_token"]

    def requeue(self, job_id, token):
        return job_requeue.requeue_owned_job(
            self.root, job_id, token, worker=broker.LOCAL_WORKER, reason="test",
            now=dt.datetime.now(dt.timezone.utc), error_type=broker.BrokerError,
        )

    def set_stored_token(self, job_id, **change):
        path = self.root / "running" / job_id / "manifest.json"
        manifest = queue.load_json(path)
        if "lease_token" in change and change["lease_token"] is None:
            manifest.pop("lease_token")
        else:
            manifest.update(change)
        queue.atomic_json(path, manifest)

    def test_only_the_exact_lease_token_requeues(self):
        job_id, token = self.claim()
        self.assertEqual(self.requeue(job_id, token), job_requeue.REQUEUED)
        self.assertTrue((self.root / "queued" / job_id).is_dir())

    def test_a_missing_malformed_or_different_stored_token_changes_nothing(self):
        for label, stored in (
            ("missing", None),
            ("malformed", "not-a-token"),
            ("different", secrets.token_hex(16)),
        ):
            with self.subTest(label):
                job_id, token = self.claim()
                self.set_stored_token(job_id, lease_token=stored)
                before = queue.load_json(
                    self.root / "running" / job_id / "manifest.json"
                )
                self.assertEqual(
                    self.requeue(job_id, token), job_requeue.OWNERSHIP_LOST
                )
                self.assertTrue((self.root / "running" / job_id).is_dir())
                self.assertFalse((self.root / "queued" / job_id).exists())
                self.assertEqual(
                    queue.load_json(self.root / "running" / job_id / "manifest.json"),
                    before,
                )
                # Reset for the next subtest: put it back in the queue by hand.
                (self.root / "running" / job_id).rename(self.root / "failed" / job_id)


if __name__ == "__main__":
    unittest.main()
