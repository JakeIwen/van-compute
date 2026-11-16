"""The Mac worker learns its lease is gone, stops the run, and launches nothing late."""

from contextlib import contextmanager
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from van_compute import job_cancel, lease_report, queue, worker
from van_compute.mac_lease_audit import LeaseAudit, LeaseLost, RunStop

from pi.tests.compute.test_van_compute_cancel import MAC, CancelHarness
from pi.tests.compute.van_compute_test_fixtures import worker_config


class Transport(Exception):
    pass


class FakeRemote:
    worker = "m4mac"

    def __init__(self, replies):
        self.replies = list(replies)
        self.calls = []
        self.during = None

    def json_command(self, *arguments):
        self.calls.append(arguments)
        if self.during is not None:
            self.during()
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply


def reply(*leases, complete=True):
    return {"worker": "m4mac", "complete": complete, "leases": [
        {"worker": w, "job": j, "attempt": a, "cancel_requested": c} for w, j, a, c in leases
    ]}


JOB = "20260722T120000Z-00000001"
NEXT = "20260722T120000Z-00000002"


class LeaseReportTests(CancelHarness, unittest.TestCase):
    def test_the_report_lists_this_workers_slot_leases_only(self):
        for _ in range(3):
            self.submit()
        # The local claim first: a remote claim writes a fresh heartbeat,
        # which keeps the Pi fallback from claiming.
        local = self.claim_local()
        job_cancel.cancel_job(self.root, local["id"])
        first = queue.worker_claim(self.root, MAC)
        second = queue.worker_claim(self.root, "otherhost.01")
        report = lease_report.leases_for(self.root, "m4mac")
        self.assertEqual(report, {"worker": "m4mac", "complete": True, "leases": [
            {"worker": MAC, "job": first["id"], "attempt": 1, "cancel_requested": False},
        ]})
        self.assertNotIn(second["id"], json.dumps(report))

    def test_an_unreadable_running_manifest_makes_the_report_incomplete(self):
        self.submit()
        claimed = queue.worker_claim(self.root, MAC)
        (self.root / "running" / claimed["id"] / "manifest.json").write_text("{")
        self.assertFalse(lease_report.leases_for(self.root, "m4mac")["complete"])

    def test_the_rpc_checks_the_protocol_version(self):
        self.submit()
        claimed = queue.worker_claim(self.root, MAC)
        out = io.StringIO()
        with mock.patch("sys.stdout", out):
            code = queue.main(["--root", str(self.root), "worker", "leases",
                               "--worker", "m4mac", "--protocol-version", "2"])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(out.getvalue())["leases"][0]["job"], claimed["id"])
        with mock.patch("sys.stderr", io.StringIO()):
            code = queue.main(["--root", str(self.root), "worker", "leases",
                               "--worker", "m4mac", "--protocol-version", "1"])
        self.assertEqual(code, 2)


class LeaseAuditTests(unittest.TestCase):
    def audit(self, replies, clock=None):
        remote = FakeRemote(replies)
        logs = []
        audit = LeaseAudit(remote, 60.0, transport_errors=(Transport,),
                           clock=clock or time.monotonic, log=logs.append)
        self.addCleanup(audit.close)
        return audit, remote, logs

    def test_a_missing_or_cancelled_lease_stops_its_run_and_only_it(self):
        audit, _remote, logs = self.audit([reply(
            ("m4mac.01", JOB, 1, False), ("m4mac.02", NEXT, 1, True),
        )])
        kept = audit._register("m4mac.01", {"id": JOB, "attempt": 1})
        cancelled = audit._register("m4mac.02", {"id": NEXT, "attempt": 1})
        missing = audit._register("m4mac.03", {"id": "20260722T120000Z-00000003", "attempt": 1})
        stopped = audit.audit_once()
        self.assertEqual(stopped, [cancelled, missing])
        self.assertFalse(kept.stop.is_set())
        self.assertEqual(len(logs), 2)

    def test_a_later_attempt_on_the_pi_means_this_attempt_lost_its_lease(self):
        audit, _remote, _logs = self.audit([reply(("m4mac.01", JOB, 2, False))])
        run = audit._register("m4mac.01", {"id": JOB, "attempt": 1})
        self.assertEqual(audit.audit_once(), [run])

    def test_a_reply_cannot_stop_a_run_registered_after_it_was_requested(self):
        audit, remote, _logs = self.audit([reply()])
        old = audit._register("m4mac.01", {"id": JOB, "attempt": 1})
        late = []

        def slot_moves_on():
            audit._unregister(old)
            late.append(audit._register("m4mac.01", {"id": NEXT, "attempt": 1}))

        remote.during = slot_moves_on
        self.assertEqual(audit.audit_once(), [])
        self.assertFalse(late[0].stop.is_set())

    def test_incomplete_replies_and_idle_audits_change_nothing(self):
        audit, remote, _logs = self.audit([reply(complete=False)])
        self.assertEqual(audit.audit_once(), [])
        self.assertEqual(remote.calls, [])  # no runs, no RPC
        run = audit._register("m4mac.01", {"id": JOB, "attempt": 1})
        self.assertEqual(audit.audit_once(), [])
        self.assertFalse(run.stop.is_set())

    def test_an_old_pi_backs_the_audit_off_and_transport_errors_retry(self):
        now = [0.0]
        audit, remote, logs = self.audit(
            [Transport("down"), RuntimeError("invalid choice: 'leases'"), reply()],
            clock=lambda: now[0],
        )
        audit._register("m4mac.01", {"id": JOB, "attempt": 1})
        self.assertEqual(audit.audit_once(), [])
        self.assertEqual(logs, [])
        self.assertEqual(audit.audit_once(), [])
        self.assertEqual(len(logs), 1)
        now[0] = 599.0
        audit.audit_once()
        self.assertEqual(len(remote.calls), 2)
        now[0] = 601.0
        self.assertEqual(len(audit.audit_once()), 1)
        self.assertEqual(len(remote.calls), 3)
        self.assertEqual(remote.calls[0], ("worker", "leases", "--worker", "m4mac",
                                           "--protocol-version", "2"))

    def test_the_wrapper_reports_a_lease_stop_as_lease_lost(self):
        audit, _remote, _logs = self.audit([])

        class Remote:
            worker = "m4mac.01"

        def lease_cut(_args, _remote, _manifest, stop):
            audit.snapshot()[0].stop.set()
            self.assertTrue(stop.is_set())
            raise worker.WorkerShutdown("interrupted")

        with self.assertRaises(LeaseLost):
            audit.wrap(lease_cut)(None, Remote(), {"id": JOB, "attempt": 1}, threading.Event())
        self.assertEqual(audit.snapshot(), [])

        def service_stop(_args, _remote, _manifest, stop):
            raise worker.WorkerShutdown("service stop")

        with self.assertRaises(worker.WorkerShutdown):
            audit.wrap(service_stop)(None, Remote(), {"id": JOB, "attempt": 1}, threading.Event())

    def test_the_audit_thread_runs_while_runs_are_registered(self):
        remote = FakeRemote([reply()] * 50)
        audit = LeaseAudit(remote, 0.01, transport_errors=(Transport,), log=lambda _m: None)
        run = audit._register("m4mac.01", {"id": JOB, "attempt": 1})
        self.assertTrue(run.stop.wait(2))
        audit.close()

    def test_run_stop_waits_on_either_stop(self):
        service, own = threading.Event(), threading.Event()
        stop = RunStop(service, own)
        self.assertFalse(stop.wait(0.01))
        threading.Timer(0.05, own.set).start()
        self.assertTrue(stop.wait(2))
        self.assertTrue(RunStop(None, threading.Event()).wait(0) is False)


SPEC = {
    "profile": "python-script", "family": "python", "argv": ["{source:tools/run.py}"],
    "outputs": [], "datasets": [], "minimum_inputs": 0, "maximum_inputs": 0,
    "input_values": False,
}


class LaunchGateTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        self.source = self.base / "job" / "source"
        (self.source / "tools").mkdir(parents=True)
        (self.source / "tools" / "run.py").write_text("print('ran')\n")
        self.profile = self.base / "test.sb"
        self.profile.write_text("test profile")
        self.launches = []

    @contextmanager
    def patched(self):
        def run_child(*_args, **_kwargs):
            self.launches.append(True)
            raise AssertionError("a stopped run was launched")

        with mock.patch.object(worker, "sandbox_command", lambda command, **_k: list(command)), \
                mock.patch.object(worker.engine, "run_child", run_child):
            yield

    def execute(self, stop):
        return worker.execute_job(
            {"id": JOB, "task": "gate", "arguments": [], "inputs": [], "execution": SPEC},
            source_root=self.source, input_paths=[], input_values=[],
            result_root=self.base / "job" / "result", python=sys.executable, timeout=30,
            nice=0, maximum_file_size=1 << 20, sandbox_profile=self.profile, stop_event=stop,
        )

    def test_a_stop_seen_after_preparation_launches_nothing(self):
        stop = RunStop(None, threading.Event())
        stop.own.set()
        with self.patched(), self.assertRaisesRegex(worker.WorkerShutdown, "before launch"):
            self.execute(stop)
        self.assertEqual(self.launches, [])

    def test_a_stop_set_while_the_cache_lock_blocks_launches_nothing(self):
        stop = RunStop(None, threading.Event())

        @contextmanager
        def blocked_then_stopped():
            stop.own.set()  # the lease audit fired while the flock was waiting
            yield

        def job_cache(_store, _request, directory):
            directory.mkdir(parents=True, exist_ok=True)
            return directory, blocked_then_stopped()

        with self.patched(), mock.patch.object(worker.task_cache, "job_cache", job_cache), \
                self.assertRaisesRegex(worker.WorkerShutdown, "before launch"):
            self.execute(stop)
        self.assertEqual(self.launches, [])

    def test_a_stop_set_during_admission_launches_nothing_and_publishes_nothing(self):
        audit = LeaseAudit(FakeRemote([]), 60.0, log=lambda _m: None)
        self.addCleanup(audit.close)

        class Remote:
            worker = "m4mac.01"

            def put_result(self, *_a, **_k):
                raise AssertionError("uploaded after a lost lease")

            def finish(self, *_a, **_k):
                raise AssertionError("finished after a lost lease")

        class Admission:
            def acquire(self, _manifest, _stop, _drain):
                audit.snapshot()[0].stop.set()
                return mock.Mock()

            def require_free_reserve(self, *_a):
                pass

        args = worker_config(work_root=self.base / "work", resource_manager=Admission(),
                             sandbox_profile=self.profile)
        prepared = (self.source, [], [])
        manifest = {"id": JOB, "attempt": 1, "task": "gate", "arguments": [], "inputs": [],
                    "sources": [], "execution": SPEC, "lease_token": "a" * 32}
        with self.patched(), mock.patch.object(worker, "prepare_job", return_value=prepared):
            with self.assertRaises(LeaseLost):
                audit.wrap(worker.run_claimed_job)(args, Remote(), manifest, threading.Event())
        self.assertEqual(self.launches, [])


BACKGROUND = (
    "import subprocess, sys, time; "
    "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']); "
    "time.sleep(60)"
)


class LaunchedGroupTests(unittest.TestCase):
    def wait(self, process, stop):
        return worker.wait_for_analysis_process(
            process, timeout=30, stop_event=stop, maximum_memory=1 << 40,
            maximum_processes=64, work_path=Path(tempfile.gettempdir()),
            minimum_free_bytes=0,
        )

    def test_a_lease_stop_ends_the_child_and_its_background_descendant(self):
        process = subprocess.Popen([sys.executable, "-c", BACKGROUND], start_new_session=True)
        self.addCleanup(lambda: process.poll() is None and process.kill())
        stop = RunStop(None, threading.Event())
        threading.Timer(0.5, stop.own.set).start()
        outcome = self.wait(process, stop)
        self.assertTrue(outcome.interrupted)
        with self.assertRaises(ProcessLookupError):
            os.killpg(process.pid, 0)

    def test_a_throwing_log_still_reaches_supervision(self):
        for error in (BrokenPipeError, RuntimeError):
            with self.subTest(error=error.__name__):
                class Closed(io.StringIO):
                    def write(self, _text):
                        raise error("stderr closed")

                process = subprocess.Popen([sys.executable, "-c", "pass"], start_new_session=True)
                with mock.patch.object(sys, "stderr", Closed()):
                    outcome = self.wait(process, None)
                self.assertEqual(outcome.exit_code, 0)


if __name__ == "__main__":
    unittest.main()
