"""A cancel that dies between steps is finished by reconcile on the next broker poll.

Each case stops the cancel at one step boundary by raising from the next step
(the queue lock is released as it would be when a process dies), checks what
readers see, then runs one broker poll and checks the job ended final:
in cancelled/, final manifest, source detached, intent gone.
"""

from pathlib import Path
import signal
import subprocess
import sys
import unittest
from unittest import mock

from van_compute import broker, finished_source, job_cancel, queue

from pi.tests.compute.test_van_compute_cancel import MAC, CancelHarness
from pi.tests.compute.van_compute_test_fixtures import GOOD_HEALTH


REPO = Path(__file__).resolve().parents[3]

# A cancel whose process is killed inside its first write (the intent), after
# `atomic_json` created its temporary and before the rename.
KILLED_IN_INTENT_WRITE = """
import os, signal, sys
from pathlib import Path
from van_compute import job_cancel
os.fsync = lambda _fd: os.kill(os.getpid(), signal.SIGKILL)
job_cancel.cancel_job(Path(sys.argv[1]), sys.argv[2])
"""


class Crash(Exception):
    pass


def _crash(*_args, **_kwargs):
    raise Crash()


def _unhealthy():
    return broker.HealthSnapshot(**{
        **GOOD_HEALTH.__dict__, "memory_available_bytes": 1, "temperature_c": 95.0,
    })


def active_queue_jobs(root):
    """The installer's count (phases.active_queue_jobs): queued + running entries."""
    return sum(1 for state in ("queued", "running") for _ in (root / state).iterdir())


class CrashTests(CancelHarness, unittest.TestCase):
    STEPS = {
        # Dies after step 0: the in-place request write never happens.
        "after-intent": ("record_request", lambda: mock.patch.object(
            job_cancel.queue, "atomic_json", side_effect=_atomic_after_intent())),
        "after-request": ("rename", lambda: mock.patch.object(
            job_cancel.os, "replace", side_effect=_replace_crash_into_cancelled())),
        "after-rename": ("final", lambda: mock.patch.object(
            job_cancel.queue, "atomic_json", side_effect=_atomic_crash_in_cancelled())),
        "after-final-write": ("detach", lambda: mock.patch.object(
            job_cancel.finished_source, "finish_detach", side_effect=_crash)),
        "after-detach": ("intent", lambda: mock.patch.object(
            job_cancel, "_remove_intent", side_effect=_crash)),
    }

    def crash_cancel(self, step, job_id):
        _name, patcher = self.STEPS[step]
        with patcher():
            with self.assertRaises(Crash):
                job_cancel.cancel_job(self.root, job_id)

    def poll(self, *, healthy=True):
        reader = (lambda: GOOD_HEALTH) if healthy else _unhealthy
        with mock.patch.object(broker, "bubblewrap_self_test", return_value=(True, "t")), \
                mock.patch.object(broker, "bubblewrap_command", side_effect=self.no_sandbox):
            return broker.run_once(self.args, health_reader=reader)

    def fresh_queue(self):
        self.tearDown()
        self.setUp()

    def check_step(self, step, *, remote, healthy=True, fresh_mac=False):
        self.fresh_queue()
        job = self.submit()
        if remote:
            self.claim_remote()
        if fresh_mac:
            queue.worker_heartbeat(self.root, "m4mac", 10, 1)
        self.crash_cancel(step, job["id"])
        origin = "running" if remote else "queued"
        if step == "after-intent":
            # The cancel never reported success: nothing changed but the intent.
            self.assertEqual(self.located(job["id"]), [origin])
            self.assertNotIn("cancel_requested_at", self.manifest(origin, job["id"]))
        elif step == "after-request":
            self.assertEqual(self.located(job["id"]), [origin])
            stranded = self.manifest(origin, job["id"])
            self.assertIn("cancel_requested_at", stranded)
            if remote:
                self.assertIn("lease_token", stranded)
            self.assertEqual(active_queue_jobs(self.root), 1)
        else:
            self.assertEqual(queue.manifest_for(self.root, job["id"])["state"], "cancelled")
            self.assertEqual(queue.wait_for_job(self.root, job["id"], 0)["state"], "cancelled")
        self.poll(healthy=healthy)
        if step == "after-intent":
            self.assertEqual(self.intents(), [])
            return job["id"]
        self.assertEqual(active_queue_jobs(self.root), 0)
        self.assert_final(job["id"], origin)
        if step == "after-final-write":
            # The detach the crash skipped ran: source/ became a trash marker.
            names = [p.name for p in (self.root / "cancelled" / job["id"]).iterdir()]
            self.assertTrue(any(n.startswith(finished_source.TRASH_PREFIX) for n in names))
        return job["id"]

    def test_every_step_boundary_for_a_queued_job(self):
        for step in self.STEPS:
            with self.subTest(step=step):
                self.check_step(step, remote=False, fresh_mac=True)

    def test_every_step_boundary_for_a_remote_running_job(self):
        for step in self.STEPS:
            with self.subTest(step=step):
                self.check_step(step, remote=True, fresh_mac=True)

    def test_reconcile_runs_before_the_health_gate_and_without_a_mac(self):
        for step in ("after-request", "after-rename"):
            with self.subTest(step=step):
                self.check_step(step, remote=False, healthy=False)

    def test_reconcile_leaves_a_pi_local_request_to_the_broker(self):
        job = self.submit()
        self.claim_local()
        job_cancel.cancel_job(self.root, job["id"])
        self.assertEqual(job_cancel.reconcile(self.root), {job["id"]: "broker-owned"})
        self.assertEqual(self.located(job["id"]), ["running"])

    def test_reconcile_waits_out_maintenance(self):
        job = self.submit()
        self.crash_cancel("after-rename", job["id"])
        queue.set_maintenance(self.root, "golden-admin", True)
        self.assertEqual(job_cancel.reconcile(self.root), {})
        queue.set_maintenance(self.root, "golden-admin", False)
        self.assertEqual(job_cancel.reconcile(self.root), {job["id"]: "completed"})
        self.assert_final(job["id"], "queued")

    def test_reconcile_removes_an_intent_write_its_dead_writer_left(self):
        job = self.submit()
        killed = subprocess.run(
            [sys.executable, "-c", KILLED_IN_INTENT_WRITE, str(self.root), job["id"]],
            cwd=REPO, capture_output=True,
        )
        self.assertEqual(killed.returncode, -signal.SIGKILL, killed.stderr)
        leftovers = self.intents()
        self.assertEqual(len(leftovers), 1)
        self.assertTrue(leftovers[0].startswith(f".{job['id']}."), leftovers)
        self.assertEqual(job_cancel.reconcile(self.root), {})
        self.assertEqual(self.intents(), [])
        # The cancel never reported success: the job is untouched.
        self.assertEqual(self.located(job["id"]), ["queued"])
        self.assertNotIn("cancel_requested_at", self.manifest("queued", job["id"]))

    def test_a_remote_request_stranded_in_running_stays_fenced_to_its_owner(self):
        job = self.submit()
        claimed = self.claim_remote()
        self.crash_cancel("after-request", job["id"])
        with self.assertRaisesRegex(queue.QueueError, "lease token does not own"):
            queue.worker_finish(self.root, job["id"], MAC, 0, [], "f" * 32)
        finished = queue.worker_finish(self.root, job["id"], MAC, 0, [], claimed["lease_token"])
        self.assertEqual(finished["state"], "cancelled")


def _atomic_after_intent():
    real = queue.atomic_json

    def write(path, payload, mode=0o600):
        if path.name == "manifest.json":
            raise Crash()
        return real(path, payload, mode)

    return write


def _replace_crash_into_cancelled():
    import os
    real = os.replace

    def replace(source, destination):
        if Path(destination).parent.name == "cancelled":
            raise Crash()
        return real(source, destination)

    return replace


def _atomic_crash_in_cancelled():
    real = queue.atomic_json

    def write(path, payload, mode=0o600):
        if path.parent.parent.name == "cancelled":
            raise Crash()
        return real(path, payload, mode)

    return write


if __name__ == "__main__":
    unittest.main()
