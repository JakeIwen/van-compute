"""Cancelling a job: each path, each guard, and what readers see."""

import datetime as dt
import io
import json
import unittest
from unittest import mock

from van_compute import broker, job_cancel, job_requeue, job_states, queue

from pi.tests.compute.van_compute_test_fixtures import BrokerHarness

MAC = "m4mac.03"


class CancelHarness(BrokerHarness):
    def setUp(self):
        super().setUp()
        queue.safe_root(self.root)

    def claim_remote(self, worker=MAC):
        claimed = queue.worker_claim(self.root, worker)
        self.assertIsNotNone(claimed)
        return claimed

    def claim_local(self):
        claimed, reason = broker.claim_local_job(
            self.root, grace_seconds=0, remote_max_age=45.0,
            eligibility=lambda _manifest: (True, "eligible"),
        )
        self.assertIsNotNone(claimed, reason)
        return claimed

    def located(self, job_id):
        return [
            state for state in job_states.STATE_DIRECTORIES
            if (self.root / state / job_id).is_dir()
        ]

    def manifest(self, state, job_id):
        return json.loads((self.root / state / job_id / "manifest.json").read_text())

    def intents(self):
        directory = self.root / job_cancel.INTENTS
        return sorted(p.name for p in directory.iterdir()) if directory.is_dir() else []

    def assert_final(self, job_id, cancelled_from):
        self.assertEqual(self.located(job_id), ["cancelled"])
        manifest = self.manifest("cancelled", job_id)
        self.assertEqual(manifest["state"], "cancelled")
        self.assertEqual(manifest["cancelled_from"], cancelled_from)
        self.assertNotIn("lease_token", manifest)
        for field in ("cancel_requested_at", "cancelled_at", "finished_at", "cancel_reason"):
            self.assertIsInstance(manifest[field], str, field)
        self.assertFalse((self.root / "cancelled" / job_id / "source").exists())
        self.assertNotIn(job_id, self.intents())
        return manifest


class CancelPathTests(CancelHarness, unittest.TestCase):
    def test_a_queued_job_is_cancelled_at_once_and_its_source_detached(self):
        job = self.submit()
        result, manifest = job_cancel.cancel_job(self.root, job["id"])
        self.assertEqual(result, "cancelled")
        self.assertEqual(manifest["state"], "cancelled")
        final = self.assert_final(job["id"], "queued")
        self.assertEqual(final["cancel_reason"], "cancel-command")
        trash = [p.name for p in (self.root / "cancelled" / job["id"]).iterdir()]
        self.assertTrue(any(name.startswith(".source-trash-") for name in trash))
        self.assertIsNone(queue.worker_claim(self.root, MAC))

    def test_a_remote_running_job_is_cancelled_at_once_and_its_mac_fenced(self):
        job = self.submit()
        claimed = self.claim_remote()
        result, _manifest = job_cancel.cancel_job(self.root, job["id"])
        self.assertEqual(result, "cancelled")
        final = self.assert_final(job["id"], "running")
        self.assertEqual(final["worker"], MAC)
        token = claimed["lease_token"]
        with self.assertRaisesRegex(queue.QueueError, "not running"):
            queue.put_result(self.root, job["id"], MAC, "late.txt", io.BytesIO(b"x"), token)
        with self.assertRaisesRegex(queue.QueueError, "not running"):
            queue.worker_finish(self.root, job["id"], MAC, 0, [], token)
        self.assertEqual(self.located(job["id"]), ["cancelled"])

    def test_a_pi_local_running_job_gets_a_request_its_finish_routes_to_cancelled(self):
        job = self.submit()
        claimed = self.claim_local()
        result, manifest = job_cancel.cancel_job(self.root, job["id"])
        self.assertEqual(result, "requested")
        self.assertEqual(manifest["state"], "running")
        self.assertEqual(self.located(job["id"]), ["running"])
        running = self.manifest("running", job["id"])
        self.assertEqual(running["lease_token"], claimed["lease_token"])
        self.assertIsInstance(running["cancel_requested_at"], str)
        self.assertEqual(self.intents(), [job["id"]])
        # A second cancel changes nothing.
        again, _ = job_cancel.cancel_job(self.root, job["id"])
        self.assertEqual(again, "requested")
        self.assertEqual(self.manifest("running", job["id"]), running)
        queue.put_result(
            self.root, job["id"], broker.LOCAL_WORKER, "stdout.txt", io.BytesIO(b"out\n"),
            claimed["lease_token"],
        )
        finished = queue.worker_finish(
            self.root, job["id"], broker.LOCAL_WORKER, 0, ["stdout.txt"], claimed["lease_token"],
        )
        self.assertEqual(finished["state"], "cancelled")
        final = self.assert_final(job["id"], "running")
        self.assertEqual(final["exit_code"], 0)
        self.assertEqual([item["path"] for item in final["results"]], ["stdout.txt"])
        output = io.BytesIO()
        queue.stream_result(self.root, job["id"], "stdout.txt", output)
        self.assertEqual(output.getvalue(), b"out\n")

    def test_cancel_is_idempotent_and_reports_finished_jobs_unchanged(self):
        job = self.submit()
        job_cancel.cancel_job(self.root, job["id"])
        before = self.manifest("cancelled", job["id"])
        result, manifest = job_cancel.cancel_job(self.root, job["id"])
        self.assertEqual(result, "already-cancelled")
        self.assertEqual(manifest, before)
        done = self.submit()
        claimed = self.claim_remote()
        queue.worker_finish(self.root, done["id"], MAC, 0, [], claimed["lease_token"])
        before = self.manifest("done", done["id"])
        result, manifest = job_cancel.cancel_job(self.root, done["id"])
        self.assertEqual(result, "already-finished")
        self.assertEqual(self.manifest("done", done["id"]), before)

    def test_unknown_ids_reasons_and_maintenance_are_refused(self):
        with self.assertRaisesRegex(queue.QueueError, "unknown job"):
            job_cancel.cancel_job(self.root, "20260101T000000Z-0000dead")
        with self.assertRaisesRegex(queue.QueueError, "invalid job id"):
            job_cancel.cancel_job(self.root, "../etc")
        job = self.submit()
        with self.assertRaisesRegex(queue.QueueError, "invalid cancel reason"):
            job_cancel.cancel_job(self.root, job["id"], "because")
        queue.set_maintenance(self.root, "golden-admin", True)
        with self.assertRaisesRegex(queue.QueueError, "maintenance"):
            job_cancel.cancel_job(self.root, job["id"])
        self.assertEqual(self.located(job["id"]), ["queued"])



class CancelGuardTests(CancelHarness, unittest.TestCase):
    def request_in_place(self, state, job_id):
        path = self.root / state / job_id
        manifest = queue.load_json(path / "manifest.json")
        with queue.queue_lock(self.root):
            job_cancel.record_request(self.root, path, state, manifest)
        return manifest

    def test_a_slot_resuming_its_own_requested_job_finalizes_it_and_claims_the_next(self):
        first = self.submit()
        self.claim_remote()
        second = self.submit()
        self.request_in_place("running", first["id"])
        claimed = queue.worker_claim(self.root, MAC)
        self.assertEqual(claimed["id"], second["id"])
        self.assert_final(first["id"], "running")

    def test_a_requested_queued_job_is_skipped_by_a_remote_claim(self):
        # Claims take the oldest id first; request the one that sorts first.
        first, second = sorted(self.submit()["id"] for _ in range(2))
        self.request_in_place("queued", first)
        claimed = queue.worker_claim(self.root, MAC)
        self.assertEqual(claimed["id"], second)
        self.assert_final(first, "queued")

    def test_the_broker_never_claims_a_requested_queued_job(self):
        job = self.submit()
        self.request_in_place("queued", job["id"])
        eligible = job_cancel.unless_cancel_requested(lambda _m: (True, "eligible"))
        claimed, reason = broker.claim_local_job(
            self.root, grace_seconds=0, remote_max_age=45.0, eligibility=eligible,
        )
        self.assertIsNone(claimed)
        self.assertEqual(reason, "cancel requested")
        result = self.run_once()
        self.assertNotEqual(result.get("action"), "executed")
        self.assert_final(job["id"], "queued")

    def test_stale_remote_recovery_cancels_a_requested_job_instead_of_requeueing(self):
        job = self.submit()
        self.claim_remote()
        self.request_in_place("running", job["id"])
        later = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)
        recovered = broker.recover_stale_remote_jobs(self.root, stale_age=300.0, now=later)
        self.assertEqual(recovered, [job["id"]])
        final = self.assert_final(job["id"], "running")
        self.assertEqual(final["attempt_history"][-1]["reason"], "remote-worker-lease-expired")
        self.assertEqual(list((self.root / "cancelled" / job["id"] / "result").iterdir()), [])

    def test_a_requested_job_stopped_for_can_is_cancelled_not_requeued(self):
        job = self.submit()
        claimed = self.claim_local()
        job_cancel.cancel_job(self.root, job["id"])
        outcome = job_requeue.requeue_owned_job(
            self.root, job["id"], claimed["lease_token"], worker=broker.LOCAL_WORKER,
            reason="pi-can-transmit", now=dt.datetime.now(dt.timezone.utc),
        )
        self.assertEqual(outcome, job_requeue.CANCELLED)
        final = self.assert_final(job["id"], "running")
        self.assertEqual(final["attempt_history"][-1]["reason"], "pi-can-transmit")

    def test_an_unrequested_requeue_is_unchanged(self):
        job = self.submit()
        claimed = self.claim_local()
        outcome = job_requeue.requeue_owned_job(
            self.root, job["id"], claimed["lease_token"], worker=broker.LOCAL_WORKER,
            reason="pi-can-transmit", now=dt.datetime.now(dt.timezone.utc),
        )
        self.assertEqual(outcome, job_requeue.REQUEUED)
        self.assertEqual(self.located(job["id"]), ["queued"])

    def test_one_failed_read_does_not_hide_a_request_in_an_unchanged_manifest(self):
        job = self.submit()
        self.claim_local()
        job_cancel.cancel_job(self.root, job["id"])
        watch = job_cancel.CancelWatch(self.root / "running" / job["id"] / "manifest.json")
        real_load = queue.load_json
        reads = []

        def flaky_load(path):
            reads.append(path)
            if len(reads) == 1:
                raise queue.QueueError("transient read failure")
            return real_load(path)

        with mock.patch.object(job_cancel.queue, "load_json", flaky_load):
            self.assertFalse(watch.requested())
            self.assertTrue(watch.requested())  # same file, never rewritten
        self.assertEqual(len(reads), 2)


if __name__ == "__main__":
    unittest.main()


class CancelledMetricsTests(CancelHarness, unittest.TestCase):
    def test_metrics_reports_do_not_change_for_cancelled_jobs(self):
        from van_compute import metrics

        done = self.submit()
        claimed = self.claim_remote()
        queue.worker_finish(self.root, done["id"], MAC, 0, [], claimed["lease_token"])
        now = dt.datetime.now(dt.timezone.utc).timestamp() + 5
        reader = metrics.ComputeMetricsReader(self.root, clock=lambda: now)
        before = reader.report(24)
        job_cancel.cancel_job(self.root, self.submit()["id"])
        self.assertEqual(metrics.ComputeMetricsReader(self.root, clock=lambda: now).report(24), before)
