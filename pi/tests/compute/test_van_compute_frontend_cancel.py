"""`pi_compute.py cancel`, exit 4, and `run --cancel-on-timeout` (in process)."""

import contextlib
import io
import json
import unittest
from unittest import mock

from van_compute import frontend, queue

from pi.tests.compute.test_van_compute_cancel import MAC, CancelHarness


class FrontendCancelTests(CancelHarness, unittest.TestCase):
    def cli(self, *arguments):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                code = frontend.main(["--root", str(self.root), *arguments])
            except SystemExit as exc:  # argparse errors
                code = exc.code
        payload = json.loads(out.getvalue()) if out.getvalue().strip() else None
        return code, payload, err.getvalue()

    def run_args(self, *extra):
        return ("run", "local-python", "--source-root", str(self.source), *extra)

    def test_cancel_reports_each_outcome(self):
        job = self.submit()
        code, payload, _ = self.cli("cancel", job["id"])
        self.assertEqual((code, payload["cancel_result"], payload["state"]), (0, "cancelled", "cancelled"))
        self.assertNotIn("lease_token", payload)
        code, payload, _ = self.cli("cancel", job["id"])
        self.assertEqual((code, payload["cancel_result"]), (0, "already-cancelled"))
        done = self.submit()
        claimed = queue.worker_claim(self.root, MAC)
        queue.worker_finish(self.root, done["id"], MAC, 0, [], claimed["lease_token"])
        code, payload, _ = self.cli("cancel", done["id"])
        self.assertEqual((code, payload["cancel_result"], payload["state"]), (0, "already-finished", "done"))
        code, payload, error = self.cli("cancel", "20260101T000000Z-0000dead")
        self.assertEqual((code, payload), (2, None))
        self.assertIn("unknown job", error)

    def test_wait_status_and_list_show_a_cancelled_job(self):
        job = self.submit()
        self.cli("cancel", job["id"])
        code, payload, _ = self.cli("wait", job["id"], "--timeout", "5")
        self.assertEqual((code, payload["state"]), (4, "cancelled"))
        code, payload, _ = self.cli("status", job["id"])
        self.assertEqual((code, payload["state"]), (0, "cancelled"))
        code, payload, _ = self.cli("list")
        self.assertEqual([item["state"] for item in payload["jobs"]], ["cancelled"])

    def test_a_job_finished_before_the_timeout_cancel_keeps_its_outcome(self):
        real = queue.wait_for_job

        def finish_then_time_out(root, job_id, timeout, **kwargs):
            claimed = queue.worker_claim(root, MAC)
            queue.worker_finish(root, job_id, MAC, 1, [], claimed["lease_token"])
            return {**queue.manifest_for(root, job_id), "state": "queued", "wait_timed_out": True}

        with mock.patch.object(queue, "wait_for_job", finish_then_time_out):
            code, payload, _ = self.cli(*self.run_args("--wait", "0", "--cancel-on-timeout"))
        self.assertIs(queue.wait_for_job, real)
        self.assertEqual((code, payload["cancel_result"], payload["state"]), (1, "already-finished", "failed"))


if __name__ == "__main__":
    unittest.main()
