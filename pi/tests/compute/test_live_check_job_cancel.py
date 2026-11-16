"""The pure parts of the post-deploy cancel live check (it never runs here)."""

import importlib.util
import json
from pathlib import Path
import sys
import unittest

SCRIPT = Path(__file__).resolve().parents[3] / "macbook/scripts/live_check_job_cancel.py"
spec = importlib.util.spec_from_file_location("live_check_job_cancel", SCRIPT)
live = importlib.util.module_from_spec(spec)
# Registered first: dataclasses look up their module while the class is built.
sys.modules[spec.name] = live
spec.loader.exec_module(live)

JOB = "20261116T010000Z-0000abcd"


class LiveCheckHelperTests(unittest.TestCase):
    def test_the_listing_finds_mac_work_and_foreign_jobs(self):
        text = "\n".join(json.dumps(item) for item in (
            {"id": JOB, "state": "running", "worker": "m4mac.03"},
            {"id": "20261116T010000Z-00000002", "state": "queued", "worker": None},
            {"id": "20261116T010000Z-00000003", "state": "running", "worker": "vanpi-local.00"},
            {"id": ".staging-20261116T010000Z-00000004", "state": "queued", "worker": None},
        )) + "\n"
        entries = live.parse_listing(text)
        self.assertEqual([entry.id for entry in live.mac_work(entries)], [JOB])
        self.assertEqual(
            live.foreign_ids(entries, [JOB]),
            ["20261116T010000Z-00000002", "20261116T010000Z-00000003"],
        )
        self.assertEqual(live.mac_work([live.Entry("x", "running", "m4macbook.01")]), [])

    def test_launch_lines_give_the_exact_group(self):
        log = (
            "van-compute-worker: launched process group 111 in 20261116T010000Z-00000009-abc\n"
            f"van-compute-worker: launched process group 222 in {JOB}-xyz\n"
            f"van-compute-worker: launched process group 333 in {JOB}-again\n"
        )
        self.assertEqual(live.mac_launch_group(log, JOB), 333)
        self.assertIsNone(live.mac_launch_group(log, "20261116T010000Z-0000ffff"))
        journal = "\n".join([
            "not json",
            json.dumps({"action": "cleanup-hold", "job": JOB, "group": 5}),
            json.dumps({"action": "job-launched", "job": JOB, "group": 4242, "leader_start": "998"}),
        ])
        self.assertEqual(live.pi_launch(journal, JOB), (4242, "998"))
        self.assertIsNone(live.pi_launch(journal, "20261116T010000Z-0000ffff"))

    def test_lease_lost_and_fresh_heartbeat_detection(self):
        self.assertTrue(live.lease_lost_logged(
            f"[m4mac.03] job {JOB} attempt 1 is no longer leased here", JOB))
        self.assertFalse(live.lease_lost_logged("nothing", JOB))
        available = {"workers": [{"worker": "m4mac", "age_seconds": 3.0},
                                 {"worker": "m4mac.01", "age_seconds": 1.0}]}
        self.assertTrue(live.coordinator_fresh(available))
        self.assertFalse(live.coordinator_fresh({"workers": [{"worker": "m4mac", "age_seconds": 50}]}))

    def test_dry_run_touches_nothing(self):
        import contextlib
        import io
        from unittest import mock

        out = io.StringIO()
        with mock.patch.object(live, "run", side_effect=AssertionError("touched a host")), \
                contextlib.redirect_stdout(out):
            self.assertEqual(live.main(["--dry-run"]), 0)
        self.assertIn("plan", json.loads(out.getvalue()))


if __name__ == "__main__":
    unittest.main()
