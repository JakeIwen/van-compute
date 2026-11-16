"""Retention ages cancelled jobs like other finished history, and purges their trash."""

import unittest

from pi.tests.compute.test_van_compute_finished_source import RetentionPurgesSourceTrashTests
from pi.tests.compute.van_compute_test_fixtures import RetentionHarness


class CancelledRetentionTests(RetentionHarness, unittest.TestCase):
    # Borrow the helpers without inheriting (and re-running) those tests.
    add_marker = RetentionPurgesSourceTrashTests.add_marker
    run_cli = RetentionPurgesSourceTrashTests.run_cli
    prune = RetentionPurgesSourceTrashTests.prune

    def eligible(self, *arguments):
        code, payload, errors = self.prune(*arguments)
        self.assertEqual((code, errors), (0, ""))
        return payload

    def test_cancelled_jobs_age_out_after_14_days_by_default(self):
        at_limit = self.add_job("cancelled", age_days=14)
        old = self.add_job("cancelled", age_days=14, age_seconds=1)
        done_young = self.add_job("done", age_days=15)
        payload = self.eligible()
        self.assertEqual(payload["retention_days"]["cancelled"], 14)
        self.assertEqual(payload["eligible_ids"], [old])
        self.assertNotIn(at_limit, payload["eligible_ids"])
        self.assertNotIn(done_young, payload["eligible_ids"])

    def test_the_owner_keep_days_applies_to_cancelled_jobs_too(self):
        kept = self.add_job("cancelled", age_days=29)
        old = self.add_job("cancelled", age_days=30, age_seconds=1)
        payload = self.eligible("--keep-days", "30")
        self.assertEqual(payload["retention_days"]["cancelled"], 30)
        self.assertEqual(payload["eligible_ids"], [old])
        self.assertNotIn(kept, payload["eligible_ids"])

    def test_the_history_budget_can_remove_young_cancelled_jobs(self):
        young = self.add_job("cancelled", age_days=1)
        baseline = self.eligible()
        self.assertEqual(baseline["eligible_ids"], [])
        payload = self.eligible("--max-history-bytes", "1")
        self.assertIn(young, payload["eligible_ids"])

    def test_citations_protect_a_cancelled_job(self):
        cited = self.add_job("cancelled", age_days=90)
        self.cite("notes/run.md", f"see {cited}\n")
        self.assertNotIn(cited, self.eligible()["eligible_ids"])

    def test_apply_deletes_old_cancelled_jobs_and_purges_young_ones_trash(self):
        old = self.add_job("cancelled", age_days=40)
        young = self.add_job("cancelled", age_days=1)
        marker = self.add_marker("cancelled", young)
        code, payload, errors = self.prune("--apply")
        self.assertEqual((code, errors), (0, ""))
        self.assertEqual(payload["deleted_ids"], [old])
        self.assertFalse((self.root / "cancelled" / old).exists())
        self.assertFalse(marker.exists())
        self.assertTrue((self.root / "cancelled" / young).is_dir())

    def test_drop_source_purge_clears_cancelled_trash(self):
        job = self.add_job("cancelled", age_days=1)
        marker = self.add_marker("cancelled", job)
        code, payload, errors = self.run_cli("drop-source", "--purge")
        self.assertEqual((code, errors), (0, ""))
        self.assertEqual(payload["purged"], [job])
        self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()
