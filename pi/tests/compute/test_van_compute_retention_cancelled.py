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

    def test_cancelled_jobs_follow_every_retention_rule(self):
        # Default age (14 days, by finished_at), then the owner's --keep-days,
        # the history budget and the citation guard.
        at_limit = self.add_job("cancelled", age_days=14)
        old = self.add_job("cancelled", age_days=14, age_seconds=1)
        done_young = self.add_job("done", age_days=15)
        payload = self.eligible()
        self.assertEqual(payload["retention_days"]["cancelled"], 14)
        self.assertEqual(payload["eligible_ids"], [old])
        self.assertNotIn(at_limit, payload["eligible_ids"])
        self.assertNotIn(done_young, payload["eligible_ids"])
        payload = self.eligible("--keep-days", "30")
        self.assertEqual(payload["retention_days"]["cancelled"], 30)
        self.assertEqual(payload["eligible_ids"], [])
        # A 1-byte budget is unsatisfiable where state directories take blocks
        # (ext4), so prune may exit 1; young jobs are still selected.
        code, payload, errors = self.prune("--max-history-bytes", "1")
        self.assertIn(code, (0, 1))
        self.assertEqual(errors, "")
        self.assertIn(at_limit, payload["eligible_ids"])
        self.cite("notes/run.md", f"see {old}\n")
        self.assertNotIn(old, self.eligible()["eligible_ids"])

    def test_apply_and_drop_source_purge_trash_under_cancelled(self):
        old = self.add_job("cancelled", age_days=40)
        young = self.add_job("cancelled", age_days=1)
        marker = self.add_marker("cancelled", young)
        code, payload, errors = self.prune("--apply")
        self.assertEqual((code, errors), (0, ""))
        self.assertEqual(payload["deleted_ids"], [old])
        self.assertFalse((self.root / "cancelled" / old).exists())
        self.assertFalse(marker.exists())
        self.assertTrue((self.root / "cancelled" / young).is_dir())
        marker = self.add_marker("cancelled", young)
        code, payload, errors = self.run_cli("drop-source", "--purge")
        self.assertEqual((code, errors), (0, ""))
        self.assertEqual(payload["purged"], [young])
        self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()
