"""Readers find a job that moves between state directories mid-read.

The interleavings are deterministic: a patched step performs a real rename at
a chosen point (between two directory checks, between locating and opening,
inside the list pass), the way a requeue or cancel would under the lock.
"""

from contextlib import contextmanager
import os
from pathlib import Path
import unittest
from unittest import mock

from van_compute import job_lookup, queue

from pi.tests.compute.van_compute_test_fixtures import BrokerHarness


class JobLookupTests(BrokerHarness, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.job = self.submit()
        self.job_id = self.job["id"]
        queue.safe_root(self.root)

    def move(self, source, destination):
        os.replace(self.root / source / self.job_id, self.root / destination / self.job_id)

    @contextmanager
    def lock_spy(self):
        taken = []
        original = queue.queue_lock

        @contextmanager
        def spy(root):
            taken.append(root)
            with original(root):
                yield

        with mock.patch.object(queue, "queue_lock", spy):
            yield taken

    def test_a_move_into_an_already_checked_directory_is_found_on_the_next_pass(self):
        self.move("queued", "running")
        original = job_lookup._is_job_dir
        moved = []

        def check(path):
            result = original(path)
            # After the scan has looked at queued/, move the job back there
            # from running/ (a requeue), so this pass misses it.
            if path == self.root / "queued" / self.job_id and not moved:
                self.move("running", "queued")
                moved.append(True)
            return result

        with self.lock_spy() as taken, mock.patch.object(job_lookup, "_is_job_dir", check):
            found = job_lookup.snapshot(self.root, self.job_id)
        self.assertEqual(found.state, "queued")
        self.assertEqual(found.manifest["id"], self.job_id)
        self.assertEqual(taken, [], "an optimistic rescan must suffice")

    def test_a_directory_moved_between_locate_and_open_is_reread_where_it_went(self):
        original = job_lookup._read
        calls = []

        def read(path):
            if not calls:
                self.move("queued", "running")
            calls.append(path)
            return original(path)

        with mock.patch.object(job_lookup, "_read", read):
            found = job_lookup.snapshot(self.root, self.job_id)
        self.assertEqual(found.state, "running")
        self.assertEqual(len(calls), 2)

    def test_the_lock_is_taken_only_before_declaring_a_job_unknown(self):
        missing = "20260101T000000Z-0000dead"
        with self.lock_spy() as taken:
            with self.assertRaisesRegex(queue.QueueError, "unknown job"):
                job_lookup.snapshot(self.root, missing)
        self.assertEqual(len(taken), 1)

    def test_a_job_that_keeps_moving_is_found_under_the_lock(self):
        original = job_lookup._locate_once
        passes = []

        def locate(root, job_id):
            passes.append(True)
            if len(passes) <= job_lookup.PASSES:
                return None  # every optimistic pass happens to miss it
            return original(root, job_id)

        with self.lock_spy() as taken, mock.patch.object(job_lookup, "_locate_once", locate):
            found = job_lookup.snapshot(self.root, self.job_id)
        self.assertEqual(found.state, "queued")
        self.assertEqual(len(taken), 1)

    def test_list_reports_a_job_moved_during_the_pass_exactly_once(self):
        other = self.submit()["id"]
        self.move("queued", "running")
        original_listdir = os.listdir
        moved = []

        def listdir(path):
            names = original_listdir(path)
            # Requeue the job right after queued/ was listed: the first pass
            # sees it nowhere, the second sees it in queued/.
            if Path(path) == self.root / "queued" and not moved:
                self.move("running", "queued")
                moved.append(True)
            return names

        with mock.patch.object(job_lookup.os, "listdir", listdir):
            ids = [item["id"] for item in job_lookup.list_manifests(self.root)]
        self.assertEqual(sorted(ids), sorted([self.job_id, other]))

    def test_list_reads_a_job_listed_in_two_directories_once_from_where_it_is(self):
        original_listdir = os.listdir
        moved = []

        def listdir(path):
            names = original_listdir(path)
            # A claim moves the job after queued/ was listed but before
            # running/ is: both listings name it.
            if Path(path) == self.root / "queued" and not moved:
                self.move("queued", "running")
                moved.append(True)
            return names

        with mock.patch.object(job_lookup.os, "listdir", listdir):
            manifests = job_lookup.list_manifests(self.root)
        self.assertEqual([item["id"] for item in manifests], [self.job_id])

    def test_list_skips_an_unreadable_manifest(self):
        (self.root / "queued" / self.job_id / "manifest.json").write_text("{", encoding="utf-8")
        self.assertEqual(job_lookup.list_manifests(self.root), [])


if __name__ == "__main__":
    unittest.main()
