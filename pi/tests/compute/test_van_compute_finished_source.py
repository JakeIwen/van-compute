import argparse
from contextlib import redirect_stderr, redirect_stdout
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
import tempfile
import fcntl
import threading
import unittest
from unittest import mock

from van_compute import finished_source

from van_compute import queue, retention, source_sweep
from pi.tests.compute.test_van_compute import SUMMARY_STUB
from pi.tests.compute.van_compute_test_fixtures import RetentionHarness


class FinishedSourceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        base = Path(self.temporary.name)
        self.source_root = base / "obd-things"
        self.queue_root = self.source_root / "tmp" / "compute"
        tools = self.source_root / "tools"
        tools.mkdir(parents=True)
        (tools / "can_capture_summary.py").write_text(SUMMARY_STUB, encoding="utf-8")
        self.capture = self.source_root / "tmp" / "captures" / "capture.log"
        self.capture.parent.mkdir(parents=True)
        self.capture.write_bytes(b"capture\n")

    def tearDown(self):
        self.temporary.cleanup()

    def finish(self, exit_code):
        queue.submit_job(
            argparse.Namespace(
                root=self.queue_root,
                source_root=self.source_root,
                task="can-capture-summary",
                argument=["--snapshot"],
                input=[str(self.capture)],
                input_value=None,
            )
        )
        root = queue.safe_root(self.queue_root)
        claimed = queue.worker_claim(root, "m4mac")
        token = claimed["lease_token"]
        queue.put_result(root, claimed["id"], "m4mac", "stdout.txt", BytesIO(b"ok\n"), token)
        manifest = queue.worker_finish(
            root, claimed["id"], "m4mac", exit_code, ["stdout.txt"], token
        )
        return root, claimed["id"], manifest

    def cli(self, *arguments):
        out, err = StringIO(), StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = queue.main(["--root", str(self.queue_root), *arguments])
        return code, (json.loads(out.getvalue()) if out.getvalue() else None)

    def test_finish_drops_source_but_keeps_manifest_sources_and_results(self):
        for exit_code, state in ((0, "done"), (1, "failed")):
            with self.subTest(state=state):
                _root, job_id, manifest = self.finish(exit_code)
                job = self.queue_root / state / job_id
                self.assertEqual(manifest["state"], state)
                self.assertFalse((job / "source").exists())
                self.assertEqual((job / "result" / "stdout.txt").read_bytes(), b"ok\n")
                recorded = json.loads((job / "manifest.json").read_text())
                self.assertTrue(recorded["sources"])

    def test_failed_drop_never_fails_the_finish(self):
        
        with mock.patch("van_compute.finished_source.detach_source", side_effect=OSError("boom")):
            _root, job_id, manifest = self.finish(0)
        self.assertEqual(manifest["state"], "done")
        self.assertTrue((self.queue_root / "done" / job_id / "source").is_dir())

    def test_drop_source_sweep_is_dry_run_by_default_and_never_follows_links(self):
        
        with mock.patch("van_compute.finished_source.detach_source", side_effect=OSError("boom")):
            _root, job_id, _manifest = self.finish(0)
            _root, failed_id, _manifest = self.finish(1)
        outside = Path(self.temporary.name) / "outside"
        outside.mkdir()
        (outside / "keep.txt").write_text("keep")
        os.symlink(outside, self.queue_root / "done" / job_id / "source" / "link")

        code, report = self.cli("drop-source")
        self.assertEqual((code, report["apply"], report["jobs_with_source"]), (0, False, 2))
        self.assertTrue((self.queue_root / "done" / job_id / "source").exists())

        code, report = self.cli("drop-source", "--purge")
        self.assertEqual(code, 0)
        self.assertCountEqual(report["purged"], [job_id, failed_id])
        self.assertFalse((self.queue_root / "done" / job_id / "source").exists())
        self.assertFalse((self.queue_root / "failed" / failed_id / "source").exists())
        self.assertEqual((outside / "keep.txt").read_text(), "keep")
        self.assertTrue((self.queue_root / "done" / job_id / "manifest.json").is_file())
        code, report = self.cli("drop-source", "--purge")
        self.assertEqual((code, report["jobs_with_source"]), (0, 0))

    def lock_is_free(self):
        with (self.queue_root / ".queue.lock").open("a+") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return False
            fcntl.flock(handle, fcntl.LOCK_UN)
            return True

    def keep_source(self, exit_code=0):
        with mock.patch("van_compute.finished_source.detach_source", side_effect=OSError("boom")):
            return self.finish(exit_code)

    def linux_mounts(self, mounts):
        """Pretend to run on Linux; `mounts` is called for each mountinfo read."""
        return (
            mock.patch.object(finished_source, "_linux", return_value=True),
            mock.patch.object(finished_source, "linux_mount_points", side_effect=mounts),
        )

    def test_bind_mount_inside_source_is_never_emptied(self):
        _root, job_id, _manifest = self.keep_source()
        job = self.queue_root / "done" / job_id
        inner = job / "source" / "tools"
        marker = inner / "can_capture_summary.py"
        self.assertTrue(marker.is_file())
        device = self.queue_root.stat().st_dev
        linux, mounts = self.linux_mounts(lambda: frozenset({inner}))
        with linux, mounts:
            with self.assertRaises(finished_source.UnsafeTree):
                finished_source.detach_source(self.queue_root / "done", job_id, device)
        self.assertTrue(marker.is_file())
        self.assertTrue((job / "source").is_dir())

    def test_replaced_job_directory_is_not_touched(self):
        _root, job_id, _manifest = self.keep_source()
        state_root = self.queue_root / "done"
        job = state_root / job_id
        real_open = os.open
        swapped = []

        def swap_before_open(path, flags, *args, **kwargs):
            if path == job_id and not swapped:
                swapped.append(True)
                job.rename(state_root / "moved-away")
                (job / "source").mkdir(parents=True)
                (job / "source" / "replacement.txt").write_text("keep")
            return real_open(path, flags, *args, **kwargs)

        with mock.patch("van_compute.finished_source.os.open", side_effect=swap_before_open):
            with self.assertRaises(finished_source.UnsafeTree):
                finished_source.detach_source(
                    state_root, job_id, self.queue_root.stat().st_dev
                )
        self.assertEqual((job / "source" / "replacement.txt").read_text(), "keep")

    def test_sweep_skips_symlinked_job_directories(self):
        _root, job_id, _manifest = self.keep_source()
        job = self.queue_root / "done" / job_id
        outside = Path(self.temporary.name) / "elsewhere"
        job.rename(outside)
        os.symlink(outside, job)

        code, report = self.cli("drop-source")
        self.assertEqual((code, report["jobs_with_source"]), (1, 0))
        self.assertEqual(report["skipped"][0]["job_id"], job_id)
        code, report = self.cli("drop-source", "--purge")
        self.assertEqual(code, 1)
        self.assertTrue((outside / "source").is_dir())

    def test_sweep_reports_unreadable_source_as_skipped(self):
        _root, job_id, _manifest = self.keep_source()
        locked = self.queue_root / "done" / job_id / "source" / "tools"
        locked.chmod(0)
        try:
            code, report = self.cli("drop-source")
        finally:
            locked.chmod(0o755)
        if os.geteuid() != 0:
            self.assertEqual(code, 1)
            self.assertEqual(report["jobs_with_source"], 0)
            self.assertIn("cannot inspect", report["skipped"][0]["reason"])

    def watch_lock(self, name):
        """Wrap a finished_source step to record whether the queue lock was held."""
        real = getattr(finished_source, name)
        held = []

        def wrapper(*args, **kwargs):
            held.append(not self.lock_is_free())
            return real(*args, **kwargs)

        return mock.patch.object(finished_source, name, side_effect=wrapper), held

    def test_dry_run_takes_no_lock_and_apply_renames_and_purges_only_under_it(self):
        _root, job_id, _manifest = self.keep_source()
        _root, other_id, _manifest = self.keep_source()
        with mock.patch("van_compute.queue.queue_lock", side_effect=AssertionError("locked")):
            code, report = self.cli("drop-source")
        self.assertEqual((code, report["jobs_with_source"]), (0, 2))

        detach, detached = self.watch_lock("detach_source")
        purge, purged = self.watch_lock("purge_trash")
        with detach, purge:
            code, report = self.cli("drop-source", "--apply")
        self.assertEqual(code, 0)
        self.assertCountEqual(report["detached"], [job_id, other_id])
        self.assertEqual((report["purged"], report["freed_bytes"]), ([], 0))
        self.assertEqual((detached, purged), ([True, True], []))
        job = self.queue_root / "done" / job_id
        self.assertEqual(len([n for n in os.listdir(job) if n.startswith(".source-trash-")]), 1)

        detach, detached = self.watch_lock("detach_source")
        purge, purged = self.watch_lock("purge_trash")
        with detach, purge:
            code, report = self.cli("drop-source", "--purge")
        self.assertEqual(code, 0)
        self.assertTrue(report["apply"])
        self.assertCountEqual(report["purged"], [job_id, other_id])
        self.assertGreater(report["freed_bytes"], 0)
        self.assertEqual(purged, [True, True])
        self.assertTrue(all(detached))
        self.assertEqual(sorted(os.listdir(job)), ["manifest.json", "result"])

    def test_finish_only_renames_under_the_lock_and_never_deletes(self):
        detach, detached = self.watch_lock("detach_source")
        with detach, mock.patch.object(
            finished_source, "remove_open_directory", side_effect=AssertionError("deleted")
        ):
            _root, job_id, _manifest = self.finish(0)
        self.assertEqual(detached, [True])
        job = self.queue_root / "done" / job_id
        markers = [name for name in os.listdir(job) if name.startswith(".source-trash-")]
        self.assertEqual(len(markers), 1)
        self.assertFalse((job / "source").exists())

        code, report = self.cli("drop-source")
        self.assertEqual(report["jobs_with_source"], 1)
        code, report = self.cli("drop-source", "--purge")
        self.assertEqual((code, report["purged"], report["detached"]), (0, [job_id], []))
        self.assertEqual(sorted(os.listdir(job)), ["manifest.json", "result"])

    def test_mount_added_after_the_inventory_blocks_the_rename(self):
        _root, job_id, _manifest = self.keep_source()
        root = queue.safe_root(self.queue_root)
        inner = root / "done" / job_id / "source" / "tools"
        kept = inner / "can_capture_summary.py"
        reads = []

        def mountinfo():
            # The preview inventory sees no mount; a bind mount appears before apply.
            reads.append(True)
            return frozenset() if len(reads) == 1 else frozenset({inner})

        linux, mounts = self.linux_mounts(mountinfo)
        with linux, mounts:
            code, report = self.cli("drop-source", "--purge")
        self.assertEqual(code, 1)
        self.assertEqual((report["detached"], report["purged"]), ([], []))
        self.assertIn("mount point", report["skipped"][0]["reason"])
        self.assertTrue(kept.is_file())
        self.assertTrue((root / "done" / job_id / "source").is_dir())

    def test_mount_added_inside_a_marker_after_the_rename_blocks_the_purge(self):
        _root, job_id, _manifest = self.keep_source()
        root = queue.safe_root(self.queue_root)
        job = root / "done" / job_id

        def mountinfo():
            # Inventory and rename see no mount; one appears inside the marker
            # before the purge lists and deletes it.
            markers = [name for name in os.listdir(job) if name.startswith(".source-trash-")]
            return frozenset({job / markers[0] / "tools"}) if markers else frozenset()

        linux, mounts = self.linux_mounts(mountinfo)
        with linux, mounts:
            code, report = self.cli("drop-source", "--purge")
        self.assertEqual(code, 1)
        self.assertEqual((report["detached"], report["purged"]), ([job_id], []))
        self.assertIn("mount point", report["skipped"][0]["reason"])
        markers = [name for name in os.listdir(job) if name.startswith(".source-trash-")]
        self.assertTrue((job / markers[0] / "tools" / "can_capture_summary.py").is_file())


class RetentionPurgesSourceTrashTests(RetentionHarness, unittest.TestCase):
    def add_marker(self, state, job_id, files=3):
        marker = self.root / state / job_id / f".source-trash-{os.urandom(8).hex()}"
        (marker / "tools").mkdir(parents=True)
        for index in range(files):
            (marker / "tools" / f"file{index}.py").write_bytes(b"x" * 8192)
        return marker

    def run_cli(self, *arguments):
        output, errors = StringIO(), StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            code = queue.main(["--root", str(self.root), *arguments])
        payload = json.loads(output.getvalue()) if output.getvalue() else None
        return code, payload, errors.getvalue()

    def prune(self, *arguments):
        return self.run_cli(
            "prune", "--citations-root", str(self.source), "--now", self.NOW.isoformat(),
            *arguments,
        )

    def test_retention_purges_markers_under_its_lock_before_measuring(self):
        young = self.add_job("done", age_days=1)
        marker = self.add_marker("done", young)
        without_marker = self.add_job("failed", age_days=1)

        code, preview, errors = self.prune()
        self.assertEqual((code, errors), (0, ""))
        self.assertEqual(preview["source_trash"]["jobs"], [young])
        self.assertGreater(preview["source_trash"]["bytes"], 0)
        self.assertTrue(marker.is_dir())

        real_purge = finished_source.purge_trash
        held = []

        def purge(*args, **kwargs):
            with (self.root / ".queue.lock").open("a+") as handle:
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    held.append(True)
                else:
                    fcntl.flock(handle, fcntl.LOCK_UN)
                    held.append(False)
            return real_purge(*args, **kwargs)

        with mock.patch.object(finished_source, "purge_trash", side_effect=purge):
            code, payload, errors = self.prune("--apply")
        self.assertEqual((code, errors), (0, ""))
        self.assertEqual(held, [True])
        self.assertEqual(payload["source_trash"], {"jobs": [young], "markers": 1, "skipped": []})
        self.assertEqual(payload["deleted_ids"], [])
        self.assertFalse(marker.exists())
        for job_id, state in ((young, "done"), (without_marker, "failed")):
            self.assertTrue((self.root / state / job_id / "manifest.json").is_file())
        budget = payload["history_budget"]
        self.assertEqual(budget["before_bytes"], budget["after_bytes"])

    def test_retention_refreshes_mountinfo_for_each_marker(self):
        job_id = self.add_job("done", age_days=1)
        marker = self.add_marker("done", job_id)
        inner = marker / "tools"
        reads = []

        def mountinfo():
            # Retention's listing sees no mount; one appears before the purge.
            reads.append(True)
            return frozenset() if len(reads) == 1 else frozenset({inner})

        with mock.patch.object(finished_source, "_linux", return_value=True), mock.patch.object(
            finished_source, "linux_mount_points", side_effect=mountinfo
        ):
            code, payload, errors = self.prune("--apply")
        self.assertEqual(code, 0, errors)
        self.assertEqual(payload["source_trash"]["jobs"], [])
        self.assertIn("mount point", payload["source_trash"]["skipped"][0]["reason"])
        self.assertTrue((inner / "file0.py").is_file())

    def test_retention_waits_while_drop_source_purges_a_marker(self):
        # Before every purge held the lock, retention could measure or delete a
        # job while an unlocked purge was emptying its marker, raising
        # FileNotFoundError or leaving a job directory without its manifest.
        job_id = self.add_job("done", age_days=90)
        marker = self.add_marker("done", job_id, files=4)
        purging, release = threading.Event(), threading.Event()
        real_remove = finished_source.remove_open_directory
        outcome = {}

        def paused_remove(descriptor, device):
            if not purging.is_set():
                purging.set()
                if not release.wait(10):
                    raise RuntimeError("purge was not released")
            return real_remove(descriptor, device)

        # The CLI redirects process-wide stdout, so the threads call the
        # commands' functions directly.
        def sweep():
            outcome["sweep"] = source_sweep.drop_sources(self.root, purge=True)

        def retain():
            outcome["prune"] = retention.prune_jobs(
                self.root, self.source, apply=True, now_text=self.NOW.isoformat()
            )

        with mock.patch.object(finished_source, "remove_open_directory", side_effect=paused_remove):
            sweeper = threading.Thread(target=sweep)
            sweeper.start()
            self.assertTrue(purging.wait(10))
            retainer = threading.Thread(target=retain)
            retainer.start()
            retainer.join(0.5)
            try:
                self.assertTrue(retainer.is_alive(), "retention ran during an unlocked purge")
                self.assertTrue((self.root / "done" / job_id / "manifest.json").is_file())
            finally:
                release.set()
                sweeper.join(10)
                retainer.join(10)

        self.assertEqual((outcome["sweep"]["purged"], outcome["sweep"]["skipped"]), ([job_id], []))
        payload = outcome["prune"]
        self.assertEqual(payload["source_trash"], {"jobs": [], "markers": 0, "skipped": []})
        self.assertEqual(payload["deleted_ids"], [job_id])
        self.assertFalse((self.root / "done" / job_id).exists())
        self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()
