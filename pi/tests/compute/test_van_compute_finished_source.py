import argparse
from contextlib import redirect_stderr, redirect_stdout
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
import tempfile
import fcntl
import unittest
from unittest import mock

from van_compute import finished_source

from van_compute import queue
from pi.tests.compute.test_van_compute import SUMMARY_STUB


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

        code, report = self.cli("drop-source", "--apply")
        self.assertEqual(code, 0)
        self.assertCountEqual(report["dropped"], [job_id, failed_id])
        self.assertFalse((self.queue_root / "done" / job_id / "source").exists())
        self.assertFalse((self.queue_root / "failed" / failed_id / "source").exists())
        self.assertEqual((outside / "keep.txt").read_text(), "keep")
        self.assertTrue((self.queue_root / "done" / job_id / "manifest.json").is_file())
        code, report = self.cli("drop-source", "--apply")
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

    def test_bind_mount_inside_source_is_never_emptied(self):
        _root, job_id, _manifest = self.keep_source()
        job = self.queue_root / "done" / job_id
        inner = job / "source" / "tools"
        marker = inner / "can_capture_summary.py"
        self.assertTrue(marker.is_file())
        device = self.queue_root.stat().st_dev
        with mock.patch("van_compute.finished_source.sys.platform", "linux"):
            with self.assertRaises(finished_source.UnsafeTree):
                finished_source.detach_source(
                    self.queue_root / "done", job_id, device, frozenset({inner})
                )
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
                    state_root, job_id, self.queue_root.stat().st_dev, frozenset()
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
        code, report = self.cli("drop-source", "--apply")
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

    def test_dry_run_takes_no_lock_and_apply_deletes_outside_it(self):
        _root, job_id, _manifest = self.keep_source()
        with mock.patch("van_compute.queue.queue_lock", side_effect=AssertionError("locked")):
            code, report = self.cli("drop-source")
        self.assertEqual((code, report["jobs_with_source"]), (0, 1))

        real_purge = finished_source.purge_trash
        observed = []

        def purge_checking_lock(*args, **kwargs):
            observed.append(self.lock_is_free())
            return real_purge(*args, **kwargs)

        with mock.patch("van_compute.finished_source.purge_trash", side_effect=purge_checking_lock):
            code, report = self.cli("drop-source", "--apply")
        self.assertEqual((code, report["dropped"]), (0, [job_id]))
        self.assertEqual(observed, [True])

    def test_finish_deletes_outside_the_lock_and_leftover_trash_is_swept(self):
        real_purge = finished_source.purge_trash
        observed = []

        def purge_checking_lock(*args, **kwargs):
            observed.append(self.lock_is_free())
            return real_purge(*args, **kwargs)

        with mock.patch("van_compute.finished_source.purge_trash", side_effect=purge_checking_lock):
            _root, job_id, _manifest = self.finish(0)
        self.assertEqual(observed, [True])

        # An exhausted budget leaves the marker in the job directory for the sweep.
        with mock.patch("van_compute.finished_source.FINISH_PURGE_SECONDS", -1.0):
            _root, left_id, _manifest = self.finish(0)
        job = self.queue_root / "done" / left_id
        markers = [name for name in os.listdir(job) if name.startswith(".source-trash-")]
        self.assertEqual(len(markers), 1)
        self.assertFalse((job / "source").exists())

        code, report = self.cli("drop-source")
        self.assertEqual(report["jobs_with_source"], 1)
        code, report = self.cli("drop-source", "--apply")
        self.assertEqual((code, report["dropped"]), (0, [left_id]))
        self.assertEqual(sorted(os.listdir(job)), ["manifest.json", "result"])


if __name__ == "__main__":
    unittest.main()
