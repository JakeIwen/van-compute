import argparse
from contextlib import redirect_stderr, redirect_stdout
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
import tempfile
import unittest

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
        from unittest import mock

        with mock.patch("van_compute.finished_source.drop_finished_source", side_effect=OSError("boom")):
            _root, job_id, manifest = self.finish(0)
        self.assertEqual(manifest["state"], "done")
        self.assertTrue((self.queue_root / "done" / job_id / "source").is_dir())

    def test_drop_source_sweep_is_dry_run_by_default_and_never_follows_links(self):
        from unittest import mock

        with mock.patch("van_compute.finished_source.drop_finished_source", side_effect=OSError("boom")):
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


if __name__ == "__main__":
    unittest.main()
