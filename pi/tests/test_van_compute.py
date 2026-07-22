import argparse
from io import BytesIO
import json
from pathlib import Path
import tempfile
import unittest

from macbook.scripts import van_compute_worker as worker
from pi.scripts import van_compute as queue
from shared.python import van_compute_protocol as protocol
from shared.python import van_compute_metrics as metrics


SUMMARY_STUB = """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('capture')
parser.add_argument('--snapshot', action='store_true')
parser.add_argument('--json', required=True)
args = parser.parse_args()
data = Path(args.capture).read_bytes()
Path(args.json).write_text(json.dumps({'bytes': len(data), 'snapshot': args.snapshot}))
print(f'summarized {len(data)} bytes')
"""


class ProtocolTests(unittest.TestCase):
    def test_signal_correlate_cannot_select_capture_mode(self):
        with self.assertRaisesRegex(protocol.ProtocolError, "not allowed"):
            protocol.validate_task_arguments("signal-correlate-analyze", ["capture"])

    def test_field_finder_requires_all_or_no_input_values(self):
        with self.assertRaisesRegex(protocol.ProtocolError, "every input"):
            protocol.validate_inputs(
                "can-field-finder", [{"value": "12.5"}, {"value": None}]
            )

    def test_summary_output_path_is_worker_controlled(self):
        command = protocol.build_command(
            "can-capture-summary",
            python="/python",
            source_root=Path("/source"),
            input_paths=[Path("/input/capture.log")],
            input_values=[None],
            result_root=Path("/result"),
            arguments=["--snapshot"],
        )
        self.assertEqual(command[-2:], ["--json", "/result/summary.json"])
        self.assertNotIn("capture", command[:3])


class QueueLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        base = Path(self.temporary.name)
        self.source_root = base / "obd-things"
        self.queue_root = self.source_root / "tmp" / "compute"
        tools = self.source_root / "tools"
        tools.mkdir(parents=True)
        (tools / "can_capture_summary.py").write_text(SUMMARY_STUB, encoding="utf-8")
        self.capture = self.source_root / "tmp" / "captures" / "drive.log"
        self.capture.parent.mkdir(parents=True)
        self.capture.write_bytes(b"first capture bytes\n")

    def tearDown(self):
        self.temporary.cleanup()

    def submit(self):
        args = argparse.Namespace(
            root=self.queue_root,
            source_root=self.source_root,
            task="can-capture-summary",
            argument=["--snapshot"],
            input=[str(self.capture)],
            input_value=None,
        )
        return queue.submit_job(args)

    def test_submit_claim_stream_and_finish(self):
        submitted = self.submit()
        job_id = submitted["id"]
        self.assertEqual(submitted["state"], "queued")
        source_copy = self.queue_root / "queued" / job_id / "source" / "tools" / "can_capture_summary.py"
        self.assertEqual(source_copy.read_text(encoding="utf-8"), SUMMARY_STUB)

        claimed = queue.worker_claim(queue.safe_root(self.queue_root), "m4mac")
        self.assertEqual(claimed["id"], job_id)
        self.assertEqual(claimed["state"], "running")

        # Appending after submission is safe: the stream remains bounded to the
        # original byte length recorded in the manifest.
        with self.capture.open("ab") as handle:
            handle.write(b"later bytes")
        streamed = BytesIO()
        queue.stream_bounded_input(claimed, 0, streamed)
        self.assertEqual(streamed.getvalue(), b"first capture bytes\n")

        uploaded = queue.put_result(
            queue.safe_root(self.queue_root), job_id, "m4mac", "stdout.txt", BytesIO(b"ok\n")
        )
        self.assertEqual(uploaded["size"], 3)
        finished = queue.worker_finish(
            queue.safe_root(self.queue_root), job_id, "m4mac", 0, ["stdout.txt"]
        )
        self.assertEqual(finished["state"], "done")
        self.assertFalse((self.queue_root / "running" / job_id).exists())
        self.assertEqual(
            (self.queue_root / "done" / job_id / "result" / "stdout.txt").read_bytes(),
            b"ok\n",
        )
        downloaded = BytesIO()
        queue.stream_result(queue.safe_root(self.queue_root), job_id, "stdout.txt", downloaded)
        self.assertEqual(downloaded.getvalue(), b"ok\n")

    def test_claim_resumes_worker_job_after_interruption(self):
        submitted = self.submit()
        root = queue.safe_root(self.queue_root)
        first = queue.worker_claim(root, "m4mac")
        second = queue.worker_claim(root, "m4mac")
        self.assertEqual(first["id"], submitted["id"])
        self.assertEqual(second["id"], submitted["id"])
        self.assertIn("resumed_at", second)

    def test_submission_rejects_input_outside_source_root(self):
        outside = Path(self.temporary.name) / "outside.log"
        outside.write_text("private", encoding="utf-8")
        args = argparse.Namespace(
            root=self.queue_root,
            source_root=self.source_root,
            task="can-capture-summary",
            argument=[],
            input=[str(outside)],
            input_value=None,
        )
        with self.assertRaisesRegex(queue.QueueError, "inside"):
            queue.submit_job(args)


class WorkerExecutionTests(unittest.TestCase):
    def test_executes_allowlisted_task_and_captures_results(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            entrypoint = source / "tools" / "can_capture_summary.py"
            entrypoint.parent.mkdir(parents=True)
            entrypoint.write_text(SUMMARY_STUB, encoding="utf-8")
            capture = root / "inputs" / "capture.log"
            capture.parent.mkdir()
            capture.write_bytes(b"123456")
            result = root / "result"
            manifest = {
                "id": "20260721T120000Z-1234abcd",
                "task": "can-capture-summary",
                "arguments": ["--snapshot"],
                "inputs": [{"value": None}],
            }
            exit_code, execution = worker.execute_job(
                manifest,
                source_root=source,
                input_paths=[capture],
                input_values=[None],
                result_root=result,
                python=worker.sys.executable,
                timeout=30,
                nice=0,
                maximum_file_size=1024 * 1024,
            )
            self.assertEqual(exit_code, 0)
            self.assertFalse(execution["timed_out"])
            self.assertGreaterEqual(execution["resource_usage"]["cpu_seconds"], 0)
            self.assertGreater(execution["resource_usage"]["peak_rss_bytes"], 0)
            self.assertEqual(execution["input_bytes"], 0)
            self.assertEqual(
                json.loads((result / "summary.json").read_text(encoding="utf-8")),
                {"bytes": 6, "snapshot": True},
            )
            self.assertIn("summarized 6 bytes", (result / "stdout.txt").read_text())


class ComputeMetricsTests(unittest.TestCase):
    NOW = 1_784_681_600.0

    @staticmethod
    def write_json(path, payload):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def test_aggregates_worker_resources_and_current_queue(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "compute"
            self.write_json(
                root / "workers" / "m4mac.json",
                {"worker": "m4mac", "seen_at": "2026-07-22T01:59:50+00:00"},
            )
            done = root / "done" / "20260722T015900Z-1234abcd"
            self.write_json(
                done / "manifest.json",
                {
                    "id": done.name,
                    "task": "can-capture-summary",
                    "state": "done",
                    "worker": "m4mac",
                    "exit_code": 0,
                    "submitted_at": "2026-07-22T01:58:50+00:00",
                    "started_at": "2026-07-22T01:59:00+00:00",
                    "finished_at": "2026-07-22T01:59:05+00:00",
                    "inputs": [{"size": 10_000}],
                    "sources": [{"size": 1_000}],
                    "results": [{"size": 2_000}],
                },
            )
            self.write_json(
                done / "result" / "execution.json",
                {
                    "duration_seconds": 4.0,
                    "resource_usage": {
                        "cpu_seconds": 6.0,
                        "average_cpu_percent": 150.0,
                        "peak_rss_bytes": 256 * 1024 * 1024,
                    },
                },
            )
            queued = root / "queued" / "20260722T015950Z-8765abcd"
            self.write_json(
                queued / "manifest.json",
                {
                    "id": queued.name,
                    "task": "can-field-finder",
                    "state": "queued",
                    "submitted_at": "2026-07-22T01:59:50+00:00",
                    "inputs": [{"size": 20_000}],
                },
            )

            report = metrics.ComputeMetricsReader(root, clock=lambda: self.NOW).report(168)

        self.assertTrue(report["status"]["available"])
        self.assertEqual(report["status"]["queued"], 1)
        self.assertEqual(report["summary"]["jobs"], 1)
        self.assertEqual(report["summary"]["telemetry_jobs"], 1)
        self.assertEqual(report["summary"]["mac_cpu_seconds"], 6.0)
        self.assertEqual(report["summary"]["mac_wall_seconds"], 4.0)
        self.assertEqual(report["summary"]["aggregate_cpu_percent"], 150.0)
        self.assertEqual(report["summary"]["peak_rss_bytes"], 256 * 1024 * 1024)
        self.assertEqual(report["summary"]["input_bytes"], 10_000)
        self.assertEqual(report["tasks"][0]["task"], "can-capture-summary")
        self.assertEqual(report["tasks"][0]["telemetry_jobs"], 1)
        self.assertEqual([job["state"] for job in report["jobs"]], ["queued", "done"])

    def test_rejects_unbounded_time_range(self):
        with tempfile.TemporaryDirectory() as directory:
            reader = metrics.ComputeMetricsReader(directory)
            with self.assertRaisesRegex(ValueError, "6, 24, 168, or 720"):
                reader.report(999)


if __name__ == "__main__":
    unittest.main()
