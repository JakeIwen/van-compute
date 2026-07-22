import argparse
from collections import deque
from io import BytesIO
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import threading
import unittest
from unittest import mock

from macbook.scripts import van_compute_worker as worker


class FakeQueueState:
    def __init__(self, count):
        self.jobs = deque(
            {"id": f"20260722T120000Z-{index:08x}"} for index in range(count)
        )
        self.lock = threading.Lock()
        self.heartbeats = []
        self.claims = 0


class FakeRemote:
    def __init__(self, worker_id, state):
        self.worker = worker_id
        self.state = state

    def claim(self):
        with self.state.lock:
            self.state.claims += 1
            if not self.state.jobs:
                return None
            return self.state.jobs.popleft()

    def heartbeat(self, *, slots_total=None, slots_busy=None):
        with self.state.lock:
            self.state.heartbeats.append(
                (self.worker, slots_total, slots_busy)
            )
        return {"worker": self.worker}


class RemoteQueueProtocolTests(unittest.TestCase):
    def test_heartbeat_and_claim_send_current_protocol_version(self):
        remote = worker.RemoteQueue("pi@vanpi", "/home/pi/scripts/van_compute.py", "m4mac.00")
        version = str(worker.protocol.WORKER_PROTOCOL_VERSION)
        with mock.patch.object(
            remote,
            "json_command",
            side_effect=({"worker": remote.worker}, {"job": None}),
        ) as command:
            remote.heartbeat()
            self.assertIsNone(remote.claim())

        self.assertEqual(
            command.call_args_list,
            [
                mock.call(
                    "worker",
                    "heartbeat",
                    "--worker",
                    "m4mac.00",
                    "--protocol-version",
                    version,
                ),
                mock.call(
                    "worker",
                    "claim",
                    "--worker",
                    "m4mac.00",
                    "--protocol-version",
                    version,
                ),
            ],
        )


def scheduler_args():
    return argparse.Namespace(
        worker="m4mac",
        poll_interval=0.01,
        heartbeat_interval=0.01,
    )


class PersistentSchedulerTests(unittest.TestCase):
    def test_resource_watchdog_shares_one_process_table_sample(self):
        worker._PROCESS_TABLE_GROUPS = {}
        worker._PROCESS_TABLE_SAMPLED_AT = 0.0
        completed = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="101 100\n101 200\n202 50\n",
            stderr="",
        )
        with mock.patch.object(
            worker.subprocess, "run", return_value=completed
        ) as run:
            first = worker.process_group_resources(101)
            second = worker.process_group_resources(202)

        self.assertEqual(first, (300 * 1024, 2))
        self.assertEqual(second, (50 * 1024, 1))
        run.assert_called_once()

    def test_slots_are_distributed_across_four_control_connections(self):
        state = FakeQueueState(0)
        pool = [object() for _ in range(worker.SSH_CONTROL_CONNECTIONS)]
        assignments = {}

        def make_remote(_args, worker_id, multiplexer):
            assignments[worker_id] = multiplexer
            return FakeRemote(worker_id, state)

        with mock.patch.object(worker, "make_remote", side_effect=make_remote):
            worker.PersistentScheduler(
                scheduler_args(),
                stop_event=threading.Event(),
                multiplexer=None,
                multiplexers=pool,
            )

        counts = [
            sum(
                assignments[f"m4mac.{index:02d}"] is connection
                for index in range(worker.SCHEDULER_SLOTS)
            )
            for connection in pool
        ]
        self.assertEqual(counts, [3, 3, 2, 2])
        self.assertIs(assignments["m4mac"], pool[-1])

    def test_drain_does_not_execute_job_returned_by_inflight_claim(self):
        state = FakeQueueState(1)
        stop = threading.Event()
        drain = threading.Event()
        executed = []

        class DrainingRemote(FakeRemote):
            def claim(self):
                manifest = super().claim()
                drain.set()
                return manifest

        worker.run_scheduler(
            scheduler_args(),
            stop_event=stop,
            drain_event=drain,
            remote_factory=lambda name: DrainingRemote(name, state),
            job_runner=lambda *_args: executed.append(True),
        )

        self.assertEqual(executed, [])
        self.assertTrue(stop.is_set())
        self.assertEqual(state.claims, 1)

    def test_idle_scheduler_uses_one_central_claim_poll(self):
        state = FakeQueueState(0)
        stop = threading.Event()
        args = scheduler_args()
        args.poll_interval = 0.05
        service = threading.Thread(
            target=worker.run_scheduler,
            args=(args,),
            kwargs={
                "stop_event": stop,
                "remote_factory": lambda name: FakeRemote(name, state),
            },
        )
        service.start()
        for _ in range(200):
            with state.lock:
                if state.claims >= 10:
                    break
            threading.Event().wait(0.002)
        threading.Event().wait(0.06)
        stop.set()
        service.join(5)
        self.assertFalse(service.is_alive())
        with state.lock:
            # Ten one-time slot probes preserve exact stale-job ownership;
            # after that, only one central empty poll runs per interval.
            self.assertGreaterEqual(state.claims, 10)
            self.assertLessEqual(state.claims, 13)

    def test_runs_no_more_than_ten_jobs_concurrently(self):
        state = FakeQueueState(20)
        stop = threading.Event()
        release = threading.Event()
        ten_started = threading.Event()
        lock = threading.Lock()
        active = 0
        peak = 0
        completed = 0
        active_workers = set()

        def run_job(_args, remote, _manifest, _stop):
            nonlocal active, peak, completed
            with lock:
                active += 1
                peak = max(peak, active)
                active_workers.add(remote.worker)
                if active == worker.SCHEDULER_SLOTS:
                    ten_started.set()
            release.wait(5)
            with lock:
                active -= 1
                completed += 1
                if completed == 20:
                    stop.set()
            return {"ok": True}

        service = threading.Thread(
            target=worker.run_scheduler,
            args=(scheduler_args(),),
            kwargs={
                "stop_event": stop,
                "remote_factory": lambda name: FakeRemote(name, state),
                "job_runner": run_job,
            },
        )
        service.start()
        self.assertTrue(ten_started.wait(5), "ten scheduler slots did not fill")
        with lock:
            self.assertEqual(active, 10)
            self.assertEqual(peak, 10)
        for _ in range(100):
            with state.lock:
                if any(name == "m4mac" and total == 10 for name, total, _ in state.heartbeats):
                    break
            threading.Event().wait(0.005)
        release.set()
        service.join(5)
        self.assertFalse(service.is_alive())
        self.assertEqual(completed, 20)
        self.assertEqual(
            active_workers,
            {f"m4mac.{index:02d}" for index in range(10)},
        )
        self.assertTrue(
            any(
                name == "m4mac" and total == 10
                for name, total, _busy in state.heartbeats
            )
        )

    def test_shutdown_interrupts_active_jobs_and_claims_no_more(self):
        state = FakeQueueState(20)
        stop = threading.Event()
        all_started = threading.Event()
        lock = threading.Lock()
        started = 0
        finished = 0

        def run_job(_args, _remote, _manifest, job_stop):
            nonlocal started, finished
            with lock:
                started += 1
                if started == 10:
                    all_started.set()
            self.assertTrue(job_stop.wait(5))
            with lock:
                finished += 1
            return {"ok": False}

        service = threading.Thread(
            target=worker.run_scheduler,
            args=(scheduler_args(),),
            kwargs={
                "stop_event": stop,
                "remote_factory": lambda name: FakeRemote(name, state),
                "job_runner": run_job,
            },
        )
        service.start()
        self.assertTrue(all_started.wait(5), "active slots did not start")
        stop.set()
        service.join(5)
        self.assertFalse(service.is_alive())
        self.assertEqual(started, 10)
        self.assertEqual(finished, 10)
        with state.lock:
            self.assertEqual(len(state.jobs), 10)


class ResourceAdmissionTests(unittest.TestCase):
    @staticmethod
    def manager(
        *,
        free_bytes=100,
        minimum_free=40,
        maximum_result=0,
        maximum_job_memory=10,
        physical_memory=100,
        memory_headroom=20,
        available_memory=100,
        group_reader=None,
    ):
        return worker.SchedulerResourceManager(
            Path("/tmp"),
            minimum_free_bytes=minimum_free,
            maximum_result_bytes=maximum_result,
            maximum_job_memory_bytes=maximum_job_memory,
            minimum_memory_headroom_bytes=memory_headroom,
            free_space_reader=lambda _path: free_bytes,
            physical_memory_reader=lambda: physical_memory,
            available_memory_reader=lambda: available_memory,
            group_resource_reader=group_reader,
        )

    def test_disk_reservations_serialize_staging_across_slots(self):
        manager = self.manager()
        manifest = {"sources": [{"size": 30}], "inputs": []}
        first = manager.acquire(manifest, None)
        acquired = threading.Event()
        reservations = []

        def acquire_second():
            reservations.append(manager.acquire(manifest, None))
            acquired.set()

        thread = threading.Thread(target=acquire_second)
        thread.start()
        self.assertFalse(acquired.wait(0.1))
        self.assertEqual(manager.reserved_disk_bytes, 60)
        first.release()
        self.assertTrue(acquired.wait(2))
        reservations[0].release()
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(manager.reserved_disk_bytes, 0)

    def test_memory_headroom_serializes_jobs_without_reducing_slot_count(self):
        manager = self.manager(
            free_bytes=1_000,
            minimum_free=0,
            maximum_job_memory=40,
            physical_memory=100,
            memory_headroom=40,
        )
        first = manager.acquire({"sources": [], "inputs": []}, None)
        acquired = threading.Event()
        reservations = []

        def acquire_second():
            reservations.append(
                manager.acquire({"sources": [], "inputs": []}, None)
            )
            acquired.set()

        thread = threading.Thread(target=acquire_second)
        thread.start()
        self.assertFalse(acquired.wait(0.1))
        self.assertEqual(manager.reserved_memory_bytes, 40)
        first.release()
        self.assertTrue(acquired.wait(2))
        reservations[0].release()
        thread.join(2)
        self.assertFalse(thread.is_alive())

    def test_global_rss_guard_stops_only_the_largest_group(self):
        readings = {101: (35, 1), 202: (40, 1)}
        manager = self.manager(
            physical_memory=100,
            memory_headroom=30,
            available_memory=20,
            group_reader=lambda group: readings[group],
        )
        manager.register_process_group(101)
        manager.register_process_group(202)

        self.assertIsNone(manager.global_memory_violation(101))
        self.assertIn("scheduler headroom", manager.global_memory_violation(202))

    def test_memory_admission_does_not_double_count_current_worker_rss(self):
        manager = self.manager(
            free_bytes=1_000,
            minimum_free=0,
            maximum_job_memory=20,
            physical_memory=100,
            memory_headroom=40,
            available_memory=65,
            group_reader=lambda _group: (15, 1),
        )
        first = manager.acquire({"sources": [], "inputs": []}, None)
        manager.register_process_group(101)

        second = manager.acquire({"sources": [], "inputs": []}, None)

        second.release()
        first.release()

    def test_drain_aborts_a_job_waiting_for_admission(self):
        manager = self.manager()
        manifest = {"sources": [{"size": 30}], "inputs": []}
        first = manager.acquire(manifest, None)
        drain = threading.Event()
        stopped = threading.Event()

        def wait_for_resources():
            with self.assertRaises(worker.WorkerShutdown):
                manager.acquire(manifest, None, drain)
            stopped.set()

        thread = threading.Thread(target=wait_for_resources)
        thread.start()
        self.assertFalse(stopped.wait(0.1))
        drain.set()
        self.assertTrue(stopped.wait(2))
        first.release()
        thread.join(2)
        self.assertFalse(thread.is_alive())

    def test_disk_reservation_covers_staging_and_packaging_peaks(self):
        manifest = {"sources": [{"size": 10}], "inputs": [{"size": 5}]}
        self.assertEqual(worker.staging_required_bytes(manifest), 25)
        self.assertEqual(worker.job_disk_reservation_bytes(manifest, 100), 215)


class WorkerIsolationTests(unittest.TestCase):
    DYNAMIC_EXECUTION = {
        "profile": "apk-analyze",
        "family": "jadx",
        "argv": ["-d", "{result:jadx}", "{input:0}"],
        "outputs": ["jadx"],
        "datasets": [],
        "minimum_inputs": 1,
        "maximum_inputs": 1,
        "input_values": False,
    }

    def setUp(self):
        self.resource_reader_patch = mock.patch.object(
            worker, "process_group_resources", return_value=(0, 1)
        )
        self.resource_reader_patch.start()

    def tearDown(self):
        self.resource_reader_patch.stop()

    @staticmethod
    def source_record(path, data):
        return {
            "path": path,
            "size": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }

    @staticmethod
    def tar_bytes(files=(), extra_members=()):
        output = BytesIO()
        with tarfile.open(fileobj=output, mode="w") as archive:
            for name, data in files:
                info = tarfile.TarInfo(name)
                info.size = len(data)
                archive.addfile(info, BytesIO(data))
            for info in extra_members:
                archive.addfile(info)
        return output.getvalue()

    def test_dynamic_jobs_fail_closed_without_a_sandbox(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaisesRegex(worker.WorkerError, "require a validated sandbox"):
                worker.execute_job(
                    {
                        "id": "20260722T120000Z-00000001",
                        "task": "apk-test",
                        "arguments": [],
                        "inputs": [{"value": None}],
                        "execution": self.DYNAMIC_EXECUTION,
                    },
                    source_root=root / "source",
                    input_paths=[root / "input.apk"],
                    input_values=[None],
                    result_root=root / "result",
                    python=worker.sys.executable,
                    timeout=30,
                    nice=0,
                    maximum_file_size=1024 * 1024,
                )

    def test_invalid_job_id_is_rejected_before_staging(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(worker.WorkerError, "invalid id"):
                worker.prepare_job(
                    None,
                    {"id": "../../unsafe", "sources": [], "inputs": []},
                    Path(directory),
                )

    def test_prepare_job_creates_empty_source_and_input_roots(self):
        class NoTransferRemote:
            worker = "m4mac.00"

        with tempfile.TemporaryDirectory() as directory:
            source_root, inputs, values = worker.prepare_job(
                NoTransferRemote(),
                {
                    "id": "20260722T120000Z-00000004",
                    "sources": [],
                    "inputs": [],
                },
                Path(directory),
            )

            self.assertTrue(source_root.is_dir())
            self.assertTrue((Path(directory) / "inputs").is_dir())
            self.assertEqual(inputs, [])
            self.assertEqual(values, [])

    def test_dataset_paths_must_be_absolute(self):
        with self.assertRaisesRegex(worker.WorkerError, "absolute"):
            worker.parse_dataset_entry("corpus=relative/path")

    def test_prepare_job_downloads_all_sources_in_one_bundle(self):
        source_data = {
            "tools/one.py": b"print('one')\n",
            "lib/two.py": b"print('two')\n",
        }

        class BundleRemote:
            worker = "m4mac.00"

            def __init__(self, payload):
                self.payload = payload
                self.bundle_calls = 0
                self.source_calls = 0

            def stream_source_bundle(self, _job, destination, **_kwargs):
                self.bundle_calls += 1
                destination.write_bytes(self.payload)

            def stream_source(self, *_args, **_kwargs):
                self.source_calls += 1

        remote = BundleRemote(self.tar_bytes(source_data.items()))
        manifest = {
            "id": "20260722T120000Z-00000005",
            "sources": [
                self.source_record(path, data) for path, data in source_data.items()
            ],
            "inputs": [],
        }
        with tempfile.TemporaryDirectory() as directory:
            source_root, inputs, values = worker.prepare_job(
                remote, manifest, Path(directory)
            )
            self.assertEqual(inputs, [])
            self.assertEqual(values, [])
            self.assertEqual(remote.bundle_calls, 1)
            self.assertEqual(remote.source_calls, 0)
            for path, data in source_data.items():
                self.assertEqual((source_root / path).read_bytes(), data)

    def test_prepare_job_falls_back_only_for_unsupported_bundle_api(self):
        data = b"legacy"
        record = self.source_record("tools/legacy.py", data)

        class LegacyRemote:
            worker = "m4mac.00"

            def stream_source_bundle(self, *_args, **_kwargs):
                raise worker.UnsupportedSourceBundle("unsupported source-bundle")

            def stream_source(self, _job, _relative, destination, **_kwargs):
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(data)

        with tempfile.TemporaryDirectory() as directory:
            source_root, _inputs, _values = worker.prepare_job(
                LegacyRemote(),
                {
                    "id": "20260722T120000Z-00000006",
                    "sources": [record],
                    "inputs": [],
                },
                Path(directory),
            )
            self.assertEqual((source_root / "tools/legacy.py").read_bytes(), data)

    def test_claimed_job_reports_phase_timing_and_uploads_telemetry_last(self):
        script = b"""#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
parser = argparse.ArgumentParser()
parser.add_argument('capture')
parser.add_argument('--json', required=True)
args = parser.parse_args()
Path(args.json).write_text(json.dumps({'bytes': Path(args.capture).stat().st_size}))
"""
        capture = b"capture-data"
        lease = "a" * 32

        class EndToEndRemote:
            worker = "m4mac.00"

            def __init__(self, bundle):
                self.bundle = bundle
                self.uploads = []
                self.finish_lease = None

            def stream_source_bundle(self, _job, destination, *, lease_token=None):
                self.assert_lease(lease_token)
                destination.write_bytes(self.bundle)

            def stream_input(self, _job, _index, destination, *, lease_token=None):
                self.assert_lease(lease_token)
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(capture)

            def put_result(self, _job, relative, path, *, lease_token=None):
                self.assert_lease(lease_token)
                self.uploads.append((relative, path.read_bytes()))
                return {"path": relative}

            def finish(self, _job, _code, results, *, lease_token=None):
                self.assert_lease(lease_token)
                self.finish_lease = lease_token
                return {"state": "done", "results": results}

            @staticmethod
            def assert_lease(value):
                if value != lease:
                    raise AssertionError("lease token was not forwarded")

        manifest = {
            "id": "20260722T120000Z-00000007",
            "task": "can-capture-summary",
            "arguments": [],
            "lease_token": lease,
            "sources": [self.source_record("tools/can_capture_summary.py", script)],
            "inputs": [
                {"index": 0, "name": "capture.log", "size": len(capture), "value": None}
            ],
        }
        remote = EndToEndRemote(
            self.tar_bytes((("tools/can_capture_summary.py", script),))
        )
        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(
                work_root=Path(directory),
                python=sys.executable,
                timeout=30,
                nice=0,
                max_result_bytes=1024 * 1024,
                max_memory_bytes=512 * 1024 * 1024,
                executables={"python": sys.executable},
                datasets={},
                sandbox_profile=None,
                allow_unsandboxed_dynamic=False,
            )
            payload = worker.run_claimed_job(args, remote, manifest)

        self.assertTrue(payload["ok"])
        self.assertEqual(remote.uploads[-1][0], "execution.json")
        telemetry = json.loads(remote.uploads[-1][1])
        timing = telemetry["timing"]
        for key in (
            "source_input_preparation_seconds",
            "analysis_seconds",
            "packaging_seconds",
            "result_upload_seconds_excluding_execution_json",
            "worker_attempt_active_seconds",
        ):
            self.assertIsInstance(timing[key], (int, float))
            self.assertGreaterEqual(timing[key], 0)
        self.assertIn("excludes final execution.json", timing["worker_attempt_active_note"])
        self.assertEqual(remote.finish_lease, lease)

    def test_shutdown_before_execution_leaves_claim_unfinished(self):
        class Remote:
            worker = "m4mac.00"

            def put_result(self, *_args, **_kwargs):
                raise AssertionError("shutdown lease must not upload results")

            def finish(self, *_args, **_kwargs):
                raise AssertionError("shutdown lease must not be finalized")

        stop = threading.Event()
        stop.set()
        manifest = {
            "id": "20260722T120000Z-00000008",
            "task": "can-capture-summary",
            "lease_token": "b" * 32,
            "sources": [],
            "inputs": [],
            "arguments": [],
        }
        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(work_root=Path(directory))
            with self.assertRaises(worker.WorkerShutdown):
                worker.run_claimed_job(args, Remote(), manifest, stop)

    def test_interrupted_analysis_leaves_claim_unfinished(self):
        class Remote:
            worker = "m4mac.00"

            def put_result(self, *_args, **_kwargs):
                raise AssertionError("interrupted lease must not upload results")

            def finish(self, *_args, **_kwargs):
                raise AssertionError("interrupted lease must not be finalized")

        manifest = {
            "id": "20260722T120000Z-00000009",
            "task": "can-capture-summary",
            "lease_token": "c" * 32,
            "sources": [],
            "inputs": [],
            "arguments": [],
        }
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            args = argparse.Namespace(
                work_root=work,
                python=sys.executable,
                timeout=30,
                nice=0,
                max_result_bytes=1024 * 1024,
                max_memory_bytes=512 * 1024 * 1024,
                max_processes=10,
                executables={"python": sys.executable},
                datasets={},
                sandbox_profile=None,
                allow_unsandboxed_dynamic=False,
            )
            with mock.patch.object(
                worker,
                "prepare_job",
                return_value=(work / "source", [], []),
            ), mock.patch.object(
                worker,
                "execute_job",
                return_value=(143, {"interrupted": True}),
            ), self.assertRaises(worker.WorkerShutdown):
                worker.run_claimed_job(
                    args, Remote(), manifest, threading.Event()
                )

    def test_memory_watchdog_terminates_over_limit_process_group(self):
        process = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            start_new_session=True,
        )
        outcome = worker.wait_for_analysis_process(
            process,
            timeout=30,
            stop_event=None,
            maximum_memory=1024,
            maximum_processes=10,
            resource_reader=lambda _group: (2048, 1),
        )

        self.assertEqual(outcome.exit_code, 137)
        self.assertIn("RSS exceeded", outcome.resource_limit)
        self.assertEqual(outcome.peak_process_group_rss_bytes, 2048)

    def test_free_space_watchdog_terminates_before_reserve_is_exhausted(self):
        process = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            start_new_session=True,
        )
        with tempfile.TemporaryDirectory() as directory:
            outcome = worker.wait_for_analysis_process(
                process,
                timeout=30,
                stop_event=None,
                maximum_memory=1024 * 1024,
                maximum_processes=10,
                resource_reader=lambda _group: (0, 1),
                work_path=Path(directory),
                minimum_free_bytes=100,
                free_space_reader=lambda _path: 50,
            )

        self.assertEqual(outcome.exit_code, 137)
        self.assertIn("free space", outcome.resource_limit)
        self.assertEqual(outcome.minimum_filesystem_free_bytes, 50)

    def test_staging_capacity_accounts_for_bundle_and_inputs(self):
        manifest = {
            "sources": [{"size": 20}],
            "inputs": [{"size": 30}],
        }
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            worker, "filesystem_free_bytes", return_value=100
        ), self.assertRaisesRegex(worker.WorkerError, "insufficient free space"):
            worker.require_staging_capacity(Path(directory), manifest, 40)

    def test_source_bundle_rejects_traversal_links_extra_and_missing_members(self):
        expected_data = b"expected"
        sources = [self.source_record("tools/expected.py", expected_data)]
        symlink = tarfile.TarInfo("tools/expected.py")
        symlink.type = tarfile.SYMTYPE
        symlink.linkname = "/private/escape"
        cases = {
            "unsafe member": self.tar_bytes((("../escape", b"bad"),)),
            "not a regular file": self.tar_bytes(extra_members=(symlink,)),
            "extra or duplicate": self.tar_bytes(
                (("tools/expected.py", expected_data), ("tools/extra.py", b"bad"))
            ),
            "missing member": self.tar_bytes(),
        }
        for message, payload in cases.items():
            with self.subTest(message=message), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                archive = root / "sources.tar"
                archive.write_bytes(payload)
                with self.assertRaisesRegex(worker.WorkerError, message):
                    worker.extract_source_bundle(archive, root / "source", sources)

    def test_declared_output_directory_becomes_one_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "result"
            output = result / "jadx" / "sources" / "example"
            output.mkdir(parents=True)
            (output / "Main.java").write_text("class Main {}", encoding="utf-8")
            manifest = {"execution": self.DYNAMIC_EXECUTION}

            artifacts = worker.package_declared_output_directories(
                manifest, result, 1024 * 1024
            )

            self.assertEqual(artifacts, {"jadx": "jadx.tar.gz"})
            self.assertFalse((result / "jadx").exists())
            with tarfile.open(result / "jadx.tar.gz", "r:gz") as archive:
                self.assertIn("jadx/sources/example/Main.java", archive.getnames())

    def test_declared_output_rejects_symlinked_parent_component(self):
        nested = dict(self.DYNAMIC_EXECUTION)
        nested["argv"] = [
            "-d",
            "{result:link/queued}",
            "{input:0}",
        ]
        nested["outputs"] = ["link/queued"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = root / "result"
            outside = root / "outside"
            queued = outside / "queued"
            queued.mkdir(parents=True)
            sentinel = queued / "keep.txt"
            sentinel.write_text("keep", encoding="utf-8")
            result.mkdir()
            (result / "link").symlink_to(outside, target_is_directory=True)

            with self.assertRaisesRegex(
                worker.WorkerError, "symlinked path component"
            ):
                worker.package_declared_output_directories(
                    {"execution": nested}, result, 1024 * 1024
                )

            self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep")

    def test_overlapping_or_missing_declared_outputs_fail_closed(self):
        overlapping = dict(self.DYNAMIC_EXECUTION)
        overlapping["outputs"] = ["jadx", "jadx/nested"]
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory) / "result"
            (result / "jadx").mkdir(parents=True)
            with self.assertRaisesRegex(
                (worker.WorkerError, worker.protocol.ProtocolError), "overlap"
            ):
                worker.package_declared_output_directories(
                    {"execution": overlapping}, result, 1024 * 1024
                )
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(worker.WorkerError, "omitted declared"):
                worker.validate_declared_outputs(
                    {"execution": self.DYNAMIC_EXECUTION},
                    Path(directory),
                    require_all=True,
                )

    def test_execution_telemetry_redacts_and_scopes_private_datasets(self):
        execution_spec = {
            "profile": "python-script",
            "family": "python",
            "argv": [
                "{source:tools/write_result.py}",
                "{dataset:declared}",
                "{result:result.json}",
            ],
            "outputs": ["result.json"],
            "datasets": ["declared", "declared-but-unused"],
            "minimum_inputs": 0,
            "maximum_inputs": 0,
            "input_values": False,
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            source = root / "source"
            tool = source / "tools" / "write_result.py"
            tool.parent.mkdir(parents=True)
            tool.write_text(
                "import json, sys\n"
                "from pathlib import Path\n"
                "Path(sys.argv[2]).write_text(json.dumps({'path': sys.argv[1]}))\n",
                encoding="utf-8",
            )
            declared = root / "mac-private-declared"
            declared_but_unused = root / "mac-private-declared-but-unused"
            undeclared = root / "mac-private-undeclared"
            declared.mkdir()
            declared_but_unused.mkdir()
            undeclared.mkdir()
            profile = root / "sandbox.sb"
            profile.write_text("test profile", encoding="utf-8")
            captured_datasets = {}

            def bypass_sandbox(command, **kwargs):
                captured_datasets.update(kwargs["datasets"])
                return list(command)

            result = root / "result"
            with mock.patch.object(
                worker, "sandbox_command", side_effect=bypass_sandbox
            ):
                exit_code, execution = worker.execute_job(
                    {
                        "id": "20260722T120000Z-00000003",
                        "task": "private-dataset-test",
                        "arguments": [],
                        "inputs": [],
                        "execution": execution_spec,
                    },
                    source_root=source,
                    input_paths=[],
                    input_values=[],
                    result_root=result,
                    python=worker.sys.executable,
                    timeout=30,
                    nice=0,
                    maximum_file_size=1024 * 1024,
                    datasets={
                        "declared": declared,
                        "declared-but-unused": declared_but_unused,
                        "undeclared": undeclared,
                    },
                    sandbox_profile=profile,
                )

            telemetry_text = (result / "execution.json").read_text()
            result_payload = json.loads((result / "result.json").read_text())
            self.assertEqual(exit_code, 0)
            self.assertEqual(set(captured_datasets), {"declared"})
            self.assertEqual(execution["placement"], "remote")
            self.assertNotIn(str(declared), telemetry_text)
            self.assertNotIn(str(declared_but_unused), telemetry_text)
            self.assertNotIn(str(undeclared), telemetry_text)
            self.assertEqual(result_payload["path"], "{dataset:declared}")

    def test_child_environment_does_not_inherit_parent_secret_or_home(self):
        summary_stub = """#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path
parser = argparse.ArgumentParser()
parser.add_argument('capture')
parser.add_argument('--json', required=True)
args = parser.parse_args()
Path(args.json).write_text(json.dumps({
    'secret': os.environ.get('VAN_COMPUTE_TEST_SECRET'),
    'home': os.environ.get('HOME'),
    'tmpdir': os.environ.get('TMPDIR'),
}))
"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            tool = source / "tools" / "can_capture_summary.py"
            tool.parent.mkdir(parents=True)
            tool.write_text(summary_stub, encoding="utf-8")
            capture = root / "inputs" / "capture.log"
            capture.parent.mkdir()
            capture.write_text("capture", encoding="utf-8")
            result = root / "result"
            manifest = {
                "id": "20260722T120000Z-00000002",
                "task": "can-capture-summary",
                "arguments": [],
                "inputs": [{"value": None}],
            }
            with mock.patch.dict(
                os.environ,
                {"VAN_COMPUTE_TEST_SECRET": "must-not-leak", "HOME": "/private/leak"},
            ):
                exit_code, _execution = worker.execute_job(
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
            environment = json.loads((result / "summary.json").read_text())
            self.assertEqual(exit_code, 0)
            self.assertIsNone(environment["secret"])
            self.assertEqual(environment["home"], "{job}/home")
            self.assertEqual(environment["tmpdir"], "{job}/tmp/")

    def test_exec_helper_does_not_import_untrusted_sitecustomize_before_sandbox(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            marker = root / "unsandboxed-marker"
            (source / "sitecustomize.py").write_text(
                f"from pathlib import Path\nPath({str(marker)!r}).write_text('unsafe')\n",
                encoding="utf-8",
            )
            environment = {
                "HOME": str(root / "home"),
                "PATH": "/usr/bin:/bin",
                "PYTHONNOUSERSITE": "1",
                "VAN_COMPUTE_CHILD_PYTHONPATH": str(source),
            }
            result = subprocess.run(
                [
                    sys.executable,
                    str(Path(worker.__file__).resolve()),
                    "__exec__",
                    "0",
                    "5",
                    str(1024 * 1024),
                    str(512 * 1024 * 1024),
                    "--",
                    "/usr/bin/true",
                ],
                env=environment,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            self.assertFalse(marker.exists())

    def test_background_descendant_is_stopped_before_results_are_packaged(self):
        summary_stub = """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
import subprocess
import sys
parser = argparse.ArgumentParser()
parser.add_argument('capture')
parser.add_argument('--json', required=True)
args = parser.parse_args()
late = args.json + '.late'
code = "import time; from pathlib import Path; time.sleep(0.4); Path(%r).write_text('late')" % late
subprocess.Popen([sys.executable, '-c', code], stdin=subprocess.DEVNULL,
                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
Path(args.json).write_text(json.dumps({'ok': True}))
"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            tool = source / "tools" / "can_capture_summary.py"
            tool.parent.mkdir(parents=True)
            tool.write_text(summary_stub, encoding="utf-8")
            capture = root / "capture.log"
            capture.write_text("capture", encoding="utf-8")
            result = root / "result"
            exit_code, _execution = worker.execute_job(
                {
                    "id": "20260722T120000Z-00000004",
                    "task": "can-capture-summary",
                    "arguments": [],
                    "inputs": [{"value": None}],
                },
                source_root=source,
                input_paths=[capture],
                input_values=[None],
                result_root=result,
                python=worker.sys.executable,
                timeout=30,
                nice=0,
                maximum_file_size=1024 * 1024,
            )
            threading.Event().wait(0.6)
            self.assertEqual(exit_code, 0)
            self.assertFalse((result / "summary.json.late").exists())


if __name__ == "__main__":
    unittest.main()
