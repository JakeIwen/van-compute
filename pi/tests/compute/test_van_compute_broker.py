import argparse
from contextlib import redirect_stderr, redirect_stdout
import datetime as dt
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import time
import unittest
from unittest import mock

from pi.scripts.compute import pi_compute
from pi.scripts.compute import van_compute as queue
from pi.scripts.compute import van_compute_broker as broker
from shared.python import van_compute_protocol as protocol


LOCAL_SCRIPT = """#!/usr/bin/env python3
import subprocess
import sys
import time

print('local result', flush=True)
if '--background' in sys.argv:
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
    print(f'background={child.pid}', flush=True)
if '--fail' in sys.argv:
    raise SystemExit(7)
"""

SUMMARY_SCRIPT = """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
parser = argparse.ArgumentParser()
parser.add_argument('capture')
parser.add_argument('--json', required=True)
args = parser.parse_args()
Path(args.json).write_text(json.dumps({'bytes': Path(args.capture).stat().st_size}))
print('summary complete')
"""


DECLARATION = {
    "schema_version": 1,
    "tasks": [
        {
            "name": "local-python",
            "profile": "python-script",
            "source_paths": ["tools/local_job.py"],
            "minimum_inputs": 0,
            "maximum_inputs": 0,
            "argv": ["{source:tools/local_job.py}", "{arguments}"],
            "outputs": [],
        },
        {
            "name": "decode-apk",
            "profile": "apk-analyze",
            "source_paths": [],
            "minimum_inputs": 1,
            "maximum_inputs": 1,
            "argv": ["-d", "{result:decoded}", "{input:0}"],
            "outputs": ["decoded"],
        },
        {
            "name": "missing-output",
            "profile": "python-script",
            "source_paths": ["tools/local_job.py"],
            "minimum_inputs": 0,
            "maximum_inputs": 0,
            "argv": ["{source:tools/local_job.py}"],
            "outputs": ["required.json"],
        },
    ],
}


GOOD_HEALTH = broker.HealthSnapshot(
    memory_available_bytes=4 * 1024 * 1024 * 1024,
    swap_total_bytes=1024 * 1024 * 1024,
    swap_used_bytes=0,
    load_1m=0.25,
    cpu_count=4,
    temperature_c=45.0,
    throttled_flags=0,
)


class BrokerHarness(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        base = Path(self.temporary.name)
        self.source = base / "obd-things"
        self.root = self.source / "tmp" / "compute"
        self.work = base / "broker-work" / "jobs"
        (self.source / "tools").mkdir(parents=True)
        (self.source / "tools" / "local_job.py").write_text(
            LOCAL_SCRIPT, encoding="utf-8"
        )
        (self.source / "tools" / "can_capture_summary.py").write_text(
            SUMMARY_SCRIPT, encoding="utf-8"
        )
        (self.source / protocol.REPO_MANIFEST).write_text(
            json.dumps(DECLARATION), encoding="utf-8"
        )
        self.apk = self.source / "tmp" / "sample.apk"
        self.apk.parent.mkdir(parents=True)
        self.apk.write_bytes(b"not really an apk")
        self.capture = self.source / "tmp" / "capture.log"
        self.capture.write_bytes(b"can capture bytes\n")
        self.args = argparse.Namespace(
            root=self.root,
            work_root=self.work,
            remote_max_age=45.0,
            remote_grace=0.0,
            stale_running_age=300.0,
            timeout=30,
            cpu_seconds=30,
            # Darwin reserves a large virtual address range even for a tiny
            # interpreter; production validation caps this at the Pi's 1 GiB.
            max_memory_bytes=64 * 1024 * 1024 * 1024,
            max_result_bytes=1024 * 1024,
            min_work_free_bytes=0,
            max_open_files=128,
            max_processes=4096,
            nice=0,
            python=sys.executable,
            sqlite3="/usr/bin/sqlite3",
            bwrap=sys.executable,
            health_thresholds=broker.HealthThresholds(
                minimum_available_bytes=512 * 1024 * 1024,
                maximum_swap_used_fraction=0.20,
                maximum_load_per_cpu=1.25,
                maximum_temperature_c=75.0,
            ),
        )

    def tearDown(self):
        self.temporary.cleanup()

    def submit(self, task="local-python", arguments=None):
        inputs = []
        if task == "decode-apk":
            inputs = [str(self.apk)]
        elif task == "can-capture-summary":
            inputs = [str(self.capture)]
        submit_args = argparse.Namespace(
            root=self.root,
            source_root=self.source,
            task=task,
            argument=list(arguments or []),
            input=inputs,
            input_value=None,
        )
        return queue.submit_job(submit_args)

    @staticmethod
    def no_sandbox(_executable, command, **paths):
        replacements = {
            "/job/source": str(paths["source_root"]),
            "/job/inputs": str(paths["inputs_root"]),
            "/job/result": str(paths["result_root"]),
            "/job/runtime/python/bin/python3": sys.executable,
            f"/job/runtime/python/bin/{Path(sys.executable).name}": sys.executable,
        }
        rendered = []
        for item in command:
            value = item
            for sandbox, host in replacements.items():
                if value == sandbox or value.startswith(f"{sandbox}/"):
                    value = host + value[len(sandbox) :]
                    break
            rendered.append(value)
        return rendered

    def run_once(self):
        with mock.patch.object(
            broker, "bubblewrap_self_test", return_value=(True, "test")
        ), mock.patch.object(broker, "bubblewrap_command", side_effect=self.no_sandbox):
            return broker.run_once(self.args, health_reader=lambda: GOOD_HEALTH)


class PlacementTests(BrokerHarness):
    def test_queue_maintenance_defers_pi_fallback(self):
        submitted = self.submit()
        root = queue.safe_root(self.root)
        queue.set_maintenance(root, "m4mac-installer", True)

        result = self.run_once()

        self.assertEqual(result["action"], "deferred")
        self.assertIn("maintenance", result["reason"])
        self.assertTrue((root / "queued" / submitted["id"]).is_dir())

    def test_local_claim_crash_before_move_leaves_job_reclaimable(self):
        submitted = self.submit()
        root = queue.safe_root(self.root)
        source = root / "queued" / submitted["id"]
        destination = root / "running" / submitted["id"]
        original_replace = broker.os.replace

        def interrupt_job_move(old, new):
            if Path(old) == source and Path(new) == destination:
                raise OSError("simulated interruption before local job move")
            return original_replace(old, new)

        with mock.patch.object(broker.os, "replace", side_effect=interrupt_job_move):
            with self.assertRaisesRegex(OSError, "simulated interruption"):
                broker.claim_local_job(
                    root,
                    grace_seconds=0,
                    remote_max_age=45,
                    eligibility=broker.local_eligibility,
                )

        interrupted = queue.load_json(source / "manifest.json")
        self.assertEqual(interrupted["worker"], broker.LOCAL_WORKER)
        self.assertFalse(destination.exists())

        reclaimed, reason = broker.claim_local_job(
            root,
            grace_seconds=0,
            remote_max_age=45,
            eligibility=broker.local_eligibility,
        )
        self.assertEqual(reason, "claimed local fallback job")
        self.assertEqual(reclaimed["id"], submitted["id"])
        self.assertNotEqual(reclaimed["lease_token"], interrupted["lease_token"])
        self.assertTrue(destination.is_dir())

    def test_local_claim_ignores_symlinked_queued_job_directory(self):
        root = queue.safe_root(self.root)
        job_id = "20260722T000000Z-deadbeef"
        outside = Path(self.temporary.name) / "outside-queued-job"
        outside.mkdir()
        marker = outside / "marker"
        marker.write_text("keep me\n", encoding="utf-8")
        queue.atomic_json(
            outside / "manifest.json",
            {
                "id": job_id,
                "task": "local-python",
                "state": "queued",
                "submitted_at": "2020-01-01T00:00:00+00:00",
                "execution": protocol.task_execution(
                    protocol._parse_repo_task(DECLARATION["tasks"][0], 0)
                ),
            },
        )
        (root / "queued" / job_id).symlink_to(outside, target_is_directory=True)

        claimed, reason = broker.claim_local_job(
            root,
            grace_seconds=0,
            remote_max_age=45,
            eligibility=broker.local_eligibility,
        )

        self.assertIsNone(claimed)
        self.assertEqual(reason, "no eligible queued jobs")
        self.assertEqual(marker.read_text(encoding="utf-8"), "keep me\n")
        self.assertTrue((root / "queued" / job_id).is_symlink())

    def test_fresh_capacity_lease_prevents_local_fallback_even_when_full(self):
        submitted = self.submit()
        root = queue.safe_root(self.root)
        queue.worker_heartbeat(root, "m4mac", slots_total=10, slots_busy=10)

        result = self.run_once()

        self.assertEqual(result["action"], "deferred")
        self.assertIn("remote worker lease", result["reason"])
        self.assertTrue((root / "queued" / submitted["id"]).is_dir())

    def test_stale_worker_permits_exactly_one_local_fallback(self):
        self.submit()
        self.submit()
        root = queue.safe_root(self.root)
        heartbeat = queue.worker_heartbeat(root, "m4mac", slots_total=10, slots_busy=0)
        heartbeat["seen_at"] = "2020-01-01T00:00:00+00:00"
        queue.atomic_json(root / "workers" / "m4mac.json", heartbeat)

        result = self.run_once()

        self.assertEqual(result["action"], "executed")
        self.assertEqual(len(list((root / "done").iterdir())), 1)
        self.assertEqual(len(list((root / "queued").iterdir())), 1)

    def test_remote_grace_starts_when_latest_lease_expires(self):
        submitted = self.submit()
        root = queue.safe_root(self.root)
        manifest_path = root / "queued" / submitted["id"] / "manifest.json"
        manifest = queue.load_json(manifest_path)
        manifest["submitted_at"] = "2020-01-01T00:00:00+00:00"
        queue.atomic_json(manifest_path, manifest)
        now = dt.datetime(2026, 7, 22, 12, 0, tzinfo=dt.timezone.utc)
        heartbeat = queue.worker_heartbeat(root, "m4mac", slots_total=10, slots_busy=0)
        heartbeat["seen_at"] = (now - dt.timedelta(seconds=55)).isoformat()
        queue.atomic_json(root / "workers" / "m4mac.json", heartbeat)

        claimed, reason = broker.claim_local_job(
            root,
            grace_seconds=30,
            remote_max_age=45,
            now=now,
            eligibility=broker.local_eligibility,
        )

        self.assertIsNone(claimed)
        self.assertIn("remote grace period", reason)
        claimed, reason = broker.claim_local_job(
            root,
            grace_seconds=30,
            remote_max_age=45,
            now=now + dt.timedelta(seconds=21),
            eligibility=broker.local_eligibility,
        )
        self.assertEqual(reason, "claimed local fallback job")
        self.assertEqual(claimed["id"], submitted["id"])

    def test_no_heartbeat_grace_falls_back_to_job_submission_time(self):
        submitted = self.submit()
        root = queue.safe_root(self.root)
        now = dt.datetime(2026, 7, 22, 12, 0, tzinfo=dt.timezone.utc)
        manifest_path = root / "queued" / submitted["id"] / "manifest.json"
        manifest = queue.load_json(manifest_path)
        manifest["submitted_at"] = (now - dt.timedelta(seconds=10)).isoformat()
        queue.atomic_json(manifest_path, manifest)

        claimed, reason = broker.claim_local_job(
            root,
            grace_seconds=30,
            remote_max_age=45,
            now=now,
            eligibility=broker.local_eligibility,
        )

        self.assertIsNone(claimed)
        self.assertIn("remote grace period", reason)
        claimed, reason = broker.claim_local_job(
            root,
            grace_seconds=30,
            remote_max_age=45,
            now=now + dt.timedelta(seconds=21),
            eligibility=broker.local_eligibility,
        )
        self.assertEqual(reason, "claimed local fallback job")
        self.assertEqual(claimed["id"], submitted["id"])

    def test_insufficient_disk_capacity_defers_before_local_claim(self):
        submitted = self.submit()
        root = queue.safe_root(self.root)
        manifest = queue.manifest_for(root, submitted["id"])
        required = broker.local_work_required_bytes(
            manifest, self.args.max_result_bytes
        )
        self.args.min_work_free_bytes = 1024

        with mock.patch.object(
            broker.shutil,
            "disk_usage",
            return_value=mock.Mock(free=required + 1023),
        ):
            result = self.run_once()

        self.assertEqual(result["action"], "deferred")
        self.assertIn("local staging needs", result["reason"])
        self.assertTrue((root / "queued" / submitted["id"]).is_dir())

    def test_health_gate_defers_without_claiming(self):
        submitted = self.submit()
        unhealthy = broker.HealthSnapshot(
            memory_available_bytes=128 * 1024 * 1024,
            swap_total_bytes=1024,
            swap_used_bytes=1024,
            load_1m=50,
            cpu_count=4,
            temperature_c=82,
            throttled_flags=0x5,
        )
        with mock.patch.object(
            broker, "bubblewrap_self_test", return_value=(True, "test")
        ):
            result = broker.run_once(self.args, health_reader=lambda: unhealthy)

        self.assertEqual(result["action"], "deferred")
        self.assertEqual(result["reason"], "Pi health gate")
        self.assertTrue((self.root / "queued" / submitted["id"]).is_dir())

    def test_remote_only_profile_stays_queued(self):
        submitted = self.submit("decode-apk")

        result = self.run_once()

        self.assertEqual(result["action"], "deferred")
        self.assertIn("remote-only", result["reason"])
        self.assertTrue((self.root / "queued" / submitted["id"]).is_dir())

    def test_any_private_dataset_task_is_remote_only(self):
        manifest = {
            "task": "private-python",
            "execution": {
                "profile": "python-script",
                "family": "python",
                "argv": ["{source:tools/private.py}", "{dataset:private-corpus}"],
                "outputs": [],
                "datasets": ["private-corpus"],
                "minimum_inputs": 0,
                "maximum_inputs": 0,
                "input_values": False,
            },
        }

        eligible, reason = broker.local_eligibility(manifest)

        self.assertFalse(eligible)
        self.assertIn("remote-only", reason)

    def test_success_records_pi_placement_results_and_one_local_event(self):
        submitted = self.submit()

        result = self.run_once()

        self.assertTrue(result["ok"], result)
        manifest = queue.manifest_for(queue.safe_root(self.root), submitted["id"])
        self.assertEqual(manifest["state"], "done")
        self.assertEqual(manifest["placement"], "pi-local")
        self.assertEqual(manifest["attempt"], 1)
        execution = json.loads(
            (
                self.root
                / "done"
                / submitted["id"]
                / "result"
                / "execution.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(execution["placement"], "pi-local")
        self.assertIn("analysis_seconds", execution)
        self.assertIn("broker_active_seconds", execution)
        self.assertIn("maximum RSS returned by wait4", execution["resource_usage"]["peak_rss_note"])
        output = BytesIO()
        queue.stream_result(
            queue.safe_root(self.root), submitted["id"], "stdout.txt", output
        )
        self.assertIn(b"local result", output.getvalue())
        events = list((self.root / "missed").glob("*.json"))
        self.assertEqual(len(events), 1)
        event = json.loads(events[0].read_text(encoding="utf-8"))
        self.assertEqual(event["profile"], "python-script")
        self.assertEqual(event["reason"], "worker-unavailable")
        self.assertEqual(event["duration_seconds"], execution["broker_active_seconds"])
        self.assertNotIn("vanpi-local.00.json", {p.name for p in (self.root / "workers").iterdir()})

    def test_nonzero_result_finishes_failed_with_telemetry(self):
        submitted = self.submit(arguments=["--fail"])

        result = self.run_once()

        self.assertFalse(result["ok"])
        manifest = queue.manifest_for(queue.safe_root(self.root), submitted["id"])
        self.assertEqual(manifest["state"], "failed")
        self.assertEqual(manifest["exit_code"], 7)
        self.assertEqual(len(list((self.root / "missed").glob("*.json"))), 1)

    def test_success_without_declared_output_fails_closed(self):
        submitted = self.submit("missing-output")

        result = self.run_once()

        self.assertFalse(result["ok"])
        manifest = queue.manifest_for(queue.safe_root(self.root), submitted["id"])
        self.assertEqual(manifest["state"], "failed")
        self.assertEqual(manifest["exit_code"], 70)
        stderr = (
            self.root / "failed" / submitted["id"] / "result" / "stderr.txt"
        ).read_text(encoding="utf-8")
        self.assertIn("omitted declared result", stderr)

    def test_fixed_offline_can_builtin_can_use_guarded_fallback(self):
        submitted = self.submit("can-capture-summary")

        result = self.run_once()

        self.assertTrue(result["ok"])
        summary = json.loads(
            (
                self.root
                / "done"
                / submitted["id"]
                / "result"
                / "summary.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(summary["bytes"], len(b"can capture bytes\n"))
        event_path = next((self.root / "missed").glob("*.json"))
        event = json.loads(event_path.read_text(encoding="utf-8"))
        self.assertEqual(event["profile"], "can-log-batch")

    def test_small_full_swap_is_not_treated_as_current_pressure(self):
        snapshot = broker.HealthSnapshot(
            memory_available_bytes=1600 * 1024 * 1024,
            swap_total_bytes=199 * 1024 * 1024,
            swap_used_bytes=198 * 1024 * 1024,
            load_1m=0.5,
            cpu_count=4,
            temperature_c=50,
            throttled_flags=0,
        )
        assessment = broker.assess_health(snapshot, self.args.health_thresholds)
        self.assertTrue(assessment.ok, assessment.reasons)

    def test_sandbox_failure_defers_before_claim(self):
        submitted = self.submit()
        with mock.patch.object(
            broker, "bubblewrap_self_test", return_value=(False, "namespace denied")
        ):
            result = broker.run_once(self.args, health_reader=lambda: GOOD_HEALTH)
        self.assertEqual(result["action"], "deferred")
        self.assertIn("sandbox self-test failed", result["reason"])
        self.assertTrue((self.root / "queued" / submitted["id"]).is_dir())


class RecoveryAndIsolationTests(BrokerHarness):
    def test_mountinfo_uses_deepest_mount_and_decodes_path(self):
        mount = (self.work / "memory backed").resolve()
        mount.mkdir(parents=True)
        mountinfo = self.work / "mountinfo"
        encoded = mount.as_posix().replace(" ", r"\040")
        mountinfo.write_text(
            "1 0 0:1 / / rw - ext4 /dev/root rw\n"
            f"2 1 0:2 / {encoded} rw - tmpfs tmpfs rw\n",
            encoding="utf-8",
        )

        filesystem_type = broker.filesystem_type_for_path(mount, mountinfo)

        self.assertEqual(filesystem_type, "tmpfs")

    def test_prepare_work_root_rejects_memory_backed_filesystem(self):
        candidate = self.work / "tmpfs-work"
        with mock.patch.object(broker.sys, "platform", "linux"), mock.patch.object(
            broker, "filesystem_type_for_path", return_value="tmpfs"
        ):
            with self.assertRaisesRegex(broker.BrokerError, "disk-backed"):
                broker.prepare_work_root(candidate)

    def test_empty_source_snapshot_creates_read_only_staging_directory(self):
        destination = self.work / "empty-source"

        broker._copy_sources(
            self.root / "queued" / "unused", {"sources": []}, destination
        )

        self.assertTrue(destination.is_dir())
        self.assertEqual(destination.stat().st_mode & 0o777, 0o500)

    def test_directory_packaging_rejects_precreated_archive_symlink(self):
        task = protocol.load_repo_tasks(self.source)["decode-apk"]
        manifest = {"execution": protocol.task_execution(task)}
        result_root = self.work / "packaging-result"
        (result_root / "decoded").mkdir(parents=True)
        (result_root / "decoded" / "output.txt").write_text(
            "safe output\n", encoding="utf-8"
        )
        victim = self.work / "victim.json"
        victim.write_text("keep me\n", encoding="utf-8")
        (result_root / "decoded.tar.gz").symlink_to(victim)

        with self.assertRaisesRegex(broker.BrokerError, "already exists"):
            broker._package_output_directories(manifest, result_root, 1024 * 1024)

        self.assertEqual(victim.read_text(encoding="utf-8"), "keep me\n")

    def test_directory_packaging_rejects_symlinked_output_parent(self):
        execution = {
            "profile": "apk-analyze",
            "family": "jadx",
            "argv": ["-d", "{result:link/decoded}", "{input:0}"],
            "outputs": ["link/decoded"],
            "datasets": [],
            "minimum_inputs": 1,
            "maximum_inputs": 1,
            "input_values": False,
        }
        manifest = {"execution": execution}
        result_root = self.work / "nested-packaging-result"
        outside = self.work / "outside-result"
        (outside / "decoded").mkdir(parents=True)
        marker = outside / "decoded" / "keep.txt"
        marker.write_text("keep me\n", encoding="utf-8")
        result_root.mkdir(parents=True)
        (result_root / "link").symlink_to(outside, target_is_directory=True)

        with self.assertRaisesRegex(broker.BrokerError, "symlinked path component"):
            broker._package_output_directories(manifest, result_root, 1024 * 1024)

        self.assertEqual(marker.read_text(encoding="utf-8"), "keep me\n")
        self.assertFalse((outside / "decoded.tar.gz").exists())

    def test_stale_remote_attempt_is_requeued_and_token_cleared(self):
        submitted = self.submit()
        root = queue.safe_root(self.root)
        claimed = queue.worker_claim(root, "m4mac.00")
        path = root / "running" / submitted["id"] / "manifest.json"
        claimed.update(
            {
                "started_at": "2020-01-01T00:00:00+00:00",
                "lease_token": "old-token",
                "attempt": 1,
            }
        )
        queue.atomic_json(path, claimed)
        stale_result = path.parent / "result" / "partial.txt"
        stale_result.parent.mkdir()
        stale_result.write_text("old attempt\n", encoding="utf-8")
        heartbeat = queue.load_json(root / "workers" / "m4mac.00.json")
        heartbeat["seen_at"] = "2020-01-01T00:00:00+00:00"
        queue.atomic_json(root / "workers" / "m4mac.00.json", heartbeat)
        # A healthy coordinator/other slots do not renew this exact attempt.
        queue.worker_heartbeat(root, "m4mac", slots_total=10, slots_busy=1)

        recovered = broker.recover_stale_remote_jobs(
            root,
            stale_age=300,
            now=dt.datetime(2026, 7, 22, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(recovered, [submitted["id"]])
        manifest = queue.load_json(root / "queued" / submitted["id"] / "manifest.json")
        self.assertNotIn("worker", manifest)
        self.assertNotIn("lease_token", manifest)
        self.assertEqual(manifest["attempt_history"][0]["worker"], "m4mac.00")
        self.assertEqual(
            list((root / "queued" / submitted["id"] / "result").iterdir()),
            [],
        )

    def test_queue_maintenance_blocks_stale_recovery_inside_queue_lock(self):
        submitted = self.submit()
        root = queue.safe_root(self.root)
        claimed = queue.worker_claim(root, "m4mac.00")
        claimed["started_at"] = "2020-01-01T00:00:00+00:00"
        queue.atomic_json(
            root / "running" / submitted["id"] / "manifest.json", claimed
        )
        heartbeat = queue.load_json(root / "workers" / "m4mac.00.json")
        heartbeat["seen_at"] = "2020-01-01T00:00:00+00:00"
        queue.atomic_json(root / "workers" / "m4mac.00.json", heartbeat)
        queue.set_maintenance(root, "m4mac-installer", True)

        recovered = broker.recover_stale_remote_jobs(
            root,
            stale_age=300,
            now=dt.datetime(2026, 7, 22, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(recovered, [])
        self.assertTrue((root / "running" / submitted["id"]).is_dir())

    def test_stale_recovery_crash_after_move_leaves_job_reclaimable(self):
        submitted = self.submit()
        root = queue.safe_root(self.root)
        claimed = queue.worker_claim(root, "m4mac.00")
        running_manifest = root / "running" / submitted["id"] / "manifest.json"
        claimed["started_at"] = "2020-01-01T00:00:00+00:00"
        queue.atomic_json(running_manifest, claimed)
        heartbeat = queue.load_json(root / "workers" / "m4mac.00.json")
        heartbeat["seen_at"] = "2020-01-01T00:00:00+00:00"
        queue.atomic_json(root / "workers" / "m4mac.00.json", heartbeat)
        stale_result = running_manifest.parent / "result" / "partial.txt"
        stale_result.parent.mkdir()
        stale_result.write_text("partial\n", encoding="utf-8")
        queued_manifest = root / "queued" / submitted["id"] / "manifest.json"
        original_atomic_json = queue.atomic_json

        def interrupt_manifest_publish(path, payload, mode=0o600):
            if Path(path) == queued_manifest:
                raise OSError("simulated interruption after requeue move")
            return original_atomic_json(path, payload, mode)

        with mock.patch.object(
            queue, "atomic_json", side_effect=interrupt_manifest_publish
        ):
            with self.assertRaisesRegex(OSError, "simulated interruption"):
                broker.recover_stale_remote_jobs(
                    root,
                    stale_age=300,
                    now=dt.datetime(2026, 7, 22, tzinfo=dt.timezone.utc),
                )

        interrupted = queue.load_json(queued_manifest)
        self.assertEqual(interrupted["worker"], "m4mac.00")
        self.assertFalse(stale_result.exists())
        reclaimed = queue.worker_claim(root, "m4mac.01")
        self.assertEqual(reclaimed["worker"], "m4mac.01")
        self.assertNotEqual(reclaimed["lease_token"], interrupted["lease_token"])
        self.assertTrue((root / "running" / submitted["id"]).is_dir())

    def test_stale_recovery_ignores_symlinked_running_job_directory(self):
        root = queue.safe_root(self.root)
        job_id = "20260722T000000Z-deadbeef"
        outside = Path(self.temporary.name) / "outside-running-job"
        result = outside / "result"
        result.mkdir(parents=True)
        marker = result / "marker"
        marker.write_text("keep me\n", encoding="utf-8")
        queue.atomic_json(
            outside / "manifest.json",
            {
                "id": job_id,
                "state": "running",
                "submitted_at": "2020-01-01T00:00:00+00:00",
                "started_at": "2020-01-01T00:00:00+00:00",
                "worker": "m4mac.00",
                "attempt": 1,
            },
        )
        (root / "running" / job_id).symlink_to(outside, target_is_directory=True)

        recovered = broker.recover_stale_remote_jobs(
            root,
            stale_age=300,
            now=dt.datetime(2026, 7, 22, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(recovered, [])
        self.assertEqual(marker.read_text(encoding="utf-8"), "keep me\n")
        self.assertTrue((root / "running" / job_id).is_symlink())

    def test_capacity_heartbeat_does_not_renew_abandoned_legacy_base_job(self):
        submitted = self.submit()
        root = queue.safe_root(self.root)
        claimed = queue.worker_claim(root, "m4mac")
        claimed["started_at"] = "2020-01-01T00:00:00+00:00"
        queue.atomic_json(
            root / "running" / submitted["id"] / "manifest.json", claimed
        )
        queue.worker_heartbeat(root, "m4mac", slots_total=10, slots_busy=0)

        recovered = broker.recover_stale_remote_jobs(
            root,
            stale_age=300,
            now=dt.datetime(2026, 7, 22, tzinfo=dt.timezone.utc),
        )

        self.assertEqual(recovered, [submitted["id"]])
        manifest = queue.load_json(root / "queued" / submitted["id"] / "manifest.json")
        self.assertEqual(manifest["attempt_history"][0]["worker"], "m4mac")

    def test_once_mode_obeys_singleton_lock(self):
        queue.safe_root(self.root)
        error = StringIO()
        with broker.broker_lock(self.root), redirect_stderr(error):
            code = broker.main(
                [
                    "--once",
                    "--root",
                    str(self.root),
                    "--work-root",
                    str(self.work),
                ]
            )
        self.assertEqual(code, 2)
        self.assertIn("already running", error.getvalue())

    def test_self_test_cli_checks_effective_sandbox_and_runtime(self):
        output = StringIO()
        arguments = [
            "--self-test",
            "--root",
            str(self.root),
            "--work-root",
            str(self.work),
            "--python",
            sys.executable,
            "--bwrap",
            sys.executable,
        ]
        with mock.patch.object(broker, "_read_throttled", return_value=0), mock.patch.object(
            broker,
            "bubblewrap_self_test",
            return_value=(True, "bubblewrap isolation verified"),
        ) as self_test, redirect_stdout(output):
            code = broker.main(arguments)

        self.assertEqual(code, 0)
        payload = json.loads(output.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["python"], sys.executable)
        self.assertEqual(payload["throttled_flags"], 0)
        self_test.assert_called_once_with(
            sys.executable,
            sys.executable,
            str(self.work.resolve()),
            str(self.root.resolve()),
        )

        error = StringIO()
        with mock.patch.object(broker, "_read_throttled", return_value=0), mock.patch.object(
            broker,
            "bubblewrap_self_test",
            return_value=(False, "network namespace denied"),
        ), redirect_stderr(error):
            code = broker.main(arguments)
        self.assertEqual(code, 2)
        self.assertIn("sandbox self-test failed", error.getvalue())

    def test_background_descendant_is_terminated_before_results_publish(self):
        submitted = self.submit(arguments=["--background"])

        result = self.run_once()

        self.assertTrue(result["ok"], result)
        stdout = (
            self.root / "done" / submitted["id"] / "result" / "stdout.txt"
        ).read_text(encoding="utf-8")
        child_pid = int(stdout.split("background=", 1)[1].splitlines()[0])
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                os.kill(child_pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.05)
        else:
            try:
                os.kill(child_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            self.fail("background child survived result publication")

    def test_bubblewrap_command_exposes_only_staged_job_and_runtime(self):
        base = Path(self.temporary.name)
        fake_bwrap = base / "bwrap"
        fake_bwrap.write_text("#!/bin/sh\n", encoding="utf-8")
        fake_bwrap.chmod(0o700)
        for name in ("source2", "inputs2", "result2", "home2", "tmp2", "cache2"):
            (base / name).mkdir()
        command = broker.bubblewrap_command(
            str(fake_bwrap),
            ["/usr/bin/python3", "-c", "print('ok')"],
            source_root=base / "source2",
            inputs_root=base / "inputs2",
            result_root=base / "result2",
            home_root=base / "home2",
            temporary_root=base / "tmp2",
            cache_root=base / "cache2",
            job_id="probe",
        )
        self.assertIn("--unshare-net", command)
        self.assertIn("--clearenv", command)
        self.assertNotIn("/home/pi", command)


class FrontendTests(BrokerHarness):
    def test_tasks_describe_inputs_arguments_outputs_and_datasets(self):
        listing = pi_compute._task_listing(self.source)
        local = next(task for task in listing["tasks"] if task["name"] == "local-python")
        self.assertEqual(local["minimum_inputs"], 0)
        self.assertEqual(local["maximum_inputs"], 0)
        self.assertTrue(local["accepts_arguments"])
        self.assertEqual(local["outputs"], [])
        self.assertEqual(local["datasets"], [])
        self.assertIn("{arguments}", local["argv_template"])

    def test_run_only_enqueues_and_wait_timeout_is_distinct(self):
        output = StringIO()
        with redirect_stdout(output):
            code = pi_compute.main(
                [
                    "--root",
                    str(self.root),
                    "run",
                    "local-python",
                    "--source-root",
                    str(self.source),
                    "--wait",
                    "0",
                ]
            )
        payload = json.loads(output.getvalue())
        self.assertEqual(code, 3)
        self.assertEqual(payload["state"], "queued")
        self.assertTrue(payload["wait_timed_out"])
        self.assertNotIn("lease_token", payload)
        self.assertEqual(list((self.root / "running").iterdir()), [])

    def test_agent_frontend_redacts_running_lease_capability(self):
        submitted = self.submit()
        root = queue.safe_root(self.root)
        claimed = queue.worker_claim(root, "m4mac.00")
        self.assertIn("lease_token", claimed)

        for command in (
            ["status", submitted["id"]],
            ["list"],
            ["wait", submitted["id"], "--timeout", "0"],
        ):
            with self.subTest(command=command), redirect_stdout(StringIO()) as output:
                code = pi_compute.main(["--root", str(root), *command])
                if command[0] == "wait":
                    self.assertEqual(code, 3)
                else:
                    self.assertEqual(code, 0)
                self.assertNotIn("lease_token", output.getvalue())

    def test_nonfinite_wait_is_rejected(self):
        error = StringIO()
        with redirect_stderr(error):
            code = pi_compute.main(
                [
                    "--root",
                    str(self.root),
                    "run",
                    "local-python",
                    "--source-root",
                    str(self.source),
                    "--wait",
                    "nan",
                ]
            )
        self.assertEqual(code, 2)
        self.assertIn("must be from", error.getvalue())


if __name__ == "__main__":
    unittest.main()
