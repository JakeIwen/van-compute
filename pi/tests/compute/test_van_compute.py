import argparse
from contextlib import redirect_stderr, redirect_stdout
from io import BytesIO
from io import StringIO
import json
from pathlib import Path
import re
import tarfile
import tempfile
import unittest
from unittest import mock

from macbook.scripts import van_compute_worker as worker
from pi.van_compute.scripts import van_compute as queue
from pi.van_compute.scripts import van_compute_metrics as metrics
from pi.van_compute.scripts import van_compute_protocol as protocol


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
    def test_shared_result_limit_matches_queue_and_mac_worker(self):
        self.assertEqual(queue.DEFAULT_MAX_RESULT_BYTES, protocol.MAX_RESULT_BYTES)
        self.assertEqual(worker.DEFAULT_MAX_RESULT_BYTES, protocol.MAX_RESULT_BYTES)

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

    def test_repo_manifest_builds_shell_free_dataset_command(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / protocol.REPO_MANIFEST).write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "tasks": [
                            {
                                "name": "oem-search",
                                "profile": "corpus-search",
                                "source_paths": [],
                                "minimum_inputs": 0,
                                "maximum_inputs": 0,
                                "datasets": ["oem-service-docs"],
                                "argv": [
                                    "--line-number",
                                    "{arguments}",
                                    "{dataset:oem-service-docs}",
                                ],
                                "outputs": [],
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            task = protocol.load_repo_tasks(source)["oem-search"]
            arguments = protocol.validate_task_arguments(
                task.name, ["ignition.{0,80}relay"], task
            )
            command = protocol.build_command(
                task.name,
                python="/python",
                source_root=source,
                input_paths=[],
                input_values=[],
                result_root=Path("/result"),
                arguments=arguments,
                execution=protocol.task_execution(task),
                executables={"rg": "/opt/bin/rg"},
                datasets={"oem-service-docs": "/private/oem-readonly"},
            )

        self.assertEqual(
            command,
            [
                "/opt/bin/rg",
                "--line-number",
                "ignition.{0,80}relay",
                "/private/oem-readonly",
            ],
        )

    def test_repo_manifest_rejects_unknown_fields_and_family_substitution(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / protocol.REPO_MANIFEST).write_text(
                json.dumps({"schema_version": 1, "tasks": [], "command": "adb shell"}),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(protocol.ProtocolError, "unknown field"):
                protocol.load_repo_tasks(source)

        with self.assertRaisesRegex(protocol.ProtocolError, "does not match"):
            protocol.validate_execution(
                {
                    "profile": "corpus-search",
                    "family": "python",
                    "argv": ["needle"],
                    "outputs": [],
                    "datasets": [],
                    "minimum_inputs": 0,
                    "maximum_inputs": 0,
                    "input_values": False,
                }
            )

    def test_source_roots_reject_repository_root_overlap_and_manifest_duplication(self):
        base = {
            "name": "repo-tests",
            "profile": "repo-test",
            "source_paths": ["tests"],
            "minimum_inputs": 0,
            "maximum_inputs": 0,
            "argv": ["-q", "tests"],
            "outputs": [],
        }
        for source_paths, message in (
            (["."], "repository root"),
            (["tools", "tools/analyze.py"], "ancestor-overlapping"),
            ([protocol.REPO_MANIFEST], "snapshotted automatically"),
        ):
            with self.subTest(source_paths=source_paths):
                with self.assertRaisesRegex(protocol.ProtocolError, message):
                    protocol._parse_repo_task(
                        dict(base, source_paths=source_paths),
                        0,
                    )

    def test_embedded_python_execution_repeats_profile_shape_validation(self):
        with self.assertRaisesRegex(protocol.ProtocolError, "Python source placeholder"):
            protocol.validate_execution(
                {
                    "profile": "python-script",
                    "family": "python",
                    "argv": ["-c", "print('escaped source policy')"],
                    "outputs": [],
                    "datasets": [],
                    "minimum_inputs": 0,
                    "maximum_inputs": 0,
                    "input_values": False,
                }
            )

    def test_dynamic_profiles_reject_command_helpers_and_path_escapes(self):
        corpus = protocol.RepoTaskDefinition(
            name="search",
            description="search",
            profile="corpus-search",
            family="rg",
            source_paths=(),
            minimum_inputs=0,
            maximum_inputs=0,
            argv=("{arguments}",),
            outputs=(),
            datasets=(),
        )
        sqlite = protocol.RepoTaskDefinition(
            name="database",
            description="database",
            profile="sqlite-readonly",
            family="sqlite3",
            source_paths=(),
            minimum_inputs=1,
            maximum_inputs=1,
            argv=("{input:0}", "{arguments}"),
            outputs=(),
            datasets=(),
        )

        with self.assertRaisesRegex(protocol.ProtocolError, "command-execution"):
            protocol.validate_task_arguments("search", ["--pre=systemctl"], corpus)
        with self.assertRaisesRegex(protocol.ProtocolError, "SELECT, WITH, or EXPLAIN"):
            protocol.validate_task_arguments("database", ["ATTACH 'other.db' AS x"], sqlite)
        with self.assertRaisesRegex(protocol.ProtocolError, "outside the staged job"):
            protocol.validate_task_arguments("search", ["--config=/tmp/rg.conf"], corpus)

    def test_sqlite_readonly_has_safe_fixed_shape_and_one_query(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            declaration = {
                "schema_version": 1,
                "tasks": [
                    {
                        "name": "database-query",
                        "profile": "sqlite-readonly",
                        "source_paths": [],
                        "minimum_inputs": 1,
                        "maximum_inputs": 1,
                        "argv": ["-json", "{input:0}", "{arguments}"],
                        "outputs": [],
                    }
                ],
            }
            (source / protocol.REPO_MANIFEST).write_text(
                json.dumps(declaration), encoding="utf-8"
            )
            task = protocol.load_repo_tasks(source)["database-query"]
            query = "WITH ids AS (SELECT id FROM modules) SELECT * FROM ids"
            arguments = protocol.validate_task_arguments(task.name, [query], task)
            command = protocol.build_command(
                task.name,
                python="/python",
                source_root=source,
                input_paths=[Path("/inputs/alfa.db")],
                input_values=[None],
                result_root=Path("/result"),
                arguments=arguments,
                execution=protocol.task_execution(task),
                executables={"sqlite3": "/usr/bin/sqlite3"},
            )

        self.assertEqual(
            command,
            [
                "/usr/bin/sqlite3",
                "-safe",
                "-readonly",
                "-batch",
                "-json",
                "/inputs/alfa.db",
                query,
            ],
        )
        for arguments, message in (
            (["-cmd"], "option-like"),
            (["SELECT 1", "SELECT 2"], "exactly one"),
            (["DELETE FROM modules"], "SELECT, WITH, or EXPLAIN"),
            (["SELECT 1; SELECT 2"], "exactly one SQL statement"),
        ):
            with self.subTest(arguments=arguments):
                with self.assertRaisesRegex(protocol.ProtocolError, message):
                    protocol.validate_task_arguments(task.name, arguments, task)

    def test_sqlite_manifest_options_and_dynamic_output_roots_are_strict(self):
        base = {
            "name": "database-query",
            "profile": "sqlite-readonly",
            "source_paths": [],
            "minimum_inputs": 1,
            "maximum_inputs": 1,
            "argv": ["-json", "{input:0}", "{arguments}"],
            "outputs": [],
        }
        for argv, message in (
            (["-cmd", "{input:0}", "{arguments}"], "safe output-format"),
            (["{input:0}", "-json", "{arguments}"], "must end"),
            (["-unsafe-testing", "{input:0}", "{arguments}"], "safe output-format"),
        ):
            with self.subTest(argv=argv):
                payload = dict(base, argv=argv)
                with self.assertRaisesRegex(protocol.ProtocolError, message):
                    protocol._parse_repo_task(payload, 0)

        fixed = protocol._parse_repo_task(
            dict(base, argv=["-json", "{input:0}", "SELECT count(*) FROM modules"]),
            0,
        )
        self.assertEqual(protocol.validate_task_arguments(fixed.name, [], fixed), ())
        fixed_command = protocol.build_command(
            fixed.name,
            python="/python",
            source_root=Path("/source"),
            input_paths=[Path("/inputs/alfa.db")],
            input_values=[None],
            result_root=Path("/result"),
            arguments=[],
            execution=protocol.task_execution(fixed),
            executables={"sqlite3": "/sqlite3"},
        )
        self.assertEqual(fixed_command[-1], "SELECT count(*) FROM modules")

        apk = {
            "name": "apk-tree",
            "profile": "apk-analyze",
            "source_paths": [],
            "minimum_inputs": 1,
            "maximum_inputs": 1,
            "argv": ["-d", "{result:jadx}", "{input:0}"],
            "outputs": ["jadx", "jadx/report.json"],
        }
        with self.assertRaisesRegex(protocol.ProtocolError, "ancestor-overlapping"):
            protocol._parse_repo_task(apk, 0)
        with self.assertRaisesRegex(protocol.ProtocolError, "portable"):
            protocol._parse_repo_task(dict(apk, outputs=["bad output"]), 0)
        with self.assertRaisesRegex(protocol.ProtocolError, "implicit worker result"):
            protocol._parse_repo_task(dict(apk, outputs=["stdout.txt"]), 0)


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
        self.assertEqual(claimed["placement"], "remote")
        self.assertEqual(claimed["attempt"], 1)
        self.assertRegex(claimed["lease_token"], r"[0-9a-f]{32}")
        lease_token = claimed["lease_token"]

        # Appending after submission is safe: the stream remains bounded to the
        # original byte length recorded in the manifest.
        with self.capture.open("ab") as handle:
            handle.write(b"later bytes")
        streamed = BytesIO()
        queue.stream_bounded_input(claimed, 0, streamed, lease_token)
        self.assertEqual(streamed.getvalue(), b"first capture bytes\n")

        uploaded = queue.put_result(
            queue.safe_root(self.queue_root),
            job_id,
            "m4mac",
            "stdout.txt",
            BytesIO(b"ok\n"),
            lease_token,
        )
        self.assertEqual(uploaded["size"], 3)
        finished = queue.worker_finish(
            queue.safe_root(self.queue_root),
            job_id,
            "m4mac",
            0,
            ["stdout.txt"],
            lease_token,
        )
        self.assertEqual(finished["state"], "done")
        self.assertNotIn("lease_token", finished)
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
        self.assertEqual(second["lease_token"], first["lease_token"])
        self.assertEqual(second["attempt"], first["attempt"])

    def test_remote_claim_crash_before_move_leaves_job_reclaimable(self):
        submitted = self.submit()
        root = queue.safe_root(self.queue_root)
        source = root / "queued" / submitted["id"]
        destination = root / "running" / submitted["id"]
        original_replace = queue.os.replace

        def interrupt_job_move(old, new):
            if Path(old) == source and Path(new) == destination:
                raise OSError("simulated interruption before job move")
            return original_replace(old, new)

        with mock.patch.object(queue.os, "replace", side_effect=interrupt_job_move):
            with self.assertRaisesRegex(OSError, "simulated interruption"):
                queue.worker_claim(root, "m4mac.00")

        interrupted = queue.load_json(source / "manifest.json")
        self.assertEqual(interrupted["state"], "running")
        self.assertEqual(interrupted["worker"], "m4mac.00")
        self.assertFalse(destination.exists())

        reclaimed = queue.worker_claim(root, "m4mac.01")
        self.assertEqual(reclaimed["id"], submitted["id"])
        self.assertEqual(reclaimed["worker"], "m4mac.01")
        self.assertNotEqual(reclaimed["lease_token"], interrupted["lease_token"])
        self.assertTrue(destination.is_dir())

    def test_queue_maintenance_blocks_claims_until_owner_exits(self):
        submitted = self.submit()
        root = queue.safe_root(self.queue_root)
        entered = queue.set_maintenance(root, "m4mac-installer", True)

        self.assertTrue(entered["active"])
        self.assertIsNone(queue.worker_claim(root, "m4mac.00"))
        self.assertTrue((root / "queued" / submitted["id"]).is_dir())
        with self.assertRaisesRegex(queue.QueueError, "maintenance"):
            self.submit()
        with self.assertRaisesRegex(queue.QueueError, "belongs to"):
            queue.set_maintenance(root, "other-installer", False)

        queue.set_maintenance(root, "m4mac-installer", False)
        claimed = queue.worker_claim(root, "m4mac.00")
        self.assertEqual(claimed["id"], submitted["id"])

    def test_claim_lease_rejects_missing_wrong_or_stale_owners(self):
        submitted = self.submit()
        root = queue.safe_root(self.queue_root)
        claimed = queue.worker_claim(root, "m4mac.00")
        token = claimed["lease_token"]
        wrong = "0" * 32 if token != "0" * 32 else "1" * 32

        with self.assertRaisesRegex(queue.QueueError, "required"):
            queue.require_running_job(root, submitted["id"], "m4mac.00")
        with self.assertRaisesRegex(
            queue.QueueError, "does not own|different worker"
        ):
            queue.require_running_job(root, submitted["id"], "m4mac.00", wrong)
        with self.assertRaisesRegex(queue.QueueError, "required"):
            queue.stream_bounded_input(claimed, 0, BytesIO())
        running, manifest = queue.require_running_job(
            root, submitted["id"], "m4mac.00", token
        )
        self.assertEqual(running.name, submitted["id"])
        self.assertEqual(manifest["lease_token"], token)

    def test_consumer_cli_redacts_lease_but_worker_claim_keeps_it(self):
        submitted = self.submit()
        root = queue.safe_root(self.queue_root)
        claimed = queue.worker_claim(root, "m4mac.00")

        for command in (
            ["status", submitted["id"]],
            ["list"],
            ["wait", submitted["id"], "--timeout", "0"],
        ):
            with self.subTest(command=command), redirect_stdout(StringIO()) as output:
                result = queue.main(["--root", str(root), *command])
                self.assertEqual(result, 0)
                self.assertNotIn("lease_token", output.getvalue())

        with redirect_stdout(StringIO()) as output:
            result = queue.main(
                [
                    "--root",
                    str(root),
                    "worker",
                    "claim",
                    "--worker",
                    "m4mac.00",
                    "--protocol-version",
                    str(protocol.WORKER_PROTOCOL_VERSION),
                ]
            )
        self.assertEqual(result, 0)
        worker_payload = json.loads(output.getvalue())
        self.assertEqual(
            worker_payload["job"]["lease_token"], claimed["lease_token"]
        )

    def test_input_prefix_edit_after_submission_is_rejected(self):
        submitted = self.submit()
        root = queue.safe_root(self.queue_root)
        claimed = queue.worker_claim(root, "m4mac.00")
        original = self.capture.read_bytes()
        self.capture.write_bytes(b"X" * len(original))

        with self.assertRaisesRegex(queue.QueueError, "prefix changed"):
            queue.stream_bounded_input(
                claimed,
                0,
                BytesIO(),
                claimed["lease_token"],
            )

        self.assertEqual(submitted["id"], claimed["id"])

    def test_stale_upload_cannot_pollute_reassigned_attempt(self):
        submitted = self.submit()
        root = queue.safe_root(self.queue_root)
        claimed = queue.worker_claim(root, "m4mac.00")

        class ReassigningStream(BytesIO):
            reassigned = None

            def read(stream_self, size=-1):
                if stream_self.reassigned is None:
                    with queue.queue_lock(root):
                        running = root / "running" / submitted["id"]
                        manifest = queue.load_json(running / "manifest.json")
                        manifest.update({"state": "queued", "requeued_at": queue.utc_now()})
                        for field in ("worker", "lease_token", "placement", "started_at"):
                            manifest.pop(field, None)
                        queue.atomic_json(running / "manifest.json", manifest)
                        running.replace(root / "queued" / submitted["id"])
                    stream_self.reassigned = queue.worker_claim(root, "m4mac.01")
                return super().read(size)

        stream = ReassigningStream(b"stale result\n")
        with self.assertRaisesRegex(
            queue.QueueError, "does not own|different worker"
        ):
            queue.put_result(
                root,
                submitted["id"],
                "m4mac.00",
                "stdout.txt",
                stream,
                claimed["lease_token"],
            )

        destination = root / "running" / submitted["id"] / "result" / "stdout.txt"
        self.assertFalse(destination.exists())
        self.assertEqual(stream.reassigned["worker"], "m4mac.01")
        self.assertFalse(any((root / "uploads").iterdir()))

    def test_result_upload_rejects_symlinked_result_root(self):
        submitted = self.submit()
        root = queue.safe_root(self.queue_root)
        claimed = queue.worker_claim(root, "m4mac.00")
        victim = Path(self.temporary.name) / "victim-results"
        victim.mkdir()
        result_root = root / "running" / submitted["id"] / "result"
        result_root.symlink_to(victim, target_is_directory=True)

        with self.assertRaisesRegex(queue.QueueError, "not a real directory"):
            queue.put_result(
                root,
                submitted["id"],
                "m4mac.00",
                "stdout.txt",
                BytesIO(b"must not escape\n"),
                claimed["lease_token"],
            )

        self.assertEqual(list(victim.iterdir()), [])
        self.assertFalse(any((root / "uploads").iterdir()))

    def test_old_tokenless_running_jobs_remain_compatible(self):
        submitted = self.submit()
        root = queue.safe_root(self.queue_root)
        claimed = queue.worker_claim(root, "legacy-worker")
        running = root / "running" / submitted["id"]
        manifest = queue.load_json(running / "manifest.json")
        manifest.pop("lease_token")
        queue.atomic_json(running / "manifest.json", manifest)

        queue.require_running_job(root, submitted["id"], "legacy-worker")
        queue.put_result(
            root,
            submitted["id"],
            "legacy-worker",
            "stdout.txt",
            BytesIO(b"legacy\n"),
        )
        finished = queue.worker_finish(
            root,
            submitted["id"],
            "legacy-worker",
            0,
            ["stdout.txt"],
        )

        self.assertEqual(finished["state"], "done")
        self.assertNotIn("lease_token", finished)

    def test_resuming_old_tokenless_job_issues_new_lease(self):
        submitted = self.submit()
        root = queue.safe_root(self.queue_root)
        first = queue.worker_claim(root, "legacy-worker")
        running = root / "running" / submitted["id"]
        manifest = queue.load_json(running / "manifest.json")
        manifest.pop("lease_token")
        queue.atomic_json(running / "manifest.json", manifest)

        resumed = queue.worker_claim(root, "legacy-worker")

        self.assertRegex(resumed["lease_token"], r"[0-9a-f]{32}")
        self.assertEqual(resumed["attempt"], first["attempt"])

    def test_source_bundle_is_normalized_verified_uncompressed_tar(self):
        submitted = self.submit()
        root = queue.safe_root(self.queue_root)
        claimed = queue.worker_claim(root, "m4mac.00")
        archive = BytesIO()

        queue.stream_source_bundle(
            root / "running" / submitted["id"],
            claimed,
            archive,
            claimed["lease_token"],
        )

        self.assertNotEqual(archive.getvalue()[:2], b"\x1f\x8b")
        archive.seek(0)
        with tarfile.open(fileobj=archive, mode="r:") as bundle:
            members = bundle.getmembers()
            self.assertEqual(
                [member.name for member in members],
                ["tools/can_capture_summary.py"],
            )
            member = members[0]
            self.assertTrue(member.isfile())
            self.assertEqual(member.uid, 0)
            self.assertEqual(member.gid, 0)
            self.assertEqual(member.uname, "")
            self.assertEqual(member.gname, "")
            self.assertEqual(member.mtime, 0)
            self.assertEqual(member.mode, 0o600)
            extracted = bundle.extractfile(member)
            self.assertIsNotNone(extracted)
            self.assertEqual(extracted.read().decode(), SUMMARY_STUB)

    def test_source_bundle_refuses_replaced_symlink(self):
        submitted = self.submit()
        root = queue.safe_root(self.queue_root)
        claimed = queue.worker_claim(root, "m4mac.00")
        source = (
            root
            / "running"
            / submitted["id"]
            / "source"
            / "tools"
            / "can_capture_summary.py"
        )
        source.unlink()
        source.symlink_to(self.capture)

        with self.assertRaisesRegex(queue.QueueError, "declared regular file"):
            queue.stream_source_bundle(
                root / "running" / submitted["id"],
                claimed,
                BytesIO(),
                claimed["lease_token"],
            )

    def test_input_stream_refuses_symlink_replacement(self):
        self.submit()
        root = queue.safe_root(self.queue_root)
        claimed = queue.worker_claim(root, "m4mac.00")
        replacement = self.source_root / "tmp" / "captures" / "replacement.log"
        replacement.write_bytes(b"first capture bytes\n")
        self.capture.unlink()
        self.capture.symlink_to(replacement)

        with self.assertRaisesRegex(queue.QueueError, "safely open input"):
            queue.stream_bounded_input(
                claimed,
                0,
                BytesIO(),
                claimed["lease_token"],
            )

    def test_concurrent_slots_claim_distinct_jobs_and_resume_only_their_own(self):
        first_submitted = self.submit()
        second_submitted = self.submit()
        root = queue.safe_root(self.queue_root)

        slot_zero = queue.worker_claim(root, "m4mac.00")
        slot_one = queue.worker_claim(root, "m4mac.01")
        slot_zero_resumed = queue.worker_claim(root, "m4mac.00")

        self.assertEqual(
            {slot_zero["id"], slot_one["id"]},
            {first_submitted["id"], second_submitted["id"]},
        )
        self.assertNotEqual(slot_zero["id"], slot_one["id"])
        self.assertEqual(slot_zero_resumed["id"], slot_zero["id"])
        self.assertEqual(slot_zero_resumed["worker"], "m4mac.00")
        self.assertNotEqual(slot_zero["lease_token"], slot_one["lease_token"])

    def test_new_claim_increments_attempt_and_records_pi_local_placement(self):
        submitted = self.submit()
        root = queue.safe_root(self.queue_root)
        queued = root / "queued" / submitted["id"]
        manifest = queue.load_json(queued / "manifest.json")
        manifest["attempt"] = 4
        queue.atomic_json(queued / "manifest.json", manifest)

        claimed = queue.worker_claim(root, "vanpi-local.00", placement="pi-local")

        self.assertEqual(claimed["attempt"], 5)
        self.assertEqual(claimed["placement"], "pi-local")
        self.assertTrue(re.fullmatch(r"[0-9a-f]{32}", claimed["lease_token"]))

    def test_repo_task_submission_embeds_execution_and_snapshots_policy(self):
        tests = self.source_root / "tests"
        tests.mkdir()
        (tests / "test_offline.py").write_text("def test_ok(): assert True\n", encoding="utf-8")
        declaration = {
            "schema_version": 1,
            "tasks": [
                {
                    "name": "repo-tests",
                    "profile": "repo-test",
                    "source_paths": ["tests"],
                    "minimum_inputs": 0,
                    "maximum_inputs": 0,
                    "argv": ["-q", "tests", "{arguments}"],
                    "outputs": [],
                }
            ],
        }
        (self.source_root / protocol.REPO_MANIFEST).write_text(
            json.dumps(declaration), encoding="utf-8"
        )
        args = argparse.Namespace(
            root=self.queue_root,
            source_root=self.source_root,
            task="repo-tests",
            argument=["-k", "ok.{0,2}"],
            input=[],
            input_value=None,
        )

        submitted = queue.submit_job(args)
        queued = self.queue_root / "queued" / submitted["id"]

        self.assertEqual(submitted["execution"]["profile"], "repo-test")
        self.assertEqual(submitted["arguments"], ["-k", "ok.{0,2}"])
        self.assertEqual(
            json.loads((queued / "source" / protocol.REPO_MANIFEST).read_text()),
            declaration,
        )
        self.assertTrue((queued / "source" / "tests" / "test_offline.py").is_file())
        self.assertEqual(
            [
                item["path"]
                for item in submitted["sources"]
                if item["path"] == protocol.REPO_MANIFEST
            ],
            [protocol.REPO_MANIFEST],
        )

    def test_source_snapshot_file_limit_fails_atomically(self):
        (self.source_root / "tools" / "second.py").write_text(
            "print('second')\n", encoding="utf-8"
        )
        declaration = {
            "schema_version": 1,
            "tasks": [
                {
                    "name": "repo-tests",
                    "profile": "repo-test",
                    "source_paths": ["tools"],
                    "minimum_inputs": 0,
                    "maximum_inputs": 0,
                    "argv": ["-q"],
                    "outputs": [],
                }
            ],
        }
        (self.source_root / protocol.REPO_MANIFEST).write_text(
            json.dumps(declaration), encoding="utf-8"
        )
        args = argparse.Namespace(
            root=self.queue_root,
            source_root=self.source_root,
            task="repo-tests",
            argument=[],
            input=[],
            input_value=None,
        )
        original_limit = queue.MAX_SNAPSHOT_FILES
        queue.MAX_SNAPSHOT_FILES = 2
        try:
            with self.assertRaisesRegex(queue.QueueError, "file total limit"):
                queue.submit_job(args)
        finally:
            queue.MAX_SNAPSHOT_FILES = original_limit

        self.assertEqual(list((self.queue_root / "queued").iterdir()), [])

    def test_source_snapshot_byte_limit_fails_atomically(self):
        args = argparse.Namespace(
            root=self.queue_root,
            source_root=self.source_root,
            task="can-capture-summary",
            argument=[],
            input=[str(self.capture)],
            input_value=None,
        )
        original_limit = queue.MAX_SNAPSHOT_BYTES
        queue.MAX_SNAPSHOT_BYTES = len(SUMMARY_STUB.encode()) - 1
        try:
            with self.assertRaisesRegex(queue.QueueError, "byte total limit"):
                queue.submit_job(args)
        finally:
            queue.MAX_SNAPSHOT_BYTES = original_limit

        self.assertEqual(list((self.queue_root / "queued").iterdir()), [])

    def test_missed_offload_events_are_bounded_and_do_not_follow_symlinks(self):
        root = queue.safe_root(self.queue_root)
        original_limit = queue.MAX_MISSED_EVENTS
        queue.MAX_MISSED_EVENTS = 2
        try:
            for index in range(3):
                args = argparse.Namespace(
                    profile="repo-test",
                    label=f"test run {index}",
                    reason="worker-unavailable",
                    duration_seconds=10.0 + index,
                    cpu_seconds=20.0 + index,
                    peak_rss_bytes=1000 + index,
                    input_bytes=2000 + index,
                )
                queue.record_missed_offload(root, args)
            target = root / "missed" / "not-an-event.json"
            target.write_text("{}", encoding="utf-8")
            link = root / "missed" / "20200101T000000Z-deadbeef.json"
            link.symlink_to(target)
            queue.record_missed_offload(root, args)
        finally:
            queue.MAX_MISSED_EVENTS = original_limit

        events = [
            path
            for path in (root / "missed").glob("*.json")
            if path.name not in {target.name, link.name}
        ]
        self.assertEqual(len(events), 2)
        self.assertTrue(link.is_symlink())
        payload = json.loads(events[-1].read_text(encoding="utf-8"))
        self.assertEqual(payload["reason"], "worker-unavailable")
        self.assertIn("cpu_seconds", payload)

    def test_queue_refuses_symlinked_missed_event_directory(self):
        root = Path(self.temporary.name) / "unsafe-compute"
        outside = Path(self.temporary.name) / "outside-events"
        root.mkdir()
        outside.mkdir()
        (root / "missed").symlink_to(outside, target_is_directory=True)

        with self.assertRaisesRegex(queue.QueueError, "not a real directory"):
            queue.safe_root(root)

    def test_coordinator_heartbeat_reports_validated_slot_capacity(self):
        root = queue.safe_root(self.queue_root)

        heartbeat = queue.worker_heartbeat(root, "m4mac", slots_total=10, slots_busy=3)

        self.assertEqual(
            heartbeat["protocol_version"], protocol.WORKER_PROTOCOL_VERSION
        )
        self.assertEqual(heartbeat["slots_total"], 10)
        self.assertEqual(heartbeat["slots_busy"], 3)
        with self.assertRaisesRegex(queue.QueueError, "supplied together"):
            queue.worker_heartbeat(root, "m4mac", slots_total=10)
        with self.assertRaisesRegex(queue.QueueError, "0 <="):
            queue.worker_heartbeat(root, "m4mac", slots_total=10, slots_busy=11)

    def test_incompatible_worker_cannot_refresh_or_claim(self):
        submitted = self.submit()
        root = queue.safe_root(self.queue_root)
        heartbeat = queue.worker_heartbeat(root, "m4mac.00")
        heartbeat_path = root / "workers" / "m4mac.00.json"
        incompatible = protocol.WORKER_PROTOCOL_VERSION + 1

        with self.assertRaisesRegex(queue.QueueError, "incompatible worker protocol"):
            queue.worker_heartbeat(
                root,
                "m4mac.00",
                protocol_version=incompatible,
            )
        self.assertEqual(queue.load_json(heartbeat_path), heartbeat)

        with self.assertRaisesRegex(queue.QueueError, "incompatible worker protocol"):
            queue.worker_claim(
                root,
                "m4mac.00",
                protocol_version=incompatible,
            )
        self.assertTrue((root / "queued" / submitted["id"]).is_dir())
        self.assertFalse((root / "running" / submitted["id"]).exists())
        self.assertEqual(queue.load_json(heartbeat_path), heartbeat)

    def test_worker_cli_requires_protocol_version(self):
        root = queue.safe_root(self.queue_root)
        with redirect_stderr(StringIO()), self.assertRaises(SystemExit) as raised:
            queue.main(
                [
                    "--root",
                    str(root),
                    "worker",
                    "heartbeat",
                    "--worker",
                    "legacy-worker",
                ]
            )
        self.assertEqual(raised.exception.code, 2)
        self.assertFalse((root / "workers" / "legacy-worker.json").exists())

    def test_available_marks_incompatible_heartbeats_unavailable(self):
        root = queue.safe_root(self.queue_root)
        incompatible_versions = (
            None,
            True,
            str(protocol.WORKER_PROTOCOL_VERSION),
            protocol.WORKER_PROTOCOL_VERSION + 1,
        )
        for index, incompatible in enumerate(incompatible_versions):
            worker = f"legacy-worker-{index}"
            heartbeat = queue.worker_heartbeat(root, worker)
            if incompatible is None:
                heartbeat.pop("protocol_version")
            else:
                heartbeat["protocol_version"] = incompatible
            queue.atomic_json(root / "workers" / f"{worker}.json", heartbeat)

        workers = queue.workers_available(root, 60)

        self.assertEqual(len(workers), len(incompatible_versions))
        self.assertTrue(all(not worker["available"] for worker in workers))

    def test_queue_wait_and_worker_age_reject_nonfinite_floats(self):
        submitted = self.submit()
        root = queue.safe_root(self.queue_root)

        for value in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(operation="available", value=value):
                with self.assertRaisesRegex(queue.QueueError, "finite positive"):
                    queue.workers_available(root, value)
            with self.subTest(operation="wait", value=value):
                with self.assertRaisesRegex(queue.QueueError, "finite non-negative"):
                    queue.wait_for_job(root, submitted["id"], value)

    def test_cli_rejects_nonfinite_wait_before_submitting(self):
        stdout = StringIO()
        stderr = StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            result = queue.main(
                [
                    "--root",
                    str(self.queue_root),
                    "submit",
                    "can-capture-summary",
                    "--source-root",
                    str(self.source_root),
                    "--input",
                    str(self.capture),
                    "--arg=--snapshot",
                    "--wait",
                    "nan",
                ]
            )

        self.assertEqual(result, 2)
        self.assertIn("finite non-negative", stderr.getvalue())
        self.assertEqual(list((self.queue_root / "queued").iterdir()), [])

    def test_cli_rejects_nonfinite_worker_max_age(self):
        with redirect_stdout(StringIO()), redirect_stderr(StringIO()) as stderr:
            result = queue.main(
                [
                    "--root",
                    str(self.queue_root),
                    "available",
                    "--max-age",
                    "inf",
                ]
            )

        self.assertEqual(result, 2)
        self.assertIn("finite positive", stderr.getvalue())

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

    def test_linux_opened_fd_verification_rejects_ancestor_escape(self):
        source_root = self.source_root.resolve()
        outside = Path(self.temporary.name) / "outside.log"
        outside.write_text("private", encoding="utf-8")
        original_readlink = queue.os.readlink

        def escaped_proc_target(path, *args, **kwargs):
            if str(path).startswith("/proc/self/fd/"):
                return str(outside)
            return original_readlink(path, *args, **kwargs)

        with mock.patch.object(queue.sys, "platform", "linux"), mock.patch.object(
            queue.os, "readlink", side_effect=escaped_proc_target
        ):
            with self.assertRaisesRegex(queue.QueueError, "escaped source root"):
                queue.input_record(str(self.capture), None, source_root, 0)

            destination = Path(self.temporary.name) / "source-copy.py"
            with self.assertRaisesRegex(queue.QueueError, "escaped source root"):
                queue.copy_source_file(
                    self.source_root / "tools" / "can_capture_summary.py",
                    destination,
                    source_root=source_root,
                    maximum_bytes=queue.MAX_SNAPSHOT_BYTES,
                )
            self.assertFalse(destination.exists())

    def test_linux_opened_fd_verification_fails_closed_without_proc(self):
        source_root = self.source_root.resolve()
        original_readlink = queue.os.readlink

        def missing_proc(path, *args, **kwargs):
            if str(path).startswith("/proc/self/fd/"):
                raise OSError("proc unavailable")
            return original_readlink(path, *args, **kwargs)

        with mock.patch.object(queue.sys, "platform", "linux"), mock.patch.object(
            queue.os, "readlink", side_effect=missing_proc
        ):
            with self.assertRaisesRegex(queue.QueueError, "cannot verify opened input"):
                queue.input_record(str(self.capture), None, source_root, 0)


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
            with mock.patch.object(
                worker, "process_group_resources", return_value=(0, 1)
            ):
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

    def test_metrics_worker_protocol_version_matches_queue_protocol(self):
        self.assertEqual(
            metrics.WORKER_PROTOCOL_VERSION,
            protocol.WORKER_PROTOCOL_VERSION,
        )

    @staticmethod
    def write_json(path, payload):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def write_completed_metric_job(
        self,
        root,
        job_id,
        *,
        placement,
        analysis_seconds,
        cpu_seconds,
        peak_rss_bytes,
        finished_at="2026-07-22T01:59:00+00:00",
        task="can-capture-summary",
        arguments=None,
        source_digest=None,
        input_digest=None,
        input_value=None,
        include_source=True,
        datasets=None,
        state="done",
        exit_code=0,
        telemetry=True,
        analysis_telemetry=True,
    ):
        source_digest = source_digest or "a" * 64
        input_digest = input_digest or "b" * 64
        job = root / state / job_id
        manifest = {
            "schema_version": 1,
            "id": job_id,
            "task": task,
            "state": state,
            "worker": "vanpi-local.00" if placement == "pi-local" else "m4mac.00",
            "placement": placement,
            "exit_code": exit_code,
            "submitted_at": "2026-07-22T01:58:00+00:00",
            "started_at": "2026-07-22T01:58:30+00:00",
            "finished_at": finished_at,
            "arguments": list(["--snapshot"] if arguments is None else arguments),
            "sources": (
                [
                    {
                        "path": "tools/private-analysis.py",
                        "size": 123,
                        "sha256": source_digest,
                    }
                ]
                if include_source
                else []
            ),
            "inputs": [
                {
                    "index": 0,
                    "name": "private-capture-name.log",
                    "size": 456,
                    "sha256": input_digest,
                    "value": input_value,
                }
            ],
            "results": [],
        }
        if datasets is not None:
            manifest["execution"] = {"datasets": list(datasets)}
        self.write_json(job / "manifest.json", manifest)
        execution = {"placement": placement}
        if analysis_telemetry:
            execution.update(
                {
                    "timing": {"analysis_seconds": analysis_seconds},
                    "duration_seconds": analysis_seconds,
                }
            )
        if telemetry:
            execution["resource_usage"] = {
                "cpu_seconds": cpu_seconds,
                "peak_rss_bytes": peak_rss_bytes,
            }
        self.write_json(job / "result" / "execution.json", execution)

    def test_aggregates_worker_resources_and_current_queue(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "compute"
            self.write_json(
                root / "workers" / "m4mac.json",
                {
                    "worker": "m4mac",
                    "seen_at": "2026-07-22T01:59:50+00:00",
                    "protocol_version": protocol.WORKER_PROTOCOL_VERSION,
                    "slots_total": 10,
                    "slots_busy": 3,
                },
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
                        "peak_process_group_rss_bytes": 300 * 1024 * 1024,
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
            self.write_json(
                root / "missed" / "20260722T005200Z-tests.json",
                {
                    "id": "20260722T005200Z-tests",
                    "recorded_at": "2026-07-22T00:52:00+00:00",
                    "profile": "repo-test",
                    "label": "Full repository tests",
                    "reason": "unsupported",
                    "duration_seconds": 20.0,
                    "cpu_seconds": 32.5,
                    "peak_rss_bytes": 128 * 1024 * 1024,
                    "input_bytes": 3_000,
                },
            )
            self.write_json(
                root / "missed" / "20260722T005100Z-search.json",
                {
                    "id": "20260722T005100Z-search",
                    "recorded_at": "2026-07-22T00:51:00+00:00",
                    "profile": "corpus-search",
                    "label": "Search service documentation",
                    "reason": "worker-unavailable",
                    "duration_seconds": 5.5,
                    "cpu_seconds": 4.0,
                    "peak_rss_bytes": 32 * 1024 * 1024,
                    "input_bytes": 7_000,
                },
            )
            # Bad records and unsafe filesystem entries are ignored rather than
            # making the read-only dashboard endpoint unavailable.
            (root / "missed" / "bad.json").write_text("{", encoding="utf-8")
            (root / "missed" / "link.json").symlink_to(
                root / "missed" / "20260722T005200Z-tests.json"
            )

            report = metrics.ComputeMetricsReader(root, clock=lambda: self.NOW).report(168)

        self.assertTrue(report["status"]["available"])
        self.assertEqual(report["status"]["queued"], 1)
        self.assertEqual(report["status"]["slots_total"], 10)
        self.assertEqual(report["status"]["slots_busy"], 3)
        self.assertEqual(report["status"]["slots_available"], 7)
        self.assertEqual(report["status"]["workers"][0]["slots_total"], 10)
        self.assertEqual(
            report["status"]["workers"][0]["protocol_version"],
            protocol.WORKER_PROTOCOL_VERSION,
        )
        self.assertEqual(report["summary"]["jobs"], 1)
        self.assertEqual(report["summary"]["telemetry_jobs"], 1)
        self.assertEqual(report["summary"]["mac_cpu_seconds"], 6.0)
        self.assertEqual(report["summary"]["mac_wall_seconds"], 4.0)
        self.assertEqual(report["summary"]["aggregate_cpu_percent"], 150.0)
        self.assertEqual(report["summary"]["peak_rss_bytes"], 300 * 1024 * 1024)
        done_job = next(job for job in report["jobs"] if job["state"] == "done")
        self.assertEqual(
            done_job["sampled_process_group_peak_rss_bytes"],
            300 * 1024 * 1024,
        )
        self.assertEqual(report["summary"]["input_bytes"], 10_000)
        self.assertEqual(report["tasks"][0]["task"], "can-capture-summary")
        self.assertEqual(report["tasks"][0]["telemetry_jobs"], 1)
        self.assertEqual([job["state"] for job in report["jobs"]], ["queued", "done"])
        local = report["eligible_local_work"]
        self.assertEqual(local["events"], 2)
        self.assertEqual(local["cpu_seconds"], 36.5)
        self.assertEqual(local["wall_seconds"], 25.5)
        self.assertEqual(local["peak_rss_bytes"], 128 * 1024 * 1024)
        self.assertEqual(local["input_bytes"], 10_000)
        self.assertEqual(
            [category["command_category"] for category in local["categories"]],
            ["repo-test", "corpus-search"],
        )
        self.assertEqual(
            [reason["reason"] for reason in local["reasons"]],
            ["unsupported", "worker-unavailable"],
        )
        self.assertIn("process-group aggregate", report["measurement_note"])
        self.assertIn("not exhaustive", report["measurement_note"])

    def test_current_jobs_are_not_displaced_by_newer_completion_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "compute"
            queued = root / "queued" / "20200101T000000Z-00000001"
            self.write_json(
                queued / "manifest.json",
                {
                    "id": queued.name,
                    "task": "old-queued-job",
                    "state": "queued",
                    "submitted_at": "2020-01-01T00:00:00+00:00",
                },
            )
            for index in range(4):
                job_id = f"20260722T0159{index:02d}Z-{index:08x}"
                done = root / "done" / job_id
                self.write_json(
                    done / "manifest.json",
                    {
                        "id": job_id,
                        "task": "completed-job",
                        "state": "done",
                        "worker": "m4mac",
                        "placement": "remote",
                        "submitted_at": "2026-07-22T01:58:00+00:00",
                        "started_at": "2026-07-22T01:58:30+00:00",
                        "finished_at": f"2026-07-22T01:59:{index:02d}+00:00",
                    },
                )

            with mock.patch.object(metrics, "MAX_SCANNED_JOBS", 3):
                report = metrics.ComputeMetricsReader(
                    root, clock=lambda: self.NOW
                ).report(6)

        self.assertEqual(report["status"]["queued"], 1)
        self.assertEqual(report["summary"]["jobs"], 2)
        self.assertIn(queued.name, {job["id"] for job in report["jobs"]})
        self.assertEqual(
            {job["id"] for job in report["jobs"] if job["state"] == "done"},
            {
                "20260722T015903Z-00000003",
                "20260722T015902Z-00000002",
            },
        )

    def test_excess_current_jobs_fail_visibly_instead_of_underreporting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "compute"
            for index, state in enumerate(("queued", "running", "queued")):
                job_id = f"20260722T0159{index:02d}Z-{index:08x}"
                job = root / state / job_id
                self.write_json(
                    job / "manifest.json",
                    {
                        "id": job_id,
                        "task": "current-job",
                        "state": state,
                        "submitted_at": "2026-07-22T01:59:00+00:00",
                    },
                )

            with mock.patch.object(metrics, "MAX_SCANNED_JOBS", 2):
                with self.assertRaisesRegex(
                    metrics.ComputeMetricsError, "current compute jobs exceed"
                ):
                    metrics.ComputeMetricsReader(
                        root, clock=lambda: self.NOW
                    ).report(6)

    def test_missed_events_are_bounded_by_time_and_sanitize_display_text(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "compute"
            self.write_json(
                root / "missed" / "recent.json",
                {
                    "recorded_at": "2026-07-22T00:52:00+00:00",
                    "profile": "python\nscript",
                    "label": "python\nanalysis.py",
                    "reason": "agent-choice",
                    "duration_seconds": "not-a-number",
                },
            )
            self.write_json(
                root / "missed" / "old.json",
                {
                    "recorded_at": "2026-06-01T00:00:00+00:00",
                    "profile": "repo-test",
                },
            )

            report = metrics.ComputeMetricsReader(root, clock=lambda: self.NOW).report(6)

        local = report["eligible_local_work"]
        self.assertEqual(local["events"], 1)
        self.assertEqual(local["cpu_seconds"], 0)
        self.assertEqual(local["recent"][0]["command_category"], "python script")
        self.assertEqual(local["recent"][0]["reason"], "agent-choice")
        self.assertEqual(local["recent"][0]["label"], "python analysis.py")

    def test_detailed_worker_timing_reports_active_and_transfer_phases(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "compute"
            done = root / "done" / "20260722T015900Z-feedface"
            self.write_json(
                done / "manifest.json",
                {
                    "id": done.name,
                    "task": "repo-tests",
                    "state": "done",
                    "worker": "m4mac.00",
                    "placement": "remote",
                    "submitted_at": "2026-07-22T01:58:00+00:00",
                    "started_at": "2026-07-22T01:59:00+00:00",
                    "finished_at": "2026-07-22T01:59:10+00:00",
                },
            )
            self.write_json(
                done / "result" / "execution.json",
                {
                    "duration_seconds": 4.0,
                    "timing": {
                        "source_input_preparation_seconds": 0.5,
                        "analysis_seconds": 4.0,
                        "packaging_seconds": 0.25,
                        "result_upload_seconds_excluding_execution_json": 0.1,
                        "worker_attempt_active_seconds": 5.0,
                    },
                    "resource_usage": {
                        "cpu_seconds": 6.0,
                        "peak_rss_bytes": 64 * 1024 * 1024,
                    },
                },
            )

            report = metrics.ComputeMetricsReader(root, clock=lambda: self.NOW).report(6)

        job = report["jobs"][0]
        self.assertTrue(job["detailed_timing"])
        self.assertEqual(job["wall_seconds"], 5.0)
        self.assertEqual(job["analysis_seconds"], 4.0)
        self.assertEqual(job["preparation_seconds"], 0.5)
        self.assertEqual(job["packaging_seconds"], 0.25)
        self.assertEqual(job["result_upload_seconds"], 0.1)
        summary = report["summary"]
        self.assertEqual(summary["timing_jobs"], 1)
        self.assertEqual(summary["mac_wall_seconds"], 5.0)
        self.assertEqual(summary["mac_analysis_seconds"], 4.0)
        self.assertEqual(summary["mac_preparation_seconds"], 0.5)
        self.assertEqual(summary["mac_packaging_seconds"], 0.25)
        self.assertEqual(summary["mac_result_upload_seconds"], 0.1)
        self.assertEqual(summary["aggregate_cpu_percent"], 120.0)

    def test_exact_content_benchmark_uses_pi_sample_averages(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "compute"
            self.write_completed_metric_job(
                root,
                "20260722T015901Z-00000001",
                placement="pi-local",
                analysis_seconds=10,
                cpu_seconds=8,
                peak_rss_bytes=100,
            )
            self.write_completed_metric_job(
                root,
                "20260722T015902Z-00000002",
                placement="pi-local",
                analysis_seconds=14,
                cpu_seconds=12,
                peak_rss_bytes=120,
            )
            self.write_completed_metric_job(
                root,
                "20260722T015903Z-00000003",
                placement="remote",
                analysis_seconds=2,
                cpu_seconds=3,
                peak_rss_bytes=80,
            )
            self.write_completed_metric_job(
                root,
                "20260722T015904Z-00000004",
                placement="remote",
                analysis_seconds=3,
                cpu_seconds=4,
                peak_rss_bytes=90,
            )

            report = metrics.ComputeMetricsReader(
                root, clock=lambda: self.NOW
            ).report(6)

        benchmark = report["benchmark"]
        self.assertEqual(benchmark["calibrated_workloads"], 1)
        self.assertEqual(benchmark["calibrated_remote_jobs"], 2)
        self.assertEqual(benchmark["pi_samples"], 2)
        self.assertEqual(
            benchmark["estimated_pi_analysis_seconds_avoided"], 24
        )
        self.assertEqual(benchmark["estimated_pi_cpu_seconds_avoided"], 20)
        self.assertEqual(benchmark["matched_mac_analysis_seconds"], 5)
        self.assertEqual(benchmark["matched_mac_cpu_seconds"], 7)
        self.assertEqual(benchmark["analysis_speedup_ratio"], 4.8)
        self.assertEqual(benchmark["pi_to_mac_cpu_ratio"], 2.857)
        self.assertEqual(benchmark["maximum_pi_peak_rss_bytes"], 120)
        serialized = json.dumps(report)
        self.assertNotIn("_workload_fingerprint", serialized)
        self.assertNotIn("private-analysis.py", serialized)
        self.assertNotIn("private-capture-name.log", serialized)
        self.assertIn("submission", report["measurement_note"])
        self.assertIn("SSH streaming overhead", report["measurement_note"])

    def test_benchmark_rejects_nonidentical_unverifiable_and_dataset_work(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "compute"
            cases = (
                ("hash", {"input_digest": "c" * 64}, {}),
                ("arguments", {"arguments": ["--snapshot"]}, {"arguments": []}),
                ("malformed", {"input_digest": "not-a-hash"}, {"input_digest": "not-a-hash"}),
                (
                    "dataset",
                    {"datasets": ["oem-service-docs"]},
                    {"datasets": ["oem-service-docs"]},
                ),
                ("failed", {}, {"state": "failed", "exit_code": 1}),
                ("no-telemetry", {}, {"telemetry": False}),
                ("no-analysis", {}, {"analysis_telemetry": False}),
                ("bad-value", {"input_value": [1]}, {"input_value": [1]}),
            )
            for index, (label, local_changes, remote_changes) in enumerate(cases):
                base = index * 2
                self.write_completed_metric_job(
                    root,
                    f"20260722T0159{base:02d}Z-{base:08x}",
                    placement="pi-local",
                    analysis_seconds=10,
                    cpu_seconds=8,
                    peak_rss_bytes=100,
                    task=f"benchmark-{label}",
                    **local_changes,
                )
                self.write_completed_metric_job(
                    root,
                    f"20260722T0159{base + 1:02d}Z-{base + 1:08x}",
                    placement="remote",
                    analysis_seconds=2,
                    cpu_seconds=3,
                    peak_rss_bytes=80,
                    task=f"benchmark-{label}",
                    **remote_changes,
                )

            # Even an exact workload cannot calibrate the selected range from a
            # Pi sample outside that same range.
            self.write_completed_metric_job(
                root,
                "20260601T000000Z-eeeeeeee",
                placement="pi-local",
                analysis_seconds=10,
                cpu_seconds=8,
                peak_rss_bytes=100,
                finished_at="2026-06-01T00:00:00+00:00",
                task="benchmark-old",
            )
            self.write_completed_metric_job(
                root,
                "20260722T015959Z-ffffffff",
                placement="remote",
                analysis_seconds=2,
                cpu_seconds=3,
                peak_rss_bytes=80,
                task="benchmark-old",
            )

            benchmark = metrics.ComputeMetricsReader(
                root, clock=lambda: self.NOW
            ).report(6)["benchmark"]

        self.assertEqual(
            benchmark,
            {
                "calibrated_workloads": 0,
                "calibrated_remote_jobs": 0,
                "pi_samples": 0,
                "estimated_pi_analysis_seconds_avoided": 0.0,
                "estimated_pi_cpu_seconds_avoided": 0.0,
                "matched_mac_analysis_seconds": 0.0,
                "matched_mac_cpu_seconds": 0.0,
                "analysis_speedup_ratio": None,
                "pi_to_mac_cpu_ratio": None,
                "maximum_pi_peak_rss_bytes": 0,
            },
        )

    def test_exact_input_benchmark_allows_empty_source_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "compute"
            for placement, suffix, analysis, cpu in (
                ("pi-local", "00000001", 9, 7),
                ("remote", "00000002", 3, 2),
            ):
                self.write_completed_metric_job(
                    root,
                    f"20260722T015900Z-{suffix}",
                    placement=placement,
                    analysis_seconds=analysis,
                    cpu_seconds=cpu,
                    peak_rss_bytes=64,
                    task="sqlite-query",
                    arguments=["SELECT 1"],
                    include_source=False,
                )

            benchmark = metrics.ComputeMetricsReader(
                root, clock=lambda: self.NOW
            ).report(6)["benchmark"]

        self.assertEqual(benchmark["calibrated_workloads"], 1)
        self.assertEqual(benchmark["calibrated_remote_jobs"], 1)
        self.assertEqual(benchmark["estimated_pi_analysis_seconds_avoided"], 9)

    def test_pi_local_fallback_never_counts_as_mac_capacity_or_offload(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "compute"
            self.write_json(
                root / "workers" / "vanpi-local.00.json",
                {
                    "worker": "vanpi-local.00",
                    "seen_at": "2026-07-22T01:59:50+00:00",
                    "slots_total": 99,
                    "slots_busy": 99,
                },
            )
            done = root / "done" / "20260722T015900Z-deadbeef"
            self.write_json(
                done / "manifest.json",
                {
                    "id": done.name,
                    "task": "repo-tests",
                    "state": "done",
                    "worker": "vanpi-local.00",
                    "placement": "pi-local",
                    "submitted_at": "2026-07-22T01:58:00+00:00",
                    "started_at": "2026-07-22T01:59:00+00:00",
                    "finished_at": "2026-07-22T01:59:10+00:00",
                },
            )
            self.write_json(
                done / "result" / "execution.json",
                {
                    "placement": "pi-local",
                    "duration_seconds": 10,
                    "resource_usage": {
                        "cpu_seconds": 12,
                        "peak_rss_bytes": 64 * 1024 * 1024,
                    },
                },
            )

            report = metrics.ComputeMetricsReader(root, clock=lambda: self.NOW).report(6)

        self.assertFalse(report["status"]["available"])
        self.assertIsNone(report["status"]["slots_total"])
        self.assertEqual(report["summary"]["jobs"], 0)
        self.assertEqual(report["summary"]["mac_cpu_seconds"], 0)
        self.assertEqual(report["jobs"], [])
        self.assertEqual(report["status"]["workers"][0]["placement"], "pi-local")

    def test_slot_capacity_is_unknown_without_fresh_capacity_heartbeat(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "compute"
            self.write_json(
                root / "workers" / "legacy.json",
                {"worker": "legacy", "seen_at": "2026-07-22T00:53:10+00:00"},
            )

            report = metrics.ComputeMetricsReader(root, clock=lambda: self.NOW).report(6)

        self.assertIsNone(report["status"]["slots_total"])
        self.assertIsNone(report["status"]["slots_busy"])
        self.assertIsNone(report["status"]["slots_available"])

    def test_metrics_rejects_unversioned_and_incompatible_heartbeats(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "compute"
            self.write_json(
                root / "workers" / "unversioned.json",
                {
                    "worker": "unversioned",
                    "seen_at": "2026-07-22T00:53:10+00:00",
                    "slots_total": 10,
                    "slots_busy": 0,
                },
            )
            self.write_json(
                root / "workers" / "incompatible.json",
                {
                    "worker": "incompatible",
                    "seen_at": "2026-07-22T00:53:10+00:00",
                    "protocol_version": protocol.WORKER_PROTOCOL_VERSION + 1,
                    "slots_total": 10,
                    "slots_busy": 0,
                },
            )
            self.write_json(
                root / "workers" / "boolean.json",
                {
                    "worker": "boolean",
                    "seen_at": "2026-07-22T00:53:10+00:00",
                    "protocol_version": True,
                },
            )
            self.write_json(
                root / "workers" / "string.json",
                {
                    "worker": "string",
                    "seen_at": "2026-07-22T00:53:10+00:00",
                    "protocol_version": str(protocol.WORKER_PROTOCOL_VERSION),
                },
            )

            report = metrics.ComputeMetricsReader(root, clock=lambda: self.NOW).report(6)

        self.assertFalse(report["status"]["available"])
        self.assertIsNone(report["status"]["slots_total"])
        workers = {worker["worker"]: worker for worker in report["status"]["workers"]}
        self.assertFalse(workers["unversioned"]["available"])
        self.assertIsNone(workers["unversioned"]["protocol_version"])
        self.assertFalse(workers["incompatible"]["available"])
        self.assertEqual(
            workers["incompatible"]["protocol_version"],
            protocol.WORKER_PROTOCOL_VERSION + 1,
        )
        self.assertFalse(workers["boolean"]["available"])
        self.assertFalse(workers["string"]["available"])

    def test_similarly_named_worker_is_remote(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "compute"
            self.write_json(
                root / "workers" / "vanpi-locality.json",
                {
                    "worker": "vanpi-locality",
                    "seen_at": "2026-07-22T00:53:10+00:00",
                    "protocol_version": protocol.WORKER_PROTOCOL_VERSION,
                },
            )

            report = metrics.ComputeMetricsReader(root, clock=lambda: self.NOW).report(6)

        worker = report["status"]["workers"][0]
        self.assertEqual(worker["placement"], "remote")
        self.assertTrue(worker["available"])
        self.assertTrue(report["status"]["available"])

    def test_rejects_unbounded_time_range(self):
        with tempfile.TemporaryDirectory() as directory:
            reader = metrics.ComputeMetricsReader(directory)
            with self.assertRaisesRegex(ValueError, "6, 24, 168, or 720"):
                reader.report(999)


if __name__ == "__main__":
    unittest.main()
