import os
import importlib.util
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from pi.scripts import van_compute_upgrade_gate as gate


class VanComputeUpgradeGateTests(unittest.TestCase):
    OWNER = "installer-11111111-1111-1111-1111-111111111111"
    OTHER_OWNER = "installer-22222222-2222-2222-2222-222222222222"

    def test_supported_public_submitters_are_recognized(self):
        self.assertTrue(
            gate._is_supported_submitter(
                [b"python3", b"/home/pi/scripts/van_compute.py", b"submit", b"repo-tests"]
            )
        )
        self.assertTrue(
            gate._is_supported_submitter(
                [b"python3", b"/home/pi/scripts/pi_compute.py", b"run", b"repo-tests"]
            )
        )
        self.assertFalse(
            gate._is_supported_submitter(
                [b"python3", b"/home/pi/scripts/van_compute.py", b"status", b"job"]
            )
        )

    def test_active_submitter_scan_includes_only_same_user_supported_commands(self):
        with tempfile.TemporaryDirectory() as directory:
            proc = Path(directory)
            for pid, arguments in (
                ("100", [b"python3", b"/x/van_compute.py", b"submit", b"task"]),
                ("101", [b"python3", b"/x/pi_compute.py", b"run", b"task"]),
                ("102", [b"python3", b"/x/van_compute.py", b"status", b"job"]),
            ):
                process = proc / pid
                process.mkdir()
                (process / "cmdline").write_bytes(b"\0".join(arguments) + b"\0")

            self.assertEqual(
                gate.active_submitter_count(
                    proc,
                    current_pid=999,
                    current_uid=os.getuid(),
                ),
                2,
            )

    def test_missing_proc_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(RuntimeError, "/proc is unavailable"):
                gate.active_submitter_count(Path(directory) / "missing")

    def test_public_queue_import_is_fenced(self):
        specification = importlib.util.spec_from_file_location(
            "van_compute", Path(gate.__file__)
        )
        self.assertIsNotNone(specification)
        self.assertIsNotNone(specification.loader)
        module = importlib.util.module_from_spec(specification)
        error = StringIO()
        with redirect_stderr(error), self.assertRaises(SystemExit) as raised:
            specification.loader.exec_module(module)
        self.assertEqual(raised.exception.code, 75)
        self.assertIn("being upgraded", error.getvalue())

    def test_acquire_and_restore_are_a_complete_owned_transition(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script_root = root / "scripts"
            script_root.mkdir()
            target = script_root / "van_compute.py"
            backup = script_root / ".van_compute.py.pre-upgrade"
            owner_record = script_root / ".van-compute-upgrade-owner"
            staged_gate = root / "staged-upgrade-gate.py"
            original = b"#!/usr/bin/env python3\nprint('original CLI')\n"
            fence = b"#!/usr/bin/env python3\nUPGRADE_GATE = True\n"
            target.write_bytes(original)
            target.chmod(0o750)
            staged_gate.write_bytes(fence)

            gate.acquire_submission_gate(
                staged_gate,
                self.OWNER,
                script_root=script_root,
            )

            self.assertEqual(target.read_bytes(), fence)
            self.assertEqual(target.stat().st_mode & 0o777, 0o750)
            self.assertEqual(backup.read_bytes(), original)
            self.assertEqual(backup.stat().st_mode & 0o777, 0o600)
            self.assertEqual(owner_record.read_text(), f"{self.OWNER}\n")
            self.assertEqual(owner_record.stat().st_mode & 0o777, 0o600)

            gate.restore_submission_cli(self.OWNER, script_root=script_root)

            self.assertEqual(target.read_bytes(), original)
            self.assertEqual(target.stat().st_mode & 0o777, 0o750)
            self.assertFalse(backup.exists())
            self.assertFalse(owner_record.exists())

    def test_competing_owner_is_rejected_without_mutating_gate_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script_root = root / "scripts"
            script_root.mkdir()
            target = script_root / "van_compute.py"
            backup = script_root / ".van_compute.py.pre-upgrade"
            owner_record = script_root / ".van-compute-upgrade-owner"
            lock = script_root / ".van-compute-upgrade.lock"
            staged_gate = root / "staged-upgrade-gate.py"
            target.write_bytes(b"original CLI\n")
            target.chmod(0o750)
            backup.write_bytes(b"existing backup\n")
            backup.chmod(0o640)
            owner_record.write_text(f"{self.OTHER_OWNER}\n")
            owner_record.chmod(0o640)
            lock.touch(mode=0o600)
            staged_gate.write_bytes(b"UPGRADE_GATE = True\n")
            before = {
                path.name: (path.read_bytes(), path.stat().st_mode & 0o777)
                for path in script_root.iterdir()
            }

            with self.assertRaisesRegex(RuntimeError, "another installer"):
                gate.acquire_submission_gate(
                    staged_gate,
                    self.OWNER,
                    script_root=script_root,
                )

            after = {
                path.name: (path.read_bytes(), path.stat().st_mode & 0o777)
                for path in script_root.iterdir()
            }
            self.assertEqual(after, before)

    def test_finalize_removes_artifacts_before_releasing_maintenance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script_root = root / "scripts"
            queue_root = root / "queue"
            script_root.mkdir()
            queue_root.mkdir()
            target = script_root / "van_compute.py"
            backup = script_root / ".van_compute.py.pre-upgrade"
            owner_record = script_root / ".van-compute-upgrade-owner"
            maintenance = queue_root / ".maintenance.json"
            target.write_bytes(b"new queue CLI\n")
            backup.write_bytes(b"old queue CLI\n")
            owner_record.write_text(f"{self.OWNER}\n")
            maintenance.write_text(f'{{"active": true, "owner": "{self.OWNER}"}}\n')

            def release(command, **kwargs):
                self.assertEqual(
                    command,
                    [
                        str(target),
                        "--root",
                        str(queue_root),
                        "maintenance",
                        "exit",
                        "--owner",
                        self.OWNER,
                    ],
                )
                self.assertTrue(kwargs["check"])
                self.assertIs(kwargs["stdin"], gate.subprocess.DEVNULL)
                self.assertIs(kwargs["stdout"], gate.subprocess.DEVNULL)
                self.assertFalse(backup.exists())
                self.assertFalse(owner_record.exists())
                maintenance.unlink()
                return gate.subprocess.CompletedProcess(command, 0)

            with mock.patch.object(gate.subprocess, "run", side_effect=release):
                gate.finalize_upgrade(
                    self.OWNER,
                    script_root=script_root,
                    queue_root=queue_root,
                )

            self.assertFalse(maintenance.exists())
            self.assertFalse(backup.exists())
            self.assertFalse(owner_record.exists())
            self.assertEqual(target.read_bytes(), b"new queue CLI\n")

    def test_finalize_release_failure_leaves_maintenance_for_owned_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            script_root = root / "scripts"
            queue_root = root / "queue"
            script_root.mkdir()
            queue_root.mkdir()
            target = script_root / "van_compute.py"
            backup = script_root / ".van_compute.py.pre-upgrade"
            owner_record = script_root / ".van-compute-upgrade-owner"
            maintenance = queue_root / ".maintenance.json"
            target.write_bytes(b"new queue CLI\n")
            backup.write_bytes(b"old queue CLI\n")
            owner_record.write_text(f"{self.OWNER}\n")
            maintenance.write_text(f'{{"active": true, "owner": "{self.OWNER}"}}\n')

            failure = gate.subprocess.CalledProcessError(2, [str(target)])
            with (
                mock.patch.object(gate.subprocess, "run", side_effect=failure),
                self.assertRaisesRegex(RuntimeError, "cannot release queue maintenance"),
            ):
                gate.finalize_upgrade(
                    self.OWNER,
                    script_root=script_root,
                    queue_root=queue_root,
                )

            self.assertTrue(maintenance.exists())
            self.assertFalse(backup.exists())
            self.assertFalse(owner_record.exists())
            self.assertEqual(target.read_bytes(), b"new queue CLI\n")


if __name__ == "__main__":
    unittest.main()
