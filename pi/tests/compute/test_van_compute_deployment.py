from __future__ import annotations

import errno
import importlib.util
import io
import json
import os
from pathlib import Path
import plistlib
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from macbook.scripts.van_compute_installer import cli as installer_cli
from macbook.scripts.van_compute_installer import constants as installer_constants
from macbook.scripts.van_compute_installer import mac as installer_mac
from macbook.scripts.van_compute_installer import orchestrator as installer_orchestrator
from macbook.scripts.van_compute_installer.models import (
    DeploymentError, Options, Paths, SourceRelease,
)
from macbook.scripts.van_compute_installer.orchestrator import Installer
from pi import deploy_python
from pi.tests.compute.van_compute_deployment_support import (
    BROKER_SERVICE, COMPUTE_ROOT, DASHBOARD_SERVICE, EXAMPLE_TASKS, FRONTEND_CLI,
    INSTALLER, QUEUE_CLI, REPOSITORY_ROOT, SHIM, UPGRADE_GATE,
    DeploymentFixtureMixin, FakeCompleted, FakeLocal, FakeLock, FakeRemote,
    WorkflowInstaller,
)
from pi.tests.unit_contract import command_arguments, parse_directives, parse_environment
from van_compute import protocol


class VanComputeDeploymentTests(DeploymentFixtureMixin, unittest.TestCase):

    def test_dry_run_is_local_only_and_side_effect_free(self):
        with tempfile.TemporaryDirectory() as directory:
            out = io.StringIO()
            local = FakeLocal()
            remote = FakeRemote()
            installer = self.make_installer(
                directory,
                options=Options(dry_run=True, if_needed=True),
                local=local,
                remote=remote,
                stdout=out,
            )
            home = installer.paths.home
            self.assertFalse(home.exists())

            self.assertEqual(installer.execute(), 0)
            plan = json.loads(out.getvalue())

            self.assertEqual(plan["mode"], "coupled")
            self.assertTrue(plan["if_needed"])
            self.assertFalse(plan["remote_probes_executed"])
            self.assertEqual(
                plan["local_operations"],
                [
                    "validate sandbox-exec and local prerequisites",
                    "provision or verify an immutable Mac worker release",
                    "validate the worker sandbox and resource watchdog",
                    "atomically install and reload the persistent LaunchAgent",
                ],
            )
            self.assertEqual(local.calls, [])
            self.assertEqual(remote.calls, [])
            self.assertFalse(home.exists())

    def test_configured_dataset_symlink_is_rejected_before_resolution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "datasets.json"
            target.write_text('{"datasets": {}}\n', encoding="utf-8")
            configured = root / "configured.json"
            configured.symlink_to(target)
            installer = self.make_installer(
                directory,
                environment={"VAN_COMPUTE_DATASET_CONFIG": str(configured)},
            )
            with self.assertRaisesRegex(
                DeploymentError, "regular non-symlink file"
            ):
                installer.dataset_source()

    def run_denial_probe(self, read_effects, network_error):
        socket_handle = mock.Mock()
        socket_handle.sendto.side_effect = network_error
        with (
            mock.patch("pathlib.Path.read_text", side_effect=read_effects),
            mock.patch("socket.socket", return_value=socket_handle),
            mock.patch.object(
                sys, "argv", ["probe", "/private/sentinel", "/alias/sentinel"]
            ),
        ):
            exec(compile(installer_constants.SANDBOX_DENIAL_PROBE, "<sandbox-probe>", "exec"), {})
        socket_handle.close.assert_called_once_with()

    def test_sandbox_denial_probe_accepts_only_permission_denials_and_missing_alias(
        self,
    ):
        self.run_denial_probe(
            [
                PermissionError(errno.EACCES, "denied"),
                FileNotFoundError(errno.ENOENT, "alias absent"),
            ],
            PermissionError(errno.EPERM, "network denied"),
        )

    def test_sandbox_denial_probe_rejects_missing_primary_sentinel(self):
        with self.assertRaises(FileNotFoundError):
            self.run_denial_probe(
                [FileNotFoundError(errno.ENOENT, "primary absent")],
                PermissionError(errno.EPERM, "network denied"),
            )

    def test_sandbox_denial_probe_rejects_readable_primary_sentinel(self):
        with (
            mock.patch("pathlib.Path.read_text", return_value="secret"),
            mock.patch.object(
                sys, "argv", ["probe", "/private/sentinel", "/alias/sentinel"]
            ),
            self.assertRaisesRegex(SystemExit, "sandbox read"),
        ):
            exec(
                compile(installer_constants.SANDBOX_DENIAL_PROBE, "<sandbox-probe>", "exec"),
                {},
            )

    def test_sandbox_denial_probe_rejects_nonpermission_network_failure(self):
        with self.assertRaises(ConnectionRefusedError):
            self.run_denial_probe(
                [
                    PermissionError(errno.EACCES, "denied"),
                    PermissionError(errno.EPERM, "denied"),
                ],
                ConnectionRefusedError(errno.ECONNREFUSED, "unexpected"),
            )

    def test_sandbox_checks_run_from_private_job_directory_and_rg_reads_probe(self):
        with tempfile.TemporaryDirectory() as directory:
            local = FakeLocal()
            installer = self.make_installer(directory, local=local)
            staging = Path(directory) / "release"
            (staging / "venv" / "bin").mkdir(parents=True)
            (staging / "sandbox.sb").write_text("(version 1)\n", encoding="utf-8")
            installer._validate_mac_release(staging, self.source)
        sandbox_calls = [
            call for call in local.calls if "/usr/bin/sandbox-exec" in call[0]
        ]
        self.assertEqual(len(sandbox_calls), 6)
        sandbox_cwds = {call[5] for call in sandbox_calls}
        self.assertEqual(len(sandbox_cwds), 1)
        sandbox_cwd = next(iter(sandbox_cwds))
        self.assertIsInstance(sandbox_cwd, Path)
        rg = next(
            call[0] for call in sandbox_calls if "/opt/homebrew/bin/rg" in call[0]
        )
        rg_index = rg.index("/opt/homebrew/bin/rg")
        self.assertEqual(rg[rg_index + 1 : rg_index + 3], ["--fixed-strings", "ok"])
        self.assertEqual(Path(rg[rg_index + 3]), sandbox_cwd / "allowed.txt")

    def test_command_line_preserves_coupled_installer_options(self):
        self.assertEqual(installer_cli.parse_arguments([]), Options())
        self.assertEqual(
            installer_cli.parse_arguments(["--if-needed", "--dry-run"]),
            Options(if_needed=True, dry_run=True),
        )
        with mock.patch("sys.stderr", new=io.StringIO()):
            with self.assertRaises(SystemExit):
                installer_cli.parse_arguments(["--pi-only"])

    def test_main_maps_hup_and_term_to_cleanup_and_restores_handlers(self):
        for triggering_signal in (installer_cli.signal.SIGHUP, installer_cli.signal.SIGTERM):
            with self.subTest(triggering_signal=triggering_signal):
                installer = WorkflowInstaller(Options(), self.source)
                installed = {}
                previous = {
                    installer_cli.signal.SIGHUP: object(),
                    installer_cli.signal.SIGTERM: object(),
                }
                signal_calls = []

                def fake_signal(signum, handler):
                    signal_calls.append((signum, handler))
                    if len(signal_calls) <= 2:
                        installed[signum] = handler
                        return previous[signum]
                    return installed[signum]

                def interrupt_drain():
                    installer.events.append("drain")
                    installed[triggering_signal](triggering_signal, None)

                installer.drain_worker = interrupt_drain
                stderr = io.StringIO()
                with (
                    mock.patch.object(installer_cli, "Installer", return_value=installer),
                    mock.patch.object(
                        installer_cli.signal, "signal", side_effect=fake_signal
                    ),
                    mock.patch.object(installer_cli.sys, "stderr", stderr),
                ):
                    self.assertEqual(installer_cli.main([]), 130)

                self.assertEqual(installer.events[-2:], ["cleanup", "lock-close"])
                self.assertNotIn("gate", installer.events)
                self.assertIn("installer: interrupted", stderr.getvalue())
                self.assertEqual(
                    signal_calls[2:],
                    [
                        (installer_cli.signal.SIGHUP, previous[installer_cli.signal.SIGHUP]),
                        (installer_cli.signal.SIGTERM, previous[installer_cli.signal.SIGTERM]),
                    ],
                )

    def test_shell_entry_point_executes_python_dry_run(self):
        with tempfile.TemporaryDirectory() as directory:
            environment = os.environ.copy()
            dry_home = Path(directory) / "home"
            environment["HOME"] = str(dry_home)
            result = subprocess.run(
                [str(SHIM), "--dry-run"],
                cwd=REPOSITORY_ROOT,
                env=environment,
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
            self.assertFalse(dry_home.exists())
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["host"], "pi@vanpi.lan")
        self.assertEqual(payload["worker"], "m4mac")
        self.assertFalse(payload["remote_probes_executed"])

    def test_coupled_workflow_orders_staging_fencing_cutover_and_health(self):
        installer = WorkflowInstaller(Options(), self.source)
        self.assertEqual(installer.execute(), 0)
        expected = [
            "source",
            "local-preflight",
            "lock",
            "remote-preflight",
            "prepare-mac",
            "dataset",
            "stage",
            "validate-stage",
            "runtime",
            "maintenance-status",
            "queue",
            "drain",
            "gate",
            "submitter-drain",
            "maintenance-enter",
            "cutover",
            "seen",
            "install-agent",
            "heartbeat:old",
            "finalize",
            "retire",
            "dashboard",
            "prune",
            "cleanup",
            "lock-close",
        ]
        self.assertEqual(installer.events, expected)
        self.assertLess(
            installer.events.index("validate-stage"), installer.events.index("drain")
        )
        self.assertLess(
            installer.events.index("runtime"), installer.events.index("gate")
        )
        self.assertLess(
            installer.events.index("finalize"), installer.events.index("retire")
        )

    def test_if_needed_skips_only_after_both_sides_are_healthy(self):
        installer = WorkflowInstaller(Options(if_needed=True), self.source)
        installer.deployment_current = mock.Mock(return_value=True)
        self.assertEqual(installer.execute(), 0)
        self.assertEqual(installer.events, ["source"])
        self.assertIn("deployment is current", installer.out.getvalue())

    def _create_current_probe_fixture(self, fixture, source):
        root = fixture / "pi" / "van_compute"
        old = fixture / "pi" / "scripts" / "compute"
        queue = fixture / "queue"
        release = root / "releases" / source.pi_version
        scripts = root / "scripts"
        (release / "van_compute").mkdir(parents=True)
        scripts.mkdir(parents=True)
        old.mkdir(parents=True)
        queue.mkdir()
        (root / "current").symlink_to(
            Path("releases") / source.pi_version, target_is_directory=True
        )
        (root / "deployment.sha256").write_text(
            source.source_fingerprint + "\n", encoding="utf-8"
        )
        (release / "source.sha256").write_text(
            source.source_fingerprint + "\n", encoding="utf-8"
        )
        (release / "van_compute" / "__init__.py").write_text(
            "", encoding="utf-8"
        )
        (release / "van_compute" / "queue.py").write_text(
            "import json, sys\n"
            "from pathlib import Path\n"
            "root = Path(sys.argv[sys.argv.index('--root') + 1])\n"
            "print(json.dumps({'active': (root / 'maintenance-active').exists()}))\n",
            encoding="utf-8",
        )
        available = {
            "workers": [
                {
                    "worker": "m4mac",
                    "age_seconds": 0,
                    "slots_total": 10,
                    "slots_busy": 0,
                }
            ]
        }
        cli = scripts / "van_compute.py"
        cli.write_text(
            "#!/bin/sh\nprintf '%s\\n' " + repr(json.dumps(available)) + "\n",
            encoding="utf-8",
        )
        cli.chmod(0o700)
        return root, old, queue, scripts

    def _run_current_probe(self, script, fixture, source, root, old, queue):
        fakes = r'''
readlink_e() { /usr/bin/python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$1"; }
systemctl() {
  case "$1" in
    is-active) return 0;;
    cat) printf '%s/current\n' "$root";;
    *) return 1;;
  esac
}
'''
        return subprocess.run(
            [
                "/bin/sh",
                "-s",
                "--",
                str(root),
                source.source_fingerprint,
                "m4mac",
                str(queue),
                str(old),
            ],
            input=fakes + script,
            text=True,
            capture_output=True,
            check=False,
            cwd=fixture,
        )

    def test_current_probe_rejects_maintenance_and_upgrade_owner_artifacts(self):
        remote = FakeRemote()
        local = FakeLocal(launch_states=["loaded\n"])
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory).resolve()
            installer = self.make_installer(
                directory, local=local, remote=remote
            )
            source = installer.build_source_release()
            mac_release = self.create_owned_release(installer, source)
            self.point_launchagent_at(installer, mac_release)
            self.assertTrue(installer.deployment_current(source))
            root, old, queue, scripts = self._create_current_probe_fixture(
                fixture, source
            )
            script = remote.scripts["current-deployment"]
            script = script.replace("/usr/bin/systemctl", "systemctl")
            script = script.replace("/usr/bin/readlink -e", "readlink_e")
            script = script.replace(
                "/bin/grep", shutil.which("grep") or "/usr/bin/grep"
            )
            script = script.replace(
                "/usr/bin/python3 -P -m van_compute.queue",
                sys.executable + " -m van_compute.queue",
            )

            def probe():
                return self._run_current_probe(
                    script, fixture, source, root, old, queue
                )

            healthy = probe()
            self.assertEqual(healthy.returncode, 0, healthy.stderr)
            maintenance = queue / "maintenance-active"
            maintenance.touch()
            self.assertNotEqual(probe().returncode, 0)
            maintenance.unlink()
            for script_root in (scripts, old):
                with self.subTest(script_root=script_root):
                    owner = script_root / ".van-compute-upgrade-owner"
                    owner.write_text("installer-owner\n", encoding="utf-8")
                    self.assertNotEqual(probe().returncode, 0)
                    owner.unlink()

    def test_stage_validation_failure_happens_before_drain_or_cutover(self):
        installer = WorkflowInstaller(Options(), self.source)

        def fail(_source):
            installer.events.append("validate-stage")
            raise DeploymentError("bad stage")

        installer.validate_remote_stage = fail
        with self.assertRaisesRegex(DeploymentError, "bad stage"):
            installer.execute()
        self.assertNotIn("drain", installer.events)
        self.assertNotIn("gate", installer.events)
        self.assertNotIn("cutover", installer.events)
        self.assertEqual(installer.events[-2:], ["cleanup", "lock-close"])

    def test_gate_denial_never_enters_maintenance_or_cutover(self):
        installer = WorkflowInstaller(Options(), self.source)

        def deny():
            installer.events.append("gate")
            raise DeploymentError("gate denied")

        installer.acquire_submission_gate = deny
        with self.assertRaisesRegex(DeploymentError, "gate denied"):
            installer.execute()
        self.assertNotIn("submitter-drain", installer.events)
        self.assertNotIn("maintenance-enter", installer.events)
        self.assertNotIn("cutover", installer.events)
        self.assertEqual(installer.events[-2:], ["cleanup", "lock-close"])

    def test_drain_timeout_forces_bootout_only_after_rechecking_empty_queue(self):
        state = "\n\tpid = 4321\n\t--serve\n"
        local = FakeLocal(launch_states=[state] * 31)
        remote = FakeRemote(responses={"active-queue-jobs": "0\n"})
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory, local=local, remote=remote)
            installer.paths.target_dir.mkdir(parents=True)
            payload = plistlib.loads(installer.paths.source_plist.read_bytes())
            payload["ProgramArguments"] = [
                "/python",
                "-m",
                "van_compute.worker",
                "--serve",
            ]
            installer.paths.target_plist.write_bytes(plistlib.dumps(payload))
            installer.drain_worker()

        commands = [call[0] for call in local.calls]
        disable = next(
            index
            for index, command in enumerate(commands)
            if command[1:2] == ["disable"]
        )
        signal = next(
            index for index, command in enumerate(commands) if command[1:2] == ["kill"]
        )
        bootout = next(
            index
            for index, command in enumerate(commands)
            if command[1:2] == ["bootout"]
        )
        enable = next(
            index
            for index, command in enumerate(commands)
            if command[1:2] == ["enable"]
        )
        self.assertLess(disable, signal)
        self.assertLess(signal, bootout)
        self.assertLess(bootout, enable)
        queue_checks = [call for call in remote.calls if call[0] == "active-queue-jobs"]
        self.assertEqual(len(queue_checks), 31)
        self.assertTrue(installer.state.restore_previous_agent)

    def test_no_prior_install_does_not_disable_or_restore_an_agent(self):
        local = FakeLocal(launch_states=[None])
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory, local=local)
            installer.drain_worker()
        mutations = {
            call[0][1]
            for call in local.calls
            if call[0][:1] == ["/bin/launchctl"] and len(call[0]) > 1
        }
        self.assertEqual(mutations, {"print"})
        self.assertFalse(installer.state.restore_previous_agent)

    def test_submitter_timeout_never_crosses_maintenance_boundary(self):
        ticks = iter([0.0, 0.5, 1.0, 1.5])
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(
                directory,
                options=Options(submitter_timeout=1),
                monotonic=lambda: next(ticks),
            )
            installer.active_submitters = mock.Mock(return_value=1)
            installer.active_queue_jobs = mock.Mock(return_value=0)
            with self.assertRaisesRegex(DeploymentError, "did not drain"):
                installer.wait_for_submitter_drain()
        self.assertFalse(installer.state.cutover_started)
        self.assertFalse(installer.state.maintenance_active)

    def test_pre_cutover_cleanup_restores_gate_before_previous_worker(self):
        local = FakeLocal()
        remote = FakeRemote()
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory, local=local, remote=remote)
            installer.owner = "installer-00000000-0000-0000-0000-000000000000"
            installer.state.upgrade_public_root = installer_constants.REMOTE_SCRIPTS
            installer.state.submission_gate_active = True
            installer.state.restore_previous_agent = True
            installer.cleanup()
        self.assertEqual(remote.calls[0][0], "restore-submission-gate")
        bootstrap = [call for call in local.calls if call[0][1:2] == ["bootstrap"]]
        self.assertEqual(len(bootstrap), 1)

    def test_failed_gate_rollback_leaves_previous_worker_disabled(self):
        local = FakeLocal()
        remote = FakeRemote(failures={"restore-submission-gate": "denied"})
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory, local=local, remote=remote)
            installer.owner = "installer-00000000-0000-0000-0000-000000000000"
            installer.state.upgrade_public_root = installer_constants.REMOTE_SCRIPTS
            installer.state.submission_gate_active = True
            installer.state.restore_previous_agent = True
            installer.cleanup()
            warning = installer.stderr.getvalue()
        self.assertFalse(any(call[0][1:2] == ["bootstrap"] for call in local.calls))
        self.assertIn("remains disabled", warning)

    def test_post_cutover_failure_never_restores_incompatible_worker(self):
        local = FakeLocal()
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory, local=local)
            installer.state.cutover_started = True
            installer.state.maintenance_active = True
            installer.state.restore_previous_agent = True
            installer.cleanup()
            warning = installer.stderr.getvalue()
        self.assertFalse(any(call[0][1:2] == ["bootstrap"] for call in local.calls))
        self.assertIn("previous worker remains unloaded", warning)
        self.assertIn("queue remains in maintenance", warning)

    def test_legacy_retirement_preserves_frozen_copy_and_refuses_mount_uncertainty(
        self,
    ):
        for mount_status, kind in (
            (32, None),
            (0, "directory"),
            (1, "directory"),
            (32, "file"),
            (32, "symlink"),
        ):
            with self.subTest(
                mount_status=mount_status
            ), tempfile.TemporaryDirectory() as directory:
                remote = FakeRemote()
                installer = self.make_installer(directory, remote=remote)
                installer.retire_legacy_layout()
                root = Path(directory)
                fake_home = root / "pi"
                frozen = fake_home / "scripts/python-automation/van_compute_protocol.py"
                frozen.parent.mkdir(parents=True)
                frozen.write_bytes(b"frozen rollback copy\n")
                retired_runtime = fake_home / ".local/share/van-compute"
                if kind is not None:
                    retired_runtime.parent.mkdir(parents=True)
                    if kind == "file":
                        retired_runtime.write_text("foreign file\n")
                    elif kind == "symlink":
                        retired_runtime.symlink_to(
                            frozen.parent, target_is_directory=True
                        )
                    else:
                        retired_runtime.mkdir()
                cmdline = root / "cmdline"
                cmdline.write_bytes(b"python3\0-m\0van_compute.broker\0")
                removal_log = root / "remove-attempt"
                script = remote.scripts["retire-legacy"]
                script = script.replace("/home/pi", str(fake_home))
                script = script.replace("/proc/$broker_pid/cmdline", str(cmdline))
                script = script.replace("/usr/bin/systemctl", "systemctl")
                script = script.replace("/usr/bin/mountpoint", "mountpoint")
                script = script.replace("/bin/rm", "rm")
                script = script.replace(
                    "/bin/grep", shutil.which("grep") or "/usr/bin/grep"
                )
                fakes = """removal_log="$3"
mount_status="$4"
sudo() { test "$1" = -n && shift; "$@"; }
systemctl() { case "$1" in cat) printf '%s/current\n' "$root";; show) printf '123\n';; *) return 0;; esac; }
mountpoint() { return "$mount_status"; }
rm() { printf attempted > "$removal_log"; return 77; }
"""
                result = subprocess.run(
                    [
                        "/bin/sh",
                        "-s",
                        "--",
                        str(fake_home / "van_compute"),
                        str(fake_home / "scripts/compute"),
                        str(removal_log),
                        str(mount_status),
                    ],
                    input=fakes + script,
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(
                    result.returncode, 0 if kind is None else 1, result.stderr
                )
                self.assertFalse(
                    removal_log.exists(),
                    "retirement tried deletion without a proven unmounted target",
                )
                self.assertEqual(frozen.read_bytes(), b"frozen rollback copy\n")


if __name__ == "__main__":
    unittest.main()
