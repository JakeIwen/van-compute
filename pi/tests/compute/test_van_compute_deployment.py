import errno
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

from pi import deploy_python
from pi.tests.unit_contract import (
    command_arguments,
    parse_directives,
    parse_environment,
)
from van_compute import protocol

from macbook.scripts import install_van_compute_worker as deployer


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
COMPUTE_ROOT = REPOSITORY_ROOT / "van_compute"
INSTALLER = REPOSITORY_ROOT / "macbook" / "scripts" / "install_van_compute_worker.py"
SHIM = REPOSITORY_ROOT / "macbook" / "scripts" / "install_van_compute_worker.zsh"
QUEUE_CLI = COMPUTE_ROOT / "entrypoints" / "van_compute.py"
FRONTEND_CLI = COMPUTE_ROOT / "entrypoints" / "pi_compute.py"
UPGRADE_GATE = COMPUTE_ROOT / "upgrade_gate.py"
EXAMPLE_TASKS = COMPUTE_ROOT / "configs" / "van-compute-obd.example.json"
DASHBOARD_SERVICE = REPOSITORY_ROOT / "pi" / "services" / "van-dashboard.service"
BROKER_SERVICE = COMPUTE_ROOT / "configs" / "van-compute-broker.service"


class FakeCompleted:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class FakeLocal:
    def __init__(self, launch_states=None):
        self.calls = []
        self.launch_states = list(launch_states or [])

    def run(
        self,
        arguments,
        *,
        input_text=None,
        capture_output=False,
        check=True,
        timeout=None,
        cwd=None,
    ):
        arguments = list(arguments)
        self.calls.append((arguments, input_text, capture_output, check, timeout, cwd))
        if arguments[:2] == ["/bin/launchctl", "print"]:
            if self.launch_states:
                state = self.launch_states.pop(0)
            else:
                state = None
            return (
                FakeCompleted(0, state)
                if state is not None
                else FakeCompleted(113, "", "not found")
            )
        return FakeCompleted()


class FakeRemote:
    def __init__(self, responses=None, failures=None):
        self.calls = []
        self.scripts = {}
        self.responses = {
            name: list(values) if isinstance(values, (list, tuple)) else [values]
            for name, values in (responses or {}).items()
        }
        self.failures = dict(failures or {})

    def run(
        self,
        name,
        script,
        arguments=(),
        *,
        capture_output=False,
        timeout=None,
    ):
        self.calls.append((name, tuple(arguments), capture_output, timeout))
        self.scripts[name] = script
        failure = self.failures.get(name)
        if failure is not None:
            raise deployer.DeploymentError(str(failure))
        values = self.responses.get(name, [""])
        value = values.pop(0) if len(values) > 1 else values[0]
        return value

    def upload(self, name, sources, destination):
        self.calls.append((name, tuple(map(str, sources)), destination, None))
        failure = self.failures.get(name)
        if failure is not None:
            raise deployer.DeploymentError(str(failure))


class FakeLock:
    def __init__(self, events):
        self.events = events

    def close(self):
        self.events.append("lock-close")


class WorkflowInstaller(deployer.Installer):
    """Exercise Installer.execute while replacing only external phase bodies."""

    def __init__(self, options, source):
        self.events = []
        self._source = source
        self._release = Path("/fake/mac-release")
        self.out = io.StringIO()
        self.err = io.StringIO()
        self.fake_local = FakeLocal()
        super().__init__(
            options,
            environment={"HOME": "/fake"},
            home=Path("/fake"),
            script=INSTALLER,
            local=self.fake_local,
            remote=FakeRemote(),
            stdout=self.out,
            stderr=self.err,
            sleep=lambda _seconds: None,
        )

    def build_source_release(self):
        self.events.append("source")
        return self._source

    def deployment_current(self, _source):
        self.events.append("current")
        return False

    def preflight_local(self, _source):
        self.events.append("local-preflight")

    def acquire_lock_and_owner(self):
        self.events.append("lock")
        self.owner = "installer-00000000-0000-0000-0000-000000000000"
        return FakeLock(self.events)

    def remote_preflight(self):
        self.events.append("remote-preflight")
        self.state.upgrade_public_root = deployer.REMOTE_SCRIPTS
        return self.state.upgrade_public_root

    def prepare_mac_release(self, _source):
        self.events.append("prepare-mac")
        self.release = self._release
        return self._release

    def install_dataset(self, _source):
        self.events.append("dataset")

    def stage_remote_release(self, _source, _release):
        self.events.append("stage")
        self.state.remote_stage_created = True

    def validate_remote_stage(self, _source):
        self.events.append("validate-stage")

    def provision_remote_runtime(self):
        self.events.append("runtime")

    def maintenance_relation(self):
        self.events.append("maintenance-status")
        return "inactive"

    def active_queue_jobs(self):
        self.events.append("queue")
        return 0

    def drain_worker(self):
        self.events.append("drain")
        self.state.restore_previous_agent = True

    def acquire_submission_gate(self):
        self.events.append("gate")
        self.state.submission_gate_active = True

    def wait_for_submitter_drain(self):
        self.events.append("submitter-drain")

    def enter_maintenance(self):
        self.events.append("maintenance-enter")
        self.state.maintenance_active = True

    def cutover_remote(self, _source):
        self.events.append("cutover")
        self.state.cutover_started = True
        self.state.remote_stage_created = False

    def coordinator_seen(self):
        self.events.append("seen")
        return "old"

    def install_launchagent(self, _release, _source):
        self.events.append("install-agent")
        self.state.restore_previous_agent = False

    def wait_for_heartbeat(self, previous_seen):
        self.events.append(f"heartbeat:{previous_seen}")
        return {"workers": []}

    def finalize_upgrade(self):
        self.events.append("finalize")
        self.state.maintenance_active = False
        self.state.submission_gate_active = False

    def retire_legacy_layout(self):
        self.events.append("retire")

    def refresh_dashboard(self):
        self.events.append("dashboard")

    def prune_local_releases(self, _release):
        self.events.append("prune")

    def cleanup(self):
        self.events.append("cleanup")


class VanComputeDeploymentTests(unittest.TestCase):
    def setUp(self):
        self.source = deployer.SourceRelease(
            files=(),
            source_fingerprint="a" * 64,
            deployment_fingerprint="b" * 64,
            pi_version="a" * 24,
            mac_version="b" * 24,
            dataset_source=None,
            dataset_fingerprint="none",
            allow_unsandboxed=False,
        )

    def make_installer(
        self, directory, *, options=None, local=None, remote=None, **kwargs
    ):
        return deployer.Installer(
            options or deployer.Options(),
            environment=kwargs.pop("environment", {}),
            home=Path(directory) / "home",
            script=INSTALLER,
            local=local or FakeLocal(),
            remote=remote or FakeRemote(),
            stdout=kwargs.pop("stdout", io.StringIO()),
            stderr=kwargs.pop("stderr", io.StringIO()),
            sleep=kwargs.pop("sleep", lambda _seconds: None),
            monotonic=kwargs.pop("monotonic", lambda: 0.0),
            **kwargs,
        )

    def make_source(self, marker):
        return deployer.SourceRelease(
            files=(),
            source_fingerprint=marker * 64,
            deployment_fingerprint=marker * 64,
            pi_version=marker * 24,
            mac_version=marker * 24,
            dataset_source=None,
            dataset_fingerprint="none",
            allow_unsandboxed=False,
        )

    def create_owned_release(self, installer, source):
        release = installer.paths.release_parent / source.mac_version
        (release / "app" / "van_compute").mkdir(parents=True)
        (release / "app" / "van_compute" / "worker.py").write_text(
            "# owned release\n", encoding="utf-8"
        )
        (release / deployer.SOURCE_HASH_FILE).write_text(
            source.source_fingerprint + "\n", encoding="utf-8"
        )
        (release / deployer.DEPLOYMENT_HASH_FILE).write_text(
            source.deployment_fingerprint + "\n", encoding="utf-8"
        )
        (release / deployer.PROVENANCE_FILE).write_text(
            json.dumps(installer.provenance(source, "mac-worker")) + "\n",
            encoding="utf-8",
        )
        if source.files:
            installer._copy_source_tree(release, source)
        installer._write_manifest(release)
        return release

    def point_launchagent_at(self, installer, release):
        installer.paths.target_dir.mkdir(parents=True, exist_ok=True)
        payload = plistlib.loads(installer.paths.source_plist.read_bytes())
        payload["ProgramArguments"] = [
            str(release / "venv/bin/python"),
            "-P",
            "-m",
            "van_compute.worker",
            "--serve",
        ]
        payload["EnvironmentVariables"] = {"PYTHONPATH": str(release / "app")}
        installer.paths.target_plist.write_bytes(plistlib.dumps(payload))

    def test_dry_run_is_local_only_and_side_effect_free(self):
        with tempfile.TemporaryDirectory() as directory:
            out = io.StringIO()
            local = FakeLocal()
            remote = FakeRemote()
            installer = self.make_installer(
                directory,
                options=deployer.Options(dry_run=True, if_needed=True),
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
                deployer.DeploymentError, "regular non-symlink file"
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
            exec(compile(deployer.SANDBOX_DENIAL_PROBE, "<sandbox-probe>", "exec"), {})
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
                compile(deployer.SANDBOX_DENIAL_PROBE, "<sandbox-probe>", "exec"),
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
        self.assertEqual(deployer.parse_arguments([]), deployer.Options())
        self.assertEqual(
            deployer.parse_arguments(["--if-needed", "--dry-run"]),
            deployer.Options(if_needed=True, dry_run=True),
        )
        with mock.patch("sys.stderr", new=io.StringIO()):
            with self.assertRaises(SystemExit):
                deployer.parse_arguments(["--pi-only"])

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
        installer = WorkflowInstaller(deployer.Options(), self.source)
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
        installer = WorkflowInstaller(deployer.Options(if_needed=True), self.source)
        installer.deployment_current = mock.Mock(return_value=True)
        self.assertEqual(installer.execute(), 0)
        self.assertEqual(installer.events, ["source"])
        self.assertIn("deployment is current", installer.out.getvalue())

    def test_stage_validation_failure_happens_before_drain_or_cutover(self):
        installer = WorkflowInstaller(deployer.Options(), self.source)

        def fail(_source):
            installer.events.append("validate-stage")
            raise deployer.DeploymentError("bad stage")

        installer.validate_remote_stage = fail
        with self.assertRaisesRegex(deployer.DeploymentError, "bad stage"):
            installer.execute()
        self.assertNotIn("drain", installer.events)
        self.assertNotIn("gate", installer.events)
        self.assertNotIn("cutover", installer.events)
        self.assertEqual(installer.events[-2:], ["cleanup", "lock-close"])

    def test_gate_denial_never_enters_maintenance_or_cutover(self):
        installer = WorkflowInstaller(deployer.Options(), self.source)

        def deny():
            installer.events.append("gate")
            raise deployer.DeploymentError("gate denied")

        installer.acquire_submission_gate = deny
        with self.assertRaisesRegex(deployer.DeploymentError, "gate denied"):
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
                options=deployer.Options(submitter_timeout=1),
                monotonic=lambda: next(ticks),
            )
            installer.active_submitters = mock.Mock(return_value=1)
            installer.active_queue_jobs = mock.Mock(return_value=0)
            with self.assertRaisesRegex(deployer.DeploymentError, "did not drain"):
                installer.wait_for_submitter_drain()
        self.assertFalse(installer.state.cutover_started)
        self.assertFalse(installer.state.maintenance_active)

    def test_pre_cutover_cleanup_restores_gate_before_previous_worker(self):
        local = FakeLocal()
        remote = FakeRemote()
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory, local=local, remote=remote)
            installer.owner = "installer-00000000-0000-0000-0000-000000000000"
            installer.state.upgrade_public_root = deployer.REMOTE_SCRIPTS
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
            installer.state.upgrade_public_root = deployer.REMOTE_SCRIPTS
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
        for mount_status in (32, 0, 1):
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
                if mount_status != 32:
                    retired_runtime.mkdir(parents=True)
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
                    result.returncode, 0 if mount_status == 32 else 1, result.stderr
                )
                self.assertFalse(
                    removal_log.exists(),
                    "retirement tried deletion without a proven unmounted target",
                )
                self.assertEqual(frozen.read_bytes(), b"frozen rollback copy\n")

    def test_existing_same_version_release_is_verified_and_reused(self):
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory)
            source = installer.build_source_release()
            release = self.create_owned_release(installer, source)
            installer._validate_mac_release = mock.Mock()
            installer._install_formulae = mock.Mock(
                side_effect=AssertionError("must not rebuild")
            )
            self.assertEqual(installer.prepare_mac_release(source), release)
        installer._validate_mac_release.assert_called_once_with(release, source)
        installer._install_formulae.assert_not_called()

    def test_release_verification_rejects_extra_symlink_directory_and_special_entry(
        self,
    ):
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory)
            release = self.create_owned_release(installer, self.source)
            linked = release / "linked-directory"
            linked.symlink_to(release / "app", target_is_directory=True)
            with self.assertRaisesRegex(deployer.DeploymentError, "contains a symlink"):
                installer._verify_release(release, self.source)
            linked.unlink()
            fifo = release / "unexpected.fifo"
            os.mkfifo(fifo)
            try:
                with self.assertRaisesRegex(deployer.DeploymentError, "special entry"):
                    installer._verify_release(release, self.source)
            finally:
                fifo.unlink()

    def test_remote_reuse_verifies_real_files_and_planned_source(self):
        remote = FakeRemote()
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory, remote=remote)
            installer.cutover_remote(self.source)
            code = remote.scripts["cutover"].split("<<'PY'\n", 1)[1].split("\nPY", 1)[0]
            root = Path(directory)
            release = root / "release"
            (release / "van_compute").mkdir(parents=True)
            payload = release / "van_compute/worker.py"
            payload.write_text("# original\n")
            (release / "source.sha256").write_text("a" * 64)
            installer._write_manifest(release)
            staged = root / "staged"
            shutil.copytree(release, staged)

            def verify():
                with mock.patch.object(
                    sys, "argv", ["verify", str(release), str(staged)]
                ):
                    exec(compile(code, "<remote-release-check>", "exec"), {})

            verify()
            linked = release / "linked"
            linked.symlink_to(release / "van_compute", target_is_directory=True)
            with self.assertRaisesRegex(SystemExit, "symlink"):
                verify()
            linked.unlink()
            extra = release / "nested/manifest.json"
            extra.parent.mkdir()
            extra.write_text("{}")
            with self.assertRaisesRegex(SystemExit, "file set mismatch"):
                verify()
            extra.unlink()
            payload.write_text("# modified existing release\n")
            installer._write_manifest(release)
            with self.assertRaisesRegex(SystemExit, "planned source"):
                verify()

    def test_all_remote_release_links_require_one_immediate_24_hex_target(self):
        remote = FakeRemote(responses={"preflight": deployer.REMOTE_SCRIPTS + "\n"})
        local = FakeLocal(launch_states=["loaded\n"])
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory, local=local, remote=remote)
            source = installer.build_source_release()
            release = self.create_owned_release(installer, source)
            self.point_launchagent_at(installer, release)
            installer.owner = "installer-00000000-0000-0000-0000-000000000000"
            self.assertTrue(installer.deployment_current(source))
            installer.remote_preflight()
            installer.cutover_remote(source)
        for script in remote.scripts.values():
            self.assertEqual(script.count('check_release_target "$root"'), 1)
        prefix = "/home/pi/van_compute"
        for target, expected in (
            (prefix + "/releases/" + "a" * 24, 0),
            (prefix + "/releases/nested/" + "a" * 24, 1),
            (prefix + "/releases/" + "A" * 24, 1),
            (prefix + "/releases/" + "a" * 23, 1),
            ("/foreign/releases/" + "a" * 24, 1),
        ):
            with self.subTest(target=target):
                result = subprocess.run(
                    [
                        "/bin/sh",
                        "-c",
                        deployer.RELEASE_LINK_GUARD
                        + '\ncheck_release_target "$1" "$2"',
                        "guard",
                        prefix,
                        target,
                    ],
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(result.returncode, expected)

    def test_release_pruning_keeps_actual_prior_and_foreign_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory)
            source_a = self.make_source("a")
            source_b = self.make_source("b")
            source_c = self.make_source("c")
            release_a = self.create_owned_release(installer, source_a)
            release_b = self.create_owned_release(installer, source_b)
            release_c = self.create_owned_release(installer, source_c)
            foreign = installer.paths.release_parent / ("d" * 24)
            foreign.mkdir()
            (foreign / "foreign.txt").write_text(
                "not installer-owned\n", encoding="utf-8"
            )
            self.point_launchagent_at(installer, release_a)

            installer.capture_prior_release()
            installer.prune_local_releases(release_c)

            self.assertEqual(installer.prior_release, release_a)
            self.assertTrue(release_a.is_dir())
            self.assertFalse(release_b.exists())
            self.assertTrue(release_c.is_dir())
            self.assertTrue(foreign.is_dir())

    def test_ambiguous_prior_release_retains_everything(self):
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory)
            release_b = self.create_owned_release(installer, self.make_source("b"))
            release_c = self.create_owned_release(installer, self.make_source("c"))
            installer.paths.target_dir.mkdir(parents=True)
            installer.paths.target_plist.write_text("not a plist\n", encoding="utf-8")

            installer.capture_prior_release()
            installer.prune_local_releases(release_c)

            self.assertTrue(installer.release_retention_ambiguous)
            self.assertTrue(release_b.is_dir())
            self.assertTrue(release_c.is_dir())

    def test_deployment_source_manifest_captures_only_owned_package_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory)
            sources = set(installer.source_paths())
        expected_package_files = {
            *COMPUTE_ROOT.glob("*.py"),
            *(COMPUTE_ROOT / "entrypoints").glob("*.py"),
            COMPUTE_ROOT / "configs" / "van-compute-broker.service",
            COMPUTE_ROOT / "configs" / "van-compute-obd.example.json",
        }
        actual_package_files = {
            path for path in sources if path.is_relative_to(COMPUTE_ROOT)
        }
        self.assertEqual(actual_package_files, expected_package_files)
        self.assertIn(INSTALLER, sources)
        self.assertIn(SHIM, sources)

    def test_source_allowlist_includes_new_modules_but_excludes_data_and_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            app = Path(directory) / "app"
            for relative in (
                Path("macbook/scripts/install_van_compute_worker.py"),
                Path("macbook/scripts/install_van_compute_worker.zsh"),
                Path("macbook/launchagents") / f"{deployer.LABEL}.plist",
            ):
                destination = app / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(REPOSITORY_ROOT / relative, destination)
            package = app / "van_compute"
            (package / "entrypoints").mkdir(parents=True)
            (package / "configs").mkdir()
            for relative in ("__init__.py", "engine.py", "entrypoints/van_compute.py"):
                (package / relative).write_text("# source\n", encoding="utf-8")
            for relative in (
                "configs/van-compute-broker.service",
                "configs/van-compute-obd.example.json",
            ):
                (package / relative).write_text("owned\n", encoding="utf-8")
            for relative in (
                ".env",
                ".DS_Store",
                "cache.sqlite3",
                "entrypoints/cache.db",
                "configs/private.json",
            ):
                (package / relative).write_text("private\n", encoding="utf-8")
            installer = deployer.Installer(
                deployer.Options(),
                environment={},
                home=Path(directory) / "home",
                script=app / "macbook/scripts/install_van_compute_worker.py",
                local=FakeLocal(),
                remote=FakeRemote(),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )
            relative = {
                path.relative_to(app.resolve()).as_posix()
                for path in installer.source_paths()
            }
        self.assertIn("van_compute/engine.py", relative)
        self.assertNotIn("van_compute/.env", relative)
        self.assertNotIn("van_compute/cache.sqlite3", relative)
        self.assertNotIn("van_compute/configs/private.json", relative)

    def test_selected_source_symlink_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            app = Path(directory) / "app"
            for relative in (
                Path("macbook/scripts/install_van_compute_worker.py"),
                Path("macbook/scripts/install_van_compute_worker.zsh"),
                Path("macbook/launchagents") / f"{deployer.LABEL}.plist",
            ):
                destination = app / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(REPOSITORY_ROOT / relative, destination)
            shutil.copytree(COMPUTE_ROOT, app / "van_compute")
            target = app / "real.py"
            target.write_text("# target\n", encoding="utf-8")
            module = app / "van_compute" / "linked.py"
            module.symlink_to(target)
            installer = deployer.Installer(
                deployer.Options(),
                environment={},
                home=Path(directory) / "home",
                script=app / "macbook/scripts/install_van_compute_worker.py",
                local=FakeLocal(),
                remote=FakeRemote(),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )
            with self.assertRaisesRegex(deployer.DeploymentError, "non-symlink"):
                installer.source_paths()

    def test_worker_and_broker_provenance_share_exact_source_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory)
            worker = installer.provenance(self.source, "mac-worker")
            broker = installer.provenance(self.source, "pi-broker")
        self.assertEqual(worker["source_sha256"], broker["source_sha256"])
        self.assertEqual(
            worker["deployment_sha256"], self.source.deployment_fingerprint
        )
        self.assertEqual(worker["host"], "pi@vanpi.lan")
        self.assertNotIn("deployment_sha256", broker)
        self.assertNotIn("dataset_sha256", broker)
        self.assertNotEqual(worker["kind"], broker["kind"])

    def test_source_edit_between_planning_and_copy_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            seed_installer = self.make_installer(directory)
            seed_source = seed_installer.build_source_release()
            frozen_release = root / "frozen"
            frozen_release.mkdir()
            seed_installer._copy_source_tree(frozen_release, seed_source)
            frozen_app = frozen_release / "app"
            installer = deployer.Installer(
                deployer.Options(),
                environment={},
                home=root / "other-home",
                script=frozen_app / "macbook/scripts/install_van_compute_worker.py",
                local=FakeLocal(),
                remote=FakeRemote(),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )
            planned = installer.build_source_release()
            (frozen_app / "van_compute" / "protocol.py").write_text(
                "# changed after planning\n", encoding="utf-8"
            )
            staging = root / "staging"
            staging.mkdir()
            installer._copy_source_tree(staging, planned)
            with self.assertRaisesRegex(
                deployer.DeploymentError, "changed after planning"
            ):
                installer._verify_staged_source(staging / "app", planned)

    def test_mac_release_carries_a_runnable_frozen_installer_source_tree(self):
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory)
            staging = Path(directory) / "release"
            staging.mkdir()
            source = installer.build_source_release()
            installer._copy_source_tree(staging, source)
            frozen = staging / "app" / "macbook" / "scripts" / INSTALLER.name
            paths = deployer.Paths.discover(frozen, Path(directory) / "other-home")
            self.assertEqual(paths.repo_root, (staging / "app").resolve())
            self.assertTrue((paths.repo_root / "van_compute" / "worker.py").is_file())
            self.assertTrue(paths.source_plist.is_file())
            self.assertTrue(frozen.is_file())

    def test_generated_launchagent_runs_the_package_with_pinned_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory)
            release = installer.paths.release_parent / self.source.mac_version
            payload = plistlib.loads(installer.build_launchagent(release, self.source))
        arguments = payload["ProgramArguments"]
        self.assertEqual(
            arguments[:4],
            [str(release / "venv/bin/python"), "-P", "-m", "van_compute.worker"],
        )
        self.assertIn("--serve", arguments)
        self.assertIn("--sandbox-profile", arguments)
        self.assertEqual(
            payload["EnvironmentVariables"]["PYTHONPATH"], str(release / "app")
        )
        self.assertEqual(
            payload["EnvironmentVariables"]["VAN_COMPUTE_SOURCE_SHA256"],
            self.source.source_fingerprint,
        )

    def test_package_entrypoints_resolve_only_the_installed_current_release(self):
        self.assertEqual(
            QUEUE_CLI.read_bytes().splitlines()[0], b"#!/usr/bin/python3 -P"
        )
        self.assertEqual(
            FRONTEND_CLI.read_bytes().splitlines()[0], b"#!/usr/bin/python3 -P"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scripts = root / "scripts"
            release = root / "releases" / "r1"
            outside = root / "outside"
            scripts.mkdir(parents=True)
            outside.mkdir()
            shutil.copytree(COMPUTE_ROOT, release / "van_compute")
            (root / "current").symlink_to(
                Path("releases") / "r1", target_is_directory=True
            )
            shutil.copy2(QUEUE_CLI, scripts / "van_compute.py")
            shutil.copy2(FRONTEND_CLI, scripts / "pi_compute.py")
            environment = os.environ.copy()
            environment.pop("PYTHONPATH", None)
            for script in (scripts / "van_compute.py", scripts / "pi_compute.py"):
                result = subprocess.run(
                    [sys.executable, "-P", str(script), "--help"],
                    cwd=outside,
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("usage:", result.stdout)

    def test_upgrade_gate_blocks_both_public_entrypoints(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            scripts = root / "scripts"
            release = root / "releases" / "r1"
            scripts.mkdir(parents=True)
            shutil.copytree(COMPUTE_ROOT, release / "van_compute")
            (root / "current").symlink_to(
                Path("releases") / "r1", target_is_directory=True
            )
            shutil.copy2(UPGRADE_GATE, scripts / "van_compute.py")
            shutil.copy2(FRONTEND_CLI, scripts / "pi_compute.py")
            for command in (
                [scripts / "van_compute.py", "submit", "repo-tests"],
                [scripts / "pi_compute.py", "run", "repo-tests"],
            ):
                result = subprocess.run(
                    [sys.executable, "-P", *(str(item) for item in command)],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
                self.assertEqual(result.returncode, 75, result.stderr)
                self.assertEqual(
                    result.stderr,
                    "van-compute is being upgraded; retry this command shortly\n",
                )

    def test_broker_and_dashboard_units_consume_the_package_current_link(self):
        broker = parse_directives(BROKER_SERVICE.read_text(encoding="utf-8"))
        broker_environment = parse_environment(broker)
        self.assertEqual(
            broker_environment["PYTHONPATH"], "/home/pi/van_compute/current"
        )
        self.assertEqual(broker_environment["PYTHONDONTWRITEBYTECODE"], "1")
        inaccessible = broker["InaccessiblePaths"][0].split()
        self.assertNotIn("/run/systemd", inaccessible)
        self.assertEqual(broker["RestrictAddressFamilies"], ["AF_UNIX AF_NETLINK"])
        self.assertEqual(broker["TasksMax"], ["256"])
        pre_start = command_arguments(broker["ExecStartPre"][0])
        start = command_arguments(broker["ExecStart"][0])
        self.assertEqual(
            pre_start[:4],
            ["/usr/bin/python3", "-P", "-m", "van_compute.broker"],
        )
        self.assertIn("--self-test", pre_start)
        self.assertEqual(
            start[:4],
            ["/usr/bin/python3", "-P", "-m", "van_compute.broker"],
        )
        self.assertIn("/home/pi/van_compute/venv/bin/python3", pre_start)
        self.assertIn("/home/pi/van_compute/venv/bin/python3", start)
        dashboard = parse_directives(DASHBOARD_SERVICE.read_text(encoding="utf-8"))
        dashboard_environment = parse_environment(dashboard)
        self.assertIn(
            "/home/pi/van_compute/current",
            dashboard_environment["PYTHONPATH"].split(":"),
        )
        self.assertIn(
            [
                "/usr/bin/test",
                "-r",
                "/home/pi/van_compute/current/van_compute/metrics.py",
            ],
            [command_arguments(value) for value in dashboard["ExecStartPre"]],
        )

    def test_staged_dashboard_imports_only_the_compute_metrics_contract(self):
        plan = deploy_python.build_plan(REPOSITORY_ROOT, mode="stage")
        with tempfile.TemporaryDirectory() as directory:
            staged = Path(directory)
            for relative in plan["manifest"]["files"]:
                if not relative.endswith(".py"):
                    continue
                destination = staged / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(REPOSITORY_ROOT / relative, destination)
            metrics_package = staged / "van_compute"
            metrics_package.mkdir(parents=True, exist_ok=True)
            (metrics_package / "__init__.py").write_text("", encoding="utf-8")
            (metrics_package / "metrics.py").write_text(
                "class ComputeMetricsError(RuntimeError):\n"
                "    pass\n\n"
                "class ComputeMetricsReader:\n"
                "    def __init__(self, root):\n"
                "        self.root = root\n",
                encoding="utf-8",
            )
            environment = os.environ.copy()
            environment["PYTHONPATH"] = str(staged)
            environment["VAN_DASHBOARD_COMPUTE_ROOT"] = str(staged / "compute")
            environment["VAN_DASHBOARD_STATE_PATH"] = str(staged / "state.json")
            environment["VAN_DASHBOARD_RUNTIME_DIR"] = str(staged / "runtime")
            result = subprocess.run(
                [
                    sys.executable,
                    "-P",
                    "-c",
                    (
                        "import os; "
                        "from van_compute import metrics; "
                        "assert {name for name in vars(metrics) if not name.startswith('_')} "
                        "== {'ComputeMetricsError', 'ComputeMetricsReader'}; "
                        "import pi.apps.van_dashboard as dashboard; "
                        "from pi.apps.van_dashboard import runtime; "
                        "app = dashboard.create_app(); "
                        "assert app is not None; "
                        "assert isinstance(runtime.compute_monitor, metrics.ComputeMetricsReader); "
                        "assert runtime.compute_monitor.root == os.environ['VAN_DASHBOARD_COMPUTE_ROOT']"
                    ),
                ],
                cwd=staged,
                env=environment,
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_example_tasks_remain_a_valid_repository_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            source_root = Path(directory)
            shutil.copy2(EXAMPLE_TASKS, source_root / protocol.REPO_MANIFEST)
            tasks = protocol.load_repo_tasks(source_root)
        self.assertIn("repo-tests", tasks)
        self.assertIn("oem-corpus-search", tasks)
        self.assertEqual(tasks["candump-diagnostic-wire-tcm"].maximum_inputs, 512)
        self.assertEqual(tasks["can-timeseries-correlate-tcm"].maximum_inputs, 513)

    def test_same_version_repair_retains_earlier_rollback_and_orphan(self):
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory)
            prior = self.create_owned_release(installer, self.make_source("a"))
            current = self.create_owned_release(installer, self.make_source("b"))
            orphan = self.create_owned_release(installer, self.make_source("c"))
            self.point_launchagent_at(installer, current)
            installer.capture_prior_release()
            installer.prune_local_releases(current)
            self.assertTrue(all(path.exists() for path in (prior, current, orphan)))

    def test_retention_never_recurses_into_a_mount(self):
        for nested in (False, True):
            with self.subTest(
                nested=nested
            ), tempfile.TemporaryDirectory() as directory:
                installer = self.make_installer(directory)
                retired = self.create_owned_release(installer, self.make_source("a"))
                current = self.create_owned_release(installer, self.make_source("b"))
                mount = retired / "app" if nested else retired
                with mock.patch.object(
                    deployer.os.path, "ismount", side_effect=lambda path: path == mount
                ):
                    installer.prune_local_releases(current)
                self.assertTrue(retired.exists())

    def test_reused_release_source_cannot_change_with_a_rewritten_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory)
            source = installer.build_source_release()
            release = self.create_owned_release(installer, source)
            installer._verify_release(release, source)
            (release / "app/van_compute/worker.py").write_text("# substituted\n")
            installer._write_manifest(release)
            with self.assertRaisesRegex(
                deployer.DeploymentError, "changed after planning"
            ):
                installer._verify_release(release, source)

    def test_launchagent_atomic_replace_stages_on_the_target_filesystem(self):
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory)
            release = self.create_owned_release(installer, self.source)
            original = os.replace

            def replace_on_same_filesystem(source, target):
                self.assertEqual(Path(source).parent, Path(target).parent)
                original(source, target)

            with mock.patch.object(
                deployer.os, "replace", side_effect=replace_on_same_filesystem
            ):
                installer.install_launchagent(release, self.source)
            self.assertEqual(
                plistlib.loads(installer.paths.target_plist.read_bytes())["Label"],
                deployer.LABEL,
            )
            self.assertEqual(installer.paths.target_plist.stat().st_mode & 0o777, 0o600)

    def test_heartbeat_requires_a_new_coordinator_before_completion(self):
        def heartbeat(seen):
            return json.dumps(
                {
                    "workers": [
                        {
                            "worker": "m4mac",
                            "available": True,
                            "seen_at": seen,
                            "slots_total": 10,
                            "slots_busy": 0,
                        }
                    ]
                }
            )

        remote = FakeRemote(
            responses={"heartbeat": [heartbeat("old"), heartbeat("fresh")]}
        )
        ticks = iter([0.0, 0.0])
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(
                directory, remote=remote, monotonic=lambda: next(ticks)
            )
            result = installer.wait_for_heartbeat("old")
        self.assertEqual(result["workers"][0]["seen_at"], "fresh")
        self.assertEqual([call[0] for call in remote.calls], ["heartbeat", "heartbeat"])

    def test_heartbeat_timeout_cannot_release_maintenance(self):
        remote = FakeRemote(responses={"heartbeat": '{"workers": []}'})
        ticks = iter([0.0, 0.0, 1.0])
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(
                directory,
                remote=remote,
                options=deployer.Options(heartbeat_timeout=1),
                monotonic=lambda: next(ticks),
            )
            installer.state.cutover_started = True
            installer.state.maintenance_active = True
            with self.assertRaisesRegex(deployer.DeploymentError, "fresh 10-slot"):
                installer.wait_for_heartbeat("old")
            self.assertTrue(installer.state.maintenance_active)
        self.assertNotIn("finalize", [call[0] for call in remote.calls])

    def test_foreign_maintenance_owner_stops_before_drain(self):
        installer = WorkflowInstaller(deployer.Options(), self.source)
        installer.maintenance_relation = lambda: "other"
        with self.assertRaisesRegex(deployer.DeploymentError, "different installer"):
            installer.execute()
        for phase in ("drain", "gate", "cutover", "install-agent", "finalize"):
            self.assertNotIn(phase, installer.events)

    def test_unknown_loaded_worker_is_never_disabled(self):
        local = FakeLocal(launch_states=["pid = 123\nunknown-program\n"])
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory, local=local)
            release = self.create_owned_release(installer, self.source)
            self.point_launchagent_at(installer, release)
            with self.assertRaisesRegex(
                deployer.DeploymentError, "not the supported persistent"
            ):
                installer.drain_worker()
        self.assertEqual([call[0][1] for call in local.calls], ["print"])

    def test_dataset_changes_after_planning_cannot_replace_installed_config(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            supplied = root / "supplied.json"
            supplied.write_bytes(b'{"datasets": {}}\n')
            installer = self.make_installer(
                directory, environment={"VAN_COMPUTE_DATASET_CONFIG": str(supplied)}
            )
            installer.paths.support_root.mkdir(parents=True)
            installed = installer.paths.dataset_target
            installed.write_bytes(b'{"datasets":{}}\n')
            original = installed.read_bytes()
            planned = installer.build_source_release()
            supplied.write_bytes(b'{"datasets": {}}  \n')
            with self.assertRaisesRegex(
                deployer.DeploymentError, "changed after planning"
            ):
                installer.install_dataset(planned)
            self.assertEqual(installed.read_bytes(), original)

    def test_retained_dataset_must_still_match_the_planned_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory)
            installer.paths.support_root.mkdir(parents=True)
            installed = installer.paths.dataset_target
            installed.write_bytes(b'{"datasets":{}}\n')
            planned = installer.build_source_release()
            installed.write_bytes(b'{"datasets": {}}\n')
            with self.assertRaisesRegex(
                deployer.DeploymentError, "changed after planning"
            ):
                installer.install_dataset(planned)


if __name__ == "__main__":
    unittest.main()
