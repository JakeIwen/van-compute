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

    @unittest.skipUnless(sys.version_info >= (3, 11), "Pi entrypoints require Python 3.11 -P")
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

    @unittest.skipUnless(sys.version_info >= (3, 11), "Pi entrypoints require Python 3.11 -P")
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

    @unittest.skipUnless(
        sys.version_info >= (3, 11) and importlib.util.find_spec("flask") is not None,
        "Pi -P/dashboard smoke needs Python 3.11 and Flask (covered by compute venv suite)",
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
                    installer_orchestrator.os.path, "ismount", side_effect=lambda path: path == mount
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
                DeploymentError, "changed after planning"
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
                installer_mac.os, "replace", side_effect=replace_on_same_filesystem
            ):
                installer.install_launchagent(release, self.source)
            launchctl = [
                call[0]
                for call in installer.local.calls
                if call[0][:1] == ["/bin/launchctl"]
            ]
            self.assertEqual(
                [command[1] for command in launchctl],
                ["bootout", "enable", "bootstrap", "kickstart"],
            )
            self.assertEqual(launchctl[1][2], launchctl[2][2] + "/" + installer_constants.LABEL)
            self.assertEqual(
                plistlib.loads(installer.paths.target_plist.read_bytes())["Label"],
                installer_constants.LABEL,
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
                options=Options(heartbeat_timeout=1),
                monotonic=lambda: next(ticks),
            )
            installer.state.cutover_started = True
            installer.state.maintenance_active = True
            with self.assertRaisesRegex(DeploymentError, "fresh 10-slot"):
                installer.wait_for_heartbeat("old")
            self.assertTrue(installer.state.maintenance_active)
        self.assertNotIn("finalize", [call[0] for call in remote.calls])

    def test_foreign_maintenance_owner_stops_before_drain(self):
        installer = WorkflowInstaller(Options(), self.source)
        installer.maintenance_relation = lambda: "other"
        with self.assertRaisesRegex(DeploymentError, "different installer"):
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
                DeploymentError, "not the supported persistent"
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
                DeploymentError, "changed after planning"
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
                DeploymentError, "changed after planning"
            ):
                installer.install_dataset(planned)

    def test_existing_remote_stage_is_refused_before_install(self):
        remote = FakeRemote(failures={"create-stage": "capture-only"})
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory, remote=remote)
            with self.assertRaisesRegex(DeploymentError, "capture-only"):
                installer.stage_remote_release(self.source, Path(directory))
            root = Path(directory)
            stage = root / "van-compute-install.fixture"
            stage.mkdir()
            marker = stage / "keep"
            marker.write_text("existing stage\n")
            log = root / "install-called"
            script = remote.scripts["create-stage"].replace(
                "/home/pi/.cache/van-compute-install.",
                str(root / "van-compute-install."),
            )
            fakes = 'log="$2"\ninstall() { printf attempted > "$log"; }\n'
            result = subprocess.run(
                ["/bin/sh", "-s", "--", str(stage), str(log)],
                input=fakes + script,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 1, result.stderr)
            self.assertFalse(log.exists())
            self.assertEqual(marker.read_text(), "existing stage\n")


if __name__ == "__main__":
    unittest.main()
