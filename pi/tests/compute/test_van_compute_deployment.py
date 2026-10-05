import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from pi import deploy_python
from pi.tests.unit_contract import command_arguments, parse_directives, parse_environment
from van_compute import protocol


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
COMPUTE_ROOT = REPOSITORY_ROOT / "van_compute"
SYNC_SCRIPT = REPOSITORY_ROOT / "pi" / "sync_scripts.sh"
INSTALLER = REPOSITORY_ROOT / "macbook" / "scripts" / "install_van_compute_worker.zsh"
QUEUE_CLI = COMPUTE_ROOT / "entrypoints" / "van_compute.py"
FRONTEND_CLI = COMPUTE_ROOT / "entrypoints" / "pi_compute.py"
UPGRADE_GATE = COMPUTE_ROOT / "upgrade_gate.py"
EXAMPLE_TASKS = COMPUTE_ROOT / "configs" / "van-compute-obd.example.json"
DASHBOARD_SERVICE = REPOSITORY_ROOT / "pi" / "services" / "van-dashboard.service"
BROKER_SERVICE = COMPUTE_ROOT / "configs" / "van-compute-broker.service"
UPDATE_SERVICES = REPOSITORY_ROOT / "pi" / "scripts" / "update_services.sh"


class VanComputeDeploymentTests(unittest.TestCase):
    def test_compute_deployment_sources_have_one_repository_root(self):
        for relative in (
            "__init__.py",
            "queue.py",
            "protocol.py",
            "frontend.py",
            "broker.py",
            "metrics.py",
            "worker.py",
            "upgrade_gate.py",
            "limited_child.py",
            "entrypoints/pi_compute.py",
            "entrypoints/van_compute.py",
            "configs/van-compute-broker.service",
            "configs/van-compute-obd.example.json",
        ):
            self.assertTrue((COMPUTE_ROOT / relative).is_file(), relative)

        for retired in (
            REPOSITORY_ROOT / "pi" / "van_compute",
            REPOSITORY_ROOT / "macbook" / "scripts" / "van_compute_worker.py",
            REPOSITORY_ROOT / "pi" / "scripts" / "compute",
            REPOSITORY_ROOT / "pi" / "services" / "van-compute-broker.service",
            REPOSITORY_ROOT / "pi" / "configs" / "van-compute-obd.example.json",
            REPOSITORY_ROOT / "shared" / "python" / "van_compute_metrics.py",
            REPOSITORY_ROOT / "shared" / "python" / "van_compute_protocol.py",
        ):
            self.assertFalse(retired.exists(), str(retired))

    def test_broker_unit_keeps_systemd_runtime_visible(self):
        service = BROKER_SERVICE.read_text(encoding="utf-8")

        inaccessible = next(
            line for line in service.splitlines() if line.startswith("InaccessiblePaths=")
        )
        self.assertNotIn("/run/systemd", inaccessible.split())
        self.assertIn("RestrictAddressFamilies=AF_UNIX AF_NETLINK", service)
        self.assertIn(
            "ExecStartPre=/usr/bin/python3 -P -m van_compute.broker --self-test",
            service,
        )
        self.assertIn(
            "ExecStart=/usr/bin/python3 -P -m van_compute.broker",
            service,
        )
        self.assertIn("Environment=PYTHONPATH=/home/pi/van_compute/current", service)
        self.assertIn("Environment=PYTHONDONTWRITEBYTECODE=1", service)
        self.assertIn("/home/pi/van_compute/venv/bin/python3", service)
        self.assertNotIn("/home/pi/scripts/compute", service)
        self.assertIn("TasksMax=256", service)

    def test_generic_sync_delegates_compute_to_conditional_installer(self):
        sync = SYNC_SCRIPT.read_text(encoding="utf-8")

        self.assertNotIn("--exclude '/compute/'", sync)
        self.assertIn('staged_services="$local_stage/services"', sync)
        self.assertIn(
            'scp $mux -r "$staged_services" "$staged_scripts"',
            sync,
        )
        self.assertNotIn(
            'scp $mux -r "$services" "$staged_scripts"',
            sync,
        )
        self.assertIn(
            'compute_installer="$dsc/macbook/scripts/install_van_compute_worker.zsh"',
            sync,
        )
        self.assertIn('"$compute_installer" --if-needed', sync)
        self.assertIn("conditional van_compute deployment failed", sync)
        self.assertGreater(
            sync.index('"$compute_installer" --if-needed'),
            sync.index('wait "$services_pid"'),
        )
        for child in (
            "home_pid",
            "dirs_pid",
            "services_pid",
            "smb_pid",
            "chmod_pid",
        ):
            self.assertIn(f'wait "${child}"', sync)
        self.assertIn("if (( sync_failed )); then", sync)
        self.assertNotIn("\nwait\n", sync)
        self.assertFalse(
            (
                REPOSITORY_ROOT
                / "pi"
                / "secrets"
                / "van-compute-datasets.json"
            ).exists(),
            "Mac-only dataset configuration would be copied by generic Pi sync",
        )

    def test_installer_deploys_example_tasks_from_configs_to_configs(self):
        installer = INSTALLER.read_text(encoding="utf-8")

        self.assertIn(
            '"$repo_root/pi/van_compute/configs/van-compute-obd.example.json"',
            installer,
        )
        self.assertIn('remote_root="/home/pi/van_compute"', installer)
        self.assertIn('remote_config_root="$remote_root/configs"', installer)
        config_installs = [
            line.strip()
            for line in installer.splitlines()
            if line.strip().startswith(
                "install -m 600 '$remote_stage/van-compute-obd.example.json'"
            )
        ]
        self.assertTrue(
            any(
                command.endswith(
                    "/home/pi/van_compute/configs/van-compute-obd.example.json"
                )
                or command.endswith(
                    "'$remote_config_root/van-compute-obd.example.json'"
                )
                for command in config_installs
            ),
            "example tasks are not installed under /home/pi/van_compute/configs",
        )

    def test_deployed_entrypoints_use_installed_release_and_upgrade_gate(self):
        self.assertEqual(QUEUE_CLI.read_bytes().splitlines()[0], b"#!/usr/bin/python3 -P")
        self.assertEqual(
            FRONTEND_CLI.read_bytes().splitlines()[0], b"#!/usr/bin/python3 -P"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pi_root = root / "pi"
            scripts = pi_root / "scripts"
            release = pi_root / "releases" / "r1"
            package = release / "van_compute"
            current = pi_root / "current"
            outside = root / "outside"
            scripts.mkdir(parents=True)
            release.mkdir(parents=True)
            outside.mkdir()
            shutil.copytree(COMPUTE_ROOT, package)
            current.symlink_to(Path("releases") / "r1", target_is_directory=True)
            queue_script = scripts / "van_compute.py"
            frontend_script = scripts / "pi_compute.py"
            shutil.copy2(QUEUE_CLI, queue_script)
            shutil.copy2(FRONTEND_CLI, frontend_script)

            environment = os.environ.copy()
            environment.pop("PYTHONPATH", None)
            for script in (queue_script, frontend_script):
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

            probe = subprocess.run(
                [
                    sys.executable,
                    "-P",
                    "-c",
                    (
                        "import runpy, sys\n"
                        "script = sys.argv[1]\n"
                        "sys.argv = [script, '--help']\n"
                        "try:\n"
                        "    runpy.run_path(script, run_name='__main__')\n"
                        "except SystemExit as exc:\n"
                        "    assert exc.code == 0, exc.code\n"
                        "import van_compute\n"
                        "print(van_compute.__file__)\n"
                    ),
                    str(queue_script),
                ],
                cwd=outside,
                env=environment,
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            self.assertEqual(probe.returncode, 0, probe.stderr)
            imported = Path(probe.stdout.splitlines()[-1]).resolve()
            self.assertTrue(imported.is_relative_to(current.resolve()), imported)
            self.assertFalse(imported.is_relative_to(REPOSITORY_ROOT), imported)

            gate_marker = b"UPGRADE_GATE = True"
            self.assertIn(gate_marker, UPGRADE_GATE.read_bytes().splitlines())
            shutil.copy2(UPGRADE_GATE, queue_script)
            expected_gate_message = (
                "van-compute is being upgraded; retry this command shortly\n"
            )
            for command in (
                [str(queue_script), "submit", "repo-tests"],
                [str(frontend_script), "run", "repo-tests"],
                [str(frontend_script), "tasks"],
            ):
                result = subprocess.run(
                    [sys.executable, "-P", *command],
                    cwd=outside,
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
                self.assertEqual(result.returncode, 75, result.stderr)
                self.assertEqual(result.stdout, "")
                self.assertEqual(result.stderr, expected_gate_message)

    def test_mac_release_contains_the_worker_protocol_package(self):
        with tempfile.TemporaryDirectory() as directory:
            app = Path(directory) / "app"
            package_dir = app / "van_compute"
            package_dir.parent.mkdir(parents=True)
            shutil.copytree(COMPUTE_ROOT, package_dir)

            environment = os.environ.copy()
            environment["PYTHONPATH"] = str(app)
            result = subprocess.run(
                [sys.executable, "-P", "-m", "van_compute.worker", "--help"],
                cwd=directory,
                env=environment,
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("usage:", result.stdout)

    def test_dashboard_declares_restart_triggers_for_compute_assets(self):
        service = DASHBOARD_SERVICE.read_text(encoding="utf-8")
        updater = UPDATE_SERVICES.read_text(encoding="utf-8")
        directives = parse_directives(service)
        environment = parse_environment(directives)

        pythonpath = environment.get("PYTHONPATH", "").split(":")
        self.assertIn("/home/pi/scripts/python-packages/current", pythonpath)
        self.assertIn("/home/pi/van_compute/current", pythonpath)

        pre_commands = [
            command_arguments(value) for value in directives.get("ExecStartPre", [])
        ]
        self.assertIn(
            [
                "/usr/bin/test",
                "-r",
                "/home/pi/van_compute/current/van_compute/metrics.py",
            ],
            pre_commands,
        )

        start = command_arguments(directives["ExecStart"][0])
        self.assertGreaterEqual(len(start), 4)
        self.assertEqual(start[:2], ["/usr/bin/python3", "-P"])
        self.assertEqual(start[2:4], ["-m", "pi.apps.van_dashboard"])
        module = start[3]
        module_parts = module.split(".")
        module_path = REPOSITORY_ROOT.joinpath(*module_parts)
        self.assertTrue(module_path.is_dir(), str(module_path))
        resolved = module_path / "__main__.py"
        self.assertTrue(resolved.is_file(), str(resolved))
        relative_module_dir = module_path.relative_to(REPOSITORY_ROOT).as_posix()
        self.assertTrue(
            any(
                relative_module_dir == allowlist
                or relative_module_dir.startswith(f"{allowlist}/")
                for allowlist in deploy_python.MODULE_DIRS
            ),
            relative_module_dir,
        )

        plan = deploy_python.build_plan(REPOSITORY_ROOT, mode="stage")
        self.assertNotIn("legacy", plan["manifest"])
        with self.assertRaises(ValueError):
            deploy_python.build_plan(REPOSITORY_ROOT, mode="legacy")

        with tempfile.TemporaryDirectory() as directory:
            staged = Path(directory)
            self.assertFalse(staged.is_relative_to(REPOSITORY_ROOT))
            for relative in plan["manifest"]["files"]:
                if not relative.endswith(".py"):
                    continue
                destination = staged / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(REPOSITORY_ROOT / relative, destination)
            metrics_package = staged / "van_compute"
            metrics_package.mkdir(parents=True, exist_ok=True)
            shutil.copy2(
                COMPUTE_ROOT / "__init__.py",
                metrics_package / "__init__.py",
            )
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
                        "assert not hasattr(metrics, 'TASK_NAME_RE'); "
                        "import pi.apps.van_dashboard as dashboard; "
                        "from pi.apps.van_dashboard import runtime; "
                        "app = dashboard.create_app(); "
                        "assert app is not None; "
                        "assert isinstance(runtime.compute_monitor, metrics.ComputeMetricsReader); "
                        "assert runtime.compute_monitor.root == " "os.environ['VAN_DASHBOARD_COMPUTE_ROOT']"
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

        self.assertIn("'s/^ExecStartPre=//p'", updater)
        self.assertIn(
            'for staged_script in "$staged_scripts"/*',
            updater,
        )
        # pi/tests/test_update_services.py exercises the mode behaviour itself.
        self.assertIn('chmod 770 "$staged_script"', updater)
        self.assertNotIn('chmod 770 "$live_scripts"/*', updater)

        installer = INSTALLER.read_text(encoding="utf-8")
        for asset in ("van_dashboard.html", "van_dashboard.js", "van_dashboard.css"):
            self.assertNotIn(asset, installer)
        self.assertIn(
            "/usr/bin/systemctl cat van-dashboard.service",
            installer,
        )
        self.assertIn(
            "'$remote_scripts_root/van_compute_metrics.py'",
            installer,
        )

    def test_installer_preflights_before_heavy_or_fenced_work(self):
        installer = INSTALLER.read_text(encoding="utf-8")
        upgrade_gate = UPGRADE_GATE.read_text(encoding="utf-8")

        self.assertLess(
            installer.index("Checking macOS sandbox capability"),
            installer.index("Checking and provisioning local worker dependencies"),
        )
        self.assertLess(
            installer.index("Checking SSH access and Pi prerequisites"),
            installer.index("Building an isolated Python environment"),
        )
        self.assertLess(
            installer.index('upgrade_public_root="$('),
            installer.index("Checking and provisioning local worker dependencies"),
        )
        self.assertIn('old_compute_root="/home/pi/scripts/compute"', installer)
        self.assertIn('"$old_compute_root" | "$remote_scripts_root"', installer)
        for migration_option in ("--queue-cli", "--retire-target"):
            self.assertIn(migration_option, installer)
            self.assertIn(migration_option, upgrade_gate)
        self.assertLess(
            installer.index(
                "The loaded worker is not the supported persistent --serve LaunchAgent"
            ),
            installer.index('/bin/launchctl disable "gui/$user_id/$label"'),
        )
        self.assertIn('! print -r -- "$loaded_agent" |', installer)
        drain_disable = installer.index(
            '/bin/launchctl disable "gui/$user_id/$label"'
        )
        drain_signal = installer.index(
            '/bin/launchctl kill SIGUSR1 "gui/$user_id/$label"'
        )
        drain_window = installer.index("for attempt in {1..30}", drain_signal)
        rollback_armed = installer.index("restore_previous_agent=1", drain_window)
        unload = installer.index(
            '/bin/launchctl bootout "gui/$user_id/$label"', drain_window
        )
        post_unload_queue_check = installer.index(
            'active_jobs="$(active_queue_jobs)"', unload
        )
        self.assertLess(drain_disable, drain_signal)
        self.assertLess(drain_signal, drain_window)
        self.assertLess(drain_window, rollback_armed)
        self.assertLess(rollback_armed, unload)
        self.assertLess(unload, post_unload_queue_check)
        self.assertIn("after a 15-second drain window", installer)
        queue_helper = installer[
            installer.index("active_queue_jobs() {") :
            installer.index("active_submitters() {")
        ]
        submitter_helper = installer[
            installer.index("active_submitters() {") :
            installer.index("activate_submission_gate() {")
        ]
        for helper in (queue_helper, submitter_helper):
            self.assertIn("-o BatchMode=yes -o ConnectTimeout=5", helper)
        self.assertLess(
            installer.index("Checking and provisioning the Pi fallback runtime"),
            installer.index('activate_submission_gate "$remote_upgrade_started"'),
        )
        self.assertIn("runtime_lock='$remote_root/runtime.lock'", installer)
        self.assertIn('exec 9>\\"\\$runtime_lock\\"', installer)
        self.assertLess(
            installer.index("The Pi fallback runtime lock is not a regular file."),
            installer.index('exec 9>\\"\\$runtime_lock\\"'),
        )
        self.assertIn("/usr/bin/flock -n 9", installer)
        self.assertIn(
            "test -d '$old_compute_root' && test ! -L '$old_compute_root'",
            installer,
        )
        self.assertIn(
            "/bin/rm -rf --one-file-system -- '$old_compute_root'", installer
        )
        self.assertIn("/usr/bin/mountpoint -q '$old_compute_root'", installer)
        self.assertIn(
            "test ! -e '$old_compute_root' && test ! -L '$old_compute_root'",
            installer,
        )
        self.assertIn(
            "old_dataset_config=/home/pi/secrets/van-compute-datasets.json",
            installer,
        )
        heartbeat_check = installer.index("coordinator = next(")
        finalize = installer.index(
            '/usr/bin/ssh "$pi_host" "${(q)finalize_arguments[@]}"'
        )
        retire_old_root = installer.index(
            "/bin/rm -rf --one-file-system -- '$old_compute_root'"
        )
        self.assertLess(heartbeat_check, finalize)
        self.assertLess(finalize, retire_old_root)
        self.assertIn("release_published=0", installer)
        self.assertIn("if (( ! release_published ))", installer)
        self.assertIn("release_published=1", installer)
        self.assertIn('sandbox_check "profile application"', installer)
        self.assertIn('sandbox_check "Python imports and isolation policy"', installer)
        self.assertIn('sandbox_check "ripgrep runtime"', installer)
        self.assertIn('sandbox_check "SQLite runtime"', installer)
        self.assertIn('sandbox_check "JADX runtime"', installer)
        self.assertIn('(subpath "/System/Volumes/Preboot/Cryptexes/OS")', installer)
        self.assertIn("local exit_code=0", installer)
        self.assertNotIn("local status=", installer)
        self.assertIn('cd "$sandbox_test"', installer)
        self.assertIn('/usr/bin/env -i', installer)
        self.assertIn('PYTHONNOUSERSITE=1', installer)
        self.assertGreater(
            installer.index("Checking Mac process-group resource watchdog"),
            installer.index("WARNING: VAN_COMPUTE_ALLOW_UNSANDBOXED=1"),
        )
        self.assertNotIn(
            "run pi/sync_scripts.sh once to publish the dashboard", installer
        )
        self.assertNotIn("retired dashboard-metrics cleanup", installer)
        self.assertIn(
            "Repository-wide updates remain available through: ./pi/sync_scripts.sh",
            installer,
        )
        self.assertIn("deployment_source_paths=(", installer)
        self.assertIn('if (( if_needed )); then', installer)
        self.assertIn('"$release/deployment.sha256"', installer)
        self.assertIn("'$remote_root/deployment.sha256'", installer)
        self.assertIn('"$release/source.sha256"', installer)
        self.assertIn('"$release/source.sha256" \\', installer)
        self.assertLess(
            installer.index('if (( if_needed )); then'),
            installer.index("Checking local installer prerequisites"),
        )
        self.assertIn(
            "van_compute deployment is current; skipping installer.", installer
        )
        self.assertIn(
            "van_compute deployment changed or is unhealthy; running installer.",
            installer,
        )

    def test_example_tasks_are_a_valid_repository_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            source_root = Path(directory)
            shutil.copy2(EXAMPLE_TASKS, source_root / protocol.REPO_MANIFEST)
            tasks = protocol.load_repo_tasks(source_root)

        self.assertIn("repo-tests", tasks)
        self.assertIn("oem-corpus-search", tasks)
        self.assertEqual(tasks["candump-diagnostic-wire-tcm"].maximum_inputs, 512)
        self.assertEqual(tasks["can-timeseries-correlate-tcm"].maximum_inputs, 513)
        self.assertEqual(
            tasks["can-timeseries-correlate-tcm"].argv[-1],
            "{inputs:1}",
        )


if __name__ == "__main__":
    unittest.main()
