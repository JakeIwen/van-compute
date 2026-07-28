import ast
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from pi.van_compute.scripts import van_compute_protocol as protocol


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
COMPUTE_ROOT = REPOSITORY_ROOT / "pi" / "van_compute"
SYNC_SCRIPT = REPOSITORY_ROOT / "pi" / "sync_scripts.sh"
INSTALLER = REPOSITORY_ROOT / "macbook" / "scripts" / "install_van_compute_worker.zsh"
WORKER = REPOSITORY_ROOT / "macbook" / "scripts" / "van_compute_worker.py"
QUEUE_CLI = REPOSITORY_ROOT / "pi" / "van_compute" / "scripts" / "van_compute.py"
UPGRADE_GATE = (
    REPOSITORY_ROOT
    / "pi"
    / "van_compute"
    / "scripts"
    / "van_compute_upgrade_gate.py"
)
PROTOCOL = (
    REPOSITORY_ROOT / "pi" / "van_compute" / "scripts" / "van_compute_protocol.py"
)
EXAMPLE_TASKS = (
    REPOSITORY_ROOT
    / "pi"
    / "van_compute"
    / "configs"
    / "van-compute-obd.example.json"
)
DASHBOARD_SERVICE = REPOSITORY_ROOT / "pi" / "services" / "van-dashboard.service"
DASHBOARD_APP = (
    REPOSITORY_ROOT / "pi" / "apps" / "van_dashboard" / "van_dashboard.py"
)
BROKER_SERVICE = (
    REPOSITORY_ROOT
    / "pi"
    / "van_compute"
    / "configs"
    / "van-compute-broker.service"
)
UPDATE_SERVICES = REPOSITORY_ROOT / "pi" / "scripts" / "update_services.sh"


class VanComputeDeploymentTests(unittest.TestCase):
    def test_compute_deployment_sources_have_one_repository_root(self):
        for relative in (
            "scripts/pi_compute.py",
            "scripts/van_compute.py",
            "scripts/van_compute_broker.py",
            "scripts/van_compute_metrics.py",
            "scripts/van_compute_protocol.py",
            "scripts/van_compute_upgrade_gate.py",
            "configs/van-compute-broker.service",
            "configs/van-compute-obd.example.json",
        ):
            self.assertTrue((COMPUTE_ROOT / relative).is_file(), relative)

        for retired in (
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
        self.assertIn("/home/pi/van_compute/scripts/van_compute_broker.py", service)
        self.assertIn("/home/pi/van_compute/venv/bin/python3", service)
        self.assertNotIn("/home/pi/scripts/compute", service)

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

    def test_deployed_queue_cli_finds_sibling_protocol_without_repo_path(self):
        with tempfile.TemporaryDirectory() as directory:
            compute = Path(directory) / "compute"
            compute.mkdir(parents=True)
            shutil.copy2(QUEUE_CLI, compute / "van_compute.py")
            shutil.copy2(PROTOCOL, compute / "van_compute_protocol.py")

            environment = os.environ.copy()
            environment.pop("PYTHONPATH", None)
            result = subprocess.run(
                [sys.executable, str(compute / "van_compute.py"), "--help"],
                cwd=directory,
                env=environment,
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("usage:", result.stdout)

    def test_mac_release_contains_the_worker_protocol_package(self):
        with tempfile.TemporaryDirectory() as directory:
            app = Path(directory) / "app"
            worker_dir = app / "macbook" / "scripts"
            package_dir = app / "pi" / "van_compute" / "scripts"
            worker_dir.mkdir(parents=True)
            package_dir.mkdir(parents=True)
            shutil.copy2(WORKER, worker_dir / "van_compute_worker.py")
            shutil.copy2(
                COMPUTE_ROOT / "__init__.py",
                package_dir.parent / "__init__.py",
            )
            shutil.copy2(
                COMPUTE_ROOT / "scripts" / "__init__.py",
                package_dir / "__init__.py",
            )
            shutil.copy2(PROTOCOL, package_dir / "van_compute_protocol.py")

            result = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    str(worker_dir / "van_compute_worker.py"),
                    "--help",
                ],
                cwd=directory,
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("usage:", result.stdout)

    def test_dashboard_declares_restart_triggers_for_compute_assets(self):
        service = DASHBOARD_SERVICE.read_text(encoding="utf-8")
        app = DASHBOARD_APP.read_text(encoding="utf-8")
        updater = UPDATE_SERVICES.read_text(encoding="utf-8")

        self.assertIn(
            "ExecStartPre=/usr/bin/test -r "
            "/home/pi/van_compute/scripts/van_compute_metrics.py",
            service,
        )
        self.assertIn(
            "Environment=PYTHONPATH=/home/pi/van_compute/scripts",
            service,
        )
        self.assertIn(
            '"VAN_COMPUTE_SCRIPTS", "/home/pi/van_compute/scripts"',
            app,
        )
        self.assertIn("from van_compute_metrics import", app)
        imported_metric_names = {
            alias.name
            for node in ast.walk(ast.parse(app))
            if isinstance(node, ast.ImportFrom)
            and node.module in (
                "pi.van_compute.scripts.van_compute_metrics",
                "van_compute_metrics",
            )
            for alias in node.names
        }
        self.assertNotIn("TASK_NAME_RE", imported_metric_names)
        self.assertIn("COMPUTE_TASK_NAME_RE = re.compile", app)
        for relative in (
            "templates/van_dashboard.html",
            "static/van_dashboard.js",
            "static/van_dashboard.css",
        ):
            self.assertIn(
                "ExecStartPre=/usr/bin/test -r "
                f"/home/pi/scripts/python-automation/{relative}",
                service,
            )
        self.assertIn("'s/^ExecStartPre=//p'", updater)
        self.assertIn(
            'for staged_script in "$staged_scripts"/*',
            updater,
        )
        self.assertIn('chmod 770 "$live_scripts/${staged_script##*/}"', updater)
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


if __name__ == "__main__":
    unittest.main()
