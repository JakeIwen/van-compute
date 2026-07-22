import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from shared.python import van_compute_protocol as protocol


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
SYNC_SCRIPT = REPOSITORY_ROOT / "pi" / "sync_scripts.sh"
INSTALLER = REPOSITORY_ROOT / "macbook" / "scripts" / "install_van_compute_worker.zsh"
QUEUE_CLI = REPOSITORY_ROOT / "pi" / "scripts" / "compute" / "van_compute.py"
PROTOCOL = REPOSITORY_ROOT / "shared" / "python" / "van_compute_protocol.py"
EXAMPLE_TASKS = REPOSITORY_ROOT / "pi" / "configs" / "van-compute-obd.example.json"
DASHBOARD_SERVICE = REPOSITORY_ROOT / "pi" / "services" / "van-dashboard.service"
UPDATE_SERVICES = REPOSITORY_ROOT / "pi" / "scripts" / "update_services.sh"


class VanComputeDeploymentTests(unittest.TestCase):
    def test_generic_sync_excludes_files_owned_by_compute_installer(self):
        sync = SYNC_SCRIPT.read_text(encoding="utf-8")

        self.assertIn("--exclude '/compute/'", sync)
        self.assertIn("--exclude '/van-compute-broker.service'", sync)
        self.assertIn(
            '/bin/rm -f -- "$python_stage/van_compute_protocol.py"',
            sync,
        )
        self.assertNotIn(
            '/bin/rm -f -- "$python_stage/van_compute_metrics.py"',
            sync,
        )

        self.assertIn('staged_services="$local_stage/services"', sync)
        self.assertIn(
            'scp $mux -r "$staged_services" "$staged_scripts"',
            sync,
        )
        self.assertNotIn(
            'scp $mux -r "$services" "$staged_scripts"',
            sync,
        )

    def test_installer_deploys_example_tasks_from_configs_to_configs(self):
        installer = INSTALLER.read_text(encoding="utf-8")

        self.assertIn(
            '"$repo_root/pi/configs/van-compute-obd.example.json"',
            installer,
        )
        self.assertIn('remote_config_root="/home/pi/configs"', installer)
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
                    "/home/pi/configs/van-compute-obd.example.json"
                )
                or command.endswith(
                    "'$remote_config_root/van-compute-obd.example.json'"
                )
                for command in config_installs
            ),
            "example tasks are not installed under /home/pi/configs",
        )

    def test_deployed_queue_cli_finds_sibling_protocol_without_repo_path(self):
        with tempfile.TemporaryDirectory() as directory:
            compute = Path(directory) / "compute"
            deployed_protocol = compute / "python-automation"
            deployed_protocol.mkdir(parents=True)
            shutil.copy2(QUEUE_CLI, compute / "van_compute.py")
            shutil.copy2(PROTOCOL, deployed_protocol / "van_compute_protocol.py")

            environment = os.environ.copy()
            environment.pop("PYTHONPATH", None)
            result = subprocess.run(
                [sys.executable, "-I", str(compute / "van_compute.py"), "--help"],
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

        for relative in (
            "van_compute_metrics.py",
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

    def test_installer_preflights_before_heavy_or_fenced_work(self):
        installer = INSTALLER.read_text(encoding="utf-8")

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
        self.assertLess(
            installer.index("Checking and provisioning the Pi fallback runtime"),
            installer.index('activate_submission_gate "$remote_upgrade_started"'),
        )
        self.assertIn("exec 9>/home/pi/.local/share/van-compute/runtime.lock", installer)
        self.assertIn("/usr/bin/flock -n 9", installer)
        self.assertIn("release_published=0", installer)
        self.assertIn("if (( ! release_published ))", installer)
        self.assertIn("release_published=1", installer)
        self.assertIn('sandbox_check "profile application"', installer)
        self.assertIn('sandbox_check "Python imports and isolation policy"', installer)
        self.assertIn('sandbox_check "ripgrep runtime"', installer)
        self.assertIn('sandbox_check "SQLite runtime"', installer)
        self.assertIn('sandbox_check "JADX runtime"', installer)
        self.assertIn('cd "$sandbox_test"', installer)
        self.assertIn('/usr/bin/env -i', installer)
        self.assertIn('PYTHONNOUSERSITE=1', installer)
        self.assertGreater(
            installer.index("Checking Mac process-group resource watchdog"),
            installer.index("WARNING: VAN_COMPUTE_ALLOW_UNSANDBOXED=1"),
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
