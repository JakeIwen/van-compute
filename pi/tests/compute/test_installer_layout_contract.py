import ast
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from macbook.scripts.van_compute_installer.models import DeploymentError
from macbook.scripts.van_compute_installer.runners import LocalRunner, RemoteRunner


ROOT = Path(__file__).resolve().parents[3]
INSTALLER = ROOT / "macbook/scripts/van_compute_installer"


class RecordingLocal:
    def __init__(self):
        self.calls = []

    def run(self, arguments, **options):
        self.calls.append((arguments, options))
        return subprocess.CompletedProcess(arguments, 0, stdout="remote output")


class InstallerLayoutContractTests(unittest.TestCase):
    def test_contract_table_covers_the_service_installer_and_launch_agent(self):
        text = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
        table = text.split("## Contracts", 1)[1].strip().split("\n\n", 1)[0]
        rows = {}
        for line in table.splitlines():
            cells = [cell.strip().strip("`") for cell in line.strip().strip("|").split("|")]
            if len(cells) == 3 and cells[0] not in {"Path", "---"}:
                rows[cells[0]] = (cells[1], cells[2])
        expected = {
            "python3 -m van_compute.broker": "van_compute/broker.py",
            "macbook/scripts/install_van_compute_worker.zsh": "macbook/scripts/install_van_compute_worker.py",
            "macbook/scripts/install_van_compute_worker.py": "macbook/scripts/van_compute_installer/cli.py",
            "macbook/launchagents/com.jacobr.van-compute-worker.plist": "van_compute/worker.py",
        }
        self.assertEqual(set(rows), set(expected))
        for contract, owner in expected.items():
            self.assertTrue(rows[contract][0], contract)
            self.assertIn(owner, rows[contract][1])
            self.assertTrue((ROOT / owner).is_file(), owner)

    def test_models_are_records_and_do_not_import_process_adapters(self):
        tree = ast.parse((INSTALLER / "models.py").read_text(encoding="utf-8"))
        imports = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imports.update(node.module for node in ast.walk(tree)
                       if isinstance(node, ast.ImportFrom) and node.module)
        self.assertFalse({"subprocess", "runners"} & imports)

    def test_local_runner_preserves_output_and_failure_contracts(self):
        runner = LocalRunner()
        completed = runner.run(
            [sys.executable, "-c", "print('ready')"],
            capture_output=True,
        )
        self.assertEqual(completed.stdout, "ready\n")
        with self.assertRaisesRegex(DeploymentError, "expected failure"):
            runner.run(
                [sys.executable, "-c", "import sys; print('expected failure', file=sys.stderr); sys.exit(3)"],
                capture_output=True,
            )

    def test_background_helper_announces_and_reports_its_exit(self):
        runner = LocalRunner()
        helper = runner.spawn([
            sys.executable, "-c",
            "import sys; print('ready', flush=True); sys.stdin.read()",
        ])
        self.assertEqual(helper.announcement(10), "ready")
        helper.finish(10)
        failing = runner.spawn([
            sys.executable, "-c",
            "import sys; print('ready', flush=True); sys.stdin.read(); "
            "print('helper broke', file=sys.stderr); sys.exit(4)",
        ])
        self.assertEqual(failing.announcement(10), "ready")
        with self.assertRaisesRegex(DeploymentError, "helper broke"):
            failing.finish(10)
        silent = runner.spawn([
            sys.executable, "-c", "import sys; print('no port', file=sys.stderr)",
        ])
        with self.assertRaisesRegex(DeploymentError, "no port"):
            silent.announcement(10)

    def test_remote_runner_owns_ssh_and_scp_vectors(self):
        local = RecordingLocal()
        remote = RemoteRunner("pi@example", local)
        self.assertEqual(
            remote.run("probe", "printf ok", ["two words"], capture_output=True),
            "remote output",
        )
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "payload"
            source.write_text("fixture", encoding="utf-8")
            remote.upload("upload", [source], "/tmp/stage")
        self.assertEqual(local.calls[0][0][:6], [
            "/usr/bin/ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "pi@example",
        ])
        self.assertEqual(local.calls[0][0][-1], "/bin/sh -s -- 'two words'")
        self.assertEqual(local.calls[0][1]["input_text"], "printf ok")
        self.assertEqual(local.calls[1][0][0:3], ["/usr/bin/scp", "-r", "-p"])
        self.assertEqual(local.calls[1][0][-1], "pi@example:/tmp/stage")


if __name__ == "__main__":
    unittest.main()
