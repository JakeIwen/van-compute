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
            with self.assertRaisesRegex(DeploymentError, "contains a symlink"):
                installer._verify_release(release, self.source)
            linked.unlink()
            fifo = release / "unexpected.fifo"
            os.mkfifo(fifo)
            try:
                with self.assertRaisesRegex(DeploymentError, "special entry"):
                    installer._verify_release(release, self.source)
            finally:
                fifo.unlink()

    def _create_remote_reuse_fixture(self, installer, directory, validation):
        code = validation.split("<<'PY_EXISTING'\n", 1)[1].split(
            "\nPY_EXISTING", 1
        )[0]
        root = Path(directory)
        release = root / "release"
        (release / "van_compute").mkdir(parents=True)
        payload = release / "van_compute/worker.py"
        payload.write_text("# original\n")
        (release / "source.sha256").write_text("a" * 64)
        (release / "provenance.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "kind": "pi-broker",
                    "source_sha256": "a" * 64,
                    "source_root": "/source/checkout",
                }
            )
        )
        installer._write_manifest(release)
        staged = root / "staged"
        shutil.copytree(release, staged)
        return code, release, staged, payload

    def _verify_remote_reuse(self, code, release, staged):
        with mock.patch.object(sys, "argv", ["verify", str(release), str(staged)]):
            exec(compile(code, "<remote-release-check>", "exec"), {})

    def test_remote_reuse_is_verified_before_cutover_and_ignores_bytecode(self):
        remote = FakeRemote()
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory, remote=remote)
            installer.validate_remote_stage(self.source)
            validation = remote.scripts["validate-stage"]
            code, release, staged, payload = self._create_remote_reuse_fixture(
                installer, directory, validation
            )

            def verify():
                self._verify_remote_reuse(code, release, staged)

            verify()
            cache = release / "van_compute/__pycache__"
            cache.mkdir()
            (cache / "worker.cpython-39.pyc").write_bytes(b"runtime cache")
            (release / "standalone.pyo").write_bytes(b"runtime cache")
            verify()
            shutil.rmtree(cache)
            (release / "standalone.pyo").unlink()

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
            payload.write_text("# original\n")
            unexpected = release / "unexpected.txt"
            unexpected.write_text("rewritten into manifest\n")
            installer._write_manifest(release)
            with self.assertRaisesRegex(SystemExit, "planned source"):
                verify()

            installer.cutover_remote(self.source)
            cutover = remote.scripts["cutover"]
            self.assertIn("reuse-existing-release", cutover)
            self.assertNotIn("manifest.json", cutover)
            self.assertNotIn("Existing release", cutover)

        validation_call = next(
            call for call in remote.calls if call[0] == "validate-stage"
        )
        self.assertEqual(
            validation_call[1],
            (
                installer.remote_stage,
                self.source.source_fingerprint,
                installer_constants.REMOTE_ROOT,
                self.source.pi_version,
            ),
        )

    def test_all_remote_release_links_require_one_immediate_24_hex_target(self):
        remote = FakeRemote(responses={"preflight": installer_constants.REMOTE_SCRIPTS + "\n"})
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
                        installer_constants.RELEASE_LINK_GUARD
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
        installer_package = REPOSITORY_ROOT / "macbook/scripts/van_compute_installer"
        expected_installer_files = set(installer_package.glob("*.py"))
        actual_installer_files = {
            path for path in sources if path.is_relative_to(installer_package)
        }
        self.assertEqual(actual_installer_files, expected_installer_files)
        self.assertIn(INSTALLER, sources)
        self.assertIn(SHIM, sources)

    def copy_frozen_source(self, app):
        for relative in (
            Path("macbook/scripts/install_van_compute_worker.py"),
            Path("macbook/scripts/install_van_compute_worker.zsh"),
            Path("macbook/launchagents") / f"{installer_constants.LABEL}.plist",
        ):
            destination = app / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(REPOSITORY_ROOT / relative, destination)
        shutil.copytree(
            REPOSITORY_ROOT / "macbook/scripts/van_compute_installer",
            app / "macbook/scripts/van_compute_installer",
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        shutil.copytree(
            COMPUTE_ROOT,
            app / "van_compute",
            ignore=shutil.ignore_patterns("__pycache__"),
        )

    def test_source_allowlist_includes_new_modules_but_excludes_data_and_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            app = Path(directory) / "app"
            for relative in (
                Path("macbook/scripts/install_van_compute_worker.py"),
                Path("macbook/scripts/install_van_compute_worker.zsh"),
                Path("macbook/launchagents") / f"{installer_constants.LABEL}.plist",
            ):
                destination = app / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(REPOSITORY_ROOT / relative, destination)
            shutil.copytree(
                REPOSITORY_ROOT / "macbook/scripts/van_compute_installer",
                app / "macbook/scripts/van_compute_installer",
                ignore=shutil.ignore_patterns("__pycache__"),
            )
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
            installer = Installer(
                Options(),
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
        self.assertIn(
            "macbook/scripts/van_compute_installer/orchestrator.py", relative
        )
        self.assertNotIn("van_compute/.env", relative)
        self.assertNotIn("van_compute/cache.sqlite3", relative)
        self.assertNotIn("van_compute/configs/private.json", relative)

    def test_selected_source_symlink_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            app = Path(directory) / "app"
            self.copy_frozen_source(app)
            target = app / "real.py"
            target.write_text("# target\n", encoding="utf-8")
            module = app / "van_compute/linked.py"
            module.symlink_to(target)
            installer = Installer(
                Options(),
                environment={},
                home=Path(directory) / "home",
                script=app / "macbook/scripts/install_van_compute_worker.py",
                local=FakeLocal(),
                remote=FakeRemote(),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )
            with self.assertRaisesRegex(DeploymentError, "non-symlink"):
                installer.source_paths()

    def test_selected_installer_helper_symlink_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            app = Path(directory) / "app"
            self.copy_frozen_source(app)
            target = app / "real.py"
            target.write_text("# target\n", encoding="utf-8")
            module = app / "macbook/scripts/van_compute_installer/orchestrator.py"
            module.unlink()
            module.symlink_to(target)
            installer = Installer(
                Options(),
                environment={},
                home=Path(directory) / "home",
                script=app / "macbook/scripts/install_van_compute_worker.py",
                local=FakeLocal(),
                remote=FakeRemote(),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )
            with self.assertRaisesRegex(DeploymentError, "non-symlink"):
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
            installer = Installer(
                Options(),
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
                DeploymentError, "changed after planning"
            ):
                installer._verify_staged_source(staging / "app", planned)

    @unittest.skipUnless(
        Path("/opt/homebrew/bin/python3").is_file()
        and Path("/usr/bin/python3").is_file(),
        "requires both Homebrew and system Python runtimes",
    )
    def test_frozen_installer_dry_run_works_without_bytecode_on_both_runtimes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            installer = self.make_installer(directory)
            staging = root / "release"
            staging.mkdir()
            installer._copy_source_tree(staging, installer.build_source_release())
            app = staging / "app"
            frozen = app / "macbook/scripts/install_van_compute_worker.py"
            environment = {
                key: value
                for key, value in os.environ.items()
                if key not in {"PYTHONDONTWRITEBYTECODE", "PYTHONHOME", "PYTHONPATH"}
            }
            environment["HOME"] = str(root / "home")
            for runtime in (Path("/opt/homebrew/bin/python3"), Path("/usr/bin/python3")):
                with self.subTest(runtime=runtime):
                    completed = subprocess.run(
                        [str(runtime), str(frozen), "--dry-run"],
                        cwd=app,
                        env=environment,
                        text=True,
                        capture_output=True,
                        check=False,
                    )
                    self.assertEqual(completed.returncode, 0, completed.stderr)
                    self.assertEqual(
                        json.loads(completed.stdout)["source_root"], str(app.resolve())
                    )
                    self.assertFalse(any(app.rglob("__pycache__")))

    def test_installer_helper_edit_changes_source_fingerprint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            app = root / "app"
            self.copy_frozen_source(app)
            installer = Installer(
                Options(),
                environment={},
                home=root / "home",
                script=app / "macbook/scripts/install_van_compute_worker.py",
                local=FakeLocal(),
                remote=FakeRemote(),
                stdout=io.StringIO(),
                stderr=io.StringIO(),
            )
            before = installer.build_source_release()
            helper = app / "macbook/scripts/van_compute_installer/source.py"
            helper.write_text(
                helper.read_text(encoding="utf-8") + "\n# fingerprint change\n",
                encoding="utf-8",
            )
            after = installer.build_source_release()
            self.assertNotEqual(before.source_fingerprint, after.source_fingerprint)
            self.assertNotEqual(
                before.deployment_fingerprint, after.deployment_fingerprint
            )

    def test_release_identity_accepts_pre_modular_schema1_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory)
            release = self.create_owned_release(installer, self.source)
            legacy = release / "app/macbook/scripts/install_van_compute_worker.py"
            legacy.parent.mkdir(parents=True)
            legacy.write_text("# legacy frozen installer\n", encoding="utf-8")
            legacy.chmod(0o700)
            installer._write_manifest(release)
            self.assertFalse(
                (release / "app/macbook/scripts/van_compute_installer").exists()
            )
            self.assertEqual(
                installer._release_identity(release),
                (
                    self.source.source_fingerprint,
                    self.source.deployment_fingerprint,
                ),
            )
            self.point_launchagent_at(installer, release)
            installer.capture_prior_release()
            self.assertEqual(installer.prior_release, release)
            self.assertFalse(installer.release_retention_ambiguous)

    def test_mac_release_carries_a_runnable_frozen_installer_source_tree(self):
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory)
            staging = Path(directory) / "release"
            staging.mkdir()
            source = installer.build_source_release()
            installer._copy_source_tree(staging, source)
            frozen = staging / "app" / "macbook" / "scripts" / INSTALLER.name
            paths = Paths.discover(frozen, Path(directory) / "other-home")
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


if __name__ == "__main__":
    unittest.main()
