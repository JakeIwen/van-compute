"""Fake-only regression coverage for immutable Mac runtime repair."""
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest import mock

from macbook.scripts import install_van_compute_worker as deployer
from pi.tests.compute import test_van_compute_deployment as fixtures


class ProvisioningLocal(fixtures.FakeLocal):
    def __init__(self, broken=()):
        super().__init__()
        self.broken = set(broken)

    def run(self, arguments, **kwargs):
        result = super().run(arguments, **kwargs)
        if list(arguments[:4]) == ['/opt/homebrew/bin/python3', '-m', 'venv', '--copies']:
            python = Path(arguments[4]) / 'bin/python'
            python.parent.mkdir(parents=True)
            python.write_text('# fake interpreter; never executed\n')
            python.chmod(0o700)
        if arguments[0] in self.broken:
            return fixtures.FakeCompleted(1, stderr='dyld: runtime missing')
        return result


class MacReleaseRepairTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = temporary.name
        self.helpers = fixtures.VanComputeDeploymentTests()
        self.local = ProvisioningLocal()
        self.installer = self.helpers.make_installer(self.directory, local=self.local)
        self.source = self.installer.build_source_release()
        self.old = self.helpers.create_owned_release(self.installer, self.source)
        self.helpers.point_launchagent_at(self.installer, self.old)
        self.installer.capture_prior_release()
        self.addCleanup(mock.patch.stopall)
        mock.patch.object(self.installer, '_install_formulae').start()
        mock.patch.object(self.installer, '_validate_mac_release').start()

    @staticmethod
    def snapshot(path):
        return {str(item.relative_to(path)): item.read_bytes()
                for item in path.rglob('*') if item.is_file()}

    def test_reuse_repairs_caches_and_ignores_ds_store(self):
        for folder in (self.old, self.old / 'app/van_compute', self.old / 'venv'):
            (folder / '.DS_Store').write_bytes(b'Finder metadata')
        self.installer.ensure_cache_directories()
        shutil.rmtree(self.installer.paths.cache_root)
        self.assertEqual(self.installer.prepare_mac_release(self.source), self.old)
        for name in ('logs', 'jobs', 'ssh'):
            path = self.installer.paths.cache_root / name
            self.assertTrue(path.is_dir())
            self.assertEqual(path.stat().st_mode & 0o777, 0o700)
        self.assertFalse(any('-m' in args and 'venv' in args for args, *_ in self.local.calls))

    def test_broken_interpreter_rebuilds_sibling_and_preserves_rollbacks(self):
        self.local.broken.add(str(self.old / 'venv/bin/python'))
        previous = self.helpers.create_owned_release(self.installer, self.helpers.make_source('c'))
        before = self.snapshot(self.old)
        prior_bytes = self.snapshot(previous)
        new = self.installer.prepare_mac_release(self.source)
        self.assertNotEqual(new, self.old)
        self.assertRegex(new.name, '^' + self.source.mac_version + '-[0-9a-f]{32}$')
        self.installer._verify_release(new, self.source)
        self.installer.prune_local_releases(new)
        self.assertEqual(self.snapshot(self.old), before)
        self.assertEqual(self.snapshot(previous), prior_bytes)
        build = next(args for args, *_ in self.local.calls if 'venv' in args)
        self.assertEqual(build[-1], str(new / 'venv'))
        self.assertTrue(any(call[4] == 15 for call in self.local.calls))
        # The actual installed generation, not the canonical unsuffixed name,
        # is verified and reused on the next manual/conditional installation.
        self.helpers.point_launchagent_at(self.installer, new)
        self.installer.capture_prior_release()
        self.assertEqual(self.installer.prepare_mac_release(self.source), new)
        self.local.launch_states = ['pid = 99']
        self.assertTrue(self.installer.deployment_current(self.source))

    def test_missing_interpreter_is_repairable_but_source_tamper_is_not(self):
        (self.old / 'venv/bin/python').unlink()
        new = self.installer.prepare_mac_release(self.source)
        self.assertNotEqual(new, self.old)
        (self.old / 'app/van_compute/worker.py').write_text('# tampered\n')
        with self.assertRaisesRegex(deployer.DeploymentError, 'hash mismatch'):
            self.installer.prepare_mac_release(self.source)

    def test_failed_rebuild_never_drains_and_leaves_old_release_unchanged(self):
        before = self.snapshot(self.old)
        with mock.patch.object(self.installer, '_runtime_healthy', return_value=False), \
             mock.patch.object(self.installer, 'preflight_local'), \
             mock.patch.object(self.installer, 'acquire_lock_and_owner', return_value=fixtures.FakeLock([])), \
             mock.patch.object(self.installer, 'remote_preflight'), \
             mock.patch.object(self.installer, 'drain_worker') as drain:
            with self.assertRaisesRegex(deployer.DeploymentError, 'new Mac release interpreter'):
                self.installer.execute()
        drain.assert_not_called()
        self.assertEqual(self.snapshot(self.old), before)
        self.assertEqual(self.installer.remote.calls, [])
        incomplete = next(path for path in self.installer.paths.release_parent.iterdir() if path != self.old)
        self.assertFalse((incomplete / deployer.MANIFEST_FILE).exists())
        with self.assertRaises(deployer.DeploymentError):
            self.installer._release_identity(incomplete)
        # A rerun gets a fresh unique generation instead of overwriting the partial.
        self.local.broken.add(str(self.old / 'venv/bin/python'))
        self.assertNotEqual(self.installer.prepare_mac_release(self.source), incomplete)

    def test_suffix_and_provenance_must_match(self):
        new = self.old.with_name(self.old.name + '-' + 'd' * 32)
        self.old.rename(new)
        with self.assertRaisesRegex(deployer.DeploymentError, 'build identity mismatch'):
            self.installer._release_identity(new)
        malformed = new.with_name(self.source.mac_version + '-not-a-uuid')
        new.rename(malformed)
        with self.assertRaisesRegex(deployer.DeploymentError, 'owned real directory'):
            self.installer._release_identity(malformed)

    def test_frozen_installer_prefers_its_own_generation(self):
        self.installer.options = deployer.Options(rebuild=True)
        new = self.installer.prepare_mac_release(self.source)
        frozen = deployer.Installer(
            deployer.Options(), environment={}, home=self.installer.home,
            script=new / 'app/macbook/scripts/install_van_compute_worker.py',
            local=self.local, remote=fixtures.FakeRemote(), stdout=io.StringIO(), stderr=io.StringIO())
        frozen.prior_release = self.old
        with mock.patch.object(frozen, '_validate_mac_release'):
            self.assertEqual(frozen.prepare_mac_release(frozen.build_source_release()), new)

    def test_rebuild_overrides_if_needed_and_dry_run_writes_nothing(self):
        options = deployer.parse_arguments(['--if-needed', '--rebuild'])
        workflow = fixtures.WorkflowInstaller(options, self.source)
        with mock.patch.object(workflow, 'deployment_current', return_value=True) as current:
            self.assertEqual(workflow.execute(), 0)
        current.assert_not_called()
        self.assertIn('drain', workflow.events)
        out = io.StringIO()
        dry = self.helpers.make_installer(
            Path(self.directory) / 'dry', stdout=out,
            options=deployer.parse_arguments(['--if-needed', '--rebuild', '--dry-run']))
        dry.execute()
        self.assertTrue(json.loads(out.getvalue())['rebuild'])
        self.assertFalse(dry.paths.home.exists())
        self.assertEqual(dry.local.calls, [])
        self.assertEqual(dry.remote.calls, [])

    def test_if_needed_recreates_caches_and_rejects_broken_runtime(self):
        self.installer.options = deployer.Options(if_needed=True)
        self.local.launch_states = ['pid = 99']
        self.assertEqual(self.installer.execute(), 0)
        self.assertTrue((self.installer.paths.cache_root / 'logs').is_dir())
        shutil.rmtree(self.installer.paths.cache_root)
        self.local.launch_states = ['pid = 99']
        self.assertEqual(self.installer.execute(), 0)
        self.assertTrue((self.installer.paths.cache_root / 'ssh').is_dir())
        self.local.broken.add(str(self.old / 'venv/bin/python'))
        self.assertFalse(self.installer.deployment_current(self.source))


if __name__ == '__main__':
    unittest.main()
