"""Regression coverage for deployment interruption and failed-build cleanup."""
import io
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest import mock

from macbook.scripts.van_compute_installer import cli as installer_cli
from macbook.scripts.van_compute_installer import constants as installer_constants
from macbook.scripts.van_compute_installer import mac as installer_mac
from macbook.scripts.van_compute_installer.models import DeploymentError, Options
from macbook.scripts.van_compute_installer.orchestrator import Installer
from pi.tests.compute import van_compute_deployment_support as fixtures


REAL_INSTALLER = Installer


class BuildLocal(fixtures.FakeLocal):
    def __init__(self, failure=None, before_failure=None):
        super().__init__()
        self.failure = failure
        self.before_failure = before_failure

    def run(self, arguments, **kwargs):
        result = super().run(arguments, **kwargs)
        if list(arguments[:4]) == [
            '/opt/homebrew/bin/python3', '-m', 'venv', '--copies'
        ]:
            python = Path(arguments[4]) / 'bin/python'
            python.parent.mkdir(parents=True)
            python.write_text('# fake interpreter; never executed\n', encoding='utf-8')
            python.chmod(0o700)
        if len(arguments) >= 4 and list(arguments[1:4]) == ['-m', 'pip', 'install']:
            release = Path(arguments[0]).parents[2]
            if self.before_failure is not None:
                self.before_failure(release)
            if self.failure is not None:
                raise self.failure
        return result


class CutoverRemote(fixtures.FakeRemote):
    def __init__(self, trigger):
        super().__init__()
        self.trigger = trigger

    def run(self, name, script, arguments=(), **kwargs):
        result = super().run(name, script, arguments, **kwargs)
        if name == 'cutover':
            self.trigger()
        return result


class CutoverWorkflow(fixtures.WorkflowInstaller):
    def __init__(self, source, remote):
        super().__init__(Options(), source)
        self.remote = remote

    def cutover_remote(self, source):
        self.events.append('cutover')
        return REAL_INSTALLER.cutover_remote(self, source)

    def cleanup(self):
        self.events.append('cleanup')
        return REAL_INSTALLER.cleanup(self)


class FailedBuildCleanupTests(unittest.TestCase):
    def setUp(self):
        self.helpers = fixtures.DeploymentFixtureMixin()

    @staticmethod
    def snapshot(path):
        return {
            str(item.relative_to(path)): item.read_bytes()
            for item in path.rglob('*')
            if item.is_file()
        }

    def make_installer(self, directory, local):
        installer = self.helpers.make_installer(directory, local=local)
        installer.uuid_factory = lambda: uuid.UUID(hex='1' * 32)
        return installer

    def create_retained_releases(self, installer):
        current = self.helpers.create_owned_release(
            installer, self.helpers.make_source('c')
        )
        historical = self.helpers.create_owned_release(
            installer, self.helpers.make_source('d')
        )
        self.helpers.point_launchagent_at(installer, current)
        installer.capture_prior_release()
        return current, historical

    def assert_retained_unchanged(self, snapshots):
        for path, before in snapshots.items():
            self.assertTrue(path.is_dir())
            self.assertEqual(self.snapshot(path), before)

    def test_pip_failure_removes_only_the_owned_generation(self):
        failure = DeploymentError('pip failed')
        with tempfile.TemporaryDirectory() as directory:
            local = BuildLocal(failure)
            installer = self.make_installer(directory, local)
            source = installer.build_source_release()
            retained = self.create_retained_releases(installer)
            snapshots = {path: self.snapshot(path) for path in retained}
            with mock.patch.object(installer, '_install_formulae'):
                with self.assertRaises(DeploymentError) as raised:
                    installer.prepare_mac_release(source)
            self.assertIs(raised.exception, failure)
            self.assertEqual(set(installer.paths.release_parent.iterdir()), set(retained))
            self.assert_retained_unchanged(snapshots)

    def test_validation_failure_and_keyboard_interrupt_remove_owned_generation(self):
        failures = (
            DeploymentError('validation failed'),
            KeyboardInterrupt(),
        )
        for failure in failures:
            with self.subTest(failure=type(failure).__name__), \
                 tempfile.TemporaryDirectory() as directory:
                installer = self.make_installer(directory, BuildLocal())
                source = installer.build_source_release()
                retained = self.create_retained_releases(installer)
                snapshots = {path: self.snapshot(path) for path in retained}
                with mock.patch.object(installer, '_install_formulae'), \
                     mock.patch.object(
                         installer, '_validate_mac_release', side_effect=failure
                     ):
                    with self.assertRaises(type(failure)) as raised:
                        installer.prepare_mac_release(source)
                self.assertIs(raised.exception, failure)
                self.assertEqual(
                    set(installer.paths.release_parent.iterdir()), set(retained)
                )
                self.assert_retained_unchanged(snapshots)

    def test_mount_or_symlink_uncertainty_retains_failed_generation(self):
        for hazard in ('mount', 'symlink'):
            failure = DeploymentError('original build failure')
            with self.subTest(hazard=hazard), \
                 tempfile.TemporaryDirectory() as directory:
                callback = None
                if hazard == 'symlink':
                    callback = lambda release: (release / 'unsafe-link').symlink_to(
                        release / 'venv', target_is_directory=True
                    )
                installer = self.make_installer(
                    directory, BuildLocal(failure, callback)
                )
                source = installer.build_source_release()
                expected = installer.paths.release_parent / (
                    source.mac_version + '-' + '1' * 32
                )
                mount_check = lambda path: hazard == 'mount' and Path(path) == expected
                with mock.patch.object(installer, '_install_formulae'), \
                     mock.patch.object(
                         installer_mac.os.path, 'ismount', side_effect=mount_check
                     ):
                    with self.assertRaises(DeploymentError) as raised:
                        installer.prepare_mac_release(source)
                self.assertIs(raised.exception, failure)
                self.assertTrue(expected.is_dir())
                self.assertIn('cleanup was unsafe', installer.stderr.getvalue())

    def test_protected_or_ambiguous_launchagent_reference_retains_build(self):
        for protection in ('prior', 'installed', 'ambiguous'):
            failure = DeploymentError('pip failed')
            with self.subTest(protection=protection), \
                 tempfile.TemporaryDirectory() as directory:
                holder = {}

                def protect(release):
                    installer = holder['installer']
                    if protection == 'prior':
                        installer.prior_release = release
                    elif protection == 'installed':
                        self.helpers.point_launchagent_at(installer, release)

                local = BuildLocal(failure, protect)
                installer = self.make_installer(directory, local)
                holder['installer'] = installer
                source = installer.build_source_release()
                if protection == 'ambiguous':
                    installer.paths.target_dir.mkdir(parents=True)
                    installer.paths.target_plist.write_text(
                        'not a plist\n', encoding='utf-8'
                    )
                with mock.patch.object(installer, '_install_formulae'):
                    with self.assertRaises(DeploymentError) as raised:
                        installer.prepare_mac_release(source)
                self.assertIs(raised.exception, failure)
                generations = list(installer.paths.release_parent.iterdir())
                self.assertEqual(len(generations), 1)
                self.assertTrue(generations[0].is_dir())
                self.assertIn('cleanup was unsafe', installer.stderr.getvalue())

    def test_preexisting_uuid_collision_is_never_owned_or_deleted(self):
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory, BuildLocal())
            source = installer.build_source_release()
            collision = installer.paths.release_parent / (
                source.mac_version + '-' + '1' * 32
            )
            collision.mkdir(parents=True)
            marker = collision / 'keep.txt'
            marker.write_text('preexisting\n', encoding='utf-8')
            with mock.patch.object(installer, '_install_formulae'):
                with self.assertRaises(FileExistsError):
                    installer.prepare_mac_release(source)
            self.assertEqual(marker.read_text(encoding='utf-8'), 'preexisting\n')

    def test_completed_build_survives_a_later_deployment_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            installer = self.make_installer(directory, BuildLocal())
            with mock.patch.object(installer, '_install_formulae'), \
                 mock.patch.object(installer, '_validate_mac_release'), \
                 mock.patch.object(installer, 'preflight_local'), \
                 mock.patch.object(
                     installer,
                     'acquire_lock_and_owner',
                     return_value=fixtures.FakeLock([]),
                 ), \
                 mock.patch.object(installer, 'remote_preflight'), \
                 mock.patch.object(installer, 'install_dataset'), \
                 mock.patch.object(
                     installer,
                     'stage_remote_release',
                     side_effect=DeploymentError('later deployment failure'),
                 ):
                with self.assertRaisesRegex(
                    DeploymentError, 'later deployment failure'
                ):
                    installer.execute()
            self.assertIsNotNone(installer.release)
            self.assertTrue((installer.release / installer_constants.MANIFEST_FILE).is_file())
            installer._verify_release(installer.release, installer.source)


class CutoverInterruptionTests(unittest.TestCase):
    def setUp(self):
        helper = fixtures.DeploymentFixtureMixin()
        helper.setUp()
        self.source = helper.source

    def test_cutover_interruptions_keep_remote_stage_and_fences(self):
        for interruption in (
            installer_cli.signal.SIGHUP,
            installer_cli.signal.SIGTERM,
            'ctrl-c',
        ):
            with self.subTest(interruption=interruption):
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

                def trigger():
                    if interruption == 'ctrl-c':
                        raise KeyboardInterrupt
                    installed[interruption](interruption, None)

                remote = CutoverRemote(trigger)
                installer = CutoverWorkflow(self.source, remote)
                stderr = io.StringIO()
                with mock.patch.object(
                    installer_cli, 'Installer', return_value=installer
                ), mock.patch.object(
                    installer_cli.signal, 'signal', side_effect=fake_signal
                ), mock.patch.object(installer_cli.sys, 'stderr', stderr):
                    self.assertEqual(installer_cli.main([]), 130)

                names = [call[0] for call in remote.calls]
                self.assertIn('cutover', names)
                self.assertNotIn('remove-stage', names)
                self.assertNotIn('exit-maintenance', names)
                self.assertNotIn('restore-submission-gate', names)
                self.assertIn('mv -T "$staged" "$release"', remote.scripts['cutover'])
                self.assertTrue(installer.state.cutover_started)
                self.assertTrue(installer.state.remote_stage_created)
                self.assertTrue(installer.state.maintenance_active)
                self.assertTrue(installer.state.submission_gate_active)
                self.assertTrue(installer.state.restore_previous_agent)
                self.assertFalse(
                    any(call[0][1:2] == ['bootstrap'] for call in installer.local.calls)
                )
                self.assertEqual(installer.events[-2:], ['cleanup', 'lock-close'])
                self.assertIn('installer: interrupted', stderr.getvalue())
                self.assertIn('queue remains in maintenance', installer.err.getvalue())
                self.assertEqual(
                    signal_calls[2:],
                    [
                        (installer_cli.signal.SIGHUP, previous[installer_cli.signal.SIGHUP]),
                        (installer_cli.signal.SIGTERM, previous[installer_cli.signal.SIGTERM]),
                    ],
                )


if __name__ == '__main__':
    unittest.main()
