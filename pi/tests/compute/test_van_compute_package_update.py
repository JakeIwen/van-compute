"""Dashboard package updates check compute before any receiver can write."""
import contextlib
import io
import subprocess
import unittest
from unittest import mock

from pi import deploy_python as deployment


class DashboardProviderPreflightTests(unittest.TestCase):
    def invoke(self, arguments, run):
        with mock.patch.object(deployment, 'build_plan', return_value={}), \
                mock.patch.object(deployment, 'make_archive') as archive, \
                mock.patch.object(deployment.subprocess, 'run', side_effect=run), \
                contextlib.redirect_stdout(io.StringIO()):
            deployment.main(arguments)
        return archive

    def test_provider_failure_prevents_package_transfer(self):
        for failure in (subprocess.CalledProcessError(1, ['fake-ssh']),
                        subprocess.TimeoutExpired(['fake-ssh'], 30),
                        OSError('fake connection failure')):
            calls = []

            def fail_guard(command, **kwargs):
                calls.append((command, kwargs))
                raise failure

            with self.subTest(failure=failure), \
                    mock.patch.object(deployment.tempfile, 'TemporaryFile') as temporary:
                with self.assertRaisesRegex(RuntimeError, 'no package deployment was started'):
                    self.invoke(['--update', '--service', deployment.DASHBOARD], fail_guard)
                temporary.assert_not_called()
                self.assertEqual(len(calls), 1)
                self.assertEqual(calls[0][0][-1], '/usr/bin/python3 -B -')
                self.assertEqual(calls[0][1]['input'],
                                 (deployment.ROOT / 'pi/check_compute_provider.py').read_bytes())

    def test_healthy_provider_precedes_receiver_for_default_and_scoped_update(self):
        for selection in ([], ['--service', deployment.DASHBOARD]):
            calls = []

            def success(command, **kwargs):
                calls.append((command, kwargs))
                return subprocess.CompletedProcess(command, 0)

            with self.subTest(selection=selection):
                archive = self.invoke(['--update', '--target', 'pi@example.test', *selection], success)
                archive.assert_called_once()
                self.assertEqual(len(calls), 2)
                self.assertEqual(calls[0][0][-2:], ['pi@example.test', '/usr/bin/python3 -B -'])
                self.assertEqual(calls[0][1]['timeout'], 30)
                self.assertIn('--receive update', calls[1][0][-1])

    def test_dry_run_never_probes_provider_or_transfers(self):
        run = mock.Mock(side_effect=AssertionError('dry-run attempted transport'))
        archive = self.invoke(['--dry-run', '--update', '--service', deployment.DASHBOARD], run)
        run.assert_not_called()
        archive.assert_not_called()

    def test_other_modes_and_scoped_services_keep_existing_transfer(self):
        for arguments in ([], ['--activate', '--service', deployment.DASHBOARD],
                          ['--update', '--service', 'video-library.service']):
            run = mock.Mock(return_value=subprocess.CompletedProcess([], 0))
            with self.subTest(arguments=arguments):
                self.invoke(arguments, run)
                run.assert_called_once()
                self.assertIn('--receive ', run.call_args.args[0][-1])

    def test_invalid_target_is_rejected_before_provider_probe(self):
        run = mock.Mock(side_effect=AssertionError('invalid target attempted transport'))
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            self.invoke(['--update', '--target=-bad'], run)
        run.assert_not_called()


if __name__ == '__main__':
    unittest.main()
