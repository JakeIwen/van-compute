"""Exercise the real staging body with an in-memory remote transport."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

from macbook.scripts.van_compute_installer import constants
from pi.tests.compute.van_compute_deployment_support import (
    DeploymentFixtureMixin, FakeRemote,
)


class InspectingRemote(FakeRemote):
    def upload(self, name, sources, destination):
        super().upload(name, sources, destination)
        self.staging = sources[0].parent
        self.payload = {
            path.relative_to(self.staging).as_posix(): path.read_bytes()
            for path in self.staging.rglob('*') if path.is_file()
        }


class RemoteStagingTests(DeploymentFixtureMixin, unittest.TestCase):
    def test_real_stage_uploads_complete_package_and_integrity_markers(self):
        with tempfile.TemporaryDirectory() as directory:
            remote = InspectingRemote()
            installer = self.make_installer(directory, remote=remote)
            source = installer.build_source_release()
            release = Path(directory) / 'release'
            release.mkdir()
            installer._copy_source_tree(release, source)
            installer.paths.support_root.mkdir(parents=True)

            installer.stage_remote_release(source, release)

            self.assertEqual([call[0] for call in remote.calls],
                             ['create-stage', 'upload-release'])
            self.assertTrue(installer.state.remote_stage_created)
            self.assertFalse(remote.staging.exists())
            self.assertEqual(remote.calls[-1][2], installer.remote_stage + '/release/')
            self.assertEqual(remote.payload[constants.SOURCE_HASH_FILE].decode().strip(),
                             source.source_fingerprint)
            provenance = json.loads(remote.payload[constants.PROVENANCE_FILE])
            self.assertEqual(provenance['kind'], 'pi-broker')
            self.assertEqual(provenance['source_sha256'], source.source_fingerprint)
            records = json.loads(remote.payload[constants.MANIFEST_FILE])['files']
            self.assertEqual(set(records), set(remote.payload) - {constants.MANIFEST_FILE})
            self.assertIn('van_compute/protocol.py', records)
            self.assertIn('van_compute/engine_process.py', records)
            self.assertFalse(any(path.startswith('macbook/') for path in records))
            for name, record in records.items():
                self.assertEqual(record['sha256'], hashlib.sha256(remote.payload[name]).hexdigest())

    def _stage_manifest(self, umask):
        """Run the real staging body under `umask`; return its manifest records."""
        captured = {}

        class Capturing(FakeRemote):
            def upload(inner, name, sources, destination):
                super().upload(name, sources, destination)
                captured['records'] = json.loads(
                    (sources[0].parent / constants.MANIFEST_FILE).read_text())['files']

        previous = os.umask(umask)
        try:
            with tempfile.TemporaryDirectory() as directory:
                installer = self.make_installer(directory, remote=Capturing())
                source = installer.build_source_release()
                release = Path(directory) / 'release'
                release.mkdir()
                installer._copy_source_tree(release, source)
                installer.paths.support_root.mkdir(parents=True)
                installer.stage_remote_release(source, release)
        finally:
            os.umask(previous)
        return captured['records']

    def test_stage_manifest_is_independent_of_caller_umask(self):
        strict = self._stage_manifest(0o077)
        relaxed = self._stage_manifest(0o022)
        self.assertEqual(strict, relaxed)
        for marker in (constants.SOURCE_HASH_FILE, constants.PROVENANCE_FILE):
            self.assertEqual(strict[marker]['mode'], 0o644)


if __name__ == '__main__':
    unittest.main()
