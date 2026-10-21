from pathlib import Path
import tempfile
import unittest
import zipfile

import run_tests


class OfflineWheelhouseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.archive = self.root / "wheels.zip"
        self.output = self.root / "output"
        self.output.mkdir()

    def tearDown(self):
        self.temporary.cleanup()

    def write_archive(self, entries):
        with zipfile.ZipFile(self.archive, "w") as bundle:
            for name, content in entries:
                bundle.writestr(name, content)

    def test_extracts_only_wheels_by_basename(self):
        self.write_archive(
            [("mac/Flask-3.1.2.whl", b"mac"), ("README.txt", b"ignore")]
        )

        extracted = run_tests._extract_wheels(self.archive, self.output)

        self.assertEqual(extracted, (self.output / "Flask-3.1.2.whl",))
        self.assertEqual(extracted[0].read_bytes(), b"mac")

    def test_duplicate_wheel_basename_is_rejected(self):
        self.write_archive([("mac/a.whl", b"a"), ("pi/a.whl", b"b")])

        with self.assertRaisesRegex(RuntimeError, "duplicate basename"):
            run_tests._extract_wheels(self.archive, self.output)

    def test_archive_without_wheels_is_rejected(self):
        self.write_archive([("README.txt", b"none")])

        with self.assertRaisesRegex(RuntimeError, "contains no .whl"):
            run_tests._extract_wheels(self.archive, self.output)


if __name__ == "__main__":
    unittest.main()
