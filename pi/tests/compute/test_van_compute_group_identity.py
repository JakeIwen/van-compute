import tempfile
import unittest
from pathlib import Path

from van_compute import job_cleanup_marker


def write_stat(proc_root, pid, text):
    directory = proc_root / str(pid)
    directory.mkdir(parents=True)
    (directory / "stat").write_text(text, encoding="ascii")


def stat_line(comm, start):
    # pid (comm) state ppid pgrp session tty tpgid flags minflt cminflt majflt
    # cmajflt utime stime cutime cstime priority nice threads itrealvalue
    # starttime ...
    after = ["S", "1", "4242", "4242", "0", "-1", "0", "0", "0", "0", "0",
             "0", "0", "0", "0", "20", "0", "1", "0", start, "1000"]
    return f"4242 ({comm}) " + " ".join(after) + "\n"


class ReadGroupStartTests(unittest.TestCase):
    def read(self, text):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            if text is not None:
                write_stat(root, 4242, text)
            return job_cleanup_marker.read_group_start(4242, root)

    def test_reads_field_22(self):
        self.assertEqual(self.read(stat_line("sleep", "123456")), "123456")

    def test_a_command_name_with_spaces_and_parentheses_is_skipped(self):
        self.assertEqual(self.read(stat_line("a) b (c", "777")), "777")

    def test_no_process_or_a_short_line_is_none(self):
        self.assertIsNone(self.read(None))
        self.assertIsNone(self.read("4242 (x) S 1\n"))
        self.assertIsNone(self.read("garbage"))


if __name__ == "__main__":
    unittest.main()
