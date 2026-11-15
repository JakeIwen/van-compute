from pathlib import Path
import tempfile
import unittest

from van_compute import worker
from van_compute.path_replace import replace_paths_once


class ReplacePathsOnceTests(unittest.TestCase):
    def test_a_replacement_is_never_rewritten_by_a_later_rule(self):
        result = replace_paths_once(
            b"/a/job/file",
            [(b"/a/job", b"{job}/tmp"), (b"/tmp", b"{tmp}")],
        )
        self.assertEqual(result, b"{job}/tmp/file")

    def test_the_longest_match_wins_at_a_position(self):
        result = replace_paths_once(
            b"/home/u/repo and /home/u",
            [(b"/home/u", b"{home}"), (b"/home/u/repo", b"{repo}")],
        )
        self.assertEqual(result, b"{repo} and {home}")

    def test_no_rules_or_empty_paths_leave_the_data_alone(self):
        self.assertEqual(replace_paths_once(b"abc", []), b"abc")
        self.assertEqual(replace_paths_once(b"abc", [(b"", b"X")]), b"abc")


class ScrubPrivatePathsTests(unittest.TestCase):
    def test_a_token_containing_another_private_path_is_not_scrubbed_again(self):
        with tempfile.TemporaryDirectory() as directory:
            result_root = Path(directory)
            (result_root / "out.txt").write_text("/q/data and /tmp/x\n")
            worker.scrub_private_paths(
                result_root,
                datasets={"x/tmp": Path("/q/data")},
                job_root=Path("/tmp"),
            )
            self.assertEqual(
                (result_root / "out.txt").read_text(),
                "{dataset:x/tmp} and {job}/x\n",
            )


if __name__ == "__main__":
    unittest.main()
