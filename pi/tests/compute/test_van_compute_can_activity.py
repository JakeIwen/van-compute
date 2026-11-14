import os
from pathlib import Path
import tempfile
import unittest

from van_compute import can_activity
from van_compute.can_activity import CanQuietTracker, probe_can_activity


def record(
    path: Path,
    *,
    kind="FLOCK",
    mode="WRITE",
    pid="1234",
    waiter=False,
    indent="",
    number=1,
) -> str:
    """A /proc/locks line for `path`, built from the file's real identity."""
    info = os.stat(path)
    device = f"{os.major(info.st_dev):02x}:{os.minor(info.st_dev):02x}"
    arrow = "-> " if waiter else ""
    return (
        f"{indent}{number}: {arrow}{kind}  ADVISORY  {mode} {pid} "
        f"{device}:{info.st_ino} 0 EOF"
    )


class ProbeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        base = Path(self.temporary.name)
        self.lock_dir = base / "locks"
        self.lock_dir.mkdir()
        self.can0 = self.lock_dir / "active-diagnostics-can0.lock"
        self.role = self.lock_dir / "active-diagnostics-can-role-drive.lock"
        self.other = self.lock_dir / "unrelated.lock"
        for path in (self.can0, self.role, self.other):
            path.write_text("")
        self.proc_locks = base / "locks.txt"

    def probe(self, *lines: str):
        self.proc_locks.write_text("\n".join(lines) + "\n", encoding="ascii")
        return probe_can_activity(self.lock_dir, self.proc_locks)

    def test_decodes_hex_device_and_decimal_inode(self):
        identities = {(179, 2, 298528): "active-diagnostics-can0.lock"}
        inodes = frozenset({298528})
        line = "7: FLOCK  ADVISORY  WRITE 812 b3:02:298528 0 EOF"
        self.assertEqual(
            can_activity.write_lock_name(line, identities, inodes),
            "active-diagnostics-can0.lock",
        )
        # Same inode on another device, or a decimal reading of the device, is
        # a different file.
        for other in ("b3:03:298528", "00:02:298528", "9:2:298528"):
            self.assertIsNone(
                can_activity.write_lock_name(
                    line.replace("b3:02:298528", other), identities, inodes
                )
            )

    def test_write_lock_of_each_type_is_active(self):
        for kind, pid in (("FLOCK", "10"), ("POSIX", "11"), ("OFDLCK", "-1")):
            with self.subTest(kind=kind):
                result = self.probe(record(self.can0, kind=kind, pid=pid))
                self.assertTrue(result.active)
                self.assertEqual(result.held_locks, ("active-diagnostics-can0.lock",))
                self.assertIsNone(result.error)

    def test_waiters_count_including_indented_records(self):
        for line in (
            record(self.can0, waiter=True),
            record(self.can0, waiter=True, indent="  "),
            "2:  " + record(self.can0, waiter=True).split(": ", 1)[1],
        ):
            with self.subTest(line=line):
                self.assertTrue(self.probe(line).active)

    def test_read_locks_and_unrelated_inodes_are_ignored(self):
        result = self.probe(
            record(self.can0, mode="READ"),
            record(self.can0, kind="POSIX", mode="READ", waiter=True, number=2),
            record(self.other, number=3),
            "4: FLOCK  ADVISORY  WRITE 1 08:01:99999999 0 EOF",
        )
        self.assertFalse(result.active)
        self.assertEqual(result.held_locks, ())
        self.assertIsNone(result.error)

    def test_names_every_held_lock_file(self):
        result = self.probe(
            record(self.role, number=1),
            record(self.can0, kind="POSIX", number=2),
        )
        self.assertEqual(
            result.held_locks,
            ("active-diagnostics-can-role-drive.lock", "active-diagnostics-can0.lock"),
        )

    def test_malformed_relevant_record_fails_closed(self):
        good = record(self.can0)
        for broken in (
            good.replace("WRITE", "EXCL"),
            good.replace("FLOCK", "LEASE"),
            good.replace(" 1234 ", " pid "),
            good.rsplit(" ", 2)[0],
        ):
            with self.subTest(line=broken):
                result = self.probe(broken)
                self.assertFalse(result.active)
                self.assertIsNotNone(result.error)
                self.assertFalse(result.quiet)

    def test_malformed_unrelated_lines_are_ignored(self):
        result = self.probe(
            "garbage",
            "9: LEASE ACTIVE READ 1 08:01:5 0 EOF",
            "10: FLOCK ADVISORY WRITE x zz:yy:123456789 0",
        )
        self.assertTrue(result.quiet)

    def test_failures_fail_closed(self):
        self.proc_locks.write_text("", encoding="ascii")
        missing = probe_can_activity(self.lock_dir / "absent", self.proc_locks)
        self.assertIsNotNone(missing.error)
        unreadable = probe_can_activity(self.lock_dir, self.lock_dir / "no-locks")
        self.assertIsNotNone(unreadable.error)
        self.assertFalse(unreadable.quiet)

    def test_stat_race_fails_closed(self):
        real_stat = os.stat

        def vanishing(path, *args, **kwargs):
            if str(path).endswith("can0.lock"):
                raise FileNotFoundError(path)
            return real_stat(path, *args, **kwargs)

        original = can_activity.os.stat
        can_activity.os.stat = vanishing
        try:
            result = self.probe(record(self.role))
        finally:
            can_activity.os.stat = original
        self.assertIsNotNone(result.error)
        self.assertFalse(result.active)

    def test_identities_are_relisted_on_every_probe(self):
        self.assertTrue(self.probe().quiet)
        replacement = self.lock_dir / "active-diagnostics-can1.lock"
        replacement.write_text("")
        self.assertTrue(self.probe(record(replacement)).active)
        replacement.unlink()
        self.assertTrue(self.probe().quiet)

    def test_probe_needs_only_stat_never_open_access(self):
        # Mode 000 makes any open() fail, so a passing probe proves it only stats.
        for path in (self.can0, self.role):
            path.chmod(0)
        self.addCleanup(self.can0.chmod, 0o600)
        self.addCleanup(self.role.chmod, 0o600)
        self.assertTrue(self.probe(record(self.can0)).active)


class TrackerTests(unittest.TestCase):
    def test_first_quiet_observation_starts_the_chain(self):
        tracker = CanQuietTracker()
        self.assertEqual(tracker.quiet_seconds(0.0), 0.0)
        tracker.observe(100.0, quiet=True)
        self.assertEqual(tracker.quiet_seconds(100.0), 0.0)
        tracker.observe(110.0, quiet=True)
        self.assertEqual(tracker.quiet_seconds(110.0), 10.0)

    def test_gap_over_fifteen_seconds_restarts_the_chain(self):
        tracker = CanQuietTracker()
        tracker.observe(0.0, quiet=True)
        tracker.observe(15.0, quiet=True)
        self.assertEqual(tracker.quiet_seconds(15.0), 15.0)
        tracker.observe(30.1, quiet=True)
        self.assertEqual(tracker.quiet_seconds(30.1), 0.0)
        tracker.observe(40.0, quiet=True)
        self.assertAlmostEqual(tracker.quiet_seconds(40.0), 9.9)

    def test_stale_chain_is_not_reported_as_quiet(self):
        tracker = CanQuietTracker()
        tracker.observe(0.0, quiet=True)
        tracker.observe(10.0, quiet=True)
        self.assertEqual(tracker.quiet_seconds(100.0), 0.0)

    def test_active_and_failed_probes_reset_the_chain(self):
        tracker = CanQuietTracker()
        tracker.observe(0.0, quiet=True)
        tracker.observe(10.0, quiet=True)
        tracker.observe(11.0, quiet=False)  # a lock appeared
        self.assertEqual(tracker.quiet_seconds(11.0), 0.0)
        tracker.observe(12.0, quiet=True)
        tracker.observe(20.0, quiet=True)
        self.assertEqual(tracker.quiet_seconds(20.0), 8.0)
        tracker.observe(21.0, quiet=probe_can_activity(Path("/nonexistent")).quiet)
        self.assertEqual(tracker.quiet_seconds(21.0), 0.0)


if __name__ == "__main__":
    unittest.main()
