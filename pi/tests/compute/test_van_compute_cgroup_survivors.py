"""The startup cgroup snapshot and the cancel recovery that waits on it."""

import io
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from van_compute import broker, job_cancel, pi_cgroup_survivors
from van_compute.can_activity import CanProbe
from van_compute.pi_cgroup_survivors import GONE, PRESENT, UNCONTAINED, Survivor
from van_compute.pi_health_monitor import HealthMonitor

from pi.tests.compute.test_van_compute_cancel import CancelHarness
from pi.tests.compute.test_van_compute_group_identity import stat_line, write_stat
from pi.tests.compute.van_compute_test_fixtures import GOOD_HEALTH

SERVICE = "/system.slice/van-compute-broker.service"
BROKER_PID = 100


class FakeHost:
    """A /proc and /sys/fs/cgroup the snapshot reads, under a temporary root."""

    def __init__(self, base: Path, cgroup_line: str | None = f"0::{SERVICE}"):
        self.proc = base / "proc"
        self.proc.mkdir()
        self.cgroups = base / "cgroup"
        self.self_cgroup = base / "self-cgroup"
        if cgroup_line is not None:
            self.self_cgroup.write_text(cgroup_line + "\n", encoding="utf-8")

    def run(self, pid: int, start: str) -> None:
        shutil.rmtree(self.proc / str(pid), ignore_errors=True)
        write_stat(self.proc, pid, stat_line("python3", start))

    def exit(self, pid: int) -> None:
        shutil.rmtree(self.proc / str(pid))

    def list_procs(self, *pids: int) -> None:
        directory = self.cgroups / SERVICE.lstrip("/")
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "cgroup.procs").write_text(
            "".join(f"{pid}\n" for pid in pids), encoding="ascii"
        )

    def capture(self, **kwargs) -> pi_cgroup_survivors.StartupSurvivors:
        snapshot = pi_cgroup_survivors.capture(
            self_cgroup=self.self_cgroup, cgroup_root=self.cgroups,
            proc_root=self.proc, self_pid=BROKER_PID,
        )
        # Never signal real processes from a fake /proc.
        return pi_cgroup_survivors.StartupSurvivors(
            snapshot.survivors, snapshot.uncontained_reason, self.proc, **kwargs,
        )


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.host = FakeHost(Path(self.temporary.name))
        self.host.run(BROKER_PID, "5000")

    def test_the_broker_itself_is_not_a_survivor(self):
        self.host.list_procs(BROKER_PID)
        snapshot = self.host.capture()
        self.assertEqual(snapshot.survivors, ())
        self.assertEqual(snapshot.status(), (GONE, ()))

    def test_a_survivor_is_present_until_it_exits(self):
        self.host.run(200, "1000")
        self.host.list_procs(BROKER_PID, 200)
        snapshot = self.host.capture()
        self.assertEqual(snapshot.survivors, (Survivor(200, "1000"),))
        self.assertEqual(snapshot.status(), (PRESENT, (200,)))
        self.host.exit(200)
        self.assertEqual(snapshot.status(), (GONE, ()))

    def test_a_reused_pid_with_another_start_time_counts_as_gone(self):
        self.host.run(200, "1000")
        self.host.list_procs(BROKER_PID, 200)
        snapshot = self.host.capture()
        self.host.run(200, "6000")
        self.assertEqual(snapshot.status(), (GONE, ()))

    def test_a_listed_pid_that_exited_before_its_read_is_skipped(self):
        self.host.list_procs(BROKER_PID, 300)
        self.assertEqual(self.host.capture().survivors, ())

    def test_an_unidentifiable_survivor_counts_while_its_proc_entry_exists(self):
        (self.host.proc / "400").mkdir()  # listed, but its stat is unreadable
        self.host.list_procs(BROKER_PID, 400)
        snapshot = self.host.capture()
        self.assertEqual(snapshot.survivors, (Survivor(400, None),))
        self.assertEqual(snapshot.status(), (PRESENT, (400,)))
        (self.host.proc / "400").rmdir()
        self.assertEqual(snapshot.status(), (GONE, ()))

    def test_no_readable_service_cgroup_is_uncontained(self):
        cases = {
            "foreground scope": "0::/user.slice/user-1000.slice/session-3.scope",
            "cgroup v1 only": "12:pids:/system.slice/van-compute-broker.service",
            "no /proc/self/cgroup": None,
            "unreadable cgroup.procs": f"0::{SERVICE}",
        }
        for name, line in cases.items():
            with self.subTest(case=name), tempfile.TemporaryDirectory() as directory:
                host = FakeHost(Path(directory), line)
                host.run(200, "1000")  # would be a survivor if the cgroup were read
                snapshot = host.capture()
                self.assertIsNotNone(snapshot.uncontained_reason)
                self.assertEqual(snapshot.status(), (UNCONTAINED, ()))

    def test_kill_remaining_signals_each_survivor_and_survives_a_fault(self):
        self.host.run(200, "1000")
        self.host.run(201, "1001")
        self.host.list_procs(BROKER_PID, 200, 201)
        killed = []

        def kill(survivor, _proc_root):
            killed.append(survivor.pid)
            raise OSError("no such process")

        self.host.capture(kill=kill).kill_remaining()
        self.assertEqual(killed, [200, 201])

    def test_an_unidentified_survivor_is_never_signalled(self):
        with mock.patch.object(pi_cgroup_survivors.os, "pidfd_open", create=True) as opened:
            pi_cgroup_survivors.kill_survivor(Survivor(400, None), self.host.proc)
        opened.assert_not_called()


QUIET = CanProbe(True, (), None)


class CancelRecoveryEvidenceTests(CancelHarness, unittest.TestCase):
    def setUp(self):
        super().setUp()
        base = Path(self.temporary.name) / "host"
        base.mkdir()
        self.host = FakeHost(base)
        self.host.run(BROKER_PID, "5000")
        self.killed = []

    def requested_local_job(self):
        job = self.submit()
        self.claim_local()
        job_cancel.cancel_job(self.root, job["id"])
        return job

    def recover(self, snapshot):
        monitor = HealthMonitor(Path("/unused"), probe=lambda _d: QUIET, survivors=snapshot)
        with mock.patch.object(broker.engine, "run_child", side_effect=AssertionError("launched")):
            return broker.run_once(self.args, health_reader=lambda: GOOD_HEALTH, monitor=monitor)

    def snapshot_with_survivor(self):
        self.host.run(200, "1000")
        self.host.list_procs(BROKER_PID, 200)
        return self.host.capture(kill=lambda survivor, _root: self.killed.append(survivor.pid))

    def test_a_live_survivor_defers_and_keeps_the_request_until_it_is_gone(self):
        job = self.requested_local_job()
        snapshot = self.snapshot_with_survivor()
        for _poll in range(2):
            result = self.recover(snapshot)
            self.assertEqual(result["action"], "deferred", result)
            self.assertEqual(result["survivors"], [200])
            self.assertEqual(self.located(job["id"]), ["running"])
            manifest = self.manifest("running", job["id"])
            self.assertTrue(job_cancel.is_cancel_requested(manifest))
            self.assertIn("lease_token", manifest)
        self.assertEqual(self.killed, [200, 200])
        self.host.exit(200)
        result = self.recover(snapshot)
        self.assertEqual(result["action"], "cancelled", result)
        self.assert_final(job["id"], "running")

    def test_a_reused_survivor_pid_does_not_hold_the_cancel(self):
        job = self.requested_local_job()
        snapshot = self.snapshot_with_survivor()
        self.host.run(200, "9999")
        self.assertEqual(self.recover(snapshot)["action"], "cancelled")
        self.assert_final(job["id"], "running")

    def test_an_uncontained_broker_finalizes_at_once(self):
        job = self.requested_local_job()
        self.host.self_cgroup.write_text("0::/user.slice/session-3.scope\n", encoding="utf-8")
        self.host.run(200, "1000")
        self.host.list_procs(BROKER_PID, 200)
        self.assertEqual(self.recover(self.host.capture())["action"], "cancelled")
        self.assert_final(job["id"], "running")


class StartupReportTests(unittest.TestCase):
    def report(self, snapshot, environ, stream=None):
        stream = io.StringIO() if stream is None else stream
        with mock.patch.object(pi_cgroup_survivors, "capture", return_value=snapshot):
            returned = pi_cgroup_survivors.capture_reported(environ, stream)
        self.assertIs(returned, snapshot)
        return stream

    def test_a_service_start_says_which_containment_evidence_it_has(self):
        unreadable = pi_cgroup_survivors.StartupSurvivors((), "cannot read cgroup.procs")
        line = json.loads(self.report(unreadable, {"INVOCATION_ID": "x"}).getvalue())
        self.assertEqual(
            line,
            {"action": "cgroup-survivors", "status": UNCONTAINED, "survivors": [],
             "reason": "cannot read cgroup.procs"},
        )
        # A foreground run (--health, --once from a shell) prints nothing extra.
        self.assertEqual(self.report(unreadable, {}).getvalue(), "")

    def test_a_broken_journal_pipe_does_not_stop_the_start(self):
        class Broken(io.StringIO):
            def write(self, text):
                raise BrokenPipeError

        self.report(pi_cgroup_survivors.StartupSurvivors(()), {"INVOCATION_ID": "x"}, Broken())


if __name__ == "__main__":
    unittest.main()
