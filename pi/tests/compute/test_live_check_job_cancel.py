"""The pure parts of the post-deploy cancel live check (it never runs here)."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import signal
import subprocess
import sys
import unittest
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[3] / "macbook/scripts/live_check_job_cancel.py"
spec = importlib.util.spec_from_file_location("live_check_job_cancel", SCRIPT)
live = importlib.util.module_from_spec(spec)
# Registered first: dataclasses look up their module while the class is built.
sys.modules[spec.name] = live
spec.loader.exec_module(live)

JOB = "20261116T010000Z-0000abcd"


class LiveCheckHelperTests(unittest.TestCase):
    def test_the_listing_finds_mac_work_and_foreign_jobs(self):
        text = "\n".join(json.dumps(item) for item in (
            {"id": JOB, "state": "running", "worker": "m4mac.03"},
            {"id": "20261116T010000Z-00000002", "state": "queued", "worker": None},
            {"id": "20261116T010000Z-00000003", "state": "running", "worker": "vanpi-local.00"},
            {"id": ".staging-20261116T010000Z-00000004", "state": "queued", "worker": None},
        )) + "\n"
        entries = live.parse_listing(text)
        self.assertEqual([entry.id for entry in live.mac_work(entries)], [JOB])
        self.assertEqual(
            live.foreign_ids(entries, [JOB]),
            ["20261116T010000Z-00000002", "20261116T010000Z-00000003"],
        )
        self.assertEqual(live.mac_work([live.Entry("x", "running", "m4macbook.01")]), [])

    def test_launch_lines_give_the_exact_group(self):
        log = (
            "van-compute-worker: launched process group 111 in 20261116T010000Z-00000009-abc\n"
            f"van-compute-worker: launched process group 222 in {JOB}-xyz\n"
            f"van-compute-worker: launched process group 333 in {JOB}-again\n"
        )
        self.assertEqual(live.mac_launch_group(log, JOB), 333)
        self.assertIsNone(live.mac_launch_group(log, "20261116T010000Z-0000ffff"))
        journal = "\n".join([
            "not json",
            json.dumps({"action": "cleanup-hold", "job": JOB, "group": 5}),
            json.dumps({"action": "job-launched", "job": JOB, "group": 4242, "leader_start": "998"}),
        ])
        self.assertEqual(live.pi_launch(journal, JOB), (4242, "998"))
        self.assertIsNone(live.pi_launch(journal, "20261116T010000Z-0000ffff"))

    def test_lease_lost_and_fresh_heartbeat_detection(self):
        self.assertTrue(live.lease_lost_logged(
            f"[m4mac.03] job {JOB} attempt 1 is no longer leased here", JOB))
        self.assertFalse(live.lease_lost_logged("nothing", JOB))
        available = {"workers": [{"worker": "m4mac", "age_seconds": 3.0},
                                 {"worker": "m4mac.01", "age_seconds": 1.0}]}
        self.assertTrue(live.coordinator_fresh(available))
        self.assertFalse(live.coordinator_fresh({"workers": [{"worker": "m4mac", "age_seconds": 50}]}))

    def test_dry_run_touches_nothing(self):
        import contextlib
        import io
        from unittest import mock

        out = io.StringIO()
        with mock.patch.object(live, "run", side_effect=AssertionError("touched a host")), \
                contextlib.redirect_stdout(out):
            self.assertEqual(live.main(["--dry-run"]), 0)
        self.assertIn("plan", json.loads(out.getvalue()))


def done(code=0, out="", err=""):
    return subprocess.CompletedProcess([], code, out, err)


class FakeHosts:
    """Stands in for launchctl and the Pi; records what the script ran."""

    def __init__(self, entries=(), heartbeat_age=0.0, bootstrap_code=0):
        self.entries = list(entries)
        self.heartbeat_age = heartbeat_age
        self.bootstrap_code = bootstrap_code
        self.loaded = True
        self.launchctl = []
        self.pi_commands = []
        self.on_enable = None
        self.entries_after_kill = None

    def run(self, command, timeout=60):
        verb = command[1]
        self.launchctl.append(verb)
        if verb == "print":
            return done(0 if self.loaded else 1)
        if verb in ("kill", "bootout"):
            self.loaded = False
            if verb == "kill" and self.entries_after_kill is not None:
                self.entries = self.entries_after_kill
        if verb == "enable" and self.on_enable:
            self.on_enable()
        if verb == "bootstrap":
            self.loaded = self.bootstrap_code == 0
            return done(self.bootstrap_code, err="boom")
        return done()

    def pi(self, command, timeout=60):
        self.pi_commands.append(command)
        return done()

    def patches(self):
        return [
            mock.patch.object(live, "run", self.run),
            mock.patch.object(live, "pi", self.pi),
            mock.patch.object(live, "listing", lambda: self.entries),
            mock.patch.object(live, "status", lambda job: {"state": "running"}),
            mock.patch.object(live, "pi_json", lambda command: (0, {
                "workers": [{"worker": "m4mac", "age_seconds": self.heartbeat_age}]})),
            mock.patch.object(live.time, "sleep", lambda _seconds: None),
            mock.patch.object(live, "MAC_BASE", "m4mac"),
        ]

    def transaction(self, pi_stage=None):
        report = live.Report()
        outage = live.MacWorkerOutage(report, heartbeat_seconds=0.05)
        stack = contextlib.ExitStack()
        with stack:
            stack.enter_context(contextlib.redirect_stderr(io.StringIO()))
            for patch in self.patches():
                stack.enter_context(patch)
            if pi_stage:
                stack.enter_context(mock.patch.object(live, "stage_pi", pi_stage))
            return report, live.stage_pi_transaction(report, live.Deadline(600), outage)


class LiveCheckRestorationTests(unittest.TestCase):
    def test_a_second_signal_during_restore_still_bootstraps_and_own_jobs_are_cancelled(self):
        hosts = FakeHosts()
        hosts.on_enable = lambda: signal.getsignal(signal.SIGINT)(signal.SIGINT, None)
        before = signal.getsignal(signal.SIGINT)

        def interrupted(report, deadline):
            report.own.append(JOB)
            signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)  # the first signal raises

        with self.assertRaises(KeyboardInterrupt):
            hosts.transaction(interrupted)
        self.assertIn("bootstrap", hosts.launchctl)
        self.assertIn(f"{live.PI_COMPUTE} cancel {JOB}", hosts.pi_commands)
        self.assertIs(signal.getsignal(signal.SIGINT), before)

    def test_an_unreadable_running_manifest_aborts_before_the_worker_is_touched(self):
        hosts = FakeHosts([live.Entry(JOB, "running", None)])
        _report, result = hosts.transaction()
        self.assertIs(result["ok"], False)
        self.assertIn(JOB, result["reason"])
        self.assertFalse({"disable", "kill", "bootout"} & set(hosts.launchctl))

    def test_a_drain_abort_without_a_new_heartbeat_fails_the_stage(self):
        # Mac work appears mid-drain, so the stage is skipped; the only heartbeat predates the outage.
        hosts = FakeHosts(heartbeat_age=3.0)
        hosts.entries_after_kill = [live.Entry(JOB, "running", "m4mac.01")]
        _report, result = hosts.transaction()
        self.assertIs(result["ok"], False)
        self.assertIs(result["restored"], False)
        self.assertFalse(result["fresh_heartbeat"])

    def test_a_failed_bootstrap_exits_nonzero_and_prints_the_manual_commands(self):
        hosts = FakeHosts(bootstrap_code=5)
        err, out = io.StringIO(), io.StringIO()
        stack = contextlib.ExitStack()
        with stack:
            for patch in hosts.patches():
                stack.enter_context(patch)
            stack.enter_context(mock.patch.object(live, "stage_queued", lambda r, d: {"ok": True}))
            stack.enter_context(mock.patch.object(live, "stage_mac", lambda r, d: {"ok": True}))
            stack.enter_context(mock.patch.object(live, "stage_pi", lambda r, d: {"ok": True, "job": JOB}))
            stack.enter_context(mock.patch.object(
                live, "MacWorkerOutage", lambda report: _REAL_OUTAGE(report, heartbeat_seconds=0.05)))
            stack.enter_context(contextlib.redirect_stdout(out))
            stack.enter_context(contextlib.redirect_stderr(err))
            code = live.main([])
        self.assertEqual(code, 1)
        self.assertIn("launchctl bootstrap", err.getvalue())


_REAL_OUTAGE = live.MacWorkerOutage


class LiveCheckPiGroupTests(unittest.TestCase):
    def scripted(self, pgrep, stat):
        def fake(command, timeout=60):
            return pgrep if "pgrep" in command else stat
        return mock.patch.object(live, "pi", fake)

    def test_members_with_a_reaped_leader_are_alive_and_errors_are_inconclusive(self):
        with self.scripted(done(0), done(1)):
            self.assertEqual(live.group_state_on_pi(4242, "998"), live.ALIVE)
        with self.scripted(done(255), done(255)):
            self.assertEqual(live.group_state_on_pi(4242, "998"), live.INCONCLUSIVE)
        with self.scripted(done(1), done(1)):
            self.assertEqual(live.group_state_on_pi(4242, "998"), live.GONE)

    def test_the_pi_stage_fails_when_the_group_cannot_be_inspected(self):
        journal = json.dumps({"action": "job-launched", "job": JOB, "group": 4242, "leader_start": "998"})

        def fake_pi(command, timeout=60):
            if "journalctl" in command:
                return done(0, journal)
            if "pgrep" in command:
                return done(255)
            return done(4)

        with mock.patch.object(live, "pi", fake_pi), \
                mock.patch.object(live, "submit", lambda: JOB), \
                mock.patch.object(live, "status", lambda job: {"worker": live.PI_WORKER}), \
                mock.patch.object(live, "pi_json", lambda c: (0, {"cancel_result": "requested"})), \
                mock.patch.object(live.time, "sleep", lambda _s: None):
            result = live.stage_pi(live.Report(), live.Deadline(600))
        self.assertIs(result["ok"], False)
        self.assertEqual(result["group_before"], live.INCONCLUSIVE)


if __name__ == "__main__":
    unittest.main()
