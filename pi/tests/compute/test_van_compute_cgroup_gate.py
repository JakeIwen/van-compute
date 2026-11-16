"""The serve loop's service-cgroup gate: what it reads, and what it holds."""

import contextlib
import datetime as dt
import io
import tempfile
import threading
import time
import types
import unittest
from pathlib import Path
from unittest import mock

from van_compute import broker, job_cancel, job_cleanup_marker, pi_cgroup_gate
from van_compute.can_activity import CanProbe
from van_compute.pi_cgroup_gate import CLEAR, HELD, UNCONTAINED, CgroupGate
from van_compute.pi_health_monitor import HealthMonitor

from pi.tests.compute.test_van_compute_broker_cancel import (
    SLEEPER, kill_group_quietly, orphan_group, _group_gone,
)
from pi.tests.compute.test_van_compute_cancel import CancelHarness
from pi.tests.compute.van_compute_test_fixtures import GOOD_HEALTH

SERVICE = "/system.slice/van-compute-broker.service"
BROKER_PID = 100
SYSTEMD = {"INVOCATION_ID": "0123456789abcdef"}


class FakeHost:
    """/proc/self/cgroup and /sys/fs/cgroup for the gate, under a temporary root."""

    def __init__(self, base: Path, cgroup_text: str | None = f"0::{SERVICE}\n"):
        self.self_cgroup = base / "self-cgroup"
        self.cgroups = base / "cgroup"
        if cgroup_text is not None:
            self.self_cgroup.write_text(cgroup_text, encoding="utf-8")

    def members(self, *pids: int, text: str | None = None) -> None:
        directory = self.cgroups / SERVICE.lstrip("/")
        directory.mkdir(parents=True, exist_ok=True)
        body = "".join(f"{pid}\n" for pid in pids) if text is None else text
        (directory / "cgroup.procs").write_text(body, encoding="ascii")

    def gate(self, environ=SYSTEMD) -> CgroupGate:
        return CgroupGate(self.self_cgroup, self.cgroups, environ, BROKER_PID)


class MembershipTests(unittest.TestCase):
    def host(self, *args):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        return FakeHost(Path(directory.name), *args)

    def test_only_the_broker_is_clear_and_any_other_member_holds(self):
        host = self.host()
        host.members(BROKER_PID)
        self.assertEqual(host.gate().read().state, CLEAR)
        host.members(BROKER_PID, 4242, 77)
        membership = host.gate().read()
        self.assertEqual((membership.state, membership.others), (HELD, (77, 4242)))

    def test_an_inspection_failure_inside_a_service_holds(self):
        cases = {
            "unreadable cgroup.procs": (f"0::{SERVICE}\n", None),
            "unparsable cgroup.procs": (f"0::{SERVICE}\n", "100\nnot-a-pid\n"),
            "broker not listed": (f"0::{SERVICE}\n", "4242\n"),
            "no /proc/self/cgroup under systemd": (None, None),
            "cgroup v1 only under systemd": ("12:pids:/system.slice/x.service\n", None),
        }
        for name, (cgroup_text, procs) in cases.items():
            with self.subTest(case=name):
                host = self.host(cgroup_text)
                if procs is not None:
                    host.members(text=procs)
                membership = host.gate().read()
                self.assertEqual(membership.state, HELD)
                self.assertIsNotNone(membership.reason)

    def test_outside_a_service_is_uncontained(self):
        cases = {
            "a session scope": ("0::/user.slice/user-1000.slice/session-3.scope\n", SYSTEMD),
            "no /proc/self/cgroup (macOS)": (None, {}),
            "cgroup v1 only, not systemd": ("12:pids:/user.slice\n", {}),
        }
        for name, (cgroup_text, environ) in cases.items():
            with self.subTest(case=name):
                host = self.host(cgroup_text)
                host.members(BROKER_PID, 4242)  # never consulted
                self.assertEqual(host.gate(environ).read().state, UNCONTAINED)

    def test_only_the_serve_loop_is_gated(self):
        for flag in ("once", "health", "self_test"):
            with self.subTest(mode=flag):
                config = types.SimpleNamespace(once=False, health=False, self_test=False)
                setattr(config, flag, True)
                self.assertIsNone(pi_cgroup_gate.for_config(config))

    def test_a_service_start_logs_its_evidence_once_and_never_throws(self):
        serve = types.SimpleNamespace(once=False, health=False, self_test=False)
        host = self.host()
        host.members(BROKER_PID, 4242)
        stream = io.StringIO()
        gate = pi_cgroup_gate.for_config(serve, gate=host.gate(), stream=stream)
        self.assertIsNotNone(gate)
        self.assertEqual(
            stream.getvalue(),
            '{"action": "cgroup-gate", "members": [4242], "state": "held"}\n',
        )
        quiet = io.StringIO()
        pi_cgroup_gate.for_config(serve, gate=self.host(None).gate({}), stream=quiet)
        self.assertEqual(quiet.getvalue(), "")  # a foreground run prints nothing

        class Broken(io.StringIO):
            def write(self, _text):
                raise BrokenPipeError("journal gone")

        self.assertIsNotNone(pi_cgroup_gate.for_config(serve, gate=host.gate(), stream=Broken()))


QUIET = CanProbe(False, (), None)


class GatedPollTests(CancelHarness, unittest.TestCase):
    def setUp(self):
        super().setUp()
        base = Path(self.temporary.name) / "host"
        base.mkdir()
        self.host = FakeHost(base)
        self.leftover(4242)

    def leftover(self, *pids):
        self.host.members(BROKER_PID, *pids)

    def poll(self, execute=None):
        monitor = HealthMonitor(Path("/unused"), probe=lambda _d: QUIET,
                                containment=self.host.gate())
        with contextlib.ExitStack() as stack:
            stack.enter_context(
                mock.patch.object(broker, "bubblewrap_self_test", return_value=(True, "t")))
            stack.enter_context(
                mock.patch.object(broker, "bubblewrap_command", side_effect=self.no_sandbox))
            if execute is not None:
                stack.enter_context(
                    mock.patch.object(broker, "execute_claimed_job", side_effect=execute))
            return broker.run_once(self.args, health_reader=lambda: GOOD_HEALTH, monitor=monitor)

    def recording(self):
        executed = []

        def execute(_args, _root, manifest, **_kwargs):
            executed.append(manifest["id"])
            return {"ok": True, "job": manifest["id"]}

        return executed, execute

    def assert_held(self, result):
        self.assertEqual(result["action"], "deferred", result)
        self.assertEqual(result["cgroup_members"], [4242])

    def test_an_unmarked_restart_cancel_waits_for_the_leftover(self):
        job = self.submit()
        self.claim_local()
        job_cancel.cancel_job(self.root, job["id"])
        for _poll in range(2):
            self.assert_held(self.poll())
            self.assertEqual(self.located(job["id"]), ["running"])
            manifest = self.manifest("running", job["id"])
            self.assertTrue(job_cancel.is_cancel_requested(manifest))
            self.assertIn("lease_token", manifest)
        self.leftover()
        self.assertEqual(self.poll()["action"], "cancelled")
        self.assert_final(job["id"], "running")

    def test_an_ordinary_resume_and_a_new_claim_wait_for_the_leftover(self):
        for case in ("resume", "claim"):
            with self.subTest(case=case):
                self.tearDown()
                self.setUp()
                job = self.submit()
                if case == "resume":
                    self.claim_local()
                executed, execute = self.recording()
                self.assert_held(self.poll(execute))
                self.assertEqual(executed, [])
                self.assertEqual(self.located(job["id"]), ["running" if case == "resume" else "queued"])
                self.leftover()
                self.assertEqual(self.poll(execute)["action"], "executed")
                self.assertEqual(executed, [job["id"]])

    def test_a_cancel_after_a_released_resume_is_an_ordinary_cancel(self):
        marker = Path(self.temporary.name) / "sleeper-started"
        (self.source / "tools" / "local_job.py").write_text(
            SLEEPER.format(marker=str(marker)), encoding="utf-8"
        )
        job = self.submit()
        self.claim_local()
        self.assert_held(self.poll())
        self.leftover()

        def cancel_when_started():
            deadline = time.monotonic() + 20
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(0.05)
            job_cancel.cancel_job(self.root, job["id"])

        thread = threading.Thread(target=cancel_when_started)
        thread.start()
        result = self.poll()
        thread.join()
        self.assertEqual(result["action"], "cancelled", result)
        self.assert_final(job["id"], "running")

    def test_marked_recovery_runs_while_the_gate_is_held(self):
        job = self.submit()
        claimed = self.claim_local()
        group = orphan_group()
        self.addCleanup(kill_group_quietly, group)
        job_cleanup_marker.mark_cleanup_unconfirmed(
            self.root, job["id"], claimed["lease_token"], worker=broker.LOCAL_WORKER,
            now=dt.datetime.now(dt.timezone.utc), group=group, boot_id=None,
        )
        job_cancel.cancel_job(self.root, job["id"])
        result = self.poll()
        self.assertEqual(result["action"], "cancelled", result)
        self.assertTrue(_group_gone(group))
        self.assert_final(job["id"], "running")


if __name__ == "__main__":
    unittest.main()
