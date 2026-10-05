import contextlib
import io
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

from van_compute import worker
from van_compute.backoff import HostRetryGate, RetryCancelled


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class BackoffTests(unittest.TestCase):
    def test_default_sequence_cap_reset_and_stale_completions(self):
        clock = Clock()
        gate = HostRetryGate(clock=clock)
        first = gate.acquire()
        concurrent = gate.acquire()
        gate.failed(first)
        gate.failed(concurrent)
        gate.reached(concurrent)
        self.assertEqual(gate.delay, 15)
        for delay in (30, 60, 60, 60):
            clock.now += gate.delay
            gate.failed(gate.acquire())
            self.assertEqual(gate.delay, delay)
        clock.now += gate.delay
        gate.reached(gate.acquire())
        self.assertEqual(gate.delay, 0)
        gate.failed(gate.acquire())
        self.assertEqual(gate.delay, 15)

    def test_base_above_cap_never_shortens_configured_poll_interval(self):
        gate = HostRetryGate(base=300)
        gate.failed(gate.acquire())
        self.assertEqual(gate.delay, 300)

    def test_only_one_recovery_probe_and_wait_is_interruptible(self):
        clock = Clock()
        gate = HostRetryGate(clock=clock)
        gate.failed(gate.acquire())
        clock.now = 15
        probe = gate.acquire()
        stop = threading.Event()
        entered = threading.Event()
        outcomes = []

        def contender():
            entered.set()
            try:
                outcomes.append(gate.acquire([stop]))
            except RetryCancelled:
                outcomes.append("cancelled")

        thread = threading.Thread(target=contender)
        thread.start()
        self.assertTrue(entered.wait(1))
        stop.set()
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(outcomes, ["cancelled"])
        gate.reached(probe)
        self.assertEqual(gate.delay, 0)

    def remote(self):
        clock = Clock()
        remote = worker.RemoteQueue("fake", "/fake/queue", "mac.00")
        remote.retry_gate = HostRetryGate(clock=clock)
        return remote, clock

    def test_ssh_255_and_timeout_back_off_without_replaying(self):
        for failure in (
            subprocess.CompletedProcess([], 255, "", "offline"),
            subprocess.TimeoutExpired("ssh", 90),
        ):
            with self.subTest(failure=failure):
                remote, _clock = self.remote()
                kwargs = (
                    {"side_effect": failure}
                    if isinstance(failure, Exception)
                    else {"return_value": failure}
                )
                with mock.patch.object(worker.subprocess, "run", **kwargs) as run:
                    with self.assertRaises(worker.TransportUnavailable):
                        remote.finish("job", 0, [])
                self.assertEqual(run.call_count, 1)
                self.assertEqual(remote.retry_gate.delay, 15)

    def test_reachable_application_errors_reset_without_backoff(self):
        for result in (
            subprocess.CompletedProcess([], 1, "", "lease conflict"),
            subprocess.CompletedProcess([], 0, "bad json", ""),
        ):
            with self.subTest(result=result):
                remote, clock = self.remote()
                remote.retry_gate.failed(remote.retry_gate.acquire())
                clock.now = 15
                with mock.patch.object(worker.subprocess, "run", return_value=result):
                    with self.assertRaises(worker.WorkerError):
                        remote.claim()
                self.assertEqual(remote.retry_gate.delay, 0)

    def test_drain_stops_waiting_claims_not_active_heartbeat_or_upload(self):
        remote, clock = self.remote()
        remote.retry_gate.failed(remote.retry_gate.acquire())
        clock.now = 15
        remote.stop_event = threading.Event()
        remote.drain_event = threading.Event()
        remote.drain_event.set()
        with mock.patch.object(
            worker.subprocess,
            "run",
            return_value=subprocess.CompletedProcess([], 0, "{}", ""),
        ) as run:
            with self.assertRaises(worker.WorkerShutdown):
                remote.claim()
            remote.heartbeat()
            remote.finish("job", 0, [])
        self.assertEqual(run.call_count, 2)

    def test_service_starts_without_eager_control_master_connections(self):
        with tempfile.TemporaryDirectory() as name:
            arguments = [
                "--serve",
                "--worker",
                "test",
                "--work-root",
                str(Path(name) / "jobs"),
            ]
            for flag in ("--python", "--sqlite3", "--rg", "--jadx"):
                arguments += [flag, sys.executable]
            with mock.patch.object(
                worker.SSHMultiplexer,
                "ensure",
                side_effect=AssertionError("eager connection"),
            ), mock.patch.object(worker.SSHMultiplexer, "close"), mock.patch.object(
                worker, "run_scheduler"
            ) as serve, mock.patch.object(
                worker.signal, "signal"
            ), contextlib.redirect_stdout(
                io.StringIO()
            ):
                self.assertEqual(worker.main(arguments), 0)
            self.assertEqual(len(serve.call_args.kwargs["multiplexers"]), 4)
            scheduler = worker.PersistentScheduler(
                serve.call_args.args[0],
                stop_event=threading.Event(),
                multiplexers=serve.call_args.kwargs["multiplexers"],
            )
            self.assertEqual(
                len({id(remote.multiplexer) for remote in scheduler.remotes}), 4
            )
            self.assertTrue(
                all(
                    remote.retry_gate is scheduler.retry_gate
                    for remote in [*scheduler.remotes, scheduler.coordinator]
                )
            )

    def test_concurrent_claim_and_heartbeat_count_as_one_outage(self):
        gate = HostRetryGate()
        barrier = threading.Barrier(2)
        errors = []
        remotes = [
            worker.RemoteQueue("fake", "/fake/queue", identity)
            for identity in ("mac", "mac.00")
        ]
        for remote in remotes:
            remote.retry_gate = gate

        def transport(*args, **kwargs):
            barrier.wait(timeout=2)
            return subprocess.CompletedProcess([], 255, "", "offline")

        def attempt(call):
            try:
                call()
            except worker.TransportUnavailable:
                errors.append("offline")

        with mock.patch.object(worker.subprocess, "run", side_effect=transport):
            threads = [
                threading.Thread(target=attempt, args=(call,))
                for call in (remotes[0].heartbeat, remotes[1].claim)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(3)
                self.assertFalse(thread.is_alive())
        self.assertEqual(errors, ["offline", "offline"])
        self.assertEqual(gate.delay, 15)

    def test_healthy_gate_does_not_change_existing_stop_behavior(self):
        gate = HostRetryGate()
        stop = threading.Event()
        stop.set()
        self.assertEqual(gate.acquire([stop]), 0)

    def test_transport_signals_preserve_published_failure_diagnostics(self):
        for original in (worker.WorkerError("offline"), OSError("cannot connect")):
            signal = worker.TransportUnavailable(str(original))
            if isinstance(original, OSError):
                signal.__cause__ = original
            with tempfile.TemporaryDirectory() as name, mock.patch.object(
                worker, "utc_now", return_value="frozen"
            ):
                root = Path(name)
                worker.record_worker_failure(
                    root / "old", {"id": "job"}, original, worker_id="mac.00"
                )
                worker.record_worker_failure(
                    root / "new", {"id": "job"}, signal, worker_id="mac.00"
                )
                for path in (root / "old").iterdir():
                    self.assertEqual(
                        path.read_bytes(), (root / "new" / path.name).read_bytes()
                    )

    def test_stream_transport_failure_never_publishes_partial_download(self):
        remote, _clock = self.remote()
        with tempfile.TemporaryDirectory() as name:
            destination = Path(name) / "bundle"
            with mock.patch.object(
                worker.subprocess,
                "run",
                return_value=subprocess.CompletedProcess([], 255, b"", b"offline"),
            ):
                with self.assertRaises(worker.TransportUnavailable):
                    remote.stream_to_file(["worker", "stream"], destination)
            self.assertFalse(destination.exists())
            self.assertEqual(list(Path(name).iterdir()), [])
            self.assertEqual(remote.retry_gate.delay, 15)


if __name__ == "__main__":
    unittest.main()
