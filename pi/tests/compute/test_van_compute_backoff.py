import contextlib
import io
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from van_compute import broker, worker
from van_compute.backoff import HostRetryGate, RetryCancelled


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class FakeMultiplexer:
    def __init__(self):
        self.invalidations = 0
        self.deadlines = []

    def client_arguments(self, deadline=None):
        self.deadlines.append(deadline)
        return []

    def invalidate(self, deadline=None):
        self.invalidations += 1
        self.deadlines.append(deadline)


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
        for delay in (30, 30, 30, 30):
            clock.now += gate.delay
            gate.failed(gate.acquire())
            self.assertEqual(gate.delay, delay)
        clock.now += gate.delay
        gate.reached(gate.acquire())
        self.assertEqual(gate.delay, 0)
        gate.failed(gate.acquire())
        self.assertEqual(gate.delay, 15)

    def test_base_is_clamped_separately_below_remote_freshness_window(self):
        gate = HostRetryGate(base=300)
        gate.failed(gate.acquire())
        self.assertEqual(gate.base, 30)
        self.assertEqual(gate.cap, 30)
        self.assertEqual(gate.delay, 30)
        self.assertLess(gate.cap, broker.DEFAULT_REMOTE_MAX_AGE)
        self.assertEqual(HostRetryGate(base=300, cap=300).cap, 30)

        namespace = worker.build_parser().parse_args(["--poll-interval", "300"])
        scheduler = worker.PersistentScheduler(
            worker.WorkerConfig.from_namespace(namespace),
            stop_event=threading.Event(),
            remote_factory=lambda identity: worker.RemoteQueue(
                "fake", "/fake/queue", identity
            ),
        )
        self.assertEqual(scheduler.retry_gate.base, 30)
        self.assertEqual(scheduler.retry_gate.cap, 30)

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
        self.assertEqual(run.call_count, 3)
        self.assertIn("available", run.call_args_list[0].args[0][-1])

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

    def test_long_recovered_transfer_does_not_block_heartbeats(self):
        for transfer in ("upload", "stream"):
            with self.subTest(transfer=transfer), tempfile.TemporaryDirectory() as name:
                clock = Clock()
                gate = HostRetryGate(clock=clock)
                gate.failed(gate.acquire())
                clock.now = gate.delay
                long_remote = worker.RemoteQueue("fake", "/fake/queue", "mac.00")
                capacity = worker.RemoteQueue("fake", "/fake/queue", "mac")
                lease = worker.RemoteQueue("fake", "/fake/queue", "mac.01")
                for remote in (long_remote, capacity, lease):
                    remote.retry_gate = gate
                transfer_started = threading.Event()
                release_transfer = threading.Event()
                errors = []

                def transport(arguments, **kwargs):
                    remote_command = arguments[-1]
                    if " available" in remote_command:
                        return subprocess.CompletedProcess(arguments, 0, b"", b"")
                    if " put-result " in remote_command or " stream " in remote_command:
                        transfer_started.set()
                        if not release_transfer.wait(2):
                            raise AssertionError("test transfer was not released")
                        return subprocess.CompletedProcess(arguments, 0, b"{}", b"")
                    if " heartbeat " in remote_command:
                        return subprocess.CompletedProcess(arguments, 0, "{}", "")
                    raise AssertionError(f"unexpected remote command: {remote_command}")

                source = Path(name) / "result"
                source.write_bytes(b"result")

                def run_transfer():
                    try:
                        if transfer == "upload":
                            long_remote.put_result("job", "result", source)
                        else:
                            long_remote.stream_to_file(
                                ["worker", "stream", "job"], Path(name) / "download"
                            )
                    except Exception as exc:
                        errors.append(exc)

                with mock.patch.object(worker.subprocess, "run", side_effect=transport):
                    thread = threading.Thread(target=run_transfer)
                    thread.start()
                    try:
                        self.assertTrue(transfer_started.wait(1))
                        capacity.heartbeat(slots_total=10, slots_busy=1)
                        lease.heartbeat()
                    finally:
                        release_transfer.set()
                        thread.join(2)
                self.assertFalse(thread.is_alive())
                self.assertEqual(errors, [])
                self.assertEqual(gate.delay, 0)

    def test_probe_control_and_transfer_timeouts_release_gate_and_cleanup(self):
        cases = ("probe", "control", "transfer")
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as name:
                clock = Clock()
                gate = HostRetryGate(clock=clock)
                multiplexer = FakeMultiplexer()
                remote = worker.RemoteQueue(
                    "fake",
                    "/fake/queue",
                    "mac.00",
                    multiplexer=multiplexer,
                )
                remote.retry_gate = gate
                if case == "probe":
                    gate.failed(gate.acquire())
                    clock.now = gate.delay

                expired = subprocess.TimeoutExpired("ssh", 1)
                with mock.patch.object(
                    worker.subprocess, "run", side_effect=expired
                ) as run:
                    with self.assertRaises(worker.TransportUnavailable):
                        if case == "transfer":
                            remote.stream_to_file(
                                ["worker", "stream", "job"],
                                Path(name) / "download",
                            )
                        else:
                            remote.heartbeat()

                self.assertEqual(multiplexer.invalidations, 1)
                timeout = run.call_args.kwargs["timeout"]
                if case == "probe":
                    self.assertLessEqual(timeout, worker.RECOVERY_PROBE_TIMEOUT)
                elif case == "control":
                    self.assertLessEqual(timeout, worker.CONTROL_RPC_TIMEOUT)
                else:
                    self.assertGreater(timeout, worker.CONTROL_RPC_TIMEOUT)
                    self.assertEqual(list(Path(name).iterdir()), [])
                clock.now += gate.delay
                generation, owns_probe = gate.acquire_attempt()
                self.assertTrue(owns_probe)
                gate.abandoned(generation)

    def test_failure_after_successful_probe_reopens_current_generation(self):
        remote, clock = self.remote()
        remote.retry_gate.failed(remote.retry_gate.acquire())
        clock.now = remote.retry_gate.delay
        results = [
            subprocess.CompletedProcess([], 0, b"", b""),
            subprocess.CompletedProcess([], 255, "", "offline"),
        ]
        with mock.patch.object(worker.subprocess, "run", side_effect=results) as run:
            with self.assertRaises(worker.TransportUnavailable):
                remote.heartbeat()
        self.assertEqual(run.call_count, 2)
        self.assertIn("available", run.call_args_list[0].args[0][-1])
        self.assertIn("heartbeat", run.call_args_list[1].args[0][-1])
        self.assertEqual(remote.retry_gate.delay, 15)

    def test_control_master_lock_and_startup_obey_deadline_and_cleanup(self):
        with tempfile.TemporaryDirectory() as name:
            multiplexer = worker.SSHMultiplexer(
                "fake", Path(name) / "control.sock"
            )
            multiplexer._lock.acquire()
            started = time.monotonic()
            try:
                with self.assertRaises(subprocess.TimeoutExpired):
                    multiplexer.ensure(time.monotonic() + 0.02)
            finally:
                multiplexer._lock.release()
            self.assertLess(time.monotonic() - started, 0.5)

            class Process:
                def __init__(self):
                    self.stderr = io.BytesIO()
                    self.terminated = False
                    self.killed = False

                def poll(self):
                    return 0 if self.terminated or self.killed else None

                def terminate(self):
                    self.terminated = True

                def wait(self, timeout=None):
                    return 0

                def kill(self):
                    self.killed = True

            process = Process()
            with mock.patch.object(worker.subprocess, "Popen", return_value=process):
                with self.assertRaises(worker.TransportUnavailable):
                    multiplexer.ensure(time.monotonic() + 0.05)
            self.assertTrue(process.terminated)
            self.assertTrue(process.stderr.closed)
            self.assertIsNone(multiplexer._process)

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
