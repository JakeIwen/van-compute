"""Stop a Mac run whose lease the Pi no longer gives it (cancelled, requeued).

The Pi finalizes a cancelled remote job at once and refuses every later
upload, but nothing tells the Mac.  While any run is active, a thread asks the
Pi (`queue.py worker leases`, one read-only RPC for all slots) which jobs this
worker's identities hold, and sets a run's own stop when its (identity, job,
attempt) is missing from a complete reply or carries a cancel request.  Runs
are snapshotted before the RPC and acted on only if still registered after
it, so a reply can never stop a run claimed after the reply was computed (a
slot's next job).

The wrapped job runner gets a stop that is set by the service stop or the
run's own stop; `run_claimed_job` already turns a set stop into
`WorkerShutdown` (nothing uploaded, nothing finished), which the wrapper
reports as `LeaseLost` when the lease was the cause.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import sys
import threading
import time
from typing import Callable, Mapping

from van_compute import protocol

UNSUPPORTED_BACKOFF_SECONDS = 600.0


class LeaseLost(RuntimeError):
    """The Pi no longer gives this run its lease; the run was stopped."""


@dataclass(eq=False)
class ActiveRun:
    identity: str
    job_id: str
    attempt: object
    stop: threading.Event = field(default_factory=threading.Event)


class RunStop:
    """The stop a run sees: the service stop or its own lease stop."""

    def __init__(self, service: threading.Event | None, own: threading.Event) -> None:
        self.service = service
        self.own = own

    def is_set(self) -> bool:
        return self.own.is_set() or (self.service is not None and self.service.is_set())

    def wait(self, timeout: float | None = None) -> bool:
        """Event-compatible wait for either stop (polled; the job runners
        treat the stop as a threading.Event)."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while not self.is_set():
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                return False
            self.own.wait(0.05 if remaining is None else min(0.05, remaining))
        return True


def _log(message: str) -> None:
    try:
        print(f"van-compute-worker: {message}", file=sys.stderr, flush=True)
    except Exception:
        pass


class LeaseAudit:
    def __init__(
        self,
        remote,
        interval: float,
        *,
        stop_event: threading.Event | None = None,
        transport_errors: tuple[type[BaseException], ...] = (),
        clock: Callable[[], float] = time.monotonic,
        log: Callable[[str], None] = _log,
    ) -> None:
        self.remote = remote
        self.interval = interval
        self.stop_event = stop_event or threading.Event()
        self.transport_errors = transport_errors
        self.clock = clock
        self.log = log
        self._runs: dict[str, ActiveRun] = {}
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._closed = threading.Event()
        self._unsupported_until: float | None = None

    # -- runs -----------------------------------------------------------------

    def wrap(self, job_runner):
        def run(args, remote, manifest, stop_event=None):
            active = self._register(remote.worker, manifest)
            try:
                return job_runner(args, remote, manifest, RunStop(stop_event, active.stop))
            except Exception as exc:
                if active.stop.is_set():
                    raise LeaseLost(
                        f"job {active.job_id} attempt {active.attempt} is no longer "
                        f"leased to {active.identity}; stopped without publishing"
                    ) from exc
                raise
            finally:
                self._unregister(active)

        return run

    def _register(self, identity: str, manifest: Mapping[str, object]) -> ActiveRun:
        active = ActiveRun(identity, str(manifest.get("id", "")), manifest.get("attempt"))
        with self._lock:
            self._runs[identity] = active
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._loop, name="van-compute-lease-audit", daemon=True
                )
                self._thread.start()
        return active

    def _unregister(self, active: ActiveRun) -> None:
        with self._lock:
            if self._runs.get(active.identity) is active:
                del self._runs[active.identity]

    def snapshot(self) -> list[ActiveRun]:
        with self._lock:
            return list(self._runs.values())

    # -- audit ------------------------------------------------------------------

    def _loop(self) -> None:
        while not self._closed.wait(self.interval):
            if self.stop_event.is_set():
                return
            self.audit_once()

    def close(self) -> None:
        self._closed.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join()

    def _fetch(self) -> Mapping[str, object] | None:
        try:
            reply = self.remote.json_command(
                "worker", "leases", "--worker", self.remote.worker,
                "--protocol-version", str(protocol.WORKER_PROTOCOL_VERSION),
            )
        except self.transport_errors:
            return None  # unreachable this round; the next one retries
        except Exception as exc:
            # An older Pi rejects the unknown verb.  Heartbeats and claims are
            # untouched; try again after a long pause, in case it was upgraded.
            self._unsupported_until = self.clock() + UNSUPPORTED_BACKOFF_SECONDS
            self.log(f"lease audit unavailable for {UNSUPPORTED_BACKOFF_SECONDS:.0f} s: {exc}")
            return None
        if reply.get("complete") is not True or not isinstance(reply.get("leases"), list):
            return None
        return reply

    def audit_once(self) -> list[ActiveRun]:
        """One audit round; returns the runs it stopped."""
        if self._unsupported_until is not None and self.clock() < self._unsupported_until:
            return []
        runs = self.snapshot()
        if not runs:
            return []
        reply = self._fetch()
        if reply is None:
            return []
        held = set()
        cancelled = set()
        for lease in reply["leases"]:
            if isinstance(lease, Mapping):
                key = (lease.get("worker"), lease.get("job"), lease.get("attempt"))
                held.add(key)
                if lease.get("cancel_requested") is True:
                    cancelled.add(key)
        stopped = []
        current = {id(run) for run in self.snapshot()}
        for run in runs:
            key = (run.identity, run.job_id, run.attempt)
            if id(run) in current and (key not in held or key in cancelled):
                run.stop.set()
                stopped.append(run)
                self.log(
                    f"[{run.identity}] job {run.job_id} attempt {run.attempt} is no "
                    "longer leased here (cancelled, requeued or finished elsewhere); stopping it"
                )
        return stopped
