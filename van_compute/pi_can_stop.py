"""Stop a running Pi-local job when a CAN transmitter appears, and requeue it.

The start gate keeps jobs from beginning while CAN is active; this covers a
transmitter that takes its lock after the job began.  The first stop cause wins
and decides what happens to the job: a service stop fails it as before, a CAN
cause terminates it fast and puts it back in the queue untouched.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
import shutil
import time
from typing import Callable

from van_compute import job_requeue
from van_compute.can_activity import CanObserver
from van_compute.engine_process import DEFAULT_TERMINATE_GRACE
from van_compute.pi_health_monitor import CAN_WATCH_INTERVAL_SECONDS

SERVICE_STOP = "service-stop"
CAN_TRANSMIT = "pi-can-transmit"
CAN_PROBE_FAILED = "can-probe-failed"
CAN_CAUSES = frozenset({CAN_TRANSMIT, CAN_PROBE_FAILED})
# SIGTERM to SIGKILL for a CAN stop: with the 0.5 s watch this keeps the job
# alive about 1.5 s after a lock appears, instead of the usual 10 s courtesy.
CAN_TERMINATE_GRACE = 1.0
NORMAL_GROUP_GRACE = 2.0


class CanStopped(Exception):
    """Unwinds a job's execution to its requeue; never a failure."""


class StopLatch:
    """Combine the service stop with a rate-limited CAN check; latch the first cause."""

    def __init__(
        self,
        service_stop: Callable[[], bool],
        observe_can: CanObserver | None,
        *,
        interval: float = CAN_WATCH_INTERVAL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._service_stop = service_stop
        self._observe_can = observe_can
        self._interval = interval
        self._clock = clock
        self._last_check: float | None = None
        self.cause: str | None = None
        self.detail: str | None = None

    @property
    def can_cause(self) -> bool:
        return self.cause in CAN_CAUSES

    def _latch(self, cause: str, detail: str | None = None) -> None:
        if self.cause is None:
            self.cause, self.detail = cause, detail

    def _check_can(self) -> None:
        """Probe CAN now; nothing the probe does may escape into the wait loop."""
        self._last_check = self._clock()
        if self._observe_can is None:
            return
        try:
            probe = self._observe_can()
        except Exception as exc:  # fail closed on any probe fault
            self._latch(CAN_PROBE_FAILED, f"{type(exc).__name__}: {exc}")
            return
        if probe.error is not None:
            self._latch(CAN_PROBE_FAILED, probe.error)
        elif probe.active:
            self._latch(CAN_TRANSMIT, ", ".join(probe.held_locks))

    def clear_to_launch(self) -> bool:
        """Check CAN right now, before a child is started (also called once the
        task cache is held, immediately before launch)."""
        if self.cause is None:
            self._check_can()
        return not self.can_cause

    def should_stop(self) -> bool:
        if self.cause is not None:
            return True
        due = (
            self._last_check is None
            or self._clock() - self._last_check >= self._interval
        )
        # CAN is checked before the service flag so that a tie is resolved
        # toward the faster kill.
        if due:
            self._check_can()
        if self.cause is None and self._service_stop():
            self._latch(SERVICE_STOP)
        return self.cause is not None

    def raise_if_can_stopped(self) -> None:
        if self.can_cause:
            raise CanStopped(self.cause)

    def terminate_grace(self) -> float:
        return CAN_TERMINATE_GRACE if self.can_cause else DEFAULT_TERMINATE_GRACE

    def group_grace(self) -> float:
        """SIGTERM wait for descendants left after the child itself has exited."""
        return 0.0 if self.can_cause else NORMAL_GROUP_GRACE


def requeue_after_can_stop(
    root: Path,
    job_id: str,
    lease_token: str,
    *,
    worker: str,
    cause: str,
    work: Path | None,
    error_type: type[RuntimeError],
) -> dict[str, object]:
    """Discard local work, then return the job to the queue if it is still ours.

    Nothing is validated, packaged, uploaded or recorded as telemetry, and
    `worker_finish` is never called: the attempt simply did not happen.
    """
    if work is not None:
        shutil.rmtree(work, ignore_errors=True)
    outcome = job_requeue.requeue_owned_job(
        root,
        job_id,
        lease_token,
        worker=worker,
        reason=cause,
        now=dt.datetime.now(dt.timezone.utc),
        error_type=error_type,
    )
    if outcome == job_requeue.REQUEUED:
        return {"action": "requeued", "reason": cause, "job_id": job_id}
    # Not ours any more (or the queue is under maintenance): leave it alone.
    return {
        "action": "deferred",
        "reason": f"CAN stop ({cause}) left job untouched: {outcome}",
        "job_id": job_id,
    }
