"""Hold a Pi job's cache scope and work directory until its process group is gone.

Descendants of a failed or stopped job may still be writing into the work
directory and holding the shared task cache.  Unwinding while they might be
alive would release the cache lock and delete the directory under them, so a
cleanup that cannot confirm the group gone keeps killing and checking until it
is confirmed, or until the broker itself is told to stop.  There is no
timeout-based release.

A stop exit does not prove the group is gone: systemd only logs "Processes
still around after SIGKILL" for uninterruptible (D-state) descendants and may
restart the unit.  So the hold first persists a marker (group id and boot id)
in the running manifest, and the next broker run holds the same group again,
then discards the job's cache entry, before failing the job.

Nothing here may block or raise into supervision: the first attempt polls
without blocking waits, and every log call is best-effort.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import json
import os
import signal
import sys
import time
from typing import Callable, Iterator

from van_compute.engine_process import wait4_nohang

FIRST_ATTEMPT_SECONDS = 1.0
FIRST_ATTEMPT_POLL_SECONDS = 0.05
HOLD_INTERVAL_SECONDS = 1.0
HOLD_LOG_INTERVAL_SECONDS = 60.0


def kill_group(group: int) -> None:
    os.killpg(group, signal.SIGKILL)


def print_json_line(payload: dict[str, object]) -> None:
    print(json.dumps(payload, sort_keys=True), flush=True)


def log_best_effort(payload: dict[str, object]) -> None:
    """For callers outside a hold (serve's stop line); never raises."""
    try:
        print_json_line(payload)
    except Exception:
        pass


@contextmanager
def stop_flag_signals() -> Iterator[Callable[[], bool]]:
    """SIGINT/SIGTERM set a flag instead of unwinding; handlers are restored.

    `serve()` has its own handlers; this gives foreground `--once` the same
    stop semantics.  A foreground run has no cgroup containment: after a stop
    during a hold, the survivors are the next broker run's to hold or discard.
    """
    stopping = False

    def handler(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    previous = {
        number: signal.signal(number, handler)
        for number in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        yield lambda: stopping
    finally:
        for number, old in previous.items():
            signal.signal(number, old)


@dataclass
class CleanupHold:
    job: str
    group_exists: Callable[[int], bool]
    service_stopping: Callable[[], bool]
    mark: Callable[[int], None]
    unmark: Callable[[], None]
    unconfirmed_error: type[RuntimeError]
    kill: Callable[[int], None] = kill_group
    reap: Callable[[object], None] = wait4_nohang
    log: Callable[[dict[str, object]], None] = print_json_line
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic

    # -- helpers --------------------------------------------------------------

    def _log(self, **fields: object) -> None:
        try:
            self.log({"action": "cleanup-hold", "job": self.job, **fields})
        except Exception:  # a broken pipe must never end the hold
            pass

    def _try(self, step: Callable[[], object]) -> bool:
        try:
            step()
            return True
        except Exception:
            return False

    def _gone(self, group: int) -> bool:
        try:
            return not self.group_exists(group)
        except Exception:  # an inspection error counts as still there
            return False

    def _stopped(self) -> bool:
        try:
            return bool(self.service_stopping())
        except Exception:
            return True

    def _raise_unconfirmed(self, group: int | None, why: str):
        label = "" if group is None else f" {group}"
        raise self.unconfirmed_error(
            f"stopped while {why} (process group{label}); the next broker run "
            "will hold or discard it"
        )

    # -- a job's own child ------------------------------------------------------

    def clean_up(self, process) -> None:
        """Return once the process group is confirmed gone (and any marker
        cleared); raise `unconfirmed_error` when the broker stops first.

        Never makes a blocking call: a leader in uninterruptible sleep would
        otherwise stall before the marker or any stop check.
        """
        group = process.pid
        started = self.clock()
        reason = "process group still has members after SIGKILL"
        while True:
            for step in (lambda: self.kill(group), lambda: self.reap(process)):
                self._try(step)
            if self._gone(group):
                return
            if self.clock() - started >= FIRST_ATTEMPT_SECONDS:
                break
            self.sleep(FIRST_ATTEMPT_POLL_SECONDS)
        self._hold(group, reason, process=process)

    def _hold(self, group: int, reason: str, *, process) -> None:
        announce = {"group": group, "reason": reason}
        self._log(**announce)
        persisted = False
        last_log = self.clock()
        while True:
            # Retried every iteration: one failed write must not leave a stop
            # with no marker.
            if not persisted:
                persisted = self._try(lambda: self.mark(group))
            self._try(lambda: self.kill(group))
            if process is not None:
                self._try(lambda: self.reap(process))
            if self._gone(group):
                # Continue into fail/requeue only with the marker cleared.
                if not persisted or self._try(self.unmark):
                    return
            if self._stopped():
                if not persisted and not self._try(lambda: self.mark(group)):
                    self._log(reason="could not write the cleanup marker", group=group)
                self._raise_unconfirmed(group, f"cleanup of job {self.job} was unconfirmed")
            if self.clock() - last_log >= HOLD_LOG_INTERVAL_SECONDS:
                self._log(**announce)
                last_log = self.clock()
            self.sleep(HOLD_INTERVAL_SECONDS)

    # -- a previous broker run's group ------------------------------------------

    def hold_previous_group(
        self,
        manifest: dict[str, object],
        read_boot_id: Callable[[], str | None],
    ) -> None:
        """Hold the group a marked job left behind, if this is the same boot and
        it still has members.  A missing or different boot id means a reboot
        ended it."""
        group = manifest.get("cleanup_group")
        recorded = manifest.get("cleanup_boot_id")
        current = read_boot_id()
        if (
            isinstance(group, bool) or not isinstance(group, int)
            or not recorded or recorded != current
        ):
            return
        if self._gone(group):
            return
        # Marker already persisted: nothing to write or clear.
        self.mark = lambda _group: None
        self.unmark = lambda: None
        self._hold(group, "previous broker run's cleanup was unconfirmed", process=None)

    def until_done(self, what: str, action: Callable[[], bool]) -> None:
        """Retry `action` (True when finished) once a second until it works or
        the broker stops, so the job is not failed with the step undone."""
        while not (self._try_bool(action)):
            if self._stopped():
                self._raise_unconfirmed(None, f"{what} was not finished")
            self.sleep(HOLD_INTERVAL_SECONDS)

    def _try_bool(self, action: Callable[[], bool]) -> bool:
        try:
            return bool(action())
        except Exception:
            return False
