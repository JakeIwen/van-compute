"""Hold a Pi job's cache scope and work directory until its process group is gone.

Descendants of a failed or stopped job may still be writing into the work
directory and holding the shared task cache.  Unwinding while they might be
alive would release the cache lock and delete the directory under them, so a
cleanup that cannot confirm the group gone keeps killing and checking until it
is confirmed, or until the broker itself is told to stop.  The stop exit
releases the scope on purpose: under systemd, `KillMode=control-group` kills
the whole cgroup, and a restarted broker cannot start until the cgroup is
empty.  There is no timeout-based release.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
import signal
import time
from typing import Callable

from van_compute.engine_process import kill_and_reap_group, wait_for_group_gone

HOLD_INTERVAL_SECONDS = 1.0
HOLD_LOG_INTERVAL_SECONDS = 60.0


def kill_group(group: int) -> None:
    os.killpg(group, signal.SIGKILL)


def reap_without_blocking(pid: int) -> None:
    os.waitpid(pid, os.WNOHANG)


def print_json_line(payload: dict[str, object]) -> None:
    print(json.dumps(payload, sort_keys=True), flush=True)


@dataclass
class CleanupHold:
    job: str
    group_exists: Callable[[int], bool]
    service_stopping: Callable[[], bool]
    mark: Callable[[], None]
    unmark: Callable[[], None]
    unconfirmed_error: type[RuntimeError]
    kill: Callable[[int], None] = kill_group
    reap: Callable[[int], None] = reap_without_blocking
    log: Callable[[dict[str, object]], None] = print_json_line
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic

    def clean_up(self, process) -> None:
        """Return once the process group is confirmed gone, or raise
        `unconfirmed_error` when the broker is stopping first."""
        group = process.pid
        try:
            kill_and_reap_group(process)
            if wait_for_group_gone(group, exists=self.group_exists):
                return
            reason = "process group still has members after SIGKILL"
        except Exception as exc:  # any cleanup failure goes to the hold
            reason = f"{type(exc).__name__}: {exc}"
        self._hold(group, reason)

    def _gone(self, group: int) -> bool:
        try:
            return not self.group_exists(group)
        except Exception:  # an inspection error counts as still there
            return False

    def _record(self, action: str) -> None:
        try:
            (self.mark if action == "mark" else self.unmark)()
        except Exception as exc:  # the hold matters more than its marker
            self.log(
                {"action": "cleanup-hold", "job": self.job,
                 "reason": f"cannot {action} manifest: {type(exc).__name__}: {exc}"}
            )

    def _hold(self, group: int, reason: str) -> None:
        self._record("mark")
        announce = {"action": "cleanup-hold", "job": self.job, "group": group,
                    "reason": reason}
        self.log(announce)
        last_log = self.clock()
        while True:
            for step, argument in ((self.kill, group), (self.reap, group)):
                try:
                    step(argument)
                except Exception:  # ESRCH or no child: keep going
                    pass
            if self._gone(group):
                self._record("unmark")
                return
            if self.service_stopping():
                raise self.unconfirmed_error(
                    f"stopped while process group {group} could not be confirmed gone"
                )
            if self.clock() - last_log >= HOLD_LOG_INTERVAL_SECONDS:
                self.log(announce)
                last_log = self.clock()
            self.sleep(HOLD_INTERVAL_SECONDS)
