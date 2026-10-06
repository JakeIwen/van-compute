"""Child supervision shared by the Pi and Mac, with explicit host differences."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import os
from pathlib import Path
import signal
import subprocess
import time
from typing import Callable


class Host(Enum):
    PI = "pi"
    MAC = "mac"


@dataclass(frozen=True)
class ProcessOutcome:
    exit_code: int
    usage: object
    timed_out: bool
    interrupted: bool
    resource_limit: str | None
    resource_monitor_error: str | None
    minimum_filesystem_free_bytes: int | None
    peak_process_group_rss_bytes: int = 0
    peak_process_count: int = 0


@dataclass
class _ProcessMonitor:
    host: Host
    error_type: type[RuntimeError]
    work_path: Path | None
    minimum_free_bytes: int
    free_space_reader: Callable[[Path], int]
    maximum_memory: int
    maximum_processes: int
    resource_reader: Callable[[int], tuple[int, int]] | None
    resource_limit: str | None = None
    resource_monitor_error: str | None = None
    lowest_free_bytes: int | None = None
    peak_group_rss: int = 0
    peak_processes: int = 0

    def sample_free_space(self):
        if self.work_path is None:
            return
        try:
            free_bytes = self.free_space_reader(self.work_path)
        except OSError as exc:
            self.resource_monitor_error = f"free-space watchdog failed: {exc}"
            return
        self.lowest_free_bytes = (
            free_bytes
            if self.lowest_free_bytes is None
            else min(self.lowest_free_bytes, free_bytes)
        )
        if free_bytes < self.minimum_free_bytes:
            label = "execution safety threshold " if self.host is Host.PI else ""
            self.resource_limit = (
                f"filesystem free space fell below {label}{self.minimum_free_bytes} bytes"
            )

    def sample_process_group(self, process):
        assert self.resource_reader is not None
        try:
            group_rss, process_count = self.resource_reader(process.pid)
        except (OSError, subprocess.SubprocessError, self.error_type) as exc:
            self.resource_monitor_error = str(exc)
            return False
        self.peak_group_rss = max(self.peak_group_rss, group_rss)
        self.peak_processes = max(self.peak_processes, process_count)
        if group_rss > self.maximum_memory:
            self.resource_limit = (
                f"process-group RSS exceeded {self.maximum_memory} bytes"
            )
            return False
        if process_count > self.maximum_processes:
            self.resource_limit = f"process count exceeded {self.maximum_processes}"
            return False
        return True

    def outcome(self, code, usage, timed_out=False, interrupted=False):
        return ProcessOutcome(
            code, usage, timed_out, interrupted, self.resource_limit,
            self.resource_monitor_error, self.lowest_free_bytes,
            self.peak_group_rss, self.peak_processes,
        )


def wait4_nohang(process: subprocess.Popen):
    waited_pid, status, usage = os.wait4(process.pid, os.WNOHANG)
    if waited_pid == 0:
        return None
    process.returncode = os.waitstatus_to_exitcode(status)
    return process.returncode, usage


def _finish_wait(process, monitor, timed_out, interrupted, poll, clock):
    limited = (
        (monitor.resource_limit is not None or monitor.resource_monitor_error is not None)
        if monitor.host is Host.PI
        else bool(monitor.resource_limit or monitor.resource_monitor_error)
    )
    if limited:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        waited_pid, status, usage = os.wait4(process.pid, 0)
        if waited_pid != process.pid:
            label = "local analysis" if monitor.host is Host.PI else "analysis"
            raise monitor.error_type(f"lost track of resource-limited {label} process")
        process.returncode = os.waitstatus_to_exitcode(status)
        return monitor.outcome(
            137 if monitor.resource_limit else 125, usage, timed_out, interrupted
        )
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    terminate_deadline = clock.monotonic() + 10
    while clock.monotonic() < terminate_deadline:
        completed = poll(process)
        if completed is not None:
            return monitor.outcome(
                124 if timed_out else 143, completed[1], timed_out, interrupted
            )
        clock.sleep(0.1)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    waited_pid, status, usage = os.wait4(process.pid, 0)
    if waited_pid != process.pid:
        label = "local analysis" if monitor.host is Host.PI else "analysis child"
        raise monitor.error_type(f"lost track of {label} process")
    process.returncode = os.waitstatus_to_exitcode(status)
    return monitor.outcome(124 if timed_out else 143, usage, timed_out, interrupted)


def wait_for_process(
    process: subprocess.Popen,
    *,
    host: Host,
    error_type: type[RuntimeError],
    timeout: int,
    should_stop: Callable[[], bool],
    work_path: Path | None,
    minimum_free_bytes: int,
    free_space_reader: Callable[[Path], int],
    maximum_memory: int = 0,
    maximum_processes: int = 0,
    resource_reader: Callable[[int], tuple[int, int]] | None = None,
    poll: Callable = wait4_nohang,
    clock=time,
) -> ProcessOutcome:
    """Shared wait/reap/escalation; Pi final disk sample and Mac RSS stay distinct."""
    deadline = clock.monotonic() + timeout
    next_resource_poll = 0.0
    timed_out = interrupted = False
    monitor = _ProcessMonitor(
        host, error_type, work_path, minimum_free_bytes, free_space_reader,
        maximum_memory, maximum_processes, resource_reader,
    )
    while True:
        completed = poll(process)
        if completed is not None:
            if host is Host.PI:
                monitor.sample_free_space()
            return monitor.outcome(
                (137 if monitor.resource_limit else
                 125 if monitor.resource_monitor_error else completed[0]),
                completed[1], timed_out, interrupted,
            )
        if should_stop():
            interrupted = True
            break
        now = clock.monotonic()
        if now >= deadline:
            timed_out = True
            break
        if now >= next_resource_poll:
            if host is Host.MAC and not monitor.sample_process_group(process):
                break
            monitor.sample_free_space()
            if (monitor.resource_limit is not None
                    or monitor.resource_monitor_error is not None):
                break
            next_resource_poll = now + (0.5 if host is Host.PI else 1.0)
        clock.sleep(0.1)
    return _finish_wait(process, monitor, timed_out, interrupted, poll, clock)


def process_group_exists(
    group: int, *, host: Host, error_type: type[RuntimeError]
) -> bool:
    try:
        os.killpg(group, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError as exc:
        if host is Host.MAC:
            return True
        raise error_type(
            f"cannot inspect analysis process group {group}: {exc}"
        ) from None


def terminate_group(
    group: int,
    *,
    exists: Callable[[int], bool],
    error_type: type[RuntimeError],
    host: Host,
    terminate_grace: float = 2.0,
    kill_grace: float = 2.0,
    clock=time,
) -> bool:
    if not exists(group):
        return False
    for sig, grace in ((signal.SIGTERM, terminate_grace), (signal.SIGKILL, kill_grace)):
        try:
            os.killpg(group, sig)
        except ProcessLookupError:
            return True
        deadline = clock.monotonic() + grace
        while clock.monotonic() < deadline:
            if not exists(group):
                return True
            clock.sleep(0.05)
    label = f" {group}" if host is Host.PI else ""
    raise error_type(f"analysis process group{label} survived SIGKILL")
