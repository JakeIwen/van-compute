"""The previous broker instance's processes still in the service's control group.

systemd's `KillMode=control-group` SIGKILLs a dead broker's whole cgroup before
`Restart=always` starts the next one, but a process in uninterruptible sleep
survives that: systemd logs "Processes still around after SIGKILL" and starts
the unit anyway.  So the broker snapshots its own cgroup once at start, before
it launches anything: every member other than itself is a previous instance's
leftover.  A survivor is identified by pid and start time (field 22 of
`/proc/<pid>/stat`), so a reused pid is not mistaken for it.

`status()` gives one of three answers.  GONE is positive evidence: the cgroup
was readable at start and no survivor remains.  PRESENT names the survivors
still alive.  UNCONTAINED means there is no service cgroup to read (not Linux,
no cgroup v2, an unreadable cgroup, or a foreground run outside a `.service`
unit): nothing contains the previous run, the documented foreground limit.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import signal
import sys
from typing import Callable, Mapping, TextIO

from van_compute.job_cleanup_marker import PROC_ROOT, read_group_start

GONE = "gone"
PRESENT = "present"
UNCONTAINED = "uncontained"

SELF_CGROUP = Path("/proc/self/cgroup")
CGROUP_ROOT = Path("/sys/fs/cgroup")


@dataclass(frozen=True)
class Survivor:
    pid: int
    # None when the process was listed but its stat was unreadable while
    # /proc/<pid> existed: it cannot be identified, so only its absence counts.
    start: str | None


def kill_survivor(survivor: Survivor, proc_root: Path = PROC_ROOT) -> None:
    """SIGKILL a survivor only if it is provably the same process.

    The pidfd pins one process, so the start-time check after opening it
    cannot be fooled by a pid reused in between.  Without pidfds (or with no
    recorded start time) identity is not certain, so nothing is signalled and
    the caller only waits."""
    if survivor.start is None or not hasattr(os, "pidfd_open"):
        return
    try:
        descriptor = os.pidfd_open(survivor.pid)
    except OSError:
        return
    try:
        if read_group_start(survivor.pid, proc_root) == survivor.start:
            signal.pidfd_send_signal(descriptor, signal.SIGKILL)
    except OSError:
        pass
    finally:
        os.close(descriptor)


@dataclass(frozen=True)
class StartupSurvivors:
    survivors: tuple[Survivor, ...]
    # Why there is no containment evidence; None when the cgroup was read.
    uncontained_reason: str | None = None
    proc_root: Path = PROC_ROOT
    read_start: Callable[[int, Path], str | None] = read_group_start
    kill: Callable[[Survivor, Path], None] = kill_survivor

    def _alive(self, survivor: Survivor) -> bool:
        if survivor.start is None:
            return (self.proc_root / str(survivor.pid)).exists()
        return self.read_start(survivor.pid, self.proc_root) == survivor.start

    def status(self) -> tuple[str, tuple[int, ...]]:
        """(GONE, PRESENT or UNCONTAINED; the pids still alive)."""
        if self.uncontained_reason is not None:
            return UNCONTAINED, ()
        alive = tuple(item.pid for item in self.survivors if self._alive(item))
        return (PRESENT, alive) if alive else (GONE, ())

    def kill_remaining(self) -> None:
        """SIGKILL, as a cleanup hold does to its old group.  systemd already
        sent one; repeating it is harmless and covers a process that escaped
        the unit's stop."""
        for survivor in self.survivors:
            try:
                self.kill(survivor, self.proc_root)
            except Exception:  # never let a signal fault end recovery
                pass


def _service_cgroup(self_cgroup: Path) -> str | None:
    """The cgroup v2 path of a `.service` unit this process runs in."""
    try:
        lines = self_cgroup.read_text(encoding="utf-8").splitlines()
    except (OSError, ValueError):
        return None
    for line in lines:
        hierarchy, _, path = line.partition("::")
        if hierarchy == "0" and path.startswith("/") and path.endswith(".service"):
            return path
    return None


def capture(
    *,
    self_cgroup: Path = SELF_CGROUP,
    cgroup_root: Path = CGROUP_ROOT,
    proc_root: Path = PROC_ROOT,
    self_pid: int | None = None,
) -> StartupSurvivors:
    """Snapshot the broker's own service cgroup; call once, before any launch."""
    path = _service_cgroup(self_cgroup)
    if path is None:
        return StartupSurvivors((), "not in a cgroup v2 .service unit", proc_root)
    procs = cgroup_root / path.lstrip("/") / "cgroup.procs"
    try:
        listed = procs.read_text(encoding="ascii").split()
    except (OSError, ValueError):
        return StartupSurvivors((), f"cannot read {procs}", proc_root)
    me = os.getpid() if self_pid is None else self_pid
    survivors = []
    for text in listed:
        if not text.isdigit() or int(text) == me:
            continue
        pid = int(text)
        start = read_group_start(pid, proc_root)
        if start is None and not (proc_root / text).exists():
            continue  # exited between the listing and the read
        survivors.append(Survivor(pid, start))
    return StartupSurvivors(tuple(survivors), None, proc_root)


def capture_reported(environ: Mapping[str, str] | None = None, stream: TextIO | None = None) -> StartupSurvivors:
    """`capture`, and inside a systemd service one journal line saying which
    containment evidence this broker has.  An unreadable cgroup otherwise
    falls back to `uncontained` silently, and restart recovery would finalize
    without evidence with nothing in the journal to show it."""
    snapshot = capture()
    if (os.environ if environ is None else environ).get("INVOCATION_ID"):
        status, alive = snapshot.status()
        line = {"action": "cgroup-survivors", "status": status, "survivors": list(alive)}
        if snapshot.uncontained_reason is not None:
            line["reason"] = snapshot.uncontained_reason
        try:
            print(json.dumps(line, sort_keys=True), file=sys.stderr if stream is None else stream, flush=True)
        except Exception:  # a broken journal pipe must not stop the broker starting
            pass
    return snapshot
