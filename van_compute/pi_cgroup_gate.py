"""Hold the Pi fallback while the broker's service cgroup has other members.

systemd's `KillMode=control-group` SIGKILLs a dead broker's whole cgroup
before `Restart=always` starts the next one, but a process in uninterruptible
sleep survives that: systemd logs "Processes still around after SIGKILL" and
starts the unit anyway.  The broker runs its one local job synchronously
inside a poll and reaps every helper it spawns before returning, so between
polls the only member of its cgroup that belongs to it is itself: any other
member is a previous instance's leftover.  While one is listed the broker
finalizes no unmarked restart cancel, resumes nothing and claims nothing
(marked recovery still runs first: it is how a marked job's old group goes
away).  The evidence is the cgroup's current membership, read on every poll;
no pid is remembered, identified or signalled (systemd has already sent
SIGKILL, and another one does nothing for a process in uninterruptible sleep).

Three answers, failing closed.  CLEAR: the cgroup was read and lists only the
broker.  HELD: it lists others, or the broker is in a service and its cgroup
could not be read or parsed.  UNCONTAINED: not a service (a `0::` path that is
not a `.service` unit, or no /proc/self/cgroup and no systemd INVOCATION_ID):
the documented foreground limit, so the broker proceeds.  Only the serve loop
is gated; `--once`, `--health` and `--self-test` are foreground commands that
may run inside some other unit's cgroup.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import sys
from typing import Mapping, TextIO

CLEAR = "clear"
HELD = "held"
UNCONTAINED = "uncontained"

SELF_CGROUP = Path("/proc/self/cgroup")
CGROUP_ROOT = Path("/sys/fs/cgroup")


@dataclass(frozen=True)
class Membership:
    state: str
    # Members other than the broker itself (pids as listed, never remembered).
    others: tuple[int, ...] = ()
    reason: str | None = None


@dataclass(frozen=True)
class CgroupGate:
    self_cgroup: Path = SELF_CGROUP
    cgroup_root: Path = CGROUP_ROOT
    environ: Mapping[str, str] | None = None
    self_pid: int | None = None

    @property
    def started_by_systemd(self) -> bool:
        return bool((os.environ if self.environ is None else self.environ).get("INVOCATION_ID"))

    def _unknown(self, reason: str) -> Membership:
        """No cgroup could be determined: held inside systemd, else foreground."""
        return Membership(HELD if self.started_by_systemd else UNCONTAINED, (), reason)

    def read(self) -> Membership:
        try:
            lines = self.self_cgroup.read_text(encoding="utf-8").splitlines()
        except FileNotFoundError:
            return self._unknown(f"no {self.self_cgroup}")
        except (OSError, ValueError) as exc:
            return Membership(HELD, (), f"cannot read {self.self_cgroup}: {exc}")
        path = next(
            (rest for hierarchy, _, rest in (line.partition("::") for line in lines)
             if hierarchy == "0" and rest.startswith("/")),
            None,
        )
        if path is None:
            return self._unknown("no cgroup v2 path in /proc/self/cgroup")
        if not path.endswith(".service"):
            return Membership(UNCONTAINED, (), f"{path} is not a .service unit")
        procs = self.cgroup_root / path.lstrip("/") / "cgroup.procs"
        try:
            listed = procs.read_text(encoding="ascii").split()
        except (OSError, ValueError) as exc:
            return Membership(HELD, (), f"cannot read {procs}: {exc}")
        if not all(text.isdigit() for text in listed):
            return Membership(HELD, (), f"cannot parse {procs}")
        me = os.getpid() if self.self_pid is None else self.self_pid
        if me not in {int(text) for text in listed}:
            # Not our own cgroup's listing after all: no evidence either way.
            return Membership(HELD, (), f"{procs} does not list this broker")
        others = tuple(sorted(int(text) for text in listed if int(text) != me))
        return Membership(HELD, others, None) if others else Membership(CLEAR)


def deferral(
    monitor, running: Mapping[str, object] | None, recovered: list[str]
) -> dict[str, object] | None:
    """The poll's deferral while the gate is held, else None.

    A marked running job is never held: its recovery kills and waits out its
    own old group first, and it launches nothing."""
    gate = getattr(monitor, "containment", None)
    if gate is None or (running is not None and running.get("cleanup_unconfirmed")):
        return None
    try:
        membership = gate.read()
    except Exception as exc:  # an inspection fault never lets the broker proceed
        membership = Membership(HELD, (), f"{type(exc).__name__}: {exc}")
    if membership.state != HELD:
        return None
    return {
        "action": "deferred",
        "reason": "a previous broker's processes are still in the service cgroup"
        if membership.reason is None else f"service cgroup unreadable: {membership.reason}",
        "cgroup_members": list(membership.others),
        "recovered_jobs": recovered,
    }


def for_config(config, *, gate: CgroupGate | None = None, stream: TextIO | None = None) -> CgroupGate | None:
    """The serve loop's gate (None for foreground commands), with one journal
    line inside a service saying what containment evidence this start has."""
    if config.once or config.health or config.self_test:
        return None
    gate = CgroupGate() if gate is None else gate
    try:
        membership = gate.read()
        if gate.started_by_systemd or membership.state != UNCONTAINED:
            line = {"action": "cgroup-gate", "state": membership.state,
                    "members": list(membership.others)}
            if membership.reason is not None:
                line["reason"] = membership.reason
            print(json.dumps(line, sort_keys=True), file=sys.stderr if stream is None else stream,
                  flush=True)
    except Exception:  # the line is informational: it must never stop a start
        pass
    return gate
