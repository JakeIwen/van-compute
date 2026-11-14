"""Detect CAN transmitters from the kernel's file-lock table.

Every Pi CAN transmitter in obd-things takes an exclusive lock on an
`active-diagnostics-*.lock` file for as long as it may transmit.  Reading
`/proc/locks` and matching inodes lets the broker see those locks without ever
opening, creating or locking the files: a probing shared lock could make a
sender's nonblocking `LOCK_EX` fail.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
from typing import Callable

from van_compute.config import DEFAULT_CAN_LOCK_DIR

LOCK_FILE_PREFIX = "active-diagnostics-"
LOCK_FILE_SUFFIX = ".lock"
PROC_LOCKS = Path("/proc/locks")
LOCK_TYPES = frozenset({"FLOCK", "POSIX", "OFDLCK"})
# A chain of quiet observations is trusted only while consecutive ones are
# this close; a longer silence says nothing about what happened in between.
MAX_OBSERVATION_GAP_SECONDS = 15.0
_ID_RE = re.compile(r"\d+:")
_DEVICE_INODE_RE = re.compile(r"[^:\s]+:[^:\s]+:[^:\s]+")

LockIdentity = tuple[int, int, int]


class ProcLocksError(ValueError):
    """A /proc/locks record about one of the lock files could not be read."""


@dataclass(frozen=True)
class CanProbe:
    active: bool
    held_locks: tuple[str, ...]
    error: str | None

    @property
    def quiet(self) -> bool:
        return self.error is None and not self.active


def lock_identities(lock_dir: Path) -> dict[LockIdentity, str]:
    """Re-list and re-stat the lock files; identities are never cached."""
    identities: dict[LockIdentity, str] = {}
    with os.scandir(lock_dir) as entries:
        for entry in entries:
            name = entry.name
            if not (
                name.startswith(LOCK_FILE_PREFIX) and name.endswith(LOCK_FILE_SUFFIX)
            ):
                continue
            info = os.stat(entry.path)
            identities[
                (os.major(info.st_dev), os.minor(info.st_dev), info.st_ino)
            ] = name
    return identities


def _record_identity(tokens: list[str], inodes: frozenset[int]) -> LockIdentity | None:
    """The record's (major, minor, inode) when it concerns one of our inodes."""
    for token in tokens:
        if not _DEVICE_INODE_RE.fullmatch(token):
            continue
        major_text, minor_text, inode_text = token.split(":")
        if not inode_text.isdigit() or int(inode_text) not in inodes:
            return None
        try:
            return int(major_text, 16), int(minor_text, 16), int(inode_text)
        except ValueError:
            raise ProcLocksError(f"unreadable device in lock record: {token!r}")
    return None


def write_lock_name(
    line: str, identities: dict[LockIdentity, str], inodes: frozenset[int]
) -> str | None:
    """The lock file name a WRITE record holds or waits on, else None.

    Unrelated records are ignored even when malformed; a record about one of
    our inodes that cannot be understood raises, so the caller fails closed.
    """
    tokens = line.split()
    identity = _record_identity(tokens, inodes)
    if identity is None or identity not in identities:
        return None
    rest = tokens[1:] if tokens and _ID_RE.fullmatch(tokens[0]) else tokens
    if rest and rest[0] == "->":
        rest = rest[1:]
    if len(rest) < 7 or rest[0] not in LOCK_TYPES or rest[2] not in ("READ", "WRITE"):
        raise ProcLocksError(f"unreadable lock record: {line.strip()!r}")
    try:
        int(rest[3])  # an OFD lock's owner pid is -1
    except ValueError:
        raise ProcLocksError(f"unreadable lock owner: {line.strip()!r}") from None
    return identities[identity] if rest[2] == "WRITE" else None


def probe_can_activity(
    lock_dir: Path = DEFAULT_CAN_LOCK_DIR, proc_locks: Path = PROC_LOCKS
) -> CanProbe:
    """Any WRITE lock, held or awaited, on a lock file means CAN transmit.

    Every failure (missing directory, list or stat error, unreadable
    /proc/locks, a garbled relevant record) is reported as an error, never as
    "no transmitters".
    """
    try:
        identities = lock_identities(lock_dir)
        lines = proc_locks.read_text(encoding="ascii").splitlines()
        inodes = frozenset(identity[2] for identity in identities)
        held = {
            name
            for line in lines
            if (name := write_lock_name(line, identities, inodes)) is not None
        }
    except (OSError, ValueError) as exc:
        return CanProbe(False, (), f"CAN lock probe failed: {exc}")
    return CanProbe(bool(held), tuple(sorted(held)), None)


class CanQuietTracker:
    """How long CAN has been continuously quiet; one per broker lifetime."""

    def __init__(self) -> None:
        self._quiet_since: float | None = None
        self._last_quiet: float | None = None

    def observe(self, now: float, *, quiet: bool) -> None:
        """Record one observation; active and failed probes are both not quiet."""
        if not quiet:
            self._quiet_since = self._last_quiet = None
            return
        if (
            self._last_quiet is None
            or now - self._last_quiet > MAX_OBSERVATION_GAP_SECONDS
        ):
            self._quiet_since = now
        self._last_quiet = now

    def quiet_seconds(self, now: float) -> float:
        if self._quiet_since is None or self._last_quiet is None:
            return 0.0
        if now - self._last_quiet > MAX_OBSERVATION_GAP_SECONDS:
            return 0.0
        return max(0.0, now - self._quiet_since)


CanObserver = Callable[[], CanProbe]
