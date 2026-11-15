"""Record in a running job's manifest that its process group was not confirmed gone.

A restarted broker must not re-execute such a job: its previous attempt's
descendants may have been alive.  The marker is written and cleared under the
queue lock with the lease token checked exactly, like a requeue.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from van_compute import job_requeue, queue

BOOT_ID_PATH = Path("/proc/sys/kernel/random/boot_id")
PROC_ROOT = Path("/proc")
MARKER = "cleanup_unconfirmed"
GROUP_START_FIELD = "cleanup_group_start"
REASON = "cleanup-unconfirmed"


def read_boot_id(path: Path = BOOT_ID_PATH) -> str | None:
    """The kernel boot id, or None where there is none (macOS)."""
    try:
        return path.read_text(encoding="ascii").strip() or None
    except (OSError, ValueError):
        return None


def read_group_start(group: int, proc_root: Path = PROC_ROOT) -> str | None:
    """Start time (clock ticks since boot) of the process whose pid is `group`.

    Field 22 of /proc/<pid>/stat.  The command name (field 2) is parenthesised
    and may contain spaces and parentheses, so fields are counted after the
    last ')'.  None when it cannot be read: no such process (the leader has
    exited, so only other members can keep the group alive) or no /proc.
    """
    try:
        text = (proc_root / str(group) / "stat").read_text(encoding="ascii")
    except (OSError, ValueError):
        return None
    _, separator, rest = text.rpartition(")")
    fields = rest.split()
    # `rest` starts at field 3 (state), so field 22 is index 19.
    if not separator or len(fields) < 20 or not fields[19].isdigit():
        return None
    return fields[19]


def mark_cleanup_unconfirmed(
    root: Path,
    job_id: str,
    lease_token: str,
    *,
    worker: str,
    now: dt.datetime,
    group: int,
    boot_id: str | None,
    group_start: str | None = None,
) -> None:
    stamp = now.replace(microsecond=0).isoformat()
    with queue.queue_lock(root):
        owned = job_requeue.owned_running_job(root, job_id, lease_token, worker)
        if owned is None:
            raise queue.QueueError("job is no longer owned by this attempt")
        job_dir, manifest = owned
        history = manifest.get("attempt_history", [])
        if not isinstance(history, list):
            history = []
        history.append(
            {
                "attempt": manifest.get("attempt"),
                "worker": manifest.get("worker"),
                "started_at": manifest.get("started_at"),
                "abandoned_at": stamp,
                "reason": REASON,
            }
        )
        manifest[MARKER] = stamp
        # Lets a restarted broker find the same group again, on the same boot.
        manifest["cleanup_group"] = group
        manifest["cleanup_boot_id"] = boot_id
        # Identifies the leader, so a pgid reused on this boot is not mistaken
        # for the old group.  Absent when the leader was unreadable.
        manifest[GROUP_START_FIELD] = group_start
        manifest["attempt_history"] = history[-job_requeue.ATTEMPT_HISTORY_LIMIT:]
        queue.atomic_json(job_dir / "manifest.json", manifest)


def clear_cleanup_unconfirmed(
    root: Path, job_id: str, lease_token: str, *, worker: str
) -> None:
    with queue.queue_lock(root):
        owned = job_requeue.owned_running_job(root, job_id, lease_token, worker)
        if owned is None:
            raise queue.QueueError("job is no longer owned by this attempt")
        job_dir, manifest = owned
        for field in (MARKER, "cleanup_group", "cleanup_boot_id", GROUP_START_FIELD):
            manifest.pop(field, None)
        history = manifest.get("attempt_history", [])
        if isinstance(history, list):
            for index in range(len(history) - 1, -1, -1):
                entry = history[index]
                if isinstance(entry, dict) and entry.get("reason") == REASON:
                    del history[index]
                    break
        queue.atomic_json(job_dir / "manifest.json", manifest)
