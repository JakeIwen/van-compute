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
MARKER = "cleanup_unconfirmed"
REASON = "cleanup-unconfirmed"


def read_boot_id(path: Path = BOOT_ID_PATH) -> str | None:
    """The kernel boot id, or None where there is none (macOS)."""
    try:
        return path.read_text(encoding="ascii").strip() or None
    except (OSError, ValueError):
        return None


def mark_cleanup_unconfirmed(
    root: Path,
    job_id: str,
    lease_token: str,
    *,
    worker: str,
    now: dt.datetime,
    group: int,
    boot_id: str | None,
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
        for field in (MARKER, "cleanup_group", "cleanup_boot_id"):
            manifest.pop(field, None)
        history = manifest.get("attempt_history", [])
        if isinstance(history, list):
            for index in range(len(history) - 1, -1, -1):
                entry = history[index]
                if isinstance(entry, dict) and entry.get("reason") == REASON:
                    del history[index]
                    break
        queue.atomic_json(job_dir / "manifest.json", manifest)
