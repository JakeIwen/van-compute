"""Record in a running job's manifest that its process group was not confirmed gone.

A restarted broker must not re-execute such a job: its previous attempt's
descendants may have been alive.  The marker is written and cleared under the
queue lock with the lease token checked exactly, like a requeue.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from van_compute import job_requeue, queue

MARKER = "cleanup_unconfirmed"
REASON = "cleanup-unconfirmed"


def mark_cleanup_unconfirmed(
    root: Path, job_id: str, lease_token: str, *, worker: str, now: dt.datetime
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
        manifest.pop(MARKER, None)
        history = manifest.get("attempt_history", [])
        if isinstance(history, list):
            for index in range(len(history) - 1, -1, -1):
                entry = history[index]
                if isinstance(entry, dict) and entry.get("reason") == REASON:
                    del history[index]
                    break
        queue.atomic_json(job_dir / "manifest.json", manifest)
