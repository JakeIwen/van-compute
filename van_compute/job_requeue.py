"""Return a running job to the queue with its attempt recorded."""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
import secrets
import shutil

from van_compute import job_cancel, job_states, queue

OWNERSHIP_FIELDS = (
    "worker",
    "lease_token",
    "placement",
    "placement_reason",
    "started_at",
    "resumed_at",
)
ATTEMPT_HISTORY_LIMIT = 20


def requeue_running_job(
    root: Path,
    job_dir: Path,
    manifest: dict[str, object],
    *,
    reason: str,
    now: dt.datetime,
    error_type: type[RuntimeError] = RuntimeError,
) -> Path:
    """Move `running/<id>` back to `queued/` and publish its ownerless manifest.

    A job with a cancel request goes to `cancelled/` instead (the returned path
    says which); its attempt is still recorded in the history.

    The caller holds `queue.queue_lock(root)`; the lock is not reentrant, so
    this never takes it.  The caller also decided that this attempt may be
    abandoned (stale remote lease, CAN stop) and verified ownership.
    """
    stamp = now.replace(microsecond=0).isoformat()
    history = manifest.get("attempt_history", [])
    if not isinstance(history, list):
        history = []
    history.append(
        {
            "attempt": manifest.get("attempt"),
            "worker": manifest.get("worker"),
            "started_at": manifest.get("started_at"),
            "abandoned_at": stamp,
            "reason": reason,
        }
    )
    # The abandoned attempt's partial uploads are never published.
    result_root = job_dir / "result"
    if result_root.is_symlink():
        raise error_type(f"job result directory is a symlink: {job_dir.name}")
    if result_root.exists():
        shutil.rmtree(result_root)
    result_root.mkdir(mode=0o700)
    if job_cancel.is_cancel_requested(manifest):
        # A requested cancel is never requeued, whoever abandons the attempt.
        manifest["attempt_history"] = history[-ATTEMPT_HISTORY_LIMIT:]
        job_cancel.finalize_cancelled(root, job_dir, job_states.RUNNING, manifest)
        return root / job_states.CANCELLED / job_dir.name
    manifest.update(
        {
            "state": "queued",
            "attempt_history": history[-ATTEMPT_HISTORY_LIMIT:],
            "requeued_at": stamp,
            "requeue_reason": reason,
        }
    )
    for field in OWNERSHIP_FIELDS:
        manifest.pop(field, None)
    destination = root / "queued" / job_dir.name
    # Move first, then publish the requeued manifest.  A crash between
    # these operations leaves a queued directory with the superseded
    # lease, which the next claimant overwrites before moving it back
    # to running.  Publishing an ownerless manifest in running first
    # would strand the job permanently after a crash.
    os.replace(job_dir, destination)
    queue.atomic_json(destination / "manifest.json", manifest)
    return destination


REQUEUED = "requeued"
CANCELLED = "cancelled"
OWNERSHIP_LOST = "ownership-lost"
MAINTENANCE = "queue-maintenance"


def owned_running_job(
    root: Path, job_id: str, lease_token: str, worker: str
) -> tuple[Path, dict[str, object]] | None:
    """The running job and manifest when this worker and exact lease own it.

    The caller holds the queue lock.  `require_running_job` still accepts a
    tokenless legacy manifest, which other callers rely on; changing a job's
    state must prove this attempt's lease, so the stored token has to be
    present, well formed and equal.
    """
    try:
        job_dir, manifest = queue.require_running_job(
            root, job_id, worker, lease_token=lease_token
        )
    except queue.QueueError:
        return None
    stored = manifest.get("lease_token")
    if (
        not isinstance(stored, str)
        or not queue.LEASE_TOKEN_RE.fullmatch(stored)
        or not secrets.compare_digest(stored, lease_token)
    ):
        return None
    return job_dir, manifest


def requeue_owned_job(
    root: Path,
    job_id: str,
    lease_token: str,
    *,
    worker: str,
    reason: str,
    now: dt.datetime,
    error_type: type[RuntimeError] = RuntimeError,
) -> str:
    """Requeue one attempt of ours; touch nothing if it is no longer ours.

    The manifest is re-read under the queue lock and must still name this
    worker and this attempt's lease token.  During queue maintenance the job is
    left running (a restarted broker resumes it) rather than moved under it.
    """
    with queue.queue_lock(root):
        if queue.maintenance_status(root) is not None:
            return MAINTENANCE
        owned = owned_running_job(root, job_id, lease_token, worker)
        if owned is None:
            return OWNERSHIP_LOST
        job_dir, manifest = owned
        destination = requeue_running_job(
            root, job_dir, manifest, reason=reason, now=now, error_type=error_type
        )
    return CANCELLED if destination.parent.name == job_states.CANCELLED else REQUEUED

