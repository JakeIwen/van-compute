"""Cancel a job: the request, the move into `cancelled/`, and crash recovery.

Every step runs under the queue lock.  The sequence is the same wherever a
cancel is carried out (the `cancel` command, a claim, finish or requeue that
meets a request, the broker, `reconcile`):

0. write an intent `.cancel-intents/<id>` naming the directory the job leaves;
a. record `cancel_requested_at` in the manifest in place, keeping its state
   and lease token (a stranded Pi job still resumes; a remote one stays fenced
   to its exact owner);
b. rename the job directory into `cancelled/` (from here it is cancelled:
   readers treat the directory as authoritative);
c. rewrite the manifest in its final form and detach `source/`;
d. remove the intent.

A manifest with `cancel_requested_at` never moves anywhere but `cancelled/`,
and nothing leaves `cancelled/` except retention.  A remote job is finalized
at once (its lease fences every late upload); a Pi-local job only gets the
request, because its process group belongs to the broker, which finishes it.
`reconcile` completes whatever a crash left between steps.
"""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
import sys
from typing import Callable, Mapping

from van_compute import finished_source, job_lookup, job_states, queue

INTENTS = ".cancel-intents"
REQUESTED = "cancel_requested_at"
DEFAULT_REASON = "cancel-command"


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def is_cancel_requested(manifest: Mapping[str, object]) -> bool:
    return isinstance(manifest.get(REQUESTED), str)


def runs_on_pi(manifest: Mapping[str, object]) -> bool:
    """A running job whose process belongs to the Pi broker (request path)."""
    return manifest.get("placement") == job_states.PI_LOCAL or job_states.is_local_worker(
        str(manifest.get("worker", ""))
    )


def _intent_path(root: Path, job_id: str) -> Path:
    return root / INTENTS / job_id


def _read_intent(root: Path, job_id: str) -> str | None:
    """The state directory the intent says the job left, or None."""
    try:
        payload = queue.load_json(_intent_path(root, job_id))
    except queue.QueueError:
        return None
    origin = payload.get("from")
    return origin if origin in job_states.ACTIVE_STATES else None


def _remove_intent(root: Path, job_id: str) -> None:
    _intent_path(root, job_id).unlink(missing_ok=True)


def record_request(
    root: Path,
    job_dir: Path,
    from_state: str,
    manifest: dict[str, object],
    *,
    reason: str = DEFAULT_REASON,
    now: str | None = None,
) -> dict[str, object]:
    """Steps 0 and (a): the intent and the in-place request.  Lock held."""
    job_id = job_dir.name
    if _read_intent(root, job_id) != from_state:
        queue.atomic_json(_intent_path(root, job_id), {"from": from_state})
    if not is_cancel_requested(manifest):
        manifest[REQUESTED] = now or utc_now()
        manifest.setdefault("cancel_reason", reason)
        queue.atomic_json(job_dir / "manifest.json", manifest)
    return manifest


def _final_manifest(
    manifest: dict[str, object], from_state: str, finish: Mapping[str, object]
) -> dict[str, object]:
    stamp = utc_now()
    final = dict(manifest)
    final.update(finish)
    final.setdefault(REQUESTED, stamp)
    final.setdefault("cancel_reason", DEFAULT_REASON)
    final.setdefault("cancelled_at", stamp)
    final.setdefault("finished_at", final["cancelled_at"])
    final.setdefault("cancelled_from", from_state)
    final["state"] = job_states.CANCELLED
    final.pop("lease_token", None)
    return final


def _complete(
    root: Path,
    job_id: str,
    from_state: str,
    manifest: dict[str, object],
    finish: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Steps (c) and (d) for a job already in `cancelled/`.  Lock held.

    The detach runs even when the manifest is already final: a crash between
    the final write and the detach leaves exactly that state, and the detach
    is a no-op once `source/` is gone.
    """
    destination = root / job_states.CANCELLED / job_id
    if manifest.get("state") != job_states.CANCELLED or "lease_token" in manifest or finish:
        manifest = _final_manifest(manifest, from_state, finish or {})
        queue.atomic_json(destination / "manifest.json", manifest)
    finished_source.finish_detach(root, job_states.CANCELLED, job_id)
    _remove_intent(root, job_id)
    return manifest


def finalize_cancelled(
    root: Path,
    job_dir: Path,
    from_state: str,
    manifest: dict[str, object],
    *,
    reason: str = DEFAULT_REASON,
    finish: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Steps 0 to (d); returns the final manifest.  Lock held by the caller."""
    record_request(root, job_dir, from_state, manifest, reason=reason)
    os.replace(job_dir, root / job_states.CANCELLED / job_dir.name)
    return _complete(root, job_dir.name, from_state, manifest, finish)


def finalize_if_requested(
    root: Path, job_dir: Path, from_state: str, manifest: dict[str, object]
) -> bool:
    """The guard every move out of queued/ or running/ runs first.  Lock held."""
    if not is_cancel_requested(manifest):
        return False
    finalize_cancelled(root, job_dir, from_state, manifest)
    return True


def unless_cancel_requested(
    check: Callable[[Mapping[str, object]], tuple[bool, str]],
) -> Callable[[Mapping[str, object]], tuple[bool, str]]:
    """Wrap a claim eligibility check so a requested job is never claimed."""

    def eligible(manifest: Mapping[str, object]) -> tuple[bool, str]:
        if is_cancel_requested(manifest):
            return False, "cancel requested"
        return check(manifest)

    return eligible


def _finish_in_cancelled(root: Path, found: job_lookup.JobSnapshot) -> dict[str, object]:
    manifest = found.manifest
    origin = _read_intent(root, found.path.name)
    if origin is None:
        prior = manifest.get("cancelled_from") or manifest.get("state")
        origin = prior if prior in job_states.ACTIVE_STATES else job_states.QUEUED
    return _complete(root, found.path.name, origin, manifest)


def _reconcile_one(root: Path, job_id: str) -> str:
    found = job_lookup.find(root, job_id, locked=True)
    if found is None or found.state in (job_states.DONE, job_states.FAILED):
        _remove_intent(root, job_id)
        return "dropped"
    if found.state == job_states.CANCELLED:
        _finish_in_cancelled(root, found)
        return "completed"
    if not is_cancel_requested(found.manifest):
        _remove_intent(root, job_id)
        return "dropped"
    if found.state == job_states.RUNNING and runs_on_pi(found.manifest):
        return "broker-owned"
    finalize_cancelled(root, found.path, found.state, found.manifest)
    return "cancelled"


def reconcile(root: Path) -> dict[str, str]:
    """Finish cancels a crash left between steps.  Cheap when nothing is pending.

    Runs on every broker poll, independent of Mac availability and Pi health.
    A requested Pi-local running job is left to the broker.  Never raises for
    one bad entry: it is logged and retried on the next poll.
    """
    try:
        names = os.listdir(root / INTENTS)
    except FileNotFoundError:
        return {}
    pending = sorted(name for name in names if queue.JOB_ID_RE.fullmatch(name))
    if not pending:
        return {}
    outcome: dict[str, str] = {}
    with queue.queue_lock(root):
        if queue.maintenance_status(root) is not None:
            return {}
        for job_id in pending:
            try:
                outcome[job_id] = _reconcile_one(root, job_id)
            except (queue.QueueError, OSError) as exc:
                outcome[job_id] = "error"
                print(f"van-compute: cancel of {job_id} not reconciled: {exc}", file=sys.stderr)
    return outcome


def cancel_job(
    root: Path, job_id: str, reason: str = DEFAULT_REASON
) -> tuple[str, dict[str, object]]:
    """Cancel one job; returns (cancel result, presented manifest)."""
    if reason not in job_states.CANCEL_REASONS:
        raise queue.QueueError(f"invalid cancel reason {reason!r}")
    with queue.queue_lock(root):
        found = job_lookup.find(root, job_id, locked=True)
        if found is None:
            raise queue.QueueError(f"unknown job {job_id}")
        if found.state == job_states.CANCELLED:
            return job_states.CANCEL_ALREADY_CANCELLED, _finish_in_cancelled(root, found)
        if found.state in (job_states.DONE, job_states.FAILED):
            return job_states.CANCEL_ALREADY_FINISHED, found.presented()
        if queue.maintenance_status(root) is not None:
            raise queue.QueueError("queue maintenance is active; retry shortly")
        manifest = found.manifest
        if found.state == job_states.RUNNING and runs_on_pi(manifest):
            record_request(root, found.path, found.state, manifest, reason=reason)
            return job_states.CANCEL_REQUESTED, manifest
        final = finalize_cancelled(root, found.path, found.state, manifest, reason=reason)
        return job_states.CANCEL_CANCELLED, final


class CancelWatch:
    """Cheap repeated check of one running manifest for a cancel request.

    Manifests can be megabytes, so the file is re-read only when its identity
    changes (`atomic_json` always replaces the inode).  Any read error counts
    as "not requested": the lock-held guards still catch the request when the
    attempt ends.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._identity: tuple[int, int, int] | None = None
        self._requested = False

    def requested(self) -> bool:
        if self._requested:
            return True
        try:
            info = os.stat(self.path)
            identity = (info.st_ino, info.st_mtime_ns, info.st_size)
            if identity != self._identity:
                self._identity = identity
                self._requested = is_cancel_requested(queue.load_json(self.path))
        except (OSError, queue.QueueError):
            return False
        return self._requested
