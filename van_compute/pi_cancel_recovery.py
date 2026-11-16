"""Finish a cancelled Pi-local job whose broker attempt is gone.

After a broker restart a requested job is cleanup, not admission: it never
waits on the health gate, CAN quiet, eligibility or staging space.  A marked
job's previous group is still held until it is gone and its cache entry
discarded (as marked recovery does); then the job moves to `cancelled/`.

An unmarked job is finalized without a hold, the same guarantee today's
unmarked resume relies on: inside one broker process every exit from
supervision confirms the group gone or marks the job, and when the broker
process itself dies, the service unit's `KillMode=control-group` with
`Restart=always` makes systemd stop the whole control group (the job's
bubblewrap group included) before the next start.  A broker run in the
foreground that crashes is outside that guarantee, exactly as for a resume.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Callable

from van_compute import job_requeue, pi_marked_recovery, queue, task_cache

REASON = "cancel-recovery"


def finish(
    tools: pi_marked_recovery.RecoveryTools,
    args,
    root: Path,
    manifest: dict[str, object],
    should_stop: Callable[[], bool],
) -> dict[str, object]:
    job_id = queue.validate_job_id(str(manifest.get("id", "")))
    lease_token = str(manifest.get("lease_token", ""))
    if manifest.get("cleanup_unconfirmed"):
        hold = pi_marked_recovery.build_hold(tools, root, job_id, lease_token, should_stop)
        cache_store = (
            None if args.task_cache_root is None
            else task_cache.TaskCacheStore(args.task_cache_root, args.task_cache_max_bytes)
        )
        pi_marked_recovery.hold_and_discard(tools, manifest, hold, cache_store)
    outcome = job_requeue.requeue_owned_job(
        root, job_id, lease_token, worker=tools.worker, reason=REASON,
        now=dt.datetime.now(dt.timezone.utc), error_type=tools.error_type,
    )
    if outcome == job_requeue.CANCELLED:
        return {"action": "cancelled", "reason": REASON, "job_id": job_id}
    return {
        "action": "deferred",
        "reason": f"cancel recovery left job untouched: {outcome}",
        "job_id": job_id,
    }
