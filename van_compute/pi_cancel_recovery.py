"""Finish a cancelled Pi-local job whose broker attempt is gone.

After a broker restart a requested job is cleanup, not admission: it never
waits on the health gate, CAN quiet, eligibility or staging space.  It moves to
`cancelled/` only once the previous attempt's processes are known gone.

A marked job's previous group is held until it is gone and its cache entry
discarded (as marked recovery does).  An unmarked job has no recorded group
(the broker died before it could write one); the service-cgroup gate
(`pi_cgroup_gate`) runs ahead of this in the serve loop and defers the poll
while any other process is in the broker's cgroup, so by the time an unmarked
job gets here the previous instance's processes are gone.  A foreground broker
(`--once`, or one not run as a `.service`) is ungated: nothing contains the
previous run, the documented foreground limit.
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
