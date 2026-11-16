"""Finish a cancelled Pi-local job whose broker attempt is gone.

After a broker restart a requested job is cleanup, not admission: it never
waits on the health gate, CAN quiet, eligibility or staging space.  It moves to
`cancelled/` only once the previous attempt's processes are known gone.

A marked job's previous group is held until it is gone and its cache entry
discarded (as marked recovery does).  An unmarked job has no recorded group: the
broker died before it could write one.  Its evidence is the startup snapshot of
the service's cgroup (`pi_cgroup_survivors`): systemd's control-group kill does
not stop a process in uninterruptible sleep, so the previous instance's
leftovers are whatever else was in the cgroup when this broker started.  Only
when none of them is still alive is the job finalized.  While any is, the job
stays `running` with its request (the installer's drain still sees it), the
survivors are sent SIGKILL again, and the poll is deferred and retried.  With no
service cgroup (a foreground broker) nothing contains the previous run, and the
job is finalized as an ordinary resume would be: the documented foreground
limit.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Callable

from van_compute import job_requeue, pi_cgroup_survivors, pi_marked_recovery, queue, task_cache
from van_compute.pi_health_monitor import HealthMonitor

REASON = "cancel-recovery"


def finish(
    tools: pi_marked_recovery.RecoveryTools,
    args,
    root: Path,
    manifest: dict[str, object],
    should_stop: Callable[[], bool],
    monitor: HealthMonitor | None = None,
) -> dict[str, object]:
    """`monitor` carries the broker's startup survivor snapshot; a caller with
    none (`run_once` driven by a bare health reader, as tests do) has no
    containment evidence, which is the uncontained case."""
    job_id = queue.validate_job_id(str(manifest.get("id", "")))
    lease_token = str(manifest.get("lease_token", ""))
    if manifest.get("cleanup_unconfirmed"):
        hold = pi_marked_recovery.build_hold(tools, root, job_id, lease_token, should_stop)
        cache_store = (
            None if args.task_cache_root is None
            else task_cache.TaskCacheStore(args.task_cache_root, args.task_cache_max_bytes)
        )
        pi_marked_recovery.hold_and_discard(tools, manifest, hold, cache_store)
    elif monitor is not None and monitor.survivors is not None:
        survivors = monitor.survivors
        status, alive = survivors.status()
        if status == pi_cgroup_survivors.PRESENT:
            survivors.kill_remaining()
            return {
                "action": "deferred",
                "reason": "cancel recovery waits for the previous broker's processes",
                "job_id": job_id,
                "survivors": list(alive),
            }
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
