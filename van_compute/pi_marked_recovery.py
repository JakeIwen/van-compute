"""Recover a running Pi job whose cleanup was left unconfirmed, before anything else.

A marked job (see `job_cleanup_marker`) is handled ahead of every eligibility,
capacity and staging step: killing the old group and discarding its cache entry
must not depend on disk space or on anything that could make the job fail early
and repeat.  The broker passes in the few names that live in `broker.py`, so
this module never imports it.
"""

from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
from pathlib import Path
from typing import Callable, Mapping

from van_compute import task_cache
from van_compute.job_cancel import CancelWatch
from van_compute.job_cleanup_marker import (
    clear_cleanup_unconfirmed, mark_cleanup_unconfirmed, read_group_start,
)
from van_compute.pi_cleanup_hold import CleanupHold

MARKED_JOB_MESSAGE = (
    "cleanup was unconfirmed in a previous broker instance; "
    "the job is not re-executed"
)


@dataclass(frozen=True)
class RecoveryTools:
    """What `broker.py` owns and the recovery needs."""

    hold_class: type[CleanupHold]
    group_exists: Callable[[int], bool]
    read_boot_id: Callable[[], str | None]
    unconfirmed_error: type[RuntimeError]
    error_type: type[RuntimeError]
    worker: str
    local_eligibility: Callable[[Mapping[str, object]], tuple[bool, str]]
    capacity_check: Callable[[Mapping[str, object], object], tuple[bool, str]]


def build_hold(
    tools: RecoveryTools, root: Path, job_id: str, lease_token: str,
    should_stop: Callable[[], bool],
) -> CleanupHold:
    hold = tools.hold_class(
        job=job_id,
        identify=read_group_start,
        group_exists=tools.group_exists,
        service_stopping=should_stop,
        mark=lambda group: mark_cleanup_unconfirmed(
            root, job_id, lease_token, worker=tools.worker,
            now=dt.datetime.now(dt.timezone.utc), group=group,
            boot_id=tools.read_boot_id(), group_start=hold.identity,
        ),
        unmark=lambda: clear_cleanup_unconfirmed(
            root, job_id, lease_token, worker=tools.worker
        ),
        unconfirmed_error=tools.unconfirmed_error,
    )
    return hold


def hold_and_discard(
    tools: RecoveryTools, manifest, hold: CleanupHold, cache_store
) -> None:
    """Hold a previous run's group until it is gone, then discard the job's
    cache entry (the killed group may have left it half written)."""
    hold.hold_previous_group(manifest, tools.read_boot_id)
    request = task_cache.request_for_manifest(manifest)
    if cache_store is not None and request is not None:
        hold.until_done(
            "cache-discard",
            lambda: cache_store.discard(request),
            entry=f"{request.namespace}/{request.name}",
        )


def recover_marked_job(
    tools: RecoveryTools, args, manifest, hold: CleanupHold, cache_store,
    cancel_requested: Callable[[], bool] = lambda: False,
) -> bool:
    """Hold and discard, then wait (logged) for the staging space the
    failure's publication needs.

    A cancel requested meanwhile ends the wait: a cancelled job publishes
    nothing.  Returns True when a cancel was seen."""
    hold_and_discard(tools, manifest, hold, cache_store)
    if cancel_requested():
        return True

    def space_reason() -> str | None:
        if cancel_requested():
            return None
        ok, reason = tools.capacity_check(manifest, args)
        return None if ok else reason

    hold.until_done("failure-publication", space_reason)
    return cancel_requested()


def require_local_eligibility(tools: RecoveryTools, args, manifest) -> None:
    eligible, reason = tools.local_eligibility(manifest)
    if not eligible:
        raise tools.error_type(reason)
    if manifest.get("placement") != "pi-local":
        raise tools.error_type("local job is missing pi-local placement metadata")
    capacity_ok, capacity_reason = tools.capacity_check(manifest, args)
    if not capacity_ok:
        raise tools.error_type(capacity_reason)


def prepare_claim(
    tools: RecoveryTools, args, root: Path, manifest, job_id: str,
    lease_token: str, should_stop: Callable[[], bool],
) -> tuple[bool, task_cache.TaskCacheStore | None, CleanupHold]:
    """Recover a marked job, or check an ordinary one's eligibility.

    Returns (marked, cache store, hold); `marked` is False once a cancel
    request arrived during recovery, since there is no failure to publish.
    """
    marked = bool(manifest.get("cleanup_unconfirmed"))
    cache_store = (
        None if args.task_cache_root is None
        else task_cache.TaskCacheStore(args.task_cache_root, args.task_cache_max_bytes)
    )
    hold = build_hold(tools, root, job_id, lease_token, should_stop)
    if marked:
        watch = CancelWatch(root / "running" / job_id / "manifest.json")
        if recover_marked_job(tools, args, manifest, hold, cache_store, watch.requested):
            # Nothing to fail or publish: the caller's pre-staging launch check
            # sees the request and discards the attempt into cancelled/.
            marked = False
    else:
        require_local_eligibility(tools, args, manifest)
    return marked, cache_store, hold
