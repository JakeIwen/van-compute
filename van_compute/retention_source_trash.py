"""Retention's handling of `.source-trash-*` markers in finished jobs.

`worker_finish` and `drop-source --apply` only rename a finished job's
`source/` to a marker. Retention deletes the markers under the queue lock it
already holds, after its apply checks and before it inventories or measures
history, so nothing else changes a finished job directory while it works and
the history budget sees the space already freed. Each marker's purge
(`finished_source.purge_trash`) reads mount points again first.
"""

from __future__ import annotations

import os
from pathlib import Path
import stat
from typing import Callable, Iterable, Iterator

from van_compute import finished_source, queue


def _skip(state: str, job_id: str, reason: str) -> dict[str, str]:
    return {"state": state, "job_id": job_id, "reason": reason}


def _jobs_with_trash(
    root: Path, states: Iterable[str], skipped: list[dict[str, str]]
) -> Iterator[tuple[str, str, list[str]]]:
    """Yield (state, job id, marker names) for real finished job directories.

    Markers are listed now, with mount points read fresh for each job. Entries
    that are not same-filesystem real directories are left to retention's
    classification, which reports them as skipped jobs.
    """
    device = root.stat().st_dev
    for state in states:
        state_root = root / state
        try:
            names = sorted(os.listdir(state_root), key=os.fsencode)
        except OSError as exc:
            raise queue.QueueError(f"cannot inspect queue state {state_root}: {exc}") from None
        for job_id in names:
            if not queue.JOB_ID_RE.fullmatch(job_id):
                continue
            try:
                details = (state_root / job_id).lstat()
                if (
                    stat.S_ISLNK(details.st_mode)
                    or not stat.S_ISDIR(details.st_mode)
                    or details.st_dev != device
                ):
                    continue
                markers = [
                    name
                    for name in finished_source.leftover_names(
                        state_root, job_id, device, finished_source.linux_mount_points()
                    )
                    if finished_source.TRASH_RE.fullmatch(name)
                ]
            except FileNotFoundError:
                continue
            except (OSError, finished_source.UnsafeTree) as exc:
                skipped.append(_skip(state, job_id, f"cannot list source trash: {exc}"))
                continue
            if markers:
                yield state, job_id, markers


def preview(
    root: Path, states: Iterable[str], measure: Callable[[Path], int]
) -> dict[str, object]:
    """Report the markers an apply would purge; changes nothing, takes no lock."""
    skipped: list[dict[str, str]] = []
    jobs: list[str] = []
    markers = 0
    total = 0
    for state, job_id, names in _jobs_with_trash(root, states, skipped):
        try:
            size = sum(measure(root / state / job_id / name) for name in names)
        except queue.QueueError as exc:
            skipped.append(_skip(state, job_id, str(exc)))
            continue
        jobs.append(job_id)
        markers += len(names)
        total += size
    return {"jobs": jobs, "markers": markers, "bytes": total, "skipped": skipped}


def purge(
    root: Path,
    states: Iterable[str],
    *,
    is_active: Callable[[str], bool],
    before_each: Callable[[], None],
) -> dict[str, object]:
    """Delete every marker in `states`. Call under the queue lock.

    `is_active` skips a job ID also present in `queued/` or `running/`;
    `before_each` repeats retention's apply checks before each job and raises
    to stop the run.
    """
    skipped: list[dict[str, str]] = []
    jobs: list[str] = []
    markers = 0
    device = root.stat().st_dev
    for state, job_id, names in _jobs_with_trash(root, states, skipped):
        if is_active(job_id):
            skipped.append(_skip(state, job_id, "also present in queued or running"))
            continue
        before_each()
        try:
            for name in names:
                finished_source.purge_trash(root / state, job_id, name, device)
                markers += 1
        except (OSError, finished_source.UnsafeTree) as exc:
            skipped.append(_skip(state, job_id, f"left source trash: {exc}"))
            continue
        jobs.append(job_id)
    return {"jobs": jobs, "markers": markers, "skipped": skipped}
