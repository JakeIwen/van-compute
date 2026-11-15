"""One-time sweep dropping `source/` from jobs that finished before it was dropped on finish.

The preview and the sizing run without the queue lock. Apply takes the lock
only to rename each `source/` aside (`finished_source.detach_source`) and
deletes the markers after releasing it, so claims, publication and recovery
are never blocked behind recursive deletion.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import stat
from typing import Callable

from van_compute import finished_source, queue, retention

SWEEP_STATES = ("done", "failed")


def add_drop_source_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("drop-source", help=argparse.SUPPRESS)
    parser.add_argument(
        "--apply", action="store_true", help="remove source/ (default: report only)"
    )


def _skip(skipped: list[dict[str, str]], state: str, job_id: str, reason: str) -> None:
    skipped.append({"job_id": job_id, "state": state, "reason": reason})


def _inventory(
    root: Path, device: int, mounts: frozenset[Path], skipped: list[dict[str, str]]
) -> list[tuple[str, str, list[str], int]]:
    """Jobs with a source or trash marker: (state, job id, names, allocated bytes)."""
    found: list[tuple[str, str, list[str], int]] = []
    for state in SWEEP_STATES:
        try:
            names = sorted(os.listdir(root / state))
        except OSError as exc:
            raise queue.QueueError(f"cannot inspect queue state {root / state}: {exc}") from None
        for job_id in names:
            if not queue.JOB_ID_RE.fullmatch(job_id):
                continue
            path = root / state / job_id
            try:
                details = path.lstat()
                if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
                    _skip(skipped, state, job_id, "job entry is not a real directory")
                    continue
                leftovers = finished_source.leftover_names(
                    root / state, job_id, device, mounts
                )
                size = 0
                for name in leftovers:
                    size += retention._tree_sizes(path / name, device, mounts)[0]
            except (OSError, finished_source.UnsafeTree, queue.QueueError) as exc:
                _skip(skipped, state, job_id, str(exc))
                continue
            if leftovers:
                found.append((state, job_id, leftovers, size))
    return found


def drop_sources(root: Path, *, apply: bool = False) -> dict[str, object]:
    device = root.stat().st_dev
    mounts = finished_source.linux_mount_points()
    skipped: list[dict[str, str]] = []
    candidates = _inventory(root, device, mounts, skipped)
    dropped: list[str] = []
    freed_bytes = 0
    if apply:
        markers: list[tuple[str, str, str, int]] = []
        with queue.queue_lock(root):
            maintenance = queue.maintenance_status(root)
            if maintenance is not None:
                raise queue.QueueError(
                    "cannot drop sources while queue maintenance is active for "
                    f"{maintenance.get('owner')}"
                )
            for state, job_id, names, size in candidates:
                if any(
                    os.path.lexists(root / active / job_id) for active in ("queued", "running")
                ):
                    _skip(skipped, state, job_id, "also present in queued or running")
                    continue
                try:
                    if finished_source.SOURCE_DIRECTORY in names:
                        names = [
                            finished_source.detach_source(
                                root / state, job_id, device, mounts
                            )
                            if name == finished_source.SOURCE_DIRECTORY
                            else name
                            for name in names
                        ]
                except (OSError, finished_source.UnsafeTree) as exc:
                    _skip(skipped, state, job_id, str(exc))
                    continue
                markers.extend((state, job_id, name, size) for name in names if name)
        failed: set[str] = set()
        for state, job_id, name, _size in markers:
            try:
                finished_source.purge_trash(root / state, job_id, name, device, mounts)
            except (OSError, finished_source.UnsafeTree) as exc:
                failed.add(job_id)
                _skip(skipped, state, job_id, f"left {name}: {exc}")
        for state, job_id, _name, size in markers:
            if job_id not in failed and job_id not in dropped:
                dropped.append(job_id)
                freed_bytes += size
    return {
        "apply": apply,
        "jobs_with_source": len(candidates),
        "dropped": dropped,
        "skipped": skipped,
        "allocated_bytes": sum(item[3] for item in candidates),
        "freed_bytes": freed_bytes,
    }


def run_drop_source_command(
    root: Path, args: argparse.Namespace, printer: Callable[[object], None]
) -> int:
    payload = drop_sources(root, apply=args.apply)
    printer(payload)
    return 1 if payload["skipped"] else 0
