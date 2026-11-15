"""One-time sweep dropping `source/` from jobs that finished before it was dropped on finish.

The preview takes no lock and changes nothing; its inventory only reports.
`--apply` renames each `source/` aside (`finished_source.detach_source`) and
`--purge` also deletes the job's trash markers, both under `.queue.lock`, which
is taken once per job so claims, publication and recovery interleave between
jobs. Every rename and purge lists the job directory and reads mount points
again under that lock; nothing from the preview inventory is trusted. Without
`--purge` the markers stay in the job directories until the next retention run
deletes them under its own hold of the lock.
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
        "--apply", action="store_true", help="rename source/ aside (default: report only)"
    )
    parser.add_argument(
        "--purge",
        action="store_true",
        help="also delete the renamed trees now, one job per queue-lock hold (implies --apply)",
    )


def _skip(skipped: list[dict[str, str]], state: str, job_id: str, reason: str) -> None:
    skipped.append({"job_id": job_id, "state": state, "reason": reason})


def _inventory(
    root: Path, device: int, mounts: frozenset[Path], skipped: list[dict[str, str]]
) -> list[tuple[str, str, int]]:
    """Report-only: jobs with a source or trash marker as (state, job id, allocated bytes)."""
    found: list[tuple[str, str, int]] = []
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
                found.append((state, job_id, size))
    return found


def _require_sweep_allowed(root: Path, job_id: str) -> None:
    """Checks repeated under each lock hold before touching a job."""
    maintenance = queue.maintenance_status(root)
    if maintenance is not None:
        raise queue.QueueError(
            "cannot drop sources while queue maintenance is active for "
            f"{maintenance.get('owner')}"
        )
    if any(os.path.lexists(root / active / job_id) for active in ("queued", "running")):
        raise finished_source.UnsafeTree("also present in queued or running")


def drop_sources(root: Path, *, apply: bool = False, purge: bool = False) -> dict[str, object]:
    apply = apply or purge
    device = root.stat().st_dev
    skipped: list[dict[str, str]] = []
    candidates = _inventory(root, device, finished_source.linux_mount_points(), skipped)
    detached: list[str] = []
    purged: list[str] = []
    freed_bytes = 0
    if apply:
        for state, job_id, size in candidates:
            try:
                with queue.queue_lock(root):
                    _require_sweep_allowed(root, job_id)
                    if finished_source.detach_source(root / state, job_id, device):
                        detached.append(job_id)
                    if purge:
                        finished_source.purge_job_trash(root / state, job_id, device)
                        purged.append(job_id)
                        freed_bytes += size
            except (OSError, finished_source.UnsafeTree) as exc:
                _skip(skipped, state, job_id, str(exc))
    return {
        "apply": apply,
        "purge": purge,
        "jobs_with_source": len(candidates),
        "allocated_bytes": sum(item[2] for item in candidates),
        "detached": detached,
        "purged": purged,
        "freed_bytes": freed_bytes,
        "skipped": skipped,
    }


def run_drop_source_command(
    root: Path, args: argparse.Namespace, printer: Callable[[object], None]
) -> int:
    payload = drop_sources(root, apply=args.apply, purge=args.purge)
    printer(payload)
    return 1 if payload["skipped"] else 0
