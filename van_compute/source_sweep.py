"""One-time sweep dropping `source/` from jobs that finished before it was dropped on finish."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import stat
from typing import Callable

from van_compute import finished_source, queue

SWEEP_STATES = ("done", "failed")


def add_drop_source_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("drop-source", help=argparse.SUPPRESS)
    parser.add_argument(
        "--apply", action="store_true", help="remove source/ (default: report only)"
    )


def _allocated_bytes(path: Path) -> int:
    total = 0
    for directory, names, files in os.walk(path):
        total += os.lstat(directory).st_blocks * 512
        for name in files:
            total += os.lstat(os.path.join(directory, name)).st_blocks * 512
    return total


def _candidates(root: Path) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    for state in SWEEP_STATES:
        for entry in sorted(os.listdir(root / state)):
            source = root / state / entry / finished_source.SOURCE_DIRECTORY
            if queue.JOB_ID_RE.fullmatch(entry) and os.path.lexists(source):
                found.append((state, entry))
    return found


def drop_sources(root: Path, *, apply: bool = False) -> dict[str, object]:
    with queue.queue_lock(root):
        if apply:
            maintenance = queue.maintenance_status(root)
            if maintenance is not None:
                raise queue.QueueError(
                    f"cannot drop sources while queue maintenance is active for {maintenance.get('owner')}"
                )
        dropped: list[str] = []
        skipped: list[dict[str, str]] = []
        pending_bytes = freed_bytes = 0
        candidates = _candidates(root)
        for state, job_id in candidates:
            source = root / state / job_id / finished_source.SOURCE_DIRECTORY
            if any(os.path.lexists(root / active / job_id) for active in ("queued", "running")):
                skipped.append({"job_id": job_id, "reason": "also present in queued or running"})
                continue
            if stat.S_ISLNK(source.lstat().st_mode):
                skipped.append({"job_id": job_id, "reason": "source is a symlink"})
                continue
            try:
                size = _allocated_bytes(source)
            except OSError as exc:
                skipped.append({"job_id": job_id, "reason": f"cannot inspect: {exc}"})
                continue
            pending_bytes += size
            if not apply:
                continue
            try:
                finished_source.drop_finished_source(root / state, job_id)
            except (OSError, finished_source.UnsafeTree) as exc:
                skipped.append({"job_id": job_id, "reason": str(exc)})
                continue
            dropped.append(job_id)
            freed_bytes += size
    return {
        "apply": apply,
        "jobs_with_source": len(candidates),
        "dropped": dropped,
        "skipped": skipped,
        "allocated_bytes": pending_bytes,
        "freed_bytes": freed_bytes,
    }


def run_drop_source_command(
    root: Path, args: argparse.Namespace, printer: Callable[[object], None]
) -> int:
    payload = drop_sources(root, apply=args.apply)
    printer(payload)
    return 1 if payload["skipped"] else 0
