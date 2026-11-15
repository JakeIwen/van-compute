"""Guarded removal of a finished job's `source/` snapshot.

Only a running job reads its `source/` (staging and bundle streaming need a
running lease); once a job is in `done/` or `failed/` the manifest's `sources`
records are the record of what ran. Removal has two steps, and both run only
under the queue lock, because retention measures and deletes finished job
directories assuming the lock excludes every other change inside them:

- `detach_source` renames `source/` to a `.source-trash-<token>` marker inside
  the same job directory. It is O(1), so `worker_finish` does it while
  publishing the job.
- `purge_job_trash` deletes a job's markers. Retention runs it under the lock
  it already holds (before measuring history), and `drop-source --purge` runs
  it one job per lock acquisition. Until then a marker counts as history.

This module is queue-free so `queue.py` can import it. Every walk is
descriptor-relative, never follows links, stays on the queue root's filesystem
and refuses a tree at or above a Linux mount point (a bind mount shares the
device number, so the device check alone does not see it). Mount points are
read from `/proc/self/mountinfo` again immediately before each rename and each
marker purge, never reused from an earlier inventory.
"""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
import re
import secrets
import stat
import sys
from typing import Iterator

SOURCE_DIRECTORY = "source"
TRASH_PREFIX = ".source-trash-"
TRASH_RE = re.compile(r"\.source-trash-[0-9a-f]{16}")
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)


class UnsafeTree(Exception):
    """A tree entry could not be handled without leaving the queue filesystem."""


def _linux() -> bool:
    return sys.platform.startswith("linux")


def linux_mount_points() -> frozenset[Path]:
    if not _linux():
        return frozenset()
    path = Path("/proc/self/mountinfo")
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise UnsafeTree(f"cannot verify mount boundaries from {path}: {exc}") from None
    mounts: set[Path] = set()
    for line in lines:
        fields = line.split()
        if len(fields) < 5:
            raise UnsafeTree(f"cannot parse mount boundary record in {path}")
        text = fields[4]
        for escaped, plain in (("\\040", " "), ("\\011", "\t"), ("\\012", "\n"), ("\\134", "\\")):
            text = text.replace(escaped, plain)
        mounts.add(Path(text))
    return frozenset(mounts)


def mount_at_or_below(target: Path, mounts: frozenset[Path]) -> bool:
    if _linux():
        return any(mount == target or mount.is_relative_to(target) for mount in mounts)
    return os.path.ismount(target)


def is_mount(path: Path, mounts: frozenset[Path]) -> bool:
    return path in mounts if _linux() else os.path.ismount(path)


def remove_open_directory(descriptor: int, device: int) -> None:
    """Empty an opened directory without following links or crossing devices."""
    for name in os.listdir(descriptor):
        details = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
        if details.st_dev != device:
            raise UnsafeTree(f"entry crosses the queue filesystem: {name}")
        if stat.S_ISDIR(details.st_mode):
            child = os.open(name, _DIRECTORY_FLAGS, dir_fd=descriptor)
            try:
                opened = os.fstat(child)
                if (opened.st_dev, opened.st_ino) != (details.st_dev, details.st_ino):
                    raise UnsafeTree(f"directory changed while opening: {name}")
                remove_open_directory(child, device)
            finally:
                os.close(child)
            os.rmdir(name, dir_fd=descriptor)
        elif stat.S_ISREG(details.st_mode) or stat.S_ISLNK(details.st_mode):
            os.unlink(name, dir_fd=descriptor)
        else:
            raise UnsafeTree(f"entry has an unsupported file type: {name}")


@contextlib.contextmanager
def _job_directory(
    state_root: Path, job_id: str, root_device: int, mounts: frozenset[Path]
) -> Iterator[int | None]:
    """Yield a verified descriptor for `<state_root>/<job_id>`, or None if absent."""
    if mount_at_or_below(state_root / job_id, mounts):
        raise UnsafeTree(f"job {job_id} is at or above a mount point")
    state_descriptor = os.open(state_root, _DIRECTORY_FLAGS)
    try:
        try:
            job = os.stat(job_id, dir_fd=state_descriptor, follow_symlinks=False)
        except FileNotFoundError:
            yield None
            return
        if not stat.S_ISDIR(job.st_mode) or job.st_dev != root_device:
            raise UnsafeTree(f"job {job_id} is not a real directory on the queue filesystem")
        job_descriptor = os.open(job_id, _DIRECTORY_FLAGS, dir_fd=state_descriptor)
        try:
            opened = os.fstat(job_descriptor)
            if (opened.st_dev, opened.st_ino) != (job.st_dev, job.st_ino):
                raise UnsafeTree(f"job {job_id} directory changed while opening")
            yield job_descriptor
        finally:
            os.close(job_descriptor)
    finally:
        os.close(state_descriptor)


def leftover_names(
    state_root: Path, job_id: str, root_device: int, mounts: frozenset[Path]
) -> list[str]:
    """Names of `source/` and trash markers present in a finished job directory."""
    with _job_directory(state_root, job_id, root_device, mounts) as job:
        if job is None:
            return []
        return sorted(
            name
            for name in os.listdir(job)
            if name == SOURCE_DIRECTORY or TRASH_RE.fullmatch(name)
        )


def detach_source(state_root: Path, job_id: str, root_device: int) -> str | None:
    """Rename `source/` to a trash marker; return its name, or None if absent.

    Call under the queue lock. Mount points are read fresh for this rename.
    """
    target = state_root / job_id / SOURCE_DIRECTORY
    mounts = linux_mount_points()
    with _job_directory(state_root, job_id, root_device, mounts) as job:
        if job is None:
            return None
        try:
            source = os.stat(SOURCE_DIRECTORY, dir_fd=job, follow_symlinks=False)
        except FileNotFoundError:
            return None
        if not stat.S_ISDIR(source.st_mode) or source.st_dev != root_device:
            raise UnsafeTree(f"{job_id}/source is not a real directory on the queue filesystem")
        if mount_at_or_below(target, mounts):
            raise UnsafeTree(f"{job_id}/source is at or above a mount point")
        trash = f"{TRASH_PREFIX}{secrets.token_hex(8)}"
        os.rename(SOURCE_DIRECTORY, trash, src_dir_fd=job, dst_dir_fd=job)
        return trash


def purge_trash(state_root: Path, job_id: str, name: str, root_device: int) -> None:
    """Delete one trash marker. Call under the queue lock.

    Mount points are read fresh for this marker; a mount at or below it, a
    changed directory or a cross-device entry stops the purge with what remains
    still a marker.
    """
    if not TRASH_RE.fullmatch(name):
        raise UnsafeTree(f"not a source trash marker: {name}")
    mounts = linux_mount_points()
    with _job_directory(state_root, job_id, root_device, mounts) as job:
        if job is None:
            return
        try:
            details = os.stat(name, dir_fd=job, follow_symlinks=False)
        except FileNotFoundError:
            return
        if not stat.S_ISDIR(details.st_mode) or details.st_dev != root_device:
            raise UnsafeTree(f"{job_id}/{name} is not a real directory on the queue filesystem")
        if mount_at_or_below(state_root / job_id / name, mounts):
            raise UnsafeTree(f"{job_id}/{name} is at or above a mount point")
        child = os.open(name, _DIRECTORY_FLAGS, dir_fd=job)
        try:
            opened = os.fstat(child)
            if (opened.st_dev, opened.st_ino) != (details.st_dev, details.st_ino):
                raise UnsafeTree(f"{job_id}/{name} changed while opening")
            remove_open_directory(child, root_device)
        finally:
            os.close(child)
        os.rmdir(name, dir_fd=job)


def purge_job_trash(state_root: Path, job_id: str, root_device: int) -> list[str]:
    """Delete every trash marker in one finished job; return the names removed.

    Call under the queue lock. The listing is taken now, not from an inventory.
    """
    names = [
        name
        for name in leftover_names(state_root, job_id, root_device, linux_mount_points())
        if TRASH_RE.fullmatch(name)
    ]
    for name in names:
        purge_trash(state_root, job_id, name, root_device)
    return names


def finish_detach(root: Path, state: str, job_id: str) -> str | None:
    """Detach a just-published job's source; never raises (call under the queue lock)."""
    try:
        return detach_source(root / state, job_id, root.stat().st_dev)
    except (OSError, UnsafeTree) as exc:
        print(f"van-compute: kept source of {state}/{job_id}: {exc}", file=sys.stderr)
        return None
