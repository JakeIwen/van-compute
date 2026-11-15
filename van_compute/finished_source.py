"""Guarded removal of a finished job's `source/` snapshot.

Only a running job reads its `source/` (staging and bundle streaming need a
running lease); once a job is in `done/` or `failed/` the manifest's `sources`
records are the record of what ran. This module is queue-free so `queue.py` can
import it; callers hold the queue lock.
"""

from __future__ import annotations

import os
import stat

SOURCE_DIRECTORY = "source"
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)


class UnsafeTree(Exception):
    """A tree entry could not be removed without leaving the queue filesystem."""


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


def drop_finished_source(state_root: str | os.PathLike[str], job_id: str) -> bool:
    """Remove `<state_root>/<job_id>/source`; return whether one was removed.

    Every step is relative to opened descriptors with no link following, and
    each directory must share the filesystem of the state directory.
    """
    state_descriptor = os.open(state_root, _DIRECTORY_FLAGS)
    try:
        device = os.fstat(state_descriptor).st_dev
        try:
            job = os.stat(job_id, dir_fd=state_descriptor, follow_symlinks=False)
        except FileNotFoundError:
            return False
        if not stat.S_ISDIR(job.st_mode) or job.st_dev != device:
            raise UnsafeTree(f"job {job_id} is not a same-filesystem real directory")
        job_descriptor = os.open(job_id, _DIRECTORY_FLAGS, dir_fd=state_descriptor)
        try:
            try:
                source = os.stat(SOURCE_DIRECTORY, dir_fd=job_descriptor, follow_symlinks=False)
            except FileNotFoundError:
                return False
            if not stat.S_ISDIR(source.st_mode) or source.st_dev != device:
                raise UnsafeTree(f"{job_id}/source is not a same-filesystem real directory")
            source_descriptor = os.open(SOURCE_DIRECTORY, _DIRECTORY_FLAGS, dir_fd=job_descriptor)
            try:
                opened = os.fstat(source_descriptor)
                if (opened.st_dev, opened.st_ino) != (source.st_dev, source.st_ino):
                    raise UnsafeTree(f"{job_id}/source changed while opening")
                remove_open_directory(source_descriptor, device)
            finally:
                os.close(source_descriptor)
            os.rmdir(SOURCE_DIRECTORY, dir_fd=job_descriptor)
            return True
        finally:
            os.close(job_descriptor)
    finally:
        os.close(state_descriptor)
