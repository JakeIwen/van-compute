"""Find a job's directory and read its manifest without taking the queue lock.

Jobs move between state directories by one atomic rename under the queue lock,
but readers (`status`, `wait`, `list`, `result`) do not take that lock.  A
scan can therefore miss a job that moves into a directory it already checked
(a requeue moves `running/` back to `queued/`), and a manifest can vanish
between locating its directory and opening it.  Reads here retry a few passes,
which a single rename cannot defeat, and fall back to one lookup under the
lock before they call a real job unknown.  Callers that already hold the lock
pass `locked=True`: nothing can move then, so one scan is exact (and the lock,
which is not reentrant, is not taken again).
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import sys

from van_compute import job_states

PASSES = 3


def _queue():
    # `python -m van_compute.queue` runs the queue as __main__; share that
    # module (its QueueError and lock) instead of importing a second copy.
    main = sys.modules.get("__main__")
    if getattr(main, "__spec__", None) is not None and main.__spec__.name == "van_compute.queue":
        sys.modules.setdefault("van_compute.queue", main)
    from van_compute import queue

    return queue


@dataclass(frozen=True)
class JobSnapshot:
    state: str
    path: Path
    manifest: dict[str, object]

    def presented(self) -> dict[str, object]:
        """The manifest as readers show it: anything in cancelled/ is cancelled,
        even if a crash left its final rewrite undone."""
        return presented(self.state, self.manifest)


def presented(state: str, manifest: dict[str, object]) -> dict[str, object]:
    if state == job_states.CANCELLED and manifest.get("state") != job_states.CANCELLED:
        return {**manifest, "state": job_states.CANCELLED}
    return manifest


def _is_job_dir(path: Path) -> bool:
    return not path.is_symlink() and path.is_dir()


def _locate_once(root: Path, job_id: str) -> tuple[str, Path] | None:
    for state in job_states.STATE_DIRECTORIES:
        candidate = root / state / job_id
        if _is_job_dir(candidate):
            return state, candidate
    return None


def _read(path: Path) -> dict[str, object] | None:
    """The manifest, or None when its directory moved away mid-read."""
    queue = _queue()
    manifest_path = path / "manifest.json"
    try:
        handle = manifest_path.open(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise queue.QueueError(f"cannot read {manifest_path}: {exc}") from None
    with handle:
        return queue.load_json_stream(handle, manifest_path)


def _snapshot_once(root: Path, job_id: str) -> JobSnapshot | None:
    found = _locate_once(root, job_id)
    if found is None:
        return None
    state, path = found
    manifest = _read(path)
    if manifest is None:
        return None
    return JobSnapshot(state, path, manifest)


def find(root: Path, job_id: str, *, locked: bool = False) -> JobSnapshot | None:
    """The job's directory and manifest, or None when it does not exist."""
    queue = _queue()
    queue.validate_job_id(job_id)
    if locked:
        return _snapshot_once(root, job_id)
    for _ in range(PASSES):
        found = _snapshot_once(root, job_id)
        if found is not None:
            return found
    with queue.queue_lock(root):
        return _snapshot_once(root, job_id)


def locate(root: Path, job_id: str) -> tuple[str, Path] | None:
    """The job's state and directory (no manifest read), with the same retries."""
    queue = _queue()
    queue.validate_job_id(job_id)
    for _ in range(PASSES):
        found = _locate_once(root, job_id)
        if found is not None:
            return found
    with queue.queue_lock(root):
        return _locate_once(root, job_id)


def snapshot(root: Path, job_id: str, *, locked: bool = False) -> JobSnapshot:
    found = find(root, job_id, locked=locked)
    if found is None:
        raise _queue().QueueError(f"unknown job {job_id}")
    return found


def _listed_ids(root: Path) -> dict[str, list[str]]:
    """Job ids per directory, from directory listings only (no manifest reads)."""
    queue = _queue()
    seen: dict[str, list[str]] = {}
    for state in job_states.STATE_DIRECTORIES:
        try:
            names = os.listdir(root / state)
        except FileNotFoundError:
            continue
        for name in names:
            if queue.JOB_ID_RE.fullmatch(name) and _is_job_dir(root / state / name):
                seen.setdefault(name, []).append(state)
    return seen


def list_manifests(root: Path) -> list[dict[str, object]]:
    """Every readable job manifest, once per job id.

    Two listing passes catch a job that moved into an already-listed directory
    during the first.  An id listed in more than one directory, or whose
    manifest vanished while it was read, is located again before it is read,
    so a moving job is reported once, from where it is now.  Unreadable
    manifests are skipped, as `list` always has.
    """
    queue = _queue()
    listed = _listed_ids(root)
    for job_id, states in _listed_ids(root).items():
        listed.setdefault(job_id, []).extend(states)
    manifests: list[dict[str, object]] = []
    for job_id, states in sorted(listed.items()):
        try:
            found = None
            if len(set(states)) == 1:
                manifest = _read(root / states[0] / job_id)
                if manifest is not None:
                    found = presented(states[0], manifest)
            if found is None:
                relocated = find(root, job_id)
                found = None if relocated is None else relocated.presented()
        except queue.QueueError:
            continue
        if found is not None:
            manifests.append(found)
    return manifests
