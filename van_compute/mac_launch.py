"""The Mac worker's launch boundary: refuse a stopped run, log what launched.

`require_launch` runs after preparation and again inside the task-cache scope
right before the child is spawned, so a stop observed at either point (a lease
lost to a cancel, a service stop, one set while the cache lock was blocking)
launches nothing.  `log_launched_group` records the child's process group for
the live check; it is best-effort and never raises, so supervision always
starts.
"""

from __future__ import annotations

from pathlib import Path
import sys


def require_launch(stop_event, shutdown_error: type[Exception]) -> None:
    if stop_event is not None and stop_event.is_set():
        raise shutdown_error("stopped before launch")


def log_launched_group(group: int, work_path: Path | None) -> None:
    try:
        where = work_path.name if work_path is not None else "?"
        print(
            f"van-compute-worker: launched process group {group} in {where}",
            file=sys.stderr,
            flush=True,
        )
    except Exception:
        pass
