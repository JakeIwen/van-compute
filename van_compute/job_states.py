"""The job vocabulary shared across the queue, broker, worker and readers.

A job's state is the queue directory that holds it; placements say which host
runs it; the local worker identities are the Pi broker's own leases.  This
module imports nothing else from the package so every reader (metrics, the
frontend, retention) can use it.
"""

from __future__ import annotations

QUEUED = "queued"
RUNNING = "running"
DONE = "done"
FAILED = "failed"

STATE_DIRECTORIES = (QUEUED, RUNNING, DONE, FAILED)
ACTIVE_STATES = (QUEUED, RUNNING)
# The states the dashboard's metrics report and schema cover.
METRICS_STATES = (QUEUED, RUNNING, DONE, FAILED)

REMOTE = "remote"
PI_LOCAL = "pi-local"
JOB_PLACEMENTS = (REMOTE, PI_LOCAL)

LOCAL_WORKER = "vanpi-local.00"
LOCAL_WORKER_PREFIX = "vanpi-local"
# "pi-local" is the identity older brokers used; its jobs are still local.
LOCAL_WORKER_PREFIXES = (LOCAL_WORKER_PREFIX, "pi-local")


def is_local_worker(worker: str) -> bool:
    """True for the Pi broker's own worker identities (base or slot form)."""
    return any(
        worker == prefix or worker.startswith(f"{prefix}.")
        for prefix in LOCAL_WORKER_PREFIXES
    )
