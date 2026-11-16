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
CANCELLED = "cancelled"

STATE_DIRECTORIES = (QUEUED, RUNNING, DONE, FAILED, CANCELLED)
ACTIVE_STATES = (QUEUED, RUNNING)
TERMINAL_STATES = (DONE, FAILED, CANCELLED)
# The states the dashboard's metrics report and schema cover.  Cancelled jobs
# are left out until the dashboard (a separate repo, golden-pinned schema)
# learns to show them.
METRICS_STATES = (QUEUED, RUNNING, DONE, FAILED)

# What `cancel` reports, and why a job was cancelled.
CANCEL_CANCELLED = "cancelled"
CANCEL_REQUESTED = "requested"
CANCEL_ALREADY_CANCELLED = "already-cancelled"
CANCEL_ALREADY_FINISHED = "already-finished"
CANCEL_RESULTS = (
    CANCEL_CANCELLED, CANCEL_REQUESTED, CANCEL_ALREADY_CANCELLED, CANCEL_ALREADY_FINISHED,
)
# A cancel that lands: the job is, or will certainly end, cancelled.
CANCEL_LANDED = (CANCEL_CANCELLED, CANCEL_REQUESTED, CANCEL_ALREADY_CANCELLED)
CANCEL_REASONS = ("cancel-command", "wait-timeout", "caller-signal")

# Command exit statuses shared by pi_compute.py and van_compute.py.
EXIT_OK = 0
EXIT_FAILED = 1
EXIT_ERROR = 2
EXIT_WAIT_TIMED_OUT = 3
EXIT_CANCELLED = 4

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


def exit_status(manifest) -> int:
    """The exit status for a job's final (or last seen) manifest."""
    state = manifest.get("state")
    if state == FAILED:
        return EXIT_FAILED
    if state == CANCELLED:
        return EXIT_CANCELLED
    return EXIT_OK
