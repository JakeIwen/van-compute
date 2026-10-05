#!/usr/bin/env python3
"""Standalone, stdlib-only resource-limit helper for both compute hosts.

Run this trusted file directly, never through a package import or a job's
PYTHONPATH. The host token selects the original host-specific argv contract.
"""

from __future__ import annotations

import os
import resource
import sys
from typing import Sequence


class BrokerError(RuntimeError):
    """Internal broker helper error; no application package imports are needed."""


class WorkerError(RuntimeError):
    """Internal worker helper error; no application package imports are needed."""


def _set_limit(kind: int, soft: int, hard: int | None = None) -> None:
    current_soft, current_hard = resource.getrlimit(kind)
    del current_soft
    desired_hard = soft if hard is None else hard
    if current_hard != resource.RLIM_INFINITY:
        desired_hard = min(desired_hard, current_hard)
        soft = min(soft, desired_hard)
    resource.setrlimit(kind, (soft, desired_hard))


def broker_main(argv: Sequence[str]) -> int:
    if len(argv) < 7 or argv[5] != "--":
        print("van-compute-broker: invalid internal child invocation", file=sys.stderr)
        return 125
    try:
        memory, cpu, file_size, nofile, nice = map(int, argv[:5])
        if nice:
            os.nice(nice)
        # Darwin exposes RLIMIT_AS but rejects lowering its synthetic infinity.
        # The broker is deployed on Linux, where this limit is mandatory and
        # complements the service MemoryMax.
        if sys.platform != "darwin":
            _set_limit(resource.RLIMIT_AS, memory)
        _set_limit(resource.RLIMIT_CPU, cpu, cpu + 5)
        _set_limit(resource.RLIMIT_FSIZE, file_size)
        _set_limit(resource.RLIMIT_NOFILE, nofile)
        # RLIMIT_NPROC is counted across every process/thread with the same
        # real UID, not just this job.  The shared pi account routinely owns
        # more tasks than a sensible per-job ceiling, which makes Bubblewrap's
        # initial namespace clone fail with EAGAIN.  The broker service's
        # TasksMax cgroup is the actual per-broker process boundary.
        command = list(argv[6:])
        if not command:
            raise BrokerError("internal child command is empty")
        os.execvpe(command[0], command, os.environ)
    except (BrokerError, OSError, ValueError) as exc:
        print(f"van-compute-broker child: {exc}", file=sys.stderr)
        return 126
    return 126


def child_limits(
    nice: int,
    timeout: int,
    maximum_file_size: int,
    maximum_memory: int,
) -> None:
    # Sandboxed or launchd-managed processes can have immutable hard limits.
    # The parent still enforces wall time and validates result sizes, so a
    # platform refusal here should not prevent an otherwise safe job.
    for operation in (
        lambda: os.nice(nice),
        lambda: resource.setrlimit(resource.RLIMIT_CPU, (timeout + 30, timeout + 60)),
        lambda: resource.setrlimit(
            resource.RLIMIT_FSIZE, (maximum_file_size, maximum_file_size)
        ),
        lambda: resource.setrlimit(
            resource.RLIMIT_AS, (maximum_memory, maximum_memory)
        ),
        lambda: resource.setrlimit(resource.RLIMIT_NOFILE, (256, 256)),
    ):
        try:
            operation()
        except (OSError, ValueError):
            pass


def worker_main(argv: Sequence[str]) -> int:
    """Apply per-job limits in a single-threaded helper, then exec argv."""
    if len(argv) < 6 or argv[4] != "--":
        print("van-compute-worker: invalid internal child invocation", file=sys.stderr)
        return 125
    try:
        nice, timeout, maximum_file_size, maximum_memory = map(int, argv[:4])
        child_pythonpath = os.environ.pop("VAN_COMPUTE_CHILD_PYTHONPATH", None)
        child_limits(nice, timeout, maximum_file_size, maximum_memory)
        command = list(argv[5:])
        if not command:
            raise WorkerError("internal child command is empty")
        # The helper interpreter must not start with untrusted snapshotted
        # source on sys.path: sitecustomize would run before limits/sandboxing.
        # Add it only at the final exec boundary, where sandbox-exec comes first.
        if child_pythonpath is not None:
            os.environ["PYTHONPATH"] = child_pythonpath
        os.execvpe(command[0], command, os.environ)
    except (OSError, ValueError, WorkerError) as exc:
        print(f"van-compute-worker child: {exc}", file=sys.stderr)
        return 126
    return 126


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(argv) if argv is not None else sys.argv[1:]
    if arguments and arguments[0] == "broker":
        return broker_main(arguments[1:])
    if arguments and arguments[0] == "worker":
        return worker_main(arguments[1:])
    print("van-compute child: expected broker or worker host token", file=sys.stderr)
    return 125


if __name__ == "__main__":
    raise SystemExit(main())
