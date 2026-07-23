#!/usr/bin/env python3
"""Small agent-facing frontend for the van-compute queue.

This command only submits named, protocol-validated tasks and reads queue
results.  It never probes workers or chooses whether work runs on the Mac or
the Pi; van-compute-broker owns that placement decision.
"""

from __future__ import annotations

import argparse
from io import BytesIO
import json
import math
import os
from pathlib import Path
import sys
from typing import Sequence


def _import_queue():
    if __package__:
        from pi.van_compute.scripts import van_compute as queue_module
    else:
        import van_compute as queue_module

    return queue_module


queue = _import_queue()
protocol = queue.protocol
MAX_WAIT_SECONDS = 24 * 60 * 60
MAX_INLINE_OUTPUT = 256 * 1024


class LimitedCapture:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.buffer = BytesIO()
        self.total = 0

    def write(self, value: bytes) -> int:
        self.total += len(value)
        remaining = self.limit - self.buffer.tell()
        if remaining > 0:
            self.buffer.write(value[:remaining])
        return len(value)


def _validate_wait(value: float, option: str) -> float:
    if not math.isfinite(value) or not 0 <= value <= MAX_WAIT_SECONDS:
        raise queue.QueueError(f"{option} must be from 0 through {MAX_WAIT_SECONDS} seconds")
    return value


def _present_manifest(
    root: Path,
    manifest: dict[str, object],
    *,
    include_output: bool,
) -> dict[str, object]:
    payload = queue.public_manifest(manifest)
    results = manifest.get("results")
    paths = [
        str(item.get("path"))
        for item in results
        if isinstance(item, dict) and isinstance(item.get("path"), str)
    ] if isinstance(results, list) else []
    if paths:
        payload["result_paths"] = paths
        job_id = str(manifest.get("id", "JOB_ID"))
        payload["retrieve"] = {
            path: f"/home/pi/van_compute/scripts/pi_compute.py result {job_id} {path}"
            for path in paths
        }
    if include_output:
        inline: dict[str, object] = {}
        for name in ("stdout.txt", "stderr.txt"):
            if name not in paths:
                continue
            capture = LimitedCapture(MAX_INLINE_OUTPUT)
            queue.stream_result(root, str(manifest["id"]), name, capture)
            inline[name] = capture.buffer.getvalue().decode("utf-8", "replace")
            if capture.total > MAX_INLINE_OUTPUT:
                inline[f"{name}_truncated"] = True
        payload["output"] = inline
    return payload


def print_json(payload: object) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(os.environ.get("VAN_COMPUTE_ROOT", queue.DEFAULT_QUEUE_ROOT)),
        help=argparse.SUPPRESS,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run", help="enqueue one named offline task")
    run.add_argument("task")
    run.add_argument(
        "--source-root",
        type=Path,
        default=Path(os.environ.get("VAN_COMPUTE_SOURCE_ROOT", queue.DEFAULT_SOURCE_ROOT)),
    )
    run.add_argument("--input", action="append", default=[])
    run.add_argument("--input-value", action="append")
    run.add_argument(
        "--arg",
        dest="argument",
        action="append",
        default=[],
        help="task argument; use --arg=--option for values beginning with a dash",
    )
    run.add_argument(
        "--wait",
        type=float,
        nargs="?",
        const=3600.0,
        metavar="SECONDS",
        help="wait for completion (default 3600 seconds when flag has no value)",
    )
    run.add_argument(
        "--stdout",
        action="store_true",
        help="include bounded stdout/stderr text when the waited job completes",
    )

    tasks = subparsers.add_parser("tasks", help="list named tasks the repository exposes")
    tasks.add_argument(
        "--source-root",
        type=Path,
        default=Path(os.environ.get("VAN_COMPUTE_SOURCE_ROOT", queue.DEFAULT_SOURCE_ROOT)),
    )

    status = subparsers.add_parser("status", help="show one queued or completed job")
    status.add_argument("job_id")

    wait = subparsers.add_parser("wait", help="wait for an existing job")
    wait.add_argument("job_id")
    wait.add_argument("--timeout", type=float, default=3600.0)
    wait.add_argument("--stdout", action="store_true")

    result = subparsers.add_parser("result", help="write one result file to stdout")
    result.add_argument("job_id")
    result.add_argument("path")

    listing = subparsers.add_parser("list", help="list recent jobs")
    listing.add_argument("--limit", type=int, default=20)
    return parser


def _task_listing(source_root: Path) -> dict[str, object]:
    source_root = source_root.expanduser().resolve()
    repo_tasks = protocol.load_repo_tasks(source_root) if source_root.is_dir() else {}
    tasks = [*protocol.TASKS.values(), *repo_tasks.values()]
    return {
        "tasks": [
            {
                "name": task.name,
                "description": task.description,
                "minimum_inputs": task.minimum_inputs,
                "maximum_inputs": task.maximum_inputs,
                "input_values": task.input_values,
                "origin": (
                    "repository"
                    if isinstance(task, protocol.RepoTaskDefinition)
                    else "built-in"
                ),
                **(
                    {
                        "profile": task.profile,
                        "argv_template": list(task.argv),
                        "outputs": list(task.outputs),
                        "datasets": list(task.datasets),
                        "accepts_arguments": "{arguments}" in task.argv,
                    }
                    if isinstance(task, protocol.RepoTaskDefinition)
                    else {
                        "outputs": [task.result_json] if task.result_json else [],
                        "datasets": [],
                        "accepts_arguments": True,
                    }
                ),
            }
            for task in tasks
        ]
    }


def main(argv: Sequence[str] | None = None) -> int:
    os.umask(0o077)
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "tasks":
            print_json(_task_listing(args.source_root))
            return 0

        root = queue.safe_root(args.root)
        if args.command == "run":
            if args.wait is not None:
                _validate_wait(args.wait, "--wait")
            manifest = queue.submit_job(args)
            if args.wait is not None:
                manifest = queue.wait_for_job(root, str(manifest["id"]), args.wait)
            print_json(_present_manifest(root, manifest, include_output=args.stdout))
            if manifest.get("wait_timed_out"):
                return 3
            return 1 if manifest.get("state") == "failed" else 0
        if args.command == "status":
            print_json(
                _present_manifest(
                    root,
                    queue.manifest_for(root, args.job_id),
                    include_output=False,
                )
            )
            return 0
        if args.command == "wait":
            _validate_wait(args.timeout, "--timeout")
            manifest = queue.wait_for_job(root, args.job_id, args.timeout)
            print_json(_present_manifest(root, manifest, include_output=args.stdout))
            if manifest.get("wait_timed_out"):
                return 3
            return 1 if manifest.get("state") == "failed" else 0
        if args.command == "list":
            if not 1 <= args.limit <= 1000:
                raise queue.QueueError("--limit must be from 1 through 1000")
            print_json(
                {
                    "jobs": [
                        queue.public_manifest(item)
                        for item in queue.list_jobs(root, args.limit)
                    ]
                }
            )
            return 0
        if args.command == "result":
            queue.stream_result(root, args.job_id, args.path, sys.stdout.buffer)
            return 0
        raise queue.QueueError(f"unknown command {args.command!r}")
    except (queue.QueueError, protocol.ProtocolError, OSError) as exc:
        print(f"pi-compute: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
