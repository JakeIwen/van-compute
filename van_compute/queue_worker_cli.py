"""The hidden `queue.py worker ...` RPC verbs a remote worker calls over SSH.

The Mac worker runs these on the Pi; their argument names and stdout bytes are
the worker protocol (pinned by the golden fixtures), so changes here are
additive only.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Callable

from van_compute import queue


def add_worker_parser(subparsers: argparse._SubParsersAction) -> None:
    worker = subparsers.add_parser("worker", help=argparse.SUPPRESS)
    worker_subparsers = worker.add_subparsers(dest="worker_command", required=True)
    for name in ("heartbeat", "claim"):
        item = worker_subparsers.add_parser(name)
        item.add_argument("--worker", required=True)
        item.add_argument("--protocol-version", required=True, type=int)
        if name == "heartbeat":
            item.add_argument("--slots-total", type=int)
            item.add_argument("--slots-busy", type=int)
        else:
            item.add_argument("--placement", choices=queue.JOB_PLACEMENTS, default="remote")
    stream = worker_subparsers.add_parser("stream")
    stream.add_argument("job_id")
    stream.add_argument("--worker", required=True)
    stream.add_argument("--kind", required=True, choices=("input", "source-bundle"))
    stream.add_argument("--index", type=int)
    stream.add_argument("--lease-token")
    put = worker_subparsers.add_parser("put-result")
    put.add_argument("job_id")
    put.add_argument("--worker", required=True)
    put.add_argument("--path", required=True)
    put.add_argument("--lease-token")
    finish = worker_subparsers.add_parser("finish")
    finish.add_argument("job_id")
    finish.add_argument("--worker", required=True)
    finish.add_argument("--exit-code", required=True, type=int)
    finish.add_argument("--result-file", action="append", default=[])
    finish.add_argument("--lease-token")


def _stream(root: Path, args: argparse.Namespace) -> int:
    job_path, manifest = queue.require_running_job(
        root,
        args.job_id,
        args.worker,
        args.lease_token,
    )
    if args.kind == "input":
        if args.index is None:
            raise queue.QueueError("input streaming requires --index")
        queue.stream_bounded_input(
            manifest,
            args.index,
            sys.stdout.buffer,
            args.lease_token,
        )
    else:
        if args.index is not None:
            raise queue.QueueError("source-bundle streaming accepts no --index")
        queue.stream_source_bundle(
            job_path,
            manifest,
            sys.stdout.buffer,
            args.lease_token,
        )
    return 0


def run_worker_command(
    root: Path, args: argparse.Namespace, print_json: Callable[[object], None]
) -> int:
    """Dispatch one worker verb; errors propagate to `queue.main`'s handler."""
    command = args.worker_command
    if command == "heartbeat":
        print_json(
            queue.worker_heartbeat(
                root,
                args.worker,
                args.slots_total,
                args.slots_busy,
                protocol_version=args.protocol_version,
            )
        )
        return 0
    if command == "claim":
        print_json(
            {
                "job": queue.worker_claim(
                    root,
                    args.worker,
                    args.placement,
                    protocol_version=args.protocol_version,
                )
            }
        )
        return 0
    if command == "stream":
        return _stream(root, args)
    if command == "put-result":
        record = queue.put_result(
            root,
            args.job_id,
            args.worker,
            args.path,
            sys.stdin.buffer,
            args.lease_token,
        )
        print_json(record)
        return 0
    if command == "finish":
        print_json(
            queue.worker_finish(
                root,
                args.job_id,
                args.worker,
                args.exit_code,
                args.result_file,
                args.lease_token,
            )
        )
        return 0
    raise queue.QueueError(f"unknown worker command {command!r}")
