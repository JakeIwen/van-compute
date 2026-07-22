#!/usr/bin/env python3
"""Submit and manage offline analysis jobs for an opportunistic Mac worker.

The queue lives on vanpi.  A Mac worker connects *to* the Pi, claims one job,
streams the bounded input snapshot, executes an allowlisted offline task, and
streams result files back.  Nothing in this program opens or configures CAN.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import stat
import sys
import tempfile
import time
from typing import BinaryIO, Iterator, Sequence


def _import_protocol():
    try:
        from shared.python import van_compute_protocol as protocol

        return protocol
    except ModuleNotFoundError:
        deployed = Path(__file__).resolve().parent / "python-automation"
        sys.path.insert(0, str(deployed))
        import van_compute_protocol as protocol

        return protocol


protocol = _import_protocol()

DEFAULT_SOURCE_ROOT = Path("/home/pi/dev/obd-things")
DEFAULT_QUEUE_ROOT = DEFAULT_SOURCE_ROOT / "tmp" / "compute"
STATE_DIRECTORIES = ("queued", "running", "done", "failed")
JOB_ID_RE = re.compile(r"\d{8}T\d{6}Z-[0-9a-f]{8}")
WORKER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
RESULT_PATH_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*(?:/[A-Za-z0-9][A-Za-z0-9_.-]*)*")
COPY_CHUNK = 1024 * 1024
DEFAULT_HEARTBEAT_MAX_AGE = 45.0
DEFAULT_MAX_RESULT_BYTES = 128 * 1024 * 1024


class QueueError(RuntimeError):
    pass


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def parse_timestamp(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def atomic_json(path: Path, payload: object, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def load_json(path: Path) -> dict[str, object]:
    try:
        with path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise QueueError(f"cannot read {path}: {exc}") from None
    if not isinstance(payload, dict):
        raise QueueError(f"{path} does not contain a JSON object")
    return payload


def safe_root(path: Path) -> Path:
    expanded = path.expanduser()
    if expanded.is_symlink():
        raise QueueError(f"queue root cannot be a symlink: {expanded}")
    path = expanded.resolve()
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink() or not path.is_dir():
        raise QueueError(f"queue root is not a real directory: {path}")
    os.chmod(path, 0o700)
    for name in STATE_DIRECTORIES:
        (path / name).mkdir(mode=0o700, exist_ok=True)
    (path / "workers").mkdir(mode=0o700, exist_ok=True)
    return path


@contextmanager
def queue_lock(root: Path) -> Iterator[None]:
    with (root / ".queue.lock").open("a+") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def validate_job_id(job_id: str) -> str:
    if not JOB_ID_RE.fullmatch(job_id):
        raise QueueError(f"invalid job id {job_id!r}")
    return job_id


def validate_worker(worker: str) -> str:
    if not WORKER_RE.fullmatch(worker):
        raise QueueError(f"invalid worker name {worker!r}")
    return worker


def validate_relative_result(path: str) -> Path:
    if not RESULT_PATH_RE.fullmatch(path):
        raise QueueError(f"invalid result path {path!r}")
    relative = Path(path)
    if relative.is_absolute() or ".." in relative.parts:
        raise QueueError(f"invalid result path {path!r}")
    return relative


def job_location(root: Path, job_id: str) -> tuple[str, Path] | None:
    validate_job_id(job_id)
    for state in STATE_DIRECTORIES:
        candidate = root / state / job_id
        if candidate.is_dir():
            return state, candidate
    return None


def require_running_job(root: Path, job_id: str, worker: str) -> tuple[Path, dict[str, object]]:
    validate_job_id(job_id)
    validate_worker(worker)
    path = root / "running" / job_id
    if not path.is_dir():
        raise QueueError(f"job {job_id} is not running")
    manifest = load_json(path / "manifest.json")
    if manifest.get("worker") != worker:
        raise QueueError(f"job {job_id} belongs to a different worker")
    return path, manifest


def is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(COPY_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def copy_source_file(source: Path, destination: Path) -> dict[str, object]:
    if source.is_symlink():
        raise QueueError(f"source snapshot refuses symlink: {source}")
    source_stat = source.stat()
    if not stat.S_ISREG(source_stat.st_mode):
        raise QueueError(f"source snapshot requires a regular file: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    shutil.copyfile(source, destination)
    os.chmod(destination, 0o600)
    return {
        "path": destination.as_posix(),
        "size": destination.stat().st_size,
        "sha256": hash_file(destination),
    }


def snapshot_sources(task, source_root: Path, destination: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    ignored_directories = {".git", "tmp", "__pycache__", ".pytest_cache"}
    for relative_text in task.source_paths:
        relative = Path(relative_text)
        unresolved_source = source_root / relative
        if unresolved_source.is_symlink():
            raise QueueError(f"source snapshot refuses symlink: {unresolved_source}")
        source = unresolved_source.resolve()
        if not is_within(source, source_root) or not source.exists():
            raise QueueError(f"required task source is missing or outside the repo: {relative_text}")
        if source.is_symlink():
            raise QueueError(f"source snapshot refuses symlink: {source}")
        if source.is_file():
            target = destination / relative
            record = copy_source_file(source, target)
            record["path"] = relative.as_posix()
            records.append(record)
            continue
        for child in sorted(source.rglob("*")):
            child_relative = child.relative_to(source_root)
            if any(part in ignored_directories for part in child_relative.parts):
                continue
            if child.is_symlink():
                raise QueueError(f"source snapshot refuses symlink: {child}")
            if child.is_file() and child.suffix != ".pyc":
                record = copy_source_file(child, destination / child_relative)
                record["path"] = child_relative.as_posix()
                records.append(record)
    if not records:
        raise QueueError(f"no source files were captured for {task.name}")
    return records


def input_record(path_text: str, value: str | None, source_root: Path, index: int) -> dict[str, object]:
    unresolved_path = Path(path_text).expanduser()
    if unresolved_path.is_symlink():
        raise QueueError(f"input cannot be a symlink: {unresolved_path}")
    path = unresolved_path.resolve()
    if not is_within(path, source_root):
        raise QueueError(f"input must be inside {source_root}: {path}")
    if path.is_symlink():
        raise QueueError(f"input cannot be a symlink: {path}")
    try:
        info = path.stat()
    except OSError as exc:
        raise QueueError(f"cannot inspect input {path}: {exc}") from None
    if not stat.S_ISREG(info.st_mode):
        raise QueueError(f"input is not a regular file: {path}")
    record: dict[str, object] = {
        "index": index,
        "name": path.name,
        "remote_path": str(path),
        "size": info.st_size,
        "device": info.st_dev,
        "inode": info.st_ino,
        "mtime_ns": info.st_mtime_ns,
    }
    if value is not None:
        record["value"] = value
    return record


def submit_job(args: argparse.Namespace) -> dict[str, object]:
    root = safe_root(args.root)
    source_root = args.source_root.expanduser().resolve()
    if not source_root.is_dir():
        raise QueueError(f"source root does not exist: {source_root}")
    task = protocol.get_task(args.task)
    arguments = protocol.validate_task_arguments(task.name, args.argument)
    if args.input_value and len(args.input_value) != len(args.input):
        raise QueueError("--input-value must be supplied once per --input, or not at all")
    values = args.input_value or [None] * len(args.input)
    inputs = [
        input_record(path, value, source_root, index)
        for index, (path, value) in enumerate(zip(args.input, values))
    ]
    protocol.validate_inputs(task.name, inputs)

    job_id = f"{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}-{secrets.token_hex(4)}"
    staging = root / "queued" / f".staging-{job_id}"
    final = root / "queued" / job_id
    staging.mkdir(mode=0o700)
    try:
        sources = snapshot_sources(task, source_root, staging / "source")
        manifest: dict[str, object] = {
            "schema_version": protocol.SCHEMA_VERSION,
            "id": job_id,
            "task": task.name,
            "description": task.description,
            "state": "queued",
            "submitted_at": utc_now(),
            "source_root": str(source_root),
            "sources": sources,
            "inputs": inputs,
            "arguments": list(arguments),
        }
        atomic_json(staging / "manifest.json", manifest)
        os.replace(staging, final)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return manifest


def manifest_for(root: Path, job_id: str) -> dict[str, object]:
    found = job_location(root, job_id)
    if found is None:
        raise QueueError(f"unknown job {job_id}")
    _state, path = found
    return load_json(path / "manifest.json")


def stream_result(root: Path, job_id: str, relative_text: str, output: BinaryIO) -> None:
    found = job_location(root, job_id)
    if found is None:
        raise QueueError(f"unknown job {job_id}")
    state, job_path = found
    if state not in {"done", "failed"}:
        raise QueueError(f"job {job_id} has not finished")
    relative = validate_relative_result(relative_text)
    manifest = load_json(job_path / "manifest.json")
    results = manifest.get("results")
    allowed = {
        str(item.get("path")): item
        for item in results if isinstance(item, dict)
    } if isinstance(results, list) else {}
    if relative.as_posix() not in allowed:
        raise QueueError(f"job has no result named {relative_text!r}")
    path = job_path / "result" / relative
    if path.is_symlink() or not path.is_file():
        raise QueueError(f"result file is missing: {relative_text}")
    if hash_file(path) != allowed[relative.as_posix()].get("sha256"):
        raise QueueError(f"result hash mismatch: {relative_text}")
    with path.open("rb") as handle:
        shutil.copyfileobj(handle, output, COPY_CHUNK)


def list_jobs(root: Path, limit: int) -> list[dict[str, object]]:
    jobs: list[dict[str, object]] = []
    for state in STATE_DIRECTORIES:
        for path in (root / state).iterdir():
            if path.is_dir() and JOB_ID_RE.fullmatch(path.name):
                try:
                    jobs.append(load_json(path / "manifest.json"))
                except QueueError:
                    continue
    jobs.sort(key=lambda item: str(item.get("submitted_at", "")), reverse=True)
    return jobs[:limit]


def worker_heartbeat(root: Path, worker: str) -> dict[str, object]:
    validate_worker(worker)
    payload = {"worker": worker, "seen_at": utc_now(), "available": True}
    atomic_json(root / "workers" / f"{worker}.json", payload)
    return payload


def workers_available(root: Path, max_age: float) -> list[dict[str, object]]:
    now = dt.datetime.now(dt.timezone.utc)
    workers: list[dict[str, object]] = []
    for path in (root / "workers").glob("*.json"):
        try:
            payload = load_json(path)
            seen = parse_timestamp(str(payload["seen_at"]))
        except (QueueError, KeyError, ValueError):
            continue
        age = max(0.0, (now - seen).total_seconds())
        payload["age_seconds"] = round(age, 3)
        payload["available"] = age <= max_age
        workers.append(payload)
    workers.sort(key=lambda item: str(item.get("worker", "")))
    return workers


def worker_claim(root: Path, worker: str) -> dict[str, object] | None:
    validate_worker(worker)
    worker_heartbeat(root, worker)
    with queue_lock(root):
        # launchd does not overlap invocations of the same job.  If a prior
        # invocation lost power or connectivity after claiming, returning its
        # job lets the next invocation safely restage and retry it.
        for path in sorted((root / "running").iterdir()):
            if not path.is_dir() or not JOB_ID_RE.fullmatch(path.name):
                continue
            manifest = load_json(path / "manifest.json")
            if manifest.get("worker") == worker:
                manifest["resumed_at"] = utc_now()
                atomic_json(path / "manifest.json", manifest)
                return manifest
        queued = sorted(
            path for path in (root / "queued").iterdir()
            if path.is_dir() and JOB_ID_RE.fullmatch(path.name)
        )
        if not queued:
            return None
        source = queued[0]
        destination = root / "running" / source.name
        os.replace(source, destination)
        manifest = load_json(destination / "manifest.json")
        manifest.update({"state": "running", "worker": worker, "started_at": utc_now()})
        atomic_json(destination / "manifest.json", manifest)
        return manifest


def stream_bounded_input(manifest: dict[str, object], index: int, output: BinaryIO) -> None:
    inputs = manifest.get("inputs")
    if not isinstance(inputs, list) or not 0 <= index < len(inputs):
        raise QueueError(f"input index {index} is out of range")
    record = inputs[index]
    if not isinstance(record, dict):
        raise QueueError("input record is invalid")
    path = Path(str(record["remote_path"]))
    info = path.stat()
    if path.is_symlink() or not stat.S_ISREG(info.st_mode):
        raise QueueError(f"input is no longer a regular file: {path}")
    if info.st_dev != record.get("device") or info.st_ino != record.get("inode"):
        raise QueueError(f"input was replaced after submission: {path}")
    remaining = int(record["size"])
    if info.st_size < remaining:
        raise QueueError(f"input shrank after submission: {path}")
    with path.open("rb") as handle:
        while remaining:
            chunk = handle.read(min(COPY_CHUNK, remaining))
            if not chunk:
                raise QueueError(f"input ended before its submitted size: {path}")
            output.write(chunk)
            remaining -= len(chunk)


def stream_source(job_path: Path, manifest: dict[str, object], relative_text: str, output: BinaryIO) -> None:
    sources = manifest.get("sources")
    allowed = {
        str(item.get("path")): item
        for item in sources if isinstance(item, dict)
    } if isinstance(sources, list) else {}
    if relative_text not in allowed:
        raise QueueError(f"source file is not in the job manifest: {relative_text}")
    relative = Path(relative_text)
    if relative.is_absolute() or ".." in relative.parts:
        raise QueueError("invalid source path")
    path = job_path / "source" / relative
    if path.is_symlink() or not path.is_file():
        raise QueueError(f"source snapshot is missing: {relative_text}")
    if hash_file(path) != allowed[relative_text].get("sha256"):
        raise QueueError(f"source snapshot hash mismatch: {relative_text}")
    with path.open("rb") as handle:
        shutil.copyfileobj(handle, output, COPY_CHUNK)


def put_result(root: Path, job_id: str, worker: str, relative_text: str, input_stream: BinaryIO) -> dict[str, object]:
    job_path, _manifest = require_running_job(root, job_id, worker)
    relative = validate_relative_result(relative_text)
    destination = job_path / "result" / relative
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
    temporary = Path(temporary_name)
    digest = hashlib.sha256()
    size = 0
    maximum = int(os.environ.get("VAN_COMPUTE_MAX_RESULT_BYTES", DEFAULT_MAX_RESULT_BYTES))
    try:
        with os.fdopen(descriptor, "wb") as handle:
            while chunk := input_stream.read(COPY_CHUNK):
                size += len(chunk)
                if size > maximum:
                    raise QueueError(f"result exceeds {maximum} bytes")
                digest.update(chunk)
                handle.write(chunk)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return {"path": relative.as_posix(), "size": size, "sha256": digest.hexdigest()}


def worker_finish(root: Path, job_id: str, worker: str, exit_code: int, result_files: Sequence[str]) -> dict[str, object]:
    with queue_lock(root):
        job_path, manifest = require_running_job(root, job_id, worker)
        verified: list[dict[str, object]] = []
        for relative_text in result_files:
            relative = validate_relative_result(relative_text)
            path = job_path / "result" / relative
            if path.is_symlink() or not path.is_file():
                raise QueueError(f"declared result is missing: {relative_text}")
            verified.append(
                {"path": relative.as_posix(), "size": path.stat().st_size, "sha256": hash_file(path)}
            )
        state = "done" if exit_code == 0 else "failed"
        manifest.update(
            {
                "state": state,
                "finished_at": utc_now(),
                "exit_code": exit_code,
                "results": verified,
            }
        )
        atomic_json(job_path / "manifest.json", manifest)
        os.replace(job_path, root / state / job_id)
        worker_heartbeat(root, worker)
        return manifest


def wait_for_job(root: Path, job_id: str, timeout: float, interval: float = 1.0) -> dict[str, object]:
    deadline = time.monotonic() + timeout
    while True:
        manifest = manifest_for(root, job_id)
        if manifest.get("state") in {"done", "failed"}:
            return manifest
        if time.monotonic() >= deadline:
            manifest = dict(manifest)
            manifest["wait_timed_out"] = True
            return manifest
        time.sleep(min(interval, max(0.0, deadline - time.monotonic())))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(os.environ.get("VAN_COMPUTE_ROOT", DEFAULT_QUEUE_ROOT)),
        help="queue root (default: %(default)s)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("tasks", help="list allowlisted offline tasks")

    submit = subparsers.add_parser("submit", help="submit an offline job")
    submit.add_argument("task", choices=sorted(protocol.TASKS))
    submit.add_argument("--source-root", type=Path, default=Path(os.environ.get("VAN_COMPUTE_SOURCE_ROOT", DEFAULT_SOURCE_ROOT)))
    submit.add_argument("--input", action="append", required=True, help="input file inside source-root; repeat as needed")
    submit.add_argument("--input-value", action="append", help="optional scalar paired with each input")
    submit.add_argument("--arg", dest="argument", action="append", default=[], help="allowlisted task argument; use --arg=--option")
    submit.add_argument("--wait", type=float, metavar="SECONDS", help="wait up to this long for completion")

    status_parser = subparsers.add_parser("status", help="show one job")
    status_parser.add_argument("job_id")

    list_parser = subparsers.add_parser("list", help="list recent jobs")
    list_parser.add_argument("--limit", type=int, default=20)

    available = subparsers.add_parser("available", help="show recent worker heartbeats")
    available.add_argument("--max-age", type=float, default=DEFAULT_HEARTBEAT_MAX_AGE)

    wait_parser = subparsers.add_parser("wait", help="wait for a submitted job")
    wait_parser.add_argument("job_id")
    wait_parser.add_argument("--timeout", type=float, default=3600)

    result_parser = subparsers.add_parser("result", help="write one completed result file to stdout")
    result_parser.add_argument("job_id")
    result_parser.add_argument("path")

    worker = subparsers.add_parser("worker", help=argparse.SUPPRESS)
    worker_subparsers = worker.add_subparsers(dest="worker_command", required=True)
    for name in ("heartbeat", "claim"):
        item = worker_subparsers.add_parser(name)
        item.add_argument("--worker", required=True)
    stream = worker_subparsers.add_parser("stream")
    stream.add_argument("job_id")
    stream.add_argument("--worker", required=True)
    stream.add_argument("--kind", required=True, choices=("input", "source"))
    stream.add_argument("--index", type=int)
    stream.add_argument("--path")
    put = worker_subparsers.add_parser("put-result")
    put.add_argument("job_id")
    put.add_argument("--worker", required=True)
    put.add_argument("--path", required=True)
    finish = worker_subparsers.add_parser("finish")
    finish.add_argument("job_id")
    finish.add_argument("--worker", required=True)
    finish.add_argument("--exit-code", required=True, type=int)
    finish.add_argument("--result-file", action="append", default=[])
    return parser


def print_json(payload: object) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "tasks":
            print_json(
                {
                    "schema_version": protocol.SCHEMA_VERSION,
                    "tasks": [
                        {"name": task.name, "description": task.description}
                        for task in protocol.TASKS.values()
                    ],
                }
            )
            return 0

        root = safe_root(args.root)
        if args.command == "submit":
            manifest = submit_job(args)
            if args.wait is not None:
                if args.wait < 0:
                    raise QueueError("--wait must be non-negative")
                manifest = wait_for_job(root, str(manifest["id"]), args.wait)
            print_json(manifest)
            return 0 if manifest.get("state") != "failed" else 1
        if args.command == "status":
            print_json(manifest_for(root, args.job_id))
            return 0
        if args.command == "list":
            if not 1 <= args.limit <= 1000:
                raise QueueError("--limit must be from 1 through 1000")
            print_json({"jobs": list_jobs(root, args.limit)})
            return 0
        if args.command == "available":
            if args.max_age <= 0:
                raise QueueError("--max-age must be positive")
            workers = workers_available(root, args.max_age)
            print_json({"available": any(item["available"] for item in workers), "workers": workers})
            return 0
        if args.command == "wait":
            if args.timeout < 0:
                raise QueueError("--timeout must be non-negative")
            manifest = wait_for_job(root, args.job_id, args.timeout)
            print_json(manifest)
            return 0 if manifest.get("state") != "failed" else 1
        if args.command == "result":
            stream_result(root, args.job_id, args.path, sys.stdout.buffer)
            return 0
        if args.command == "worker":
            if args.worker_command == "heartbeat":
                print_json(worker_heartbeat(root, args.worker))
                return 0
            if args.worker_command == "claim":
                print_json({"job": worker_claim(root, args.worker)})
                return 0
            if args.worker_command == "stream":
                job_path, manifest = require_running_job(root, args.job_id, args.worker)
                if args.kind == "input":
                    if args.index is None or args.path is not None:
                        raise QueueError("input streaming requires --index only")
                    stream_bounded_input(manifest, args.index, sys.stdout.buffer)
                else:
                    if args.path is None or args.index is not None:
                        raise QueueError("source streaming requires --path only")
                    stream_source(job_path, manifest, args.path, sys.stdout.buffer)
                return 0
            if args.worker_command == "put-result":
                record = put_result(root, args.job_id, args.worker, args.path, sys.stdin.buffer)
                print_json(record)
                return 0
            if args.worker_command == "finish":
                print_json(worker_finish(root, args.job_id, args.worker, args.exit_code, args.result_file))
                return 0
    except (OSError, QueueError, protocol.ProtocolError) as exc:
        print(f"van-compute: {exc}", file=sys.stderr)
        return 2
    parser.error("unhandled command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
