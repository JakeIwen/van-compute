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
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import stat
import sys
import tarfile
import tempfile
import time
from typing import BinaryIO, Iterator, Sequence


def _import_protocol():
    if __package__:
        from pi.van_compute.scripts import van_compute_protocol as protocol
    else:
        import van_compute_protocol as protocol

    return protocol


protocol = _import_protocol()

DEFAULT_SOURCE_ROOT = Path("/home/pi/dev/obd-things")
DEFAULT_QUEUE_ROOT = DEFAULT_SOURCE_ROOT / "tmp" / "compute"
STATE_DIRECTORIES = ("queued", "running", "done", "failed")
JOB_ID_RE = re.compile(r"\d{8}T\d{6}Z-[0-9a-f]{8}")
WORKER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
LEASE_TOKEN_RE = re.compile(r"[0-9a-f]{32}")
RESULT_PATH_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*(?:/[A-Za-z0-9][A-Za-z0-9_.-]*)*")
COPY_CHUNK = 1024 * 1024
DEFAULT_HEARTBEAT_MAX_AGE = 45.0
DEFAULT_MAX_RESULT_BYTES = protocol.MAX_RESULT_BYTES
MAINTENANCE_FILE = ".maintenance.json"
MAX_SNAPSHOT_FILES = 10_000
MAX_SNAPSHOT_BYTES = 256 * 1024 * 1024
MAX_MISSED_EVENTS = 2000
MAX_MISSED_LABEL = 300
MISSED_REASONS = (
    "worker-unavailable",
    "unsupported",
    "queue-busy",
    "failed-offload",
    "agent-choice",
    "other",
)
JOB_PLACEMENTS = ("remote", "pi-local")


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
    for name in (*STATE_DIRECTORIES, "workers", "missed", "uploads"):
        child = path / name
        child.mkdir(mode=0o700, exist_ok=True)
        if child.is_symlink() or not child.is_dir():
            raise QueueError(f"queue state directory is not a real directory: {child}")
        os.chmod(child, 0o700)
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


def require_worker_protocol_version(version: int) -> int:
    expected = protocol.WORKER_PROTOCOL_VERSION
    if isinstance(version, bool) or not isinstance(version, int) or version != expected:
        raise QueueError(
            f"incompatible worker protocol version {version!r}; expected {expected}"
        )
    return version


def maintenance_status(root: Path) -> dict[str, object] | None:
    path = root / MAINTENANCE_FILE
    if path.is_symlink():
        raise QueueError("queue maintenance marker cannot be a symlink")
    if not path.exists():
        return None
    payload = load_json(path)
    owner = payload.get("owner")
    if not isinstance(owner, str) or not WORKER_RE.fullmatch(owner):
        raise QueueError("queue maintenance marker has an invalid owner")
    return payload


def set_maintenance(root: Path, owner: str, enabled: bool) -> dict[str, object]:
    validate_worker(owner)
    with queue_lock(root):
        current = maintenance_status(root)
        path = root / MAINTENANCE_FILE
        if enabled:
            if current is not None and current.get("owner") != owner:
                raise QueueError(
                    f"queue maintenance is already owned by {current.get('owner')}"
                )
            payload = {
                "active": True,
                "owner": owner,
                "updated_at": utc_now(),
            }
            atomic_json(path, payload)
            return payload
        if current is None:
            return {"active": False, "owner": owner}
        if current.get("owner") != owner:
            raise QueueError(
                f"queue maintenance belongs to {current.get('owner')}"
            )
        path.unlink()
        return {"active": False, "owner": owner, "updated_at": utc_now()}


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
        if not candidate.is_symlink() and candidate.is_dir():
            return state, candidate
    return None


def validate_lease_token(lease_token: str) -> str:
    if not isinstance(lease_token, str) or not LEASE_TOKEN_RE.fullmatch(lease_token):
        raise QueueError("invalid lease token")
    return lease_token


def require_manifest_lease(
    manifest: dict[str, object],
    lease_token: str | None,
) -> None:
    expected = manifest.get("lease_token")
    if expected is None:
        if lease_token is not None:
            validate_lease_token(lease_token)
        return
    if not isinstance(expected, str) or not LEASE_TOKEN_RE.fullmatch(expected):
        raise QueueError("running job has an invalid lease token")
    if lease_token is None:
        raise QueueError("lease token is required for this job")
    validate_lease_token(lease_token)
    if not secrets.compare_digest(expected, lease_token):
        raise QueueError("lease token does not own this job")


def require_running_job(
    root: Path,
    job_id: str,
    worker: str,
    lease_token: str | None = None,
) -> tuple[Path, dict[str, object]]:
    validate_job_id(job_id)
    validate_worker(worker)
    path = root / "running" / job_id
    if path.is_symlink() or not path.is_dir():
        raise QueueError(f"job {job_id} is not running")
    manifest = load_json(path / "manifest.json")
    if manifest.get("worker") != worker:
        raise QueueError(f"job {job_id} belongs to a different worker")
    require_manifest_lease(manifest, lease_token)
    return path, manifest


def is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def verify_open_file_within(
    descriptor: int,
    source_root: Path,
    context: str,
) -> None:
    """On Linux, verify the kernel-resolved opened file remains in source_root."""
    if not sys.platform.startswith("linux"):
        return
    proc_path = Path("/proc/self/fd") / str(descriptor)
    try:
        target_text = os.readlink(proc_path)
    except OSError as exc:
        raise QueueError(
            f"cannot verify opened {context} against {source_root}: {exc}"
        ) from None
    if target_text.endswith(" (deleted)"):
        raise QueueError(f"opened {context} was deleted before it could be verified")
    target = Path(target_text)
    if not target.is_absolute() or not is_within(target, source_root):
        raise QueueError(f"opened {context} escaped source root {source_root}")


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(COPY_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def copy_source_file(
    source: Path,
    destination: Path,
    *,
    source_root: Path,
    maximum_bytes: int,
) -> dict[str, object]:
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        initial_stat = source.lstat()
    except OSError as exc:
        raise QueueError(f"cannot inspect source snapshot {source}: {exc}") from None
    if not stat.S_ISREG(initial_stat.st_mode):
        raise QueueError(f"source snapshot requires a regular file: {source}")
    flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
    try:
        source_descriptor = os.open(source, flags)
    except OSError as exc:
        raise QueueError(f"cannot safely open source snapshot {source}: {exc}") from None
    destination_created = False
    try:
        with os.fdopen(source_descriptor, "rb") as source_handle:
            source_stat = os.fstat(source_handle.fileno())
            if not stat.S_ISREG(source_stat.st_mode):
                raise QueueError(f"source snapshot requires a regular file: {source}")
            if (
                source_stat.st_dev != initial_stat.st_dev
                or source_stat.st_ino != initial_stat.st_ino
            ):
                raise QueueError(f"source changed before being snapshotted: {source}")
            verify_open_file_within(
                source_handle.fileno(), source_root, f"source snapshot {source}"
            )
            if source_stat.st_size > maximum_bytes:
                raise QueueError(
                    f"source snapshot exceeds the {MAX_SNAPSHOT_BYTES}-byte total limit"
                )
            destination_descriptor = os.open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
            destination_created = True
            digest = hashlib.sha256()
            remaining = source_stat.st_size
            with os.fdopen(destination_descriptor, "wb") as destination_handle:
                while remaining:
                    chunk = source_handle.read(min(COPY_CHUNK, remaining))
                    if not chunk:
                        raise QueueError(f"source changed while being snapshotted: {source}")
                    digest.update(chunk)
                    destination_handle.write(chunk)
                    remaining -= len(chunk)
            completed_stat = os.fstat(source_handle.fileno())
            if (
                completed_stat.st_size != source_stat.st_size
                or completed_stat.st_mtime_ns != source_stat.st_mtime_ns
                or completed_stat.st_ctime_ns != source_stat.st_ctime_ns
            ):
                raise QueueError(f"source changed while being snapshotted: {source}")
    except Exception:
        if destination_created:
            destination.unlink(missing_ok=True)
        raise
    return {
        "path": destination.as_posix(),
        "size": source_stat.st_size,
        "sha256": digest.hexdigest(),
    }


def snapshot_sources(task, source_root: Path, destination: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    total_bytes = 0
    ignored_directories = {".git", "tmp", "__pycache__", ".pytest_cache"}
    relative_paths = list(task.source_paths)
    if (
        isinstance(task, protocol.RepoTaskDefinition)
        and protocol.REPO_MANIFEST not in relative_paths
    ):
        # Include the declaration that produced the embedded execution spec so
        # each job has an immutable, hashed audit record of repository policy.
        relative_paths.append(protocol.REPO_MANIFEST)

    def capture(source: Path, relative: Path) -> None:
        nonlocal total_bytes
        if len(records) >= MAX_SNAPSHOT_FILES:
            raise QueueError(
                f"source snapshot exceeds the {MAX_SNAPSHOT_FILES}-file total limit"
            )
        record = copy_source_file(
            source,
            destination / relative,
            source_root=source_root,
            maximum_bytes=MAX_SNAPSHOT_BYTES - total_bytes,
        )
        record["path"] = relative.as_posix()
        records.append(record)
        total_bytes += int(record["size"])

    def walk_error(exc: OSError) -> None:
        raise QueueError(f"cannot walk source snapshot: {exc}")

    for relative_text in relative_paths:
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
            capture(source, relative)
            continue
        if not source.is_dir():
            raise QueueError(f"source snapshot requires a file or directory: {source}")
        for current_text, directory_names, file_names in os.walk(
            source,
            topdown=True,
            followlinks=False,
            onerror=walk_error,
        ):
            current = Path(current_text)
            retained_directories: list[str] = []
            for name in sorted(directory_names):
                child = current / name
                child_relative = child.relative_to(source_root)
                if any(part in ignored_directories for part in child_relative.parts):
                    continue
                if child.is_symlink():
                    raise QueueError(f"source snapshot refuses symlink: {child}")
                retained_directories.append(name)
            directory_names[:] = retained_directories
            for name in sorted(file_names):
                child = current / name
                child_relative = child.relative_to(source_root)
                if any(part in ignored_directories for part in child_relative.parts):
                    continue
                if child.is_symlink():
                    raise QueueError(f"source snapshot refuses symlink: {child}")
                if child.suffix != ".pyc":
                    capture(child, child_relative)
    if not records:
        raise QueueError(f"no source files were captured for {task.name}")
    records.sort(key=lambda record: str(record["path"]))
    return records


def _hash_open_prefix(handle: BinaryIO, size: int) -> str:
    handle.seek(0)
    remaining = size
    digest = hashlib.sha256()
    while remaining:
        chunk = handle.read(min(COPY_CHUNK, remaining))
        if not chunk:
            raise QueueError("input ended while its submitted prefix was fingerprinted")
        digest.update(chunk)
        remaining -= len(chunk)
    return digest.hexdigest()


def input_record(path_text: str, value: str | None, source_root: Path, index: int) -> dict[str, object]:
    unresolved_path = Path(path_text).expanduser()
    if unresolved_path.is_symlink():
        raise QueueError(f"input cannot be a symlink: {unresolved_path}")
    path = unresolved_path.resolve()
    if not is_within(path, source_root):
        raise QueueError(f"input must be inside {source_root}: {path}")
    if path.is_symlink():
        raise QueueError(f"input cannot be a symlink: {path}")
    flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise QueueError(f"cannot safely open input {path}: {exc}") from None
    with os.fdopen(descriptor, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise QueueError(f"input is not a regular file: {path}")
        verify_open_file_within(handle.fileno(), source_root, f"input {path}")
        submitted_size = info.st_size
        digest = _hash_open_prefix(handle, submitted_size)
        after = os.fstat(handle.fileno())
        if (
            after.st_dev != info.st_dev
            or after.st_ino != info.st_ino
            or after.st_size < submitted_size
        ):
            raise QueueError(f"input changed while it was fingerprinted: {path}")
        if (
            after.st_mtime_ns != info.st_mtime_ns
            or after.st_ctime_ns != info.st_ctime_ns
        ):
            # A live capture may append while it is submitted. Re-reading the
            # bounded prefix distinguishes that safe case from an in-place edit.
            if _hash_open_prefix(handle, submitted_size) != digest:
                raise QueueError(f"input prefix changed while it was fingerprinted: {path}")
            final = os.fstat(handle.fileno())
            if (
                final.st_dev != info.st_dev
                or final.st_ino != info.st_ino
                or final.st_size < submitted_size
            ):
                raise QueueError(f"input changed while it was fingerprinted: {path}")
    record: dict[str, object] = {
        "index": index,
        "name": path.name,
        "remote_path": str(path),
        "size": submitted_size,
        "device": info.st_dev,
        "inode": info.st_ino,
        "mtime_ns": info.st_mtime_ns,
        "ctime_ns": info.st_ctime_ns,
        "sha256": digest,
    }
    if value is not None:
        record["value"] = value
    return record


def submit_job(args: argparse.Namespace) -> dict[str, object]:
    source_root = args.source_root.expanduser().resolve()
    if not source_root.is_dir():
        raise QueueError(f"source root does not exist: {source_root}")
    root = safe_root(args.root)
    if maintenance_status(root) is not None:
        raise QueueError("queue maintenance is active; retry submission shortly")
    repo_tasks = protocol.load_repo_tasks(source_root)
    task = protocol.get_task(args.task, repo_tasks)
    arguments = protocol.validate_task_arguments(task.name, args.argument, task)
    if args.input_value and len(args.input_value) != len(args.input):
        raise QueueError("--input-value must be supplied once per --input, or not at all")
    values = args.input_value or [None] * len(args.input)
    inputs = [
        input_record(path, value, source_root, index)
        for index, (path, value) in enumerate(zip(args.input, values))
    ]
    protocol.validate_inputs(task.name, inputs, task)

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
        execution = protocol.task_execution(task)
        if execution is not None:
            manifest["execution"] = execution
        atomic_json(staging / "manifest.json", manifest)
        with queue_lock(root):
            if maintenance_status(root) is not None:
                raise QueueError(
                    "queue maintenance became active; retry submission shortly"
                )
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


def public_manifest(manifest: dict[str, object]) -> dict[str, object]:
    """Return consumer-visible job state without worker ownership capabilities."""
    payload = dict(manifest)
    payload.pop("lease_token", None)
    return payload


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
            if not path.is_symlink() and path.is_dir() and JOB_ID_RE.fullmatch(path.name):
                try:
                    jobs.append(load_json(path / "manifest.json"))
                except QueueError:
                    continue
    jobs.sort(key=lambda item: str(item.get("submitted_at", "")), reverse=True)
    return jobs[:limit]


def _bounded_metric(value: object, name: str, maximum: float) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise QueueError(f"{name} must be a number")
    numeric = float(value)
    if not math.isfinite(numeric) or not 0 <= numeric <= maximum:
        raise QueueError(f"{name} must be from 0 through {maximum:g}")
    return int(value) if isinstance(value, int) else numeric


def record_missed_offload(root: Path, args: argparse.Namespace) -> dict[str, object]:
    """Record bounded telemetry for eligible work that still ran on the Pi."""
    if args.profile not in {*protocol.PROFILE_FAMILIES, "other"}:
        raise QueueError("--profile is not a known offline profile")
    if args.reason not in MISSED_REASONS:
        raise QueueError("--reason is not a known missed-offload reason")
    label = args.label.strip()
    if not label or len(label) > MAX_MISSED_LABEL or any(
        character in label for character in ("\x00", "\r", "\n")
    ):
        raise QueueError(f"--label must be 1-{MAX_MISSED_LABEL} characters on one line")
    event_id = f"{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}-{secrets.token_hex(4)}"
    payload: dict[str, object] = {
        "schema_version": protocol.SCHEMA_VERSION,
        "id": event_id,
        "recorded_at": utc_now(),
        "profile": args.profile,
        "label": label,
        "reason": args.reason,
    }
    for field, option, maximum in (
        ("duration_seconds", args.duration_seconds, 10 * 365 * 24 * 60 * 60),
        ("cpu_seconds", args.cpu_seconds, 10 * 365 * 24 * 60 * 60),
        ("peak_rss_bytes", args.peak_rss_bytes, 1 << 60),
        ("input_bytes", args.input_bytes, 1 << 60),
    ):
        value = _bounded_metric(option, f"--{field.replace('_', '-')}", maximum)
        if value is not None:
            payload[field] = value

    missed_root = root / "missed"
    with queue_lock(root):
        atomic_json(missed_root / f"{event_id}.json", payload)
        # Only unlink names with the exact event pattern.  lstat/is_file are not
        # used to open candidates, so a symlink is never followed while pruning.
        candidates = sorted(
            path
            for path in missed_root.iterdir()
            if not path.is_symlink()
            and path.name.endswith(".json")
            and JOB_ID_RE.fullmatch(path.name[:-5])
            and stat.S_ISREG(path.lstat().st_mode)
        )
        for old in candidates[:-MAX_MISSED_EVENTS]:
            old.unlink()
    return payload


def worker_heartbeat(
    root: Path,
    worker: str,
    slots_total: int | None = None,
    slots_busy: int | None = None,
    *,
    protocol_version: int = protocol.WORKER_PROTOCOL_VERSION,
) -> dict[str, object]:
    validate_worker(worker)
    require_worker_protocol_version(protocol_version)
    payload = {
        "worker": worker,
        "seen_at": utc_now(),
        "available": True,
        "protocol_version": protocol_version,
    }
    if (slots_total is None) != (slots_busy is None):
        raise QueueError("--slots-total and --slots-busy must be supplied together")
    if slots_total is not None:
        if (
            isinstance(slots_total, bool)
            or not isinstance(slots_total, int)
            or isinstance(slots_busy, bool)
            or not isinstance(slots_busy, int)
            or not 0 <= slots_busy <= slots_total <= 64
        ):
            raise QueueError("worker capacity must satisfy 0 <= slots-busy <= slots-total <= 64")
        payload.update({"slots_total": slots_total, "slots_busy": slots_busy})
    atomic_json(root / "workers" / f"{worker}.json", payload)
    return payload


def workers_available(root: Path, max_age: float) -> list[dict[str, object]]:
    if not math.isfinite(max_age) or max_age <= 0:
        raise QueueError("--max-age must be a finite positive number")
    now = dt.datetime.now(dt.timezone.utc)
    workers: list[dict[str, object]] = []
    for path in (root / "workers").glob("*.json"):
        try:
            payload = load_json(path)
            seen = parse_timestamp(str(payload["seen_at"]))
        except (QueueError, KeyError, ValueError):
            continue
        age = max(0.0, (now - seen).total_seconds())
        try:
            require_worker_protocol_version(payload.get("protocol_version"))
        except QueueError:
            protocol_compatible = False
        else:
            protocol_compatible = True
        payload["age_seconds"] = round(age, 3)
        payload["available"] = age <= max_age and protocol_compatible
        workers.append(payload)
    workers.sort(key=lambda item: str(item.get("worker", "")))
    return workers


def _manifest_attempt(manifest: dict[str, object], *, increment: bool) -> int:
    value = manifest.get("attempt", 0)
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < 1_000_000:
        raise QueueError("job manifest has an invalid attempt number")
    if increment:
        if value == 999_999:
            raise QueueError("job has exhausted its attempt counter")
        value += 1
    return max(1, value)


def worker_claim(
    root: Path,
    worker: str,
    placement: str = "remote",
    *,
    protocol_version: int = protocol.WORKER_PROTOCOL_VERSION,
) -> dict[str, object] | None:
    validate_worker(worker)
    require_worker_protocol_version(protocol_version)
    if placement not in JOB_PLACEMENTS:
        raise QueueError(f"invalid job placement {placement!r}")
    worker_heartbeat(root, worker, protocol_version=protocol_version)
    with queue_lock(root):
        if maintenance_status(root) is not None:
            return None
        # Resume only a job owned by this exact worker/slot identity.  With a
        # concurrent scheduler, matching a shared base hostname here could let
        # two slots execute and finish the same job.
        for path in sorted((root / "running").iterdir()):
            if (
                path.is_symlink()
                or not path.is_dir()
                or not JOB_ID_RE.fullmatch(path.name)
            ):
                continue
            manifest = load_json(path / "manifest.json")
            if manifest.get("worker") == worker:
                token = manifest.get("lease_token")
                if token is None:
                    token = secrets.token_hex(16)
                elif not isinstance(token, str) or not LEASE_TOKEN_RE.fullmatch(token):
                    raise QueueError("running job has an invalid lease token")
                manifest["lease_token"] = token
                manifest["attempt"] = _manifest_attempt(manifest, increment=False)
                manifest.setdefault("placement", placement)
                manifest["resumed_at"] = utc_now()
                atomic_json(path / "manifest.json", manifest)
                return manifest
        queued = sorted(
            path for path in (root / "queued").iterdir()
            if not path.is_symlink() and path.is_dir() and JOB_ID_RE.fullmatch(path.name)
        )
        if not queued:
            return None
        source = queued[0]
        destination = root / "running" / source.name
        manifest = load_json(source / "manifest.json")
        if manifest.get("id") != source.name:
            raise QueueError(
                f"queued job manifest id does not match its directory: {source.name}"
            )
        manifest.update(
            {
                "state": "running",
                "worker": worker,
                "placement": placement,
                "lease_token": secrets.token_hex(16),
                "attempt": _manifest_attempt(manifest, increment=True),
                "started_at": utc_now(),
            }
        )
        # Persist ownership before the state-directory move.  If the process is
        # killed before os.replace, the job remains in queued and the next
        # claimant safely overwrites this never-issued lease.  Moving first
        # would leave an ownerless manifest stranded in running after a crash.
        atomic_json(source / "manifest.json", manifest)
        os.replace(source, destination)
        return manifest


def stream_bounded_input(
    manifest: dict[str, object],
    index: int,
    output: BinaryIO,
    lease_token: str | None = None,
) -> None:
    require_manifest_lease(manifest, lease_token)
    inputs = manifest.get("inputs")
    if not isinstance(inputs, list) or not 0 <= index < len(inputs):
        raise QueueError(f"input index {index} is out of range")
    record = inputs[index]
    if not isinstance(record, dict):
        raise QueueError("input record is invalid")
    remote_path = record.get("remote_path")
    device = record.get("device")
    inode = record.get("inode")
    submitted_size = record.get("size")
    expected_hash = record.get("sha256")
    if (
        not isinstance(remote_path, str)
        or any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in (device, inode, submitted_size)
        )
        or not isinstance(expected_hash, str)
        or not re.fullmatch(r"[0-9a-f]{64}", expected_hash)
    ):
        raise QueueError("input record has invalid file identity metadata")
    path = Path(remote_path)
    flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise QueueError(f"cannot safely open input {path}: {exc}") from None
    with os.fdopen(descriptor, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise QueueError(f"input is no longer a regular file: {path}")
        if info.st_dev != device or info.st_ino != inode:
            raise QueueError(f"input was replaced after submission: {path}")
        remaining = submitted_size
        digest = hashlib.sha256()
        if info.st_size < remaining:
            raise QueueError(f"input shrank after submission: {path}")
        while remaining:
            chunk = handle.read(min(COPY_CHUNK, remaining))
            if not chunk:
                raise QueueError(f"input ended before its submitted size: {path}")
            digest.update(chunk)
            output.write(chunk)
            remaining -= len(chunk)
        after = os.fstat(handle.fileno())
        if (
            after.st_dev != device
            or after.st_ino != inode
            or after.st_size < submitted_size
        ):
            raise QueueError(f"input changed while it was streamed: {path}")
        if digest.hexdigest() != expected_hash:
            raise QueueError(f"input prefix changed after submission: {path}")


class _HashingSourceReader:
    def __init__(self, handle: BinaryIO, size: int) -> None:
        self.handle = handle
        self.remaining = size
        self.digest = hashlib.sha256()
        self.bytes_read = 0

    def read(self, size: int = -1) -> bytes:
        if self.remaining <= 0:
            return b""
        requested = self.remaining if size < 0 else min(size, self.remaining)
        chunk = self.handle.read(requested)
        self.remaining -= len(chunk)
        self.bytes_read += len(chunk)
        self.digest.update(chunk)
        return chunk


def stream_source_bundle(
    job_path: Path,
    manifest: dict[str, object],
    output: BinaryIO,
    lease_token: str | None = None,
) -> None:
    """Write one normalized, uncompressed streaming tar of manifest sources."""
    require_manifest_lease(manifest, lease_token)
    raw_sources = manifest.get("sources")
    if not isinstance(raw_sources, list):
        raise QueueError("job manifest has invalid sources")
    seen: set[str] = set()
    with tarfile.open(
        fileobj=output,
        mode="w|",
        format=tarfile.PAX_FORMAT,
    ) as bundle:
        for record in raw_sources:
            if not isinstance(record, dict):
                raise QueueError("job manifest has an invalid source record")
            relative_text = record.get("path")
            size = record.get("size")
            expected_hash = record.get("sha256")
            if not isinstance(relative_text, str):
                raise QueueError("job source has an invalid path")
            relative = Path(relative_text)
            if (
                not relative_text
                or "\\" in relative_text
                or any(character in relative_text for character in ("\x00", "\r", "\n"))
                or relative.is_absolute()
                or "." in relative.parts
                or ".." in relative.parts
                or relative.as_posix() != relative_text
                or relative_text in seen
            ):
                raise QueueError(f"job source has an unsafe or duplicate path: {relative_text!r}")
            if (
                isinstance(size, bool)
                or not isinstance(size, int)
                or size < 0
                or not isinstance(expected_hash, str)
                or not re.fullmatch(r"[0-9a-f]{64}", expected_hash)
            ):
                raise QueueError(f"job source has invalid metadata: {relative_text}")
            seen.add(relative_text)
            path = job_path / "source" / relative
            try:
                info = path.lstat()
            except OSError as exc:
                raise QueueError(f"cannot inspect source snapshot {relative_text}: {exc}") from None
            if path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_size != size:
                raise QueueError(f"source snapshot is not the declared regular file: {relative_text}")

            header = tarfile.TarInfo(relative_text)
            header.size = size
            header.mode = 0o600
            header.uid = 0
            header.gid = 0
            header.uname = ""
            header.gname = ""
            header.mtime = 0
            header.type = tarfile.REGTYPE
            flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
            try:
                descriptor = os.open(path, flags)
            except OSError as exc:
                raise QueueError(f"cannot open source snapshot {relative_text}: {exc}") from None
            with os.fdopen(descriptor, "rb") as handle:
                opened = os.fstat(handle.fileno())
                if not stat.S_ISREG(opened.st_mode) or opened.st_size != size:
                    raise QueueError(
                        f"source snapshot changed before streaming: {relative_text}"
                    )
                reader = _HashingSourceReader(handle, size)
                bundle.addfile(header, reader)
            if (
                reader.bytes_read != size
                or reader.remaining != 0
                or reader.digest.hexdigest() != expected_hash
            ):
                raise QueueError(f"source snapshot hash mismatch: {relative_text}")


def put_result(
    root: Path,
    job_id: str,
    worker: str,
    relative_text: str,
    input_stream: BinaryIO,
    lease_token: str | None = None,
) -> dict[str, object]:
    require_running_job(root, job_id, worker, lease_token)
    relative = validate_relative_result(relative_text)
    upload_root = root / "uploads"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{job_id}.{relative.name}.", dir=upload_root
    )
    temporary = Path(temporary_name)
    digest = hashlib.sha256()
    size = 0
    maximum = DEFAULT_MAX_RESULT_BYTES
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
        # A stale recovery can move and reassign the job while a large upload
        # is in flight. Revalidate ownership while holding the queue lock, then
        # install from a staging directory that never moves with the job.
        with queue_lock(root):
            job_path, _manifest = require_running_job(
                root, job_id, worker, lease_token
            )
            result_root = job_path / "result"
            result_root.mkdir(mode=0o700, exist_ok=True)
            if result_root.is_symlink() or not result_root.is_dir():
                raise QueueError("job result root is not a real directory")
            destination_parent = result_root
            for part in relative.parts[:-1]:
                destination_parent = destination_parent / part
                destination_parent.mkdir(mode=0o700, exist_ok=True)
                if destination_parent.is_symlink() or not destination_parent.is_dir():
                    raise QueueError(
                        "result destination parent is not a real directory"
                    )
            destination = destination_parent / relative.name
            os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return {"path": relative.as_posix(), "size": size, "sha256": digest.hexdigest()}


def worker_finish(
    root: Path,
    job_id: str,
    worker: str,
    exit_code: int,
    result_files: Sequence[str],
    lease_token: str | None = None,
) -> dict[str, object]:
    with queue_lock(root):
        job_path, manifest = require_running_job(root, job_id, worker, lease_token)
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
        manifest.pop("lease_token", None)
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
    if not math.isfinite(timeout) or timeout < 0:
        raise QueueError("wait timeout must be a finite non-negative number")
    if not math.isfinite(interval) or interval <= 0:
        raise QueueError("wait interval must be a finite positive number")
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

    tasks_parser = subparsers.add_parser("tasks", help="list allowlisted offline tasks and profiles")
    tasks_parser.add_argument(
        "--source-root",
        type=Path,
        default=Path(os.environ.get("VAN_COMPUTE_SOURCE_ROOT", DEFAULT_SOURCE_ROOT)),
        help="source repository containing optional .van-compute.json",
    )

    submit = subparsers.add_parser("submit", help="submit an offline job")
    submit.add_argument("task", help="built-in or repository-declared task name")
    submit.add_argument("--source-root", type=Path, default=Path(os.environ.get("VAN_COMPUTE_SOURCE_ROOT", DEFAULT_SOURCE_ROOT)))
    submit.add_argument("--input", action="append", default=[], help="input file inside source-root; repeat as needed")
    submit.add_argument("--input-value", action="append", help="optional scalar paired with each input")
    submit.add_argument("--arg", dest="argument", action="append", default=[], help="allowlisted task argument; use --arg=--option")
    submit.add_argument("--wait", type=float, metavar="SECONDS", help="wait up to this long for completion")

    status_parser = subparsers.add_parser("status", help="show one job")
    status_parser.add_argument("job_id")

    list_parser = subparsers.add_parser("list", help="list recent jobs")
    list_parser.add_argument("--limit", type=int, default=20)

    available = subparsers.add_parser("available", help="show recent worker heartbeats")
    available.add_argument("--max-age", type=float, default=DEFAULT_HEARTBEAT_MAX_AGE)

    missed = subparsers.add_parser(
        "missed-offload",
        help="record eligible heavy work that ran locally instead of on a worker",
    )
    missed.add_argument("--profile", required=True, choices=sorted((*protocol.PROFILE_FAMILIES, "other")))
    missed.add_argument("--label", required=True, help="short, non-secret workload label")
    missed.add_argument("--reason", required=True, choices=MISSED_REASONS)
    missed.add_argument("--duration-seconds", type=float)
    missed.add_argument("--cpu-seconds", type=float)
    missed.add_argument("--peak-rss-bytes", type=int)
    missed.add_argument("--input-bytes", type=int)

    wait_parser = subparsers.add_parser("wait", help="wait for a submitted job")
    wait_parser.add_argument("job_id")
    wait_parser.add_argument("--timeout", type=float, default=3600)

    result_parser = subparsers.add_parser("result", help="write one completed result file to stdout")
    result_parser.add_argument("job_id")
    result_parser.add_argument("path")

    maintenance = subparsers.add_parser("maintenance", help=argparse.SUPPRESS)
    maintenance.add_argument("action", choices=("enter", "exit", "status"))
    maintenance.add_argument("--owner")

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
            item.add_argument("--placement", choices=JOB_PLACEMENTS, default="remote")
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
    return parser


def print_json(payload: object) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "tasks":
            source_root = args.source_root.expanduser().resolve()
            repo_tasks = protocol.load_repo_tasks(source_root) if source_root.is_dir() else {}
            all_tasks = [*protocol.TASKS.values(), *repo_tasks.values()]
            print_json(
                {
                    "schema_version": protocol.SCHEMA_VERSION,
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
                                    "family": task.family,
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
                        for task in all_tasks
                    ],
                    "profiles": [
                        {
                            "name": name,
                            "family": protocol.PROFILE_FAMILIES[name],
                            "description": protocol.PROFILE_DESCRIPTIONS[name],
                        }
                        for name in protocol.PROFILE_FAMILIES
                    ],
                }
            )
            return 0

        root = safe_root(args.root)
        if args.command == "submit":
            if args.wait is not None and (not math.isfinite(args.wait) or args.wait < 0):
                raise QueueError("--wait must be a finite non-negative number")
            manifest = submit_job(args)
            if args.wait is not None:
                manifest = wait_for_job(root, str(manifest["id"]), args.wait)
            print_json(public_manifest(manifest))
            return 0 if manifest.get("state") != "failed" else 1
        if args.command == "status":
            print_json(public_manifest(manifest_for(root, args.job_id)))
            return 0
        if args.command == "list":
            if not 1 <= args.limit <= 1000:
                raise QueueError("--limit must be from 1 through 1000")
            print_json(
                {"jobs": [public_manifest(item) for item in list_jobs(root, args.limit)]}
            )
            return 0
        if args.command == "available":
            workers = workers_available(root, args.max_age)
            print_json({"available": any(item["available"] for item in workers), "workers": workers})
            return 0
        if args.command == "missed-offload":
            print_json(record_missed_offload(root, args))
            return 0
        if args.command == "wait":
            manifest = wait_for_job(root, args.job_id, args.timeout)
            print_json(public_manifest(manifest))
            return 0 if manifest.get("state") != "failed" else 1
        if args.command == "result":
            stream_result(root, args.job_id, args.path, sys.stdout.buffer)
            return 0
        if args.command == "maintenance":
            if args.action == "status":
                if args.owner is not None:
                    raise QueueError("maintenance status does not accept --owner")
                print_json(maintenance_status(root) or {"active": False})
            else:
                if args.owner is None:
                    raise QueueError("maintenance enter/exit requires --owner")
                print_json(
                    set_maintenance(root, args.owner, args.action == "enter")
                )
            return 0
        if args.command == "worker":
            if args.worker_command == "heartbeat":
                print_json(
                    worker_heartbeat(
                        root,
                        args.worker,
                        args.slots_total,
                        args.slots_busy,
                        protocol_version=args.protocol_version,
                    )
                )
                return 0
            if args.worker_command == "claim":
                print_json(
                    {
                        "job": worker_claim(
                            root,
                            args.worker,
                            args.placement,
                            protocol_version=args.protocol_version,
                        )
                    }
                )
                return 0
            if args.worker_command == "stream":
                job_path, manifest = require_running_job(
                    root,
                    args.job_id,
                    args.worker,
                    args.lease_token,
                )
                if args.kind == "input":
                    if args.index is None:
                        raise QueueError("input streaming requires --index")
                    stream_bounded_input(
                        manifest,
                        args.index,
                        sys.stdout.buffer,
                        args.lease_token,
                    )
                else:
                    if args.index is not None:
                        raise QueueError("source-bundle streaming accepts no --index")
                    stream_source_bundle(
                        job_path,
                        manifest,
                        sys.stdout.buffer,
                        args.lease_token,
                    )
                return 0
            if args.worker_command == "put-result":
                record = put_result(
                    root,
                    args.job_id,
                    args.worker,
                    args.path,
                    sys.stdin.buffer,
                    args.lease_token,
                )
                print_json(record)
                return 0
            if args.worker_command == "finish":
                print_json(
                    worker_finish(
                        root,
                        args.job_id,
                        args.worker,
                        args.exit_code,
                        args.result_file,
                        args.lease_token,
                    )
                )
                return 0
    except (OSError, tarfile.TarError, QueueError, protocol.ProtocolError) as exc:
        print(f"van-compute: {exc}", file=sys.stderr)
        return 2
    parser.error("unhandled command")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
