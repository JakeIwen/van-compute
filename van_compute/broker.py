#!/usr/bin/env python3
"""Place queued van-compute jobs on a remote worker or safe Pi fallback.

The broker never probes the network.  Fresh queue heartbeats are leases: while
one exists, queued work is left for remote workers.  If every remote lease is
stale, an eligible job may run locally after a short grace period and only when
the Pi health gate passes.

Local execution uses the same shell-free, embedded execution specification as
the Mac worker.  It deliberately supports only bounded offline profiles; it
can run the fixed saved-CAN-log analyzers, but not live CAN access, corpus
datasets, JADX, ADB, service commands, or an arbitrary shell command.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import asdict, dataclass
import datetime as dt
import fcntl
from functools import lru_cache
import inspect
import json
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
from typing import Callable, Mapping, Sequence


from van_compute import engine, limited_child, queue, task_cache
from van_compute.config import BrokerConfig, MissedOffloadRecord
from van_compute.job_requeue import requeue_running_job
from van_compute.pi_gate_flags import add_gate_arguments, validated_thresholds
from van_compute.pi_health_monitor import HealthMonitor
from van_compute.pi_health import (
    HealthAssessment,
    HealthProbeError,
    HealthSnapshot,
    _read_meminfo,
    _read_temperature,
    _read_throttled,
    assess_health,
    read_health_snapshot,
)

protocol = queue.protocol

LOCAL_WORKER = "vanpi-local.00"
LOCAL_WORKER_PREFIX = "vanpi-local"
LOCAL_PROFILES = frozenset(
    {
        "repo-test",
        "python-script",
        "python-module",
        "sqlite-readonly",
        "can-log-batch",
    }
)
REMOTE_ONLY_PROFILES = frozenset({"apk-analyze", "corpus-search"})
LOCAL_BUILTIN_TASKS = frozenset(protocol.TASKS)
DEFAULT_WORK_ROOT = queue.DEFAULT_QUEUE_ROOT / "local-work"
DEFAULT_REMOTE_MAX_AGE = 45.0
DEFAULT_REMOTE_GRACE = 30.0
DEFAULT_STALE_RUNNING_AGE = 300.0
DEFAULT_POLL_INTERVAL = 5.0
DEFAULT_TIMEOUT = 1800
DEFAULT_CPU_SECONDS = 1200
DEFAULT_MAX_MEMORY_BYTES = 768 * 1024 * 1024
DEFAULT_MAX_RESULT_BYTES = protocol.MAX_RESULT_BYTES
DEFAULT_MIN_WORK_FREE_BYTES = 1024 * 1024 * 1024
DEFAULT_NOFILE = 256
DEFAULT_BWRAP = "/usr/bin/bwrap"
DEFAULT_LOCAL_PYTHON = "/home/pi/van_compute/venv/bin/python3"
RESOURCE_POLL_INTERVAL = 0.5
SAFE_INPUT_RE = re.compile(r"[^A-Za-z0-9_.-]+")
MOUNTINFO_ESCAPE_RE = re.compile(r"\\([0-7]{3})")
MEMORY_BACKED_FILESYSTEMS = frozenset(
    {"tmpfs", "ramfs", "devtmpfs", "hugetlbfs"}
)


class BrokerError(RuntimeError):
    """The broker cannot safely place or execute a job."""


@dataclass(frozen=True)
class LocalProcessOutcome:
    exit_code: int
    usage: object
    timed_out: bool
    interrupted: bool
    resource_limit: str | None
    resource_monitor_error: str | None
    minimum_filesystem_free_bytes: int | None


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def _decode_mountinfo_path(value: str) -> str:
    return MOUNTINFO_ESCAPE_RE.sub(
        lambda match: chr(int(match.group(1), 8)), value
    )


def filesystem_type_for_path(
    path: Path,
    mountinfo_path: Path = Path("/proc/self/mountinfo"),
) -> str:
    """Return the filesystem type for an existing path from Linux mountinfo."""
    candidate = path.resolve(strict=True)
    try:
        lines = mountinfo_path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise BrokerError(f"cannot inspect filesystem for {candidate}: {exc}") from None
    best_mount: Path | None = None
    best_type: str | None = None
    for line in lines:
        fields = line.split()
        try:
            separator = fields.index("-")
            mount = Path(_decode_mountinfo_path(fields[4]))
            filesystem_type = fields[separator + 1]
        except (IndexError, ValueError):
            continue
        if candidate != mount and mount not in candidate.parents:
            continue
        if best_mount is None or len(mount.parts) > len(best_mount.parts):
            best_mount = mount
            best_type = filesystem_type
    if best_type is None:
        raise BrokerError(
            f"cannot determine the mounted filesystem containing {candidate}"
        )
    return best_type


def prepare_work_root(path: Path) -> Path:
    """Create a private, disk-backed staging directory and fail closed."""
    unresolved = path.expanduser()
    if unresolved.is_symlink():
        raise BrokerError(f"work root cannot be a symlink: {unresolved}")
    try:
        unresolved.mkdir(parents=True, exist_ok=True, mode=0o700)
        resolved = unresolved.resolve(strict=True)
        info = resolved.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise BrokerError(f"work root is not a real directory: {resolved}")
        os.chmod(resolved, 0o700)
    except OSError as exc:
        raise BrokerError(f"cannot prepare work root {unresolved}: {exc}") from None
    if sys.platform.startswith("linux"):
        filesystem_type = filesystem_type_for_path(resolved)
        if filesystem_type in MEMORY_BACKED_FILESYSTEMS:
            raise BrokerError(
                f"work root must be disk-backed; {resolved} is on {filesystem_type}"
            )
    return resolved


def _is_local_worker(worker: str) -> bool:
    return (
        worker == "pi-local"
        or worker.startswith("pi-local.")
        or worker == LOCAL_WORKER_PREFIX
        or worker.startswith(f"{LOCAL_WORKER_PREFIX}.")
    )


def _compatible_worker_protocol(payload: Mapping[str, object]) -> bool:
    version = payload.get("protocol_version")
    return (
        isinstance(version, int)
        and not isinstance(version, bool)
        and version == protocol.WORKER_PROTOCOL_VERSION
    )


def _remote_worker_leases(
    root: Path,
) -> list[tuple[dict[str, object], dt.datetime]]:
    leases: list[tuple[dict[str, object], dt.datetime]] = []
    workers_root = root / "workers"
    for path in workers_root.glob("*.json"):
        try:
            payload = queue.load_json(path)
            worker = str(payload["worker"])
            seen_at = queue.parse_timestamp(str(payload["seen_at"]))
        except (queue.QueueError, KeyError, ValueError):
            continue
        if (
            seen_at.tzinfo is None
            or _is_local_worker(worker)
            or not _compatible_worker_protocol(payload)
        ):
            continue
        leases.append((dict(payload), seen_at))
    return leases


def fresh_remote_workers(
    root: Path,
    max_age: float,
    *,
    now: dt.datetime | None = None,
) -> list[dict[str, object]]:
    """Return fresh non-local worker leases without touching the network."""
    now = now or dt.datetime.now(dt.timezone.utc)
    workers: list[dict[str, object]] = []
    for payload, seen_at in _remote_worker_leases(root):
        age = max(0.0, (now - seen_at).total_seconds())
        if age <= max_age:
            item = payload
            item["age_seconds"] = round(age, 3)
            workers.append(item)
    workers.sort(key=lambda item: str(item.get("worker", "")))
    return workers


def latest_remote_worker_seen(root: Path) -> dt.datetime | None:
    """Return the newest known non-local heartbeat, including stale leases."""
    seen = [seen_at for _payload, seen_at in _remote_worker_leases(root)]
    return max(seen, default=None)


def job_profile(manifest: Mapping[str, object]) -> str | None:
    execution = manifest.get("execution")
    if execution is None:
        return None
    try:
        specification = protocol.validate_execution(execution)
    except protocol.ProtocolError:
        return None
    return str(specification["profile"])


def local_eligibility(manifest: Mapping[str, object]) -> tuple[bool, str]:
    profile = job_profile(manifest)
    if profile is None:
        task = str(manifest.get("task", ""))
        if manifest.get("execution") is None and task in LOCAL_BUILTIN_TASKS:
            return True, "eligible fixed offline built-in"
        return False, "unknown legacy or invalid tasks require a remote worker"
    if profile in REMOTE_ONLY_PROFILES:
        return False, f"{profile} is remote-only"
    specification = protocol.validate_execution(manifest.get("execution"))
    if specification["datasets"]:
        return False, "configured worker datasets are remote-only"
    if profile not in LOCAL_PROFILES:
        return False, f"{profile} is not approved for Pi fallback"
    return True, "eligible"


@lru_cache(maxsize=8)
def _python_has_pytest(executable: str) -> bool:
    try:
        result = subprocess.run(
            [executable, "-c", "import pytest"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env={
                "HOME": "/nonexistent",
                "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
                "PYTHONNOUSERSITE": "1",
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
            },
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0


def _manifest_record_bytes(
    manifest: Mapping[str, object], field: str
) -> int:
    records = manifest.get(field)
    if not isinstance(records, list):
        raise BrokerError(f"job manifest has no {field} records")
    total = 0
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise BrokerError(f"job has an invalid {field} record at index {index}")
        size = record.get("size")
        if (
            isinstance(size, bool)
            or not isinstance(size, int)
            or not 0 <= size <= (1 << 63) - 1
        ):
            raise BrokerError(
                f"job has an invalid {field} size at index {index}"
            )
        total += size
    return total


def local_work_required_bytes(
    manifest: Mapping[str, object], maximum_result_bytes: int
) -> int:
    """Bound staging plus directory-to-archive result packaging at admission."""
    if (
        isinstance(maximum_result_bytes, bool)
        or not isinstance(maximum_result_bytes, int)
        or maximum_result_bytes < 0
    ):
        raise BrokerError("maximum result bytes must be a non-negative integer")
    return (
        _manifest_record_bytes(manifest, "sources")
        + _manifest_record_bytes(manifest, "inputs")
        + (2 * maximum_result_bytes)
    )


def work_capacity_eligibility(
    manifest: Mapping[str, object], args: BrokerConfig
) -> tuple[bool, str]:
    try:
        required = local_work_required_bytes(manifest, args.max_result_bytes)
        available = shutil.disk_usage(args.work_root).free
    except (BrokerError, OSError) as exc:
        return False, f"cannot verify local staging capacity: {exc}"
    reserve = args.min_work_free_bytes
    if available - required < reserve:
        return (
            False,
            "local staging needs "
            f"{required} bytes while preserving {reserve} free bytes; "
            f"only {available} bytes are available",
        )
    return True, "eligible"


def runtime_eligibility(
    manifest: Mapping[str, object], args: BrokerConfig
) -> tuple[bool, str]:
    eligible, reason = local_eligibility(manifest)
    if not eligible:
        return eligible, reason
    bwrap = Path(args.bwrap)
    if not bwrap.is_absolute() or not bwrap.is_file() or not os.access(bwrap, os.X_OK):
        return False, f"bubblewrap is unavailable: {bwrap}"
    profile_value = job_profile(manifest)
    profile = str(profile_value) if profile_value is not None else "can-log-batch"
    family = "python" if profile_value is None else protocol.PROFILE_FAMILIES[profile]
    executable_text = args.python if family == "python" else args.sqlite3
    executable = Path(executable_text)
    if (
        not executable.is_absolute()
        or not executable.is_file()
        or not os.access(executable, os.X_OK)
    ):
        return False, f"required {family} executable is unavailable: {executable}"
    if profile == "repo-test" and not _python_has_pytest(str(executable)):
        return False, f"repo-test runtime has no pytest: {executable}"
    eligible, reason = work_capacity_eligibility(manifest, args)
    if not eligible:
        return eligible, reason
    return True, "eligible"


def _job_age(manifest: Mapping[str, object], now: dt.datetime) -> float | None:
    try:
        submitted = queue.parse_timestamp(str(manifest["submitted_at"]))
    except (KeyError, ValueError):
        return None
    return max(0.0, (now - submitted).total_seconds())


def _heartbeat_age(root: Path, worker: str, now: dt.datetime) -> float:
    try:
        payload = queue.load_json(root / "workers" / f"{worker}.json")
        seen_at = queue.parse_timestamp(str(payload["seen_at"]))
    except (queue.QueueError, KeyError, ValueError):
        return math.inf
    if not _compatible_worker_protocol(payload):
        return math.inf
    # Capacity coordinators advertise slots but never execute a job under the
    # base identity. During a one-shot-to-scheduler upgrade, their fresh base
    # heartbeat must not perpetually renew an abandoned legacy base-worker job.
    if payload.get("slots_total") is not None:
        return math.inf
    return max(0.0, (now - seen_at).total_seconds())


def recover_stale_remote_jobs(
    root: Path,
    *,
    stale_age: float,
    now: dt.datetime | None = None,
) -> list[str]:
    """Return conservatively abandoned remote attempts to the queue."""
    now = now or dt.datetime.now(dt.timezone.utc)
    recovered: list[str] = []
    with queue.queue_lock(root):
        if queue.maintenance_status(root) is not None:
            return recovered
        for path in sorted((root / "running").iterdir()):
            if (
                path.is_symlink()
                or not path.is_dir()
                or not queue.JOB_ID_RE.fullmatch(path.name)
            ):
                continue
            try:
                manifest = queue.load_json(path / "manifest.json")
            except queue.QueueError:
                continue
            if manifest.get("id") != path.name:
                continue
            worker = str(manifest.get("worker", ""))
            if not worker or _is_local_worker(worker):
                continue
            try:
                queue.validate_worker(worker)
            except queue.QueueError:
                continue
            try:
                started = queue.parse_timestamp(str(manifest["started_at"]))
            except (KeyError, ValueError):
                continue
            running_age = max(0.0, (now - started).total_seconds())
            if running_age < stale_age or _heartbeat_age(root, worker, now) < stale_age:
                continue
            requeue_running_job(
                root,
                path,
                manifest,
                reason="remote-worker-lease-expired",
                now=now,
                error_type=BrokerError,
            )
            recovered.append(path.name)
    return recovered


def queue_supports_lease_tokens() -> bool:
    return all(
        "lease_token" in inspect.signature(function).parameters
        for function in (queue.require_running_job, queue.put_result, queue.worker_finish)
    )


def _require_local_job(root: Path, job_id: str, lease_token: str):
    return queue.require_running_job(
        root, job_id, LOCAL_WORKER, lease_token=lease_token
    )


def _put_local_result(
    root: Path,
    job_id: str,
    lease_token: str,
    relative: str,
    handle,
) -> dict[str, object]:
    return queue.put_result(
        root,
        job_id,
        LOCAL_WORKER,
        relative,
        handle,
        lease_token=lease_token,
    )


def _finish_local_job(
    root: Path,
    job_id: str,
    lease_token: str,
    exit_code: int,
    uploaded: Sequence[str],
) -> dict[str, object]:
    return queue.worker_finish(
        root,
        job_id,
        LOCAL_WORKER,
        exit_code,
        uploaded,
        lease_token=lease_token,
    )


def _running_local_job(root: Path) -> dict[str, object] | None:
    for path in sorted((root / "running").iterdir()):
        if (
            path.is_symlink()
            or not path.is_dir()
            or not queue.JOB_ID_RE.fullmatch(path.name)
        ):
            continue
        try:
            manifest = queue.load_json(path / "manifest.json")
        except queue.QueueError:
            continue
        if manifest.get("id") != path.name:
            continue
        if _is_local_worker(str(manifest.get("worker", ""))):
            return manifest
    return None


def claim_local_job(
    root: Path,
    *,
    grace_seconds: float,
    remote_max_age: float,
    now: dt.datetime | None = None,
    eligibility: Callable[[Mapping[str, object]], tuple[bool, str]] = local_eligibility,
) -> tuple[dict[str, object] | None, str]:
    """Atomically resume or claim at most one eligible local-fallback job."""
    now = now or dt.datetime.now(dt.timezone.utc)
    with queue.queue_lock(root):
        if queue.maintenance_status(root) is not None:
            return None, "queue maintenance is active"
        running = _running_local_job(root)
        if running is not None:
            if running.get("worker") != LOCAL_WORKER:
                return None, "another local fallback worker already owns a job"
            running = dict(running)
            running["resumed_at"] = utc_now()
            path = root / "running" / str(running["id"]) / "manifest.json"
            queue.atomic_json(path, running)
            return running, "resumed local fallback job"

        if fresh_remote_workers(root, remote_max_age, now=now):
            return None, "fresh remote worker lease"

        latest_remote_seen = latest_remote_worker_seen(root)
        if latest_remote_seen is not None:
            lease_expired_at = latest_remote_seen + dt.timedelta(
                seconds=remote_max_age
            )
            local_fallback_at = lease_expired_at + dt.timedelta(
                seconds=grace_seconds
            )
            if now < local_fallback_at:
                return (
                    None,
                    "remote worker lease expired; waiting through the remote grace period",
                )

        saw_grace = False
        unavailable_reason: str | None = None
        for source in sorted((root / "queued").iterdir()):
            if (
                source.is_symlink()
                or not source.is_dir()
                or not queue.JOB_ID_RE.fullmatch(source.name)
            ):
                continue
            try:
                manifest = queue.load_json(source / "manifest.json")
            except queue.QueueError:
                continue
            if manifest.get("id") != source.name:
                continue
            try:
                queue.validate_job_id(str(manifest["id"]))
            except (KeyError, queue.QueueError):
                continue
            eligible, eligibility_reason = eligibility(manifest)
            if not eligible:
                unavailable_reason = eligibility_reason
                continue
            # Without any historical remote heartbeat, submission time is the
            # only safe anchor. Once a worker has advertised a lease, the
            # grace period above starts when the latest lease expires instead.
            if latest_remote_seen is None:
                age = _job_age(manifest, now)
                if age is None or age < grace_seconds:
                    saw_grace = True
                    continue
            destination = root / "running" / source.name
            prior_attempt = manifest.get("attempt", 0)
            if isinstance(prior_attempt, bool) or not isinstance(prior_attempt, int):
                prior_attempt = 0
            manifest.update(
                {
                    "state": "running",
                    "worker": LOCAL_WORKER,
                    "started_at": utc_now(),
                    "placement": "pi-local",
                    "placement_reason": "remote-worker-lease-expired",
                    "attempt": prior_attempt + 1,
                    "lease_token": secrets.token_hex(16),
                }
            )
            # As in the remote claim path, write the lease while the job is
            # still queued.  A crash before the move therefore leaves work
            # reclaimable instead of creating an ownerless running directory.
            queue.atomic_json(source / "manifest.json", manifest)
            os.replace(source, destination)
            return manifest, "claimed local fallback job"
        if saw_grace:
            return None, "eligible job is waiting through the remote grace period"
        if unavailable_reason is not None:
            return None, unavailable_reason
        return None, "no eligible queued jobs"


def _safe_input_name(name: object, index: int) -> str:
    cleaned = SAFE_INPUT_RE.sub("_", Path(str(name)).name).strip("._")
    return f"{index:03d}-{cleaned or 'input'}"


def _copy_sources(
    job_path: Path,
    manifest: Mapping[str, object],
    destination: Path,
) -> None:
    records = manifest.get("sources")
    if not isinstance(records, list):
        raise BrokerError("job manifest has no source records")
    # Profiles such as sqlite-readonly legitimately have an empty source
    # snapshot.  They still need the read-only /job/source mount point.
    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
    for record in records:
        if not isinstance(record, dict):
            raise BrokerError("job has an invalid source record")
        relative_text = str(record.get("path", ""))
        relative = Path(relative_text)
        if relative.is_absolute() or ".." in relative.parts:
            raise BrokerError(f"job has an unsafe source path: {relative_text!r}")
        source = job_path / "source" / relative
        if source.is_symlink() or not source.is_file():
            raise BrokerError(f"source snapshot is missing: {relative_text}")
        if source.stat().st_size != record.get("size") or queue.hash_file(source) != record.get(
            "sha256"
        ):
            raise BrokerError(f"source snapshot verification failed: {relative_text}")
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        shutil.copyfile(source, target)
        os.chmod(target, 0o400)
    for directory in sorted(
        (path for path in destination.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    ):
        os.chmod(directory, 0o500)
    os.chmod(destination, 0o500)


def _copy_inputs(
    manifest: dict[str, object],
    destination: Path,
    lease_token: str,
) -> tuple[list[Path], list[object | None]]:
    records = manifest.get("inputs")
    if not isinstance(records, list):
        raise BrokerError("job manifest has no input records")
    paths: list[Path] = []
    values: list[object | None] = []
    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
    for index, record in enumerate(records):
        if not isinstance(record, dict) or record.get("index") != index:
            raise BrokerError("job has an invalid input record")
        target = destination / _safe_input_name(record.get("name", "input"), index)
        with target.open("wb") as output:
            queue.stream_bounded_input(
                manifest, index, output, lease_token=lease_token
            )
        os.chmod(target, 0o400)
        paths.append(target)
        values.append(record.get("value"))
    os.chmod(destination, 0o500)
    return paths, values


def _wait4_nohang(process: subprocess.Popen[bytes]):
    return engine.wait4_nohang(process)


def _wait_for_process(
    process: subprocess.Popen[bytes],
    *,
    timeout: int,
    should_stop: Callable[[], bool],
    work_path: Path,
    minimum_free_bytes: int,
    free_space_reader: Callable[[Path], int] = lambda path: max(
        0, shutil.disk_usage(path).free
    ),
) -> LocalProcessOutcome:
    outcome = engine.wait_for_process(
        process,
        host=engine.Host.PI,
        error_type=BrokerError,
        timeout=timeout,
        should_stop=should_stop,
        work_path=work_path,
        minimum_free_bytes=minimum_free_bytes,
        free_space_reader=free_space_reader,
        poll=_wait4_nohang,
        clock=time,
    )
    return LocalProcessOutcome(
        outcome.exit_code,
        outcome.usage,
        outcome.timed_out,
        outcome.interrupted,
        outcome.resource_limit,
        outcome.resource_monitor_error,
        outcome.minimum_filesystem_free_bytes,
    )


def _process_group_exists(process_group: int) -> bool:
    return engine.process_group_exists(
        process_group, host=engine.Host.PI, error_type=BrokerError
    )


def terminate_remaining_process_group(
    process_group: int, *, terminate_grace: float = 2.0, kill_grace: float = 2.0
) -> bool:
    return engine.terminate_group(
        process_group,
        exists=_process_group_exists,
        error_type=BrokerError,
        host=engine.Host.PI,
        terminate_grace=terminate_grace,
        kill_grace=kill_grace,
        clock=time,
    )


def _resource_usage(usage, wall_seconds: float) -> dict[str, object]:
    return {
        **engine.resource_usage(usage, wall_seconds),
        "scope": "wait4 resource usage for the local analysis process",
        "peak_rss_note": (
            "maximum RSS returned by wait4 for the reaped process; it is not a "
            "simultaneous sum of background descendant RSS"
        ),
    }


def bubblewrap_command(
    executable: str,
    command: Sequence[str],
    *,
    source_root: Path,
    inputs_root: Path,
    result_root: Path,
    home_root: Path,
    temporary_root: Path,
    cache_root: Path,
    job_id: str,
    runtime_bindings: Sequence[tuple[Path, str]] = (),
) -> list[str]:
    sandbox: engine.Sandbox = engine.BubblewrapSandbox(
        executable,
        source_root,
        inputs_root,
        result_root,
        home_root,
        temporary_root,
        cache_root,
        job_id,
        BrokerError,
        runtime_bindings,
    )
    return sandbox.wrap(command)


def sandbox_executable(
    executable: str, family: str
) -> tuple[str, list[tuple[Path, str]]]:
    return engine._sandbox_executable(executable, family, BrokerError)


@lru_cache(maxsize=4)
def bubblewrap_self_test(
    executable: str,
    python_executable: str,
    work_root_text: str,
    queue_root_text: str,
) -> tuple[bool, str]:
    """Exercise the effective sandbox before any Pi-local ownership claim."""
    work_root = Path(work_root_text).expanduser().resolve()
    queue_root = Path(queue_root_text).expanduser().resolve()
    try:
        host_python = Path(python_executable)
        if (
            not host_python.is_absolute()
            or not host_python.is_file()
            or not os.access(host_python, os.X_OK)
        ):
            raise BrokerError(
                f"configured local Python is unavailable: {host_python}"
            )
        sandbox_python, runtime_bindings = sandbox_executable(
            str(host_python), "python"
        )
        work_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        with tempfile.TemporaryDirectory(prefix=".sandbox-sentinel-", dir=queue_root) as outside_text:
            outside = Path(outside_text)
            sentinel = outside / "must-not-be-visible"
            sentinel.write_text("secret sentinel\n", encoding="utf-8")
            with tempfile.TemporaryDirectory(prefix="sandbox-probe-", dir=work_root) as probe_text:
                probe = Path(probe_text)
                source = probe / "source"
                inputs = probe / "inputs"
                result = probe / "result"
                home = probe / "home"
                temporary = probe / "tmp"
                cache = probe / "cache"
                for path in (source, inputs, result, home, temporary, cache):
                    path.mkdir(mode=0o700)
                (source / "readable.txt").write_text("staged\n", encoding="utf-8")
                program = (
                    "import os, pytest, socket; "
                    "assert open('/job/source/readable.txt').read() == 'staged\\n'; "
                    f"assert not os.path.exists({str(sentinel)!r}); "
                    "blocked = False; "
                    "\ntry:\n s=socket.socket(socket.AF_INET,socket.SOCK_STREAM); "
                    "s.settimeout(0.25); blocked=s.connect_ex(('1.1.1.1',53)) != 0; s.close()"
                    "\nexcept OSError:\n blocked=True"
                    "\nassert blocked; "
                    "open('/job/result/probe-ok','w').write('ok\\n')"
                )
                command = bubblewrap_command(
                    executable,
                    [sandbox_python, "-c", program],
                    source_root=source,
                    inputs_root=inputs,
                    result_root=result,
                    home_root=home,
                    temporary_root=temporary,
                    cache_root=cache,
                    job_id="sandbox-self-test",
                    runtime_bindings=runtime_bindings,
                )
                completed = subprocess.run(
                    command,
                    cwd=probe,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env={
                        "HOME": str(home),
                        "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
                        "LANG": "C.UTF-8",
                        "LC_ALL": "C.UTF-8",
                    },
                    timeout=15,
                    check=False,
                )
                marker = result / "probe-ok"
                if completed.returncode != 0 or not marker.is_file():
                    detail = completed.stderr.decode("utf-8", "replace").strip()
                    return False, detail or f"bubblewrap self-test exited {completed.returncode}"
                if marker.read_text(encoding="utf-8") != "ok\n":
                    return False, "bubblewrap self-test result was corrupted"
    except (BrokerError, OSError, subprocess.TimeoutExpired) as exc:
        return False, str(exc)
    return True, "bubblewrap isolation verified"


def _real_declared_output(result_root: Path, relative: Path) -> Path | None:
    return engine.real_declared_output(result_root, relative, BrokerError)


def _real_declared_output_directory(
    result_root: Path,
    relative: Path,
) -> Path | None:
    output = _real_declared_output(result_root, relative)
    return output if output is not None and output.is_dir() else None


def validate_declared_outputs(
    manifest: Mapping[str, object], result_root: Path, *, require_all: bool
) -> None:
    engine.validate_declared_outputs(
        manifest,
        result_root,
        require_all=require_all,
        host=engine.Host.PI,
        error_type=BrokerError,
    )


def _package_output_directories(
    manifest: Mapping[str, object], result_root: Path, maximum_bytes: int
) -> dict[str, str]:
    return engine.package_output_directories(
        manifest,
        result_root,
        maximum_bytes,
        host=engine.Host.PI,
        error_type=BrokerError,
    )


def _result_files(
    manifest: Mapping[str, object],
    result_root: Path,
    maximum_bytes: int,
    artifacts: Mapping[str, str],
) -> list[tuple[str, Path]]:
    expected = {"stdout.txt", "stderr.txt", "execution.json"}
    if manifest.get("execution") is None:
        task_name = str(manifest.get("task", ""))
        if task_name not in LOCAL_BUILTIN_TASKS:
            raise BrokerError("unknown legacy task cannot publish local results")
        result_json = protocol.TASKS[task_name].result_json
        if result_json is not None:
            expected.add(result_json)
    else:
        specification = protocol.validate_execution(manifest.get("execution"))
        expected.update(
            artifacts.get(str(path), str(path)) for path in specification["outputs"]
        )
    return engine.result_files(
        result_root,
        maximum_bytes,
        expected=expected,
        host=engine.Host.PI,
        error_type=BrokerError,
    )


def _failure_results(
    result_root: Path,
    manifest: Mapping[str, object],
    exc: Exception,
) -> tuple[int, dict[str, object]]:
    if result_root.exists():
        shutil.rmtree(result_root)
    result_root.mkdir(parents=True, mode=0o700)
    (result_root / "stdout.txt").touch(mode=0o600)
    (result_root / "stderr.txt").write_text(
        f"van-compute local fallback: {type(exc).__name__}: {exc}\n",
        encoding="utf-8",
    )
    execution = {
        "schema_version": protocol.SCHEMA_VERSION,
        "job_id": manifest.get("id"),
        "task": manifest.get("task"),
        "worker": LOCAL_WORKER,
        "finished_at": utc_now(),
        "exit_code": 70,
        "worker_error": str(exc),
        "placement": "pi-local",
    }
    (result_root / "execution.json").write_text(
        json.dumps(execution, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 70, execution


def _record_local_execution_once(
    root: Path,
    manifest: Mapping[str, object],
    execution: Mapping[str, object],
) -> dict[str, object] | None:
    """Mirror one real Pi fallback run into eligible-local-work telemetry."""
    usage = execution.get("resource_usage")
    duration = execution.get("broker_active_seconds", execution.get("duration_seconds"))
    if not isinstance(usage, dict) or not isinstance(duration, (int, float)):
        return None
    profile = job_profile(manifest)
    if profile is None and str(manifest.get("task", "")) in LOCAL_BUILTIN_TASKS:
        profile = "can-log-batch"
    if profile is None:
        return None
    label = f"pi-local {manifest.get('task', 'task')} {manifest.get('id', 'job')}"
    # record_missed_offload intentionally allocates random event IDs.  The
    # label makes its public API idempotent across a broker restart after the
    # event was written but before worker_finish moved the job.
    for path in (root / "missed").glob("*.json"):
        try:
            prior = queue.load_json(path)
        except queue.QueueError:
            continue
        if prior.get("label") == label:
            return prior
    telemetry_record = MissedOffloadRecord(
        profile=profile,
        label=label,
        reason="worker-unavailable",
        duration_seconds=float(duration),
        cpu_seconds=float(usage.get("cpu_seconds", 0.0)),
        peak_rss_bytes=int(usage.get("peak_rss_bytes", 0)),
        input_bytes=int(execution.get("input_bytes", 0)),
    )
    return queue.record_missed_offload(root, telemetry_record)


def _local_analysis_command(
    args: BrokerConfig, manifest, sandbox: engine.Sandbox, source_root,
    input_paths, input_values, result_root,
):
    host_executables = {"python": args.python, "sqlite3": args.sqlite3}
    profile = job_profile(manifest)
    family = (
        "python" if profile is None else protocol.PROFILE_FAMILIES[str(profile)]
    )
    executable = Path(host_executables[family])
    if not executable.is_file() or not os.access(executable, os.X_OK):
        raise BrokerError(
            f"required {family} executable is unavailable: {executable}"
        )
    sandbox_binary, runtime_bindings = sandbox.executable(
        str(executable), family
    )
    executables = dict(host_executables)
    executables[family] = sandbox_binary
    sandbox_python = (
        sandbox_binary
        if family == "python"
        else sandbox.executable(args.python, "python")[0]
    )
    sandbox_source = sandbox.command_path(source_root, "source")
    sandbox_inputs = [
        sandbox.command_path(path, "inputs") for path in input_paths
    ]
    sandbox_result = sandbox.command_path(result_root, "result")
    command = protocol.build_command(
        str(manifest.get("task", "")),
        python=sandbox_python,
        source_root=sandbox_source,
        input_paths=sandbox_inputs,
        input_values=input_values,
        result_root=sandbox_result,
        arguments=manifest.get("arguments", []),
        execution=manifest.get("execution"),
        executables=executables,
        datasets={},
    )
    return command, runtime_bindings


def execute_claimed_job(
    args: BrokerConfig,
    root: Path,
    manifest: dict[str, object],
    *,
    should_stop: Callable[[], bool] = lambda: False,
) -> dict[str, object]:
    job_id = queue.validate_job_id(str(manifest.get("id", "")))
    lease_token = str(manifest.get("lease_token", ""))
    if not re.fullmatch(r"[0-9a-f]{32}", lease_token):
        raise BrokerError("local job is missing a valid ownership token")
    job_path, authoritative = _require_local_job(root, job_id, lease_token)
    if job_path.name != job_id or authoritative.get("id") != job_id:
        raise BrokerError("claimed manifest id does not match its running directory")
    manifest = authoritative
    eligible, reason = local_eligibility(manifest)
    if not eligible:
        raise BrokerError(reason)
    if manifest.get("placement") != "pi-local":
        raise BrokerError("local job is missing pi-local placement metadata")
    capacity_ok, capacity_reason = work_capacity_eligibility(manifest, args)
    if not capacity_ok:
        raise BrokerError(capacity_reason)
    broker_active_started = time.monotonic()
    work_root = prepare_work_root(args.work_root)
    cache_store = (
        None if args.task_cache_root is None
        else task_cache.TaskCacheStore(args.task_cache_root, args.task_cache_max_bytes)
    )
    with tempfile.TemporaryDirectory(prefix=f"{job_id}-", dir=work_root) as temporary:
        work = Path(temporary)
        source_root = work / "source"
        inputs_root = work / "inputs"
        result_root = work / "result"
        artifacts: dict[str, str] = {}
        try:
            source_preparation_started = time.monotonic()
            _copy_sources(job_path, manifest, source_root)
            source_preparation_seconds = time.monotonic() - source_preparation_started
            input_preparation_started = time.monotonic()
            input_paths, input_values = _copy_inputs(manifest, inputs_root, lease_token)
            input_preparation_seconds = time.monotonic() - input_preparation_started
            result_root.mkdir(mode=0o700)
            home_root = work / "home"
            temporary_root = work / "tmp"
            for path in (home_root, temporary_root):
                path.mkdir(mode=0o700)
            cache_root, cache_scope = task_cache.job_cache(
                cache_store, task_cache.request_for_manifest(manifest), work / "cache"
            )
            sandbox: engine.Sandbox = engine.BubblewrapSandbox(
                args.bwrap,
                source_root,
                inputs_root,
                result_root,
                home_root,
                temporary_root,
                cache_root,
                job_id,
                BrokerError,
            )
            command, runtime_bindings = _local_analysis_command(
                args, manifest, sandbox, source_root, input_paths,
                input_values, result_root,
            )
            sandboxed_command = bubblewrap_command(
                args.bwrap,
                command,
                source_root=source_root,
                inputs_root=inputs_root,
                result_root=result_root,
                home_root=home_root,
                temporary_root=temporary_root,
                cache_root=cache_root,
                job_id=job_id,
                runtime_bindings=runtime_bindings,
            )
            environment = {
                "HOME": str(home_root),
                "TMPDIR": str(temporary_root) + "/",
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            }
            child_command = [
                sys.executable,
                str(Path(limited_child.__file__).resolve()),
                "broker",
                str(args.max_memory_bytes),
                str(args.cpu_seconds),
                str(args.max_result_bytes),
                str(args.max_open_files),
                str(args.nice),
                "--",
                *sandboxed_command,
            ]
            started = utc_now()
            started_monotonic = time.monotonic()

            def supervise(process):
                outcome = _wait_for_process(
                    process,
                    timeout=args.timeout,
                    should_stop=should_stop,
                    work_path=work,
                    minimum_free_bytes=args.min_work_free_bytes + args.max_result_bytes,
                )
                cleaned = terminate_remaining_process_group(process.pid)
                return outcome, cleaned

            # The cache stays locked until the child and its process group are reaped.
            with cache_scope:
                outcome, background_descendants_terminated = engine.run_child(
                    child_command,
                    cwd=work,
                    environment=environment,
                    result_root=result_root,
                    supervise=supervise,
                )
            duration = time.monotonic() - started_monotonic
            resource_usage = _resource_usage(outcome.usage, duration)
            resource_usage.update(
                {
                    "minimum_filesystem_free_bytes": (
                        outcome.minimum_filesystem_free_bytes
                    ),
                    "free_space_watchdog_interval_seconds": RESOURCE_POLL_INTERVAL,
                }
            )
            execution = {
                "schema_version": protocol.SCHEMA_VERSION,
                "job_id": job_id,
                "task": manifest.get("task"),
                "worker": LOCAL_WORKER,
                "placement": "pi-local",
                "started_at": started,
                "finished_at": utc_now(),
                "duration_seconds": round(duration, 6),
                "analysis_seconds": round(duration, 6),
                "exit_code": outcome.exit_code,
                "timed_out": outcome.timed_out,
                "interrupted": outcome.interrupted,
                "resource_limit": outcome.resource_limit,
                "resource_monitor_error": outcome.resource_monitor_error,
                "background_descendants_terminated": background_descendants_terminated,
                "source_preparation_seconds": round(source_preparation_seconds, 6),
                "input_preparation_seconds": round(input_preparation_seconds, 6),
                "command": command,
                "sandbox": "bubblewrap",
                "resource_usage": resource_usage,
                "input_bytes": sum(
                    int(item.get("size", 0))
                    for item in manifest.get("inputs", [])
                    if isinstance(item, dict)
                ),
                "source_bytes": sum(
                    int(item.get("size", 0))
                    for item in manifest.get("sources", [])
                    if isinstance(item, dict)
                ),
            }
            (result_root / "execution.json").write_text(
                json.dumps(execution, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            if (
                outcome.resource_limit is not None
                or outcome.resource_monitor_error is not None
            ):
                # Free space is already under the execution headroom. Remove
                # potentially large task output before publishing only bounded
                # failure metadata back into the queue on the same filesystem.
                shutil.rmtree(result_root)
                result_root.mkdir(mode=0o700)
                (result_root / "stdout.txt").touch(mode=0o600)
                failure_reason = (
                    outcome.resource_limit or outcome.resource_monitor_error
                )
                (result_root / "stderr.txt").write_text(
                    f"van-compute local fallback stopped: {failure_reason}\n",
                    encoding="utf-8",
                )
                (result_root / "execution.json").write_text(
                    json.dumps(execution, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                artifacts = {}
            else:
                validate_declared_outputs(
                    manifest,
                    result_root,
                    require_all=outcome.exit_code == 0,
                )
                packaging_started = time.monotonic()
                artifacts = _package_output_directories(
                    manifest, result_root, args.max_result_bytes
                )
                execution["packaging_seconds"] = round(
                    time.monotonic() - packaging_started, 6
                )
                if artifacts:
                    execution["output_artifacts"] = artifacts
                    (result_root / "execution.json").write_text(
                        json.dumps(execution, indent=2, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
            files = _result_files(
                manifest, result_root, args.max_result_bytes, artifacts
            )
        except Exception as exc:
            exit_code, execution = _failure_results(result_root, manifest, exc)
            artifacts = {}
            files = _result_files(
                manifest, result_root, args.max_result_bytes, artifacts
            )

        else:
            exit_code = outcome.exit_code

        uploaded: list[str] = []
        upload_started = time.monotonic()
        for relative, path in files:
            if relative == "execution.json":
                continue
            with path.open("rb") as handle:
                _put_local_result(root, job_id, lease_token, relative, handle)
            uploaded.append(relative)
        execution["result_upload_seconds_excluding_execution_json"] = round(
            time.monotonic() - upload_started, 6
        )
        execution["broker_active_seconds"] = round(
            time.monotonic() - broker_active_started, 6
        )
        try:
            event = _record_local_execution_once(root, manifest, execution)
        except (queue.QueueError, ValueError, TypeError) as exc:
            event = None
            execution["local_telemetry_error"] = str(exc)
        if event is not None:
            execution["eligible_local_event_id"] = event.get("id")
        (result_root / "execution.json").write_text(
            json.dumps(execution, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        files = _result_files(manifest, result_root, args.max_result_bytes, artifacts)
        for relative, path in files:
            if relative != "execution.json":
                continue
            with path.open("rb") as handle:
                _put_local_result(root, job_id, lease_token, relative, handle)
            uploaded.append(relative)
        try:
            completed = _finish_local_job(
                root, job_id, lease_token, exit_code, uploaded
            )
        finally:
            # worker_finish refreshes all workers for backward compatibility.
            # A Pi-local lease must not make the dashboard or broker believe a
            # remote worker is available.
            (root / "workers" / f"{LOCAL_WORKER}.json").unlink(missing_ok=True)
        result = {
            "ok": exit_code == 0,
            "state": completed.get("state"),
            "job": job_id,
            "worker": LOCAL_WORKER,
            "execution": execution,
            "results": uploaded,
        }
    # After results are published: eviction never delays a job's outcome.
    task_cache.prune_quietly(
        cache_store, 2 * args.min_work_free_bytes, label="van-compute-broker"
    )
    return result


def run_once(
    args: BrokerConfig,
    *,
    now: dt.datetime | None = None,
    health_reader: Callable[[], HealthSnapshot] | None = None,
    should_stop: Callable[[], bool] = lambda: False,
    monitor: HealthMonitor | None = None,
) -> dict[str, object]:
    # Observe CAN before any early return so deferrals (maintenance, a fresh
    # remote lease) keep the quiet chain unbroken.  The monitor is owned by
    # serve() or main(); building one here would reset the chain every poll.
    if monitor is not None:
        monitor.observe_can()
    if health_reader is None:
        if monitor is None:
            raise BrokerError("run_once needs a health monitor or a health reader")
        health_reader = monitor.snapshot
    root = queue.safe_root(args.root)
    work_root = prepare_work_root(args.work_root)
    args.work_root = work_root
    now = now or dt.datetime.now(dt.timezone.utc)
    if queue.maintenance_status(root) is not None:
        return {"action": "deferred", "reason": "queue maintenance is active"}
    running = _running_local_job(root)
    recovered: list[str] = []
    if running is None:
        if queue_supports_lease_tokens():
            recovered = recover_stale_remote_jobs(
                root,
                stale_age=args.stale_running_age,
                now=now,
            )
        remotes = fresh_remote_workers(root, args.remote_max_age, now=now)
        if remotes:
            return {
                "action": "deferred",
                "reason": "fresh remote worker lease",
                "remote_workers": [str(item.get("worker")) for item in remotes],
                "recovered_jobs": recovered,
            }
    try:
        assessment = assess_health(health_reader(), args.health_thresholds)
    except (BrokerError, HealthProbeError) as exc:
        return {"action": "deferred", "reason": f"health probe failed: {exc}"}
    if not assessment.ok:
        return {
            "action": "deferred",
            "reason": "Pi health gate",
            "health": asdict(assessment),
        }
    sandbox_ok, sandbox_reason = bubblewrap_self_test(
        args.bwrap,
        args.python,
        str(work_root),
        str(root),
    )
    if not sandbox_ok:
        return {
            "action": "deferred",
            "reason": f"sandbox self-test failed: {sandbox_reason}",
            "recovered_jobs": recovered,
        }
    manifest, reason = claim_local_job(
        root,
        grace_seconds=args.remote_grace,
        remote_max_age=args.remote_max_age,
        now=now,
        eligibility=lambda candidate: runtime_eligibility(candidate, args),
    )
    if manifest is None:
        return {"action": "deferred", "reason": reason}
    result = execute_claimed_job(
        args,
        root,
        manifest,
        should_stop=should_stop,
    )
    return {"action": "executed", **result}


def run_self_test(args: BrokerConfig) -> dict[str, object]:
    """Verify the effective task sandbox and configured Python runtime."""
    root = queue.safe_root(args.root)
    work_root = prepare_work_root(args.work_root)
    args.work_root = work_root
    throttle_flags = _read_throttled()
    if throttle_flags is None:
        raise BrokerError(
            "Raspberry Pi throttle probe is unavailable in the service boundary"
        )
    ok, reason = bubblewrap_self_test(
        args.bwrap,
        args.python,
        str(work_root),
        str(root),
    )
    if not ok:
        raise BrokerError(f"sandbox self-test failed: {reason}")
    return {
        "ok": True,
        "sandbox": "bubblewrap",
        "python": str(Path(args.python).expanduser()),
        "throttled_flags": throttle_flags,
        "result": reason,
    }


def health_report(config: BrokerConfig, monitor: HealthMonitor) -> dict[str, object]:
    """The `--health` payload: what the gate sees now and what it decides."""
    assessment = assess_health(monitor.snapshot(), config.health_thresholds)
    return {**asdict(assessment), "thresholds": asdict(config.health_thresholds)}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(os.environ.get("VAN_COMPUTE_ROOT", queue.DEFAULT_QUEUE_ROOT)),
    )
    parser.add_argument("--work-root", type=Path, default=DEFAULT_WORK_ROOT)
    parser.add_argument(
        "--task-cache-root",
        type=Path,
        help="persistent task caches (default: <queue root>/task-caches)",
    )
    parser.add_argument(
        "--task-cache-max-bytes", type=int, default=task_cache.PI_DEFAULT_MAX_BYTES
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--once",
        action="store_true",
        help="make one placement decision and exit; first watches CAN for up to "
        "--can-quiet-seconds (stopping early if a transmitter lock appears, which "
        "defers the job)",
    )
    mode.add_argument(
        "--health",
        action="store_true",
        help="print the current health snapshot and assessment as JSON and exit 0 "
        "whether or not the gate passes (CAN quiet time counts from this process "
        "start; takes no queue lock, so it works beside the running service)",
    )
    mode.add_argument(
        "--self-test",
        action="store_true",
        help="verify the effective Bubblewrap boundary and local Python runtime",
    )
    parser.add_argument("--remote-max-age", type=float, default=DEFAULT_REMOTE_MAX_AGE)
    parser.add_argument("--remote-grace", type=float, default=DEFAULT_REMOTE_GRACE)
    parser.add_argument(
        "--stale-running-age", type=float, default=DEFAULT_STALE_RUNNING_AGE
    )
    parser.add_argument("--poll-interval", type=float, default=DEFAULT_POLL_INTERVAL)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--cpu-seconds", type=int, default=DEFAULT_CPU_SECONDS)
    parser.add_argument("--max-memory-bytes", type=int, default=DEFAULT_MAX_MEMORY_BYTES)
    parser.add_argument("--max-result-bytes", type=int, default=DEFAULT_MAX_RESULT_BYTES)
    parser.add_argument(
        "--min-work-free-bytes",
        type=int,
        default=DEFAULT_MIN_WORK_FREE_BYTES,
        help="free disk bytes the Pi fallback must preserve during local work",
    )
    parser.add_argument("--max-open-files", type=int, default=DEFAULT_NOFILE)
    parser.add_argument("--nice", type=int, default=10)
    parser.add_argument(
        "--python",
        default=os.environ.get("VAN_COMPUTE_LOCAL_PYTHON", DEFAULT_LOCAL_PYTHON),
    )
    parser.add_argument("--sqlite3", default="/usr/bin/sqlite3")
    parser.add_argument("--bwrap", default=DEFAULT_BWRAP)
    add_gate_arguments(parser)
    return parser


def _validate_args(args: argparse.Namespace) -> BrokerConfig:
    if not 1 <= args.timeout <= 24 * 60 * 60:
        raise BrokerError("--timeout must be from 1 second through 24 hours")
    if not 1 <= args.cpu_seconds <= 24 * 60 * 60:
        raise BrokerError("--cpu-seconds must be from 1 second through 24 hours")
    if not 256 * 1024 * 1024 <= args.max_memory_bytes <= 1024 * 1024 * 1024:
        raise BrokerError("--max-memory-bytes must be from 256 MiB through 1 GiB")
    if not 1024 <= args.max_result_bytes <= protocol.MAX_RESULT_BYTES:
        raise BrokerError("--max-result-bytes must be from 1 KiB through 128 MiB")
    if not 256 * 1024 * 1024 <= args.min_work_free_bytes <= 1024**4:
        raise BrokerError("--min-work-free-bytes must be from 256 MiB through 1 TiB")
    if not 32 <= args.max_open_files <= 1024:
        raise BrokerError("--max-open-files must be from 32 through 1024")
    if not 0 <= args.nice <= 19:
        raise BrokerError("--nice must be from 0 through 19")
    if (
        not all(
            math.isfinite(value)
            for value in (
                args.remote_max_age,
                args.remote_grace,
                args.stale_running_age,
                args.poll_interval,
            )
        )
        or args.remote_max_age <= 0
        or args.remote_grace < 0
        or args.stale_running_age < args.remote_max_age
        or args.poll_interval <= 0
    ):
        raise BrokerError(
            "worker timing values must be finite and stale-running-age must be at least remote-max-age"
        )
    health_thresholds = validated_thresholds(args, BrokerError)
    if args.task_cache_max_bytes < 1:
        raise BrokerError("--task-cache-max-bytes must be positive")
    if args.task_cache_root is None:
        # Beside the queue the service may already write, wherever --root points.
        args.task_cache_root = args.root / "task-caches"
    return BrokerConfig.from_namespace(args, health_thresholds=health_thresholds)


def _start_task_cache(config: BrokerConfig) -> None:
    """Prepare the cache root beside the work root and prune before any job runs."""
    if config.task_cache_root is None:
        return
    try:
        config.task_cache_root = task_cache.start_store(
            config.task_cache_root,
            config.task_cache_max_bytes,
            prepare_work_root(config.work_root),
            2 * config.min_work_free_bytes,
            label="van-compute-broker",
        )
    except task_cache.TaskCacheError as exc:
        raise BrokerError(str(exc)) from None


def serve(args: BrokerConfig, monitor: HealthMonitor) -> None:
    stopping = False

    def stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    last_message = ""
    while not stopping:
        result = run_once(args, should_stop=lambda: stopping, monitor=monitor)
        rendered = json.dumps(result, sort_keys=True)
        if (
            rendered != last_message
            and result.get("reason") != "no eligible queued jobs"
        ):
            print(rendered, flush=True)
        last_message = rendered
        if stopping:
            break
        if result.get("action") != "executed":
            deadline = time.monotonic() + args.poll_interval
            while not stopping and time.monotonic() < deadline:
                time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))


@contextmanager
def broker_lock(queue_root: Path):
    """Hold the singleton lock for both service and --once execution."""
    root = queue.safe_root(queue_root)
    lock_path = root / ".broker.lock"
    with lock_path.open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise BrokerError("another local fallback broker is already running") from None
        yield


def main(argv: Sequence[str] | None = None) -> int:
    raw = list(argv) if argv is not None else sys.argv[1:]
    os.umask(0o077)
    parser = build_parser()
    args = parser.parse_args(raw)
    try:
        config = _validate_args(args)
        monitor = HealthMonitor(config.can_lock_dir)
        if config.health:
            # Read-only, so no broker lock: it must work beside the service.
            print(json.dumps(health_report(config, monitor), indent=2, sort_keys=True))
            return 0
        with broker_lock(config.root):
            if not config.self_test:
                _start_task_cache(config)
            if config.self_test:
                print(json.dumps(run_self_test(config), indent=2, sort_keys=True))
            elif config.once:
                monitor.wait_for_quiet(config.can_quiet_seconds)
                result = run_once(config, monitor=monitor)
                print(json.dumps(result, indent=2, sort_keys=True))
            else:
                serve(config, monitor)
        return 0
    except (
        BrokerError,
        HealthProbeError,
        queue.QueueError,
        protocol.ProtocolError,
        OSError,
    ) as exc:
        print(f"van-compute-broker: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
