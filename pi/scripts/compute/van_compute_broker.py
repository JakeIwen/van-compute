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
import resource
import secrets
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
from typing import Callable, Mapping, Sequence


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
if (REPOSITORY_ROOT / "shared" / "python").is_dir():
    sys.path.insert(0, str(REPOSITORY_ROOT))


def _import_queue():
    try:
        from pi.scripts.compute import van_compute as queue_module

        return queue_module
    except ModuleNotFoundError:
        import van_compute as queue_module

        return queue_module


queue = _import_queue()
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
DEFAULT_MIN_AVAILABLE_BYTES = 1536 * 1024 * 1024
DEFAULT_MAX_SWAP_FRACTION = 0.20
DEFAULT_MAX_SWAP_BYTES = 512 * 1024 * 1024
DEFAULT_MAX_LOAD_PER_CPU = 1.25
DEFAULT_MAX_TEMPERATURE_C = 75.0
DEFAULT_NOFILE = 256
DEFAULT_NPROC = 256
DEFAULT_BWRAP = "/usr/bin/bwrap"
DEFAULT_LOCAL_PYTHON = "/home/pi/.local/share/van-compute/venv/bin/python3"
RESOURCE_POLL_INTERVAL = 0.5
SAFE_INPUT_RE = re.compile(r"[^A-Za-z0-9_.-]+")
MOUNTINFO_ESCAPE_RE = re.compile(r"\\([0-7]{3})")
MEMORY_BACKED_FILESYSTEMS = frozenset(
    {"tmpfs", "ramfs", "devtmpfs", "hugetlbfs"}
)


class BrokerError(RuntimeError):
    """The broker cannot safely place or execute a job."""


@dataclass(frozen=True)
class HealthSnapshot:
    memory_available_bytes: int
    swap_total_bytes: int
    swap_used_bytes: int
    load_1m: float
    cpu_count: int
    temperature_c: float | None
    throttled_flags: int | None


@dataclass(frozen=True)
class HealthThresholds:
    minimum_available_bytes: int = DEFAULT_MIN_AVAILABLE_BYTES
    maximum_swap_used_fraction: float = DEFAULT_MAX_SWAP_FRACTION
    maximum_swap_used_bytes: int = DEFAULT_MAX_SWAP_BYTES
    maximum_load_per_cpu: float = DEFAULT_MAX_LOAD_PER_CPU
    maximum_temperature_c: float = DEFAULT_MAX_TEMPERATURE_C


@dataclass(frozen=True)
class HealthAssessment:
    ok: bool
    reasons: tuple[str, ...]
    snapshot: HealthSnapshot


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


def _read_meminfo(path: Path = Path("/proc/meminfo")) -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        lines = path.read_text(encoding="ascii").splitlines()
    except OSError as exc:
        raise BrokerError(f"cannot read memory health from {path}: {exc}") from None
    for line in lines:
        name, separator, remainder = line.partition(":")
        if not separator:
            continue
        parts = remainder.split()
        if not parts or not parts[0].isdigit():
            continue
        multiplier = 1024 if len(parts) > 1 and parts[1].lower() == "kb" else 1
        values[name] = int(parts[0]) * multiplier
    if "MemAvailable" not in values:
        raise BrokerError(f"{path} has no MemAvailable value")
    return values


def _read_temperature(path: Path = Path("/sys/class/thermal/thermal_zone0/temp")) -> float | None:
    try:
        raw = path.read_text(encoding="ascii").strip()
    except OSError:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return value / 1000.0 if value > 1000 else value


def _read_throttled(executable: Path = Path("/usr/bin/vcgencmd")) -> int | None:
    if not executable.is_file() or not os.access(executable, os.X_OK):
        return None
    try:
        result = subprocess.run(
            [str(executable), "get_throttled"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    _prefix, separator, value = result.stdout.strip().partition("=")
    if not separator:
        return None
    try:
        return int(value, 0)
    except ValueError:
        return None


def read_health_snapshot() -> HealthSnapshot:
    memory = _read_meminfo()
    swap_total = memory.get("SwapTotal", 0)
    swap_free = memory.get("SwapFree", 0)
    return HealthSnapshot(
        memory_available_bytes=memory["MemAvailable"],
        swap_total_bytes=swap_total,
        swap_used_bytes=max(0, swap_total - swap_free),
        load_1m=float(os.getloadavg()[0]),
        cpu_count=max(1, os.cpu_count() or 1),
        temperature_c=_read_temperature(),
        throttled_flags=_read_throttled(),
    )


def assess_health(
    snapshot: HealthSnapshot,
    thresholds: HealthThresholds,
) -> HealthAssessment:
    reasons: list[str] = []
    if snapshot.memory_available_bytes < thresholds.minimum_available_bytes:
        reasons.append(
            "available memory is below "
            f"{thresholds.minimum_available_bytes // (1024 * 1024)} MiB"
        )
    if snapshot.swap_total_bytes > 0:
        fraction = snapshot.swap_used_bytes / snapshot.swap_total_bytes
        # Small Pi swap partitions can stay nearly full with cold historical
        # pages even when there is no current pressure.  MemAvailable and the
        # service's MemorySwapMax=0 are primary; this secondary gate triggers
        # only after both a relative and a meaningful absolute threshold.
        swap_limit = max(
            thresholds.maximum_swap_used_bytes,
            int(snapshot.swap_total_bytes * thresholds.maximum_swap_used_fraction),
        )
        if snapshot.swap_used_bytes > swap_limit:
            reasons.append(
                f"swap use is {fraction:.1%} ({snapshot.swap_used_bytes // (1024 * 1024)} MiB), "
                f"above {swap_limit // (1024 * 1024)} MiB"
            )
    maximum_load = snapshot.cpu_count * thresholds.maximum_load_per_cpu
    if snapshot.load_1m > maximum_load:
        reasons.append(
            f"1-minute load {snapshot.load_1m:.2f} exceeds {maximum_load:.2f}"
        )
    if (
        snapshot.temperature_c is not None
        and snapshot.temperature_c > thresholds.maximum_temperature_c
    ):
        reasons.append(
            f"temperature {snapshot.temperature_c:.1f} C exceeds "
            f"{thresholds.maximum_temperature_c:.1f} C"
        )
    # Bits 0-3 describe current under-voltage, frequency cap, throttling, and
    # soft-temperature limit.  Historical bits 16-19 do not block new work.
    if snapshot.throttled_flags is not None and snapshot.throttled_flags & 0xF:
        reasons.append(
            f"current Raspberry Pi throttle flags are 0x{snapshot.throttled_flags:x}"
        )
    return HealthAssessment(not reasons, tuple(reasons), snapshot)


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
    manifest: Mapping[str, object], args: argparse.Namespace
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
    manifest: Mapping[str, object], args: argparse.Namespace
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
            history = manifest.get("attempt_history", [])
            if not isinstance(history, list):
                history = []
            history.append(
                {
                    "attempt": manifest.get("attempt"),
                    "worker": worker,
                    "started_at": manifest.get("started_at"),
                    "abandoned_at": now.replace(microsecond=0).isoformat(),
                    "reason": "remote-worker-lease-expired",
                }
            )
            manifest.update(
                {
                    "state": "queued",
                    "attempt_history": history[-20:],
                    "requeued_at": now.replace(microsecond=0).isoformat(),
                    "requeue_reason": "remote-worker-lease-expired",
                }
            )
            for field in (
                "worker",
                "lease_token",
                "placement",
                "placement_reason",
                "started_at",
                "resumed_at",
            ):
                manifest.pop(field, None)
            result_root = path / "result"
            if result_root.is_symlink():
                raise BrokerError(f"job result directory is a symlink: {path.name}")
            if result_root.exists():
                shutil.rmtree(result_root)
            result_root.mkdir(mode=0o700)
            destination = root / "queued" / path.name
            # Move first, then publish the requeued manifest.  A crash between
            # these operations leaves a queued directory with the superseded
            # lease, which the next claimant overwrites before moving it back
            # to running.  Publishing an ownerless manifest in running first
            # would strand the job permanently after a crash.
            os.replace(path, destination)
            queue.atomic_json(destination / "manifest.json", manifest)
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


def _set_limit(kind: int, soft: int, hard: int | None = None) -> None:
    current_soft, current_hard = resource.getrlimit(kind)
    del current_soft
    desired_hard = soft if hard is None else hard
    if current_hard != resource.RLIM_INFINITY:
        desired_hard = min(desired_hard, current_hard)
        soft = min(soft, desired_hard)
    resource.setrlimit(kind, (soft, desired_hard))


def limited_child_main(argv: Sequence[str]) -> int:
    if len(argv) < 8 or argv[6] != "--":
        print("van-compute-broker: invalid internal child invocation", file=sys.stderr)
        return 125
    try:
        memory, cpu, file_size, nofile, nproc, nice = map(int, argv[:6])
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
        _set_limit(resource.RLIMIT_NPROC, nproc)
        command = list(argv[7:])
        if not command:
            raise BrokerError("internal child command is empty")
        os.execvpe(command[0], command, os.environ)
    except (BrokerError, OSError, ValueError) as exc:
        print(f"van-compute-broker child: {exc}", file=sys.stderr)
        return 126
    return 126


def _wait4_nohang(process: subprocess.Popen[bytes]):
    waited_pid, status, usage = os.wait4(process.pid, os.WNOHANG)
    if waited_pid == 0:
        return None
    process.returncode = os.waitstatus_to_exitcode(status)
    return process.returncode, usage


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
    deadline = time.monotonic() + timeout
    next_resource_poll = 0.0
    timed_out = False
    interrupted = False
    resource_limit: str | None = None
    resource_monitor_error: str | None = None
    lowest_free_bytes: int | None = None

    def sample_free_space() -> None:
        nonlocal lowest_free_bytes, resource_limit, resource_monitor_error
        try:
            free_bytes = free_space_reader(work_path)
        except OSError as exc:
            resource_monitor_error = f"free-space watchdog failed: {exc}"
            return
        lowest_free_bytes = (
            free_bytes
            if lowest_free_bytes is None
            else min(lowest_free_bytes, free_bytes)
        )
        if free_bytes < minimum_free_bytes:
            resource_limit = (
                "filesystem free space fell below execution safety threshold "
                f"{minimum_free_bytes} bytes"
            )

    while True:
        completed = _wait4_nohang(process)
        if completed is not None:
            # A fast child may finish between polls after creating substantial
            # output. Sample once more before any packaging or result upload.
            sample_free_space()
            return LocalProcessOutcome(
                137
                if resource_limit is not None
                else 125
                if resource_monitor_error is not None
                else completed[0],
                completed[1],
                timed_out,
                interrupted,
                resource_limit,
                resource_monitor_error,
                lowest_free_bytes,
            )
        if should_stop():
            interrupted = True
            break
        now = time.monotonic()
        if now >= deadline:
            timed_out = True
            break
        if now >= next_resource_poll:
            sample_free_space()
            if resource_limit is not None or resource_monitor_error is not None:
                break
            next_resource_poll = now + RESOURCE_POLL_INTERVAL
        time.sleep(0.1)
    if resource_limit is not None or resource_monitor_error is not None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        waited_pid, status, usage = os.wait4(process.pid, 0)
        if waited_pid != process.pid:
            raise BrokerError("lost track of resource-limited local analysis process")
        process.returncode = os.waitstatus_to_exitcode(status)
        return LocalProcessOutcome(
            137 if resource_limit is not None else 125,
            usage,
            timed_out,
            interrupted,
            resource_limit,
            resource_monitor_error,
            lowest_free_bytes,
        )
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    terminate_deadline = time.monotonic() + 10
    while time.monotonic() < terminate_deadline:
        completed = _wait4_nohang(process)
        if completed is not None:
            return LocalProcessOutcome(
                124 if timed_out else 143,
                completed[1],
                timed_out,
                interrupted,
                resource_limit,
                resource_monitor_error,
                lowest_free_bytes,
            )
        time.sleep(0.1)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    waited_pid, status, usage = os.wait4(process.pid, 0)
    if waited_pid != process.pid:
        raise BrokerError("lost track of local analysis process")
    process.returncode = os.waitstatus_to_exitcode(status)
    return LocalProcessOutcome(
        124 if timed_out else 143,
        usage,
        timed_out,
        interrupted,
        resource_limit,
        resource_monitor_error,
        lowest_free_bytes,
    )


def _process_group_exists(process_group: int) -> bool:
    try:
        os.killpg(process_group, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError as exc:
        raise BrokerError(f"cannot inspect analysis process group {process_group}: {exc}") from None


def terminate_remaining_process_group(
    process_group: int,
    *,
    terminate_grace: float = 2.0,
    kill_grace: float = 2.0,
) -> bool:
    """Stop background descendants left after the command leader exits."""
    if not _process_group_exists(process_group):
        return False
    try:
        os.killpg(process_group, signal.SIGTERM)
    except ProcessLookupError:
        return True
    deadline = time.monotonic() + terminate_grace
    while time.monotonic() < deadline:
        if not _process_group_exists(process_group):
            return True
        time.sleep(0.05)
    try:
        os.killpg(process_group, signal.SIGKILL)
    except ProcessLookupError:
        return True
    deadline = time.monotonic() + kill_grace
    while time.monotonic() < deadline:
        if not _process_group_exists(process_group):
            return True
        time.sleep(0.05)
    raise BrokerError(f"analysis process group {process_group} survived SIGKILL")


def _resource_usage(usage, wall_seconds: float) -> dict[str, object]:
    user_seconds = max(0.0, usage.ru_utime)
    system_seconds = max(0.0, usage.ru_stime)
    cpu_seconds = user_seconds + system_seconds
    peak_rss_bytes = int(usage.ru_maxrss)
    if sys.platform != "darwin":
        peak_rss_bytes *= 1024
    return {
        "user_cpu_seconds": round(user_seconds, 6),
        "system_cpu_seconds": round(system_seconds, 6),
        "cpu_seconds": round(cpu_seconds, 6),
        "average_cpu_percent": (
            round(100 * cpu_seconds / wall_seconds, 2) if wall_seconds > 0 else None
        ),
        "peak_rss_bytes": peak_rss_bytes,
        "minor_page_faults": max(0, usage.ru_minflt),
        "major_page_faults": max(0, usage.ru_majflt),
        "voluntary_context_switches": max(0, usage.ru_nvcsw),
        "involuntary_context_switches": max(0, usage.ru_nivcsw),
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
    """Build a fixed, no-network sandbox around one protocol command."""
    bwrap = Path(executable)
    if not bwrap.is_absolute() or not bwrap.is_file() or not os.access(bwrap, os.X_OK):
        raise BrokerError(f"bubblewrap is required for Pi fallback: {bwrap}")
    wrapped = [
        str(bwrap),
        "--die-with-parent",
        "--new-session",
        "--unshare-user",
        "--unshare-pid",
        "--unshare-net",
        "--unshare-ipc",
        "--unshare-uts",
        "--disable-userns",
        "--uid",
        "0",
        "--gid",
        "0",
        "--cap-drop",
        "ALL",
        "--hostname",
        "van-compute",
        "--proc",
        "/proc",
        "--dev",
        "/dev",
        "--tmpfs",
        "/tmp",
        "--dir",
        "/run",
        "--dir",
        "/etc",
        "--dir",
        "/job",
        "--dir",
        "/job/runtime",
    ]
    # These contain the interpreter, shared libraries, and ordinary system
    # modules.  No user or service data directory is exposed.
    for raw in ("/usr", "/bin", "/sbin", "/lib", "/lib64"):
        path = Path(raw)
        if path.exists():
            wrapped.extend(("--ro-bind", raw, raw))
    for raw in (
        "/etc/ld.so.cache",
        "/etc/ld.so.conf",
        "/etc/ld.so.conf.d",
        "/etc/localtime",
        "/etc/passwd",
        "/etc/group",
        "/etc/python3",
    ):
        path = Path(raw)
        if path.exists():
            wrapped.extend(("--ro-bind", raw, raw))
    for host, sandbox_path in runtime_bindings:
        if not host.is_dir() or host.is_symlink():
            raise BrokerError(f"sandbox runtime is not a real directory: {host}")
        wrapped.extend(("--ro-bind", str(host), sandbox_path))
    wrapped.extend(
        (
            "--ro-bind",
            str(source_root),
            "/job/source",
            "--ro-bind",
            str(inputs_root),
            "/job/inputs",
            "--bind",
            str(result_root),
            "/job/result",
            "--bind",
            str(home_root),
            "/job/home",
            "--bind",
            str(temporary_root),
            "/job/tmp",
            "--bind",
            str(cache_root),
            "/job/cache",
            "--chdir",
            "/job/source",
            "--clearenv",
            "--setenv",
            "HOME",
            "/job/home",
            "--setenv",
            "TMPDIR",
            "/job/tmp/",
            "--setenv",
            "XDG_CACHE_HOME",
            "/job/cache",
            "--setenv",
            "XDG_CONFIG_HOME",
            "/job/home/.config",
            "--setenv",
            "LANG",
            "C.UTF-8",
            "--setenv",
            "LC_ALL",
            "C.UTF-8",
            "--setenv",
            "PATH",
            "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "--setenv",
            "PYTHONPATH",
            "/job/source",
            "--setenv",
            "PYTHONNOUSERSITE",
            "1",
            "--setenv",
            "PYTHONDONTWRITEBYTECODE",
            "1",
            "--setenv",
            "PYTEST_ADDOPTS",
            "-p no:cacheprovider",
            "--setenv",
            "VAN_COMPUTE_JOB_ID",
            job_id,
            "--setenv",
            "VAN_COMPUTE_PLACEMENT",
            "pi-local",
            "--",
            *command,
        )
    )
    return wrapped


def sandbox_executable(executable: str, family: str) -> tuple[str, list[tuple[Path, str]]]:
    """Map a dedicated Python venv into the sandbox without exposing its home."""
    path = Path(executable)
    for system_root in (Path("/usr"), Path("/bin"), Path("/sbin"), Path("/lib")):
        try:
            path.relative_to(system_root)
            return str(path), []
        except ValueError:
            continue
    if family != "python" or path.parent.name != "bin":
        raise BrokerError(f"non-system {family} runtime is not supported: {path}")
    runtime_root = path.parent.parent
    sandbox_root = "/job/runtime/python"
    return f"{sandbox_root}/bin/{path.name}", [(runtime_root, sandbox_root)]


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


def _real_declared_output(
    result_root: Path,
    relative: Path,
) -> Path | None:
    """Return a declared file/directory only when every component is real."""
    try:
        root_info = result_root.lstat()
    except OSError as exc:
        raise BrokerError(f"cannot inspect result root: {exc}") from None
    if stat.S_ISLNK(root_info.st_mode) or not stat.S_ISDIR(root_info.st_mode):
        raise BrokerError("result root is not a real directory")

    current = result_root
    for index, part in enumerate(relative.parts):
        current = current / part
        try:
            info = current.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise BrokerError(
                f"cannot inspect declared output component {current}: {exc}"
            ) from None
        if stat.S_ISLNK(info.st_mode):
            raise BrokerError(
                f"declared output has a symlinked path component: {relative}"
            )
        if index < len(relative.parts) - 1 and not stat.S_ISDIR(info.st_mode):
            return None
        if index == len(relative.parts) - 1:
            if stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode):
                return current
            raise BrokerError(f"declared output is a special file: {relative}")
    return None


def _real_declared_output_directory(
    result_root: Path,
    relative: Path,
) -> Path | None:
    output = _real_declared_output(result_root, relative)
    return output if output is not None and output.is_dir() else None


def validate_declared_outputs(
    manifest: Mapping[str, object],
    result_root: Path,
    *,
    require_all: bool,
) -> None:
    embedded = manifest.get("execution")
    if embedded is None:
        return
    specification = protocol.validate_execution(embedded)
    missing: list[str] = []
    for item in specification["outputs"]:
        relative = Path(str(item))
        if _real_declared_output(result_root, relative) is None:
            missing.append(relative.as_posix())
    if require_all and missing:
        raise BrokerError(
            "successful job omitted declared result(s): " + ", ".join(missing)
        )


def _package_output_directories(
    manifest: Mapping[str, object],
    result_root: Path,
    maximum_bytes: int,
) -> dict[str, str]:
    if manifest.get("execution") is None:
        return {}
    specification = protocol.validate_execution(manifest.get("execution"))
    artifacts: dict[str, str] = {}
    for output_text in specification["outputs"]:
        relative = Path(str(output_text))
        output = _real_declared_output_directory(result_root, relative)
        if output is None:
            continue
        estimated = 4096
        for entry in output.rglob("*"):
            if entry.is_symlink():
                raise BrokerError(f"declared output contains a symlink: {relative}")
            if entry.is_file():
                estimated += entry.stat().st_size
            elif not entry.is_dir():
                raise BrokerError(f"declared output contains a special file: {relative}")
            estimated += 4096
            if estimated > maximum_bytes:
                raise BrokerError(f"declared output exceeds result limit: {relative}")
        archive_relative = relative.with_name(f"{relative.name}.tar.gz")
        archive = result_root / archive_relative
        archive.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if archive.exists() or archive.is_symlink():
            raise BrokerError(f"output archive path already exists: {archive_relative}")
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{archive.name}.", suffix=".partial", dir=archive.parent
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as raw_archive:
                with tarfile.open(
                    fileobj=raw_archive, mode="w:gz", dereference=False
                ) as bundle:
                    bundle.add(output, arcname=relative.as_posix(), recursive=True)
                raw_archive.flush()
                os.fsync(raw_archive.fileno())
                archive_size = os.fstat(raw_archive.fileno()).st_size
            if archive_size > maximum_bytes:
                raise BrokerError(
                    f"declared output archive exceeds result limit: {relative}"
                )
            # os.replace replaces the directory entry itself and never follows
            # a destination symlink created after the collision check.
            os.replace(temporary, archive)
        finally:
            temporary.unlink(missing_ok=True)
        shutil.rmtree(output)
        artifacts[relative.as_posix()] = archive_relative.as_posix()
    return artifacts


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
    files: list[tuple[str, Path]] = []
    total = 0
    for path in sorted(result_root.rglob("*")):
        if path.is_symlink():
            raise BrokerError(f"result contains a symlink: {path}")
        if not path.is_file():
            continue
        relative = path.relative_to(result_root).as_posix()
        if relative not in expected:
            raise BrokerError(f"job produced undeclared result file: {relative}")
        size = path.stat().st_size
        if size > maximum_bytes:
            raise BrokerError(f"result exceeds per-file limit: {relative}")
        total += size
        if total > maximum_bytes:
            raise BrokerError("job results exceed total result limit")
        files.append((relative, path))
    if len(files) > protocol.MAX_OUTPUTS + 3:
        raise BrokerError("job produced too many result files")
    return files


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
    duration = execution.get(
        "broker_active_seconds", execution.get("duration_seconds")
    )
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
    telemetry_args = argparse.Namespace(
        profile=profile,
        label=label,
        reason="worker-unavailable",
        duration_seconds=float(duration),
        cpu_seconds=float(usage.get("cpu_seconds", 0.0)),
        peak_rss_bytes=int(usage.get("peak_rss_bytes", 0)),
        input_bytes=int(execution.get("input_bytes", 0)),
    )
    return queue.record_missed_offload(root, telemetry_args)


def execute_claimed_job(
    args: argparse.Namespace,
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
            input_paths, input_values = _copy_inputs(
                manifest, inputs_root, lease_token
            )
            input_preparation_seconds = time.monotonic() - input_preparation_started
            result_root.mkdir(mode=0o700)
            home_root = work / "home"
            temporary_root = work / "tmp"
            cache_root = work / "cache"
            for path in (home_root, temporary_root, cache_root):
                path.mkdir(mode=0o700)
            host_executables = {"python": args.python, "sqlite3": args.sqlite3}
            profile = job_profile(manifest)
            family = "python" if profile is None else protocol.PROFILE_FAMILIES[str(profile)]
            executable = Path(host_executables[family])
            if not executable.is_file() or not os.access(executable, os.X_OK):
                raise BrokerError(f"required {family} executable is unavailable: {executable}")
            sandbox_binary, runtime_bindings = sandbox_executable(
                str(executable), family
            )
            executables = dict(host_executables)
            executables[family] = sandbox_binary
            sandbox_python = (
                sandbox_binary
                if family == "python"
                else sandbox_executable(args.python, "python")[0]
            )
            sandbox_source = Path("/job/source")
            sandbox_inputs = [Path("/job/inputs") / path.name for path in input_paths]
            sandbox_result = Path("/job/result")
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
                str(Path(__file__).resolve()),
                "__exec__",
                str(args.max_memory_bytes),
                str(args.cpu_seconds),
                str(args.max_result_bytes),
                str(args.max_open_files),
                str(args.max_processes),
                str(args.nice),
                "--",
                *sandboxed_command,
            ]
            stdout_path = result_root / "stdout.txt"
            stderr_path = result_root / "stderr.txt"
            started = utc_now()
            started_monotonic = time.monotonic()
            with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
                process = subprocess.Popen(
                    child_command,
                    cwd=work,
                    env=environment,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout,
                    stderr=stderr,
                    start_new_session=True,
                )
                outcome = _wait_for_process(
                    process,
                    timeout=args.timeout,
                    should_stop=should_stop,
                    work_path=work,
                    minimum_free_bytes=(
                        args.min_work_free_bytes + args.max_result_bytes
                    ),
                )
                background_descendants_terminated = terminate_remaining_process_group(
                    process.pid
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
            if outcome.resource_limit is not None or outcome.resource_monitor_error is not None:
                # Free space is already under the execution headroom. Remove
                # potentially large task output before publishing only bounded
                # failure metadata back into the queue on the same filesystem.
                shutil.rmtree(result_root)
                result_root.mkdir(mode=0o700)
                (result_root / "stdout.txt").touch(mode=0o600)
                failure_reason = outcome.resource_limit or outcome.resource_monitor_error
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
            files = _result_files(manifest, result_root, args.max_result_bytes, artifacts)
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
                _put_local_result(
                    root, job_id, lease_token, relative, handle
                )
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
        return {
            "ok": exit_code == 0,
            "state": completed.get("state"),
            "job": job_id,
            "worker": LOCAL_WORKER,
            "execution": execution,
            "results": uploaded,
        }


def run_once(
    args: argparse.Namespace,
    *,
    now: dt.datetime | None = None,
    health_reader: Callable[[], HealthSnapshot] = read_health_snapshot,
    should_stop: Callable[[], bool] = lambda: False,
) -> dict[str, object]:
    root = queue.safe_root(args.root)
    args.work_root = prepare_work_root(args.work_root)
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
    except BrokerError as exc:
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
        str(args.work_root),
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


def run_self_test(args: argparse.Namespace) -> dict[str, object]:
    """Verify the effective task sandbox and configured Python runtime."""
    root = queue.safe_root(args.root)
    args.work_root = prepare_work_root(args.work_root)
    throttle_flags = _read_throttled()
    if throttle_flags is None:
        raise BrokerError(
            "Raspberry Pi throttle probe is unavailable in the service boundary"
        )
    ok, reason = bubblewrap_self_test(
        args.bwrap,
        args.python,
        str(args.work_root),
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(os.environ.get("VAN_COMPUTE_ROOT", queue.DEFAULT_QUEUE_ROOT)),
    )
    parser.add_argument("--work-root", type=Path, default=DEFAULT_WORK_ROOT)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--once", action="store_true", help="make one placement decision and exit"
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
    parser.add_argument("--max-processes", type=int, default=DEFAULT_NPROC)
    parser.add_argument("--nice", type=int, default=10)
    parser.add_argument(
        "--python",
        default=os.environ.get("VAN_COMPUTE_LOCAL_PYTHON", DEFAULT_LOCAL_PYTHON),
    )
    parser.add_argument("--sqlite3", default="/usr/bin/sqlite3")
    parser.add_argument("--bwrap", default=DEFAULT_BWRAP)
    parser.add_argument(
        "--min-available-memory-mb",
        type=int,
        default=DEFAULT_MIN_AVAILABLE_BYTES // (1024 * 1024),
    )
    parser.add_argument("--max-swap-used", type=float, default=DEFAULT_MAX_SWAP_FRACTION)
    parser.add_argument(
        "--max-swap-used-mb",
        type=int,
        default=DEFAULT_MAX_SWAP_BYTES // (1024 * 1024),
    )
    parser.add_argument("--max-load-per-cpu", type=float, default=DEFAULT_MAX_LOAD_PER_CPU)
    parser.add_argument("--max-temperature-c", type=float, default=DEFAULT_MAX_TEMPERATURE_C)
    return parser


def _validate_args(args: argparse.Namespace) -> None:
    if not 1 <= args.timeout <= 24 * 60 * 60:
        raise BrokerError("--timeout must be from 1 second through 24 hours")
    if not 1 <= args.cpu_seconds <= 24 * 60 * 60:
        raise BrokerError("--cpu-seconds must be from 1 second through 24 hours")
    if not 256 * 1024 * 1024 <= args.max_memory_bytes <= 1024 * 1024 * 1024:
        raise BrokerError("--max-memory-bytes must be from 256 MiB through 1 GiB")
    if not 1024 <= args.max_result_bytes <= protocol.MAX_RESULT_BYTES:
        raise BrokerError("--max-result-bytes must be from 1 KiB through 128 MiB")
    if not 256 * 1024 * 1024 <= args.min_work_free_bytes <= 1024**4:
        raise BrokerError(
            "--min-work-free-bytes must be from 256 MiB through 1 TiB"
        )
    if not 32 <= args.max_open_files <= 1024:
        raise BrokerError("--max-open-files must be from 32 through 1024")
    if not 16 <= args.max_processes <= 512:
        raise BrokerError("--max-processes must be from 16 through 512")
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
    if args.min_available_memory_mb < 256:
        raise BrokerError("--min-available-memory-mb must be at least 256")
    if not math.isfinite(args.max_swap_used) or not 0 <= args.max_swap_used <= 1:
        raise BrokerError("--max-swap-used must be from 0 through 1")
    if not 0 <= args.max_swap_used_mb <= 16 * 1024:
        raise BrokerError("--max-swap-used-mb must be from 0 through 16384")
    if not math.isfinite(args.max_load_per_cpu) or args.max_load_per_cpu <= 0:
        raise BrokerError("--max-load-per-cpu must be positive")
    if not math.isfinite(args.max_temperature_c) or not 40 <= args.max_temperature_c <= 100:
        raise BrokerError("--max-temperature-c must be from 40 through 100")
    args.health_thresholds = HealthThresholds(
        minimum_available_bytes=args.min_available_memory_mb * 1024 * 1024,
        maximum_swap_used_fraction=args.max_swap_used,
        maximum_swap_used_bytes=args.max_swap_used_mb * 1024 * 1024,
        maximum_load_per_cpu=args.max_load_per_cpu,
        maximum_temperature_c=args.max_temperature_c,
    )


def serve(args: argparse.Namespace) -> None:
    stopping = False

    def stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    last_message = ""
    while not stopping:
        result = run_once(args, should_stop=lambda: stopping)
        rendered = json.dumps(result, sort_keys=True)
        if rendered != last_message and result.get("reason") != "no eligible queued jobs":
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
    if raw and raw[0] == "__exec__":
        return limited_child_main(raw[1:])
    os.umask(0o077)
    parser = build_parser()
    args = parser.parse_args(raw)
    try:
        _validate_args(args)
        with broker_lock(args.root):
            if args.self_test:
                print(json.dumps(run_self_test(args), indent=2, sort_keys=True))
            elif args.once:
                print(json.dumps(run_once(args), indent=2, sort_keys=True))
            else:
                serve(args)
        return 0
    except (BrokerError, queue.QueueError, protocol.ProtocolError, OSError) as exc:
        print(f"van-compute-broker: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
