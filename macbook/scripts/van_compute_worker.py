#!/usr/bin/env python3
"""Pull and execute allowlisted vanpi analysis jobs on this Mac.

The worker initiates every connection.  It needs ordinary key-based SSH access
to vanpi, but the Mac does not need Remote Login enabled.  Commands are built as
argument vectors from the shared task catalog and never passed through a local
shell.  Service mode provides ten equal, persistent worker slots.  The job HOME,
temporary directory, and environment are isolated.  Repo-defined tasks also
fail closed unless the installer-validated ``sandbox-exec`` profile is active;
an explicitly named emergency escape hatch exists because that deprecated
macOS facility may not survive a future OS release.
"""

from __future__ import annotations

import argparse
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import resource
import shlex
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from typing import BinaryIO, Callable, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
from shared.python import van_compute_protocol as protocol


DEFAULT_HOST = "pi@vanpi"
DEFAULT_REMOTE_CLI = "/home/pi/scripts/compute/van_compute.py"
DEFAULT_PRIVATE_ROOT = Path.home() / "Library" / "Caches" / "van-compute"
DEFAULT_WORK_ROOT = DEFAULT_PRIVATE_ROOT / "jobs"
DEFAULT_CONTROL_PATH = DEFAULT_PRIVATE_ROOT / "ssh" / "control.sock"
DEFAULT_TIMEOUT = 3600
DEFAULT_MAX_RESULT_BYTES = protocol.MAX_RESULT_BYTES
DEFAULT_MAX_MEMORY_BYTES = 2 * 1024 * 1024 * 1024
DEFAULT_MAX_PROCESSES = 256
DEFAULT_MIN_FREE_BYTES = 5 * 1024 * 1024 * 1024
DEFAULT_MIN_MEMORY_HEADROOM_BYTES = 4 * 1024 * 1024 * 1024
DEFAULT_HEARTBEAT_INTERVAL = 15.0
DEFAULT_POLL_INTERVAL = 15.0
SCHEDULER_SLOTS = 10
SSH_CONTROL_CONNECTIONS = 4
RESOURCE_POLL_INTERVAL = 1.0
_PROCESS_TABLE_LOCK = threading.Lock()
_PROCESS_TABLE_SAMPLED_AT = 0.0
_PROCESS_TABLE_GROUPS: dict[int, tuple[int, int]] = {}
_AVAILABLE_MEMORY_LOCK = threading.Lock()
_AVAILABLE_MEMORY_SAMPLED_AT = 0.0
_AVAILABLE_MEMORY_BYTES = 0
COPY_CHUNK = 1024 * 1024
DATASET_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
JOB_ID_RE = re.compile(r"\d{8}T\d{6}Z-[0-9a-f]{8}")
TEXT_RESULT_SUFFIXES = {
    ".csv",
    ".java",
    ".json",
    ".jsonl",
    ".kt",
    ".log",
    ".md",
    ".py",
    ".sql",
    ".tsv",
    ".txt",
    ".xml",
    ".yaml",
    ".yml",
}


class WorkerError(RuntimeError):
    pass


class WorkerShutdown(RuntimeError):
    """Leave a claimed lease unfinished so the exact slot can resume it."""


@dataclass(frozen=True)
class AnalysisOutcome:
    exit_code: int
    usage: object
    timed_out: bool
    interrupted: bool
    resource_limit: str | None
    resource_monitor_error: str | None
    peak_process_group_rss_bytes: int
    peak_process_count: int
    minimum_filesystem_free_bytes: int | None = None


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def bounded_seconds(started_monotonic: float) -> float:
    return round(min(7 * 24 * 60 * 60, max(0.0, time.monotonic() - started_monotonic)), 6)


def safe_filename(name: str, index: int) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", Path(name).name).strip("._")
    return f"{index:03d}-{cleaned or 'input'}"


def manifest_lease_token(manifest: Mapping[str, object]) -> str | None:
    token = manifest.get("lease_token")
    if token is None:
        return None
    if not isinstance(token, str) or not re.fullmatch(r"[0-9a-f]{32}", token):
        raise WorkerError("job manifest has an invalid lease token")
    return token


def add_lease_argument(arguments: list[str], lease_token: str | None) -> list[str]:
    if lease_token is not None:
        arguments.extend(("--lease-token", lease_token))
    return arguments


class SSHMultiplexer:
    """Own one SSH ControlMaster shared by every scheduler slot."""

    def __init__(
        self,
        host: str,
        control_path: Path,
        *,
        ssh: str = "/usr/bin/ssh",
        connect_timeout: int = 5,
    ) -> None:
        self.host = host
        self.control_path = control_path.expanduser().resolve()
        self.ssh = ssh
        self.connect_timeout = connect_timeout
        self._lock = threading.Lock()
        self._process: subprocess.Popen[bytes] | None = None

    def _master_arguments(self) -> list[str]:
        return [
            self.ssh,
            "-N",
            "-M",
            "-S",
            str(self.control_path),
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={self.connect_timeout}",
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=2",
            self.host,
        ]

    def _check_locked(self) -> bool:
        if self._process is None or self._process.poll() is not None:
            return False
        check = subprocess.run(
            [
                self.ssh,
                "-S",
                str(self.control_path),
                "-O",
                "check",
                self.host,
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=max(2, self.connect_timeout),
            check=False,
        )
        return check.returncode == 0

    def ensure(self) -> None:
        with self._lock:
            if self._check_locked():
                return
            self._stop_locked()
            self.control_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(self.control_path.parent, 0o700)
            self.control_path.unlink(missing_ok=True)
            self._process = subprocess.Popen(
                self._master_arguments(),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            deadline = time.monotonic() + self.connect_timeout + 5
            while time.monotonic() < deadline:
                if self._process.poll() is not None:
                    detail = b""
                    if self._process.stderr is not None:
                        detail = self._process.stderr.read()
                    self._process = None
                    raise WorkerError(
                        detail.decode("utf-8", "replace").strip()
                        or "SSH ControlMaster exited before becoming ready"
                    )
                if self.control_path.exists() and self._check_locked():
                    return
                time.sleep(0.05)
            self._stop_locked()
            raise WorkerError("timed out starting the SSH ControlMaster")

    def client_arguments(self) -> list[str]:
        self.ensure()
        return ["-S", str(self.control_path), "-o", "ControlMaster=no"]

    def invalidate(self) -> None:
        with self._lock:
            self._stop_locked()

    def _stop_locked(self) -> None:
        process = self._process
        if process is not None and process.poll() is None and self.control_path.exists():
            subprocess.run(
                [
                    self.ssh,
                    "-S",
                    str(self.control_path),
                    "-O",
                    "exit",
                    self.host,
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=5,
                check=False,
            )
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if process is not None and process.stderr is not None:
            process.stderr.close()
        self._process = None
        self.control_path.unlink(missing_ok=True)

    def close(self) -> None:
        with self._lock:
            self._stop_locked()

    def __enter__(self) -> "SSHMultiplexer":
        self.ensure()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class RemoteQueue:
    def __init__(
        self,
        host: str,
        remote_cli: str,
        worker: str,
        *,
        ssh: str = "/usr/bin/ssh",
        remote_root: str | None = None,
        connect_timeout: int = 5,
        multiplexer: SSHMultiplexer | None = None,
    ) -> None:
        self.host = host
        self.remote_cli = remote_cli
        self.worker = worker
        self.ssh = ssh
        self.remote_root = remote_root
        self.connect_timeout = connect_timeout
        self.multiplexer = multiplexer

    def _remote_arguments(self, *arguments: str) -> list[str]:
        result = [self.remote_cli]
        if self.remote_root:
            result.extend(("--root", self.remote_root))
        result.extend(arguments)
        return result

    def _ssh_arguments(self, *arguments: str) -> list[str]:
        remote_command = shlex.join(self._remote_arguments(*arguments))
        result = [
            self.ssh,
        ]
        if self.multiplexer is not None:
            result.extend(self.multiplexer.client_arguments())
        result.extend([
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={self.connect_timeout}",
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=2",
            self.host,
            remote_command,
        ])
        return result

    def _notice_ssh_failure(self, returncode: int) -> None:
        if returncode == 255 and self.multiplexer is not None:
            self.multiplexer.invalidate()

    def json_command(self, *arguments: str, input_file: BinaryIO | None = None) -> dict[str, object]:
        result = subprocess.run(
            self._ssh_arguments(*arguments),
            stdin=input_file,
            capture_output=True,
            text=input_file is None,
            timeout=None if input_file is not None else 90,
            check=False,
        )
        if result.returncode != 0:
            self._notice_ssh_failure(result.returncode)
            stderr = result.stderr
            if isinstance(stderr, bytes):
                stderr = stderr.decode("utf-8", "replace")
            raise WorkerError((stderr or f"remote exit status {result.returncode}").strip())
        stdout = result.stdout
        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", "replace")
        try:
            payload = json.loads(stdout)
        except (TypeError, json.JSONDecodeError) as exc:
            raise WorkerError(f"remote command returned invalid JSON: {exc}") from None
        if not isinstance(payload, dict):
            raise WorkerError("remote command did not return a JSON object")
        return payload

    def heartbeat(
        self,
        *,
        slots_total: int | None = None,
        slots_busy: int | None = None,
    ) -> dict[str, object]:
        arguments = [
            "worker",
            "heartbeat",
            "--worker",
            self.worker,
            "--protocol-version",
            str(protocol.WORKER_PROTOCOL_VERSION),
        ]
        if slots_total is not None or slots_busy is not None:
            if slots_total is None or slots_busy is None:
                raise WorkerError("capacity heartbeat requires total and busy slots")
            arguments.extend(
                ("--slots-total", str(slots_total), "--slots-busy", str(slots_busy))
            )
        return self.json_command(*arguments)

    def claim(self) -> dict[str, object] | None:
        payload = self.json_command(
            "worker",
            "claim",
            "--worker",
            self.worker,
            "--protocol-version",
            str(protocol.WORKER_PROTOCOL_VERSION),
        )
        job = payload.get("job")
        if job is None:
            return None
        if not isinstance(job, dict):
            raise WorkerError("claim returned an invalid job")
        return job

    def stream_to_file(self, arguments: Sequence[str], destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.partial")
        try:
            with temporary.open("wb") as output:
                process = subprocess.run(
                    self._ssh_arguments(*arguments),
                    stdout=output,
                    stderr=subprocess.PIPE,
                    timeout=None,
                    check=False,
                )
            if process.returncode != 0:
                self._notice_ssh_failure(process.returncode)
                detail = process.stderr.decode("utf-8", "replace").strip()
                raise WorkerError(detail or f"stream failed with status {process.returncode}")
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)

    def stream_source_bundle(
        self,
        job_id: str,
        destination: Path,
        *,
        lease_token: str | None = None,
    ) -> None:
        self.stream_to_file(
            add_lease_argument(
                [
                    "worker", "stream", job_id, "--worker", self.worker,
                    "--kind", "source-bundle",
                ],
                lease_token,
            ),
            destination,
        )

    def stream_input(
        self,
        job_id: str,
        index: int,
        destination: Path,
        *,
        lease_token: str | None = None,
    ) -> None:
        self.stream_to_file(
            add_lease_argument(
                [
                    "worker", "stream", job_id, "--worker", self.worker,
                    "--kind", "input", "--index", str(index),
                ],
                lease_token,
            ),
            destination,
        )

    def put_result(
        self,
        job_id: str,
        relative: str,
        path: Path,
        *,
        lease_token: str | None = None,
    ) -> dict[str, object]:
        with path.open("rb") as handle:
            return self.json_command(
                *add_lease_argument(
                    [
                        "worker", "put-result", job_id, "--worker", self.worker,
                        "--path", relative,
                    ],
                    lease_token,
                ),
                input_file=handle,
            )

    def finish(
        self,
        job_id: str,
        exit_code: int,
        result_files: Sequence[str],
        *,
        lease_token: str | None = None,
    ) -> dict[str, object]:
        arguments = [
            "worker", "finish", job_id, "--worker", self.worker, "--exit-code", str(exit_code)
        ]
        for relative in result_files:
            arguments.extend(("--result-file", relative))
        add_lease_argument(arguments, lease_token)
        return self.json_command(*arguments)


def safe_bundle_member_path(name: str) -> PurePosixPath:
    normalized_name = name.rstrip("/")
    relative = PurePosixPath(normalized_name)
    if (
        not normalized_name
        or normalized_name == "."
        or relative.is_absolute()
        or ".." in relative.parts
        or "." in relative.parts
        or relative.as_posix() != normalized_name
    ):
        raise WorkerError(f"source bundle has an unsafe member path: {name!r}")
    return relative


def extract_source_bundle(
    archive: Path,
    source_root: Path,
    sources: Sequence[dict[str, object]],
) -> None:
    expected: dict[str, dict[str, object]] = {}
    allowed_directories: set[str] = set()
    for record in sources:
        if not isinstance(record, dict):
            raise WorkerError("job has an invalid source record")
        relative_text = str(record.get("path", ""))
        relative = safe_bundle_member_path(relative_text)
        if relative_text in expected:
            raise WorkerError(f"job repeats source path: {relative_text}")
        expected[relative_text] = record
        allowed_directories.update(
            parent.as_posix()
            for parent in relative.parents
            if parent.as_posix() != "."
        )

    extracted: set[str] = set()
    seen_directories: set[str] = set()
    try:
        with tarfile.open(archive, mode="r:*") as bundle:
            for member in bundle:
                relative = safe_bundle_member_path(member.name)
                relative_text = relative.as_posix()
                if member.isdir():
                    normalized = relative_text.rstrip("/")
                    if normalized not in allowed_directories or normalized in seen_directories:
                        raise WorkerError(
                            f"source bundle has an extra member: {member.name}"
                        )
                    seen_directories.add(normalized)
                    continue
                if member.name.endswith("/"):
                    raise WorkerError(
                        f"source bundle member has an invalid file name: {member.name}"
                    )
                if not member.isfile():
                    raise WorkerError(
                        f"source bundle member is not a regular file: {member.name}"
                    )
                if relative_text not in expected or relative_text in extracted:
                    raise WorkerError(
                        f"source bundle has an extra or duplicate member: {member.name}"
                    )
                record = expected[relative_text]
                size = record.get("size")
                digest_text = record.get("sha256")
                if (
                    isinstance(size, bool)
                    or not isinstance(size, int)
                    or size < 0
                    or not isinstance(digest_text, str)
                    or not re.fullmatch(r"[0-9a-f]{64}", digest_text)
                    or member.size != size
                ):
                    raise WorkerError(
                        f"source bundle metadata mismatch: {relative_text}"
                    )
                input_stream = bundle.extractfile(member)
                if input_stream is None:
                    raise WorkerError(f"cannot read source bundle member: {relative_text}")
                destination = source_root.joinpath(*relative.parts)
                destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                temporary = destination.with_name(f".{destination.name}.partial")
                digest = hashlib.sha256()
                written = 0
                try:
                    with input_stream, temporary.open("xb") as output:
                        while chunk := input_stream.read(COPY_CHUNK):
                            written += len(chunk)
                            if written > size:
                                raise WorkerError(
                                    f"source bundle member exceeds manifest size: {relative_text}"
                                )
                            digest.update(chunk)
                            output.write(chunk)
                    if written != size or digest.hexdigest() != digest_text:
                        raise WorkerError(
                            f"source verification failed: {relative_text}"
                        )
                    os.replace(temporary, destination)
                finally:
                    temporary.unlink(missing_ok=True)
                extracted.add(relative_text)
    except (tarfile.TarError, OSError) as exc:
        raise WorkerError(f"invalid source bundle: {exc}") from None
    missing = sorted(set(expected) - extracted)
    if missing:
        raise WorkerError("source bundle is missing member(s): " + ", ".join(missing))


def prepare_job(remote: RemoteQueue, manifest: dict[str, object], job_root: Path) -> tuple[Path, list[Path], list[object | None]]:
    job_id = str(manifest.get("id", ""))
    if not JOB_ID_RE.fullmatch(job_id):
        raise WorkerError("job manifest has an invalid id")
    lease_token = manifest_lease_token(manifest)
    source_root = job_root / "source"
    inputs_root = job_root / "inputs"
    # Several valid profiles (SQLite, corpus search, JADX) deliberately have
    # no snapshotted repository sources. Their execution cwd and input staging
    # roots must still exist.
    source_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    inputs_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    sources = manifest.get("sources")
    inputs = manifest.get("inputs")
    if not isinstance(sources, list) or not isinstance(inputs, list):
        raise WorkerError("job manifest is missing sources or inputs")

    if sources:
        archive = job_root / ".source-bundle.tar"
        remote.stream_source_bundle(job_id, archive, lease_token=lease_token)
        try:
            extract_source_bundle(archive, source_root, sources)
        finally:
            archive.unlink(missing_ok=True)

    input_paths: list[Path] = []
    values: list[object | None] = []
    for index, record in enumerate(inputs):
        if not isinstance(record, dict) or record.get("index") != index:
            raise WorkerError("job has an invalid input record")
        destination = inputs_root / safe_filename(str(record.get("name", "input")), index)
        remote.stream_input(job_id, index, destination, lease_token=lease_token)
        if destination.stat().st_size != record.get("size"):
            raise WorkerError(f"input size verification failed for index {index}")
        input_paths.append(destination)
        values.append(record.get("value"))
    return source_root, input_paths, values


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


def child_resource_usage(usage, wall_seconds: float) -> dict[str, object]:
    """Return usage for one waited job process, safe under concurrent slots."""
    user_seconds = max(0.0, usage.ru_utime)
    system_seconds = max(0.0, usage.ru_stime)
    cpu_seconds = user_seconds + system_seconds
    # Darwin reports ru_maxrss in bytes; Linux and most BSD-derived Python
    # builds report KiB.  The production worker is Darwin, but keeping the
    # conversion explicit makes local protocol tests portable.
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
        "scope": "wait4 resource usage for the analysis process",
        "peak_rss_note": (
            "wait4 maximum resident set; concurrent descendant RSS is not summed"
        ),
    }


def _wait4_nohang(process: subprocess.Popen[bytes]):
    waited_pid, status, usage = os.wait4(process.pid, os.WNOHANG)
    if waited_pid == 0:
        return None
    process.returncode = os.waitstatus_to_exitcode(status)
    return process.returncode, usage


def process_group_exists(group_id: int) -> bool:
    try:
        os.killpg(group_id, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def process_group_resources(group_id: int) -> tuple[int, int]:
    """Return aggregate RSS/count from one shared process-table sample."""
    global _PROCESS_TABLE_GROUPS, _PROCESS_TABLE_SAMPLED_AT
    with _PROCESS_TABLE_LOCK:
        now = time.monotonic()
        if (
            group_id not in _PROCESS_TABLE_GROUPS
            or now - _PROCESS_TABLE_SAMPLED_AT >= RESOURCE_POLL_INTERVAL
        ):
            try:
                completed = subprocess.run(
                    ["/bin/ps", "-axo", "pgid=,rss="],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=5,
                    check=False,
                )
            except (OSError, subprocess.SubprocessError) as exc:
                raise WorkerError(
                    f"resource watchdog could not run ps: {exc}"
                ) from None
            if completed.returncode != 0:
                detail = completed.stderr.strip()
                raise WorkerError(
                    detail or f"resource watchdog ps exited {completed.returncode}"
                )
            groups: dict[int, tuple[int, int]] = {}
            for line in completed.stdout.splitlines():
                fields = line.split()
                if len(fields) != 2:
                    continue
                try:
                    process_group, resident_kib = map(int, fields)
                except ValueError:
                    continue
                prior_rss, prior_count = groups.get(process_group, (0, 0))
                groups[process_group] = (
                    prior_rss + max(0, resident_kib) * 1024,
                    prior_count + 1,
                )
            _PROCESS_TABLE_GROUPS = groups
            _PROCESS_TABLE_SAMPLED_AT = now
        return _PROCESS_TABLE_GROUPS.get(group_id, (0, 0))


def filesystem_free_bytes(path: Path) -> int:
    return max(0, shutil.disk_usage(path).free)


def physical_memory_bytes() -> int:
    """Return installed physical memory without starting another process."""
    try:
        pages = int(os.sysconf("SC_PHYS_PAGES"))
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
    except (OSError, ValueError) as exc:
        raise WorkerError(f"cannot determine physical memory: {exc}") from None
    total = pages * page_size
    if total <= 0:
        raise WorkerError("cannot determine physical memory")
    return total


def system_available_memory_bytes() -> int:
    """Return reclaimable host memory from one shared, short-lived sample."""
    global _AVAILABLE_MEMORY_BYTES, _AVAILABLE_MEMORY_SAMPLED_AT
    with _AVAILABLE_MEMORY_LOCK:
        now = time.monotonic()
        if now - _AVAILABLE_MEMORY_SAMPLED_AT < RESOURCE_POLL_INTERVAL:
            return _AVAILABLE_MEMORY_BYTES
        if sys.platform == "darwin":
            try:
                completed = subprocess.run(
                    ["/usr/bin/vm_stat"],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=5,
                    check=False,
                )
            except (OSError, subprocess.SubprocessError) as exc:
                raise WorkerError(
                    f"cannot inspect available system memory: {exc}"
                ) from None
            if completed.returncode != 0:
                raise WorkerError(
                    completed.stderr.strip()
                    or f"vm_stat exited {completed.returncode}"
                )
            first, *rows = completed.stdout.splitlines()
            match = re.search(r"page size of (\d+) bytes", first)
            if match is None:
                raise WorkerError("vm_stat did not report its page size")
            page_size = int(match.group(1))
            pages: dict[str, int] = {}
            for row in rows:
                name, separator, raw_value = row.partition(":")
                if not separator:
                    continue
                value = raw_value.strip().rstrip(".")
                if value.isdigit():
                    pages[name.strip()] = int(value)
            available_pages = sum(
                pages.get(name, 0)
                for name in (
                    "Pages free",
                    "Pages inactive",
                    "Pages speculative",
                )
            )
            available = available_pages * page_size
        elif sys.platform.startswith("linux"):
            try:
                records = Path("/proc/meminfo").read_text(encoding="utf-8")
            except OSError as exc:
                raise WorkerError(
                    f"cannot inspect available system memory: {exc}"
                ) from None
            match = re.search(r"^MemAvailable:\s+(\d+)\s+kB$", records, re.MULTILINE)
            if match is None:
                raise WorkerError("/proc/meminfo has no MemAvailable record")
            available = int(match.group(1)) * 1024
        else:
            # The production worker is Darwin. Fail conservatively on other
            # test/development Unix platforms rather than claiming free memory.
            available = physical_memory_bytes()
        if available < 0:
            raise WorkerError("available system memory is invalid")
        _AVAILABLE_MEMORY_BYTES = available
        _AVAILABLE_MEMORY_SAMPLED_AT = now
        return available


def manifest_record_bytes(records: object) -> int:
    if not isinstance(records, list):
        return 0
    return sum(
        value
        for item in records
        if isinstance(item, dict)
        and not isinstance((value := item.get("size")), bool)
        and isinstance(value, int)
        and value >= 0
    )


def job_disk_reservation_bytes(
    manifest: Mapping[str, object], maximum_result_bytes: int
) -> int:
    """Conservatively bound a job's peak private-workspace footprint."""
    sources = manifest_record_bytes(manifest.get("sources"))
    inputs = manifest_record_bytes(manifest.get("inputs"))
    # Preparation briefly stores both the normalized source tar and extracted
    # files. Packaging can briefly store a declared directory and its archive.
    return max(2 * sources + inputs, sources + inputs + 2 * maximum_result_bytes)


class ResourceReservation:
    def __init__(
        self,
        manager: "SchedulerResourceManager",
        disk_bytes: int,
        memory_bytes: int,
    ) -> None:
        self._manager = manager
        self.disk_bytes = disk_bytes
        self.memory_bytes = memory_bytes
        self._released = False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._manager.release(self)


class SchedulerResourceManager:
    """Coordinate conservative disk/RAM admission across all ten slots."""

    def __init__(
        self,
        work_root: Path,
        *,
        minimum_free_bytes: int,
        maximum_result_bytes: int,
        maximum_job_memory_bytes: int,
        minimum_memory_headroom_bytes: int,
        free_space_reader: Callable[[Path], int] = filesystem_free_bytes,
        physical_memory_reader: Callable[[], int] = physical_memory_bytes,
        available_memory_reader: Callable[[], int] = system_available_memory_bytes,
        group_resource_reader: Callable[[int], tuple[int, int]] | None = None,
    ) -> None:
        self.work_root = work_root.expanduser().resolve()
        self.minimum_free_bytes = minimum_free_bytes
        self.maximum_result_bytes = maximum_result_bytes
        self.maximum_job_memory_bytes = maximum_job_memory_bytes
        self.minimum_memory_headroom_bytes = minimum_memory_headroom_bytes
        self.free_space_reader = free_space_reader
        self.available_memory_reader = available_memory_reader
        self.group_resource_reader = group_resource_reader or process_group_resources
        physical = physical_memory_reader()
        self.maximum_worker_memory_bytes = physical - minimum_memory_headroom_bytes
        if self.maximum_worker_memory_bytes <= 0:
            raise WorkerError(
                "configured memory headroom is not smaller than physical memory"
            )
        if maximum_job_memory_bytes > self.maximum_worker_memory_bytes:
            raise WorkerError(
                "one job's memory limit exceeds memory available after headroom"
            )
        self._condition = threading.Condition()
        self._reserved_disk_bytes = 0
        self._reserved_memory_bytes = 0
        self._active_reservations = 0
        self._process_groups: set[int] = set()

    @property
    def reserved_disk_bytes(self) -> int:
        with self._condition:
            return self._reserved_disk_bytes

    @property
    def reserved_memory_bytes(self) -> int:
        with self._condition:
            return self._reserved_memory_bytes

    def acquire(
        self,
        manifest: Mapping[str, object],
        stop_event: threading.Event | None,
        drain_event: threading.Event | None = None,
    ) -> ResourceReservation:
        disk_bytes = job_disk_reservation_bytes(
            manifest, self.maximum_result_bytes
        )
        memory_bytes = self.maximum_job_memory_bytes
        with self._condition:
            while True:
                if stop_event is not None and stop_event.is_set():
                    raise WorkerShutdown(
                        "worker is shutting down while waiting for resource headroom"
                    )
                if drain_event is not None and drain_event.is_set():
                    raise WorkerShutdown(
                        "worker is draining while job waits for resource headroom"
                    )
                try:
                    free_bytes = self.free_space_reader(self.work_root)
                except OSError as exc:
                    raise WorkerError(
                        f"cannot inspect worker filesystem free space: {exc}"
                    ) from None
                disk_available = (
                    free_bytes
                    - self._reserved_disk_bytes
                    - disk_bytes
                    >= self.minimum_free_bytes
                )
                current_worker_rss = sum(
                    self.group_resource_reader(group_id)[0]
                    for group_id in self._process_groups
                )
                remaining_reserved_memory = max(
                    0, self._reserved_memory_bytes - current_worker_rss
                )
                available_memory = self.available_memory_reader()
                memory_available = (
                    available_memory
                    - remaining_reserved_memory
                    - memory_bytes
                    >= self.minimum_memory_headroom_bytes
                )
                if disk_available and memory_available:
                    self._reserved_disk_bytes += disk_bytes
                    self._reserved_memory_bytes += memory_bytes
                    self._active_reservations += 1
                    return ResourceReservation(self, disk_bytes, memory_bytes)
                if self._active_reservations == 0 and not disk_available:
                    raise WorkerError(
                        "insufficient free space for job preparation and packaging "
                        f"while preserving the {self.minimum_free_bytes}-byte reserve"
                    )
                self._condition.wait(0.5)

    def release(self, reservation: ResourceReservation) -> None:
        with self._condition:
            self._reserved_disk_bytes -= reservation.disk_bytes
            self._reserved_memory_bytes -= reservation.memory_bytes
            self._active_reservations -= 1
            if (
                self._reserved_disk_bytes < 0
                or self._reserved_memory_bytes < 0
                or self._active_reservations < 0
            ):
                raise WorkerError("scheduler resource reservation accounting underflow")
            self._condition.notify_all()

    def require_free_reserve(self, path: Path, phase: str) -> None:
        try:
            free_bytes = self.free_space_reader(path)
        except OSError as exc:
            raise WorkerError(f"free-space check failed during {phase}: {exc}") from None
        if free_bytes < self.minimum_free_bytes:
            raise WorkerError(
                f"filesystem free space fell below reserve during {phase}"
            )

    def register_process_group(self, group_id: int) -> None:
        with self._condition:
            self._process_groups.add(group_id)

    def unregister_process_group(self, group_id: int) -> None:
        with self._condition:
            self._process_groups.discard(group_id)

    def global_memory_violation(self, group_id: int) -> str | None:
        with self._condition:
            groups = tuple(self._process_groups)
            reserved_memory = self._reserved_memory_bytes
        readings = {
            candidate: self.group_resource_reader(candidate)[0]
            for candidate in groups
        }
        total = sum(readings.values())
        remaining_reserved_memory = max(0, reserved_memory - total)
        available_memory = self.available_memory_reader()
        if (
            available_memory - remaining_reserved_memory
            >= self.minimum_memory_headroom_bytes
            or not readings
        ):
            return None
        # Stop one largest group per sample rather than killing every active job.
        largest = max(readings, key=lambda candidate: (readings[candidate], candidate))
        if largest != group_id:
            return None
        return (
            "projected available system memory fell below scheduler headroom of "
            f"{self.minimum_memory_headroom_bytes} bytes"
        )


def terminate_remaining_process_group(group_id: int, grace_seconds: float = 2.0) -> None:
    """Stop background descendants left behind after the job leader exits.

    Children that deliberately create a new session can evade process-group
    cleanup. They still inherit the mandatory macOS sandbox, but this worker
    does not claim to be a container or a dedicated OS account.
    """
    if not process_group_exists(group_id):
        return
    try:
        os.killpg(group_id, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        if not process_group_exists(group_id):
            return
        time.sleep(0.05)
    try:
        os.killpg(group_id, signal.SIGKILL)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        if not process_group_exists(group_id):
            return
        time.sleep(0.05)
    raise WorkerError("analysis process group survived SIGKILL")


def wait_for_analysis_process(
    process: subprocess.Popen[bytes],
    *,
    timeout: int,
    stop_event: threading.Event | None,
    maximum_memory: int,
    maximum_processes: int,
    resource_reader: Callable[[int], tuple[int, int]] | None = None,
    work_path: Path | None = None,
    minimum_free_bytes: int = DEFAULT_MIN_FREE_BYTES,
    free_space_reader: Callable[[Path], int] = filesystem_free_bytes,
    global_memory_guard: Callable[[int], str | None] | None = None,
) -> AnalysisOutcome:
    """Wait for one process without mixing rusage between concurrent jobs."""
    resource_reader = resource_reader or process_group_resources
    deadline = time.monotonic() + timeout
    next_resource_poll = 0.0
    timed_out = False
    interrupted = False
    resource_limit: str | None = None
    resource_monitor_error: str | None = None
    peak_group_rss = 0
    peak_processes = 0
    lowest_free_bytes: int | None = None
    while True:
        completed = _wait4_nohang(process)
        if completed is not None:
            terminate_remaining_process_group(process.pid)
            return AnalysisOutcome(
                completed[0],
                completed[1],
                timed_out,
                interrupted,
                resource_limit,
                resource_monitor_error,
                peak_group_rss,
                peak_processes,
                lowest_free_bytes,
            )
        if stop_event is not None and stop_event.is_set():
            interrupted = True
            break
        now = time.monotonic()
        if now >= deadline:
            timed_out = True
            break
        if now >= next_resource_poll:
            try:
                group_rss, process_count = resource_reader(process.pid)
            except (OSError, subprocess.SubprocessError, WorkerError) as exc:
                resource_monitor_error = str(exc)
                break
            peak_group_rss = max(peak_group_rss, group_rss)
            peak_processes = max(peak_processes, process_count)
            if group_rss > maximum_memory:
                resource_limit = (
                    f"process-group RSS exceeded {maximum_memory} bytes"
                )
                break
            if global_memory_guard is not None:
                try:
                    resource_limit = global_memory_guard(process.pid)
                except (OSError, subprocess.SubprocessError, WorkerError) as exc:
                    resource_monitor_error = str(exc)
                    break
                if resource_limit is not None:
                    break
            if process_count > maximum_processes:
                resource_limit = (
                    f"process count exceeded {maximum_processes}"
                )
                break
            if work_path is not None:
                try:
                    free_bytes = free_space_reader(work_path)
                except OSError as exc:
                    resource_monitor_error = (
                        f"free-space watchdog failed: {exc}"
                    )
                    break
                lowest_free_bytes = (
                    free_bytes
                    if lowest_free_bytes is None
                    else min(lowest_free_bytes, free_bytes)
                )
                if free_bytes < minimum_free_bytes:
                    resource_limit = (
                        "filesystem free space fell below "
                        f"{minimum_free_bytes} bytes"
                    )
                    break
            next_resource_poll = now + RESOURCE_POLL_INTERVAL
        time.sleep(0.1)

    if resource_limit or resource_monitor_error:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        waited_pid, status, usage = os.wait4(process.pid, 0)
        if waited_pid != process.pid:
            raise WorkerError("lost track of resource-limited analysis process")
        process.returncode = os.waitstatus_to_exitcode(status)
        terminate_remaining_process_group(process.pid)
        return AnalysisOutcome(
            137 if resource_limit else 125,
            usage,
            timed_out,
            interrupted,
            resource_limit,
            resource_monitor_error,
            peak_group_rss,
            peak_processes,
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
            terminate_remaining_process_group(process.pid)
            forced_exit = (
                137
                if resource_limit
                else 125
                if resource_monitor_error
                else 124
                if timed_out
                else 143
            )
            return AnalysisOutcome(
                forced_exit,
                completed[1],
                timed_out,
                interrupted,
                resource_limit,
                resource_monitor_error,
                peak_group_rss,
                peak_processes,
                lowest_free_bytes,
            )
        time.sleep(0.1)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    waited_pid, status, usage = os.wait4(process.pid, 0)
    if waited_pid != process.pid:
        raise WorkerError("lost track of analysis child process")
    process.returncode = os.waitstatus_to_exitcode(status)
    terminate_remaining_process_group(process.pid)
    forced_exit = (
        137
        if resource_limit
        else 125
        if resource_monitor_error
        else 124
        if timed_out
        else 143
    )
    return AnalysisOutcome(
        forced_exit,
        usage,
        timed_out,
        interrupted,
        resource_limit,
        resource_monitor_error,
        peak_group_rss,
        peak_processes,
        lowest_free_bytes,
    )


def limited_child_main(argv: Sequence[str]) -> int:
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


def sandbox_command(
    command: Sequence[str],
    *,
    profile: Path | None,
    job_root: Path,
    source_root: Path,
    result_root: Path,
    environment: Mapping[str, str],
    datasets: Mapping[str, Path],
) -> list[str]:
    if profile is None:
        return list(command)
    profile = profile.expanduser().resolve()
    if not profile.is_file():
        raise WorkerError(f"sandbox profile does not exist: {profile}")
    sandbox = Path("/usr/bin/sandbox-exec")
    if not sandbox.is_file():
        raise WorkerError("sandbox-exec was requested but is unavailable")
    parameters = {
        "WORKER_ROOT": Path(__file__).resolve().parents[3],
        "JOB_ROOT": job_root,
        "SOURCE_ROOT": source_root,
        "INPUT_ROOT": job_root / "inputs",
        "RESULT_ROOT": result_root,
        "HOME": environment["HOME"],
        "TMPDIR": environment["TMPDIR"],
    }
    dataset_paths = [path for _, path in sorted(datasets.items())]
    # The installed profile has a fixed number of parameter slots.  Unused
    # ones point at /dev/null; configured dataset roots receive read access but
    # never appear in the Pi-side job manifest.
    for index in range(16):
        parameters[f"DATASET_{index}"] = (
            dataset_paths[index] if index < len(dataset_paths) else Path("/dev/null")
        )
    wrapped = [str(sandbox), "-f", str(profile)]
    for name, path in parameters.items():
        wrapped.extend(("-D", f"{name}={path}"))
    wrapped.extend(command)
    return wrapped


def logical_command_record(
    task_name: str,
    embedded_execution: object | None,
    arguments: Sequence[str],
) -> dict[str, object]:
    """Describe execution without serializing private Mac paths."""
    if embedded_execution is None:
        return {
            "kind": "legacy-task",
            "task": task_name,
            "arguments": list(arguments),
        }
    specification = protocol.validate_execution(embedded_execution)
    return {
        "kind": "repo-task",
        "profile": specification["profile"],
        "family": specification["family"],
        "argv_template": list(specification["argv"]),
        "arguments": list(arguments),
        "datasets": list(specification["datasets"]),
    }


def scrub_private_paths(
    result_root: Path,
    *,
    datasets: Mapping[str, Path],
    job_root: Path,
) -> None:
    """Keep private Mac roots out of uploaded text results and diagnostics."""
    replacements: list[tuple[bytes, bytes]] = []
    for name, path in datasets.items():
        replacements.append(
            (str(path).encode(), f"{{dataset:{name}}}".encode())
        )
    replacements.extend(
        (
            (str(job_root).encode(), b"{job}"),
            (str(Path(__file__).resolve().parents[3]).encode(), b"{worker-root}"),
            (str(Path.home()).encode(), b"{mac-home}"),
        )
    )
    replacements.sort(key=lambda item: len(item[0]), reverse=True)
    for path in result_root.rglob("*"):
        if (
            path.is_symlink()
            or not path.is_file()
            or path.suffix.lower() not in TEXT_RESULT_SUFFIXES
        ):
            continue
        original = path.read_bytes()
        scrubbed = original
        for private, logical in replacements:
            scrubbed = scrubbed.replace(private, logical)
        if scrubbed != original:
            path.write_bytes(scrubbed)


def validate_raw_result_bytes(result_root: Path, maximum_result_bytes: int) -> None:
    """Bound aggregate child output before reading, scrubbing, or archiving it."""
    total = 0
    for path in result_root.rglob("*"):
        if path.is_symlink():
            raise WorkerError(f"result contains a symlink: {path}")
        if not path.is_file():
            continue
        total += path.stat().st_size
        if total > maximum_result_bytes:
            raise WorkerError("job results exceed the total result limit")


def execute_job(
    manifest: dict[str, object],
    *,
    source_root: Path,
    input_paths: Sequence[Path],
    input_values: Sequence[object | None],
    result_root: Path,
    python: str,
    timeout: int,
    nice: int,
    maximum_file_size: int,
    maximum_memory: int = DEFAULT_MAX_MEMORY_BYTES,
    maximum_processes: int = DEFAULT_MAX_PROCESSES,
    minimum_free_bytes: int = DEFAULT_MIN_FREE_BYTES,
    executables: Mapping[str, str] | None = None,
    datasets: Mapping[str, Path] | None = None,
    sandbox_profile: Path | None = None,
    stop_event: threading.Event | None = None,
    worker_id: str | None = None,
    allow_unsandboxed_dynamic: bool = False,
    resource_manager: SchedulerResourceManager | None = None,
) -> tuple[int, dict[str, object]]:
    task_name = str(manifest.get("task", ""))
    arguments = manifest.get("arguments", [])
    inputs = manifest.get("inputs", [])
    if not isinstance(arguments, list) or not all(isinstance(item, str) for item in arguments):
        raise WorkerError("job arguments are invalid")
    if not isinstance(inputs, list):
        raise WorkerError("job inputs are invalid")
    embedded_execution = manifest.get("execution")
    specification: dict[str, object] | None = None
    if embedded_execution is None:
        protocol.validate_inputs(task_name, inputs)
    else:
        specification = protocol.validate_execution(embedded_execution)
        if sandbox_profile is None and not allow_unsandboxed_dynamic:
            raise WorkerError(
                "repo-defined tasks require a validated sandbox profile; "
                "use the explicit unsandboxed escape hatch only after reviewing the risk"
            )
    configured_datasets = datasets or {}
    referenced_datasets = {
        match.group(1)
        for token in (() if specification is None else specification["argv"])
        if (match := re.fullmatch(r"\{dataset:([A-Za-z0-9_.-]+)\}", str(token)))
    }
    datasets = {
        name: configured_datasets[name]
        for name in referenced_datasets
        if name in configured_datasets
    }
    command = protocol.build_command(
        task_name,
        python=python,
        source_root=source_root,
        input_paths=input_paths,
        input_values=input_values,
        result_root=result_root,
        arguments=arguments,
        execution=embedded_execution,
        executables=executables,
        datasets=datasets,
    )
    job_root = source_root.parent.resolve()
    home_root = job_root / "home"
    temporary_root = job_root / "tmp"
    cache_root = job_root / "cache"
    for path in (result_root, home_root, temporary_root, cache_root):
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(path, 0o700)
    stdout_path = result_root / "stdout.txt"
    stderr_path = result_root / "stderr.txt"
    environment = {
        "HOME": str(home_root),
        "TMPDIR": str(temporary_root) + "/",
        "XDG_CACHE_HOME": str(cache_root),
        "XDG_CONFIG_HOME": str(home_root / ".config"),
        "LANG": "en_US.UTF-8",
        "LC_ALL": "en_US.UTF-8",
        "PATH": "/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin",
        "VAN_COMPUTE_CHILD_PYTHONPATH": str(source_root),
        "PYTHONNOUSERSITE": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "VAN_COMPUTE_JOB_ID": str(manifest.get("id", "")),
    }
    command = sandbox_command(
        command,
        profile=sandbox_profile,
        job_root=job_root,
        source_root=source_root,
        result_root=result_root,
        environment=environment,
        datasets=datasets,
    )
    child_command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "__exec__",
        str(nice),
        str(timeout),
        str(maximum_file_size),
        str(maximum_memory),
        "--",
        *command,
    ]
    started = utc_now()
    started_monotonic = time.monotonic()
    with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
        process = subprocess.Popen(
            child_command,
            cwd=source_root,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
        if resource_manager is not None:
            resource_manager.register_process_group(process.pid)
        try:
            outcome = wait_for_analysis_process(
                process,
                timeout=timeout,
                stop_event=stop_event,
                maximum_memory=maximum_memory,
                maximum_processes=maximum_processes,
                work_path=job_root,
                minimum_free_bytes=minimum_free_bytes,
                global_memory_guard=(
                    resource_manager.global_memory_violation
                    if resource_manager is not None
                    else None
                ),
            )
        finally:
            if resource_manager is not None:
                resource_manager.unregister_process_group(process.pid)
    duration_seconds = time.monotonic() - started_monotonic
    resource_usage = child_resource_usage(outcome.usage, duration_seconds)
    resource_usage.update(
        {
            "peak_process_group_rss_bytes": outcome.peak_process_group_rss_bytes,
            "peak_process_count": outcome.peak_process_count,
            "process_group_watchdog_interval_seconds": RESOURCE_POLL_INTERVAL,
            "minimum_filesystem_free_bytes": (
                outcome.minimum_filesystem_free_bytes
            ),
        }
    )
    execution = {
        "schema_version": protocol.SCHEMA_VERSION,
        "job_id": manifest.get("id"),
        "task": task_name,
        "worker": worker_id or socket.gethostname(),
        "placement": "remote",
        "started_at": started,
        "finished_at": utc_now(),
        "duration_seconds": round(duration_seconds, 6),
        "duration_scope": "analysis child execution plus descendant process-group cleanup",
        "exit_code": outcome.exit_code,
        "timed_out": outcome.timed_out,
        "interrupted": outcome.interrupted,
        "resource_limit": outcome.resource_limit,
        "resource_monitor_error": outcome.resource_monitor_error,
        "command": logical_command_record(task_name, embedded_execution, arguments),
        "resource_usage": resource_usage,
        "input_bytes": sum(
            int(item.get("size", 0)) for item in inputs if isinstance(item, dict)
        ),
        "source_bytes": sum(
            int(item.get("size", 0))
            for item in manifest.get("sources", [])
            if isinstance(item, dict)
        ),
    }
    (result_root / "execution.json").write_text(
        json.dumps(execution, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    validate_raw_result_bytes(result_root, maximum_file_size)
    scrub_private_paths(result_root, datasets=datasets, job_root=job_root)
    return outcome.exit_code, execution


def real_declared_output(
    result_root: Path,
    relative: Path,
) -> Path | None:
    """Return a declared file/directory only when every component is real."""
    try:
        root_info = result_root.lstat()
    except OSError as exc:
        raise WorkerError(f"cannot inspect result root: {exc}") from None
    if stat.S_ISLNK(root_info.st_mode) or not stat.S_ISDIR(root_info.st_mode):
        raise WorkerError("result root is not a real directory")

    current = result_root
    for index, part in enumerate(relative.parts):
        current = current / part
        try:
            info = current.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise WorkerError(
                f"cannot inspect declared output component {current}: {exc}"
            ) from None
        if stat.S_ISLNK(info.st_mode):
            raise WorkerError(
                f"declared output has a symlinked path component: {relative}"
            )
        if index < len(relative.parts) - 1 and not stat.S_ISDIR(info.st_mode):
            return None
        if index == len(relative.parts) - 1:
            if stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode):
                return current
            raise WorkerError(f"declared output is a special file: {relative}")
    return None


def real_declared_output_directory(
    result_root: Path,
    relative: Path,
) -> Path | None:
    output = real_declared_output(result_root, relative)
    return output if output is not None and output.is_dir() else None


def package_declared_output_directories(
    manifest: dict[str, object],
    result_root: Path,
    maximum_result_bytes: int,
) -> dict[str, str]:
    """Turn declared directory outputs (notably JADX trees) into one artifact."""
    embedded = manifest.get("execution")
    if embedded is None:
        return {}
    specification = protocol.validate_execution(embedded)
    declared = [Path(str(item)) for item in specification["outputs"]]
    for index, first in enumerate(declared):
        for second in declared[index + 1:]:
            if first == second or first in second.parents or second in first.parents:
                raise WorkerError(
                    f"declared result paths overlap: {first} and {second}"
                )
    artifacts: dict[str, str] = {}
    for output_text in specification["outputs"]:
        relative = Path(str(output_text))
        output = real_declared_output_directory(result_root, relative)
        if output is None:
            continue
        estimated_bytes = 4096
        entry_count = 0
        for entry in output.rglob("*"):
            entry_count += 1
            estimated_bytes += 4096
            if entry_count > 100_000:
                raise WorkerError(f"declared output has too many entries: {relative}")
            if entry.is_symlink():
                raise WorkerError(f"declared output contains a symlink: {relative}")
            if entry.is_file():
                estimated_bytes += entry.stat().st_size
            elif not entry.is_dir():
                raise WorkerError(f"declared output contains a special file: {relative}")
            if estimated_bytes > maximum_result_bytes:
                raise WorkerError(f"declared output exceeds the result limit: {relative}")
        archive_relative = relative.with_name(relative.name + ".tar.gz")
        archive = result_root / archive_relative
        if archive_relative in declared or archive.exists() or archive.is_symlink():
            raise WorkerError(
                f"output archive path collides with another result: {archive_relative}"
            )
        archive.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = archive.with_name(f".{archive.name}.partial")

        def portable_metadata(info: tarfile.TarInfo) -> tarfile.TarInfo:
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            info.mtime = 0
            return info

        try:
            with temporary.open("xb") as raw_archive:
                with tarfile.open(
                    fileobj=raw_archive, mode="w:gz", dereference=False
                ) as bundle:
                    bundle.add(
                        output,
                        arcname=relative.as_posix(),
                        recursive=True,
                        filter=portable_metadata,
                    )
            if temporary.stat().st_size > maximum_result_bytes:
                raise WorkerError(f"output archive exceeds the result limit: {relative}")
            os.replace(temporary, archive)
        finally:
            temporary.unlink(missing_ok=True)
        shutil.rmtree(output)
        artifacts[relative.as_posix()] = archive_relative.as_posix()
    return artifacts


def validate_declared_outputs(
    manifest: dict[str, object], result_root: Path, *, require_all: bool
) -> None:
    embedded = manifest.get("execution")
    if embedded is None:
        return
    specification = protocol.validate_execution(embedded)
    declared = [Path(str(item)) for item in specification["outputs"]]
    for index, first in enumerate(declared):
        for second in declared[index + 1:]:
            if first == second or first in second.parents or second in first.parents:
                raise WorkerError(
                    f"declared result paths overlap: {first} and {second}"
                )
    missing = [
        path.as_posix()
        for path in declared
        if real_declared_output(result_root, path) is None
    ]
    if require_all and missing:
        raise WorkerError(
            "successful job omitted declared result(s): " + ", ".join(missing)
        )


def expected_result_paths(
    manifest: dict[str, object], artifacts: Mapping[str, str]
) -> set[str] | None:
    embedded = manifest.get("execution")
    if embedded is None:
        return None
    specification = protocol.validate_execution(embedded)
    expected = {"stdout.txt", "stderr.txt", "execution.json"}
    for output in specification["outputs"]:
        output_text = str(output)
        expected.add(artifacts.get(output_text, output_text))
    return expected


def result_files(
    result_root: Path,
    maximum_file_size: int,
    *,
    expected: set[str] | None = None,
) -> list[tuple[str, Path]]:
    files: list[tuple[str, Path]] = []
    total_bytes = 0
    for path in sorted(result_root.rglob("*")):
        if path.is_symlink():
            raise WorkerError(f"result contains a symlink: {path}")
        if not path.is_file():
            continue
        relative = path.relative_to(result_root).as_posix()
        if expected is not None and relative not in expected:
            raise WorkerError(f"job produced undeclared result file: {relative}")
        if path.stat().st_size > maximum_file_size:
            raise WorkerError(f"result exceeds the per-file limit: {relative}")
        total_bytes += path.stat().st_size
        if total_bytes > maximum_file_size:
            raise WorkerError("job results exceed the total result limit")
        files.append((relative, path))
    if len(files) > protocol.MAX_OUTPUTS + 3:
        raise WorkerError(
            f"job produced more than {protocol.MAX_OUTPUTS + 3} result files"
        )
    return files


def record_worker_failure(
    result_root: Path,
    manifest: dict[str, object],
    exc: Exception,
    *,
    worker_id: str | None = None,
) -> int:
    # The path is always the result child of our per-job TemporaryDirectory.
    # Drop untrusted or over-limit task output before publishing the failure.
    if result_root.exists():
        shutil.rmtree(result_root)
    result_root.mkdir(parents=True, exist_ok=True)
    message = f"van-compute worker: {type(exc).__name__}: {exc}\n"
    (result_root / "stderr.txt").write_text(message, encoding="utf-8")
    (result_root / "stdout.txt").touch()
    execution = {
        "schema_version": protocol.SCHEMA_VERSION,
        "job_id": manifest.get("id"),
        "task": manifest.get("task"),
        "worker": worker_id or socket.gethostname(),
        "placement": "remote",
        "finished_at": utc_now(),
        "exit_code": 70,
        "worker_error": str(exc),
    }
    (result_root / "execution.json").write_text(
        json.dumps(execution, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 70


def make_remote(
    args: argparse.Namespace,
    worker_id: str,
    multiplexer: SSHMultiplexer | None = None,
) -> RemoteQueue:
    return RemoteQueue(
        args.host,
        args.remote_cli,
        worker_id,
        remote_root=args.remote_root,
        connect_timeout=args.connect_timeout,
        multiplexer=multiplexer,
    )


def resource_manager_for_args(
    args: argparse.Namespace, work_root: Path
) -> SchedulerResourceManager:
    configured = getattr(args, "resource_manager", None)
    if configured is not None:
        return configured
    return SchedulerResourceManager(
        work_root,
        minimum_free_bytes=getattr(
            args, "min_free_bytes", DEFAULT_MIN_FREE_BYTES
        ),
        maximum_result_bytes=getattr(
            args, "max_result_bytes", DEFAULT_MAX_RESULT_BYTES
        ),
        maximum_job_memory_bytes=getattr(
            args, "max_memory_bytes", DEFAULT_MAX_MEMORY_BYTES
        ),
        minimum_memory_headroom_bytes=getattr(
            args,
            "min_memory_headroom_bytes",
            DEFAULT_MIN_MEMORY_HEADROOM_BYTES,
        ),
    )


def run_claimed_job(
    args: argparse.Namespace,
    remote: RemoteQueue,
    manifest: dict[str, object],
    stop_event: threading.Event | None = None,
) -> dict[str, object]:
    work_root = args.work_root.expanduser().resolve()
    work_root.mkdir(parents=True, exist_ok=True)
    os.chmod(work_root, 0o700)
    job_id = str(manifest.get("id", ""))
    if not JOB_ID_RE.fullmatch(job_id):
        raise WorkerError("job manifest has an invalid id")
    lease_token = manifest_lease_token(manifest)
    resources = resource_manager_for_args(args, work_root)
    attempt_started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix=f"{job_id}-", dir=work_root) as temporary:
        job_root = Path(temporary)
        os.chmod(job_root, 0o700)
        result_root = job_root / "result"
        artifacts: dict[str, str] = {}
        timing = {
            "resource_admission_seconds": 0.0,
            "source_input_preparation_seconds": 0.0,
            "analysis_seconds": 0.0,
            "packaging_seconds": 0.0,
            "result_upload_seconds_excluding_execution_json": 0.0,
        }
        phase = "preparation"
        phase_started = time.monotonic()
        reservation: ResourceReservation | None = None
        try:
            if stop_event is not None and stop_event.is_set():
                raise WorkerShutdown("worker is shutting down before job start")
            admission_started = time.monotonic()
            admission_stop_event = getattr(
                args, "resource_admission_stop_event", None
            )
            reservation = resources.acquire(
                manifest, stop_event, admission_stop_event
            )
            if (
                admission_stop_event is not None
                and admission_stop_event.is_set()
            ):
                raise WorkerShutdown(
                    "worker began draining before admitted job preparation"
                )
            timing["resource_admission_seconds"] = bounded_seconds(
                admission_started
            )
            source_root, input_paths, values = prepare_job(remote, manifest, job_root)
            resources.require_free_reserve(job_root, "job preparation")
            timing["source_input_preparation_seconds"] = bounded_seconds(phase_started)
            phase = "analysis"
            phase_started = time.monotonic()
            exit_code, execution = execute_job(
                manifest,
                source_root=source_root,
                input_paths=input_paths,
                input_values=values,
                result_root=result_root,
                python=args.python,
                timeout=args.timeout,
                nice=args.nice,
                maximum_file_size=args.max_result_bytes,
                maximum_memory=args.max_memory_bytes,
                maximum_processes=getattr(
                    args, "max_processes", DEFAULT_MAX_PROCESSES
                ),
                minimum_free_bytes=getattr(
                    args, "min_free_bytes", DEFAULT_MIN_FREE_BYTES
                ),
                executables=args.executables,
                datasets=args.datasets,
                sandbox_profile=args.sandbox_profile,
                stop_event=stop_event,
                worker_id=remote.worker,
                allow_unsandboxed_dynamic=args.allow_unsandboxed_dynamic,
                resource_manager=resources,
            )
            if execution.get("interrupted"):
                raise WorkerShutdown(
                    "worker shutdown interrupted analysis; leaving lease resumable"
                )
            timing["analysis_seconds"] = float(
                execution.get("duration_seconds", bounded_seconds(phase_started))
            )
            phase = "packaging"
            phase_started = time.monotonic()
            resources.require_free_reserve(job_root, "result packaging")
            validate_declared_outputs(
                manifest, result_root, require_all=exit_code == 0
            )
            artifacts = package_declared_output_directories(
                manifest, result_root, args.max_result_bytes
            )
            resources.require_free_reserve(job_root, "result packaging")
            if artifacts:
                execution["output_artifacts"] = artifacts
                (result_root / "execution.json").write_text(
                    json.dumps(execution, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
            scrub_private_paths(
                result_root, datasets=args.datasets, job_root=job_root
            )
            files = result_files(
                result_root,
                args.max_result_bytes,
                expected=expected_result_paths(manifest, artifacts),
            )
            timing["packaging_seconds"] = bounded_seconds(phase_started)
        except WorkerShutdown:
            raise
        except Exception as exc:
            timing[f"{phase}_seconds" if phase != "preparation" else "source_input_preparation_seconds"] = bounded_seconds(phase_started)
            print(
                f"van-compute-worker[{remote.worker}] local job error: "
                f"{type(exc).__name__}: {exc}",
                file=sys.stderr,
                flush=True,
            )
            exit_code = record_worker_failure(
                result_root, manifest, exc, worker_id=remote.worker
            )
            artifacts = {}
            failure_packaging_started = time.monotonic()
            scrub_private_paths(
                result_root, datasets=args.datasets, job_root=job_root
            )
            files = result_files(
                result_root,
                args.max_result_bytes,
                expected=expected_result_paths(manifest, artifacts),
            )
            timing["packaging_seconds"] = round(
                timing["packaging_seconds"]
                + bounded_seconds(failure_packaging_started),
                6,
            )
            execution = json.loads(
                (result_root / "execution.json").read_text(encoding="utf-8")
            )
        finally:
            if reservation is not None:
                reservation.release()

        execution["timing"] = {
            **timing,
            "worker_attempt_active_seconds": None,
            "worker_attempt_active_note": (
                "from post-claim staging through non-telemetry result uploads; "
                "excludes final execution.json upload and queue finish"
            ),
            "packaging_note": (
                "validation, redaction, result enumeration, and directory archiving; "
                "excludes final telemetry serialization"
            ),
        }
        execution_path = result_root / "execution.json"
        execution_path.write_text(
            json.dumps(execution, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        scrub_private_paths(result_root, datasets=args.datasets, job_root=job_root)
        files = result_files(
            result_root,
            args.max_result_bytes,
            expected=expected_result_paths(manifest, artifacts),
        )
        uploaded: list[str] = []
        upload_started = time.monotonic()
        for relative, path in files:
            if relative == "execution.json":
                continue
            remote.put_result(
                job_id, relative, path, lease_token=lease_token
            )
            uploaded.append(relative)
        timing["result_upload_seconds_excluding_execution_json"] = bounded_seconds(
            upload_started
        )
        execution["timing"].update(timing)
        execution["timing"]["worker_attempt_active_seconds"] = bounded_seconds(
            attempt_started
        )
        execution_path.write_text(
            json.dumps(execution, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        scrub_private_paths(result_root, datasets=args.datasets, job_root=job_root)
        files = result_files(
            result_root,
            args.max_result_bytes,
            expected=expected_result_paths(manifest, artifacts),
        )
        remote.put_result(
            job_id, "execution.json", execution_path, lease_token=lease_token
        )
        uploaded.append("execution.json")
        completed = remote.finish(
            job_id, exit_code, uploaded, lease_token=lease_token
        )
        return {
            "ok": exit_code == 0,
            "worker": remote.worker,
            "job": job_id,
            "execution": execution,
            "state": completed.get("state"),
            "results": uploaded,
        }


def run_once(args: argparse.Namespace) -> dict[str, object]:
    remote = make_remote(args, args.worker)
    remote.heartbeat()
    manifest = remote.claim()
    if manifest is None:
        return {"ok": True, "worker": args.worker, "job": None}
    return run_claimed_job(args, remote, manifest)


class PersistentScheduler:
    """Run exactly ten equal worker identities until asked to stop."""

    def __init__(
        self,
        args: argparse.Namespace,
        *,
        stop_event: threading.Event,
        drain_event: threading.Event | None = None,
        multiplexer: SSHMultiplexer | None = None,
        multiplexers: Sequence[SSHMultiplexer] | None = None,
        remote_factory: Callable[[str], RemoteQueue] | None = None,
        job_runner: Callable[
            [argparse.Namespace, RemoteQueue, dict[str, object], threading.Event],
            dict[str, object],
        ] = run_claimed_job,
    ) -> None:
        self.args = args
        self.stop_event = stop_event
        self.drain_event = drain_event or threading.Event()
        self.args.resource_admission_stop_event = self.drain_event
        self.multiplexers = list(
            multiplexers or ([] if multiplexer is None else [multiplexer])
        )
        self.job_runner = job_runner
        if remote_factory is not None:
            self.remotes = [
                remote_factory(f"{args.worker}.{index:02d}")
                for index in range(SCHEDULER_SLOTS)
            ]
            self.coordinator = remote_factory(args.worker)
        else:
            self.remotes = [
                make_remote(
                    args,
                    f"{args.worker}.{index:02d}",
                    (
                        self.multiplexers[index % len(self.multiplexers)]
                        if self.multiplexers
                        else None
                    ),
                )
                for index in range(SCHEDULER_SLOTS)
            ]
            self.coordinator = make_remote(
                args,
                args.worker,
                self.multiplexers[-1] if self.multiplexers else None,
            )
        self._futures: dict[str, Future[dict[str, object]] | None] = {
            remote.worker: None for remote in self.remotes
        }
        self._needs_probe: set[str] = {remote.worker for remote in self.remotes}
        self._probe_retry_at: dict[str, float] = {}
        self._dispatch_index = 0
        self._slot_changed = threading.Event()
        self._busy: set[str] = set()
        self._busy_lock = threading.Lock()

    def busy_count(self) -> int:
        with self._busy_lock:
            return len(self._busy)

    def _set_busy(self, worker_id: str, busy: bool) -> None:
        with self._busy_lock:
            if busy:
                self._busy.add(worker_id)
            else:
                self._busy.discard(worker_id)

    def _run_slot_job(
        self, remote: RemoteQueue, manifest: dict[str, object]
    ) -> dict[str, object]:
        self._set_busy(remote.worker, True)
        try:
            return self.job_runner(self.args, remote, manifest, self.stop_event)
        finally:
            self._set_busy(remote.worker, False)
            self._slot_changed.set()

    def _reap_jobs(self) -> None:
        for remote in self.remotes:
            future = self._futures[remote.worker]
            if future is None or not future.done():
                continue
            try:
                future.result()
            except Exception as exc:
                # An interrupted upload/finish may have left this exact slot's
                # lease running. Probe it first so only that identity resumes.
                self._needs_probe.add(remote.worker)
                self._probe_retry_at.pop(remote.worker, None)
                print(
                    f"van-compute-worker[{remote.worker}]: {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
            self._futures[remote.worker] = None

    def _next_free_remote(self) -> RemoteQueue | None:
        # Rotate recovery probes too: one corrupt/stuck exact-slot lease must
        # not starve the other nine identities indefinitely.
        now = time.monotonic()
        for offset in range(SCHEDULER_SLOTS):
            index = (self._dispatch_index + offset) % SCHEDULER_SLOTS
            remote = self.remotes[index]
            if (
                remote.worker in self._needs_probe
                and self._probe_retry_at.get(remote.worker, 0.0) <= now
                and self._futures[remote.worker] is None
            ):
                self._dispatch_index = (index + 1) % SCHEDULER_SLOTS
                return remote
        for offset in range(SCHEDULER_SLOTS):
            index = (self._dispatch_index + offset) % SCHEDULER_SLOTS
            remote = self.remotes[index]
            if (
                remote.worker not in self._needs_probe
                and self._futures[remote.worker] is None
            ):
                self._dispatch_index = (index + 1) % SCHEDULER_SLOTS
                return remote
        return None

    def _dispatch_loop(self, executor: ThreadPoolExecutor) -> None:
        while not self.stop_event.is_set():
            self._reap_jobs()
            if self.drain_event.is_set():
                if all(future is None for future in self._futures.values()):
                    self.stop_event.set()
                    return
                self._slot_changed.clear()
                self._slot_changed.wait(min(1.0, self.args.poll_interval))
                continue
            remote = self._next_free_remote()
            if remote is None:
                self._slot_changed.clear()
                self._slot_changed.wait(min(1.0, self.args.poll_interval))
                continue
            try:
                manifest = remote.claim()
            except Exception as exc:
                self._needs_probe.add(remote.worker)
                self._probe_retry_at[remote.worker] = (
                    time.monotonic() + max(1.0, self.args.poll_interval)
                )
                print(
                    f"van-compute-worker[{remote.worker}] claim: "
                    f"{type(exc).__name__}: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
                continue
            self._needs_probe.discard(remote.worker)
            self._probe_retry_at.pop(remote.worker, None)
            if self.drain_event.is_set():
                # A claim RPC already in flight may complete after drain was
                # requested. Leave its exact-slot lease untouched; the
                # installer will detect it and restart this release to resume.
                if manifest is not None:
                    self._needs_probe.add(remote.worker)
                    self._probe_retry_at.pop(remote.worker, None)
                self.stop_event.set()
                return
            if manifest is None:
                # Complete every startup/recovery probe once so an older job
                # owned by a later slot cannot be stranded. Otherwise one
                # empty claim proves the global queued directory is empty.
                if self._needs_probe:
                    retry_at = min(
                        self._probe_retry_at.get(worker_id, 0.0)
                        for worker_id in self._needs_probe
                    )
                    retry_delay = max(0.0, retry_at - time.monotonic())
                    if retry_delay > 0:
                        self._slot_changed.clear()
                        self._slot_changed.wait(min(1.0, retry_delay))
                    continue
                self._slot_changed.clear()
                self._slot_changed.wait(self.args.poll_interval)
                continue
            future = executor.submit(
                self._run_slot_job, remote, manifest
            )
            future.add_done_callback(lambda _future: self._slot_changed.set())
            self._futures[remote.worker] = future
            # A claimed job means more may be queued. Immediately fill another
            # free slot instead of waiting for the poll interval.

    def _busy_heartbeat_loop(self, remote: RemoteQueue) -> None:
        while not self.stop_event.wait(self.args.heartbeat_interval):
            with self._busy_lock:
                busy = remote.worker in self._busy
            if not busy:
                continue
            try:
                remote.heartbeat()
            except Exception as exc:
                print(
                    f"van-compute-worker[{remote.worker}] heartbeat: {exc}",
                    file=sys.stderr,
                    flush=True,
                )

    def _coordinator_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.coordinator.heartbeat(
                    slots_total=SCHEDULER_SLOTS,
                    slots_busy=self.busy_count(),
                )
            except Exception as exc:
                print(
                    f"van-compute-worker[{self.args.worker}] capacity heartbeat: {exc}",
                    file=sys.stderr,
                    flush=True,
                )
            self.stop_event.wait(self.args.heartbeat_interval)

    def serve(self) -> None:
        threads: list[threading.Thread] = []
        for remote in self.remotes:
            threads.append(
                threading.Thread(
                    target=self._busy_heartbeat_loop,
                    args=(remote,),
                    name=f"van-compute-heartbeat-{remote.worker}",
                )
            )
        threads.append(
            threading.Thread(
                target=self._coordinator_loop,
                name="van-compute-capacity-heartbeat",
            )
        )
        for thread in threads:
            thread.start()
        try:
            with ThreadPoolExecutor(
                max_workers=SCHEDULER_SLOTS,
                thread_name_prefix="van-compute-job",
            ) as executor:
                self._dispatch_loop(executor)
        finally:
            self.stop_event.set()
            self._slot_changed.set()
            for thread in threads:
                thread.join()


def run_scheduler(
    args: argparse.Namespace,
    *,
    stop_event: threading.Event | None = None,
    drain_event: threading.Event | None = None,
    multiplexer: SSHMultiplexer | None = None,
    multiplexers: Sequence[SSHMultiplexer] | None = None,
    remote_factory: Callable[[str], RemoteQueue] | None = None,
    job_runner: Callable[
        [argparse.Namespace, RemoteQueue, dict[str, object], threading.Event],
        dict[str, object],
    ] = run_claimed_job,
) -> None:
    stop_event = stop_event or threading.Event()
    scheduler = PersistentScheduler(
        args,
        stop_event=stop_event,
        drain_event=drain_event,
        multiplexer=multiplexer,
        multiplexers=multiplexers,
        remote_factory=remote_factory,
        job_runner=job_runner,
    )
    scheduler.serve()


def parse_dataset_entry(entry: str) -> tuple[str, Path]:
    name, separator, raw_path = entry.partition("=")
    if not separator or not DATASET_NAME_RE.fullmatch(name) or not raw_path:
        raise WorkerError("--dataset must be NAME=/absolute/read-only/path")
    unresolved = Path(raw_path).expanduser()
    if not unresolved.is_absolute():
        raise WorkerError("--dataset must be NAME=/absolute/read-only/path")
    path = unresolved.resolve()
    if not path.exists():
        raise WorkerError(f"dataset {name!r} does not exist: {path}")
    return name, path


def load_datasets(config: Path | None, entries: Sequence[str]) -> dict[str, Path]:
    datasets: dict[str, Path] = {}
    if config is not None:
        config = config.expanduser().resolve()
        try:
            payload = json.loads(config.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise WorkerError(f"cannot read dataset config {config}: {exc}") from None
        if not isinstance(payload, dict) or set(payload) != {"datasets"}:
            raise WorkerError("dataset config must contain only a 'datasets' object")
        records = payload["datasets"]
        if not isinstance(records, dict):
            raise WorkerError("dataset config 'datasets' must be an object")
        for name, raw_path in records.items():
            if not isinstance(name, str) or not isinstance(raw_path, str):
                raise WorkerError("dataset config names and paths must be strings")
            parsed_name, path = parse_dataset_entry(f"{name}={raw_path}")
            datasets[parsed_name] = path
    for entry in entries:
        name, path = parse_dataset_entry(entry)
        datasets[name] = path
    if len(datasets) > 16:
        raise WorkerError("at most 16 datasets may be configured")
    return datasets


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--serve",
        action="store_true",
        help=f"run the persistent {SCHEDULER_SLOTS}-slot scheduler",
    )
    mode.add_argument(
        "--run-once",
        action="store_true",
        help="claim at most one job and exit (the backward-compatible default)",
    )
    parser.add_argument("--host", default=os.environ.get("VAN_COMPUTE_HOST", DEFAULT_HOST))
    parser.add_argument("--remote-cli", default=os.environ.get("VAN_COMPUTE_REMOTE_CLI", DEFAULT_REMOTE_CLI))
    parser.add_argument("--remote-root", default=os.environ.get("VAN_COMPUTE_ROOT"))
    parser.add_argument("--worker", default=os.environ.get("VAN_COMPUTE_WORKER", socket.gethostname().split(".")[0]))
    parser.add_argument("--work-root", type=Path, default=DEFAULT_WORK_ROOT)
    parser.add_argument("--control-path", type=Path, default=DEFAULT_CONTROL_PATH)
    parser.add_argument("--python", default=os.environ.get("VAN_COMPUTE_PYTHON", "/opt/homebrew/bin/python3"))
    parser.add_argument("--sqlite3", default="/usr/bin/sqlite3")
    parser.add_argument("--rg", default="/opt/homebrew/bin/rg")
    parser.add_argument("--jadx", default="/opt/homebrew/bin/jadx")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--connect-timeout", type=int, default=5)
    parser.add_argument("--nice", type=int, default=10)
    parser.add_argument("--max-result-bytes", type=int, default=DEFAULT_MAX_RESULT_BYTES)
    parser.add_argument("--max-memory-bytes", type=int, default=DEFAULT_MAX_MEMORY_BYTES)
    parser.add_argument("--max-processes", type=int, default=DEFAULT_MAX_PROCESSES)
    parser.add_argument("--min-free-bytes", type=int, default=DEFAULT_MIN_FREE_BYTES)
    parser.add_argument(
        "--min-memory-headroom-bytes",
        type=int,
        default=DEFAULT_MIN_MEMORY_HEADROOM_BYTES,
    )
    parser.add_argument("--heartbeat-interval", type=float, default=DEFAULT_HEARTBEAT_INTERVAL)
    parser.add_argument("--poll-interval", type=float, default=DEFAULT_POLL_INTERVAL)
    parser.add_argument("--dataset-config", type=Path)
    parser.add_argument("--dataset", action="append", default=[], metavar="NAME=PATH")
    parser.add_argument(
        "--sandbox-profile",
        type=Path,
        help="experimental sandbox-exec profile; any profile error fails the job closed",
    )
    parser.add_argument(
        "--allow-unsandboxed-dynamic",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    raw_arguments = list(argv) if argv is not None else sys.argv[1:]
    if raw_arguments and raw_arguments[0] == "__exec__":
        return limited_child_main(raw_arguments[1:])
    os.umask(0o077)
    parser = build_parser()
    args = parser.parse_args(raw_arguments)
    try:
        if not 1 <= args.timeout <= 24 * 60 * 60:
            raise WorkerError("--timeout must be from 1 second through 24 hours")
        if not 0 <= args.nice <= 19:
            raise WorkerError("--nice must be from 0 through 19")
        if not 1024 <= args.max_result_bytes <= protocol.MAX_RESULT_BYTES:
            raise WorkerError("--max-result-bytes must be from 1 KiB through 128 MiB")
        if not 256 * 1024 * 1024 <= args.max_memory_bytes <= 64 * 1024 * 1024 * 1024:
            raise WorkerError("--max-memory-bytes must be from 256 MiB through 64 GiB")
        if not 1 <= args.max_processes <= 4096:
            raise WorkerError("--max-processes must be from 1 through 4096")
        if not 1024 * 1024 * 1024 <= args.min_free_bytes <= 10 * 1024**4:
            raise WorkerError("--min-free-bytes must be from 1 GiB through 10 TiB")
        if not 1024 * 1024 * 1024 <= args.min_memory_headroom_bytes <= 1024**4:
            raise WorkerError(
                "--min-memory-headroom-bytes must be from 1 GiB through 1 TiB"
            )
        if not 1 <= args.heartbeat_interval <= 40:
            raise WorkerError("--heartbeat-interval must be from 1 through 40 seconds")
        if not 1 <= args.poll_interval <= 300:
            raise WorkerError("--poll-interval must be from 1 through 300 seconds")
        if not DATASET_NAME_RE.fullmatch(args.worker) or len(args.worker) > 60:
            raise WorkerError("--worker must be a safe name no longer than 60 characters")
        args.executables = {
            "python": args.python,
            "sqlite3": args.sqlite3,
            "rg": args.rg,
            "jadx": args.jadx,
        }
        for family, executable in args.executables.items():
            path = Path(executable)
            if not path.is_file() or not os.access(path, os.X_OK):
                raise WorkerError(f"{family} executable does not exist: {executable}")
        args.datasets = load_datasets(args.dataset_config, args.dataset)
        if args.sandbox_profile is not None and not args.sandbox_profile.expanduser().is_file():
            raise WorkerError(f"sandbox profile does not exist: {args.sandbox_profile}")
        args.work_root = args.work_root.expanduser().resolve()
        args.work_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(args.work_root, 0o700)
        args.resource_manager = SchedulerResourceManager(
            args.work_root,
            minimum_free_bytes=args.min_free_bytes,
            maximum_result_bytes=args.max_result_bytes,
            maximum_job_memory_bytes=args.max_memory_bytes,
            minimum_memory_headroom_bytes=args.min_memory_headroom_bytes,
        )
        if not args.serve:
            payload = run_once(args)
            print(json.dumps(payload, indent=2, sort_keys=True))
            return 0 if payload.get("ok") else 1

        private_root = args.work_root.expanduser().resolve().parent
        private_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(private_root, 0o700)
        lock_path = private_root / "scheduler.lock"
        with lock_path.open("a+", encoding="utf-8") as lock:
            os.chmod(lock_path, 0o600)
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise WorkerError("another persistent scheduler is already running") from None
            stop_event = threading.Event()
            drain_event = threading.Event()

            def request_stop(signum: int, _frame: object) -> None:
                print(
                    f"van-compute-worker: received signal {signum}; stopping jobs cleanly",
                    file=sys.stderr,
                    flush=True,
                )
                stop_event.set()

            def request_drain(signum: int, _frame: object) -> None:
                print(
                    f"van-compute-worker: received signal {signum}; draining without new claims",
                    file=sys.stderr,
                    flush=True,
                )
                drain_event.set()

            previous_handlers = {
                signal.SIGTERM: signal.signal(signal.SIGTERM, request_stop),
                signal.SIGINT: signal.signal(signal.SIGINT, request_stop),
                signal.SIGUSR1: signal.signal(signal.SIGUSR1, request_drain),
            }
            multiplexers = [
                SSHMultiplexer(
                    args.host,
                    args.control_path.with_name(
                        f"{args.control_path.name}.{index}"
                    ),
                    connect_timeout=args.connect_timeout,
                )
                for index in range(SSH_CONTROL_CONNECTIONS)
            ]
            try:
                for multiplexer in multiplexers:
                    multiplexer.ensure()
                print(
                    json.dumps(
                        {
                            "ok": True,
                            "mode": "persistent",
                            "worker": args.worker,
                            "slots": SCHEDULER_SLOTS,
                            "ssh_control_connections": SSH_CONTROL_CONNECTIONS,
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )
                run_scheduler(
                    args,
                    stop_event=stop_event,
                    drain_event=drain_event,
                    multiplexers=multiplexers,
                )
            finally:
                stop_event.set()
                for multiplexer in multiplexers:
                    multiplexer.close()
                for signum, handler in previous_handlers.items():
                    signal.signal(signum, handler)
        return 0
    except (OSError, subprocess.SubprocessError, WorkerError, protocol.ProtocolError) as exc:
        print(f"van-compute-worker: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
