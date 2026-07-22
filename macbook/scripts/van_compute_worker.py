#!/usr/bin/env python3
"""Pull and execute one allowlisted vanpi analysis job on this Mac.

The worker initiates every connection.  It needs ordinary key-based SSH access
to vanpi, but the Mac does not need Remote Login enabled.  Commands are built as
argument vectors from the shared task catalog and never passed through a local
shell.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from typing import BinaryIO, Sequence


REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
from shared.python import van_compute_protocol as protocol


DEFAULT_HOST = "pi@vanpi"
DEFAULT_REMOTE_CLI = "/home/pi/scripts/van_compute.py"
DEFAULT_WORK_ROOT = Path.home() / "Library" / "Caches" / "van-compute"
DEFAULT_TIMEOUT = 3600
DEFAULT_MAX_RESULT_BYTES = 128 * 1024 * 1024
COPY_CHUNK = 1024 * 1024


class WorkerError(RuntimeError):
    pass


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(COPY_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def safe_filename(name: str, index: int) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", Path(name).name).strip("._")
    return f"{index:03d}-{cleaned or 'input'}"


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
    ) -> None:
        self.host = host
        self.remote_cli = remote_cli
        self.worker = worker
        self.ssh = ssh
        self.remote_root = remote_root
        self.connect_timeout = connect_timeout

    def _remote_arguments(self, *arguments: str) -> list[str]:
        result = [self.remote_cli]
        if self.remote_root:
            result.extend(("--root", self.remote_root))
        result.extend(arguments)
        return result

    def _ssh_arguments(self, *arguments: str) -> list[str]:
        remote_command = shlex.join(self._remote_arguments(*arguments))
        return [
            self.ssh,
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
        ]

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

    def heartbeat(self) -> dict[str, object]:
        return self.json_command("worker", "heartbeat", "--worker", self.worker)

    def claim(self) -> dict[str, object] | None:
        payload = self.json_command("worker", "claim", "--worker", self.worker)
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
                detail = process.stderr.decode("utf-8", "replace").strip()
                raise WorkerError(detail or f"stream failed with status {process.returncode}")
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)

    def stream_source(self, job_id: str, relative: str, destination: Path) -> None:
        self.stream_to_file(
            ("worker", "stream", job_id, "--worker", self.worker, "--kind", "source", "--path", relative),
            destination,
        )

    def stream_input(self, job_id: str, index: int, destination: Path) -> None:
        self.stream_to_file(
            ("worker", "stream", job_id, "--worker", self.worker, "--kind", "input", "--index", str(index)),
            destination,
        )

    def put_result(self, job_id: str, relative: str, path: Path) -> dict[str, object]:
        with path.open("rb") as handle:
            return self.json_command(
                "worker", "put-result", job_id, "--worker", self.worker, "--path", relative,
                input_file=handle,
            )

    def finish(self, job_id: str, exit_code: int, result_files: Sequence[str]) -> dict[str, object]:
        arguments = [
            "worker", "finish", job_id, "--worker", self.worker, "--exit-code", str(exit_code)
        ]
        for relative in result_files:
            arguments.extend(("--result-file", relative))
        return self.json_command(*arguments)


def prepare_job(remote: RemoteQueue, manifest: dict[str, object], job_root: Path) -> tuple[Path, list[Path], list[object | None]]:
    job_id = str(manifest.get("id", ""))
    source_root = job_root / "source"
    inputs_root = job_root / "inputs"
    sources = manifest.get("sources")
    inputs = manifest.get("inputs")
    if not isinstance(sources, list) or not isinstance(inputs, list):
        raise WorkerError("job manifest is missing sources or inputs")

    for record in sources:
        if not isinstance(record, dict):
            raise WorkerError("job has an invalid source record")
        relative_text = str(record.get("path", ""))
        relative = Path(relative_text)
        if relative.is_absolute() or ".." in relative.parts:
            raise WorkerError(f"job has an unsafe source path: {relative_text!r}")
        destination = source_root / relative
        remote.stream_source(job_id, relative_text, destination)
        if destination.stat().st_size != record.get("size") or hash_file(destination) != record.get("sha256"):
            raise WorkerError(f"source verification failed: {relative_text}")

    input_paths: list[Path] = []
    values: list[object | None] = []
    for index, record in enumerate(inputs):
        if not isinstance(record, dict) or record.get("index") != index:
            raise WorkerError("job has an invalid input record")
        destination = inputs_root / safe_filename(str(record.get("name", "input")), index)
        remote.stream_input(job_id, index, destination)
        if destination.stat().st_size != record.get("size"):
            raise WorkerError(f"input size verification failed for index {index}")
        input_paths.append(destination)
        values.append(record.get("value"))
    return source_root, input_paths, values


def child_limits(nice: int, timeout: int, maximum_file_size: int) -> None:
    # Sandboxed or launchd-managed processes can have immutable hard limits.
    # The parent still enforces wall time and validates result sizes, so a
    # platform refusal here should not prevent an otherwise safe job.
    for operation in (
        lambda: os.nice(nice),
        lambda: resource.setrlimit(resource.RLIMIT_CPU, (timeout + 30, timeout + 60)),
        lambda: resource.setrlimit(
            resource.RLIMIT_FSIZE, (maximum_file_size, maximum_file_size)
        ),
    ):
        try:
            operation()
        except (OSError, ValueError):
            pass


def child_resource_usage(before, after, wall_seconds: float) -> dict[str, object]:
    """Return measured analysis-child usage, excluding earlier SSH helpers."""
    user_seconds = max(0.0, after.ru_utime - before.ru_utime)
    system_seconds = max(0.0, after.ru_stime - before.ru_stime)
    cpu_seconds = user_seconds + system_seconds
    # Darwin reports ru_maxrss in bytes; Linux and most BSD-derived Python
    # builds report KiB.  The production worker is Darwin, but keeping the
    # conversion explicit makes local protocol tests portable.
    peak_rss_bytes = int(after.ru_maxrss)
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
        "minor_page_faults": max(0, after.ru_minflt - before.ru_minflt),
        "major_page_faults": max(0, after.ru_majflt - before.ru_majflt),
        "voluntary_context_switches": max(0, after.ru_nvcsw - before.ru_nvcsw),
        "involuntary_context_switches": max(0, after.ru_nivcsw - before.ru_nivcsw),
        "scope": "analysis child process tree",
        "peak_rss_note": "process-tree upper bound from getrusage(RUSAGE_CHILDREN)",
    }


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
) -> tuple[int, dict[str, object]]:
    task_name = str(manifest.get("task", ""))
    arguments = manifest.get("arguments", [])
    inputs = manifest.get("inputs", [])
    if not isinstance(arguments, list) or not all(isinstance(item, str) for item in arguments):
        raise WorkerError("job arguments are invalid")
    if not isinstance(inputs, list):
        raise WorkerError("job inputs are invalid")
    protocol.validate_inputs(task_name, inputs)
    command = protocol.build_command(
        task_name,
        python=python,
        source_root=source_root,
        input_paths=input_paths,
        input_values=input_values,
        result_root=result_root,
        arguments=arguments,
    )
    result_root.mkdir(parents=True, exist_ok=True)
    stdout_path = result_root / "stdout.txt"
    stderr_path = result_root / "stderr.txt"
    environment = {
        key: value
        for key, value in os.environ.items()
        if key in {"HOME", "LANG", "LC_ALL", "PATH", "TMPDIR", "USER"}
    }
    environment.update(
        {
            "PYTHONPATH": str(source_root),
            "PYTHONNOUSERSITE": "1",
            "VAN_COMPUTE_JOB_ID": str(manifest.get("id", "")),
        }
    )
    started = utc_now()
    started_monotonic = time.monotonic()
    timed_out = False
    usage_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
        process = subprocess.Popen(
            command,
            cwd=source_root,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
            preexec_fn=lambda: child_limits(nice, timeout, maximum_file_size),
        )
        try:
            exit_code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGTERM)
            try:
                exit_code = process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                exit_code = process.wait()
            exit_code = 124
    usage_after = resource.getrusage(resource.RUSAGE_CHILDREN)
    duration_seconds = time.monotonic() - started_monotonic
    execution = {
        "schema_version": protocol.SCHEMA_VERSION,
        "job_id": manifest.get("id"),
        "task": task_name,
        "worker": socket.gethostname(),
        "started_at": started,
        "finished_at": utc_now(),
        "duration_seconds": round(duration_seconds, 6),
        "exit_code": exit_code,
        "timed_out": timed_out,
        "command": command,
        "resource_usage": child_resource_usage(
            usage_before, usage_after, duration_seconds
        ),
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
    return exit_code, execution


def result_files(result_root: Path, maximum_file_size: int) -> list[tuple[str, Path]]:
    files: list[tuple[str, Path]] = []
    for path in sorted(result_root.rglob("*")):
        if path.is_symlink():
            raise WorkerError(f"result contains a symlink: {path}")
        if not path.is_file():
            continue
        relative = path.relative_to(result_root).as_posix()
        if path.stat().st_size > maximum_file_size:
            raise WorkerError(f"result exceeds the per-file limit: {relative}")
        files.append((relative, path))
    if len(files) > 64:
        raise WorkerError("job produced more than 64 result files")
    return files


def record_worker_failure(result_root: Path, manifest: dict[str, object], exc: Exception) -> int:
    result_root.mkdir(parents=True, exist_ok=True)
    message = f"van-compute worker: {type(exc).__name__}: {exc}\n"
    (result_root / "stderr.txt").write_text(message, encoding="utf-8")
    (result_root / "stdout.txt").touch()
    execution = {
        "schema_version": protocol.SCHEMA_VERSION,
        "job_id": manifest.get("id"),
        "task": manifest.get("task"),
        "worker": socket.gethostname(),
        "finished_at": utc_now(),
        "exit_code": 70,
        "worker_error": str(exc),
    }
    (result_root / "execution.json").write_text(
        json.dumps(execution, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 70


def run_once(args: argparse.Namespace) -> dict[str, object]:
    work_root = args.work_root.expanduser().resolve()
    work_root.mkdir(parents=True, exist_ok=True)
    os.chmod(work_root, 0o700)
    remote = RemoteQueue(
        args.host,
        args.remote_cli,
        args.worker,
        remote_root=args.remote_root,
        connect_timeout=args.connect_timeout,
    )
    remote.heartbeat()
    manifest = remote.claim()
    if manifest is None:
        return {"ok": True, "worker": args.worker, "job": None}
    job_id = str(manifest.get("id", ""))
    with tempfile.TemporaryDirectory(prefix=f"{job_id}-", dir=work_root) as temporary:
        job_root = Path(temporary)
        result_root = job_root / "result"
        try:
            source_root, input_paths, values = prepare_job(remote, manifest, job_root)
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
            )
        except Exception as exc:
            exit_code = record_worker_failure(result_root, manifest, exc)
            execution = {"worker_error": str(exc), "exit_code": exit_code}
        files = result_files(result_root, args.max_result_bytes)
        uploaded: list[str] = []
        for relative, path in files:
            remote.put_result(job_id, relative, path)
            uploaded.append(relative)
        completed = remote.finish(job_id, exit_code, uploaded)
        return {
            "ok": exit_code == 0,
            "worker": args.worker,
            "job": job_id,
            "execution": execution,
            "state": completed.get("state"),
            "results": uploaded,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=os.environ.get("VAN_COMPUTE_HOST", DEFAULT_HOST))
    parser.add_argument("--remote-cli", default=os.environ.get("VAN_COMPUTE_REMOTE_CLI", DEFAULT_REMOTE_CLI))
    parser.add_argument("--remote-root", default=os.environ.get("VAN_COMPUTE_ROOT"))
    parser.add_argument("--worker", default=os.environ.get("VAN_COMPUTE_WORKER", socket.gethostname().split(".")[0]))
    parser.add_argument("--work-root", type=Path, default=DEFAULT_WORK_ROOT)
    parser.add_argument("--python", default=os.environ.get("VAN_COMPUTE_PYTHON", "/opt/homebrew/bin/python3"))
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--connect-timeout", type=int, default=5)
    parser.add_argument("--nice", type=int, default=10)
    parser.add_argument("--max-result-bytes", type=int, default=DEFAULT_MAX_RESULT_BYTES)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if not 1 <= args.timeout <= 24 * 60 * 60:
            raise WorkerError("--timeout must be from 1 second through 24 hours")
        if not 0 <= args.nice <= 19:
            raise WorkerError("--nice must be from 0 through 19")
        if not 1024 <= args.max_result_bytes <= 1024 * 1024 * 1024:
            raise WorkerError("--max-result-bytes must be from 1 KiB through 1 GiB")
        if not Path(args.python).is_file():
            raise WorkerError(f"Python executable does not exist: {args.python}")
        payload = run_once(args)
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0 if payload.get("ok") else 1
    except (OSError, subprocess.SubprocessError, WorkerError, protocol.ProtocolError) as exc:
        print(f"van-compute-worker: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
