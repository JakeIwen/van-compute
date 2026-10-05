"""Shared child execution and result primitives; host differences are explicit.

Ownership, staging, telemetry serialization and publication stay with the host.
See docs in pi/docs/compute/VAN_COMPUTE.md for the preserved compatibility drifts.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
from typing import Callable, Mapping, Protocol, Sequence, TypeVar

from van_compute import protocol


class Host(Enum):
    PI = "pi"
    MAC = "mac"


class Sandbox(Protocol):
    """Map protocol paths/runtime argv and wrap a shell-free command."""

    def command_path(self, path: Path, area: str) -> Path: ...
    def executable(
        self, executable: str, family: str
    ) -> tuple[str, list[tuple[Path, str]]]: ...
    def wrap(self, command: Sequence[str]) -> list[str]: ...


@dataclass(frozen=True)
class ProcessOutcome:
    exit_code: int
    usage: object
    timed_out: bool
    interrupted: bool
    resource_limit: str | None
    resource_monitor_error: str | None
    minimum_filesystem_free_bytes: int | None
    peak_process_group_rss_bytes: int = 0
    peak_process_count: int = 0


T = TypeVar("T")


def run_child(
    command: Sequence[str],
    *,
    cwd: Path,
    environment: Mapping[str, str],
    result_root: Path,
    supervise: Callable[[subprocess.Popen], T],
) -> T:
    """One launch boundary: private redirected output and a new process group."""
    with (result_root / "stdout.txt").open("wb") as stdout, (
        result_root / "stderr.txt"
    ).open("wb") as stderr:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
        return supervise(process)


def wait4_nohang(process: subprocess.Popen):
    waited_pid, status, usage = os.wait4(process.pid, os.WNOHANG)
    if waited_pid == 0:
        return None
    process.returncode = os.waitstatus_to_exitcode(status)
    return process.returncode, usage


def wait_for_process(
    process: subprocess.Popen,
    *,
    host: Host,
    error_type: type[RuntimeError],
    timeout: int,
    should_stop: Callable[[], bool],
    work_path: Path | None,
    minimum_free_bytes: int,
    free_space_reader: Callable[[Path], int],
    maximum_memory: int = 0,
    maximum_processes: int = 0,
    resource_reader: Callable[[int], tuple[int, int]] | None = None,
    poll: Callable = wait4_nohang,
    clock=time,
) -> ProcessOutcome:
    """Shared wait/reap/escalation; Pi final disk sample and Mac RSS stay distinct."""
    deadline = clock.monotonic() + timeout
    next_resource_poll = 0.0
    timed_out = interrupted = False
    resource_limit = resource_monitor_error = None
    lowest_free_bytes = None
    peak_group_rss = peak_processes = 0

    def sample_free_space():
        nonlocal lowest_free_bytes, resource_limit, resource_monitor_error
        if work_path is None:
            return
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
            label = "execution safety threshold " if host is Host.PI else ""
            resource_limit = (
                f"filesystem free space fell below {label}{minimum_free_bytes} bytes"
            )

    def outcome(code, usage):
        return ProcessOutcome(
            code,
            usage,
            timed_out,
            interrupted,
            resource_limit,
            resource_monitor_error,
            lowest_free_bytes,
            peak_group_rss,
            peak_processes,
        )

    while True:
        completed = poll(process)
        if completed is not None:
            if host is Host.PI:
                sample_free_space()
            return outcome(
                (
                    137
                    if resource_limit
                    else 125 if resource_monitor_error else completed[0]
                ),
                completed[1],
            )
        if should_stop():
            interrupted = True
            break
        now = clock.monotonic()
        if now >= deadline:
            timed_out = True
            break
        if now >= next_resource_poll:
            if host is Host.MAC:
                assert resource_reader is not None
                try:
                    group_rss, process_count = resource_reader(process.pid)
                except (OSError, subprocess.SubprocessError, error_type) as exc:
                    resource_monitor_error = str(exc)
                    break
                peak_group_rss = max(peak_group_rss, group_rss)
                peak_processes = max(peak_processes, process_count)
                if group_rss > maximum_memory:
                    resource_limit = (
                        f"process-group RSS exceeded {maximum_memory} bytes"
                    )
                    break
                if process_count > maximum_processes:
                    resource_limit = f"process count exceeded {maximum_processes}"
                    break
            sample_free_space()
            if resource_limit is not None or resource_monitor_error is not None:
                break
            next_resource_poll = now + (0.5 if host is Host.PI else 1.0)
        clock.sleep(0.1)

    limited = (
        (resource_limit is not None or resource_monitor_error is not None)
        if host is Host.PI
        else bool(resource_limit or resource_monitor_error)
    )
    if limited:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        waited_pid, status, usage = os.wait4(process.pid, 0)
        if waited_pid != process.pid:
            label = "local analysis" if host is Host.PI else "analysis"
            raise error_type(f"lost track of resource-limited {label} process")
        process.returncode = os.waitstatus_to_exitcode(status)
        return outcome(137 if resource_limit else 125, usage)

    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    terminate_deadline = clock.monotonic() + 10
    while clock.monotonic() < terminate_deadline:
        completed = poll(process)
        if completed is not None:
            return outcome(124 if timed_out else 143, completed[1])
        clock.sleep(0.1)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    waited_pid, status, usage = os.wait4(process.pid, 0)
    if waited_pid != process.pid:
        label = "local analysis" if host is Host.PI else "analysis child"
        raise error_type(f"lost track of {label} process")
    process.returncode = os.waitstatus_to_exitcode(status)
    return outcome(124 if timed_out else 143, usage)


def process_group_exists(
    group: int, *, host: Host, error_type: type[RuntimeError]
) -> bool:
    try:
        os.killpg(group, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError as exc:
        if host is Host.MAC:
            return True
        raise error_type(
            f"cannot inspect analysis process group {group}: {exc}"
        ) from None


def terminate_group(
    group: int,
    *,
    exists: Callable[[int], bool],
    error_type: type[RuntimeError],
    host: Host,
    terminate_grace: float = 2.0,
    kill_grace: float = 2.0,
    clock=time,
) -> bool:
    if not exists(group):
        return False
    for sig, grace in ((signal.SIGTERM, terminate_grace), (signal.SIGKILL, kill_grace)):
        try:
            os.killpg(group, sig)
        except ProcessLookupError:
            return True
        deadline = clock.monotonic() + grace
        while clock.monotonic() < deadline:
            if not exists(group):
                return True
            clock.sleep(0.05)
    label = f" {group}" if host is Host.PI else ""
    raise error_type(f"analysis process group{label} survived SIGKILL")


def resource_usage(usage, wall_seconds: float) -> dict[str, object]:
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
    }


def real_declared_output(
    result_root: Path, relative: Path, error_type: type[RuntimeError]
) -> Path | None:
    try:
        root_info = result_root.lstat()
    except OSError as exc:
        raise error_type(f"cannot inspect result root: {exc}") from None
    if stat.S_ISLNK(root_info.st_mode) or not stat.S_ISDIR(root_info.st_mode):
        raise error_type("result root is not a real directory")
    current = result_root
    for index, part in enumerate(relative.parts):
        current = current / part
        try:
            info = current.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise error_type(
                f"cannot inspect declared output component {current}: {exc}"
            ) from None
        if stat.S_ISLNK(info.st_mode):
            raise error_type(
                f"declared output has a symlinked path component: {relative}"
            )
        if index < len(relative.parts) - 1 and not stat.S_ISDIR(info.st_mode):
            return None
        if index == len(relative.parts) - 1:
            if stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode):
                return current
            raise error_type(f"declared output is a special file: {relative}")
    return None


def declared_paths(
    manifest: Mapping[str, object], *, host: Host, error_type: type[RuntimeError]
) -> list[Path]:
    embedded = manifest.get("execution")
    if embedded is None:
        return []
    declared = [
        Path(str(item)) for item in protocol.validate_execution(embedded)["outputs"]
    ]
    if host is Host.MAC:
        for index, first in enumerate(declared):
            for second in declared[index + 1 :]:
                if (
                    first == second
                    or first in second.parents
                    or second in first.parents
                ):
                    raise error_type(
                        f"declared result paths overlap: {first} and {second}"
                    )
    return declared


def validate_declared_outputs(
    manifest: Mapping[str, object],
    result_root: Path,
    *,
    require_all: bool,
    host: Host,
    error_type: type[RuntimeError],
) -> None:
    missing = [
        path.as_posix()
        for path in declared_paths(manifest, host=host, error_type=error_type)
        if real_declared_output(result_root, path, error_type) is None
    ]
    if require_all and missing:
        raise error_type(
            "successful job omitted declared result(s): " + ", ".join(missing)
        )


def package_output_directories(
    manifest: Mapping[str, object],
    result_root: Path,
    maximum_bytes: int,
    *,
    host: Host,
    error_type: type[RuntimeError],
) -> dict[str, str]:
    declared = declared_paths(manifest, host=host, error_type=error_type)
    artifacts = {}
    for relative in declared:
        output = real_declared_output(result_root, relative, error_type)
        if output is None or not output.is_dir():
            continue
        estimated = 4096
        for count, entry in enumerate(output.rglob("*"), 1):
            if host is Host.MAC and count > 100_000:
                raise error_type(f"declared output has too many entries: {relative}")
            if entry.is_symlink():
                raise error_type(f"declared output contains a symlink: {relative}")
            if entry.is_file():
                estimated += entry.stat().st_size
            elif not entry.is_dir():
                raise error_type(f"declared output contains a special file: {relative}")
            estimated += 4096
            if estimated > maximum_bytes:
                article = "the " if host is Host.MAC else ""
                raise error_type(
                    f"declared output exceeds {article}result limit: {relative}"
                )
        archive_relative = relative.with_name(relative.name + ".tar.gz")
        archive = result_root / archive_relative
        if host is Host.PI:
            archive.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if (
            (host is Host.MAC and archive_relative in declared)
            or archive.exists()
            or archive.is_symlink()
        ):
            text = (
                "collides with another result" if host is Host.MAC else "already exists"
            )
            raise error_type(f"output archive path {text}: {archive_relative}")
        if host is Host.MAC:
            archive.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            temporary = archive.with_name(f".{archive.name}.partial")
        else:
            descriptor, name = tempfile.mkstemp(
                prefix=f".{archive.name}.", suffix=".partial", dir=archive.parent
            )
            temporary = Path(name)

        def portable_metadata(info):
            info.uid = info.gid = info.mtime = 0
            info.uname = info.gname = ""
            return info

        try:
            with (
                temporary.open("xb")
                if host is Host.MAC
                else os.fdopen(descriptor, "wb")
            ) as raw_archive:
                with tarfile.open(
                    fileobj=raw_archive, mode="w:gz", dereference=False
                ) as bundle:
                    if host is Host.MAC:
                        bundle.add(
                            output,
                            arcname=relative.as_posix(),
                            recursive=True,
                            filter=portable_metadata,
                        )
                    else:
                        bundle.add(output, arcname=relative.as_posix(), recursive=True)
                if host is Host.PI:
                    raw_archive.flush()
                    os.fsync(raw_archive.fileno())
                    archive_size = os.fstat(raw_archive.fileno()).st_size
            if host is Host.MAC:
                archive_size = temporary.stat().st_size
            if archive_size > maximum_bytes:
                message = (
                    "output archive exceeds the result limit"
                    if host is Host.MAC
                    else "declared output archive exceeds result limit"
                )
                raise error_type(f"{message}: {relative}")
            os.replace(temporary, archive)
        finally:
            temporary.unlink(missing_ok=True)
        shutil.rmtree(output)
        artifacts[relative.as_posix()] = archive_relative.as_posix()
    return artifacts


def result_files(
    result_root: Path,
    maximum_bytes: int,
    *,
    expected: set[str] | None,
    host: Host,
    error_type: type[RuntimeError],
) -> list[tuple[str, Path]]:
    files = []
    total = 0
    for path in sorted(result_root.rglob("*")):
        if path.is_symlink():
            raise error_type(f"result contains a symlink: {path}")
        if not path.is_file():
            continue
        relative = path.relative_to(result_root).as_posix()
        if expected is not None and relative not in expected:
            raise error_type(f"job produced undeclared result file: {relative}")
        size = path.stat().st_size
        article = "the " if host is Host.MAC else ""
        if size > maximum_bytes:
            raise error_type(f"result exceeds {article}per-file limit: {relative}")
        # Mac historically stats twice; preserve the check/read order.
        total += path.stat().st_size if host is Host.MAC else size
        if total > maximum_bytes:
            raise error_type(f"job results exceed {article}total result limit")
        files.append((relative, path))
    if len(files) > protocol.MAX_OUTPUTS + 3:
        message = (
            f"job produced more than {protocol.MAX_OUTPUTS + 3} result files"
            if host is Host.MAC
            else "job produced too many result files"
        )
        raise error_type(message)
    return files


def _bubblewrap_command(
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
    error_type: type[RuntimeError],
) -> list[str]:
    """Build a fixed, no-network sandbox around one protocol command."""
    bwrap = Path(executable)
    if not bwrap.is_absolute() or not bwrap.is_file() or not os.access(bwrap, os.X_OK):
        raise error_type(f"bubblewrap is required for Pi fallback: {bwrap}")
    wrapped = [
        str(bwrap),
        "--die-with-parent",
        "--new-session",
        "--unshare-user",
        "--unshare-pid",
        "--unshare-net",
        "--unshare-ipc",
        "--unshare-uts",
        "--uid",
        "0",
        "--gid",
        "0",
        "--cap-drop",
        "ALL",
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
            raise error_type(f"sandbox runtime is not a real directory: {host}")
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


def _sandbox_executable(
    executable: str, family: str, error_type: type[RuntimeError]
) -> tuple[str, list[tuple[Path, str]]]:
    """Map a dedicated Python venv into the sandbox without exposing its home."""
    path = Path(executable)
    for system_root in (Path("/usr"), Path("/bin"), Path("/sbin"), Path("/lib")):
        try:
            path.relative_to(system_root)
            return str(path), []
        except ValueError:
            continue
    if family != "python" or path.parent.name != "bin":
        raise error_type(f"non-system {family} runtime is not supported: {path}")
    runtime_root = path.parent.parent
    sandbox_root = "/job/runtime/python"
    return f"{sandbox_root}/bin/{path.name}", [(runtime_root, sandbox_root)]


def _mac_sandbox_command(
    command: Sequence[str],
    *,
    profile: Path | None,
    job_root: Path,
    source_root: Path,
    result_root: Path,
    environment: Mapping[str, str],
    datasets: Mapping[str, Path],
    worker_root: Path,
    error_type: type[RuntimeError],
) -> list[str]:
    if profile is None:
        return list(command)
    profile = profile.expanduser().resolve()
    if not profile.is_file():
        raise error_type(f"sandbox profile does not exist: {profile}")
    sandbox = Path("/usr/bin/sandbox-exec")
    if not sandbox.is_file():
        raise error_type("sandbox-exec was requested but is unavailable")
    parameters = {
        "WORKER_ROOT": worker_root,
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


@dataclass(frozen=True)
class BubblewrapSandbox:
    binary: str
    source_root: Path
    inputs_root: Path
    result_root: Path
    home_root: Path
    temporary_root: Path
    cache_root: Path
    job_id: str
    error_type: type[RuntimeError]
    runtime_bindings: Sequence[tuple[Path, str]] = ()

    def command_path(self, path: Path, area: str) -> Path:
        root = {
            "source": self.source_root,
            "inputs": self.inputs_root,
            "result": self.result_root,
        }[area]
        return Path("/job") / area / path.relative_to(root)

    def executable(
        self, executable: str, family: str
    ) -> tuple[str, list[tuple[Path, str]]]:
        return _sandbox_executable(executable, family, self.error_type)

    def wrap(self, command: Sequence[str]) -> list[str]:
        return _bubblewrap_command(
            self.binary,
            command,
            source_root=self.source_root,
            inputs_root=self.inputs_root,
            result_root=self.result_root,
            home_root=self.home_root,
            temporary_root=self.temporary_root,
            cache_root=self.cache_root,
            job_id=self.job_id,
            runtime_bindings=self.runtime_bindings,
            error_type=self.error_type,
        )


@dataclass(frozen=True)
class MacSandbox:
    profile: Path | None
    job_root: Path
    source_root: Path
    result_root: Path
    environment: Mapping[str, str]
    datasets: Mapping[str, Path]
    worker_root: Path
    error_type: type[RuntimeError]

    def command_path(self, path: Path, area: str) -> Path:
        return path

    def executable(
        self, executable: str, family: str
    ) -> tuple[str, list[tuple[Path, str]]]:
        return executable, []

    def wrap(self, command: Sequence[str]) -> list[str]:
        return _mac_sandbox_command(
            command,
            profile=self.profile,
            job_root=self.job_root,
            source_root=self.source_root,
            result_root=self.result_root,
            environment=self.environment,
            datasets=self.datasets,
            worker_root=self.worker_root,
            error_type=self.error_type,
        )
