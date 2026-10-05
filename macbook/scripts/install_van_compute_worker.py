#!/usr/bin/python3 -B
"""Deploy the coupled van-compute Pi broker and persistent Mac worker.

The shell entry point is retained as a compatibility shim. This module owns the
actual deployment so the safety-sensitive ordering can be exercised with fake
local and remote runners.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import plistlib
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from typing import Callable, Mapping, Sequence, TextIO
import uuid


LABEL = "com.jacobr.van-compute-worker"
DEFAULT_HOST = "pi@vanpi.lan"
DEFAULT_WORKER = "m4mac"
REMOTE_ROOT = "/home/pi/van_compute"
REMOTE_SCRIPTS = f"{REMOTE_ROOT}/scripts"
REMOTE_CONFIGS = f"{REMOTE_ROOT}/configs"
REMOTE_RELEASES = f"{REMOTE_ROOT}/releases"
REMOTE_VENV = f"{REMOTE_ROOT}/venv"
OLD_COMPUTE_ROOT = "/home/pi/scripts/compute"
QUEUE_ROOT = "/home/pi/dev/obd-things/tmp/compute"
SOURCE_HASH_FILE = "source.sha256"
DEPLOYMENT_HASH_FILE = "deployment.sha256"
PROVENANCE_FILE = "provenance.json"
MANIFEST_FILE = "manifest.json"
OWNER_RE = re.compile(
    r"installer-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
)
SAFE_WORKER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,59}")

SANDBOX_PROFILE = r'''(version 1)
(deny default)
(allow process*)
(allow sysctl-read)
(allow ipc-posix*)
; Metadata is needed to traverse operator-configured dataset roots. Contents
; and directory reads remain constrained by file-read* below.
(allow file-read-metadata)
(allow file-read*
    (subpath "/System/Library")
    ; macOS 26 stores the dyld shared cache used by system executables here.
    (subpath "/System/Volumes/Preboot/Cryptexes/OS")
    (subpath "/usr")
    (subpath "/bin")
    (subpath "/sbin")
    (subpath "/opt/homebrew")
    (subpath "/private/etc")
    (subpath "/private/var/db/timezone")
    (subpath "/Library/Apple")
    (subpath "/Library/Java")
    (subpath (param "WORKER_ROOT"))
    (subpath (param "JOB_ROOT"))
    (subpath (param "DATASET_0"))
    (subpath (param "DATASET_1"))
    (subpath (param "DATASET_2"))
    (subpath (param "DATASET_3"))
    (subpath (param "DATASET_4"))
    (subpath (param "DATASET_5"))
    (subpath (param "DATASET_6"))
    (subpath (param "DATASET_7"))
    (subpath (param "DATASET_8"))
    (subpath (param "DATASET_9"))
    (subpath (param "DATASET_10"))
    (subpath (param "DATASET_11"))
    (subpath (param "DATASET_12"))
    (subpath (param "DATASET_13"))
    (subpath (param "DATASET_14"))
    (subpath (param "DATASET_15"))
    (literal "/dev/null")
    (literal "/dev/random")
    (literal "/dev/urandom"))
(allow file-write*
    (subpath (param "JOB_ROOT"))
    (literal "/dev/null"))
'''

SANDBOX_DENIAL_PROBE = r'''
import errno
from pathlib import Path
import socket
import sys

for index, raw_sentinel in enumerate(sys.argv[1:]):
    sentinel = Path(raw_sentinel)
    try:
        sentinel.read_text(encoding="utf-8")
    except OSError as exc:
        if index and exc.errno == errno.ENOENT:
            continue
        if exc.errno not in {errno.EACCES, errno.EPERM}:
            raise
    else:
        raise SystemExit(f"sandbox read a private-home sentinel via {sentinel}")
probe_socket = None
try:
    probe_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    probe_socket.sendto(b"x", ("127.0.0.1", 9))
except OSError as exc:
    if exc.errno not in {errno.EACCES, errno.EPERM}:
        raise
else:
    raise SystemExit("sandbox allowed network output")
finally:
    if probe_socket is not None:
        probe_socket.close()
'''


RELEASE_LINK_GUARD = r'''
check_release_target() {
  release_target_name="${2#"$1/releases/"}"
  case "$release_target_name" in ''|*[!0123456789abcdef]*) return 1;; esac
  test "${#release_target_name}" -eq 24 || return 1
  test "$2" = "$1/releases/$release_target_name"
}
'''


class DeploymentError(RuntimeError):
    """A fail-closed installer error suitable for an operator."""


@dataclass(frozen=True)
class Options:
    if_needed: bool = False
    dry_run: bool = False
    heartbeat_timeout: int = 45
    submitter_timeout: int = 120


@dataclass(frozen=True)
class Paths:
    repo_root: Path
    home: Path
    source_plist: Path
    target_dir: Path
    target_plist: Path
    cache_root: Path
    support_root: Path
    release_parent: Path
    dataset_target: Path
    installer_lock: Path
    owner_file: Path

    @classmethod
    def discover(cls, script: Path, home: Path) -> "Paths":
        script = script.resolve()
        # A frozen installer is copied to <release>/app/macbook/scripts. Its
        # source root is that app directory, while a checkout uses parents[2].
        checkout_root = script.parents[2]
        if (checkout_root / "van_compute").is_dir():
            repo_root = checkout_root
        else:
            app_root = script.parents[2]
            if not (app_root / "van_compute").is_dir():
                raise DeploymentError(
                    "cannot locate the van_compute deployment source root"
                )
            repo_root = app_root
        support_root = home / "Library" / "Application Support" / "van-compute"
        target_dir = home / "Library" / "LaunchAgents"
        cache_root = home / "Library" / "Caches" / "van-compute"
        return cls(
            repo_root=repo_root,
            home=home,
            source_plist=repo_root / "macbook" / "launchagents" / f"{LABEL}.plist",
            target_dir=target_dir,
            target_plist=target_dir / f"{LABEL}.plist",
            cache_root=cache_root,
            support_root=support_root,
            release_parent=support_root / "releases",
            dataset_target=support_root / "datasets.json",
            installer_lock=support_root / "installer.lock",
            owner_file=support_root / "installer-owner",
        )


@dataclass(frozen=True)
class SourceRelease:
    files: tuple[Path, ...]
    source_fingerprint: str
    deployment_fingerprint: str
    pi_version: str
    mac_version: str
    dataset_source: Path | None
    dataset_fingerprint: str
    allow_unsandboxed: bool


@dataclass
class State:
    remote_stage_created: bool = False
    previous_agent_disabled: bool = False
    restore_previous_agent: bool = False
    submission_gate_active: bool = False
    maintenance_active: bool = False
    cutover_started: bool = False
    rollback_safe: bool = True
    upgrade_public_root: str = ""


class LocalRunner:
    """Vector-only local command runner; injectable in tests."""

    def run(
        self,
        arguments: Sequence[str],
        *,
        input_text: str | None = None,
        capture_output: bool = False,
        check: bool = True,
        timeout: float | None = None,
        cwd: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        try:
            return subprocess.run(
                list(arguments),
                input=input_text,
                stdin=subprocess.DEVNULL if input_text is None else None,
                stdout=subprocess.PIPE if capture_output else None,
                stderr=subprocess.PIPE if capture_output else None,
                text=True,
                timeout=timeout,
                check=check,
                cwd=cwd,
            )
        except (
            OSError,
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
        ) as exc:
            detail = ""
            if isinstance(exc, subprocess.CalledProcessError):
                detail = (exc.stderr or exc.stdout or "").strip()
            raise DeploymentError(
                detail or f"local command failed: {shlex.join(arguments)}"
            ) from None


class RemoteRunner:
    """One-purpose SSH/SCP adapter with named calls for deterministic fakes."""

    def __init__(self, host: str, local: LocalRunner) -> None:
        self.host = host
        self.local = local

    def run(
        self,
        name: str,
        script: str,
        arguments: Sequence[str] = (),
        *,
        capture_output: bool = False,
        timeout: float | None = None,
    ) -> str:
        remote_command = "/bin/sh -s -- " + " ".join(
            shlex.quote(item) for item in arguments
        )
        completed = self.local.run(
            [
                "/usr/bin/ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=5",
                self.host,
                remote_command,
            ],
            input_text=script,
            capture_output=capture_output,
            timeout=timeout,
        )
        return completed.stdout if capture_output else ""

    def upload(self, name: str, sources: Sequence[Path], destination: str) -> None:
        self.local.run(
            [
                "/usr/bin/scp",
                "-r",
                *(str(path) for path in sources),
                f"{self.host}:{destination}",
            ]
        )


class Installer:
    def __init__(
        self,
        options: Options,
        *,
        environment: Mapping[str, str] | None = None,
        home: Path | None = None,
        script: Path | None = None,
        local: LocalRunner | None = None,
        remote: RemoteRunner | None = None,
        stdout: TextIO = sys.stdout,
        stderr: TextIO = sys.stderr,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        uuid_factory: Callable[[], uuid.UUID] = uuid.uuid4,
    ) -> None:
        self.options = options
        self.environment = dict(os.environ if environment is None else environment)
        self.home = (Path.home() if home is None else home).expanduser().resolve()
        self.script = Path(__file__) if script is None else script
        self.paths = Paths.discover(self.script, self.home)
        self.host = self.environment.get("VAN_COMPUTE_HOST", DEFAULT_HOST)
        self.worker = self.environment.get("VAN_COMPUTE_WORKER", DEFAULT_WORKER)
        self.local = local or LocalRunner()
        self.remote = remote or RemoteRunner(self.host, self.local)
        self.stdout = stdout
        self.stderr = stderr
        self.sleep = sleep
        self.monotonic = monotonic
        self.uuid_factory = uuid_factory
        self.install_id = str(uuid_factory()).lower()
        self.remote_stage = f"/home/pi/.cache/van-compute-install.{self.install_id}"
        self.state = State()
        self.owner = ""
        self.release: Path | None = None
        self.source: SourceRelease | None = None
        self.prior_release: Path | None = None
        self.release_retention_ambiguous = False

    def say(self, message: str) -> None:
        print(message, file=self.stdout, flush=True)

    def warn(self, message: str) -> None:
        print(message, file=self.stderr, flush=True)

    @staticmethod
    def _regular_file(path: Path, description: str) -> None:
        try:
            details = path.lstat()
        except OSError as exc:
            raise DeploymentError(
                f"{description} is missing or unreadable: {path}: {exc}"
            ) from None
        if not stat.S_ISREG(details.st_mode):
            raise DeploymentError(
                f"{description} is not a regular non-symlink file: {path}"
            )

    @staticmethod
    def _hash_file(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
        return digest.hexdigest()

    def source_paths(self) -> tuple[Path, ...]:
        relative = [
            Path("macbook/scripts/install_van_compute_worker.zsh"),
            Path("macbook/scripts/install_van_compute_worker.py"),
            Path("macbook/launchagents") / f"{LABEL}.plist",
        ]
        package = self.paths.repo_root / "van_compute"
        entrypoints = package / "entrypoints"
        configs = package / "configs"
        if (
            not package.is_dir()
            or package.is_symlink()
            or not entrypoints.is_dir()
            or entrypoints.is_symlink()
            or not configs.is_dir()
            or configs.is_symlink()
        ):
            raise DeploymentError(
                f"compute deployment source is missing or unsafe: {package}"
            )
        relative.extend(
            path.relative_to(self.paths.repo_root)
            for path in sorted(package.glob("*.py"))
        )
        relative.extend(
            path.relative_to(self.paths.repo_root)
            for path in sorted(entrypoints.glob("*.py"))
        )
        relative.extend(
            (
                Path("van_compute/configs/van-compute-broker.service"),
                Path("van_compute/configs/van-compute-obd.example.json"),
            )
        )
        files = tuple(self.paths.repo_root / item for item in relative)
        for path in files:
            self._regular_file(path, "compute deployment source")
        return files

    def _fingerprint_source_tree(
        self, root: Path, relative_paths: Sequence[Path]
    ) -> str:
        digest = hashlib.sha256()
        for relative in relative_paths:
            path = root / relative
            self._regular_file(path, "compute deployment source")
            digest.update(relative.as_posix().encode() + b"\0")
            digest.update(bytes.fromhex(self._hash_file(path)))
        return digest.hexdigest()

    def _verify_staged_source(self, app_root: Path, source: SourceRelease) -> None:
        relative_paths = tuple(
            path.relative_to(self.paths.repo_root) for path in source.files
        )
        expected = {path.as_posix() for path in relative_paths}
        actual: set[str] = set()
        for path in app_root.rglob("*"):
            details = path.lstat()
            if stat.S_ISLNK(details.st_mode):
                raise DeploymentError(
                    f"staged deployment source contains a symlink: {path}"
                )
            if stat.S_ISDIR(details.st_mode):
                continue
            if not stat.S_ISREG(details.st_mode):
                raise DeploymentError(
                    f"staged deployment source contains a special entry: {path}"
                )
            actual.add(path.relative_to(app_root).as_posix())
        if actual != expected:
            raise DeploymentError(
                "staged deployment source file set changed after planning"
            )
        fingerprint = self._fingerprint_source_tree(app_root, relative_paths)
        if fingerprint != source.source_fingerprint:
            raise DeploymentError("staged deployment source changed after planning")

    def dataset_source(self) -> Path | None:
        configured = self.environment.get("VAN_COMPUTE_DATASET_CONFIG", "")
        if configured:
            path = Path(configured).expanduser()
            self._regular_file(path, "VAN_COMPUTE_DATASET_CONFIG")
            return path.resolve()
        default = (
            self.paths.repo_root / "macbook" / "secrets" / "van-compute-datasets.json"
        )
        if default.exists() or default.is_symlink():
            self._regular_file(default, "default dataset configuration")
            return default.resolve()
        return None

    def allow_unsandboxed(self) -> bool:
        raw = self.environment.get("VAN_COMPUTE_ALLOW_UNSANDBOXED")
        if (
            raw is None
            and self.paths.target_plist.is_file()
            and not self.paths.target_plist.is_symlink()
        ):
            try:
                payload = plistlib.loads(self.paths.target_plist.read_bytes())
                arguments = payload.get("ProgramArguments", [])
                raw = "1" if "--allow-unsandboxed-dynamic" in arguments else "0"
            except (OSError, plistlib.InvalidFileException, AttributeError):
                raw = "0"
        raw = "0" if raw is None else raw
        if raw not in {"0", "1"}:
            raise DeploymentError("VAN_COMPUTE_ALLOW_UNSANDBOXED must be 0 or 1")
        return raw == "1"

    def build_source_release(self) -> SourceRelease:
        if not SAFE_WORKER_RE.fullmatch(self.worker):
            raise DeploymentError(
                "VAN_COMPUTE_WORKER must be a safe name no longer than 60 characters"
            )
        files = self.source_paths()
        relative_paths = tuple(path.relative_to(self.paths.repo_root) for path in files)
        source_fingerprint = self._fingerprint_source_tree(
            self.paths.repo_root, relative_paths
        )
        dataset_source = self.dataset_source()
        if dataset_source is not None:
            dataset_fingerprint = self._hash_file(dataset_source)
        elif (
            self.paths.dataset_target.is_file()
            and not self.paths.dataset_target.is_symlink()
        ):
            dataset_fingerprint = self._hash_file(self.paths.dataset_target)
        else:
            dataset_fingerprint = "none"
        allow_unsandboxed = self.allow_unsandboxed()
        deployment = (
            f"source={source_fingerprint} host={self.host} worker={self.worker} "
            f"unsandboxed={int(allow_unsandboxed)} dataset={dataset_fingerprint}"
        )
        deployment_fingerprint = hashlib.sha256(deployment.encode()).hexdigest()
        return SourceRelease(
            files=files,
            source_fingerprint=source_fingerprint,
            deployment_fingerprint=deployment_fingerprint,
            pi_version=source_fingerprint[:24],
            mac_version=deployment_fingerprint[:24],
            dataset_source=dataset_source,
            dataset_fingerprint=dataset_fingerprint,
            allow_unsandboxed=allow_unsandboxed,
        )

    def plan(self, source: SourceRelease) -> dict[str, object]:
        return {
            "dry_run": True,
            "mode": "coupled",
            "if_needed": self.options.if_needed,
            "source_root": str(self.paths.repo_root),
            "host": self.host,
            "worker": self.worker,
            "pi_release": f"{REMOTE_RELEASES}/{source.pi_version}",
            "mac_release": str(self.paths.release_parent / source.mac_version),
            "source_sha256": source.source_fingerprint,
            "deployment_sha256": source.deployment_fingerprint,
            "local_operations": [
                "validate sandbox-exec and local prerequisites",
                "provision or verify an immutable Mac worker release",
                "validate the worker sandbox and resource watchdog",
                "atomically install and reload the persistent LaunchAgent",
            ],
            "remote_operations": [
                "preflight paths, prerequisites, queue emptiness, and upgrade ownership",
                "stage and validate the complete release before fencing",
                "provision or verify the locked fallback runtime before fencing",
                "drain the worker, fence all submitters, and enter queue maintenance",
                "publish current/previous atomically and restart the broker",
                "require a fresh compatible worker heartbeat before releasing maintenance",
            ],
            "rollback": {
                "before_cutover": "restore the exact queue CLI and prior LaunchAgent",
                "after_cutover_started": "leave the queue fenced and rerun this installer forward",
            },
            "remote_probes_executed": False,
        }

    def _run_capture(self, arguments: Sequence[str], *, check: bool = True) -> str:
        completed = self.local.run(arguments, capture_output=True, check=check)
        return completed.stdout

    def _launch_domain(self) -> str:
        try:
            user_id = os.getuid()
        except AttributeError:
            user_id = int(self._run_capture(["/usr/bin/id", "-u"]).strip())
        return f"gui/{user_id}/{LABEL}"

    def _launch_state(self) -> str | None:
        completed = self.local.run(
            ["/bin/launchctl", "print", self._launch_domain()],
            capture_output=True,
            check=False,
        )
        return completed.stdout if completed.returncode == 0 else None

    @staticmethod
    def _agent_pid(payload: str | None) -> str:
        if not payload:
            return ""
        match = re.search(r"^\s*pid = ([0-9]+)\s*$", payload, re.MULTILINE)
        return "" if match is None else match.group(1)

    def capture_prior_release(self) -> None:
        target = self.paths.target_plist
        if not target.exists() and not target.is_symlink():
            self.prior_release = None
            return
        try:
            self._regular_file(target, "installed LaunchAgent")
            payload = plistlib.loads(target.read_bytes())
            arguments = payload.get("ProgramArguments")
            if not isinstance(arguments, list):
                raise DeploymentError("installed LaunchAgent arguments are invalid")
            module_index = arguments.index("van_compute.worker")
            if module_index < 2 or arguments[module_index - 1] != "-m":
                raise DeploymentError("installed LaunchAgent is not package-based")
            python = Path(str(arguments[0]))
            release = python.parents[2]
            environment = payload.get("EnvironmentVariables")
            if (
                python != release / "venv/bin/python"
                or not isinstance(environment, dict)
                or environment.get("PYTHONPATH") != str(release / "app")
            ):
                raise DeploymentError(
                    "installed LaunchAgent release paths are inconsistent"
                )
            self._release_identity(release)
        except (
            DeploymentError,
            IndexError,
            OSError,
            ValueError,
            plistlib.InvalidFileException,
        ):
            self.release_retention_ambiguous = True
            self.warn(
                "WARNING: the prior Mac release could not be verified; retaining all releases."
            )
            return
        self.prior_release = release

    def deployment_current(self, source: SourceRelease) -> bool:
        try:
            self._regular_file(self.paths.target_plist, "installed LaunchAgent")
            plist = plistlib.loads(self.paths.target_plist.read_bytes())
            arguments = plist.get("ProgramArguments", [])
            environment = plist.get("EnvironmentVariables", {})
            if not isinstance(arguments, list) or not isinstance(environment, dict):
                return False
            module_index = arguments.index("van_compute.worker")
            if module_index < 2 or arguments[module_index - 1] != "-m":
                return False
            python = Path(str(arguments[0]))
            installed_release = python.parents[2]
            if (
                installed_release.parent != self.paths.release_parent
                or python != installed_release / "venv/bin/python"
                or installed_release.is_symlink()
                or not installed_release.is_dir()
                or environment.get("PYTHONPATH") != str(installed_release / "app")
            ):
                return False
            self._verify_release(installed_release, source)
            if self._launch_state() is None:
                return False
            script = (
                RELEASE_LINK_GUARD
                + r'''
set -eu
root="$1"
source_hash="$2"
worker="$3"
test -f "$root/deployment.sha256"
test ! -L "$root/deployment.sha256"
test "$(/bin/cat "$root/deployment.sha256")" = "$source_hash"
test -L "$root/current"
current="$(/usr/bin/readlink -e "$root/current")"
check_release_target "$root" "$current" || { echo "Invalid compute release target" >&2; exit 1; }
test -f "$current/source.sha256"
test "$(/bin/cat "$current/source.sha256")" = "$source_hash"
/usr/bin/systemctl is-active --quiet van-compute-broker.service
/usr/bin/systemctl cat van-compute-broker.service | /bin/grep -Fq "$root/current"
"$root/scripts/van_compute.py" available | /usr/bin/python3 -c '
import json, sys
payload = json.load(sys.stdin)
worker = next((item for item in payload.get("workers", []) if item.get("worker") == sys.argv[1]), None)
raise SystemExit(
    worker is None
    or worker.get("age_seconds", 999) > 45
    or worker.get("slots_total") != 10
    or not isinstance(worker.get("slots_busy"), int)
    or not 0 <= worker["slots_busy"] <= 10
)
' "$worker"
'''
            )
            self.remote.run(
                "current-deployment",
                script,
                [REMOTE_ROOT, source.source_fingerprint, self.worker],
            )
            return True
        except (
            DeploymentError,
            IndexError,
            OSError,
            ValueError,
            plistlib.InvalidFileException,
        ):
            return False

    def preflight_local(self, source: SourceRelease) -> None:
        # Parse the template without invoking plutil so malformed or surprising
        # input fails before package installation or any remote mutation.
        try:
            plist = plistlib.loads(self.paths.source_plist.read_bytes())
        except (OSError, plistlib.InvalidFileException) as exc:
            raise DeploymentError(f"invalid LaunchAgent template: {exc}") from None
        if plist.get("Label") != LABEL or not isinstance(
            plist.get("ProgramArguments"), list
        ):
            raise DeploymentError(
                "LaunchAgent template has an unexpected label or arguments"
            )
        if not source.allow_unsandboxed:
            self.say("Checking macOS sandbox capability...")
            completed = self.local.run(
                [
                    "/usr/bin/sandbox-exec",
                    "-p",
                    "(version 1)(allow default)",
                    "/usr/bin/true",
                ],
                capture_output=True,
                check=False,
            )
            if completed.returncode != 0:
                detail = (completed.stderr or completed.stdout or "").strip()
                if detail:
                    self.warn(detail)
                raise DeploymentError(
                    "macOS could not apply a sandbox profile; open a fresh Terminal.app "
                    "or iTerm window outside another sandbox and rerun"
                )
        self.say("Checking local installer prerequisites...")
        for executable, description in (
            (Path("/opt/homebrew/bin/brew"), "Homebrew"),
            (Path("/opt/homebrew/bin/python3"), "Homebrew Python"),
        ):
            if not executable.is_file() or not os.access(executable, os.X_OK):
                raise DeploymentError(f"{description} is required at {executable}")

    def acquire_lock_and_owner(self) -> TextIO:
        self.paths.support_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.paths.support_root, 0o700)
        descriptor = os.open(
            self.paths.installer_lock,
            os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW,
            0o600,
        )
        try:
            details = os.fstat(descriptor)
            if not stat.S_ISREG(details.st_mode):
                raise DeploymentError(
                    f"installer lock is not a regular file: {self.paths.installer_lock}"
                )
            os.fchmod(descriptor, 0o600)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise DeploymentError(
                    "another van-compute worker installer is already running"
                ) from None
            if self.paths.owner_file.exists() or self.paths.owner_file.is_symlink():
                self._regular_file(self.paths.owner_file, "installer owner record")
                self.owner = self.paths.owner_file.read_text(encoding="utf-8").strip()
            else:
                self.owner = f"installer-{self.install_id}"
                temporary = (
                    self.paths.support_root / f".installer-owner.{self.install_id}"
                )
                temporary.write_text(self.owner + "\n", encoding="utf-8")
                os.chmod(temporary, 0o600)
                os.replace(temporary, self.paths.owner_file)
            os.chmod(self.paths.owner_file, 0o600)
            if not OWNER_RE.fullmatch(self.owner):
                raise DeploymentError(
                    f"installer owner record is invalid: {self.paths.owner_file}"
                )
            return os.fdopen(descriptor, "r+")
        except BaseException:
            os.close(descriptor)
            raise

    def remote_preflight(self) -> str:
        self.say("Checking SSH access and Pi prerequisites...")
        script = (
            RELEASE_LINK_GUARD
            + r'''
set -eu
owner="$1"
old="$2"
new="$3"
root="$4"

for executable in /usr/bin/python3 /usr/bin/bwrap /usr/bin/sqlite3 /usr/bin/flock /usr/bin/mountpoint; do
  test -x "$executable" || { echo "Missing required executable: $executable" >&2; exit 1; }
done
sudo -n true
test -d /home/pi/dev/obd-things
test ! -L /home/pi/dev/obd-things
for directory in "$root" "$root/scripts" "$root/configs" "$root/releases" "$root/venv" "$old"; do
  if test -e "$directory" || test -L "$directory"; then
    test -d "$directory" && test ! -L "$directory" || {
      echo "A compute deployment directory is unsafe: $directory" >&2
      exit 1
    }
  fi
done
for link in "$root/current" "$root/previous"; do
  if test -e "$link" || test -L "$link"; then
    test -L "$link" || { echo "A compute release link is unsafe: $link" >&2; exit 1; }
    resolved="$(/usr/bin/readlink -e "$link")"
    check_release_target "$root" "$resolved" || { echo "Invalid compute release target" >&2; exit 1; }
    test -d "$resolved" && test ! -L "$resolved" || exit 1
  fi
done
runtime_lock="$root/runtime.lock"
if test -e "$runtime_lock" || test -L "$runtime_lock"; then
  test -f "$runtime_lock" && test ! -L "$runtime_lock" || {
    echo "The compute runtime lock is unsafe: $runtime_lock" >&2
    exit 1
  }
fi
cli_state() {
  cli="$1/van_compute.py"
  if ! test -e "$cli" && ! test -L "$cli"; then echo missing; return; fi
  test -f "$cli" && test ! -L "$cli" && test -x "$cli" || {
    echo "A compute CLI is unsafe or not executable: $cli" >&2; exit 1;
  }
  if /bin/grep -Fxq 'UPGRADE_GATE = True' "$cli"; then echo gate; else echo normal; fi
}
validate_artifacts() {
  directory="$1"
  for artifact in .van-compute-upgrade-owner .van_compute.py.pre-upgrade .van-compute-upgrade.lock; do
    path="$directory/$artifact"
    if test -e "$path" || test -L "$path"; then
      test -f "$path" && test ! -L "$path" || { echo "Unsafe upgrade artifact: $path" >&2; exit 1; }
    fi
  done
  owner_record="$directory/.van-compute-upgrade-owner"
  backup="$directory/.van_compute.py.pre-upgrade"
  if test -f "$owner_record"; then
    test "$(/bin/cat "$owner_record")" = "$owner" || {
      echo "A compute upgrade is owned by another installer: $directory" >&2; exit 1;
    }
  elif test -e "$backup" || test -L "$backup"; then
    echo "A compute rollback CLI has no owner: $directory" >&2; exit 1
  fi
}
validate_artifacts "$old"
validate_artifacts "$new"
old_state="$(cli_state "$old")"
new_state="$(cli_state "$new")"
if test "$old_state" = gate; then
  test -f "$old/.van-compute-upgrade-owner" && test -f "$old/.van_compute.py.pre-upgrade" || exit 1
  test "$new_state" != gate || { echo "Both compute layouts are gated" >&2; exit 1; }
  selected="$old"
elif test "$new_state" = gate; then
  test -f "$new/.van-compute-upgrade-owner" && test -f "$new/.van_compute.py.pre-upgrade" || exit 1
  test "$old_state" = missing || { echo "The new CLI is gated while the old CLI is live" >&2; exit 1; }
  selected="$new"
elif test "$old_state" = normal && test "$new_state" = missing; then selected="$old"
elif test "$old_state" = missing && test "$new_state" = normal; then selected="$new"
elif test "$old_state" = normal && test "$new_state" = normal; then
  echo "Both old and new compute CLIs are live" >&2; exit 1
else
  echo "No supported compute CLI deployment was found" >&2; exit 1
fi
for legacy in /home/pi/scripts/van_compute.py /home/pi/scripts/.van-compute-upgrade-owner /home/pi/scripts/.van_compute.py.pre-upgrade; do
  test ! -e "$legacy" && test ! -L "$legacy" || { echo "Unsupported flat compute artifact: $legacy" >&2; exit 1; }
done
for state in queued running; do
  directory="$5/$state"
  test -d "$directory" && test ! -L "$directory" || exit 1
  test -z "$(/usr/bin/find "$directory" -mindepth 1 -maxdepth 1 -print -quit)" || {
    echo "The Pi compute queue has pending or running work" >&2; exit 1;
  }
done
/usr/bin/python3 -m venv --help >/dev/null
printf '%s\n' "$selected"
'''
        )
        selected = self.remote.run(
            "preflight",
            script,
            [self.owner, OLD_COMPUTE_ROOT, REMOTE_SCRIPTS, REMOTE_ROOT, QUEUE_ROOT],
            capture_output=True,
        ).strip()
        if selected not in {OLD_COMPUTE_ROOT, REMOTE_SCRIPTS}:
            raise DeploymentError(
                f"the Pi returned an invalid compute deployment root: {selected}"
            )
        self.state.upgrade_public_root = selected
        return selected

    def provenance(self, source: SourceRelease, kind: str) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": 1,
            "kind": kind,
            "source_sha256": source.source_fingerprint,
            "source_root": str(self.paths.repo_root),
        }
        if kind == "mac-worker":
            payload.update(
                {
                    "deployment_sha256": source.deployment_fingerprint,
                    "host": self.host,
                    "worker": self.worker,
                    "allow_unsandboxed": source.allow_unsandboxed,
                    "dataset_sha256": source.dataset_fingerprint,
                }
            )
        return payload

    def _write_manifest(self, root: Path) -> None:
        records: dict[str, dict[str, object]] = {}
        for path in sorted(root.rglob("*")):
            if path == root / MANIFEST_FILE:
                continue
            details = path.lstat()
            if stat.S_ISLNK(details.st_mode):
                raise DeploymentError(f"release contains a symlink: {path}")
            if stat.S_ISDIR(details.st_mode):
                continue
            if not stat.S_ISREG(details.st_mode):
                raise DeploymentError(f"release contains a special entry: {path}")
            records[path.relative_to(root).as_posix()] = {
                "sha256": self._hash_file(path),
                "mode": stat.S_IMODE(details.st_mode),
            }
        (root / MANIFEST_FILE).write_text(
            json.dumps(
                {"schema_version": 1, "files": records}, indent=2, sort_keys=True
            )
            + "\n",
            encoding="utf-8",
        )
        os.chmod(root / MANIFEST_FILE, 0o600)

    def _release_identity(self, root: Path) -> tuple[str, str]:
        if (
            root.parent != self.paths.release_parent
            or not re.fullmatch(r"[0-9a-f]{24}", root.name)
            or root.is_symlink()
            or not root.is_dir()
        ):
            raise DeploymentError(f"release is not an owned real directory: {root}")
        marker_values: dict[str, str] = {}
        for marker in (SOURCE_HASH_FILE, DEPLOYMENT_HASH_FILE):
            path = root / marker
            self._regular_file(path, f"release {marker}")
            value = path.read_text(encoding="utf-8").strip()
            if not re.fullmatch(r"[0-9a-f]{64}", value):
                raise DeploymentError(f"release marker is invalid: {path}")
            marker_values[marker] = value
        if root.name != marker_values[DEPLOYMENT_HASH_FILE][:24]:
            raise DeploymentError(
                f"release directory does not match its identity: {root}"
            )
        provenance_path = root / PROVENANCE_FILE
        self._regular_file(provenance_path, "release provenance")
        try:
            provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            raise DeploymentError(
                f"release provenance is invalid: {provenance_path}"
            ) from None
        if (
            not isinstance(provenance, dict)
            or provenance.get("schema_version") != 1
            or provenance.get("kind") != "mac-worker"
            or provenance.get("source_sha256") != marker_values[SOURCE_HASH_FILE]
            or provenance.get("deployment_sha256")
            != marker_values[DEPLOYMENT_HASH_FILE]
        ):
            raise DeploymentError(
                f"release provenance identity mismatch: {provenance_path}"
            )
        manifest_path = root / MANIFEST_FILE
        self._regular_file(manifest_path, "release manifest")
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            records = manifest["files"]
        except (OSError, json.JSONDecodeError, KeyError, TypeError):
            raise DeploymentError(
                f"release manifest is invalid: {manifest_path}"
            ) from None
        if not isinstance(records, dict):
            raise DeploymentError(f"release manifest is invalid: {manifest_path}")
        actual: set[str] = set()
        for path in root.rglob("*"):
            details = path.lstat()
            if stat.S_ISLNK(details.st_mode):
                raise DeploymentError(f"release contains a symlink: {path}")
            if stat.S_ISDIR(details.st_mode):
                continue
            if not stat.S_ISREG(details.st_mode):
                raise DeploymentError(f"release contains a special entry: {path}")
            if path != manifest_path:
                actual.add(path.relative_to(root).as_posix())
        if actual != set(records):
            raise DeploymentError(f"release manifest file set mismatch: {root}")
        for relative, record in records.items():
            path = root / relative
            if path.is_symlink() or not path.is_file() or not isinstance(record, dict):
                raise DeploymentError(f"release entry is unsafe: {path}")
            if self._hash_file(path) != record.get("sha256"):
                raise DeploymentError(f"release entry hash mismatch: {path}")
            if stat.S_IMODE(path.stat().st_mode) != record.get("mode"):
                raise DeploymentError(f"release entry mode mismatch: {path}")
        return (
            marker_values[SOURCE_HASH_FILE],
            marker_values[DEPLOYMENT_HASH_FILE],
        )

    def _verify_release(self, root: Path, source: SourceRelease) -> None:
        source_hash, deployment_hash = self._release_identity(root)
        if (
            source_hash != source.source_fingerprint
            or deployment_hash != source.deployment_fingerprint
        ):
            raise DeploymentError(f"release marker mismatch: {root}")
        self._verify_staged_source(root / "app", source)

    def _copy_source_tree(self, staging: Path, source: SourceRelease) -> None:
        for source_path in source.files:
            relative = source_path.relative_to(self.paths.repo_root)
            destination = staging / "app" / relative
            destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            shutil.copyfile(source_path, destination, follow_symlinks=False)
            executable = (
                relative.parts[:2] == ("macbook", "scripts")
                or relative.parts[:2] == ("van_compute", "entrypoints")
                or relative == Path("van_compute/upgrade_gate.py")
            )
            os.chmod(destination, 0o700 if executable else 0o600)

    def _install_formulae(self) -> None:
        formulae = []
        if not os.access("/opt/homebrew/bin/rg", os.X_OK):
            formulae.append("ripgrep")
        if not os.access("/opt/homebrew/bin/jadx", os.X_OK):
            formulae.append("jadx")
        if formulae:
            self.say("Installing Homebrew worker tools: " + " ".join(formulae))
            self.local.run(["/opt/homebrew/bin/brew", "install", *formulae])
        if not os.access("/usr/bin/sqlite3", os.X_OK):
            raise DeploymentError(
                "the macOS /usr/bin/sqlite3 executable is unavailable"
            )

    def _sandbox_arguments(self, staging: Path, sandbox_test: Path) -> list[str]:
        arguments = [
            "/usr/bin/sandbox-exec",
            "-f",
            str(staging / "sandbox.sb"),
            "-D",
            f"WORKER_ROOT={staging}",
            "-D",
            f"JOB_ROOT={sandbox_test}",
        ]
        for index in range(16):
            arguments.extend(("-D", f"DATASET_{index}=/dev/null"))
        return arguments

    def _validate_mac_release(self, staging: Path, source: SourceRelease) -> None:
        if source.allow_unsandboxed:
            self.warn(
                "WARNING: VAN_COMPUTE_ALLOW_UNSANDBOXED=1 disables OS-level job isolation."
            )
            self.warn(
                "Jobs still get private HOME/TMP/env and limits, but can address host files/network."
            )
        else:
            self.say(
                "Validating macOS job isolation before changing the running worker..."
            )
            self.paths.cache_root.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(self.paths.cache_root, 0o700)
            sandbox_test = Path(
                tempfile.mkdtemp(prefix=".sandbox-test.", dir=self.paths.cache_root)
            )
            os.chmod(sandbox_test, 0o700)
            sentinel = self.paths.cache_root / f".sandbox-sentinel.{self.install_id}"
            sentinel.write_text("must-not-be-readable\n", encoding="utf-8")
            os.chmod(sentinel, 0o600)
            environment = [
                "/usr/bin/env",
                "-i",
                f"HOME={sandbox_test}",
                f"TMPDIR={sandbox_test}/",
                f"XDG_CACHE_HOME={sandbox_test}",
                f"XDG_CONFIG_HOME={sandbox_test}/.config",
                "LANG=en_US.UTF-8",
                "LC_ALL=en_US.UTF-8",
                "PATH=/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin",
                "PYTHONNOUSERSITE=1",
                "PYTHONDONTWRITEBYTECODE=1",
            ]
            prefix = self._sandbox_arguments(staging, sandbox_test) + environment
            checks = [
                ("profile application", ["/usr/bin/true"]),
                (
                    "Python imports and isolation policy",
                    [
                        str(staging / "venv/bin/python"),
                        "-c",
                        "import androguard,isotp,numpy,pytest; from pathlib import Path; "
                        "p=Path(__import__('sys').argv[1])/'allowed.txt'; p.write_text('ok'); "
                        "assert p.read_text()=='ok'",
                        str(sandbox_test),
                    ],
                ),
                (
                    "ripgrep runtime",
                    [
                        "/opt/homebrew/bin/rg",
                        "--fixed-strings",
                        "ok",
                        str(sandbox_test / "allowed.txt"),
                    ],
                ),
                (
                    "SQLite runtime",
                    [
                        "/usr/bin/sqlite3",
                        "-readonly",
                        "-batch",
                        ":memory:",
                        "select 1;",
                    ],
                ),
                ("JADX runtime", ["/opt/homebrew/bin/jadx", "--version"]),
            ]
            try:
                for label, command in checks:
                    completed = self.local.run(
                        prefix + command,
                        capture_output=True,
                        check=False,
                        cwd=sandbox_test,
                    )
                    if completed.returncode != 0:
                        detail = (completed.stderr or completed.stdout or "").strip()
                        raise DeploymentError(
                            (detail + "\n" if detail else "")
                            + f"sandbox validation stage failed: {label} (status {completed.returncode})"
                        )
                self.local.run(
                    prefix
                    + [
                        str(staging / "venv/bin/python"),
                        "-c",
                        SANDBOX_DENIAL_PROBE,
                        str(sentinel),
                        f"/System/Volumes/Data{sentinel}",
                    ],
                    cwd=sandbox_test,
                )
            finally:
                sentinel.unlink(missing_ok=True)
                shutil.rmtree(sandbox_test, ignore_errors=True)
        self.say("Checking Mac process-group resource watchdog...")
        self.local.run(
            [
                str(staging / "venv/bin/python"),
                "-c",
                "import os,subprocess; r=subprocess.run(['/bin/ps','-axo','pgid=,rss='],"
                "check=True,capture_output=True,text=True); g=os.getpgrp(); "
                "assert any(len(x)==2 and int(x[0])==g and int(x[1])>=0 "
                "for x in (line.split() for line in r.stdout.splitlines()))",
            ]
        )

    def prepare_mac_release(self, source: SourceRelease) -> Path:
        release = self.paths.release_parent / source.mac_version
        if release.exists() or release.is_symlink():
            self._verify_release(release, source)
            self._validate_mac_release(release, source)
            self.say(f"Verified existing immutable Mac release: {release}")
            self.release = release
            return release
        self.say("Checking and provisioning local worker dependencies...")
        self._install_formulae()
        self.paths.release_parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.paths.cache_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        for path in (
            self.paths.release_parent,
            self.paths.cache_root,
            self.paths.cache_root / "logs",
            self.paths.cache_root / "jobs",
            self.paths.cache_root / "ssh",
        ):
            path.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(path, 0o700)
        staging = Path(
            tempfile.mkdtemp(prefix=".install.", dir=self.paths.release_parent)
        )
        os.chmod(staging, 0o700)
        try:
            self.say("Building an isolated Python environment...")
            self.local.run(
                [
                    "/opt/homebrew/bin/python3",
                    "-m",
                    "venv",
                    "--copies",
                    str(staging / "venv"),
                ]
            )
            self.local.run(
                [
                    str(staging / "venv/bin/python"),
                    "-m",
                    "pip",
                    "install",
                    "--disable-pip-version-check",
                    "--no-input",
                    "androguard",
                    "can-isotp",
                    "numpy",
                    "pytest",
                ]
            )
            self._copy_source_tree(staging, source)
            self._verify_staged_source(staging / "app", source)
            (staging / "sandbox.sb").write_text(SANDBOX_PROFILE, encoding="utf-8")
            os.chmod(staging / "sandbox.sb", 0o600)
            self._validate_mac_release(staging, source)
            (staging / SOURCE_HASH_FILE).write_text(
                source.source_fingerprint + "\n", encoding="utf-8"
            )
            (staging / DEPLOYMENT_HASH_FILE).write_text(
                source.deployment_fingerprint + "\n", encoding="utf-8"
            )
            (staging / PROVENANCE_FILE).write_text(
                json.dumps(
                    self.provenance(source, "mac-worker"), indent=2, sort_keys=True
                )
                + "\n",
                encoding="utf-8",
            )
            for marker in (SOURCE_HASH_FILE, DEPLOYMENT_HASH_FILE, PROVENANCE_FILE):
                os.chmod(staging / marker, 0o600)
            self._write_manifest(staging)
            try:
                os.rename(staging, release)
                staging = Path()
            except FileExistsError:
                self._verify_release(release, source)
            self._verify_release(release, source)
            self.release = release
            return release
        finally:
            if (
                staging
                and staging.is_dir()
                and staging.parent == self.paths.release_parent
            ):
                shutil.rmtree(staging)

    def validate_dataset(self, path: Path) -> None:
        self._regular_file(path, "dataset configuration")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DeploymentError(f"invalid dataset configuration: {exc}") from None
        if not isinstance(payload, dict) or set(payload) != {"datasets"}:
            raise DeploymentError(
                "dataset configuration must contain only a datasets object"
            )
        datasets = payload["datasets"]
        if not isinstance(datasets, dict) or len(datasets) > 16:
            raise DeploymentError(
                "dataset configuration must contain at most 16 datasets"
            )
        for name, raw_path in datasets.items():
            if not isinstance(name, str) or not re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", name
            ):
                raise DeploymentError(f"invalid dataset alias: {name!r}")
            if not isinstance(raw_path, str):
                raise DeploymentError(f"dataset path for {name} is not a string")
            target = Path(raw_path).expanduser()
            if not target.is_absolute() or not target.exists():
                raise DeploymentError(
                    f"dataset path for {name} is not an existing absolute path"
                )

    def install_dataset(self, source: SourceRelease) -> None:
        if source.dataset_source is not None:
            self._regular_file(source.dataset_source, "dataset configuration")
            payload = source.dataset_source.read_bytes()
            if hashlib.sha256(payload).hexdigest() != source.dataset_fingerprint:
                raise DeploymentError("dataset configuration changed after planning")
            descriptor, name = tempfile.mkstemp(
                prefix=".datasets.json.", dir=self.paths.support_root
            )
            temporary = Path(name)
            try:
                with os.fdopen(descriptor, "wb") as output:
                    output.write(payload)
                    output.flush()
                    os.fsync(output.fileno())
                    os.fchmod(output.fileno(), 0o600)
                self.validate_dataset(temporary)
                os.replace(temporary, self.paths.dataset_target)
            finally:
                temporary.unlink(missing_ok=True)
        if self.paths.dataset_target.exists() or self.paths.dataset_target.is_symlink():
            self.validate_dataset(self.paths.dataset_target)
            if self._hash_file(self.paths.dataset_target) != source.dataset_fingerprint:
                raise DeploymentError(
                    "installed dataset configuration changed after planning"
                )
        elif source.dataset_fingerprint != "none":
            raise DeploymentError(
                "installed dataset configuration disappeared after planning"
            )

    def stage_remote_release(self, source: SourceRelease, release: Path) -> None:
        self.say("Staging the isolated Pi compute deployment...")
        self.remote.run(
            "create-stage",
            'set -eu\nstage="$1"\ncase "$stage" in /home/pi/.cache/van-compute-install.*) ;; *) exit 2;; esac\n'
            'test ! -e "$stage" && test ! -L "$stage" || exit 1\ninstall -d -m 700 "$stage" "$stage/release"\n',
            [self.remote_stage],
        )
        self.state.remote_stage_created = True
        pi_staging = Path(
            tempfile.mkdtemp(prefix=".pi-release.", dir=self.paths.support_root)
        )
        os.chmod(pi_staging, 0o700)
        try:
            package_target = pi_staging / "van_compute"
            shutil.copytree(
                release / "app" / "van_compute", package_target, symlinks=False
            )
            (pi_staging / SOURCE_HASH_FILE).write_text(
                source.source_fingerprint + "\n", encoding="utf-8"
            )
            (pi_staging / PROVENANCE_FILE).write_text(
                json.dumps(
                    self.provenance(source, "pi-broker"), indent=2, sort_keys=True
                )
                + "\n",
                encoding="utf-8",
            )
            self._write_manifest(pi_staging)
            self.remote.upload(
                "upload-release",
                [
                    package_target,
                    pi_staging / SOURCE_HASH_FILE,
                    pi_staging / PROVENANCE_FILE,
                    pi_staging / MANIFEST_FILE,
                ],
                f"{self.remote_stage}/release/",
            )
        finally:
            shutil.rmtree(pi_staging)

    def validate_remote_stage(self, source: SourceRelease) -> None:
        self.say(
            "Validating the staged Pi broker before stopping the current worker..."
        )
        script = r'''
set -eu
stage="$1"
expected="$2"
release="$stage/release"
test -d "$release" && test ! -L "$release" || exit 1
test -d "$release/van_compute" && test ! -L "$release/van_compute" || exit 1
test -z "$(/usr/bin/find "$release" -type l -print -quit)" || {
  echo "The staged Pi release contains a symlink" >&2; exit 1;
}
test "$(/bin/cat "$release/source.sha256")" = "$expected"
/usr/bin/python3 - "$release" <<'PY'
import hashlib, json, pathlib, stat, sys
root = pathlib.Path(sys.argv[1])
manifest = root / "manifest.json"
payload = json.loads(manifest.read_text())
records = payload["files"]
actual = set()
for path in root.rglob("*"):
    mode = path.lstat().st_mode
    if stat.S_ISLNK(mode):
        raise SystemExit(f"staged release contains a symlink: {path}")
    if stat.S_ISDIR(mode):
        continue
    if not stat.S_ISREG(mode):
        raise SystemExit(f"staged release contains a special entry: {path}")
    if path != manifest:
        actual.add(path.relative_to(root).as_posix())
if actual != set(records):
    raise SystemExit("staged release manifest file set mismatch")
for relative, record in records.items():
    path = root / relative
    if path.is_symlink() or not path.is_file():
        raise SystemExit(f"unsafe staged release entry: {relative}")
    if hashlib.sha256(path.read_bytes()).hexdigest() != record["sha256"]:
        raise SystemExit(f"staged release hash mismatch: {relative}")
    if stat.S_IMODE(path.stat().st_mode) != record["mode"]:
        raise SystemExit(f"staged release mode mismatch: {relative}")
PY
/usr/bin/python3 -m compileall -q -f "$release/van_compute"
/bin/rm -rf "$release/van_compute/__pycache__" "$release/van_compute/entrypoints/__pycache__"
/bin/cp "$release/van_compute/configs/van-compute-obd.example.json" "$release/.van-compute.json"
PYTHONPATH="$release" PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -P -m van_compute.queue tasks --source-root "$release" >/dev/null
/bin/rm -f "$release/.van-compute.json"
PYTHONPATH="$release" PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -P -m van_compute.frontend --help >/dev/null
PYTHONPATH="$release" PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -P -m van_compute.broker --help >/dev/null
sudo -n /usr/bin/systemd-analyze verify "$release/van_compute/configs/van-compute-broker.service" >/dev/null
'''
        self.remote.run(
            "validate-stage",
            script,
            [self.remote_stage, source.source_fingerprint],
        )

    def provision_remote_runtime(self) -> None:
        self.say("Checking and provisioning the Pi fallback runtime...")
        script = r'''
set -eu
root="$1"
venv="$2"
install -d -m 700 "$root"
if test -e "$venv" || test -L "$venv"; then
  test -d "$venv" && test ! -L "$venv" || { echo "The Pi fallback venv is not a real directory" >&2; exit 1; }
fi
runtime_lock="$root/runtime.lock"
if test -e "$runtime_lock" || test -L "$runtime_lock"; then
  test -f "$runtime_lock" && test ! -L "$runtime_lock" || { echo "The Pi fallback runtime lock is unsafe" >&2; exit 1; }
fi
umask 077
exec 9>"$runtime_lock"
chmod 600 "$runtime_lock"
/usr/bin/flock -n 9 || { echo "Another installer is provisioning the Pi fallback runtime" >&2; exit 1; }
runtime="$venv/bin/python3"
if test -x "$runtime" && "$runtime" -c 'import isotp,numpy,pytest' >/dev/null 2>&1; then exit 0; fi
if /usr/bin/systemctl is-active --quiet van-compute-broker.service; then
  unit="$(/usr/bin/systemctl cat van-compute-broker.service)"
  if /usr/bin/printf '%s\n' "$unit" | /bin/grep -Fq "$venv/bin/python3"; then
    echo "The active Pi broker has an invalid fallback runtime" >&2; exit 1
  fi
fi
/usr/bin/python3 -m venv --system-site-packages --clear "$venv"
"$runtime" -m pip install --disable-pip-version-check --no-input can-isotp numpy pytest
"$runtime" -c 'import isotp,numpy,pytest'
'''
        self.remote.run("provision-runtime", script, [REMOTE_ROOT, REMOTE_VENV])

    def maintenance_relation(self) -> str:
        script = r'''
set -eu
stage="$1"
queue="$2"
owner="$3"
payload="$(PYTHONPATH="$stage/release" PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -P -m van_compute.queue --root "$queue" maintenance status)"
printf '%s' "$payload" | /usr/bin/python3 -c '
import json,sys
payload=json.load(sys.stdin); owner=sys.argv[1]
print("inactive" if not payload.get("active") else "ours" if payload.get("owner")==owner else "other")
' "$owner"
'''
        relation = self.remote.run(
            "maintenance-status",
            script,
            [self.remote_stage, QUEUE_ROOT, self.owner],
            capture_output=True,
        ).strip()
        if relation not in {"inactive", "ours", "other"}:
            raise DeploymentError(f"invalid maintenance status from Pi: {relation}")
        return relation

    def active_queue_jobs(self) -> int:
        script = r'''
set -eu
root="$1"
/usr/bin/python3 - "$root" <<'PY'
from pathlib import Path
import sys
roots=[Path(sys.argv[1])/"queued", Path(sys.argv[1])/"running"]
if not all(path.is_dir() and not path.is_symlink() for path in roots):
    raise SystemExit("queue state directory is missing or unsafe")
print(sum(1 for path in roots for _entry in path.iterdir()))
PY
'''
        raw = self.remote.run(
            "active-queue-jobs", script, [QUEUE_ROOT], capture_output=True
        ).strip()
        if not raw.isdigit():
            raise DeploymentError(f"invalid active queue count: {raw!r}")
        return int(raw)

    def active_submitters(self) -> int:
        script = 'set -eu\n/usr/bin/python3 "$1/release/van_compute/upgrade_gate.py" --active-submitter-count\n'
        raw = self.remote.run(
            "active-submitters", script, [self.remote_stage], capture_output=True
        ).strip()
        if not raw.isdigit():
            raise DeploymentError(f"invalid active submitter count: {raw!r}")
        return int(raw)

    def drain_worker(self) -> None:
        loaded = self._launch_state()
        if loaded is None:
            return
        if (
            self.paths.target_plist.is_symlink()
            or not self.paths.target_plist.is_file()
        ):
            raise DeploymentError(
                "the loaded worker has no supported regular LaunchAgent plist"
            )
        try:
            arguments = plistlib.loads(self.paths.target_plist.read_bytes())[
                "ProgramArguments"
            ]
        except (KeyError, plistlib.InvalidFileException):
            raise DeploymentError("the loaded worker LaunchAgent is invalid") from None
        if (
            "--serve" not in arguments
            or re.search(r"^\s*--serve\s*$", loaded, re.MULTILINE) is None
        ):
            raise DeploymentError(
                "the loaded worker is not the supported persistent --serve LaunchAgent"
            )
        original_pid = self._agent_pid(loaded)
        domain = self._launch_domain()
        self.state.previous_agent_disabled = True
        self.local.run(["/bin/launchctl", "disable", domain])
        self.local.run(["/bin/launchctl", "kill", "SIGUSR1", domain], check=False)
        departed = False
        active_jobs = 0
        for _attempt in range(30):
            active_jobs = self.active_queue_jobs()
            if active_jobs:
                break
            current_pid = self._agent_pid(self._launch_state())
            if not original_pid or not current_pid or current_pid != original_pid:
                departed = True
                break
            self.sleep(0.5)
        if active_jobs:
            raise DeploymentError(
                "queue work appeared while draining; the current release was retained"
            )
        if not departed:
            self.say(
                "The idle worker is blocked in an RPC; unloading it after a 15-second drain window."
            )
        self.state.restore_previous_agent = True
        bootout = self.local.run(
            ["/bin/launchctl", "bootout", domain], capture_output=True, check=False
        )
        if bootout.returncode != 0 and self._launch_state() is not None:
            raise DeploymentError("the previous worker could not be unloaded safely")
        if self.active_queue_jobs():
            raise DeploymentError(
                "queue work appeared while unloading; the previous worker will be restored"
            )
        self.local.run(["/bin/launchctl", "enable", domain])
        self.state.previous_agent_disabled = False

    def acquire_submission_gate(self) -> None:
        script = r'''
set -eu
stage="$1"
owner="$2"
script_root="$3"
resuming="$4"
set -- /usr/bin/python3 "$stage/release/van_compute/upgrade_gate.py" --acquire --owner "$owner" --gate "$stage/release/van_compute/upgrade_gate.py" --script-root "$script_root"
if test "$resuming" = 1; then set -- "$@" --allow-existing-backup; fi
exec "$@"
'''
        self.remote.run(
            "acquire-submission-gate",
            script,
            [
                self.remote_stage,
                self.owner,
                self.state.upgrade_public_root,
                "1" if self.state.cutover_started else "0",
            ],
        )
        self.state.submission_gate_active = True

    def wait_for_submitter_drain(self) -> None:
        deadline = self.monotonic() + self.options.submitter_timeout
        while True:
            submitters = self.active_submitters()
            jobs = self.active_queue_jobs()
            if jobs:
                if self.state.cutover_started:
                    raise DeploymentError(
                        "a submission reached the queue; the interrupted upgrade remains fenced"
                    )
                raise DeploymentError(
                    "a submission reached the queue while fencing; the previous CLI and worker will be restored"
                )
            if submitters == 0:
                return
            if self.monotonic() >= deadline:
                if self.state.cutover_started:
                    raise DeploymentError(
                        "a Pi submission did not drain before the timeout; the interrupted upgrade remains fenced"
                    )
                raise DeploymentError(
                    "a Pi submission did not drain before the timeout; the previous CLI and worker will be restored"
                )
            self.sleep(0.5)

    def enter_maintenance(self) -> None:
        script = r'''
set -eu
PYTHONPATH="$1/release" PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -P -m van_compute.queue --root "$2" maintenance enter --owner "$3" >/dev/null
'''
        self.remote.run(
            "enter-maintenance", script, [self.remote_stage, QUEUE_ROOT, self.owner]
        )
        self.state.maintenance_active = True
        if self.active_submitters() or self.active_queue_jobs():
            raise DeploymentError(
                "submission activity appeared across the maintenance boundary; upgrade is stopping safely"
            )

    def cutover_remote(self, source: SourceRelease) -> None:
        # From this point onward the previous protocol is not restored on error.
        self.state.cutover_started = True
        script = (
            RELEASE_LINK_GUARD
            + r'''
set -eu
stage="$1"
root="$2"
version="$3"
expected="$4"
install_id="$5"
old_root="$6"
release="$root/releases/$version"
staged="$stage/release"

if /usr/bin/systemctl is-active --quiet van-compute-broker.service; then
  sudo -n systemctl stop van-compute-broker.service
fi
test -x "$root/venv/bin/python3"
"$root/venv/bin/python3" -c 'import isotp,numpy,pytest'
install -d -m 700 "$root" "$root/releases" "$root/scripts" "$root/configs" "$root/venv"
if test -e "$release" || test -L "$release"; then
  test -d "$release" && test ! -L "$release" || { echo "Existing release is unsafe: $release" >&2; exit 1; }
  test -f "$release/source.sha256" && test ! -L "$release/source.sha256" || exit 1
  test "$(/bin/cat "$release/source.sha256")" = "$expected" || { echo "Existing release provenance mismatch" >&2; exit 1; }
  test -z "$(/usr/bin/find "$release" -type l -print -quit)" || { echo "Existing release contains a symlink" >&2; exit 1; }
  /usr/bin/python3 - "$release" "$staged" <<'PY'
import hashlib,json,pathlib,stat,sys
root=pathlib.Path(sys.argv[1]); manifest=root/'manifest.json'; records=json.loads(manifest.read_text())['files']
actual=set()
for p in root.rglob('*'):
 mode=p.lstat().st_mode
 if stat.S_ISLNK(mode): raise SystemExit(f'existing release contains a symlink: {p}')
 if stat.S_ISDIR(mode): continue
 if not stat.S_ISREG(mode): raise SystemExit(f'existing release contains a special entry: {p}')
 if p != manifest: actual.add(p.relative_to(root).as_posix())
if actual != set(records): raise SystemExit('existing release manifest file set mismatch')
for relative,record in records.items():
 p=root/relative
 if p.is_symlink() or not p.is_file() or hashlib.sha256(p.read_bytes()).hexdigest()!=record['sha256'] or stat.S_IMODE(p.stat().st_mode)!=record['mode']:
  raise SystemExit(f'existing release verification failed: {relative}')
wanted=json.loads((pathlib.Path(sys.argv[2])/'manifest.json').read_text())['files']
def source_records(items):
 return {name: record for name,record in items.items() if name == 'source.sha256' or name.startswith('van_compute/')}
if source_records(records) != source_records(wanted):
 raise SystemExit('existing release does not match the planned source')
PY
  /bin/rm -rf -- "$staged"
else
  mv "$staged" "$release"
fi
install -m 600 "$release/van_compute/configs/van-compute-obd.example.json" "$root/configs/van-compute-obd.example.json"
install -m 600 "$release/van_compute/configs/van-compute-broker.service" "$root/configs/van-compute-broker.service"
install -m 600 "$release/source.sha256" "$root/deployment.sha256"
install -m 700 "$release/van_compute/entrypoints/pi_compute.py" "$root/scripts/pi_compute.py"
install -m 700 "$release/van_compute/upgrade_gate.py" "$root/scripts/upgrade_gate.py"
sudo -n install -m 644 "$release/van_compute/configs/van-compute-broker.service" /etc/systemd/system/van-compute-broker.service

current=""
if test -e "$root/current" || test -L "$root/current"; then
  test -L "$root/current" || { echo "Current release path is not a symlink" >&2; exit 1; }
  current="$(/usr/bin/readlink -e "$root/current")"
  check_release_target "$root" "$current" || { echo "Invalid compute release target" >&2; exit 1; }
fi
if test "$current" != "$release"; then
  if test -n "$current"; then
    previous_name="${current##*/}"
    ln -s "releases/$previous_name" "$root/.previous.$install_id"
    /bin/mv -Tf -- "$root/.previous.$install_id" "$root/previous"
  fi
  ln -s "releases/$version" "$root/.current.$install_id"
  /bin/mv -Tf -- "$root/.current.$install_id" "$root/current"
fi
# Publish the ordinary queue wrapper only after current names the complete release.
install -m 700 "$release/van_compute/entrypoints/van_compute.py" "$root/scripts/.van_compute.py.install.$install_id"
mv -f "$root/scripts/.van_compute.py.install.$install_id" "$root/scripts/van_compute.py"
/bin/rm -rf -- "$stage"
"$root/scripts/van_compute.py" tasks >/dev/null
"$root/scripts/pi_compute.py" tasks >/dev/null
sudo -n systemctl daemon-reload
sudo -n systemctl enable van-compute-broker.service
sudo -n systemctl restart van-compute-broker.service
sudo -n systemctl is-active --quiet van-compute-broker.service
sudo -n test -f /etc/systemd/system/van-compute-broker.service
sudo -n test ! -L /etc/systemd/system/van-compute-broker.service
sudo -n /bin/grep -Fq "$root/current" /etc/systemd/system/van-compute-broker.service
if sudo -n /bin/grep -Fq "$old_root" /etc/systemd/system/van-compute-broker.service; then
  echo "The installed broker unit still references the retired compute root" >&2; exit 1
fi
'''
        )
        self.remote.run(
            "cutover",
            script,
            [
                self.remote_stage,
                REMOTE_ROOT,
                source.pi_version,
                source.source_fingerprint,
                self.install_id,
                OLD_COMPUTE_ROOT,
            ],
        )
        self.state.remote_stage_created = False

    def build_launchagent(self, release: Path, source: SourceRelease) -> bytes:
        try:
            payload = plistlib.loads(self.paths.source_plist.read_bytes())
        except (OSError, plistlib.InvalidFileException) as exc:
            raise DeploymentError(f"invalid LaunchAgent template: {exc}") from None
        python = release / "venv" / "bin" / "python"
        arguments = [
            str(python),
            "-P",
            "-m",
            "van_compute.worker",
            "--serve",
            "--host",
            self.host,
            "--worker",
            self.worker,
            "--python",
            str(python),
            "--work-root",
            str(self.paths.cache_root / "jobs"),
            "--control-path",
            str(self.paths.cache_root / "ssh" / "control.sock"),
        ]
        if source.allow_unsandboxed:
            arguments.append("--allow-unsandboxed-dynamic")
        else:
            arguments.extend(("--sandbox-profile", str(release / "sandbox.sb")))
        if (
            self.paths.dataset_target.is_file()
            and not self.paths.dataset_target.is_symlink()
        ):
            arguments.extend(("--dataset-config", str(self.paths.dataset_target)))
        payload["ProgramArguments"] = arguments
        payload["StandardOutPath"] = str(
            self.paths.cache_root / "logs" / "worker.stdout.log"
        )
        payload["StandardErrorPath"] = str(
            self.paths.cache_root / "logs" / "worker.stderr.log"
        )
        variables = payload.get("EnvironmentVariables", {})
        if not isinstance(variables, dict):
            raise DeploymentError(
                "LaunchAgent EnvironmentVariables is not a dictionary"
            )
        variables.update(
            {
                "PYTHONPATH": str(release / "app"),
                "PYTHONDONTWRITEBYTECODE": "1",
                "VAN_COMPUTE_SOURCE_SHA256": source.source_fingerprint,
            }
        )
        payload["EnvironmentVariables"] = variables
        return plistlib.dumps(payload, fmt=plistlib.FMT_XML, sort_keys=True)

    def install_launchagent(self, release: Path, source: SourceRelease) -> None:
        self.say("Installing the persistent 10-slot LaunchAgent...")
        self.paths.target_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        data = self.build_launchagent(release, source)
        descriptor, stage_name = tempfile.mkstemp(
            prefix=f".{LABEL}.", suffix=".plist", dir=self.paths.target_dir
        )
        stage = Path(stage_name)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
                os.fchmod(handle.fileno(), 0o600)
            os.replace(stage, self.paths.target_plist)
        finally:
            stage.unlink(missing_ok=True)
        os.chmod(self.paths.target_plist, 0o600)
        domain = self._launch_domain()
        self.local.run(["/bin/launchctl", "bootout", domain], check=False)
        self.local.run(
            [
                "/bin/launchctl",
                "bootstrap",
                domain.rsplit("/", 1)[0],
                str(self.paths.target_plist),
            ]
        )
        self.local.run(["/bin/launchctl", "kickstart", "-k", domain])
        self.state.restore_previous_agent = False

    def coordinator_seen(self) -> str:
        script = r'''
set -eu
"$1/van_compute.py" available | /usr/bin/python3 -c '
import json,sys
payload=json.load(sys.stdin); base=sys.argv[1]
print(next((str(w.get("seen_at", "")) for w in payload.get("workers", []) if w.get("worker")==base), ""))
' "$2"
'''
        return self.remote.run(
            "coordinator-seen",
            script,
            [REMOTE_SCRIPTS, self.worker],
            capture_output=True,
        ).strip()

    def heartbeat(self) -> dict[str, object]:
        script = 'set -eu\nexec "$1/van_compute.py" available\n'
        raw = self.remote.run(
            "heartbeat", script, [REMOTE_SCRIPTS], capture_output=True
        )
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            raise DeploymentError(
                "worker heartbeat response was invalid JSON"
            ) from None
        if not isinstance(payload, dict):
            raise DeploymentError("worker heartbeat response was not an object")
        return payload

    def wait_for_heartbeat(self, previous_seen: str) -> dict[str, object]:
        self.say("Installed. Waiting for the 10-slot scheduler heartbeat...")
        deadline = self.monotonic() + self.options.heartbeat_timeout
        last: dict[str, object] = {}
        while True:
            try:
                last = self.heartbeat()
            except DeploymentError:
                last = {}
            workers = last.get("workers", [])
            coordinator = (
                next(
                    (
                        item
                        for item in workers
                        if isinstance(item, dict)
                        and item.get("worker") == self.worker
                        and item.get("available")
                    ),
                    None,
                )
                if isinstance(workers, list)
                else None
            )
            if (
                isinstance(coordinator, dict)
                and coordinator.get("seen_at") != previous_seen
                and coordinator.get("slots_total") == 10
                and isinstance(coordinator.get("slots_busy"), int)
                and 0 <= int(coordinator["slots_busy"]) <= 10
            ):
                return last
            if self.monotonic() >= deadline:
                raise DeploymentError(
                    f"worker did not publish a fresh 10-slot coordinator heartbeat within "
                    f"{self.options.heartbeat_timeout} seconds; inspect "
                    f"{self.paths.cache_root}/logs/worker.stderr.log"
                )
            self.sleep(1)

    def finalize_upgrade(self) -> None:
        retire = self.state.upgrade_public_root == OLD_COMPUTE_ROOT
        script = r'''
set -eu
release="$1"
owner="$2"
script_root="$3"
queue_cli="$4"
retire="$5"
set -- /usr/bin/python3 "$release/van_compute/upgrade_gate.py" --finalize --owner "$owner" --script-root "$script_root"
if test "$retire" = 1; then set -- "$@" --queue-cli "$queue_cli" --retire-target; fi
exec "$@"
'''
        # The staged release has moved into the immutable release before finalize.
        gate_script_root = (
            f"{REMOTE_RELEASES}/{self.source.pi_version}" if self.source else ""
        )
        self.remote.run(
            "finalize",
            script,
            [
                gate_script_root,
                self.owner,
                self.state.upgrade_public_root,
                f"{REMOTE_SCRIPTS}/van_compute.py",
                "1" if retire else "0",
            ],
        )
        self.state.maintenance_active = False
        self.state.submission_gate_active = False

    def retire_legacy_layout(self) -> None:
        script = r'''
set -eu
require_unmounted() {
  if /usr/bin/mountpoint -q "$1"; then
    echo "Refusing to remove a mounted compute path: $1" >&2
    return 1
  else
    mount_result=$?
    test "$mount_result" -eq 32 || {
      echo "Cannot establish compute mount state: $1" >&2
      return 1
    }
  fi
}
root="$1"
old="$2"
sudo -n systemctl is-active --quiet van-compute-broker.service
sudo -n systemctl cat van-compute-broker.service | /bin/grep -Fq "$root/current"
broker_pid="$(/usr/bin/systemctl show --property MainPID --value van-compute-broker.service)"
case "$broker_pid" in ''|0|*[!0-9]*) echo "The active broker PID could not be verified" >&2; exit 1;; esac
/usr/bin/tr '\0' ' ' < "/proc/$broker_pid/cmdline" | /bin/grep -Fq 'van_compute.broker' || {
  echo "The active broker process is not using the package deployment" >&2; exit 1;
}
if test -e "$old" || test -L "$old"; then
  test -d "$old" && test ! -L "$old" || { echo "The old compute deployment is unsafe" >&2; exit 1; }
  require_unmounted "$old" || exit 1
  unexpected="$(/usr/bin/find "$old" -mindepth 1 -maxdepth 1 ! -name __pycache__ ! -name python-automation ! -name pi_compute.py ! -name van_compute.py ! -name van_compute_broker.py ! -name van_compute_metrics.py ! -name van_compute_protocol.py ! -name van_compute_upgrade_gate.py ! -name van-compute-broker.service ! -name van-compute-obd.example.json ! -name .van-compute-upgrade.lock ! -name .van-compute-upgrade-owner ! -name .van_compute.py.pre-upgrade ! -name '.van_compute.py.install.*' -print -quit)"
  test -z "$unexpected" || { echo "Refusing to remove unexpected old compute entry: $unexpected" >&2; exit 1; }
  /bin/rm -rf --one-file-system -- "$old"
fi
for path in /home/pi/configs/van-compute-obd.example.json /home/pi/secrets/van-compute-datasets.json; do
  if test -e "$path" || test -L "$path"; then
    test -f "$path" && test ! -L "$path" || { echo "Retired compute file is unsafe: $path" >&2; exit 1; }
    require_unmounted "$path" || exit 1
    /bin/rm -f -- "$path"
  fi
done
old_runtime=/home/pi/.local/share/van-compute
if test -e "$old_runtime" || test -L "$old_runtime"; then
  test -d "$old_runtime" && test ! -L "$old_runtime" || exit 1
  require_unmounted "$old_runtime" || exit 1
  unexpected="$(/usr/bin/find "$old_runtime" -mindepth 1 -maxdepth 1 ! -name venv ! -name runtime.lock -print -quit)"
  test -z "$unexpected" || { echo "Refusing unexpected old runtime entry: $unexpected" >&2; exit 1; }
  /bin/rm -rf --one-file-system -- "$old_runtime"
fi
'''
        self.remote.run("retire-legacy", script, [REMOTE_ROOT, OLD_COMPUTE_ROOT])

    def refresh_dashboard(self) -> None:
        script = r'''
set -eu
if /usr/bin/systemctl is-active --quiet van-dashboard.service && /usr/bin/systemctl cat van-dashboard.service | /bin/grep -Fq "$1/current"; then
  sudo -n systemctl restart van-dashboard.service
  sudo -n systemctl is-active --quiet van-dashboard.service
fi
'''
        try:
            self.remote.run("refresh-dashboard", script, [REMOTE_ROOT])
        except DeploymentError:
            self.warn(
                "WARNING: compute is healthy, but van-dashboard could not be refreshed."
            )
            self.warn("Inspect van-dashboard.service after this installer exits.")

    def restore_submission_cli(self) -> None:
        script = r'''
set -eu
/usr/bin/python3 "$1/release/van_compute/upgrade_gate.py" --restore --owner "$2" --script-root "$3"
'''
        self.remote.run(
            "restore-submission-gate",
            script,
            [self.remote_stage, self.owner, self.state.upgrade_public_root],
        )

    def exit_maintenance(self) -> None:
        script = r'''
set -eu
PYTHONPATH="$1/release" PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -P -m van_compute.queue --root "$2" maintenance exit --owner "$3" >/dev/null
'''
        self.remote.run(
            "exit-maintenance", script, [self.remote_stage, QUEUE_ROOT, self.owner]
        )

    def remove_remote_stage(self) -> None:
        script = r'''
set -eu
stage="$1"
case "$stage" in /home/pi/.cache/van-compute-install.*) /bin/rm -rf -- "$stage";; *) exit 2;; esac
'''
        self.remote.run("remove-stage", script, [self.remote_stage])

    def cleanup(self) -> None:
        # Before protocol replacement, undo the fence in the only safe order:
        # maintenance first, then the public CLI, then the prior worker.
        if self.state.maintenance_active and not self.state.cutover_started:
            try:
                self.exit_maintenance()
                self.state.maintenance_active = False
            except DeploymentError:
                self.state.rollback_safe = False
                self.warn("The Pi maintenance marker could not be released safely.")
                self.warn(
                    "The submission gate and previous Mac worker remain disabled; rerun this installer."
                )
        if (
            self.state.submission_gate_active
            and not self.state.cutover_started
            and self.state.rollback_safe
        ):
            try:
                self.restore_submission_cli()
                self.state.submission_gate_active = False
            except DeploymentError:
                self.state.rollback_safe = False
                self.warn(
                    "The temporary Pi submission gate could not be rolled back safely."
                )
                self.warn(
                    "The previous Mac worker remains disabled; rerun this installer."
                )
        if self.state.remote_stage_created:
            try:
                self.remove_remote_stage()
            except DeploymentError:
                pass
            self.state.remote_stage_created = False
        domain = self._launch_domain()
        if self.state.previous_agent_disabled and self.state.rollback_safe:
            self.local.run(["/bin/launchctl", "enable", domain], check=False)
            self.state.previous_agent_disabled = False
        if (
            self.state.restore_previous_agent
            and not self.state.cutover_started
            and self.state.rollback_safe
        ):
            self.local.run(
                [
                    "/bin/launchctl",
                    "bootstrap",
                    domain.rsplit("/", 1)[0],
                    str(self.paths.target_plist),
                ],
                check=False,
            )
            self.state.restore_previous_agent = False
        elif self.state.restore_previous_agent and self.state.cutover_started:
            self.warn(
                "The previous worker remains unloaded because the Pi protocol upgrade began."
            )
            self.warn(
                "Rerun this installer to finish installing the compatible persistent worker."
            )
        if self.state.maintenance_active and self.state.cutover_started:
            self.warn(
                "The compute queue remains in maintenance mode after an incomplete protocol upgrade."
            )
            self.warn(
                "Rerun this installer to validate the deployment and release queued work."
            )
        elif self.state.maintenance_active and not self.state.rollback_safe:
            self.warn(
                "The compute queue remains in maintenance mode because rollback was incomplete."
            )

    def prune_local_releases(self, current: Path) -> None:
        # Reusing the active release is not a new cutover. Its prior rollback
        # release cannot be inferred from mtimes (failed stages may be newer).
        if self.release_retention_ambiguous or self.prior_release == current:
            return
        protected = {current}
        if self.prior_release is not None:
            protected.add(self.prior_release)
        for candidate in self.paths.release_parent.iterdir():
            if candidate in protected:
                continue
            try:
                self._release_identity(candidate)
                if os.path.ismount(candidate) or any(
                    os.path.ismount(path) for path in candidate.rglob("*")
                ):
                    continue
            except (DeploymentError, OSError):
                # Unknown, malformed, foreign, or mounted entries are retained.
                continue
            shutil.rmtree(candidate)

    def execute(self) -> int:
        source = self.build_source_release()
        self.source = source
        if self.options.dry_run:
            print(
                json.dumps(self.plan(source), indent=2, sort_keys=True),
                file=self.stdout,
            )
            return 0
        if self.options.if_needed and self.deployment_current(source):
            self.say("van_compute deployment is current; skipping installer.")
            return 0
        if self.options.if_needed:
            self.say(
                "van_compute deployment changed or is unhealthy; running installer."
            )
        self.preflight_local(source)
        lock = self.acquire_lock_and_owner()
        try:
            self.capture_prior_release()
            self.remote_preflight()
            release = self.prepare_mac_release(source)
            self.install_dataset(source)
            self.stage_remote_release(source, release)
            self.validate_remote_stage(source)
            self.provision_remote_runtime()
            relation = self.maintenance_relation()
            if relation == "other":
                raise DeploymentError(
                    "the compute queue is in maintenance under a different installer; no changes were made"
                )
            if relation == "ours":
                self.state.maintenance_active = True
                self.state.cutover_started = True
                self.say("Resuming this Mac's interrupted protocol upgrade.")
            if self.active_queue_jobs():
                raise DeploymentError(
                    "the Pi compute queue has pending or running work; let it finish and rerun"
                )
            self.drain_worker()
            self.acquire_submission_gate()
            self.wait_for_submitter_drain()
            self.enter_maintenance()
            self.cutover_remote(source)
            previous_seen = self.coordinator_seen()
            self.install_launchagent(release, source)
            heartbeat = self.wait_for_heartbeat(previous_seen)
            self.finalize_upgrade()
            self.retire_legacy_layout()
            self.refresh_dashboard()
            self.prune_local_releases(release)
            print(json.dumps(heartbeat, indent=2, sort_keys=True), file=self.stdout)
            self.say(
                "Worker isolation: "
                + ("disabled" if source.allow_unsandboxed else "sandbox-exec validated")
            )
            self.say(
                "A dedicated worker account, container, or VM would provide stronger isolation "
                "but requires separate admin setup."
            )
            self.say(f"Installed Pi release: {REMOTE_RELEASES}/{source.pi_version}")
            self.say(f"Installed Mac release: {release}")
            self.say(
                "Dashboard: perform its separate package update after this first compute cutover; see pi/docs/compute/VAN_COMPUTE.md."
            )
            self.say(
                "Repository-wide updates remain available through: ./pi/sync_scripts.sh"
            )
            return 0
        finally:
            try:
                self.cleanup()
            finally:
                lock.close()


def parse_arguments(argv: Sequence[str] | None = None) -> Options:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--if-needed",
        action="store_true",
        help="skip only when both installed sides are current and healthy",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the local plan without probing dependencies, launchd, or the Pi",
    )
    arguments = parser.parse_args(argv)
    return Options(
        if_needed=arguments.if_needed,
        dry_run=arguments.dry_run,
    )


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return Installer(parse_arguments(argv)).execute()
    except DeploymentError as exc:
        print(f"van-compute installer: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
