from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
import plistlib
import re
import shutil
import stat
import tempfile
from typing import Sequence, TextIO
import uuid

from .constants import (
    DEPLOYMENT_HASH_FILE, LABEL, MANIFEST_FILE, OWNER_RE, PROVENANCE_FILE,
    SANDBOX_DENIAL_PROBE, SANDBOX_PROFILE, SOURCE_HASH_FILE,
)
from .models import DeploymentError, OwnedBuild, SourceRelease


class MacProvisionMixin:

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
            self._release_identity(release, verify_runtime=False)
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

    def _sandbox_environment(self, sandbox_test: Path) -> list[str]:
        return [
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

    def _sandbox_checks(
        self, staging: Path, sandbox_test: Path
    ) -> list[tuple[str, list[str]]]:
        return [
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

    def _run_sandbox_checks(
        self,
        staging: Path,
        sandbox_test: Path,
        sentinel: Path,
        prefix: list[str],
        checks: list[tuple[str, list[str]]],
    ) -> None:
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

    def _validate_sandbox_release(self, staging: Path) -> None:
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
        environment = self._sandbox_environment(sandbox_test)
        prefix = self._sandbox_arguments(staging, sandbox_test) + environment
        checks = self._sandbox_checks(staging, sandbox_test)
        try:
            self._run_sandbox_checks(
                staging, sandbox_test, sentinel, prefix, checks
            )
        finally:
            sentinel.unlink(missing_ok=True)
            shutil.rmtree(sandbox_test, ignore_errors=True)

    def _validate_process_watchdog(self, staging: Path) -> None:
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

    def _validate_mac_release(self, staging: Path, source: SourceRelease) -> None:
        if source.allow_unsandboxed:
            self.warn(
                "WARNING: VAN_COMPUTE_ALLOW_UNSANDBOXED=1 disables OS-level job isolation."
            )
            self.warn(
                "Jobs still get private HOME/TMP/env and limits, but can address host files/network."
            )
        else:
            self._validate_sandbox_release(staging)
        self._validate_process_watchdog(staging)

    def ensure_cache_directories(self) -> None:
        for path in (self.paths.cache_root, *(self.paths.cache_root / name
                                             for name in ("logs", "jobs", "ssh"))):
            if path.is_symlink():
                raise DeploymentError(f"cache directory is a symlink: {path}")
            path.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(path, 0o700)

    def _runtime_healthy(self, release: Path) -> bool:
        python = release / "venv/bin/python"
        if any(path.is_symlink() for path in (release / "venv", python.parent, python)):
            return False
        if not python.is_file() or not os.access(python, os.X_OK):
            return False
        try:
            result = self.local.run(
                [str(python), "-I", "-B", "-c",
                 "import sys; assert sys.version_info >= (3, 11); "
                 "import androguard,isotp,numpy,pytest"],
                capture_output=True, check=False, timeout=15,
            )
            return result.returncode == 0
        except DeploymentError:
            return False

    def _installed_release_reference(self) -> Path | None:
        target = self.paths.target_plist
        try:
            details = target.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise DeploymentError(
                f"installed LaunchAgent is unreadable: {target}: {exc}"
            ) from None
        if not stat.S_ISREG(details.st_mode):
            raise DeploymentError(
                f"installed LaunchAgent is not a regular non-symlink file: {target}"
            )
        try:
            payload = plistlib.loads(target.read_bytes())
            arguments = payload.get("ProgramArguments")
            environment = payload.get("EnvironmentVariables")
            if (
                not isinstance(arguments, list)
                or not all(isinstance(item, str) for item in arguments)
                or arguments.count("van_compute.worker") != 1
                or not isinstance(environment, dict)
            ):
                raise ValueError
            module_index = arguments.index("van_compute.worker")
            if module_index < 2 or arguments[module_index - 1] != "-m":
                raise ValueError
            python = Path(arguments[0])
            release = python.parents[2]
        except (
            AttributeError,
            IndexError,
            OSError,
            TypeError,
            ValueError,
            plistlib.InvalidFileException,
        ):
            raise DeploymentError(
                f"installed LaunchAgent release reference is ambiguous: {target}"
            ) from None
        if (
            release.parent != self.paths.release_parent
            or not re.fullmatch(r"[0-9a-f]{24}(?:-[0-9a-f]{32})?", release.name)
            or python != release / "venv/bin/python"
            or environment.get("PYTHONPATH") != str(release / "app")
        ):
            raise DeploymentError(
                f"installed LaunchAgent release reference is ambiguous: {target}"
            )
        return release

    def _validate_failed_build_cleanup(self, owned: OwnedBuild) -> None:
        root = owned.path
        if (
            root.parent != self.paths.release_parent
            or not re.fullmatch(r"[0-9a-f]{24}-[0-9a-f]{32}", root.name)
        ):
            raise DeploymentError(f"failed build path is outside the release root: {root}")
        try:
            parent_details = root.parent.lstat()
            root_details = root.lstat()
        except OSError as exc:
            raise DeploymentError(
                f"failed build identity could not be revalidated: {root}: {exc}"
            ) from None
        if not stat.S_ISDIR(parent_details.st_mode) or stat.S_ISLNK(
            parent_details.st_mode
        ):
            raise DeploymentError(f"Mac release parent became unsafe: {root.parent}")
        if (
            not stat.S_ISDIR(root_details.st_mode)
            or stat.S_ISLNK(root_details.st_mode)
            or (root_details.st_dev, root_details.st_ino)
            != (owned.device, owned.inode)
        ):
            raise DeploymentError(f"failed build identity changed: {root}")
        if self.release_retention_ambiguous or self.prior_release == root:
            raise DeploymentError(f"failed build is protected from cleanup: {root}")
        installed = self._installed_release_reference()
        if installed == root:
            raise DeploymentError(f"failed build is referenced by the LaunchAgent: {root}")
        for path in (root, *root.rglob("*")):
            details = path.lstat()
            if stat.S_ISLNK(details.st_mode):
                raise DeploymentError(f"failed build contains a symlink: {path}")
            if not (stat.S_ISDIR(details.st_mode) or stat.S_ISREG(details.st_mode)):
                raise DeploymentError(f"failed build contains a special entry: {path}")
            if os.path.ismount(path):
                raise DeploymentError(f"failed build contains a mount: {path}")

    def _remove_failed_build(self, owned: OwnedBuild) -> None:
        self._validate_failed_build_cleanup(owned)
        shutil.rmtree(owned.path)

    def _build_mac_release(self, source: SourceRelease) -> Path:
        build_id = uuid.UUID(str(self.uuid_factory())).hex
        release = self.paths.release_parent / f"{source.mac_version}-{build_id}"
        # Build at the final unique path: pip entrypoint shebangs embed it. mkdir
        # is exclusive, so only a directory created here can become owned here.
        release.mkdir(mode=0o700)
        details = release.lstat()
        if not stat.S_ISDIR(details.st_mode) or stat.S_ISLNK(details.st_mode):
            raise DeploymentError(f"new Mac release directory is unsafe: {release}")
        owned = OwnedBuild(release, details.st_dev, details.st_ino)
        try:
            self.say("Building an isolated Python environment...")
            self.local.run([
                "/opt/homebrew/bin/python3", "-m", "venv", "--copies", str(release / "venv"),
            ])
            self.local.run([
                str(release / "venv/bin/python"), "-m", "pip", "install",
                "--disable-pip-version-check", "--no-input",
                "androguard", "can-isotp", "numpy", "pytest",
            ])
            self._copy_source_tree(release, source)
            self._verify_staged_source(release / "app", source)
            (release / "sandbox.sb").write_text(SANDBOX_PROFILE, encoding="utf-8")
            os.chmod(release / "sandbox.sb", 0o600)
            if not self._runtime_healthy(release):
                raise DeploymentError(
                    f"new Mac release interpreter validation failed: {release}"
                )
            self._validate_mac_release(release, source)
            (release / SOURCE_HASH_FILE).write_text(
                source.source_fingerprint + "\n", encoding="utf-8"
            )
            (release / DEPLOYMENT_HASH_FILE).write_text(
                source.deployment_fingerprint + "\n", encoding="utf-8"
            )
            provenance = self.provenance(source, "mac-worker")
            provenance["build_id"] = build_id
            (release / PROVENANCE_FILE).write_text(
                json.dumps(provenance, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            for marker in (SOURCE_HASH_FILE, DEPLOYMENT_HASH_FILE, PROVENANCE_FILE):
                os.chmod(release / marker, 0o600)
            self._write_manifest(release)
            self._verify_release(release, source)
        except BaseException:
            try:
                self._remove_failed_build(owned)
            except BaseException as cleanup_error:
                try:
                    self.warn(
                        f"WARNING: failed Mac build was retained because cleanup was unsafe: "
                        f"{release}: {cleanup_error}"
                    )
                except BaseException:
                    pass
            raise
        self.release = release
        return release

    def prepare_mac_release(self, source: SourceRelease) -> Path:
        self.ensure_cache_directories()
        canonical = self.paths.release_parent / source.mac_version
        candidate = canonical
        # A frozen installer preserves its own dependency generation on rollback.
        frozen = self.paths.repo_root.parent
        choices = [frozen, self.prior_release]
        for path in choices:
            if path is None or path.parent != self.paths.release_parent:
                continue
            hashes = self._release_identity(path, verify_runtime=False)
            if hashes == (source.source_fingerprint, source.deployment_fingerprint):
                candidate = path
                break
        if candidate.exists() or candidate.is_symlink():
            # Validate source/provenance before treating only runtime breakage as
            # repairable. Never turn arbitrary source corruption into a rebuild.
            self._verify_release(candidate, source, verify_runtime=False)
            if not self.options.rebuild and self._runtime_healthy(candidate):
                self._verify_release(candidate, source)
                self._validate_mac_release(candidate, source)
                self.say(f"Verified existing immutable Mac release: {candidate}")
                self.release = candidate
                return candidate
            self.rebuilt_release = True
            self.say(f"Building a replacement without modifying Mac release: {candidate}")
        elif self.options.rebuild:
            self.rebuilt_release = True
        self.say("Checking and provisioning local worker dependencies...")
        self._install_formulae()
        self.paths.release_parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.paths.release_parent.is_symlink():
            raise DeploymentError("Mac release parent must not be a symlink")
        os.chmod(self.paths.release_parent, 0o700)
        return self._build_mac_release(source)

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
        self.local.run(["/bin/launchctl", "enable", domain])
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
