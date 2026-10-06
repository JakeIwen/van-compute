from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shlex
import subprocess
from typing import Sequence

from .constants import LABEL

class DeploymentError(RuntimeError):
    """A fail-closed installer error suitable for an operator."""


@dataclass(frozen=True)
class Options:
    if_needed: bool = False
    dry_run: bool = False
    rebuild: bool = False
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


@dataclass(frozen=True)
class OwnedBuild:
    path: Path
    device: int
    inode: int


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
