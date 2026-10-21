from __future__ import annotations

from pathlib import Path
import shlex
import subprocess
from typing import Sequence

from .models import DeploymentError


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
