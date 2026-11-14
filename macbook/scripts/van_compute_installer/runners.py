from __future__ import annotations

from pathlib import Path
import select
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

    def spawn(
        self, arguments: Sequence[str], *, cwd: Path | None = None
    ) -> BackgroundProcess:
        return BackgroundProcess(arguments, cwd=cwd)


class BackgroundProcess:
    """A local helper that announces itself on one stdout line and exits when
    its stdin closes, for probes that need a server alive across commands."""

    def __init__(self, arguments: Sequence[str], *, cwd: Path | None = None) -> None:
        self._label = shlex.join(arguments)
        try:
            self._process = subprocess.Popen(
                list(arguments),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=cwd,
            )
        except OSError as exc:
            raise DeploymentError(
                f"local helper failed to start: {self._label}: {exc}"
            ) from None

    def announcement(self, timeout: float) -> str:
        stdout = self._process.stdout
        ready, _writable, _errors = select.select([stdout], [], [], timeout)
        line = stdout.readline() if ready else ""
        if not line:
            detail = self.close(timeout)
            raise DeploymentError(
                (detail + "\n" if detail else "")
                + f"local helper did not announce itself: {self._label}"
            )
        return line.strip()

    def close(self, timeout: float) -> str:
        """Close stdin and reap the helper, killing it if it lingers; return
        its stderr. Never raises, so it is safe on an error path."""
        try:
            _stdout, stderr = self._process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            self._process.kill()
            _stdout, stderr = self._process.communicate()
        except (OSError, ValueError):
            self._process.kill()
            self._process.wait()
            stderr = ""
        return (stderr or "").strip()

    def finish(self, timeout: float) -> None:
        """Close the helper and fail unless it exited cleanly."""
        detail = self.close(timeout)
        if self._process.returncode != 0:
            raise DeploymentError(
                (detail + "\n" if detail else "")
                + f"local helper failed (status {self._process.returncode}): {self._label}"
            )


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
