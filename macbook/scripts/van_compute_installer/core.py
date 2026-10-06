from __future__ import annotations

import hashlib
import os
from pathlib import Path
import stat
import sys
import time
from typing import Callable, Mapping, TextIO
import uuid

from .constants import DEFAULT_HOST, DEFAULT_WORKER
from .models import (
    DeploymentError, LocalRunner, Options, Paths, RemoteRunner, SourceRelease, State,
)

DEFAULT_SCRIPT = Path(__file__).parent.parent / "install_van_compute_worker.py"


class InstallerCore:

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
        self.script = DEFAULT_SCRIPT if script is None else script
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
        self.rebuilt_release = False

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
