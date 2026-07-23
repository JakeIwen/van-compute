#!/usr/bin/env python3
"""Fail-closed public CLI fence used while van-compute is being upgraded.

The installer places this script at the public ``van_compute.py`` path only
after the current Mac worker has drained. Existing submit processes keep their
already-loaded code, so the installer also uses ``--active-submitter-count``
from its private staging path before crossing the protocol boundary.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import errno
import fcntl
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
from typing import Iterator


UPGRADE_GATE = True
UPGRADE_OWNER_RE = re.compile(
    r"installer-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
)
DEFAULT_SCRIPT_ROOT = Path("/home/pi/van_compute/scripts")
DEFAULT_QUEUE_ROOT = Path("/home/pi/dev/obd-things/tmp/compute")

# ``pi_compute.py`` imports the public queue module before it parses a command.
# When this file occupies that public path, stop the importing frontend with the
# same temporary-failure status instead of exposing a partial module traceback.
if __name__ == "van_compute":
    print(
        "van-compute is being upgraded; retry this command shortly",
        file=sys.stderr,
    )
    raise SystemExit(75)


def _is_supported_submitter(arguments: list[bytes]) -> bool:
    if not arguments:
        return False
    names = {Path(os.fsdecode(argument)).name for argument in arguments if argument}
    return (
        ("van_compute.py" in names and b"submit" in arguments)
        or ("pi_compute.py" in names and b"run" in arguments)
    )


def active_submitter_count(
    proc: Path = Path("/proc"),
    *,
    current_pid: int | None = None,
    current_uid: int | None = None,
) -> int:
    """Count same-user supported CLI submissions that loaded before the fence."""
    if not proc.is_dir():
        raise RuntimeError("/proc is unavailable; cannot drain active submissions")
    current_pid = os.getpid() if current_pid is None else current_pid
    current_uid = os.getuid() if current_uid is None else current_uid
    count = 0
    for process in proc.iterdir():
        if not process.name.isdigit() or int(process.name) == current_pid:
            continue
        try:
            if process.stat().st_uid != current_uid:
                continue
            raw = (process / "cmdline").read_bytes()
        except FileNotFoundError:
            # Normal race with a process exiting between directory scan and read.
            continue
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                continue
            raise RuntimeError(
                f"cannot inspect same-user process {process.name}: {exc}"
            ) from None
        if _is_supported_submitter([part for part in raw.split(b"\0") if part]):
            count += 1
    return count


def _regular_file(path: Path, description: str) -> os.stat_result:
    try:
        details = path.lstat()
    except OSError as exc:
        raise RuntimeError(f"cannot inspect {description}: {exc}") from None
    if not stat.S_ISREG(details.st_mode):
        raise RuntimeError(f"{description} is not a regular file: {path}")
    return details


def _optional_regular_file(path: Path, description: str) -> bool:
    try:
        details = path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise RuntimeError(f"cannot inspect {description}: {exc}") from None
    if not stat.S_ISREG(details.st_mode):
        raise RuntimeError(f"{description} is not a regular file: {path}")
    return True


def _read_bytes(path: Path, description: str) -> bytes:
    _regular_file(path, description)
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as handle:
            return handle.read()
    except OSError as exc:
        raise RuntimeError(f"cannot read {description}: {exc}") from None


def _is_gate(path: Path) -> bool:
    return b"UPGRADE_GATE = True" in _read_bytes(path, "public queue CLI").splitlines()


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_bytes(path: Path, payload: bytes, mode: int) -> None:
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
            os.fchmod(handle.fileno(), mode)
        os.replace(temporary, path)
        _sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def upgrade_lock(script_root: Path = DEFAULT_SCRIPT_ROOT) -> Iterator[None]:
    """Serialize ownership and gate changes across every compute node installer."""
    if script_root.is_symlink() or not script_root.is_dir():
        raise RuntimeError(f"script root is not a real directory: {script_root}")
    lock_path = script_root / ".van-compute-upgrade.lock"
    flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise RuntimeError(f"cannot open upgrade lock: {exc}") from None
    try:
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode):
            raise RuntimeError(f"upgrade lock is not a regular file: {lock_path}")
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _validate_owner(owner: str) -> str:
    if not UPGRADE_OWNER_RE.fullmatch(owner):
        raise RuntimeError("upgrade owner is invalid")
    return owner


def _paths(script_root: Path) -> tuple[Path, Path, Path]:
    return (
        script_root / "van_compute.py",
        script_root / ".van_compute.py.pre-upgrade",
        script_root / ".van-compute-upgrade-owner",
    )


def _require_owner(owner_record: Path, owner: str) -> None:
    if not _optional_regular_file(owner_record, "upgrade owner record"):
        raise RuntimeError("upgrade owner record is missing")
    try:
        recorded = _read_bytes(owner_record, "upgrade owner record").decode().strip()
    except UnicodeDecodeError:
        raise RuntimeError("upgrade owner record is invalid") from None
    if recorded != owner:
        raise RuntimeError("upgrade is owned by another installer")


def acquire_submission_gate(
    gate: Path,
    owner: str,
    *,
    allow_existing_backup: bool = False,
    script_root: Path = DEFAULT_SCRIPT_ROOT,
) -> None:
    """Atomically acquire ownership, preserve the CLI, and publish the fence."""
    owner = _validate_owner(owner)
    gate_bytes = _read_bytes(gate, "staged upgrade gate")
    if b"UPGRADE_GATE = True" not in gate_bytes.splitlines():
        raise RuntimeError("staged upgrade gate is missing its marker")
    with upgrade_lock(script_root):
        target, backup, owner_record = _paths(script_root)
        target_details = _regular_file(target, "public queue CLI")
        if _optional_regular_file(owner_record, "upgrade owner record"):
            _require_owner(owner_record, owner)
        else:
            _atomic_bytes(owner_record, f"{owner}\n".encode(), 0o600)

        if _is_gate(target):
            _regular_file(backup, "pre-upgrade queue CLI backup")
            return

        target_bytes = _read_bytes(target, "public queue CLI")
        if _optional_regular_file(backup, "pre-upgrade queue CLI backup"):
            if not allow_existing_backup and _read_bytes(
                backup, "pre-upgrade queue CLI backup"
            ) != target_bytes:
                raise RuntimeError(
                    "existing upgrade backup does not match the active CLI"
                )
        else:
            _atomic_bytes(backup, target_bytes, 0o600)
        _atomic_bytes(target, gate_bytes, stat.S_IMODE(target_details.st_mode) or 0o700)


def restore_submission_cli(
    owner: str,
    *,
    script_root: Path = DEFAULT_SCRIPT_ROOT,
) -> None:
    """Restore only artifacts owned by this installer, under the shared lock."""
    owner = _validate_owner(owner)
    with upgrade_lock(script_root):
        target, backup, owner_record = _paths(script_root)
        _require_owner(owner_record, owner)
        target_details = _regular_file(target, "public queue CLI")
        if _is_gate(target):
            backup_bytes = _read_bytes(backup, "pre-upgrade queue CLI backup")
            _atomic_bytes(
                target,
                backup_bytes,
                stat.S_IMODE(target_details.st_mode) or 0o700,
            )
        elif _optional_regular_file(backup, "pre-upgrade queue CLI backup"):
            if _read_bytes(backup, "pre-upgrade queue CLI backup") != _read_bytes(
                target, "public queue CLI"
            ):
                raise RuntimeError("cannot prove the pre-upgrade CLI can be restored")
        backup.unlink(missing_ok=True)
        owner_record.unlink()
        _sync_directory(script_root)


def finalize_upgrade(
    owner: str,
    *,
    script_root: Path = DEFAULT_SCRIPT_ROOT,
    queue_root: Path = DEFAULT_QUEUE_ROOT,
    queue_cli: Path | None = None,
    retire_target: bool = False,
) -> None:
    """Finalize an owned CLI transition and release queue maintenance."""
    owner = _validate_owner(owner)
    with upgrade_lock(script_root):
        target, backup, owner_record = _paths(script_root)
        _require_owner(owner_record, owner)
        _regular_file(target, "public queue CLI")
        if queue_cli is None:
            queue_cli = target
        _regular_file(queue_cli, "replacement queue CLI")
        if _is_gate(queue_cli):
            raise RuntimeError("replacement queue CLI is still the upgrade gate")
        target_is_gate = _is_gate(target)
        if not retire_target and target_is_gate:
            raise RuntimeError("public queue CLI is still the upgrade gate")
        if retire_target and queue_cli == target:
            raise RuntimeError("cannot retire the queue CLI used to release maintenance")
        if retire_target and not target_is_gate:
            raise RuntimeError("retired queue CLI is not the upgrade gate")
        if retire_target:
            # Remove the obsolete public entry point first, but retain its
            # owner and rollback record until the installer deletes the whole
            # old root. An interruption at any later point is then resumable:
            # the new CLI is authoritative, maintenance still names this owner
            # if release failed, and the retired root remains attributable.
            _regular_file(backup, "pre-upgrade queue CLI backup")
            target.unlink()
        else:
            # A same-root upgrade keeps its replacement CLI, so stale rollback
            # artifacts must disappear before maintenance is released.
            if _optional_regular_file(backup, "pre-upgrade queue CLI backup"):
                backup.unlink()
            owner_record.unlink()
        _sync_directory(script_root)
        try:
            subprocess.run(
                [
                    str(queue_cli),
                    "--root",
                    str(queue_root),
                    "maintenance",
                    "exit",
                    "--owner",
                    owner,
                ],
                check=True,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                timeout=30,
            )
        except (
            OSError,
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
        ) as exc:
            raise RuntimeError(f"cannot release queue maintenance: {exc}") from None


def main() -> int:
    if sys.argv[1:] == ["--active-submitter-count"]:
        try:
            print(active_submitter_count())
        except RuntimeError as exc:
            print(f"van-compute upgrade gate: {exc}", file=sys.stderr)
            return 2
        return 0
    # Gate-management operations are accepted only from the separately staged
    # helper, never from the copy occupying the public queue CLI path.
    if Path(__file__).name == "van_compute.py":
        print(
            "van-compute is being upgraded; retry this command shortly",
            file=sys.stderr,
        )
        return 75
    parser = argparse.ArgumentParser(description=__doc__)
    operation = parser.add_mutually_exclusive_group(required=True)
    operation.add_argument("--acquire", action="store_true")
    operation.add_argument("--restore", action="store_true")
    operation.add_argument("--finalize", action="store_true")
    parser.add_argument("--owner", required=True)
    parser.add_argument("--gate", type=Path)
    parser.add_argument("--script-root", type=Path, default=DEFAULT_SCRIPT_ROOT)
    parser.add_argument("--queue-cli", type=Path)
    parser.add_argument("--retire-target", action="store_true")
    parser.add_argument("--allow-existing-backup", action="store_true")
    arguments = parser.parse_args()
    try:
        if arguments.acquire:
            if arguments.gate is None:
                parser.error("--acquire requires --gate")
            acquire_submission_gate(
                arguments.gate,
                arguments.owner,
                allow_existing_backup=arguments.allow_existing_backup,
                script_root=arguments.script_root,
            )
        elif arguments.restore:
            if (
                arguments.gate is not None
                or arguments.queue_cli is not None
                or arguments.retire_target
                or arguments.allow_existing_backup
            ):
                parser.error("--restore does not accept acquire/finalize options")
            restore_submission_cli(
                arguments.owner,
                script_root=arguments.script_root,
            )
        else:
            if arguments.gate is not None or arguments.allow_existing_backup:
                parser.error("--finalize does not accept acquire options")
            finalize_upgrade(
                arguments.owner,
                script_root=arguments.script_root,
                queue_cli=arguments.queue_cli,
                retire_target=arguments.retire_target,
            )
    except RuntimeError as exc:
        print(f"van-compute upgrade gate: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
