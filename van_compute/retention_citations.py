"""Git working-tree citation discovery for compute-job retention."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
from typing import Iterable

from van_compute import queue


READ_CHUNK = 1024 * 1024
GIT_TIMEOUT_SECONDS = 120
JOB_ID_BYTES_RE = re.compile(queue.JOB_ID_RE.pattern.encode("ascii"))


@dataclass(frozen=True)
class CitationScan:
    job_ids: frozenset[str]
    files_scanned: int


def _run_git(checkout: Path, arguments: list[str]) -> bytes:
    executable = shutil.which("git")
    if executable is None:
        raise queue.QueueError("cannot scan citations: git is not installed")
    try:
        result = subprocess.run(
            [executable, "-C", str(checkout), *arguments],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=GIT_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise queue.QueueError(f"cannot scan citations with git: {exc}") from None
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", "replace").strip().replace("\n", " ")
        suffix = f": {detail[:400]}" if detail else ""
        raise queue.QueueError(
            f"cannot scan citations: git {' '.join(arguments)} failed{suffix}"
        )
    return result.stdout


def _citation_paths(checkout: Path) -> tuple[Path, ...]:
    if checkout.is_symlink() or not checkout.is_dir():
        raise queue.QueueError(f"citation checkout is not a real directory: {checkout}")
    resolved = checkout.resolve()
    top = os.fsdecode(_run_git(resolved, ["rev-parse", "--show-toplevel"])).strip()
    if not top or Path(top).resolve() != resolved:
        raise queue.QueueError(
            f"citation checkout is not the Git working-tree root: {resolved}"
        )
    output = _run_git(
        resolved,
        ["ls-files", "--cached", "--others", "--exclude-standard", "-z"],
    )
    paths: set[Path] = set()
    for raw in output.split(b"\0"):
        if not raw:
            continue
        relative = Path(os.fsdecode(raw))
        if relative.is_absolute() or ".." in relative.parts:
            raise queue.QueueError(f"git returned an unsafe citation path: {relative}")
        if relative.parts and relative.parts[0] == "tmp":
            continue
        paths.add(relative)
    return tuple(sorted(paths, key=lambda item: os.fsencode(item.as_posix())))


def job_ids_from_chunks(chunks: Iterable[bytes]) -> set[str]:
    found: set[str] = set()
    carry = b""
    overlap = 64
    for chunk in chunks:
        data = carry + chunk
        found.update(
            match.group().decode("ascii")
            for match in JOB_ID_BYTES_RE.finditer(data)
        )
        carry = data[-overlap:]
    return found


def _ids_in_citation_file(path: Path) -> set[str] | None:
    try:
        details = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise queue.QueueError(f"cannot inspect citation file {path}: {exc}") from None
    if stat.S_ISLNK(details.st_mode):
        try:
            target = os.fsencode(os.readlink(path))
            completed = path.lstat()
        except OSError as exc:
            raise queue.QueueError(f"cannot read citation symlink {path}: {exc}") from None
        if (completed.st_dev, completed.st_ino) != (details.st_dev, details.st_ino):
            raise queue.QueueError(f"citation symlink changed while reading: {path}")
        return job_ids_from_chunks((target,))
    if not stat.S_ISREG(details.st_mode):
        raise queue.QueueError(f"citation path is not a regular file: {path}")
    flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
        with os.fdopen(descriptor, "rb") as handle:
            opened = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(opened.st_mode)
                or (opened.st_dev, opened.st_ino) != (details.st_dev, details.st_ino)
            ):
                raise queue.QueueError(f"citation path changed while opening: {path}")
            found = job_ids_from_chunks(iter(lambda: handle.read(READ_CHUNK), b""))
            completed = os.fstat(handle.fileno())
            if (
                completed.st_size != opened.st_size
                or completed.st_mtime_ns != opened.st_mtime_ns
                or completed.st_ctime_ns != opened.st_ctime_ns
            ):
                raise queue.QueueError(f"citation file changed while reading: {path}")
            return found
    except OSError as exc:
        raise queue.QueueError(f"cannot read citation file {path}: {exc}") from None


def scan_citations(checkout: Path) -> CitationScan:
    checkout = checkout.expanduser().resolve()
    job_ids: set[str] = set()
    files_scanned = 0
    for relative in _citation_paths(checkout):
        job_ids.update(job_ids_from_chunks((os.fsencode(relative.as_posix()),)))
        found = _ids_in_citation_file(checkout / relative)
        if found is None:
            continue
        files_scanned += 1
        job_ids.update(found)
    return CitationScan(frozenset(job_ids), files_scanned)
