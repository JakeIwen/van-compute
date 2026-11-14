"""Opt-in persistent per-task caches, shared by the Pi broker and the Mac worker.

A repo task that declares ``"cache": "<name>"`` gets a directory that outlives
the job and becomes its ``XDG_CACHE_HOME``.  Layout under a host root::

    <root>/.prune.lock            one pruner at a time
    <root>/<namespace>/<name>.lock  flock target and last-used mtime
    <root>/<namespace>/<name>/    the job's cache directory

The lock file lives beside the cache directory, outside what the job can see
or rename.  Jobs hold a shared flock for their whole run; initialization,
eviction and ``clear`` need the exclusive flock, so a cache in use is never
touched.  Caches are never trusted inputs: a job can leave anything in one,
and the store treats its contents as hostile (descriptor-relative, no-follow
traversal everywhere below the root).
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
import fcntl
import json
import os
from pathlib import Path
import shutil
import stat
import sys
from typing import ContextManager, Iterator, Mapping

from van_compute import protocol

# Matches the broker default, <queue root>/task-caches, for the default queue root.
PI_DEFAULT_ROOT = Path("/home/pi/dev/obd-things/tmp/compute/task-caches")
MAC_DEFAULT_ROOT = Path.home() / "Library" / "Caches" / "van-compute" / "task-caches"
PI_DEFAULT_MAX_BYTES = 2 * 1024**3
MAC_DEFAULT_MAX_BYTES = 4 * 1024**3
LOCK_SUFFIX = ".lock"
PRUNE_LOCK_NAME = ".prune.lock"
# A cache whose directory is swapped under a shared lock is retried after the
# holders finish; this bounds how long a hostile job can keep others waiting.
SETUP_ATTEMPTS = 5

_DIR_FLAGS = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
_LOCK_FLAGS = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)


class TaskCacheError(RuntimeError):
    pass


@dataclass(frozen=True)
class CacheRequest:
    namespace: str
    name: str


@dataclass(frozen=True)
class CacheInfo:
    namespace: str
    name: str
    size_bytes: int | None  # None: could not be measured
    last_used: float | None
    in_use: bool


@dataclass
class PruneReport:
    skipped: bool = False
    evicted: tuple[str, ...] = ()
    failed: tuple[str, ...] = ()


def default_root() -> Path:
    return MAC_DEFAULT_ROOT if sys.platform == "darwin" else PI_DEFAULT_ROOT


def request_for_manifest(manifest: Mapping[str, object]) -> CacheRequest | None:
    """Return the cache a queued job asked for, or None for an uncached job."""
    embedded = manifest.get("execution")
    if embedded is None:
        return None
    return cache_request(manifest, protocol.validate_execution(embedded))


def cache_request(
    manifest: Mapping[str, object], specification: Mapping[str, object]
) -> CacheRequest | None:
    name = specification.get("cache")
    if name is None:
        return None
    return CacheRequest(protocol.task_cache_namespace(manifest.get("source_root")), str(name))


def require_same_filesystem(root: Path, work_root: Path) -> None:
    """Keep the work-root free-space watchdog and admission honest for caches."""
    if root.stat().st_dev != work_root.stat().st_dev:
        raise TaskCacheError(
            f"task cache root {root} must be on the same filesystem as {work_root}"
        )


def prepare_root(path: Path) -> Path:
    """Create the host cache root privately and return its resolved path."""
    path = path.expanduser()
    if path.is_symlink():
        raise TaskCacheError(f"task cache root cannot be a symlink: {path}")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)
    return path.resolve(strict=True)


def start_store(
    root: Path, max_bytes: int, work_root: Path, free_target_bytes: int, *, label: str
) -> Path:
    """Host startup: prepare the root, check its filesystem, prune; return it resolved."""
    resolved = prepare_root(root)
    require_same_filesystem(resolved, work_root)
    prune_quietly(TaskCacheStore(resolved, max_bytes), free_target_bytes, label=label)
    return resolved


def job_cache(
    store: TaskCacheStore | None,
    request: CacheRequest | None,
    default_directory: Path,
) -> tuple[Path, ContextManager[object]]:
    """Pick a job's cache directory and the scope that must wrap its child.

    Uncached jobs keep their private per-job directory.  Cached jobs get the
    store path; the returned context holds its shared lock until the caller
    leaves it, which must be after the child and its process group are gone.
    """
    if request is None:
        default_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(default_directory, 0o700)
        return default_directory, nullcontext()
    if store is None:
        raise TaskCacheError("this host has no task cache root configured")
    return store.path(request), store.use(request)


def prune_quietly(
    store: TaskCacheStore | None, free_target_bytes: int, *, label: str
) -> None:
    """Prune between jobs; a failure here must never fail or delay a job."""
    if store is None:
        return
    try:
        report = store.prune(free_target_bytes)
        if report.failed:
            print(f"{label}: task cache eviction failed for {', '.join(report.failed)}",
                  file=sys.stderr, flush=True)
    except Exception as exc:  # noqa: BLE001 - housekeeping only
        print(f"{label}: task cache prune failed: {type(exc).__name__}: {exc}",
              file=sys.stderr, flush=True)


class TaskCacheStore:
    def __init__(self, root: Path, max_bytes: int):
        self.root = root
        self.max_bytes = max_bytes

    def path(self, request: CacheRequest) -> Path:
        return self.root / request.namespace / request.name

    # -- jobs ---------------------------------------------------------------

    @contextmanager
    def use(self, request: CacheRequest) -> Iterator[Path]:
        """Hold a cache shared for one job's whole run and yield its path."""
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        root_fd = os.open(self.root, _DIR_FLAGS)
        try:
            namespace_fd = _open_child_directory(root_fd, request.namespace, create=True)
        finally:
            os.close(root_fd)
        lock_fd = -1
        try:
            lock_fd = self._acquire_shared(namespace_fd, request)
            os.utime(lock_fd)
            try:
                yield self.path(request)
            finally:
                os.utime(lock_fd)
        finally:
            if lock_fd >= 0:
                os.close(lock_fd)  # closing releases the flock
            os.close(namespace_fd)

    def _acquire_shared(self, namespace_fd: int, request: CacheRequest) -> int:
        lock_name = request.name + LOCK_SUFFIX
        for _ in range(SETUP_ATTEMPTS):
            lock_fd = os.open(
                lock_name, _LOCK_FLAGS | os.O_CREAT, 0o600, dir_fd=namespace_fd
            )
            try:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    _initialize_directory(namespace_fd, request.name)
                    # Not an atomic downgrade: a pruner may slip in between,
                    # which the re-check below turns into another attempt.
                    fcntl.flock(lock_fd, fcntl.LOCK_SH)
                except BlockingIOError:
                    # Other jobs hold it shared, so it is already initialized.
                    fcntl.flock(lock_fd, fcntl.LOCK_SH)
                if _is_real_directory(namespace_fd, request.name):
                    return lock_fd
            except BaseException:
                os.close(lock_fd)
                raise
            os.close(lock_fd)
        raise TaskCacheError(
            f"task cache {request.namespace}/{request.name} is not a usable directory"
        )

    # -- housekeeping -------------------------------------------------------

    def prune(self, free_target_bytes: int) -> PruneReport:
        """Evict whole idle caches, least recently used first.

        Runs until the total is within ``max_bytes`` and the filesystem has
        ``free_target_bytes`` free.  The cap is a between-job target, not a
        quota: caches in use are counted but never touched.
        """
        if not self.root.is_dir():
            return PruneReport()
        root_fd = os.open(self.root, _DIR_FLAGS)
        try:
            prune_fd = os.open(
                PRUNE_LOCK_NAME, _LOCK_FLAGS | os.O_CREAT, 0o600, dir_fd=root_fd
            )
            try:
                try:
                    fcntl.flock(prune_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    return PruneReport(skipped=True)
                return self._prune_locked(root_fd, free_target_bytes)
            finally:
                os.close(prune_fd)
        finally:
            os.close(root_fd)

    def _prune_locked(self, root_fd: int, free_target_bytes: int) -> PruneReport:
        device = os.fstat(root_fd).st_dev
        entries = []
        for namespace, name in _enumerate(root_fd):
            try:
                namespace_fd = _open_child_directory(root_fd, namespace, create=False)
                try:
                    size = _measure(namespace_fd, name, device)
                    last_used = _lock_mtime(namespace_fd, name)
                finally:
                    os.close(namespace_fd)
            except (OSError, TaskCacheError):
                continue
            entries.append((namespace, name, size, last_used))
        total = sum(size or 0 for _, _, size, _ in entries)
        # Unmeasurable caches count as over the cap for as long as one exists
        # (even a busy one we cannot touch), so idle caches keep being evicted.
        unmeasurable = sum(1 for _, _, size, _ in entries if size is None)
        entries.sort(key=lambda item: (item[2] is not None, item[3]))
        evicted: list[str] = []
        failed: list[str] = []
        for namespace, name, size, _ in entries:
            if not unmeasurable and total <= self.max_bytes and (
                shutil.disk_usage(self.root).free >= free_target_bytes
            ):
                break
            try:
                outcome = self._evict(root_fd, namespace, name, device)
            except Exception:  # noqa: BLE001 - one poisoned cache must not stop the rest
                outcome = "failed"
            if outcome in ("evicted", "absent"):
                total -= size or 0
                unmeasurable -= size is None
                if outcome == "evicted":
                    evicted.append(f"{namespace}/{name}")
            elif outcome == "failed":
                failed.append(f"{namespace}/{name}")
        return PruneReport(evicted=tuple(evicted), failed=tuple(failed))

    def _evict(self, root_fd: int, namespace: str, name: str, device: int) -> str:
        """Delete one idle cache under its exclusive lock.

        Returns evicted, absent (only its lock file was left), busy or failed.
        """
        namespace_fd = _open_child_directory(root_fd, namespace, create=False)
        try:
            lock_fd = os.open(
                name + LOCK_SUFFIX, _LOCK_FLAGS | os.O_CREAT, 0o600, dir_fd=namespace_fd
            )
            try:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    return "busy"
                try:
                    removed = _remove_entry(namespace_fd, name, device)
                except Exception:  # noqa: BLE001 - reported as failed by the caller
                    return "failed"
                return "evicted" if removed else "absent"
            finally:
                os.close(lock_fd)
        finally:
            os.close(namespace_fd)

    def clear(
        self, namespace: str | None = None, name: str | None = None
    ) -> dict[str, list[str]]:
        """Delete idle caches (all, or one namespace/name); report cleared and busy."""
        report: dict[str, list[str]] = {"cleared": [], "busy": [], "failed": []}
        if not self.root.is_dir():
            return report
        root_fd = os.open(self.root, _DIR_FLAGS)
        try:
            device = os.fstat(root_fd).st_dev
            for found_namespace, found_name in _enumerate(root_fd):
                if namespace not in (None, found_namespace) or name not in (None, found_name):
                    continue
                try:
                    outcome = self._evict(root_fd, found_namespace, found_name, device)
                except Exception:  # noqa: BLE001 - report it, keep clearing the rest
                    outcome = "failed"
                if outcome != "absent":
                    key = "cleared" if outcome == "evicted" else outcome
                    report[key].append(f"{found_namespace}/{found_name}")
        finally:
            os.close(root_fd)
        return report

    def caches(self) -> list[CacheInfo]:
        if not self.root.is_dir():
            return []
        root_fd = os.open(self.root, _DIR_FLAGS)
        try:
            device = os.fstat(root_fd).st_dev
            infos = []
            for namespace, name in _enumerate(root_fd):
                try:
                    namespace_fd = _open_child_directory(root_fd, namespace, create=False)
                except TaskCacheError:
                    continue
                try:
                    infos.append(CacheInfo(
                        namespace, name,
                        _measure(namespace_fd, name, device),
                        _lock_mtime(namespace_fd, name),
                        _is_locked(namespace_fd, name),
                    ))
                finally:
                    os.close(namespace_fd)
            return infos
        finally:
            os.close(root_fd)


# -- descriptor-relative helpers ----------------------------------------------


def _open_child_directory(parent_fd: int, name: str, *, create: bool) -> int:
    if create:
        try:
            os.mkdir(name, 0o700, dir_fd=parent_fd)
        except FileExistsError:
            pass
    try:
        return os.open(name, _DIR_FLAGS, dir_fd=parent_fd)
    except OSError as exc:
        raise TaskCacheError(f"task cache namespace {name!r} is unusable: {exc}") from None


def _is_real_directory(namespace_fd: int, name: str) -> bool:
    try:
        return stat.S_ISDIR(os.stat(name, dir_fd=namespace_fd, follow_symlinks=False).st_mode)
    except FileNotFoundError:
        return False


def _initialize_directory(namespace_fd: int, name: str) -> None:
    """Create or repair the cache directory; caller holds the exclusive lock."""
    try:
        details = os.stat(name, dir_fd=namespace_fd, follow_symlinks=False)
    except FileNotFoundError:
        os.mkdir(name, 0o700, dir_fd=namespace_fd)
        return
    if stat.S_ISDIR(details.st_mode):
        # Nothing else touches it while we hold the exclusive lock, so the
        # path-following chmod cannot be redirected.
        os.chmod(name, 0o700, dir_fd=namespace_fd)
        return
    os.unlink(name, dir_fd=namespace_fd)
    os.mkdir(name, 0o700, dir_fd=namespace_fd)


def _enumerate(root_fd: int) -> list[tuple[str, str]]:
    """Every (namespace, name) pair that has a lock file or a directory."""
    pairs = []
    for namespace in sorted(os.listdir(root_fd)):
        if namespace.startswith("."):
            continue
        try:
            namespace_fd = _open_child_directory(root_fd, namespace, create=False)
        except TaskCacheError:
            continue
        try:
            names = set()
            for entry in os.listdir(namespace_fd):
                if entry.endswith(LOCK_SUFFIX):
                    names.add(entry[: -len(LOCK_SUFFIX)])
                else:
                    names.add(entry)
            pairs.extend((namespace, name) for name in sorted(names) if name)
        finally:
            os.close(namespace_fd)
    return pairs


def _lock_mtime(namespace_fd: int, name: str) -> float:
    try:
        return os.stat(name + LOCK_SUFFIX, dir_fd=namespace_fd, follow_symlinks=False).st_mtime
    except FileNotFoundError:
        return 0.0


def _is_locked(namespace_fd: int, name: str) -> bool:
    try:
        lock_fd = os.open(name + LOCK_SUFFIX, _LOCK_FLAGS, dir_fd=namespace_fd)
    except OSError:
        return False
    try:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        return False
    finally:
        os.close(lock_fd)


def _measure(namespace_fd: int, name: str, device: int) -> int | None:
    """Allocated bytes of one cache, or None when any part cannot be read.

    Only a cache that is already absent counts as 0; an entry vanishing while
    the tree is walked (a job is running in it) makes the size unmeasurable.
    """
    try:
        details = os.stat(name, dir_fd=namespace_fd, follow_symlinks=False)
    except FileNotFoundError:
        return 0
    except OSError:
        return None
    try:
        if not stat.S_ISDIR(details.st_mode):
            return details.st_blocks * 512
        descriptor = os.open(name, _DIR_FLAGS, dir_fd=namespace_fd)
        try:
            return details.st_blocks * 512 + _traverse(descriptor, device, remove=False)
        finally:
            os.close(descriptor)
    except (OSError, TaskCacheError):
        return None


def _remove_entry(namespace_fd: int, name: str, device: int) -> bool:
    """Remove the cache directory (or whatever replaced it); False if absent."""
    try:
        details = os.stat(name, dir_fd=namespace_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    if not stat.S_ISDIR(details.st_mode):
        os.unlink(name, dir_fd=namespace_fd)
        return True
    descriptor = _open_directory_for_walk(namespace_fd, name, details, device, remove=True)
    try:
        _traverse(descriptor, device, remove=True)
    finally:
        os.close(descriptor)
    os.rmdir(name, dir_fd=namespace_fd)
    return True


def _open_directory_for_walk(
    parent_fd: int, name: str, details: os.stat_result, device: int, *, remove: bool
) -> int:
    if details.st_dev != device:
        raise TaskCacheError(f"cache entry crosses a filesystem boundary: {name}")
    try:
        descriptor = os.open(name, _DIR_FLAGS, dir_fd=parent_fd)
    except PermissionError:
        if not remove:
            raise
        # A job may have dropped its own rwx; repair that one directory. The
        # exclusive cache lock keeps nothing else running in the tree.
        os.chmod(name, stat.S_IMODE(details.st_mode) | 0o700, dir_fd=parent_fd)
        descriptor = os.open(name, _DIR_FLAGS, dir_fd=parent_fd)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (details.st_dev, details.st_ino):
            raise TaskCacheError(f"directory changed while opening: {name}")
        if remove and stat.S_IMODE(opened.st_mode) & 0o700 != 0o700:
            os.fchmod(descriptor, stat.S_IMODE(opened.st_mode) | 0o700)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _traverse(top_fd: int, device: int, *, remove: bool) -> int:
    """Walk (and with ``remove`` empty) the tree under ``top_fd``; return its
    allocated bytes below it.

    Iterative with exactly one descriptor open at a time: a job controls the
    tree's depth, so recursion or an fd per level would let it break pruning.
    The way back up reopens ``..`` and checks it is the directory we left.
    """
    total = 0
    current = top_fd
    pending = os.listdir(current)
    inode = os.fstat(current).st_ino
    stack: list[tuple[list[str], int, str]] = []
    try:
        while True:
            if pending:
                child = pending.pop()
                details = os.stat(child, dir_fd=current, follow_symlinks=False)
                total += details.st_blocks * 512
                if not stat.S_ISDIR(details.st_mode):
                    if remove:
                        os.unlink(child, dir_fd=current)
                    continue
                if details.st_dev != device and not remove:
                    continue  # never measure across a mount
                descriptor = _open_directory_for_walk(
                    current, child, details, device, remove=remove
                )
                stack.append((pending, inode, child))
                if current != top_fd:
                    os.close(current)
                current, inode = descriptor, details.st_ino
                pending = os.listdir(current)
                continue
            if not stack:
                return total
            pending, parent_inode, child_name = stack.pop()
            parent = os.open("..", _DIR_FLAGS, dir_fd=current)
            os.close(current)
            current = parent
            if os.fstat(current).st_ino != parent_inode:
                raise TaskCacheError(f"tree changed while walking: {child_name}")
            if remove:
                os.rmdir(child_name, dir_fd=current)
            inode = parent_inode
            if not stack:
                # Back at the top: keep the caller's descriptor as the only one.
                if current != top_fd:
                    os.close(current)
                    current = top_fd
    finally:
        if current != top_fd:
            os.close(current)


# -- command line ---------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Inspect or clear persistent task caches.")
    parser.add_argument("--root", type=Path, default=default_root())
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="show every cache with size, last use and state")
    clear = commands.add_parser("clear", help="delete idle caches")
    clear.add_argument("--all", action="store_true", help="clear every cache")
    clear.add_argument("target", nargs="?", help="<namespace>/<name> to clear")
    args = parser.parse_args(argv)
    store = TaskCacheStore(args.root.expanduser(), 0)
    if args.command == "list":
        rows = [
            {
                "namespace": info.namespace,
                "name": info.name,
                "bytes": info.size_bytes,
                "measurable": info.size_bytes is not None,
                "last_used": info.last_used,
                "in_use": info.in_use,
            }
            for info in store.caches()
        ]
        print(json.dumps(rows, indent=2, sort_keys=True))
        return 0
    if args.all == bool(args.target):
        parser.error("give exactly one of --all or <namespace>/<name>")
    if args.all:
        report = store.clear()
    else:
        namespace, _, name = args.target.partition("/")
        if not namespace or not name:
            parser.error("target must look like <namespace>/<name>")
        report = store.clear(namespace, name)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if report["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
