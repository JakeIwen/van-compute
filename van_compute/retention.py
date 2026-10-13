"""Conservative retention planning and pruning for completed queue jobs."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass, replace
import datetime as dt
from io import BytesIO, TextIOWrapper
import os
from pathlib import Path
import stat
import sys
import time
from typing import Callable, Iterable, Iterator
import uuid

from van_compute import queue, upgrade_gate
from van_compute.retention_citations import (
    CitationScan,
    job_ids_from_chunks,
    scan_citations,
)


RETENTION_DAYS = {"done": 60, "failed": 31}
RETENTION_STATES = tuple(RETENTION_DAYS)
CATEGORIES = ("protected", "kept-young", "skipped", "eligible")
SUBMITTER_DRAIN_TIMEOUT = 120.0
SUBMITTER_POLL_INTERVAL = 0.5


def add_prune_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("prune", help=argparse.SUPPRESS)
    parser.add_argument("--apply", action="store_true", help="delete eligible jobs")
    parser.add_argument(
        "--citations-root",
        type=Path,
        default=Path(
            os.environ.get("VAN_COMPUTE_SOURCE_ROOT", queue.DEFAULT_SOURCE_ROOT)
        ),
        help="Git checkout whose non-tmp files protect cited jobs",
    )
    parser.add_argument("--now", help="override the current ISO timestamp")


@dataclass(frozen=True)
class JobDecision:
    state: str
    job_id: str
    path: Path
    size: int
    category: str
    reason: str
    finished_at: str | None = None
    identity: tuple[int, int] | None = None


class _UnsafeTarget(queue.QueueError):
    pass


def _ids_in_manifest(value: object) -> set[str]:
    found: set[str] = set()
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, str):
            found.update(queue.JOB_ID_RE.findall(item))
        elif isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)
    return found


def _read_manifest(
    job_path: Path,
    device: int,
    *,
    context: str,
    required: bool,
    allow_invalid: bool = False,
) -> tuple[dict[str, object] | None, frozenset[str]]:
    path = job_path / "manifest.json"
    try:
        details = path.lstat()
    except FileNotFoundError:
        if required:
            raise queue.QueueError(f"{context} is missing: {path}") from None
        return None, frozenset()
    except OSError as exc:
        raise queue.QueueError(f"cannot inspect {context} {path}: {exc}") from None
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISREG(details.st_mode):
        raise queue.QueueError(f"{context} is not a regular file: {path}")
    if details.st_dev != device:
        raise queue.QueueError(f"{context} crosses the queue filesystem: {path}")

    flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
        with os.fdopen(descriptor, "rb") as handle:
            opened = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(opened.st_mode)
                or (opened.st_dev, opened.st_ino) != (details.st_dev, details.st_ino)
            ):
                raise queue.QueueError(f"{context} changed while opening: {path}")
            data = handle.read()
            completed = os.fstat(handle.fileno())
            if (
                completed.st_size != opened.st_size
                or completed.st_mtime_ns != opened.st_mtime_ns
                or completed.st_ctime_ns != opened.st_ctime_ns
            ):
                raise queue.QueueError(f"{context} changed while reading: {path}")
    except OSError as exc:
        raise queue.QueueError(f"cannot read {context} {path}: {exc}") from None

    references = job_ids_from_chunks((data,))
    try:
        with TextIOWrapper(BytesIO(data), encoding="utf-8") as handle:
            manifest = queue.load_json_stream(handle, path)
    except queue.QueueError as exc:
        if allow_invalid:
            return None, frozenset(references)
        raise queue.QueueError(f"cannot parse {context} {path}: {exc}") from None
    references.update(_ids_in_manifest(manifest))
    return manifest, frozenset(references)


def active_dependency_ids(root: Path) -> frozenset[str]:
    device = root.stat().st_dev
    protected: set[str] = set()
    for state in ("queued", "running"):
        state_root = root / state
        try:
            entries = tuple(state_root.iterdir())
        except OSError as exc:
            raise queue.QueueError(f"cannot inspect active queue state {state_root}: {exc}") from None
        for job_path in entries:
            if not queue.JOB_ID_RE.fullmatch(job_path.name):
                continue
            if job_path.is_symlink() or not job_path.is_dir():
                raise queue.QueueError(f"active job path is unsafe: {job_path}")
            manifest, references = _read_manifest(
                job_path,
                device,
                context="active job manifest",
                required=True,
            )
            if manifest is None or manifest.get("id") != job_path.name:
                raise queue.QueueError(
                    f"active job manifest identity is invalid: {job_path}"
                )
            protected.update(references)
    return frozenset(protected)


def protected_job_ids(root: Path, direct: frozenset[str]) -> frozenset[str]:
    protected = set(direct)
    pending = list(direct)
    expanded: set[str] = set()
    device = root.stat().st_dev
    while pending:
        job_id = pending.pop()
        if job_id in expanded:
            continue
        expanded.add(job_id)
        for state in queue.STATE_DIRECTORIES:
            job_path = root / state / job_id
            if not os.path.lexists(job_path):
                continue
            if job_path.is_symlink() or not job_path.is_dir():
                raise queue.QueueError(
                    f"cannot follow protected job manifest through unsafe path: {job_path}"
                )
            _manifest, references = _read_manifest(
                job_path,
                device,
                context="protected job manifest",
                required=False,
                allow_invalid=True,
            )
            for referenced in references - protected:
                protected.add(referenced)
                pending.append(referenced)
    return frozenset(protected)


def parse_now(value: str | None) -> dt.datetime:
    if value is None:
        parsed = dt.datetime.now(dt.timezone.utc)
    else:
        try:
            parsed = queue.parse_timestamp(value)
        except (TypeError, ValueError):
            raise queue.QueueError(f"invalid --now timestamp {value!r}") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise queue.QueueError("--now and manifest finished_at timestamps need a timezone")
    return parsed.astimezone(dt.timezone.utc)


def _linux_mount_points() -> frozenset[Path]:
    if not sys.platform.startswith("linux"):
        return frozenset()
    path = Path("/proc/self/mountinfo")
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise queue.QueueError(f"cannot verify mount boundaries from {path}: {exc}") from None
    mounts: set[Path] = set()
    for line in lines:
        fields = line.split()
        if len(fields) < 5:
            raise queue.QueueError(f"cannot parse mount boundary record in {path}")
        text = fields[4]
        for escaped, plain in (("\\040", " "), ("\\011", "\t"), ("\\012", "\n"), ("\\134", "\\")):
            text = text.replace(escaped, plain)
        mounts.add(Path(text))
    return frozenset(mounts)


def _mount_at_or_below(target: Path, mounts: frozenset[Path]) -> bool:
    if sys.platform.startswith("linux"):
        return any(mount == target or queue.is_within(mount, target) for mount in mounts)
    return os.path.ismount(target)


def _tree_size(target: Path, device: int, mounts: frozenset[Path]) -> int:
    total = 0
    pending = [target]
    while pending:
        path = pending.pop()
        try:
            details = path.lstat()
        except OSError as exc:
            raise _UnsafeTarget(f"cannot inspect {path}: {exc}") from None
        if details.st_dev != device:
            raise _UnsafeTarget(f"entry crosses the queue filesystem: {path}")
        if stat.S_ISDIR(details.st_mode):
            if _mount_at_or_below(path, mounts):
                raise _UnsafeTarget(f"entry is at or below a mount point: {path}")
            try:
                with os.scandir(path) as entries:
                    pending.extend(Path(entry.path) for entry in entries)
            except OSError as exc:
                raise _UnsafeTarget(f"cannot inspect {path}: {exc}") from None
        elif not (stat.S_ISREG(details.st_mode) or stat.S_ISLNK(details.st_mode)):
            raise _UnsafeTarget(f"entry has an unsupported file type: {path}")
        total += details.st_size
    return total


def _skipped(state: str, path: Path, reason: str, size: int = 0) -> JobDecision:
    return JobDecision(state, path.name, path, size, "skipped", reason)


def _active_copy_exists(root: Path, job_id: str) -> bool:
    return any(os.path.lexists(root / state / job_id) for state in ("queued", "running"))


def _finished_at(path: Path, state: str, device: int) -> tuple[str, dt.datetime]:
    try:
        manifest, _references = _read_manifest(
            path,
            device,
            context="completed job manifest",
            required=True,
        )
    except queue.QueueError as exc:
        raise _UnsafeTarget(str(exc)) from None
    if manifest is None or manifest.get("id") != path.name or manifest.get("state") != state:
        raise _UnsafeTarget("manifest identity or state does not match its directory")
    value = manifest.get("finished_at")
    if not isinstance(value, str):
        raise _UnsafeTarget("manifest has no finished_at timestamp")
    try:
        finished = queue.parse_timestamp(value)
        if finished.tzinfo is None or finished.utcoffset() is None:
            raise ValueError("timezone is missing")
        return value, finished.astimezone(dt.timezone.utc)
    except (TypeError, ValueError, OverflowError) as exc:
        raise _UnsafeTarget(f"manifest finished_at is invalid: {exc}") from None


def _classify_job(
    root: Path,
    state: str,
    path: Path,
    protected: frozenset[str],
    now: dt.datetime,
    mounts: frozenset[Path],
) -> JobDecision:
    try:
        details = path.lstat()
    except FileNotFoundError:
        return _skipped(state, path, "entry disappeared during inspection")
    except OSError as exc:
        return _skipped(state, path, f"cannot inspect entry: {exc}")
    if not queue.JOB_ID_RE.fullmatch(path.name):
        return _skipped(state, path, "entry name is not a job ID", details.st_size)
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
        return _skipped(state, path, "job entry is not a real directory", details.st_size)
    device = root.stat().st_dev
    if details.st_dev != device:
        return _skipped(state, path, "job entry is on a different filesystem", details.st_size)
    try:
        size = _tree_size(path, device, mounts)
    except _UnsafeTarget as exc:
        return _skipped(state, path, str(exc), details.st_size)
    try:
        finished_at, finished = _finished_at(path, state, device)
    except _UnsafeTarget as exc:
        return _skipped(state, path, str(exc), size)
    if path.name in protected:
        return JobDecision(
            state,
            path.name,
            path,
            size,
            "protected",
            "citation closure",
            finished_at,
        )
    if _active_copy_exists(root, path.name):
        return _skipped(state, path, "job ID is also present in queued or running", size)
    age_limit = dt.timedelta(days=RETENTION_DAYS[state])
    if now - finished <= age_limit:
        return JobDecision(
            state,
            path.name,
            path,
            size,
            "kept-young",
            "inside retention window",
            finished_at,
        )
    return JobDecision(
        state,
        path.name,
        path,
        size,
        "eligible",
        "older than retention window",
        finished_at,
        identity=(details.st_dev, details.st_ino),
    )


def _inventory(
    root: Path,
    protected: frozenset[str],
    now: dt.datetime,
    mounts: frozenset[Path],
) -> list[JobDecision]:
    decisions: list[JobDecision] = []
    for state in RETENTION_STATES:
        state_root = root / state
        try:
            entries = sorted(state_root.iterdir(), key=lambda item: os.fsencode(item.name))
        except OSError as exc:
            raise queue.QueueError(f"cannot inspect queue state {state_root}: {exc}") from None
        decisions.extend(
            _classify_job(root, state, path, protected, now, mounts)
            for path in entries
        )
    return decisions


def _summary(decisions: Iterable[JobDecision]) -> dict[str, object]:
    categories = {name: {"count": 0, "bytes": 0} for name in CATEGORIES}
    by_state = {
        state: {name: {"count": 0, "bytes": 0} for name in CATEGORIES}
        for state in RETENTION_STATES
    }
    for item in decisions:
        for bucket in (categories[item.category], by_state[item.state][item.category]):
            bucket["count"] += 1
            bucket["bytes"] += item.size
    return {"categories": categories, "by_state": by_state}


def _remove_open_directory(descriptor: int, device: int) -> None:
    for name in os.listdir(descriptor):
        details = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
        if details.st_dev != device:
            raise _UnsafeTarget(f"entry crosses the queue filesystem: {name}")
        if stat.S_ISDIR(details.st_mode):
            flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
            child = os.open(name, flags, dir_fd=descriptor)
            try:
                opened = os.fstat(child)
                if (opened.st_dev, opened.st_ino) != (details.st_dev, details.st_ino):
                    raise _UnsafeTarget(f"directory changed while opening: {name}")
                _remove_open_directory(child, device)
            finally:
                os.close(child)
            os.rmdir(name, dir_fd=descriptor)
        elif stat.S_ISREG(details.st_mode) or stat.S_ISLNK(details.st_mode):
            os.unlink(name, dir_fd=descriptor)
        else:
            raise _UnsafeTarget(f"entry has an unsupported file type: {name}")


def _delete_tree(
    root: Path,
    state: str,
    job_id: str,
    mounts: frozenset[Path],
    expected_identity: tuple[int, int],
) -> None:
    target = root / state / job_id
    device = root.stat().st_dev
    if _active_copy_exists(root, job_id):
        raise _UnsafeTarget(f"job {job_id} is also present in queued or running")
    if _mount_at_or_below(target, mounts):
        raise _UnsafeTarget(f"job {job_id} is at or above a mount point")
    flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
    state_descriptor = os.open(root / state, flags)
    try:
        details = os.stat(job_id, dir_fd=state_descriptor, follow_symlinks=False)
        if not stat.S_ISDIR(details.st_mode) or details.st_dev != device:
            raise _UnsafeTarget(f"job {job_id} is not a same-filesystem real directory")
        if (details.st_dev, details.st_ino) != expected_identity:
            raise _UnsafeTarget(f"job {job_id} directory changed since eligibility scan")
        target_descriptor = os.open(job_id, flags, dir_fd=state_descriptor)
        try:
            opened = os.fstat(target_descriptor)
            if (opened.st_dev, opened.st_ino) != expected_identity:
                raise _UnsafeTarget(f"job {job_id} directory changed while opening")
            _remove_open_directory(target_descriptor, device)
        finally:
            os.close(target_descriptor)
        os.rmdir(job_id, dir_fd=state_descriptor)
    finally:
        os.close(state_descriptor)


def _payload(
    *,
    apply: bool,
    citations_root: Path,
    now: dt.datetime,
    scan: CitationScan,
    citation_scans: int,
    protected: frozenset[str],
    decisions: list[JobDecision],
    deleted: list[str],
) -> dict[str, object]:
    eligible = [item.job_id for item in decisions if item.category == "eligible"]
    skipped = [
        {"id": item.job_id, "state": item.state, "reason": item.reason}
        for item in decisions
        if item.category == "skipped"
    ]
    return {
        "apply": apply,
        "citations_root": str(citations_root),
        "now": now.isoformat(),
        "citation_scans": citation_scans,
        "citation_files_scanned": scan.files_scanned,
        "direct_citation_ids": len(scan.job_ids),
        "protected_ids": len(protected),
        "summary": _summary(decisions),
        "eligible_ids": eligible,
        "deleted_ids": deleted,
        "skipped": skipped,
    }


def _merge_scans(first: CitationScan, second: CitationScan) -> CitationScan:
    return CitationScan(
        frozenset(first.job_ids | second.job_ids),
        max(first.files_scanned, second.files_scanned),
    )


def _protected_set(root: Path, scan: CitationScan) -> frozenset[str]:
    seeds = frozenset(scan.job_ids | active_dependency_ids(root))
    return protected_job_ids(root, seeds)


def _inspect(
    root: Path,
    citations_root: Path,
    now: dt.datetime,
) -> tuple[CitationScan, frozenset[str], list[JobDecision]]:
    scan = scan_citations(citations_root)
    protected = _protected_set(root, scan)
    decisions = _inventory(root, protected, now, _linux_mount_points())
    return scan, protected, decisions


@contextmanager
def _submission_gate() -> Iterator[str]:
    script_root = upgrade_gate.DEFAULT_SCRIPT_ROOT
    try:
        existing = upgrade_gate.upgrade_status(script_root)
    except RuntimeError as exc:
        raise queue.QueueError(f"cannot verify upgrade fence: {exc}") from None
    if existing is not None:
        owner = existing.get("owner") or "unknown installer"
        raise queue.QueueError(f"cannot prune while compute upgrade is active for {owner}")
    owner = f"installer-{str(uuid.uuid4()).lower()}"
    gate = Path(upgrade_gate.__file__).resolve()
    try:
        try:
            upgrade_gate.acquire_submission_gate(
                gate,
                owner,
                script_root=script_root,
            )
        except RuntimeError as exc:
            raise queue.QueueError(f"cannot acquire submission gate: {exc}") from None
        deadline = time.monotonic() + SUBMITTER_DRAIN_TIMEOUT
        while True:
            try:
                submitters = upgrade_gate.active_submitter_count()
            except RuntimeError as exc:
                raise queue.QueueError(f"cannot drain active submissions: {exc}") from None
            if submitters == 0:
                break
            if time.monotonic() >= deadline:
                raise queue.QueueError("active submissions did not drain before prune timeout")
            time.sleep(SUBMITTER_POLL_INTERVAL)
        yield owner
    finally:
        _release_submission_gate(owner, script_root)


def _release_submission_gate(owner: str, script_root: Path) -> None:
    # Acquisition writes the owner record first and can publish the fence, then
    # still fail or be interrupted before returning. The owner is unique to this
    # run, so any record naming it is ours to restore; anything else is left alone.
    try:
        status = upgrade_gate.upgrade_status(script_root)
        if status is None or status.get("owner") != owner:
            return
        upgrade_gate.restore_submission_cli(owner, script_root=script_root)
    except RuntimeError as exc:
        raise queue.QueueError(f"cannot restore submission gate: {exc}") from None


def _require_apply_allowed(root: Path, gate_owner: str) -> None:
    maintenance = queue.maintenance_status(root)
    if maintenance is not None:
        raise queue.QueueError(
            f"cannot prune while queue maintenance is active for {maintenance.get('owner')}"
        )
    try:
        upgrading = upgrade_gate.upgrade_status()
    except RuntimeError as exc:
        raise queue.QueueError(f"cannot verify upgrade fence: {exc}") from None
    if upgrading is not None and upgrading.get("owner") != gate_owner:
        owner = upgrading.get("owner") or "unknown installer"
        raise queue.QueueError(f"cannot prune while compute upgrade is active for {owner}")


def _apply_second_scan(
    root: Path,
    citations_root: Path,
    first: CitationScan,
    decisions: list[JobDecision],
) -> tuple[CitationScan, frozenset[str], list[JobDecision]]:
    merged = _merge_scans(first, scan_citations(citations_root))
    protected = _protected_set(root, merged)
    revised = [
        replace(item, category="protected", reason="citation closure")
        if item.category == "eligible" and item.job_id in protected
        else item
        for item in decisions
    ]
    return merged, protected, revised


def _delete_eligible(
    root: Path,
    protected: frozenset[str],
    now: dt.datetime,
    decisions: list[JobDecision],
    gate_owner: str,
) -> tuple[list[JobDecision], list[str]]:
    final: list[JobDecision] = []
    deleted: list[str] = []
    for decision in decisions:
        if decision.category != "eligible":
            final.append(decision)
            continue
        current = _classify_job(
            root,
            decision.state,
            decision.path,
            protected,
            now,
            _linux_mount_points(),
        )
        if current.category != "eligible":
            final.append(current)
            continue
        if decision.identity is None or current.identity != decision.identity:
            final.append(
                _skipped(
                    decision.state,
                    decision.path,
                    "job directory changed since eligibility scan",
                    current.size,
                )
            )
            continue
        _require_apply_allowed(root, gate_owner)
        try:
            _delete_tree(
                root,
                current.state,
                current.job_id,
                _linux_mount_points(),
                decision.identity,
            )
        except _UnsafeTarget as exc:
            final.append(_skipped(current.state, current.path, str(exc), current.size))
            continue
        final.append(current)
        deleted.append(current.job_id)
    return final, deleted


def prune_from_args(root: Path, args: argparse.Namespace) -> dict[str, object]:
    return prune_jobs(
        root,
        args.citations_root,
        apply=args.apply,
        now_text=args.now,
    )


def run_prune_command(
    root: Path,
    args: argparse.Namespace,
    printer: Callable[[object], None],
) -> int:
    printer(prune_from_args(root, args))
    return 0


def prune_jobs(
    root: Path,
    citations_root: Path,
    *,
    apply: bool = False,
    now_text: str | None = None,
) -> dict[str, object]:
    now = parse_now(now_text)
    citations_root = citations_root.expanduser().resolve()

    if not apply:
        scan, protected, decisions = _inspect(root, citations_root, now)
        return _payload(
            apply=False,
            citations_root=citations_root,
            now=now,
            scan=scan,
            citation_scans=1,
            protected=protected,
            decisions=decisions,
            deleted=[],
        )

    with _submission_gate() as gate_owner:
        with queue.queue_lock(root):
            _require_apply_allowed(root, gate_owner)
            first, _initial_protected, decisions = _inspect(
                root,
                citations_root,
                now,
            )
            scan, protected, decisions = _apply_second_scan(
                root,
                citations_root,
                first,
                decisions,
            )
            final, deleted = _delete_eligible(
                root,
                protected,
                now,
                decisions,
                gate_owner,
            )
            return _payload(
                apply=True,
                citations_root=citations_root,
                now=now,
                scan=scan,
                citation_scans=2,
                protected=protected,
                decisions=final,
                deleted=deleted,
            )
