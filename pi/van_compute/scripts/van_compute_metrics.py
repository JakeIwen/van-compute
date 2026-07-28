#!/usr/bin/env python3
"""Read-only aggregation of van-compute queue and resource telemetry."""

from __future__ import annotations

import datetime as dt
import hashlib
import heapq
import json
import math
import os
from pathlib import Path
import re
import stat
import time
from typing import Callable


DEFAULT_QUEUE_ROOT = Path("/home/pi/dev/obd-things/tmp/compute")
# Keep telemetry readable if a queue protocol rollout is interrupted. A unit
# test pins this value to van_compute_protocol.WORKER_PROTOCOL_VERSION.
WORKER_PROTOCOL_VERSION = 2
STATE_DIRECTORIES = ("queued", "running", "done", "failed")
JOB_ID_RE = re.compile(r"\d{8}T\d{6}Z-[0-9a-f]{8}")
# Keep task queries aligned with van_compute_protocol._NAME_RE without
# importing the executable protocol surface into the read-only metrics reader.
TASK_NAME_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,63}")
MAX_JSON_BYTES = 2 * 1024 * 1024
MAX_SCANNED_JOBS = 2000
MAX_RECENT_JOBS = 50
MAX_JOB_DETAIL_TEXT_BYTES = 32 * 1024
MAX_DIAGNOSTIC_TEXT = 4096
MAX_MISSED_EVENT_BYTES = 64 * 1024
MAX_SCANNED_MISSED_EVENTS = 2000
MAX_RECENT_MISSED_EVENTS = 50
LOCAL_WORKER_PREFIXES = ("vanpi-local", "pi-local")
SHA256_RE = re.compile(r"[0-9a-fA-F]{64}")


class ComputeMetricsError(RuntimeError):
    pass


def _finite_number(value, default=0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _nonnegative_number(value, default=0.0) -> float:
    return max(0.0, _finite_number(value, default))


def _nonnegative_integer(value, default=0) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    return max(0, number)


def _timestamp(value) -> float | None:
    if isinstance(value, (int, float)) and math.isfinite(value):
        return float(value)
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.timestamp()


def _read_json(path: Path, *, max_bytes: int = MAX_JSON_BYTES) -> dict[str, object]:
    try:
        info = path.lstat()
    except OSError as exc:
        raise ComputeMetricsError(f"cannot inspect {path}: {exc}") from None
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise ComputeMetricsError(f"refusing non-regular JSON file: {path}")
    if info.st_size > max_bytes:
        raise ComputeMetricsError(f"JSON file is unexpectedly large: {path}")
    try:
        with path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ComputeMetricsError(f"cannot read {path}: {exc}") from None
    if not isinstance(payload, dict):
        raise ComputeMetricsError(f"JSON file is not an object: {path}")
    return payload


def _open_regular_at(directory: Path, name: str) -> tuple[int, int, os.stat_result]:
    """Open one real file relative to a real directory and bind both inodes."""
    directory_flags = os.O_RDONLY
    directory_flags |= getattr(os, "O_CLOEXEC", 0)
    directory_flags |= getattr(os, "O_DIRECTORY", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        directory_descriptor = os.open(directory, directory_flags)
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise ComputeMetricsError(f"cannot inspect {directory}: {exc}") from None
    descriptor: int | None = None
    try:
        directory_info = os.fstat(directory_descriptor)
        if not stat.S_ISDIR(directory_info.st_mode):
            raise ComputeMetricsError(f"refusing non-directory path: {directory}")
        file_flags = os.O_RDONLY
        file_flags |= getattr(os, "O_CLOEXEC", 0)
        file_flags |= getattr(os, "O_NOFOLLOW", 0)
        file_flags |= getattr(os, "O_NONBLOCK", 0)
        descriptor = os.open(name, file_flags, dir_fd=directory_descriptor)
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise ComputeMetricsError(
                f"refusing non-regular result file: {directory / name}"
            )
    except FileNotFoundError:
        if descriptor is not None:
            os.close(descriptor)
        os.close(directory_descriptor)
        raise
    except OSError as exc:
        if descriptor is not None:
            os.close(descriptor)
        os.close(directory_descriptor)
        raise ComputeMetricsError(
            f"cannot inspect {directory / name}: {exc}"
        ) from None
    except ComputeMetricsError:
        if descriptor is not None:
            os.close(descriptor)
        os.close(directory_descriptor)
        raise
    assert descriptor is not None
    return directory_descriptor, descriptor, info


def _read_json_at(
    directory: Path, name: str, *, max_bytes: int = MAX_JSON_BYTES
) -> dict[str, object]:
    directory_descriptor, descriptor, info = _open_regular_at(directory, name)
    try:
        if info.st_size > max_bytes:
            raise ComputeMetricsError(
                f"JSON file is unexpectedly large: {directory / name}"
            )
        chunks: list[bytes] = []
        remaining = info.st_size
        while remaining:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        try:
            payload = json.loads(b"".join(chunks).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ComputeMetricsError(
                f"cannot read {directory / name}: {exc}"
            ) from None
    except OSError as exc:
        raise ComputeMetricsError(
            f"cannot read {directory / name}: {exc}"
        ) from None
    finally:
        os.close(descriptor)
        os.close(directory_descriptor)
    if not isinstance(payload, dict):
        raise ComputeMetricsError(
            f"JSON file is not an object: {directory / name}"
        )
    return payload


def _sum_sizes(items) -> int:
    if not isinstance(items, list):
        return 0
    return sum(
        _nonnegative_integer(item.get("size"))
        for item in items
        if isinstance(item, dict)
    )


def _duration(start, finish) -> float | None:
    start_timestamp = _timestamp(start)
    finish_timestamp = _timestamp(finish)
    if start_timestamp is None or finish_timestamp is None:
        return None
    return max(0.0, finish_timestamp - start_timestamp)


def _optional_nonnegative_integer(value) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return max(0, number)


def _optional_nonnegative_number(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return max(0.0, number)


def _bounded_text(value, default: str, maximum: int) -> str:
    if not isinstance(value, str):
        return default
    # Event writers already sanitize labels, but flatten control characters here
    # too so a malformed hand-written record cannot disrupt the dashboard.
    cleaned = " ".join(value.split())
    return (cleaned or default)[:maximum]


def _diagnostic_text(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value[:MAX_DIAGNOSTIC_TEXT]


def _failure_classification(
    state: str,
    exit_code: object,
    execution: dict[str, object],
    *,
    execution_available: bool,
) -> str | None:
    """Classify failures from explicit worker markers, never numeric codes."""
    if state != "failed":
        return None
    if (
        _diagnostic_text(execution.get("worker_error"))
        or _diagnostic_text(execution.get("resource_monitor_error"))
        or execution.get("interrupted") is True
    ):
        return "infrastructure"
    if _diagnostic_text(execution.get("resource_limit")):
        return "resource"
    if execution.get("timed_out") is True:
        return "timeout"
    if (
        execution_available
        and isinstance(exit_code, int)
        and not isinstance(exit_code, bool)
        and exit_code != 0
    ):
        return "task"
    return "unknown"


def _failure_summary(classification: str | None, exit_code: object) -> str | None:
    if classification == "infrastructure":
        return "Worker or infrastructure failure"
    if classification == "resource":
        return "Resource limit stopped execution"
    if classification == "timeout":
        return "Execution exceeded its time limit"
    if classification == "task":
        return f"Task exited with status {exit_code}"
    if classification == "unknown":
        return "Failure reason unavailable"
    return None


def _stream_excerpt(result_root: Path, name: str) -> dict[str, object]:
    """Return a bounded tail from one real result file without following links."""
    unavailable = {
        "available": False,
        "bytes": 0,
        "excerpt": "",
        "truncated": False,
        "excerpt_from": "none",
    }
    try:
        directory_descriptor, descriptor, info = _open_regular_at(result_root, name)
    except (OSError, ComputeMetricsError):
        return unavailable
    try:
        offset = max(0, info.st_size - MAX_JOB_DETAIL_TEXT_BYTES)
        os.lseek(descriptor, offset, os.SEEK_SET)
        remaining = MAX_JOB_DETAIL_TEXT_BYTES
        chunks: list[bytes] = []
        while remaining:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
    except OSError:
        return unavailable
    finally:
        os.close(descriptor)
        os.close(directory_descriptor)
    return {
        "available": True,
        "bytes": info.st_size,
        "excerpt": b"".join(chunks).decode("utf-8", "replace"),
        "truncated": offset > 0,
        "excerpt_from": "tail" if offset > 0 else "full",
    }


def _placement(value: object, worker: object = None, *, default: str = "remote") -> str:
    """Normalize placement while remaining compatible with older manifests."""
    if value in {"remote", "pi-local"}:
        return str(value)
    worker_name = str(worker or "")
    if _is_local_worker(worker_name):
        return "pi-local"
    return default


def _is_local_worker(worker: str) -> bool:
    return any(
        worker == prefix or worker.startswith(f"{prefix}.")
        for prefix in LOCAL_WORKER_PREFIXES
    )


def _workload_fingerprint(manifest: dict[str, object]) -> str | None:
    """Identify an exact snapshotted workload without exposing its details."""
    task = manifest.get("task")
    arguments = manifest.get("arguments", [])
    sources = manifest.get("sources")
    inputs = manifest.get("inputs")
    execution = manifest.get("execution")
    if (
        not isinstance(task, str)
        or not task
        or not isinstance(arguments, list)
        or not all(isinstance(argument, str) for argument in arguments)
        or not isinstance(sources, list)
        or not isinstance(inputs, list)
        or (not sources and not inputs)
    ):
        return None
    if execution is not None:
        if not isinstance(execution, dict):
            return None
        datasets = execution.get("datasets")
        # A dataset alias does not fingerprint the private corpus content, so a
        # source/input match cannot prove that the two executions saw the same
        # workload.
        if not isinstance(datasets, list) or datasets:
            return None

    canonical_sources: list[dict[str, object]] = []
    source_paths: set[str] = set()
    for record in sources:
        if not isinstance(record, dict):
            return None
        path = record.get("path")
        size = record.get("size")
        digest = record.get("sha256")
        if (
            not isinstance(path, str)
            or not path
            or path in source_paths
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
            or not isinstance(digest, str)
            or not SHA256_RE.fullmatch(digest)
        ):
            return None
        source_paths.add(path)
        canonical_sources.append(
            {"path": path, "size": size, "sha256": digest.lower()}
        )
    canonical_sources.sort(key=lambda record: str(record["path"]))

    canonical_inputs: list[dict[str, object]] = []
    input_indexes: set[int] = set()
    for record in inputs:
        if not isinstance(record, dict):
            return None
        index = record.get("index")
        name = record.get("name")
        size = record.get("size")
        digest = record.get("sha256")
        value = record.get("value")
        if (
            isinstance(index, bool)
            or not isinstance(index, int)
            or index < 0
            or index in input_indexes
            or not isinstance(name, str)
            or not name
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
            or not isinstance(digest, str)
            or not SHA256_RE.fullmatch(digest)
            or (value is not None and not isinstance(value, str))
        ):
            return None
        input_indexes.add(index)
        canonical_inputs.append(
            {
                "index": index,
                "name": name,
                "size": size,
                "sha256": digest.lower(),
                "value": value,
            }
        )
    canonical_inputs.sort(key=lambda record: int(record["index"]))
    if [record["index"] for record in canonical_inputs] != list(
        range(len(canonical_inputs))
    ):
        return None

    payload = {
        "schema_version": manifest.get("schema_version"),
        "task": task,
        "arguments": arguments,
        "execution": execution,
        "sources": canonical_sources,
        "inputs": canonical_inputs,
    }
    try:
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError):
        return None
    return hashlib.sha256(encoded).hexdigest()


def _compatible_worker_protocol(version: object) -> bool:
    return (
        isinstance(version, int)
        and not isinstance(version, bool)
        and version == WORKER_PROTOCOL_VERSION
    )


class ComputeMetricsReader:
    """Summarize queue manifests without executing the queue CLI or a job."""

    def __init__(
        self,
        root: str | os.PathLike[str] = DEFAULT_QUEUE_ROOT,
        *,
        clock: Callable[[], float] = time.time,
        heartbeat_max_age: float = 45.0,
    ) -> None:
        self.root = Path(root)
        self.clock = clock
        self.heartbeat_max_age = heartbeat_max_age

    def workers(self, now: float) -> list[dict[str, object]]:
        directory = self.root / "workers"
        if not directory.is_dir() or directory.is_symlink():
            return []
        workers: list[dict[str, object]] = []
        for path in sorted(directory.glob("*.json"))[:100]:
            try:
                payload = _read_json(path)
            except ComputeMetricsError:
                continue
            seen_at = _timestamp(payload.get("seen_at"))
            age = None if seen_at is None else max(0.0, now - seen_at)
            slots_total = _optional_nonnegative_integer(payload.get("slots_total"))
            slots_busy = _optional_nonnegative_integer(payload.get("slots_busy"))
            if slots_total is not None and slots_busy is not None:
                slots_busy = min(slots_busy, slots_total)
            protocol_version = payload.get("protocol_version")
            workers.append(
                {
                    "worker": str(payload.get("worker") or path.stem)[:64],
                    "placement": _placement(
                        payload.get("placement"), payload.get("worker") or path.stem
                    ),
                    "seen_at": seen_at,
                    "age_seconds": round(age, 3) if age is not None else None,
                    "available": (
                        age is not None
                        and age <= self.heartbeat_max_age
                        and _compatible_worker_protocol(protocol_version)
                    ),
                    "protocol_version": protocol_version,
                    "slots_total": slots_total,
                    "slots_busy": slots_busy,
                    "slots_available": (
                        slots_total - slots_busy
                        if slots_total is not None and slots_busy is not None
                        else None
                    ),
                }
            )
        return workers

    def _missed_events(self) -> list[dict[str, object]]:
        directory = self.root / "missed"
        if not directory.is_dir() or directory.is_symlink():
            return []
        events: list[dict[str, object]] = []
        for path in sorted(directory.glob("*.json"), reverse=True)[:MAX_SCANNED_MISSED_EVENTS]:
            try:
                payload = _read_json(path, max_bytes=MAX_MISSED_EVENT_BYTES)
            except ComputeMetricsError:
                continue
            recorded_at = _timestamp(payload.get("recorded_at"))
            if recorded_at is None:
                continue
            events.append(
                {
                    "id": _bounded_text(payload.get("id"), path.stem, 100),
                    "recorded_at": recorded_at,
                    "command_category": _bounded_text(
                        payload.get("profile"), "uncategorized", 80
                    ),
                    "label": _bounded_text(payload.get("label"), "Eligible local work", 160),
                    "reason": _bounded_text(payload.get("reason"), "other", 80),
                    "wall_seconds": round(
                        _nonnegative_number(payload.get("duration_seconds")), 6
                    ),
                    "cpu_seconds": round(
                        _nonnegative_number(payload.get("cpu_seconds")), 6
                    ),
                    "peak_rss_bytes": _nonnegative_integer(payload.get("peak_rss_bytes")),
                    "input_bytes": _nonnegative_integer(payload.get("input_bytes")),
                }
            )
        return events

    @staticmethod
    def _local_work_summary(events: list[dict[str, object]]) -> dict[str, object]:
        categories: dict[str, dict[str, object]] = {}
        reasons: dict[str, dict[str, object]] = {}
        for event in events:
            category = categories.setdefault(
                event["command_category"],
                {
                    "command_category": event["command_category"],
                    "events": 0,
                    "wall_seconds": 0.0,
                    "cpu_seconds": 0.0,
                    "peak_rss_bytes": 0,
                    "input_bytes": 0,
                },
            )
            reason = reasons.setdefault(
                event["reason"],
                {
                    "reason": event["reason"],
                    "events": 0,
                    "wall_seconds": 0.0,
                    "cpu_seconds": 0.0,
                    "peak_rss_bytes": 0,
                    "input_bytes": 0,
                },
            )
            category["events"] += 1
            category["wall_seconds"] += event["wall_seconds"]
            category["cpu_seconds"] += event["cpu_seconds"]
            category["peak_rss_bytes"] = max(
                category["peak_rss_bytes"], event["peak_rss_bytes"]
            )
            category["input_bytes"] += event["input_bytes"]
            reason["events"] += 1
            reason["wall_seconds"] += event["wall_seconds"]
            reason["cpu_seconds"] += event["cpu_seconds"]
            reason["peak_rss_bytes"] = max(reason["peak_rss_bytes"], event["peak_rss_bytes"])
            reason["input_bytes"] += event["input_bytes"]
        category_rows = sorted(
            categories.values(),
            key=lambda item: (-item["cpu_seconds"], -item["wall_seconds"], item["command_category"]),
        )
        for category in category_rows:
            category["wall_seconds"] = round(category["wall_seconds"], 6)
            category["cpu_seconds"] = round(category["cpu_seconds"], 6)
        reason_rows = sorted(
            reasons.values(), key=lambda item: (-item["events"], -item["cpu_seconds"], item["reason"])
        )
        for reason in reason_rows:
            reason["wall_seconds"] = round(reason["wall_seconds"], 6)
            reason["cpu_seconds"] = round(reason["cpu_seconds"], 6)
        return {
            "events": len(events),
            "wall_seconds": round(sum(event["wall_seconds"] for event in events), 6),
            "cpu_seconds": round(sum(event["cpu_seconds"] for event in events), 6),
            "peak_rss_bytes": max((event["peak_rss_bytes"] for event in events), default=0),
            "input_bytes": sum(event["input_bytes"] for event in events),
            "categories": category_rows,
            "reasons": reason_rows,
            "recent": sorted(events, key=lambda event: event["recorded_at"], reverse=True)[
                :MAX_RECENT_MISSED_EVENTS
            ],
        }

    def _job_paths(self) -> list[tuple[str, Path]]:
        current: list[tuple[str, Path]] = []
        for state in ("queued", "running"):
            directory = self.root / state
            if not directory.is_dir() or directory.is_symlink():
                continue
            for path in directory.iterdir():
                if path.is_dir() and not path.is_symlink() and not path.name.startswith("."):
                    current.append((state, path))
                    if len(current) > MAX_SCANNED_JOBS:
                        raise ComputeMetricsError(
                            "current compute jobs exceed the dashboard scan limit of "
                            f"{MAX_SCANNED_JOBS}"
                        )
        current.sort(key=lambda item: (item[1].name, item[0]), reverse=True)
        remaining = MAX_SCANNED_JOBS - len(current)
        if remaining == 0:
            return current

        def completed_paths():
            for state in ("done", "failed"):
                directory = self.root / state
                if not directory.is_dir() or directory.is_symlink():
                    continue
                for path in directory.iterdir():
                    if (
                        path.is_dir()
                        and not path.is_symlink()
                        and not path.name.startswith(".")
                    ):
                        yield state, path

        # Current work is correctness-critical, while completed history is a
        # bounded dashboard sample. Keep only the newest completion paths in
        # memory and allocate them whatever scan budget current work leaves.
        completed = heapq.nlargest(
            remaining,
            completed_paths(),
            key=lambda item: (item[1].name, item[0]),
        )
        return [*current, *completed]

    def _job(self, state: str, path: Path) -> dict[str, object] | None:
        try:
            manifest = _read_json(path / "manifest.json")
        except ComputeMetricsError:
            return None
        job_id = path.name
        task = str(manifest.get("task") or "unknown")[:100]
        submitted_at = _timestamp(manifest.get("submitted_at"))
        started_at = _timestamp(manifest.get("started_at"))
        finished_at = _timestamp(manifest.get("finished_at"))
        execution: dict[str, object] = {}
        execution_available = False
        result_root = path / "result"
        try:
            execution = _read_json_at(result_root, "execution.json")
            execution_available = True
        except (FileNotFoundError, ComputeMetricsError):
            execution = {}
        job_state = state
        exit_code = manifest.get("exit_code")
        failure_classification = _failure_classification(
            job_state,
            exit_code,
            execution,
            execution_available=execution_available,
        )
        usage = execution.get("resource_usage")
        usage = usage if isinstance(usage, dict) else {}
        timing = execution.get("timing")
        timing = timing if isinstance(timing, dict) else {}
        raw_analysis_seconds = _optional_nonnegative_number(
            timing.get("analysis_seconds", execution.get("duration_seconds"))
        )
        if raw_analysis_seconds is None:
            derived = _duration(manifest.get("started_at"), manifest.get("finished_at"))
            analysis_seconds = derived if derived is not None else 0.0
        else:
            analysis_seconds = raw_analysis_seconds
        active_seconds = _optional_nonnegative_number(
            timing.get("worker_attempt_active_seconds")
        )
        detailed_timing = active_seconds is not None
        if active_seconds is None:
            active_seconds = analysis_seconds
        preparation_seconds = _optional_nonnegative_number(
            timing.get("source_input_preparation_seconds")
        )
        packaging_seconds = _optional_nonnegative_number(timing.get("packaging_seconds"))
        upload_seconds = _optional_nonnegative_number(
            timing.get("result_upload_seconds_excluding_execution_json")
        )
        raw_cpu_seconds = _optional_nonnegative_number(usage.get("cpu_seconds"))
        cpu_seconds = raw_cpu_seconds if raw_cpu_seconds is not None else 0.0
        average_cpu = usage.get("average_cpu_percent")
        average_cpu = (
            _nonnegative_number(average_cpu)
            if average_cpu is not None
            else (100 * cpu_seconds / analysis_seconds if analysis_seconds > 0 else None)
        )
        leader_peak_rss = _nonnegative_integer(usage.get("peak_rss_bytes"))
        sampled_group_peak_rss = _nonnegative_integer(
            usage.get("peak_process_group_rss_bytes")
        )
        peak_rss = max(leader_peak_rss, sampled_group_peak_rss)
        worker = manifest.get("worker") or execution.get("worker")
        placement = _placement(
            execution.get("placement", manifest.get("placement")),
            worker,
            default="pending" if state == "queued" else "remote",
        )
        return {
            "id": job_id,
            "task": task,
            "state": job_state,
            "worker": worker,
            "placement": placement,
            "exit_code": exit_code,
            "failure_classification": failure_classification,
            "failure_summary": _failure_summary(
                failure_classification, exit_code
            ),
            "details_available": bool(JOB_ID_RE.fullmatch(job_id)),
            "submitted_at": submitted_at,
            "started_at": started_at,
            "finished_at": finished_at,
            "queue_seconds": _duration(manifest.get("submitted_at"), manifest.get("started_at")),
            # Keep the established field name for API compatibility, but for
            # current workers it is the whole active attempt (staging through
            # non-telemetry uploads), not merely child-process runtime.
            "wall_seconds": round(active_seconds, 6),
            "active_seconds": round(active_seconds, 6),
            "analysis_seconds": round(analysis_seconds, 6),
            "preparation_seconds": round(preparation_seconds or 0.0, 6),
            "packaging_seconds": round(packaging_seconds or 0.0, 6),
            "result_upload_seconds": round(upload_seconds or 0.0, 6),
            "detailed_timing": detailed_timing,
            "cpu_seconds": round(cpu_seconds, 6),
            "average_cpu_percent": round(average_cpu, 2) if average_cpu is not None else None,
            "peak_rss_bytes": peak_rss,
            "leader_peak_rss_bytes": leader_peak_rss,
            "sampled_process_group_peak_rss_bytes": sampled_group_peak_rss,
            "input_bytes": _sum_sizes(manifest.get("inputs")),
            "source_bytes": _sum_sizes(manifest.get("sources")),
            "result_bytes": _sum_sizes(manifest.get("results")),
            "telemetry": bool(usage),
            "timed_out": execution.get("timed_out") is True,
            "interrupted": execution.get("interrupted") is True,
            # This opaque key is used only while aggregating the report and is
            # removed before any job is returned through the dashboard API.
            "_workload_fingerprint": _workload_fingerprint(manifest),
            "_benchmark_telemetry": (
                raw_analysis_seconds is not None and raw_cpu_seconds is not None
            ),
        }

    def _job_location(self, job_id: str) -> tuple[str, Path]:
        if not isinstance(job_id, str) or not JOB_ID_RE.fullmatch(job_id):
            raise ValueError("invalid compute job id")
        for state in STATE_DIRECTORIES:
            state_root = self.root / state
            try:
                state_info = state_root.lstat()
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise ComputeMetricsError(
                    f"cannot inspect compute queue: {exc}"
                ) from None
            if stat.S_ISLNK(state_info.st_mode) or not stat.S_ISDIR(
                state_info.st_mode
            ):
                raise ComputeMetricsError(
                    "compute queue state path is not a real directory"
                )
            candidate = state_root / job_id
            try:
                info = candidate.lstat()
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise ComputeMetricsError(
                    f"cannot inspect compute job: {exc}"
                ) from None
            if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                raise ComputeMetricsError("compute job path is not a real directory")
            return state, candidate
        raise FileNotFoundError(job_id)

    def job_details(self, job_id: str) -> dict[str, object]:
        """Return bounded, explicitly whitelisted diagnostics for one job."""
        state, path = self._job_location(job_id)
        job = self._job(state, path)
        if job is None:
            raise ComputeMetricsError("compute job manifest is unavailable")
        result_root = path / "result"
        execution: dict[str, object] = {}
        try:
            execution = _read_json_at(result_root, "execution.json")
        except FileNotFoundError:
            execution = {}
        return {
            "ok": True,
            "job": {
                key: value
                for key, value in job.items()
                if not key.startswith("_")
            },
            "diagnostics": {
                "failure_classification": job["failure_classification"],
                "worker_error": _diagnostic_text(execution.get("worker_error")),
                "resource_limit": _diagnostic_text(execution.get("resource_limit")),
                "resource_monitor_error": _diagnostic_text(
                    execution.get("resource_monitor_error")
                ),
                "timed_out": execution.get("timed_out") is True,
                "interrupted": execution.get("interrupted") is True,
                "truncated": {
                    key: isinstance(execution.get(key), str)
                    and len(execution[key]) > MAX_DIAGNOSTIC_TEXT
                    for key in (
                        "worker_error",
                        "resource_limit",
                        "resource_monitor_error",
                    )
                },
            },
            "stderr": _stream_excerpt(result_root, "stderr.txt"),
            "stdout": _stream_excerpt(result_root, "stdout.txt"),
        }

    def jobs_for_task(self, hours: int, task: str) -> dict[str, object]:
        """Return a bounded recent-job list for one exact task name."""
        if hours not in (6, 24, 168, 720):
            raise ValueError(
                "compute metrics range must be 6, 24, 168, or 720 hours"
            )
        if (
            not isinstance(task, str)
            or not TASK_NAME_RE.fullmatch(task)
        ):
            raise ValueError(
                "compute task must use 1 to 64 lowercase letters, digits, or hyphens"
            )
        cutoff = self.clock() - hours * 3600
        matches: list[dict[str, object]] = []
        for state, path in self._job_paths():
            job = self._job(state, path)
            if job is None or job["task"] != task:
                continue
            if state in {"queued", "running"}:
                if state == "queued" or job["placement"] == "remote":
                    matches.append(job)
                continue
            if (
                job["placement"] == "remote"
                and job["finished_at"] is not None
                and job["finished_at"] >= cutoff
            ):
                matches.append(job)
        matches.sort(
            key=lambda job: (
                job["finished_at"]
                or job["started_at"]
                or job["submitted_at"]
                or 0
            ),
            reverse=True,
        )
        return {
            "ok": True,
            "range_hours": hours,
            "task": task,
            "matching_jobs": len(matches),
            "truncated": len(matches) > MAX_RECENT_JOBS,
            "jobs": [
                {
                    key: value
                    for key, value in job.items()
                    if not key.startswith("_")
                }
                for job in matches[:MAX_RECENT_JOBS]
            ],
        }

    @staticmethod
    def _benchmark_summary(completed: list[dict[str, object]]) -> dict[str, object]:
        """Calibrate matched remote work from exact-content Pi-local samples."""
        local_by_workload: dict[str, list[dict[str, object]]] = {}
        remote_by_workload: dict[str, list[dict[str, object]]] = {}
        for job in completed:
            fingerprint = job.get("_workload_fingerprint")
            exit_code = job.get("exit_code")
            if (
                job.get("state") != "done"
                or isinstance(exit_code, bool)
                or exit_code != 0
                or not job.get("_benchmark_telemetry")
                or not isinstance(fingerprint, str)
            ):
                continue
            placement = job.get("placement")
            if placement == "pi-local":
                local_by_workload.setdefault(fingerprint, []).append(job)
            elif placement == "remote":
                remote_by_workload.setdefault(fingerprint, []).append(job)

        calibrated = sorted(set(local_by_workload) & set(remote_by_workload))
        calibrated_remote_jobs = 0
        pi_samples = 0
        estimated_pi_analysis = 0.0
        estimated_pi_cpu = 0.0
        measured_mac_analysis = 0.0
        measured_mac_cpu = 0.0
        maximum_pi_peak_rss = 0
        for fingerprint in calibrated:
            local_samples = local_by_workload[fingerprint]
            remote_samples = remote_by_workload[fingerprint]
            sample_count = len(local_samples)
            remote_count = len(remote_samples)
            average_pi_analysis = sum(
                float(job["analysis_seconds"]) for job in local_samples
            ) / sample_count
            average_pi_cpu = sum(
                float(job["cpu_seconds"]) for job in local_samples
            ) / sample_count
            calibrated_remote_jobs += remote_count
            pi_samples += sample_count
            estimated_pi_analysis += average_pi_analysis * remote_count
            estimated_pi_cpu += average_pi_cpu * remote_count
            measured_mac_analysis += sum(
                float(job["analysis_seconds"]) for job in remote_samples
            )
            measured_mac_cpu += sum(
                float(job["cpu_seconds"]) for job in remote_samples
            )
            maximum_pi_peak_rss = max(
                maximum_pi_peak_rss,
                *(int(job["peak_rss_bytes"]) for job in local_samples),
            )

        return {
            "calibrated_workloads": len(calibrated),
            "calibrated_remote_jobs": calibrated_remote_jobs,
            "pi_samples": pi_samples,
            "estimated_pi_analysis_seconds_avoided": round(
                estimated_pi_analysis, 6
            ),
            "estimated_pi_cpu_seconds_avoided": round(estimated_pi_cpu, 6),
            "matched_mac_analysis_seconds": round(measured_mac_analysis, 6),
            "matched_mac_cpu_seconds": round(measured_mac_cpu, 6),
            "analysis_speedup_ratio": (
                round(estimated_pi_analysis / measured_mac_analysis, 3)
                if measured_mac_analysis > 0
                else None
            ),
            "pi_to_mac_cpu_ratio": (
                round(estimated_pi_cpu / measured_mac_cpu, 3)
                if measured_mac_cpu > 0
                else None
            ),
            "maximum_pi_peak_rss_bytes": maximum_pi_peak_rss,
        }

    def report(self, hours: int = 168) -> dict[str, object]:
        if hours not in (6, 24, 168, 720):
            raise ValueError("compute metrics range must be 6, 24, 168, or 720 hours")
        now = self.clock()
        workers = self.workers(now)
        jobs = [
            job
            for state, path in self._job_paths()
            if (job := self._job(state, path)) is not None
        ]
        current = [job for job in jobs if job["state"] in {"queued", "running"}]
        cutoff = now - hours * 3600
        completed = [
            job
            for job in jobs
            if job["state"] in {"done", "failed"}
            and job["finished_at"] is not None
            and job["finished_at"] >= cutoff
        ]
        remote_completed = [job for job in completed if job["placement"] == "remote"]
        benchmark = self._benchmark_summary(completed)
        local_events = [
            event for event in self._missed_events() if event["recorded_at"] >= cutoff
        ]
        eligible_local_work = self._local_work_summary(local_events)
        remote_current = [
            job
            for job in current
            if job["state"] == "queued" or job["placement"] == "remote"
        ]
        visible = sorted(
            [*remote_current, *remote_completed],
            key=lambda job: job["finished_at"] or job["started_at"] or job["submitted_at"] or 0,
            reverse=True,
        )
        telemetry = [job for job in remote_completed if job["telemetry"]]
        total_wall = sum(job["wall_seconds"] for job in telemetry)
        total_cpu = sum(job["cpu_seconds"] for job in telemetry)
        timing_telemetry = [job for job in telemetry if job["detailed_timing"]]
        queue_delays = [
            job["queue_seconds"]
            for job in remote_completed
            if job["queue_seconds"] is not None
        ]
        tasks: dict[str, dict[str, object]] = {}
        for job in remote_completed:
            task = tasks.setdefault(
                job["task"],
                {
                    "task": job["task"],
                    "jobs": 0,
                    "succeeded": 0,
                    "failed": 0,
                    "telemetry_jobs": 0,
                    "cpu_seconds": 0.0,
                    "wall_seconds": 0.0,
                    "input_bytes": 0,
                    "peak_rss_bytes": 0,
                },
            )
            task["jobs"] += 1
            task["succeeded" if job["state"] == "done" else "failed"] += 1
            task["telemetry_jobs"] += int(job["telemetry"])
            task["cpu_seconds"] += job["cpu_seconds"]
            task["wall_seconds"] += job["wall_seconds"]
            task["input_bytes"] += job["input_bytes"]
            task["peak_rss_bytes"] = max(task["peak_rss_bytes"], job["peak_rss_bytes"])
        task_rows = sorted(tasks.values(), key=lambda item: (-item["cpu_seconds"], item["task"]))
        for item in task_rows:
            item["cpu_seconds"] = round(item["cpu_seconds"], 6)
            item["wall_seconds"] = round(item["wall_seconds"], 6)
        summary = {
            "jobs": len(remote_completed),
            "succeeded": sum(job["state"] == "done" for job in remote_completed),
            "failed": sum(job["state"] == "failed" for job in remote_completed),
            "telemetry_jobs": len(telemetry),
            "timing_jobs": len(timing_telemetry),
            "mac_cpu_seconds": round(total_cpu, 6),
            "mac_wall_seconds": round(total_wall, 6),
            "mac_analysis_seconds": round(
                sum(job["analysis_seconds"] for job in timing_telemetry), 6
            ),
            "mac_preparation_seconds": round(
                sum(job["preparation_seconds"] for job in timing_telemetry), 6
            ),
            "mac_packaging_seconds": round(
                sum(job["packaging_seconds"] for job in timing_telemetry), 6
            ),
            "mac_result_upload_seconds": round(
                sum(job["result_upload_seconds"] for job in timing_telemetry), 6
            ),
            "aggregate_cpu_percent": round(100 * total_cpu / total_wall, 2) if total_wall > 0 else None,
            "peak_rss_bytes": max((job["peak_rss_bytes"] for job in telemetry), default=0),
            "input_bytes": sum(job["input_bytes"] for job in remote_completed),
            "source_bytes": sum(job["source_bytes"] for job in remote_completed),
            "result_bytes": sum(job["result_bytes"] for job in remote_completed),
            "average_queue_seconds": (
                round(sum(queue_delays) / len(queue_delays), 3) if queue_delays else None
            ),
            "last_finished_at": max(
                (
                    job["finished_at"]
                    for job in remote_completed
                    if job["finished_at"] is not None
                ),
                default=None,
            ),
        }
        queued = sum(job["state"] == "queued" for job in current)
        running = sum(
            job["state"] == "running" and job["placement"] == "remote"
            for job in current
        )
        local_running = sum(
            job["state"] == "running" and job["placement"] == "pi-local"
            for job in current
        )
        capacity_workers = [
            worker
            for worker in workers
            if worker["available"]
            and worker["placement"] == "remote"
            and worker["slots_total"] is not None
            and worker["slots_busy"] is not None
        ]
        slots_total = (
            sum(worker["slots_total"] for worker in capacity_workers)
            if capacity_workers
            else None
        )
        slots_busy = (
            sum(worker["slots_busy"] for worker in capacity_workers)
            if capacity_workers
            else None
        )
        return {
            "ok": True,
            "generated_at": now,
            "range_hours": hours,
            "status": {
                "configured": self.root.is_dir(),
                "available": any(
                    worker["available"] and worker["placement"] == "remote"
                    for worker in workers
                ),
                "busy": running > 0,
                "queued": queued,
                "running": running,
                "local_running": local_running,
                "workers": workers,
                "slots_total": slots_total,
                "slots_busy": slots_busy,
                "slots_available": (
                    slots_total - slots_busy
                    if slots_total is not None and slots_busy is not None
                    else None
                ),
            },
            "summary": summary,
            "tasks": task_rows,
            "jobs": [
                {
                    key: value
                    for key, value in job.items()
                    if not key.startswith("_")
                }
                for job in visible[:MAX_RECENT_JOBS]
            ],
            "benchmark": benchmark,
            "eligible_local_work": eligible_local_work,
            "measurement_note": (
                "Mac CPU is child CPU time; Mac active time covers staging through "
                "non-telemetry result uploads when detailed timing is available. Maximum "
                "Mac RSS is the higher of the wait4 leader maximum and a once-per-second "
                "process-group aggregate sample; brief spikes or detached processes may be "
                "missed. Pi-local events are recorded broker fallback runs, plus any "
                "manually recorded eligible local work; those records are not exhaustive. "
                "Exact-content calibration excludes dataset-backed work and estimates only "
                "Pi analysis time and CPU for matched jobs. It excludes Pi submission, "
                "snapshot, and SSH streaming overhead and does not extrapolate to unmatched "
                "work. Pi and Mac maximum RSS measurements have different sampling scopes."
            ),
        }
