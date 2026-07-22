#!/usr/bin/env python3
"""Read-only aggregation of van-compute queue and resource telemetry."""

from __future__ import annotations

import datetime as dt
import json
import math
import os
from pathlib import Path
import stat
import time
from typing import Callable


DEFAULT_QUEUE_ROOT = Path("/home/pi/dev/obd-things/tmp/compute")
STATE_DIRECTORIES = ("queued", "running", "done", "failed")
MAX_JSON_BYTES = 2 * 1024 * 1024
MAX_SCANNED_JOBS = 2000
MAX_RECENT_JOBS = 50


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


def _read_json(path: Path) -> dict[str, object]:
    try:
        info = path.lstat()
    except OSError as exc:
        raise ComputeMetricsError(f"cannot inspect {path}: {exc}") from None
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise ComputeMetricsError(f"refusing non-regular JSON file: {path}")
    if info.st_size > MAX_JSON_BYTES:
        raise ComputeMetricsError(f"JSON file is unexpectedly large: {path}")
    try:
        with path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise ComputeMetricsError(f"cannot read {path}: {exc}") from None
    if not isinstance(payload, dict):
        raise ComputeMetricsError(f"JSON file is not an object: {path}")
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
            workers.append(
                {
                    "worker": str(payload.get("worker") or path.stem)[:64],
                    "seen_at": seen_at,
                    "age_seconds": round(age, 3) if age is not None else None,
                    "available": age is not None and age <= self.heartbeat_max_age,
                }
            )
        return workers

    def _job_paths(self) -> list[tuple[str, Path]]:
        found: list[tuple[str, Path]] = []
        for state in STATE_DIRECTORIES:
            directory = self.root / state
            if not directory.is_dir() or directory.is_symlink():
                continue
            for path in directory.iterdir():
                if path.is_dir() and not path.is_symlink() and not path.name.startswith("."):
                    found.append((state, path))
        found.sort(key=lambda item: item[1].name, reverse=True)
        return found[:MAX_SCANNED_JOBS]

    def _job(self, state: str, path: Path) -> dict[str, object] | None:
        try:
            manifest = _read_json(path / "manifest.json")
        except ComputeMetricsError:
            return None
        job_id = str(manifest.get("id") or path.name)
        task = str(manifest.get("task") or "unknown")[:100]
        submitted_at = _timestamp(manifest.get("submitted_at"))
        started_at = _timestamp(manifest.get("started_at"))
        finished_at = _timestamp(manifest.get("finished_at"))
        execution: dict[str, object] = {}
        execution_path = path / "result" / "execution.json"
        if execution_path.exists():
            try:
                execution = _read_json(execution_path)
            except ComputeMetricsError:
                execution = {}
        usage = execution.get("resource_usage")
        usage = usage if isinstance(usage, dict) else {}
        raw_wall_seconds = execution.get("duration_seconds")
        if raw_wall_seconds is None:
            derived = _duration(manifest.get("started_at"), manifest.get("finished_at"))
            wall_seconds = derived if derived is not None else 0.0
        else:
            wall_seconds = _nonnegative_number(raw_wall_seconds)
        cpu_seconds = _nonnegative_number(usage.get("cpu_seconds"))
        average_cpu = usage.get("average_cpu_percent")
        average_cpu = (
            _nonnegative_number(average_cpu)
            if average_cpu is not None
            else (100 * cpu_seconds / wall_seconds if wall_seconds > 0 else None)
        )
        peak_rss = _nonnegative_integer(usage.get("peak_rss_bytes"))
        return {
            "id": job_id,
            "task": task,
            "state": str(manifest.get("state") or state),
            "worker": manifest.get("worker"),
            "exit_code": manifest.get("exit_code"),
            "submitted_at": submitted_at,
            "started_at": started_at,
            "finished_at": finished_at,
            "queue_seconds": _duration(manifest.get("submitted_at"), manifest.get("started_at")),
            "wall_seconds": round(wall_seconds, 6),
            "cpu_seconds": round(cpu_seconds, 6),
            "average_cpu_percent": round(average_cpu, 2) if average_cpu is not None else None,
            "peak_rss_bytes": peak_rss,
            "input_bytes": _sum_sizes(manifest.get("inputs")),
            "source_bytes": _sum_sizes(manifest.get("sources")),
            "result_bytes": _sum_sizes(manifest.get("results")),
            "telemetry": bool(usage),
            "timed_out": bool(execution.get("timed_out", False)),
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
        visible = sorted(
            [*current, *completed],
            key=lambda job: job["finished_at"] or job["started_at"] or job["submitted_at"] or 0,
            reverse=True,
        )
        telemetry = [job for job in completed if job["telemetry"]]
        total_wall = sum(job["wall_seconds"] for job in telemetry)
        total_cpu = sum(job["cpu_seconds"] for job in telemetry)
        queue_delays = [job["queue_seconds"] for job in completed if job["queue_seconds"] is not None]
        tasks: dict[str, dict[str, object]] = {}
        for job in completed:
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
            "jobs": len(completed),
            "succeeded": sum(job["state"] == "done" for job in completed),
            "failed": sum(job["state"] == "failed" for job in completed),
            "telemetry_jobs": len(telemetry),
            "mac_cpu_seconds": round(total_cpu, 6),
            "mac_wall_seconds": round(total_wall, 6),
            "aggregate_cpu_percent": round(100 * total_cpu / total_wall, 2) if total_wall > 0 else None,
            "peak_rss_bytes": max((job["peak_rss_bytes"] for job in telemetry), default=0),
            "input_bytes": sum(job["input_bytes"] for job in completed),
            "source_bytes": sum(job["source_bytes"] for job in completed),
            "result_bytes": sum(job["result_bytes"] for job in completed),
            "average_queue_seconds": (
                round(sum(queue_delays) / len(queue_delays), 3) if queue_delays else None
            ),
            "last_finished_at": max(
                (job["finished_at"] for job in completed if job["finished_at"] is not None),
                default=None,
            ),
        }
        queued = sum(job["state"] == "queued" for job in current)
        running = sum(job["state"] == "running" for job in current)
        return {
            "ok": True,
            "generated_at": now,
            "range_hours": hours,
            "status": {
                "configured": self.root.is_dir(),
                "available": any(worker["available"] for worker in workers),
                "busy": running > 0,
                "queued": queued,
                "running": running,
                "workers": workers,
            },
            "summary": summary,
            "tasks": task_rows,
            "jobs": visible[:MAX_RECENT_JOBS],
            "measurement_note": (
                "Mac CPU and memory are measured resources used by offline jobs. "
                "They demonstrate work not executed on vanpi, but are not calibrated Pi-equivalent savings."
            ),
        }
