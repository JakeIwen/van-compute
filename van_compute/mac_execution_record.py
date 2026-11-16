"""Build the Mac worker's `execution.json` record for one analysis run.

The worker supplies everything host- or time-dependent (worker name, clock
readings, the logical command and wait4 usage); this module only assembles the
record, so its bytes stay identical to what the worker always wrote.
"""

from __future__ import annotations

from typing import Mapping, Sequence

from van_compute import protocol


def analysis_execution(
    manifest: Mapping[str, object],
    outcome,
    *,
    task_name: str,
    worker: str,
    started: str,
    finished_at: str,
    duration_seconds: float,
    resource_usage: dict[str, object],
    watchdog_interval: float,
    command: dict[str, object],
    inputs: Sequence[object],
) -> dict[str, object]:
    resource_usage.update(
        {
            "peak_process_group_rss_bytes": outcome.peak_process_group_rss_bytes,
            "peak_process_count": outcome.peak_process_count,
            "process_group_watchdog_interval_seconds": watchdog_interval,
            "minimum_filesystem_free_bytes": (outcome.minimum_filesystem_free_bytes),
        }
    )
    return {
        "schema_version": protocol.SCHEMA_VERSION,
        "job_id": manifest.get("id"),
        "task": task_name,
        "worker": worker,
        "placement": "remote",
        "started_at": started,
        "finished_at": finished_at,
        "duration_seconds": round(duration_seconds, 6),
        "duration_scope": "analysis child execution plus descendant process-group cleanup",
        "exit_code": outcome.exit_code,
        "timed_out": outcome.timed_out,
        "interrupted": outcome.interrupted,
        "resource_limit": outcome.resource_limit,
        "resource_monitor_error": outcome.resource_monitor_error,
        "command": command,
        "resource_usage": resource_usage,
        "input_bytes": sum(
            int(item.get("size", 0)) for item in inputs if isinstance(item, dict)
        ),
        "source_bytes": sum(
            int(item.get("size", 0))
            for item in manifest.get("sources", [])
            if isinstance(item, dict)
        ),
    }
