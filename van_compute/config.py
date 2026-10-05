"""Typed runtime configuration for the van-compute broker and worker."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from pathlib import Path
import threading
from typing import Mapping, Protocol


@dataclass(frozen=True)
class HealthThresholds:
    minimum_available_bytes: int = 1536 * 1024 * 1024
    maximum_swap_used_fraction: float = 0.20
    maximum_swap_used_bytes: int = 512 * 1024 * 1024
    maximum_load_per_cpu: float = 1.25
    maximum_temperature_c: float = 75.0


@dataclass
class BrokerConfig:
    root: Path
    work_root: Path
    once: bool
    self_test: bool
    remote_max_age: float
    remote_grace: float
    stale_running_age: float
    poll_interval: float
    timeout: int
    cpu_seconds: int
    max_memory_bytes: int
    max_result_bytes: int
    min_work_free_bytes: int
    max_open_files: int
    nice: int
    python: str
    sqlite3: str
    bwrap: str
    min_available_memory_mb: int
    max_swap_used: float
    max_swap_used_mb: int
    max_load_per_cpu: float
    max_temperature_c: float
    health_thresholds: HealthThresholds = field(default_factory=HealthThresholds)

    @classmethod
    def from_namespace(
        cls,
        args: argparse.Namespace,
        *,
        health_thresholds: HealthThresholds | None = None,
    ) -> "BrokerConfig":
        return cls(
            root=args.root,
            work_root=args.work_root,
            once=args.once,
            self_test=args.self_test,
            remote_max_age=args.remote_max_age,
            remote_grace=args.remote_grace,
            stale_running_age=args.stale_running_age,
            poll_interval=args.poll_interval,
            timeout=args.timeout,
            cpu_seconds=args.cpu_seconds,
            max_memory_bytes=args.max_memory_bytes,
            max_result_bytes=args.max_result_bytes,
            min_work_free_bytes=args.min_work_free_bytes,
            max_open_files=args.max_open_files,
            nice=args.nice,
            python=args.python,
            sqlite3=args.sqlite3,
            bwrap=args.bwrap,
            min_available_memory_mb=args.min_available_memory_mb,
            max_swap_used=args.max_swap_used,
            max_swap_used_mb=args.max_swap_used_mb,
            max_load_per_cpu=args.max_load_per_cpu,
            max_temperature_c=args.max_temperature_c,
            health_thresholds=(
                HealthThresholds() if health_thresholds is None else health_thresholds
            ),
        )


class Reservation(Protocol):
    def release(self) -> None: ...


class ResourceManager(Protocol):
    def acquire(
        self,
        manifest: Mapping[str, object],
        stop_event: threading.Event | None,
        drain_event: threading.Event | None = None,
    ) -> Reservation: ...

    def require_free_reserve(self, path: Path, phase: str) -> None: ...


@dataclass(frozen=True)
class WorkerConfig:
    host: str
    remote_cli: str
    remote_root: str | None
    worker: str
    work_root: Path
    control_path: Path
    python: str
    sqlite3: str
    rg: str
    jadx: str
    timeout: int
    connect_timeout: int
    nice: int
    max_result_bytes: int
    max_memory_bytes: int
    max_processes: int
    min_free_bytes: int
    heartbeat_interval: float
    poll_interval: float
    dataset_config: Path | None
    dataset: tuple[str, ...]
    sandbox_profile: Path | None
    allow_unsandboxed_dynamic: bool
    serve: bool
    run_once: bool
    executables: Mapping[str, str] = field(default_factory=dict)
    datasets: Mapping[str, Path] = field(default_factory=dict)
    resource_manager: ResourceManager | None = None
    resource_admission_stop_event: threading.Event | None = None

    @classmethod
    def from_namespace(
        cls,
        args: argparse.Namespace,
        *,
        work_root: Path | None = None,
        executables: Mapping[str, str] | None = None,
        datasets: Mapping[str, Path] | None = None,
        resource_manager: ResourceManager | None = None,
    ) -> "WorkerConfig":
        return cls(
            host=args.host,
            remote_cli=args.remote_cli,
            remote_root=args.remote_root,
            worker=args.worker,
            work_root=args.work_root if work_root is None else work_root,
            control_path=args.control_path,
            python=args.python,
            sqlite3=args.sqlite3,
            rg=args.rg,
            jadx=args.jadx,
            timeout=args.timeout,
            connect_timeout=args.connect_timeout,
            nice=args.nice,
            max_result_bytes=args.max_result_bytes,
            max_memory_bytes=args.max_memory_bytes,
            max_processes=args.max_processes,
            min_free_bytes=args.min_free_bytes,
            heartbeat_interval=args.heartbeat_interval,
            poll_interval=args.poll_interval,
            dataset_config=args.dataset_config,
            dataset=tuple(args.dataset),
            sandbox_profile=args.sandbox_profile,
            allow_unsandboxed_dynamic=args.allow_unsandboxed_dynamic,
            serve=args.serve,
            run_once=args.run_once,
            executables={} if executables is None else executables,
            datasets={} if datasets is None else datasets,
            resource_manager=resource_manager,
        )


@dataclass(frozen=True)
class MissedOffloadRecord:
    profile: str
    label: str
    reason: str
    duration_seconds: float | int | None = None
    cpu_seconds: float | int | None = None
    peak_rss_bytes: int | None = None
    input_bytes: int | None = None


__all__ = [
    "BrokerConfig",
    "HealthThresholds",
    "MissedOffloadRecord",
    "Reservation",
    "ResourceManager",
    "WorkerConfig",
]
