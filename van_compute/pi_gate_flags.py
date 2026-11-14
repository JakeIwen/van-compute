"""Command-line flags that configure the Pi fallback health gate."""

from __future__ import annotations

import argparse
import math
from pathlib import Path

from van_compute.config import DEFAULT_CAN_LOCK_DIR, HealthThresholds

MIB = 1024 * 1024
GATE_DEFAULTS = HealthThresholds()
# Flags removed with the swap-used and load gates; kept registered only so an
# old unit or script gets a pointer to the replacement instead of a usage error.
REMOVED_GATE_FLAGS = {
    "max_swap_used": ("--max-swap-used", "--max-swap-pages-per-second"),
    "max_swap_used_mb": ("--max-swap-used-mb", "--max-swap-pages-per-second"),
    "max_load_per_cpu": ("--max-load-per-cpu", "--max-cpu-busy"),
}


def add_gate_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--min-available-memory-mb",
        type=int,
        default=GATE_DEFAULTS.minimum_available_bytes // MIB,
        help="MemAvailable the Pi must keep free to start a job (at least 256 and "
        "the job memory ceiling)",
    )
    parser.add_argument(
        "--max-swap-pages-per-second",
        type=float,
        default=GATE_DEFAULTS.maximum_swap_pages_per_second,
        help="current swap-in plus swap-out rate above which no job starts",
    )
    parser.add_argument(
        "--max-cpu-busy",
        type=float,
        default=GATE_DEFAULTS.maximum_cpu_busy_fraction,
        help="CPU share (excluding idle and iowait) above which no job starts",
    )
    parser.add_argument(
        "--max-temperature-c", type=float, default=GATE_DEFAULTS.maximum_temperature_c
    )
    parser.add_argument(
        "--can-lock-dir",
        type=Path,
        default=DEFAULT_CAN_LOCK_DIR,
        help="directory of active-diagnostics-*.lock files; a write lock on any "
        "means a CAN transmitter is active",
    )
    parser.add_argument(
        "--can-quiet-seconds",
        type=float,
        default=GATE_DEFAULTS.can_quiet_seconds,
        help="CAN must have been continuously lock-free this long before a job starts",
    )
    for dest, (flag, _replacement) in REMOVED_GATE_FLAGS.items():
        parser.add_argument(flag, dest=dest, default=None, help=argparse.SUPPRESS)


def validated_thresholds(
    args: argparse.Namespace, error_type: type[Exception]
) -> HealthThresholds:
    """Check the gate flags and build the thresholds; raise `error_type`."""
    for dest, (flag, replacement) in REMOVED_GATE_FLAGS.items():
        if getattr(args, dest) is not None:
            raise error_type(f"{flag} was removed; use {replacement} instead")
    job_memory_mb = -(-args.max_memory_bytes // MIB)
    if args.min_available_memory_mb < max(256, job_memory_mb):
        raise error_type(
            "--min-available-memory-mb must be at least 256 and at least "
            f"--max-memory-bytes ({job_memory_mb} MiB)"
        )
    if not math.isfinite(args.max_swap_pages_per_second) or (
        args.max_swap_pages_per_second <= 0
    ):
        raise error_type("--max-swap-pages-per-second must be positive and finite")
    if not math.isfinite(args.max_cpu_busy) or not 0 < args.max_cpu_busy <= 1:
        raise error_type("--max-cpu-busy must be above 0 and at most 1")
    if not math.isfinite(args.can_quiet_seconds) or not (
        0 <= args.can_quiet_seconds <= 3600
    ):
        raise error_type("--can-quiet-seconds must be from 0 through 3600")
    if (
        not math.isfinite(args.max_temperature_c)
        or not 40 <= args.max_temperature_c <= 100
    ):
        raise error_type("--max-temperature-c must be from 40 through 100")
    return HealthThresholds(
        minimum_available_bytes=args.min_available_memory_mb * MIB,
        maximum_swap_pages_per_second=args.max_swap_pages_per_second,
        maximum_cpu_busy_fraction=args.max_cpu_busy,
        maximum_temperature_c=args.max_temperature_c,
        can_quiet_seconds=args.can_quiet_seconds,
    )
