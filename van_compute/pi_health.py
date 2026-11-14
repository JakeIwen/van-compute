"""Pi fallback health snapshot and the gate that assesses it."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import subprocess
from typing import TYPE_CHECKING

from van_compute.config import HealthThresholds

if TYPE_CHECKING:
    from van_compute.can_activity import CanProbe
    from van_compute.pi_rates import Rates


class HealthProbeError(RuntimeError):
    """A health input could not be read; the gate must defer, never assume health."""


@dataclass(frozen=True)
class HealthSnapshot:
    memory_available_bytes: int
    # Swap totals, used and load stay in the report only; they do not gate.
    swap_total_bytes: int
    swap_used_bytes: int
    load_1m: float
    cpu_count: int
    temperature_c: float | None
    throttled_flags: int | None
    can_active: bool
    can_held_locks: tuple[str, ...]
    can_quiet_seconds: float
    can_error: str | None
    swap_pages_per_second: float
    cpu_busy_fraction: float


@dataclass(frozen=True)
class HealthAssessment:
    ok: bool
    reasons: tuple[str, ...]
    snapshot: HealthSnapshot


def _read_meminfo(path: Path = Path("/proc/meminfo")) -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        lines = path.read_text(encoding="ascii").splitlines()
    except OSError as exc:
        raise HealthProbeError(f"cannot read memory health from {path}: {exc}") from None
    for line in lines:
        name, separator, remainder = line.partition(":")
        if not separator:
            continue
        parts = remainder.split()
        if not parts or not parts[0].isdigit():
            continue
        multiplier = 1024 if len(parts) > 1 and parts[1].lower() == "kb" else 1
        values[name] = int(parts[0]) * multiplier
    if "MemAvailable" not in values:
        raise HealthProbeError(f"{path} has no MemAvailable value")
    return values


def _read_temperature(path: Path = Path("/sys/class/thermal/thermal_zone0/temp")) -> float | None:
    try:
        raw = path.read_text(encoding="ascii").strip()
    except OSError:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return value / 1000.0 if value > 1000 else value


def _read_throttled(executable: Path = Path("/usr/bin/vcgencmd")) -> int | None:
    if not executable.is_file() or not os.access(executable, os.X_OK):
        return None
    try:
        result = subprocess.run(
            [str(executable), "get_throttled"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    _prefix, separator, value = result.stdout.strip().partition("=")
    if not separator:
        return None
    try:
        return int(value, 0)
    except ValueError:
        return None


def read_health_snapshot(
    rates: "Rates", can: "CanProbe", can_quiet_seconds: float
) -> HealthSnapshot:
    memory = _read_meminfo()
    swap_total = memory.get("SwapTotal", 0)
    swap_free = memory.get("SwapFree", 0)
    return HealthSnapshot(
        memory_available_bytes=memory["MemAvailable"],
        swap_total_bytes=swap_total,
        swap_used_bytes=max(0, swap_total - swap_free),
        load_1m=float(os.getloadavg()[0]),
        cpu_count=max(1, os.cpu_count() or 1),
        temperature_c=_read_temperature(),
        throttled_flags=_read_throttled(),
        can_active=can.active,
        can_held_locks=can.held_locks,
        can_quiet_seconds=can_quiet_seconds,
        can_error=can.error,
        swap_pages_per_second=rates.swap_pages_per_second,
        cpu_busy_fraction=rates.cpu_busy_fraction,
    )


def _can_reasons(snapshot: HealthSnapshot, thresholds: HealthThresholds) -> list[str]:
    if snapshot.can_error is not None:
        return [snapshot.can_error]
    if snapshot.can_active:
        return [f"CAN transmit lock held: {', '.join(snapshot.can_held_locks)}"]
    if snapshot.can_quiet_seconds < thresholds.can_quiet_seconds:
        return [
            f"CAN quiet for {snapshot.can_quiet_seconds:.0f} s, "
            f"below {thresholds.can_quiet_seconds:.0f} s"
        ]
    return []


def assess_health(
    snapshot: HealthSnapshot,
    thresholds: HealthThresholds,
) -> HealthAssessment:
    reasons = _can_reasons(snapshot, thresholds)
    if snapshot.memory_available_bytes < thresholds.minimum_available_bytes:
        reasons.append(
            "available memory is below "
            f"{thresholds.minimum_available_bytes // (1024 * 1024)} MiB"
        )
    if snapshot.swap_pages_per_second > thresholds.maximum_swap_pages_per_second:
        reasons.append(
            f"swap paging {snapshot.swap_pages_per_second:.0f} pages/s exceeds "
            f"{thresholds.maximum_swap_pages_per_second:.0f}"
        )
    if snapshot.cpu_busy_fraction > thresholds.maximum_cpu_busy_fraction:
        reasons.append(
            f"CPU busy {snapshot.cpu_busy_fraction:.0%} exceeds "
            f"{thresholds.maximum_cpu_busy_fraction:.0%}"
        )
    if (
        snapshot.temperature_c is not None
        and snapshot.temperature_c > thresholds.maximum_temperature_c
    ):
        reasons.append(
            f"temperature {snapshot.temperature_c:.1f} C exceeds "
            f"{thresholds.maximum_temperature_c:.1f} C"
        )
    # Bits 0-3 describe current under-voltage, frequency cap, throttling, and
    # soft-temperature limit.  Historical bits 16-19 do not block new work.
    if snapshot.throttled_flags is not None and snapshot.throttled_flags & 0xF:
        reasons.append(
            f"current Raspberry Pi throttle flags are 0x{snapshot.throttled_flags:x}"
        )
    return HealthAssessment(not reasons, tuple(reasons), snapshot)
