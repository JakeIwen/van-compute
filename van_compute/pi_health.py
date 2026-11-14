"""Pi fallback health snapshot and the gate that assesses it."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import subprocess

from van_compute.config import HealthThresholds


class HealthProbeError(RuntimeError):
    """A health input could not be read; the gate must defer, never assume health."""


@dataclass(frozen=True)
class HealthSnapshot:
    memory_available_bytes: int
    swap_total_bytes: int
    swap_used_bytes: int
    load_1m: float
    cpu_count: int
    temperature_c: float | None
    throttled_flags: int | None


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


def read_health_snapshot() -> HealthSnapshot:
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
    )


def assess_health(
    snapshot: HealthSnapshot,
    thresholds: HealthThresholds,
) -> HealthAssessment:
    reasons: list[str] = []
    if snapshot.memory_available_bytes < thresholds.minimum_available_bytes:
        reasons.append(
            "available memory is below "
            f"{thresholds.minimum_available_bytes // (1024 * 1024)} MiB"
        )
    if snapshot.swap_total_bytes > 0:
        fraction = snapshot.swap_used_bytes / snapshot.swap_total_bytes
        # Small Pi swap partitions can stay nearly full with cold historical
        # pages even when there is no current pressure.  MemAvailable and the
        # service's MemorySwapMax=0 are primary; this secondary gate triggers
        # only after both a relative and a meaningful absolute threshold.
        swap_limit = max(
            thresholds.maximum_swap_used_bytes,
            int(snapshot.swap_total_bytes * thresholds.maximum_swap_used_fraction),
        )
        if snapshot.swap_used_bytes > swap_limit:
            reasons.append(
                f"swap use is {fraction:.1%} ({snapshot.swap_used_bytes // (1024 * 1024)} MiB), "
                f"above {swap_limit // (1024 * 1024)} MiB"
            )
    maximum_load = snapshot.cpu_count * thresholds.maximum_load_per_cpu
    if snapshot.load_1m > maximum_load:
        reasons.append(
            f"1-minute load {snapshot.load_1m:.2f} exceeds {maximum_load:.2f}"
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
