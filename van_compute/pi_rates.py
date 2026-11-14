"""Swap-paging and CPU-busy rates from /proc counters, for the Pi health gate.

Swap *used* and load average stay high on the Pi for reasons that do not hurt a
job (cold pages parked in swap, backup I/O wait), so the gate looks at current
activity instead: pages swapped per second and the CPU share that is neither
idle nor waiting on I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import time
from typing import Callable

from van_compute.pi_health import HealthProbeError

# A previous sample older than this describes a different moment (after a job
# ran, or between --once calls), so a fresh window is measured instead.
MAX_SAMPLE_AGE_SECONDS = 30.0
FRESH_WINDOW_SECONDS = 1.0
# user nice system idle iowait irq softirq steal; guest time is already inside
# user and nice, so adding it would count it twice.
CPU_FIELDS = 8
IDLE_FIELD = 3
IOWAIT_FIELD = 4


@dataclass(frozen=True)
class Rates:
    swap_pages_per_second: float
    cpu_busy_fraction: float


@dataclass(frozen=True)
class _Counters:
    at: float
    swap_pages: int
    cpu_busy: int
    cpu_total: int


def _read_swap_pages(path: Path) -> int:
    values: dict[str, int] = {}
    try:
        for line in path.read_text(encoding="ascii").splitlines():
            name, _, value = line.partition(" ")
            if name in ("pswpin", "pswpout"):
                values[name] = int(value)
    except (OSError, ValueError) as exc:
        raise HealthProbeError(f"cannot read swap activity from {path}: {exc}") from None
    if len(values) != 2:
        raise HealthProbeError(f"{path} lacks pswpin or pswpout")
    return values["pswpin"] + values["pswpout"]


def _read_cpu_times(path: Path) -> tuple[int, int]:
    """Return (busy, total) jiffies, where busy excludes idle and iowait."""
    try:
        first = path.read_text(encoding="ascii").splitlines()[0].split()
        fields = [int(value) for value in first[1 : 1 + CPU_FIELDS]]
    except (OSError, IndexError, ValueError) as exc:
        raise HealthProbeError(f"cannot read CPU times from {path}: {exc}") from None
    if first[0] != "cpu" or len(fields) != CPU_FIELDS:
        raise HealthProbeError(f"{path} has no complete aggregate cpu line")
    total = sum(fields)
    return total - fields[IDLE_FIELD] - fields[IOWAIT_FIELD], total


class RateSampler:
    """Stateful sampler; one per broker lifetime so polls share their deltas."""

    def __init__(
        self,
        *,
        vmstat_path: Path = Path("/proc/vmstat"),
        stat_path: Path = Path("/proc/stat"),
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._vmstat_path = vmstat_path
        self._stat_path = stat_path
        self._clock = clock
        self._sleep = sleep
        self._previous: _Counters | None = None

    def _read(self) -> _Counters:
        busy, total = _read_cpu_times(self._stat_path)
        return _Counters(
            self._clock(), _read_swap_pages(self._vmstat_path), busy, total
        )

    @staticmethod
    def _between(earlier: _Counters, later: _Counters) -> Rates | None:
        elapsed = later.at - earlier.at
        total = later.cpu_total - earlier.cpu_total
        pages = later.swap_pages - earlier.swap_pages
        busy = later.cpu_busy - earlier.cpu_busy
        if elapsed <= 0 or total <= 0 or pages < 0 or busy < 0:
            return None
        return Rates(pages / elapsed, min(1.0, busy / total))

    def sample(self) -> Rates:
        current = self._read()
        previous, self._previous = self._previous, current
        if previous is not None and current.at - previous.at <= MAX_SAMPLE_AGE_SECONDS:
            rates = self._between(previous, current)
            if rates is not None:
                return rates
        self._sleep(FRESH_WINDOW_SECONDS)
        later = self._read()
        self._previous = later
        rates = self._between(current, later)
        if rates is None:
            raise HealthProbeError("swap and CPU counters gave no valid sample window")
        return rates
