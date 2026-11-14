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
    swap_in: int
    swap_out: int
    cpu: tuple[int, ...]


def _read_swap_pages(path: Path) -> tuple[int, int]:
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
    if min(values.values()) < 0:
        raise HealthProbeError(f"{path} has a negative swap counter")
    return values["pswpin"], values["pswpout"]


def _read_cpu_times(path: Path) -> tuple[int, ...]:
    """Return the aggregate cpu line's jiffy fields, each kept separate so a
    decrease in one cannot be hidden by an increase in another."""
    try:
        first = path.read_text(encoding="ascii").splitlines()[0].split()
        fields = [int(value) for value in first[1 : 1 + CPU_FIELDS]]
    except (OSError, IndexError, ValueError) as exc:
        raise HealthProbeError(f"cannot read CPU times from {path}: {exc}") from None
    if first[0] != "cpu" or len(fields) != CPU_FIELDS:
        raise HealthProbeError(f"{path} has no complete aggregate cpu line")
    if min(fields) < 0:
        raise HealthProbeError(f"{path} has a negative CPU counter")
    return tuple(fields)


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
        cpu = _read_cpu_times(self._stat_path)
        swap_in, swap_out = _read_swap_pages(self._vmstat_path)
        return _Counters(self._clock(), swap_in, swap_out, cpu)

    @staticmethod
    def _between(earlier: _Counters, later: _Counters) -> Rates | None:
        elapsed = later.at - earlier.at
        # Every counter only grows; any one going down means a reset or a bad
        # read, and the other counters' growth must not mask it.
        swap_deltas = (
            later.swap_in - earlier.swap_in,
            later.swap_out - earlier.swap_out,
        )
        cpu_deltas = [new - old for new, old in zip(later.cpu, earlier.cpu)]
        if elapsed <= 0 or min(swap_deltas + tuple(cpu_deltas)) < 0:
            return None
        total = sum(cpu_deltas)
        if total <= 0:
            return None
        busy = total - cpu_deltas[IDLE_FIELD] - cpu_deltas[IOWAIT_FIELD]
        return Rates(sum(swap_deltas) / elapsed, busy / total)

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
