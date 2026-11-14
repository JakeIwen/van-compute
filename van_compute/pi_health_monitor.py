"""One broker-lifetime bundle of CAN observation and rate sampling.

The CAN quiet chain and the rate sampler both depend on being fed on every
poll, so the service (or `--once`) builds one monitor and passes it down.
"""

from __future__ import annotations

from pathlib import Path
import time
from typing import Callable

from van_compute.can_activity import CanProbe, CanQuietTracker, probe_can_activity
from van_compute.pi_health import HealthSnapshot, read_health_snapshot
from van_compute.pi_rates import RateSampler

# Also the cadence at which a running job's CAN check is rate-limited.
CAN_WATCH_INTERVAL_SECONDS = 0.5


class HealthMonitor:
    def __init__(
        self,
        lock_dir: Path,
        *,
        probe: Callable[[Path], CanProbe] = probe_can_activity,
        tracker: CanQuietTracker | None = None,
        sampler: RateSampler | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._lock_dir = lock_dir
        self._probe = probe
        self.tracker = CanQuietTracker() if tracker is None else tracker
        self.sampler = RateSampler() if sampler is None else sampler
        self._clock = clock
        self._sleep = sleep

    def observe_can(self) -> CanProbe:
        """Probe CAN once and feed the quiet chain; a probe error is a result."""
        try:
            result = self._probe(self._lock_dir)
        except Exception as exc:  # any probe fault must fail closed
            result = CanProbe(False, (), f"CAN lock probe failed: {exc}")
        self.tracker.observe(self._clock(), quiet=result.quiet)
        return result

    def snapshot(self) -> HealthSnapshot:
        can = self.observe_can()
        rates = self.sampler.sample()
        return read_health_snapshot(
            rates, can, self.tracker.quiet_seconds(self._clock())
        )

    def wait_for_quiet(self, quiet_seconds: float) -> None:
        """Watch CAN until it has been quiet long enough, or stop at the first
        lock or probe failure (the gate then defers)."""
        while True:
            if not self.observe_can().quiet:
                return
            if self.tracker.quiet_seconds(self._clock()) >= quiet_seconds:
                return
            self._sleep(CAN_WATCH_INTERVAL_SECONDS)
