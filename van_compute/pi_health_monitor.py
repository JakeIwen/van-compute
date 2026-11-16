"""One broker-lifetime bundle of CAN observation, rate sampling and startup state.

The CAN quiet chain and the rate sampler both depend on being fed on every
poll, so the service (or `--once`) builds one monitor and passes it down.  It
also carries the serve loop's service-cgroup gate (`pi_cgroup_gate`).
"""

from __future__ import annotations

from pathlib import Path
import time
from typing import Callable

from van_compute.can_activity import CanProbe, CanQuietTracker, probe_can_activity
from van_compute.pi_cgroup_gate import CgroupGate
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
        containment: CgroupGate | None = None,
    ) -> None:
        self._lock_dir = lock_dir
        self._probe = probe
        self.tracker = CanQuietTracker() if tracker is None else tracker
        self.sampler = RateSampler() if sampler is None else sampler
        self._clock = clock
        self._sleep = sleep
        # None outside the serve loop (foreground commands, tests): ungated.
        self.containment = containment

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

    def quiet_seconds(self) -> float:
        return self.tracker.quiet_seconds(self._clock())

    def wait_for_quiet(self, quiet_seconds: float) -> CanProbe:
        """Watch CAN until it has been quiet long enough, or stop at the first
        lock or probe failure.  Returns the last observation: a caller that
        sees it not quiet must defer rather than re-observe, since a later
        quiet observation would hide the activity that ended the wait."""
        while True:
            probe = self.observe_can()
            if not probe.quiet:
                return probe
            if self.tracker.quiet_seconds(self._clock()) >= quiet_seconds:
                return probe
            self._sleep(CAN_WATCH_INTERVAL_SECONDS)


def observe_and_pick_reader(
    monitor: HealthMonitor | None,
    health_reader: Callable[[], HealthSnapshot] | None,
    error_type: type[Exception],
) -> Callable[[], HealthSnapshot]:
    """Observe CAN before any early return so deferrals (maintenance, a fresh
    remote lease) keep the quiet chain unbroken.  The monitor is owned by
    serve() or main(); building one per call would reset the chain every poll."""
    if monitor is not None:
        monitor.observe_can()
    if health_reader is not None:
        return health_reader
    if monitor is None:
        raise error_type("run_once needs a health monitor or a health reader")
    return monitor.snapshot
