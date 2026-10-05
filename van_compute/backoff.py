"""A shared, interruptible recovery gate for one unreachable broker host.

The gate never replays an operation. Callers retain exact-slot lease recovery,
including ambiguous upload/finish failures. One failed generation counts once,
regardless of how many in-flight connections fail concurrently.
"""

from __future__ import annotations

import threading
import time
from typing import Callable, Sequence


class RetryCancelled(RuntimeError):
    pass


class HostRetryGate:
    def __init__(
        self,
        base: float = 15.0,
        cap: float = 60.0,
        *,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.base = max(1.0, base)
        self.cap = max(self.base, cap)
        self.clock = clock
        self._condition = threading.Condition()
        self._generation = 0
        self._delay = 0.0
        self._retry_at = 0.0
        self._probe = False

    @property
    def delay(self) -> float:
        with self._condition:
            return self._delay

    def acquire(self, stops: Sequence[threading.Event] = ()) -> int:
        with self._condition:
            while True:
                # Healthy calls retain the original stop/drain semantics. Only
                # the newly introduced outage wait adds cancellation points.
                if not self._delay:
                    return self._generation
                if any(event.is_set() for event in stops):
                    raise RetryCancelled(
                        "worker stopped while waiting for broker recovery"
                    )
                remaining = self._retry_at - self.clock()
                if remaining <= 0 and not self._probe:
                    self._probe = True
                    return self._generation
                # Bounded wait observes stop/drain without depending on signal handlers.
                self._condition.wait(min(0.1, remaining) if remaining > 0 else 0.1)

    def failed(self, generation: int) -> None:
        with self._condition:
            if generation != self._generation:
                return
            self._delay = min(
                self.cap, self.base if not self._delay else 2 * self._delay
            )
            self._retry_at = self.clock() + self._delay
            self._probe = False
            self._generation += 1
            self._condition.notify_all()

    def reached(self, generation: int) -> None:
        with self._condition:
            if generation != self._generation:
                return
            if self._delay:
                self._generation += 1
            self._delay = self._retry_at = 0.0
            self._probe = False
            self._condition.notify_all()

    def abandoned(self, generation: int) -> None:
        """Release a probe on a local non-transport exception, without resetting."""
        with self._condition:
            if generation == self._generation:
                self._probe = False
                self._condition.notify_all()
