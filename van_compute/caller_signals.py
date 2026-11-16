"""Latch the signals that tell `run --cancel-on-timeout` its caller gave up.

An agent's tool timeout usually sends SIGTERM (a terminal close sends SIGHUP,
Ctrl-C SIGINT).  The handlers only record the first signal number and return;
Python then resumes the interrupted sleep or system call (PEP 475), so a
submission in progress finishes and publishes before the caller acts.  The
cancel itself runs in the main flow, never inside a handler, and later
signals during it are absorbed by the same latch.  SIGKILL cannot be caught.
"""

from __future__ import annotations

from contextlib import contextmanager
import signal
from typing import Iterator

CAUGHT = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)


class SignalLatch:
    def __init__(self) -> None:
        self.number: int | None = None

    def is_set(self) -> bool:
        return self.number is not None

    def _handle(self, signum: int, _frame: object) -> None:
        if self.number is None:
            self.number = signum


@contextmanager
def latched(enabled: bool) -> Iterator[SignalLatch]:
    """Install latching handlers when enabled; restore the previous ones."""
    latch = SignalLatch()
    if not enabled:
        yield latch
        return
    previous = {number: signal.signal(number, latch._handle) for number in CAUGHT}
    try:
        yield latch
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)
