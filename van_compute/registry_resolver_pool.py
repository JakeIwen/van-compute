"""Fixed-size, bounded pool of name-lookup threads for the registry proxy."""

from __future__ import annotations

from collections import deque
from concurrent.futures import Future
import threading
from typing import Callable, Sequence

# Maps (host, port) to the vetted (address, port) pairs to connect to.
Resolver = Callable[[str, int], Sequence[tuple[str, int]]]


class ResolverPool:
    """Run name lookups on a fixed set of daemon threads.

    ``getaddrinfo`` cannot be interrupted, so callers wait on a Future with a
    deadline and then ``abandon`` it: a queued lookup is removed at once, and
    a running one's late answer lands in an abandoned Future and is discarded.
    Retained work is therefore at most ``max_pending`` queued lookups plus one
    per thread, however long resolvers stay stuck. Daemon threads keep a
    lookup that never returns from blocking worker exit.
    """

    def __init__(self, resolver: Resolver, threads: int, max_pending: int) -> None:
        self._resolver = resolver
        self._max_pending = max_pending
        self._pending: deque[tuple[Future, str, int]] = deque()
        self._condition = threading.Condition()
        self._closed = False
        self._threads = [
            threading.Thread(
                target=self._work, name="van-compute-registry-resolver", daemon=True
            )
            for _ in range(threads)
        ]
        for thread in self._threads:
            thread.start()

    @property
    def pending(self) -> int:
        """Queued lookups no thread has started; never above ``max_pending``."""
        with self._condition:
            return len(self._pending)

    def submit(self, host: str, port: int) -> Future | None:
        """Queue a lookup, or return None when the queue is full or closed."""
        with self._condition:
            if self._closed or len(self._pending) >= self._max_pending:
                return None
            future: Future = Future()
            self._pending.append((future, host, port))
            self._condition.notify()
            return future

    def abandon(self, future: Future) -> None:
        with self._condition:
            for job in self._pending:
                if job[0] is future:
                    self._pending.remove(job)
                    break
        future.cancel()

    def close(self) -> None:
        with self._condition:
            self._closed = True
            for future, _host, _port in self._pending:
                future.cancel()
            self._pending.clear()
            self._condition.notify_all()

    def _work(self) -> None:
        while True:
            with self._condition:
                while not self._pending and not self._closed:
                    self._condition.wait()
                if self._closed:
                    return
                future, host, port = self._pending.popleft()
            # False when the caller abandoned it between the pop and here.
            if not future.set_running_or_notify_cancel():
                continue
            try:
                future.set_result(self._resolver(host, port))
            except BaseException as exc:  # handed to the waiting caller
                future.set_exception(exc)
