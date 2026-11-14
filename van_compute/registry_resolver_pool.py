"""Fixed-size pool of name-lookup threads for the registry proxy."""

from __future__ import annotations

from concurrent.futures import Future
import queue
import threading
from typing import Callable, Sequence

# Maps (host, port) to the vetted (address, port) pairs to connect to.
Resolver = Callable[[str, int], Sequence[tuple[str, int]]]


class ResolverPool:
    """Run name lookups on a fixed set of daemon threads.

    ``getaddrinfo`` cannot be interrupted, so callers wait on a Future with a
    deadline and walk away; a late answer lands in an abandoned Future and is
    discarded. Daemon threads keep a lookup that never returns from blocking
    worker exit.
    """

    def __init__(self, resolver: Resolver, threads: int) -> None:
        self._resolver = resolver
        self._jobs: "queue.Queue[tuple[Future, str, int] | None]" = queue.Queue()
        self._threads = [
            threading.Thread(
                target=self._work, name="van-compute-registry-resolver", daemon=True
            )
            for _ in range(threads)
        ]
        for thread in self._threads:
            thread.start()

    def submit(self, host: str, port: int) -> Future:
        future: Future = Future()
        self._jobs.put((future, host, port))
        return future

    def close(self) -> None:
        for _thread in self._threads:
            self._jobs.put(None)

    def _work(self) -> None:
        while True:
            job = self._jobs.get()
            if job is None:
                return
            future, host, port = job
            # False once a caller gave up and cancelled it while queued.
            if not future.set_running_or_notify_cancel():
                continue
            try:
                future.set_result(self._resolver(host, port))
            except BaseException as exc:  # handed to the waiting caller
                future.set_exception(exc)
