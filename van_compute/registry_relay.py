"""Bounded, nonblocking two-way byte relay for registry proxy tunnels.

Every wait, writes included, is capped by the tunnel's absolute deadline, so
a peer that stops reading cannot hold a proxy slot past its lifetime; each
direction buffers at most RELAY_BUFFER_BYTES before reading from its source
pauses (backpressure instead of unbounded memory).
"""

from __future__ import annotations

from dataclasses import dataclass
import selectors
import socket
import threading
import time

RELAY_CHUNK_BYTES = 64 * 1024
RELAY_BUFFER_BYTES = 256 * 1024


@dataclass(frozen=True)
class RelayResult:
    sent: int  # bytes read from the client, the buffered ClientHello included
    received: int  # bytes read from the upstream
    ending: str


class _Relay:
    def __init__(
        self, client: socket.socket, upstream: socket.socket, first: bytes
    ) -> None:
        self.client = client
        self.upstream = upstream
        self.peer = {client: upstream, upstream: client}
        # Bytes waiting to be written to each side.
        self.pending = {client: bytearray(), upstream: bytearray(first)}
        self.counts = {client: len(first), upstream: 0}
        self.reading = {client, upstream}
        self.shut: set[socket.socket] = set()
        self.selector = selectors.DefaultSelector()
        self.watched: dict[socket.socket, int] = {}

    def result(self, ending: str) -> RelayResult:
        return RelayResult(self.counts[self.client], self.counts[self.upstream], ending)

    def finished(self) -> bool:
        """Pass half-closes on once drained; True when both ways are done."""
        for source in (self.client, self.upstream):
            destination = self.peer[source]
            if (
                source not in self.reading
                and not self.pending[destination]
                and destination not in self.shut
            ):
                self.shut.add(destination)
                try:
                    destination.shutdown(socket.SHUT_WR)
                except OSError:
                    pass
        return not self.reading and not any(self.pending.values())

    def watch(self) -> None:
        for side in (self.client, self.upstream):
            mask = 0
            if side in self.reading and len(self.pending[self.peer[side]]) < RELAY_BUFFER_BYTES:
                mask |= selectors.EVENT_READ
            if self.pending[side]:
                mask |= selectors.EVENT_WRITE
            current = self.watched.get(side, 0)
            if mask == current:
                continue
            if not current:
                self.selector.register(side, mask)
            elif not mask:
                self.selector.unregister(side)
            else:
                self.selector.modify(side, mask)
            self.watched[side] = mask

    def step(self, side: socket.socket, events: int) -> bool:
        """Move what is ready on ``side``; True if any byte moved."""
        moved = False
        if events & selectors.EVENT_WRITE and self.pending[side]:
            written = side.send(self.pending[side])
            del self.pending[side][:written]
            moved = written > 0
        if events & selectors.EVENT_READ and side in self.reading:
            room = RELAY_BUFFER_BYTES - len(self.pending[self.peer[side]])
            data = side.recv(min(RELAY_CHUNK_BYTES, room))
            if not data:
                self.reading.discard(side)
            else:
                self.pending[self.peer[side]] += data
                self.counts[side] += len(data)
                moved = True
        return moved


def relay(
    client: socket.socket,
    upstream: socket.socket,
    first: bytes,
    *,
    deadline: float,
    idle_timeout: float,
    stopping: threading.Event,
) -> RelayResult:
    """Copy bytes both ways until both sides finish, the tunnel has been idle
    for ``idle_timeout``, the monotonic ``deadline`` passes, or ``stopping``."""
    for side in (client, upstream):
        side.setblocking(False)
    state = _Relay(client, upstream, first)
    idle_ends = time.monotonic() + idle_timeout
    try:
        while not state.finished():
            now = time.monotonic()
            if stopping.is_set():
                return state.result("proxy stopped")
            if now >= deadline:
                return state.result("lifetime limit")
            if now >= idle_ends:
                return state.result("idle timeout")
            state.watch()
            for key, events in state.selector.select(min(deadline, idle_ends) - now):
                try:
                    if state.step(key.fileobj, events):
                        idle_ends = time.monotonic() + idle_timeout
                except (BlockingIOError, InterruptedError):
                    continue
        return state.result("closed")
    except OSError as exc:
        return state.result(type(exc).__name__)
    finally:
        state.selector.close()
