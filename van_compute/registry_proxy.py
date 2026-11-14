"""Loopback CONNECT proxy that limits sandboxed Mac jobs to package registries.

Seatbelt can grant TCP to one loopback port but cannot filter by host name, so
the Mac job profile allows only this proxy's port and the proxy, running in the
worker process outside the sandbox, enforces the policy:

* only ``CONNECT <host>:443`` for a host in ``REGISTRY_HOSTS``; no plain-HTTP
  proxying, IP literals or other ports;
* the name must resolve only to global unicast addresses, and the proxy
  connects to the address it vetted, never re-resolving, so a poisoned or
  rebound name cannot reach the LAN or loopback services;
* the client's first TLS record must be a ClientHello whose SNI equals the
  CONNECT host and that carries no encrypted-ClientHello extension, because
  the registries sit on shared CDN addresses and an opaque tunnel bounds only
  the address, not the site.

After that check the tunnel is opaque (no TLS interception). HTTP domain
fronting inside the encrypted channel remains possible; the CDNs' own
SNI/Host matching is relied on to refuse it. The proxy listens on the same
port on 127.0.0.1 and ::1 because Seatbelt's ``localhost`` covers both, so no
other service can hold the granted port in either family.
"""

from __future__ import annotations

from concurrent.futures import TimeoutError as FutureTimeout
from contextlib import contextmanager
from dataclasses import dataclass
import errno
import ipaddress
import selectors
import socket
import sys
import threading
import time
from typing import Callable, Iterator, Sequence

from van_compute.registry_relay import relay
from van_compute.registry_resolver_pool import Resolver, ResolverPool

REGISTRY_HOSTS = frozenset(
    {"pypi.org", "files.pythonhosted.org", "registry.npmjs.org"}
)
REGISTRY_PORT = 443
PROXY_ADDRESS_V4 = "127.0.0.1"
PROXY_ADDRESS_V6 = "::1"
MAX_HEAD_BYTES = 8 * 1024
TLS_RECORD_HEADER_BYTES = 5
# RFC 8446 section 5.1: a plaintext record fragment is at most 2**14 bytes.
MAX_TLS_RECORD_PAYLOAD = 16 * 1024
TLS_CONTENT_HANDSHAKE = 0x16
TLS_HANDSHAKE_CLIENT_HELLO = 1
TLS_EXTENSION_SERVER_NAME = 0x0000
SNI_HOST_NAME = 0
# An encrypted inner ClientHello could name any site behind the same CDN.
REFUSED_TLS_EXTENSIONS = {
    0xFE0D: "encrypted ClientHello",
    0xFFCE: "encrypted server name",
}
BIND_ATTEMPTS = 16
LISTEN_BACKLOG = 128
STOP_JOIN_SECONDS = 5.0
# How often a handler waiting on a lookup rechecks for shutdown.
RESOLVE_POLL_SECONDS = 0.05
STATUS_REASONS = {
    200: "Connection Established",
    400: "Bad Request",
    403: "Forbidden",
    405: "Method Not Allowed",
    408: "Request Timeout",
    431: "Request Header Fields Too Large",
    502: "Bad Gateway",
    503: "Service Unavailable",
    504: "Gateway Timeout",
}

AddressLookup = Callable[..., Sequence[tuple]]


@dataclass(frozen=True)
class ProxyLimits:
    max_tunnels: int = 64
    head_timeout: float = 10.0
    resolve_timeout: float = 10.0
    # A fixed pool: a stalled resolver can hold at most this many threads,
    # and waiting lookups beyond it are bounded by max_tunnels.
    resolver_threads: int = 4
    hello_timeout: float = 10.0
    # Shared by every vetted address of one tunnel, not per attempt.
    connect_timeout: float = 10.0
    # No bytes in either direction for this long ends a tunnel; clients
    # reconnect pooled keep-alive connections.
    idle_timeout: float = 120.0
    max_lifetime: float = 3600.0


class ProxyRefusal(Exception):
    """A request the proxy declines; ``status`` is the HTTP reply, if any."""

    def __init__(self, status: int, reason: str) -> None:
        super().__init__(reason)
        self.status = status
        self.reason = reason


def child_environment(port: int | None) -> dict[str, str]:
    """Proxy variables for a sandboxed job's environment; none without a proxy.

    Only HTTPS is proxied: plain HTTP would be refused anyway, and NO_PROXY is
    left unset so nothing bypasses the proxy toward a denied direct connection.
    """
    if port is None:
        return {}
    url = f"http://{PROXY_ADDRESS_V4}:{port}"
    return {"HTTPS_PROXY": url, "https_proxy": url}


def parse_connect_head(head: bytes) -> str:
    """Return the allowlisted host a CONNECT request head names, or refuse."""
    request_line = head.split(b"\r\n", 1)[0]
    try:
        text = request_line.decode("ascii")
    except UnicodeDecodeError:
        raise ProxyRefusal(400, "request line is not ASCII") from None
    parts = text.split(" ")
    if len(parts) != 3 or parts[2] not in {"HTTP/1.1", "HTTP/1.0"}:
        raise ProxyRefusal(400, "malformed request line")
    method, target, _version = parts
    if method != "CONNECT":
        raise ProxyRefusal(405, f"method {method[:16]!r} is not CONNECT")
    host, separator, port = target.rpartition(":")
    if not separator:
        raise ProxyRefusal(400, "CONNECT target has no port")
    host = host.lower()
    if host not in REGISTRY_HOSTS:
        raise ProxyRefusal(403, f"host {host[:80]!r} is not a package registry")
    if port != str(REGISTRY_PORT):
        raise ProxyRefusal(403, f"port {port[:8]!r} is not {REGISTRY_PORT}")
    return host


def is_public_unicast(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """True for a globally routable unicast address, including the IPv4 that
    a mapped, 6to4 or NAT64 IPv6 address actually reaches."""
    if isinstance(address, ipaddress.IPv6Address):
        embedded = address.ipv4_mapped or address.sixtofour
        if embedded is None and address in ipaddress.ip_network("64:ff9b::/96"):
            embedded = ipaddress.IPv4Address(int(address) & 0xFFFFFFFF)
        if embedded is not None and not is_public_unicast(embedded):
            return False
    return address.is_global and not address.is_multicast


def resolve_public(
    host: str, port: int, *, lookup: AddressLookup = socket.getaddrinfo
) -> list[tuple[str, int]]:
    """Resolve ``host`` to TCP addresses, refusing if any answer is not public.

    One non-public answer refuses the whole name: a registry never publishes
    one, so it means a poisoned or rebound record.
    """
    try:
        answers = lookup(host, port, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP)
    except OSError as exc:
        raise ProxyRefusal(502, f"cannot resolve {host}: {exc}") from None
    addresses: list[tuple[str, int]] = []
    for answer in answers:
        text = str(answer[4][0])
        try:
            address = ipaddress.ip_address(text)
        except ValueError:
            raise ProxyRefusal(403, f"{host} resolved to a scoped or invalid address") from None
        if not is_public_unicast(address):
            raise ProxyRefusal(403, f"{host} resolved to non-public address {address}")
        if (text, port) not in addresses:
            addresses.append((text, port))
    if not addresses:
        raise ProxyRefusal(502, f"{host} has no addresses")
    return addresses


class _Reader:
    """Bounds-checked cursor over TLS wire bytes; truncation refuses."""

    def __init__(self, data: bytes) -> None:
        self.data = data
        self.offset = 0

    @property
    def remaining(self) -> int:
        return len(self.data) - self.offset

    def take(self, count: int) -> bytes:
        if count > self.remaining:
            raise ProxyRefusal(0, "truncated ClientHello")
        chunk = self.data[self.offset:self.offset + count]
        self.offset += count
        return chunk

    def number(self, width: int) -> int:
        return int.from_bytes(self.take(width), "big")

    def vector(self, length_width: int) -> bytes:
        return self.take(self.number(length_width))

    def end(self) -> None:
        if self.remaining:
            raise ProxyRefusal(0, "trailing bytes in ClientHello")


def tls_record_length(prefix: bytes) -> int | None:
    """Total bytes of the first TLS record, once its header has arrived."""
    if len(prefix) < TLS_RECORD_HEADER_BYTES:
        return None
    if prefix[0] != TLS_CONTENT_HANDSHAKE or prefix[1] != 3:
        raise ProxyRefusal(0, "first tunnel bytes are not a TLS handshake record")
    payload = int.from_bytes(prefix[3:5], "big")
    if not 0 < payload <= MAX_TLS_RECORD_PAYLOAD:
        raise ProxyRefusal(0, "TLS record length is out of range")
    return TLS_RECORD_HEADER_BYTES + payload


def _sni_host_name(data: bytes) -> str:
    extension = _Reader(data)
    names = _Reader(extension.vector(2))
    extension.end()
    host_names = []
    while names.remaining:
        name_type = names.number(1)
        name = names.vector(2)
        if name_type == SNI_HOST_NAME:
            host_names.append(name)
    if len(host_names) != 1:
        raise ProxyRefusal(0, "ClientHello must name exactly one SNI host")
    try:
        return host_names[0].decode("ascii").lower()
    except UnicodeDecodeError:
        raise ProxyRefusal(0, "SNI host name is not ASCII") from None


def client_hello_server_name(record: bytes) -> str:
    """Return the SNI host of a complete first TLS record, or refuse.

    The whole ClientHello must sit in this one record; a hello fragmented
    across records is refused rather than reassembled.
    """
    length = tls_record_length(record)
    if length is None or len(record) < length:
        raise ProxyRefusal(0, "incomplete TLS record")
    handshake = _Reader(record[TLS_RECORD_HEADER_BYTES:length])
    if handshake.number(1) != TLS_HANDSHAKE_CLIENT_HELLO:
        raise ProxyRefusal(0, "first handshake message is not a ClientHello")
    hello = _Reader(handshake.vector(3))
    handshake.end()
    hello.take(2 + 32)  # legacy_version, random
    hello.vector(1)  # legacy_session_id
    hello.vector(2)  # cipher_suites
    hello.vector(1)  # legacy_compression_methods
    if not hello.remaining:
        raise ProxyRefusal(0, "ClientHello has no extensions")
    extensions = _Reader(hello.vector(2))
    hello.end()
    seen: set[int] = set()
    server_name: str | None = None
    while extensions.remaining:
        kind = extensions.number(2)
        data = extensions.vector(2)
        if kind in seen:
            raise ProxyRefusal(0, f"duplicate TLS extension {kind:#06x}")
        seen.add(kind)
        if kind in REFUSED_TLS_EXTENSIONS:
            raise ProxyRefusal(0, f"ClientHello carries {REFUSED_TLS_EXTENSIONS[kind]}")
        if kind == TLS_EXTENSION_SERVER_NAME:
            server_name = _sni_host_name(data)
    if server_name is None:
        raise ProxyRefusal(0, "ClientHello has no SNI")
    return server_name


def client_hello_record(
    server_name: str | None, *, extra_extensions: Sequence[tuple[int, bytes]] = ()
) -> bytes:
    """Build a minimal ClientHello record for offline probes and tests.

    It is only well-formed enough for ``client_hello_server_name``; no server
    could complete a handshake with it.
    """
    extensions = b""
    if server_name is not None:
        name = server_name.encode("ascii")
        entry = bytes([SNI_HOST_NAME]) + len(name).to_bytes(2, "big") + name
        body = len(entry).to_bytes(2, "big") + entry
        extensions += TLS_EXTENSION_SERVER_NAME.to_bytes(2, "big")
        extensions += len(body).to_bytes(2, "big") + body
    for kind, data in extra_extensions:
        extensions += kind.to_bytes(2, "big") + len(data).to_bytes(2, "big") + data
    hello = (
        b"\x03\x03" + bytes(32) + b"\x00"
        + b"\x00\x02\x13\x01" + b"\x01\x00"
        + len(extensions).to_bytes(2, "big") + extensions
    )
    handshake = bytes([TLS_HANDSHAKE_CLIENT_HELLO]) + len(hello).to_bytes(3, "big") + hello
    return (
        bytes([TLS_CONTENT_HANDSHAKE]) + b"\x03\x01"
        + len(handshake).to_bytes(2, "big") + handshake
    )


def _status_reply(status: int) -> bytes:
    return (
        f"HTTP/1.1 {status} {STATUS_REASONS[status]}\r\n"
        + ("" if status == 200 else "Connection: close\r\nContent-Length: 0\r\n")
        + "\r\n"
    ).encode("ascii")


def _default_log(message: str) -> None:
    print(f"van-compute-registry-proxy: {message}", file=sys.stderr, flush=True)


def _bind_listener(family: int, address: str, port: int) -> socket.socket:
    listener = socket.socket(family, socket.SOCK_STREAM)
    try:
        if family == socket.AF_INET6:
            listener.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
        listener.bind((address, port))
        return listener
    except BaseException:
        listener.close()
        raise


def bind_dual_stack() -> list[socket.socket]:
    """Listen on one port on both 127.0.0.1 and ::1, or fail closed.

    The IPv4 bind picks an ephemeral port; if ::1 already has that port taken,
    retry with a fresh one rather than leave a granted port to someone else.
    """
    for _attempt in range(BIND_ATTEMPTS):
        ipv4 = _bind_listener(socket.AF_INET, PROXY_ADDRESS_V4, 0)
        port = ipv4.getsockname()[1]
        try:
            ipv6 = _bind_listener(socket.AF_INET6, PROXY_ADDRESS_V6, port)
        except OSError as exc:
            ipv4.close()
            if exc.errno == errno.EADDRINUSE:
                continue
            raise
        listeners = [ipv4, ipv6]
        for listener in listeners:
            listener.listen(LISTEN_BACKLOG)
            listener.setblocking(False)
        return listeners
    raise OSError(
        errno.EADDRINUSE,
        "could not hold one registry proxy port on both 127.0.0.1 and ::1",
    )


class RegistryProxy:
    """Threaded CONNECT proxy; ``start()`` binds, ``stop()`` closes everything."""

    def __init__(
        self,
        *,
        resolver: Resolver = resolve_public,
        limits: ProxyLimits = ProxyLimits(),
        log: Callable[[str], None] = _default_log,
        socket_factory: Callable[[int, int], socket.socket] = socket.socket,
    ) -> None:
        self._resolver = resolver
        # Upstream sockets come from here so tests can substitute ones whose
        # connect stalls; nothing else uses it.
        self._socket_factory = socket_factory
        self._limits = limits
        self._log = log
        self._slots = threading.BoundedSemaphore(limits.max_tunnels)
        self._lock = threading.Lock()
        self._active: set[socket.socket] = set()
        self._handlers: set[threading.Thread] = set()
        self._listeners: list[socket.socket] = []
        self._accept_thread: threading.Thread | None = None
        self._wake_reader: socket.socket | None = None
        self._wake_writer: socket.socket | None = None
        self._stopping = threading.Event()
        self._resolvers: ResolverPool | None = None
        self.port: int | None = None

    def __enter__(self) -> "RegistryProxy":
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.stop()

    def start(self) -> int:
        if self._accept_thread is not None:
            raise RuntimeError("registry proxy already started")
        self._listeners = bind_dual_stack()
        self.port = self._listeners[0].getsockname()[1]
        self._wake_reader, self._wake_writer = socket.socketpair()
        self._wake_reader.setblocking(False)
        self._resolvers = ResolverPool(self._resolver, self._limits.resolver_threads)
        self._accept_thread = threading.Thread(
            target=self._accept_loop, name="van-compute-registry-proxy", daemon=True
        )
        self._accept_thread.start()
        return self.port

    def stop(self) -> None:
        """Stop accepting and end every tunnel, waiting briefly for handlers."""
        self._stopping.set()
        if self._wake_writer is not None:
            try:
                self._wake_writer.send(b"x")
            except OSError:
                pass
        if self._accept_thread is not None:
            self._accept_thread.join(STOP_JOIN_SECONDS)
        with self._lock:
            active = list(self._active)
            handlers = list(self._handlers)
        for connection in active:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        deadline = time.monotonic() + STOP_JOIN_SECONDS
        for handler in handlers:
            handler.join(max(0.0, deadline - time.monotonic()))
        # Handlers close what they own; this catches any still stuck after
        # the join, so stop() never leaves a descriptor open.
        with self._lock:
            leftover = list(self._active)
            self._active.clear()
        for item in (*leftover, *self._listeners, self._wake_reader, self._wake_writer):
            if item is not None:
                item.close()
        if self._resolvers is not None:
            self._resolvers.close()

    def _accept_loop(self) -> None:
        selector = selectors.DefaultSelector()
        try:
            for listener in self._listeners:
                selector.register(listener, selectors.EVENT_READ)
            selector.register(self._wake_reader, selectors.EVENT_READ)
            while not self._stopping.is_set():
                for key, _events in selector.select():
                    if key.fileobj is self._wake_reader or self._stopping.is_set():
                        return
                    try:
                        self._accept_one(key.fileobj)
                    except (OSError, RuntimeError) as exc:
                        # Out of descriptors or threads: keep serving, but
                        # pause so a still-readable listener cannot spin.
                        self._log(f"accept failed: {type(exc).__name__}: {exc}")
                        self._stopping.wait(0.1)
        except Exception as exc:  # the worker must outlive any proxy fault
            self._log(f"accept loop stopped: {type(exc).__name__}: {exc}")
        finally:
            selector.close()

    def _accept_one(self, listener: socket.socket) -> None:
        try:
            connection, _peer = listener.accept()
        except (BlockingIOError, InterruptedError, ConnectionAbortedError):
            return
        # Admission happens before any thread, read or name lookup.
        if not self._slots.acquire(blocking=False):
            self._refuse_busy(connection)
            return
        handler = threading.Thread(
            target=self._serve_admitted,
            args=(connection,),
            name="van-compute-registry-tunnel",
            daemon=True,
        )
        with self._lock:
            self._active.add(connection)
            self._handlers.add(handler)
        try:
            handler.start()
        except BaseException:
            self._forget([connection], handler)
            self._slots.release()
            connection.close()
            raise

    def _refuse_busy(self, connection: socket.socket) -> None:
        try:
            connection.setblocking(False)
            connection.send(_status_reply(503))
        except OSError:
            pass
        finally:
            connection.close()
        self._log(f"refused 503: more than {self._limits.max_tunnels} tunnels")

    def _forget(
        self,
        connections: Sequence[socket.socket | None],
        handler: threading.Thread | None = None,
    ) -> None:
        with self._lock:
            for connection in connections:
                if connection is not None:
                    self._active.discard(connection)
            if handler is not None:
                self._handlers.discard(handler)

    def _serve_admitted(self, client: socket.socket) -> None:
        # Every socket this tunnel opens joins ``owned`` the moment it exists,
        # so each exit path, an exception included, closes it here.
        owned = [client]
        try:
            self._handle(client, owned)
        except Exception as exc:  # one bad tunnel never reaches the worker
            self._log(f"tunnel error: {type(exc).__name__}: {exc}")
        finally:
            self._forget(owned, threading.current_thread())
            for item in owned:
                item.close()
            self._slots.release()

    def _handle(self, client: socket.socket, owned: list[socket.socket]) -> None:
        started = time.monotonic()
        # One absolute deadline caps every phase; phase timeouts only shorten it.
        lifetime_ends = started + self._limits.max_lifetime
        try:
            head, early = self._read_head(client, lifetime_ends)
            host = parse_connect_head(head)
            addresses = self._resolve(host, min(
                lifetime_ends, time.monotonic() + self._limits.resolve_timeout
            ))
        except ProxyRefusal as refusal:
            self._reply(client, refusal.status, lifetime_ends)
            self._log(f"refused {refusal.status}: {refusal.reason}")
            return
        client.settimeout(max(0.001, lifetime_ends - time.monotonic()))
        client.sendall(_status_reply(200))
        try:
            hello = self._read_hello(client, early, lifetime_ends)
            server_name = client_hello_server_name(hello)
            if server_name != host:
                raise ProxyRefusal(0, f"SNI {server_name[:80]!r} does not match {host}")
        except ProxyRefusal as refusal:
            self._log(f"closed {host}: {refusal.reason}")
            return
        upstream = self._connect_upstream(host, addresses, owned, min(
            lifetime_ends, time.monotonic() + self._limits.connect_timeout
        ))
        if upstream is None:
            return
        outcome = relay(
            client, upstream, hello, deadline=lifetime_ends,
            idle_timeout=self._limits.idle_timeout, stopping=self._stopping,
        )
        self._log(
            f"tunnel host={host} sent={outcome.sent} received={outcome.received} "
            f"seconds={time.monotonic() - started:.1f} end={outcome.ending}"
        )

    def _resolve(self, host: str, deadline: float) -> Sequence[tuple[str, int]]:
        """Wait for a pooled lookup until ``deadline`` or shutdown, then give up."""
        if self._resolvers is None:
            raise RuntimeError("registry proxy is not started")
        future = self._resolvers.submit(host, REGISTRY_PORT)
        try:
            while True:
                remaining = deadline - time.monotonic()
                if self._stopping.is_set():
                    raise ProxyRefusal(503, "proxy stopping")
                if remaining <= 0:
                    raise ProxyRefusal(504, f"resolving {host} timed out")
                try:
                    return future.result(min(remaining, RESOLVE_POLL_SECONDS))
                except FutureTimeout:
                    continue
        finally:
            future.cancel()  # a still-queued lookup never runs; a late one is discarded

    def _reply(self, client: socket.socket, status: int, deadline: float) -> None:
        try:
            client.settimeout(max(0.001, deadline - time.monotonic()))
            client.sendall(_status_reply(status))
        except OSError:
            pass

    def _read_until(
        self,
        client: socket.socket,
        buffer: bytes,
        deadline: float,
        done: Callable[[bytes], bool],
        limit: int,
    ) -> bytes:
        while not done(buffer):
            if len(buffer) >= limit:
                raise ProxyRefusal(431, "request head exceeds the size limit")
            remaining = deadline - time.monotonic()
            if remaining <= 0 or self._stopping.is_set():
                raise ProxyRefusal(408, "client was too slow")
            client.settimeout(remaining)
            try:
                chunk = client.recv(limit - len(buffer))
            except socket.timeout:
                raise ProxyRefusal(408, "client was too slow") from None
            if not chunk:
                raise ProxyRefusal(400, "client closed before finishing")
            buffer += chunk
        return buffer

    def _read_head(
        self, client: socket.socket, lifetime_ends: float
    ) -> tuple[bytes, bytes]:
        deadline = min(lifetime_ends, time.monotonic() + self._limits.head_timeout)
        buffer = self._read_until(
            client, b"", deadline, lambda data: b"\r\n\r\n" in data, MAX_HEAD_BYTES
        )
        head, _separator, early = buffer.partition(b"\r\n\r\n")
        return head, early

    def _read_hello(
        self, client: socket.socket, early: bytes, lifetime_ends: float
    ) -> bytes:
        deadline = min(lifetime_ends, time.monotonic() + self._limits.hello_timeout)
        limit = TLS_RECORD_HEADER_BYTES + MAX_TLS_RECORD_PAYLOAD

        def complete(data: bytes) -> bool:
            length = tls_record_length(data)
            return length is not None and len(data) >= length

        if complete(early):
            return early
        return self._read_until(client, early, deadline, complete, limit)

    def _connect_upstream(
        self,
        host: str,
        addresses: Sequence[tuple[str, int]],
        owned: list[socket.socket],
        deadline: float,
    ) -> socket.socket | None:
        """Connect to the first vetted address that answers before ``deadline``;
        never re-resolve. Each socket is owned by the tunnel as soon as it exists."""
        for address, port in addresses:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._log(f"upstream {host} connect deadline passed")
                return None
            family = (
                socket.AF_INET6
                if ipaddress.ip_address(address).version == 6
                else socket.AF_INET
            )
            upstream = self._socket_factory(family, socket.SOCK_STREAM)
            owned.append(upstream)
            # Registered under the lock stop() snapshots with, after the stop
            # flag check, so stop() always sees and shuts down this socket.
            with self._lock:
                if self._stopping.is_set():
                    return None
                self._active.add(upstream)
            upstream.settimeout(remaining)
            try:
                upstream.connect((address, port))
                return upstream
            except OSError as exc:
                self._log(f"upstream {host} {address} failed: {exc}")
                self._forget([upstream])
                owned.remove(upstream)
                upstream.close()
        return None


@contextmanager
def sandbox_registry_proxy(enabled: bool) -> Iterator[int | None]:
    """Run a proxy for the duration when sandboxed jobs need one.

    Yields its port, or None when jobs run without a sandbox profile; a proxy
    that cannot bind raises, so sandboxed mode never starts without network.
    """
    if not enabled:
        yield None
        return
    proxy = RegistryProxy()
    proxy.start()
    try:
        yield proxy.port
    finally:
        proxy.stop()


__all__ = [
    "REGISTRY_HOSTS",
    "ProxyLimits",
    "ProxyRefusal",
    "RegistryProxy",
    "child_environment",
    "client_hello_record",
    "client_hello_server_name",
    "is_public_unicast",
    "parse_connect_head",
    "resolve_public",
    "sandbox_registry_proxy",
]
