"""The registry proxy over real loopback sockets, with a local fake upstream."""

from __future__ import annotations

from contextlib import redirect_stderr
import errno
import functools
import io
import os
import socket
import struct
import threading
import time
import unittest

from pi.tests.compute.van_compute_test_fixtures import (
    IPV6_LOOPBACK_SKIP,
    ipv6_loopback_available,
)

from van_compute import registry_proxy
from van_compute.registry_resolver_pool import ResolverPool
from van_compute.registry_proxy import (
    ProxyLimits, RegistryProxy, client_hello_record, resolve_public,
)

FAST = ProxyLimits(
    max_tunnels=4,
    head_timeout=1.0,
    hello_timeout=1.0,
    connect_timeout=1.0,
    idle_timeout=5.0,
    max_lifetime=10.0,
)


class EchoUpstream:
    """Stands in for a registry: echoes every byte until the client closes."""

    def __init__(self) -> None:
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.port = self.listener.getsockname()[1]
        self.accepted = 0
        self.connections: list[socket.socket] = []
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self) -> None:
        while True:
            try:
                connection, _peer = self.listener.accept()
            except OSError:
                return
            self.accepted += 1
            self.connections.append(connection)
            threading.Thread(target=self._echo, args=(connection,), daemon=True).start()

    @staticmethod
    def _echo(connection: socket.socket) -> None:
        with connection:
            try:
                while data := connection.recv(65536):
                    connection.sendall(data)
            except OSError:
                pass

    def close(self) -> None:
        self.listener.close()
        for connection in self.connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def fake_lookup(*addresses):
    def lookup(host, port, **_options):
        return [
            (socket.AF_INET6 if ":" in address else socket.AF_INET,
             socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (address, port))
            for address in addresses
        ]
    return lookup


def read_all(connection: socket.socket, timeout: float = 5.0) -> bytes:
    connection.settimeout(timeout)
    chunks = []
    while True:
        try:
            data = connection.recv(65536)
        except ConnectionResetError:
            break
        if not data:
            break
        chunks.append(data)
    return b"".join(chunks)


def read_status(connection: socket.socket) -> bytes:
    connection.settimeout(5.0)
    buffer = b""
    while b"\r\n\r\n" not in buffer:
        data = connection.recv(4096)
        if not data:
            break
        buffer += data
    return buffer.split(b"\r\n", 1)[0]


class ResettingUpstream:
    """Accepts and immediately resets, so the proxy's first write can fail."""

    def __init__(self) -> None:
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.port = self.listener.getsockname()[1]
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self) -> None:
        while True:
            try:
                connection, _peer = self.listener.accept()
            except OSError:
                return
            connection.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
            )
            connection.close()


class FloodingUpstream:
    """Reads the ClientHello, then writes as fast as the tunnel accepts."""

    def __init__(self) -> None:
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.port = self.listener.getsockname()[1]
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self) -> None:
        while True:
            try:
                connection, _peer = self.listener.accept()
            except OSError:
                return
            threading.Thread(target=self._flood, args=(connection,), daemon=True).start()

    @staticmethod
    def _flood(connection: socket.socket) -> None:
        with connection:
            try:
                connection.recv(65536)
                while True:
                    connection.sendall(b"x" * 65536)
            except OSError:
                pass


class StalledResolver:
    """Blocks every lookup until released, as a hung getaddrinfo would."""

    def __init__(self, port: int) -> None:
        self.port = port
        self.entered = threading.Event()
        self.release = threading.Event()

    def __call__(self, host, port):
        self.entered.set()
        self.release.wait(30)
        return [("127.0.0.1", self.port)]


def open_descriptors() -> int:
    return len(os.listdir("/dev/fd"))


def resolver_threads() -> int:
    return sum(
        thread.name == "van-compute-registry-resolver" for thread in threading.enumerate()
    )


class ResolverPoolTests(unittest.TestCase):
    def test_queue_is_bounded_and_abandoned_lookups_leave_it(self):
        stalled = StalledResolver(443)
        self.addCleanup(stalled.release.set)
        pool = ResolverPool(stalled, threads=1, max_pending=2)
        self.addCleanup(pool.close)
        running = pool.submit("pypi.org", 443)
        self.assertTrue(stalled.entered.wait(5))
        first, second = pool.submit("pypi.org", 443), pool.submit("pypi.org", 443)
        self.assertEqual(pool.pending, 2)
        self.assertIsNone(pool.submit("pypi.org", 443))  # full: refused, not queued
        pool.abandon(first)
        self.assertEqual(pool.pending, 1)
        self.assertTrue(first.cancelled())
        pool.close()
        self.assertEqual(pool.pending, 0)
        self.assertTrue(second.cancelled())
        self.assertIsNone(pool.submit("pypi.org", 443))
        stalled.release.set()
        self.assertEqual(running.result(5), [("127.0.0.1", 443)])
        pool.abandon(running)  # abandoning a finished lookup is harmless


@unittest.skipUnless(ipv6_loopback_available(), IPV6_LOOPBACK_SKIP)
class RegistryProxyTests(unittest.TestCase):
    def setUp(self):
        self.upstream = EchoUpstream()
        self.addCleanup(self.upstream.close)
        self.log: list[str] = []

    def start_proxy(self, resolver=None, limits=FAST, **options):
        upstream_port = self.upstream.port
        proxy = RegistryProxy(
            resolver=resolver or (lambda host, port: [("127.0.0.1", upstream_port)]),
            limits=limits,
            log=self.log.append,
            **options,
        )
        proxy.start()
        self.addCleanup(proxy.stop)
        return proxy

    def connect(self, proxy, address="127.0.0.1"):
        family = socket.AF_INET6 if ":" in address else socket.AF_INET
        connection = socket.socket(family, socket.SOCK_STREAM)
        self.addCleanup(connection.close)
        connection.settimeout(5.0)
        connection.connect((address, proxy.port))
        return connection

    def request(self, proxy, request_line, address="127.0.0.1"):
        connection = self.connect(proxy, address)
        connection.sendall(request_line.encode() + b"\r\nHost: x\r\n\r\n")
        return connection, read_status(connection)

    def tunnel(self, proxy, host="pypi.org", address="127.0.0.1"):
        connection, status = self.request(proxy, f"CONNECT {host}:443 HTTP/1.1", address)
        self.assertEqual(status, b"HTTP/1.1 200 Connection Established")
        return connection

    def wait_for_log(self, fragment, timeout=5.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if any(fragment in line for line in self.log):
                return
            time.sleep(0.01)
        self.fail(f"no log line containing {fragment!r}: {self.log}")

    def test_allowlisted_tunnel_relays_both_ways_on_both_loopback_families(self):
        proxy = self.start_proxy()
        for address in ("127.0.0.1", "::1"):
            with self.subTest(address=address):
                connection = self.tunnel(proxy, address=address)
                hello = client_hello_record("pypi.org")
                connection.sendall(hello + b"after-hello")
                connection.shutdown(socket.SHUT_WR)
                self.assertEqual(read_all(connection), hello + b"after-hello")
        self.wait_for_log("tunnel host=pypi.org sent=")
        self.assertEqual(self.upstream.accepted, 2)

    def test_host_matching_ignores_case_and_accepts_http_1_0(self):
        proxy = self.start_proxy()
        connection, status = self.request(proxy, "CONNECT Registry.NPMJS.org:443 HTTP/1.0")
        self.assertEqual(status, b"HTTP/1.1 200 Connection Established")
        hello = client_hello_record("registry.npmjs.org")
        connection.sendall(hello)
        connection.shutdown(socket.SHUT_WR)
        self.assertEqual(read_all(connection), hello)

    def test_client_hello_split_across_reads_is_reassembled(self):
        proxy = self.start_proxy()
        connection = self.tunnel(proxy, host="files.pythonhosted.org")
        hello = client_hello_record("files.pythonhosted.org")
        for piece in (hello[:3], hello[3:20], hello[20:]):
            connection.sendall(piece)
            time.sleep(0.05)
        connection.shutdown(socket.SHUT_WR)
        self.assertEqual(read_all(connection), hello)

    def test_targets_outside_the_allowlist_are_forbidden(self):
        proxy = self.start_proxy()
        for target in (
            "example.com:443",
            "151.101.0.223:443",
            "[2a04:4e42::223]:443",
            "pypi.org.:443",
            "evil.pypi.org:443",
            "pypi.org:80",
            "pypi.org:0443",
            "pypi.org:443:443",
        ):
            with self.subTest(target=target):
                _connection, status = self.request(proxy, f"CONNECT {target} HTTP/1.1")
                self.assertEqual(status, b"HTTP/1.1 403 Forbidden")
        self.assertEqual(self.upstream.accepted, 0)

    def test_plain_http_and_malformed_requests_are_refused(self):
        proxy = self.start_proxy()
        cases = {
            "GET http://pypi.org/simple/ HTTP/1.1": b"HTTP/1.1 405 Method Not Allowed",
            "GET / HTTP/1.1": b"HTTP/1.1 405 Method Not Allowed",
            "POST https://pypi.org:443/ HTTP/1.1": b"HTTP/1.1 405 Method Not Allowed",
            "CONNECT pypi.org:443": b"HTTP/1.1 400 Bad Request",
            "CONNECT pypi.org:443 HTTP/2": b"HTTP/1.1 400 Bad Request",
            "CONNECT pypi.org HTTP/1.1": b"HTTP/1.1 400 Bad Request",
        }
        for line, expected in cases.items():
            with self.subTest(line=line):
                _connection, status = self.request(proxy, line)
                self.assertEqual(status, expected)
        self.assertEqual(self.upstream.accepted, 0)

    def test_oversize_head_is_refused(self):
        proxy = self.start_proxy()
        connection = self.connect(proxy)
        connection.sendall(b"CONNECT pypi.org:443 HTTP/1.1\r\nX-Pad: " + b"a" * 9000)
        self.assertEqual(read_status(connection), b"HTTP/1.1 431 Request Header Fields Too Large")
        self.assertEqual(self.upstream.accepted, 0)

    def test_slow_head_times_out(self):
        proxy = self.start_proxy(limits=ProxyLimits(head_timeout=0.3))
        connection = self.connect(proxy)
        connection.sendall(b"CONNECT pypi.org:443 HTTP/1.1\r\n")
        started = time.monotonic()
        self.assertEqual(read_status(connection), b"HTTP/1.1 408 Request Timeout")
        self.assertLess(time.monotonic() - started, 3.0)

    def assert_tunnel_closed_without_upstream(self, proxy, payload, host="pypi.org"):
        connection = self.tunnel(proxy, host=host)
        if payload:
            connection.sendall(payload)
        self.assertEqual(read_all(connection), b"")
        self.assertEqual(self.upstream.accepted, 0)

    def test_client_hello_that_does_not_match_the_connect_host_is_closed(self):
        proxy = self.start_proxy()
        cases = {
            "sni names another host": client_hello_record("files.pythonhosted.org"),
            "sni with trailing dot": client_hello_record("pypi.org."),
            "missing sni": client_hello_record(None),
            "encrypted client hello": client_hello_record(
                "pypi.org", extra_extensions=[(0xFE0D, b"\x00" * 8)]
            ),
            "legacy encrypted sni": client_hello_record(
                "pypi.org", extra_extensions=[(0xFFCE, b"\x00" * 8)]
            ),
            "duplicate sni": client_hello_record(
                "pypi.org", extra_extensions=[(0x0000, b"")]
            ),
            "plain http inside the tunnel": b"GET / HTTP/1.1\r\nHost: pypi.org\r\n\r\n",
            "truncated record then close": client_hello_record("pypi.org")[:-4],
        }
        for label, payload in cases.items():
            with self.subTest(label):
                self.assert_tunnel_closed_without_upstream(proxy, payload)
        self.wait_for_log("does not match pypi.org")
        self.wait_for_log("encrypted ClientHello")

    def test_missing_client_hello_times_out_without_contacting_upstream(self):
        proxy = self.start_proxy(limits=ProxyLimits(hello_timeout=0.3))
        started = time.monotonic()
        self.assert_tunnel_closed_without_upstream(proxy, b"")
        self.assertLess(time.monotonic() - started, 3.0)

    def test_non_public_resolved_addresses_are_refused(self):
        for addresses in (
            ("10.0.0.1",),
            ("192.168.6.1",),
            ("127.0.0.1",),
            ("169.254.1.1",),
            ("100.64.0.1",),
            ("224.0.0.251",),
            ("::1",),
            ("fe80::1",),
            ("ff02::1",),
            ("::ffff:192.168.6.103",),
            ("64:ff9b::a00:1",),
            ("151.101.0.223", "192.168.6.1"),
        ):
            with self.subTest(addresses=addresses):
                proxy = self.start_proxy(
                    resolver=functools.partial(resolve_public, lookup=fake_lookup(*addresses))
                )
                _connection, status = self.request(proxy, "CONNECT pypi.org:443 HTTP/1.1")
                self.assertEqual(status, b"HTTP/1.1 403 Forbidden")
        self.assertEqual(self.upstream.accepted, 0)

    def test_public_resolution_keeps_vetted_addresses_and_reports_failures(self):
        lookup = fake_lookup("151.101.0.223", "2a04:4e42::223", "151.101.0.223")
        self.assertEqual(
            resolve_public("pypi.org", 443, lookup=lookup),
            [("151.101.0.223", 443), ("2a04:4e42::223", 443)],
        )

        def failing(*_args, **_kwargs):
            raise socket.gaierror(socket.EAI_NONAME, "unknown")

        proxy = self.start_proxy(resolver=functools.partial(resolve_public, lookup=failing))
        _connection, status = self.request(proxy, "CONNECT pypi.org:443 HTTP/1.1")
        self.assertEqual(status, b"HTTP/1.1 502 Bad Gateway")

    def test_unreachable_upstream_closes_the_tunnel(self):
        unused = socket.create_server(("127.0.0.1", 0))
        port = unused.getsockname()[1]
        unused.close()
        proxy = self.start_proxy(resolver=lambda host, _port: [("127.0.0.1", port)])
        connection = self.tunnel(proxy)
        connection.sendall(client_hello_record("pypi.org"))
        self.assertEqual(read_all(connection), b"")
        self.wait_for_log("upstream pypi.org 127.0.0.1 failed")

    def test_admission_cap_refuses_excess_connections_at_once(self):
        proxy = self.start_proxy(limits=ProxyLimits(max_tunnels=2))
        held = [self.tunnel(proxy), self.tunnel(proxy)]
        excess = self.connect(proxy)
        self.assertEqual(read_status(excess), b"HTTP/1.1 503 Service Unavailable")
        held[0].close()
        deadline = time.monotonic() + 5
        while True:
            connection, status = self.request(proxy, "CONNECT pypi.org:443 HTTP/1.1")
            if status.startswith(b"HTTP/1.1 200") or time.monotonic() > deadline:
                break
            connection.close()
            time.sleep(0.05)
        self.assertEqual(status, b"HTTP/1.1 200 Connection Established")

    def test_idle_tunnel_is_closed(self):
        proxy = self.start_proxy(limits=ProxyLimits(idle_timeout=0.3))
        connection = self.tunnel(proxy)
        hello = client_hello_record("pypi.org")
        connection.sendall(hello)
        started = time.monotonic()
        self.assertEqual(read_all(connection), hello)
        self.assertLess(time.monotonic() - started, 3.0)
        self.wait_for_log("end=idle timeout")

    def test_busy_tunnel_ends_at_its_lifetime_limit(self):
        proxy = self.start_proxy(limits=ProxyLimits(idle_timeout=5.0, max_lifetime=0.5))
        connection = self.tunnel(proxy)
        connection.sendall(client_hello_record("pypi.org"))
        started = time.monotonic()
        connection.settimeout(0.1)
        while time.monotonic() - started < 5.0:
            try:
                connection.sendall(b"keepalive")
                if connection.recv(65536) == b"":
                    break
            except socket.timeout:
                continue
            except (BrokenPipeError, ConnectionResetError):
                break
            time.sleep(0.05)
        self.assertLess(time.monotonic() - started, 3.0)
        self.wait_for_log("end=lifetime limit")

    def test_stop_closes_active_tunnels_and_listeners(self):
        proxy = self.start_proxy()
        connection = self.tunnel(proxy)
        hello = client_hello_record("pypi.org")
        connection.sendall(hello)
        connection.settimeout(5.0)
        self.assertEqual(connection.recv(65536), hello)
        port = proxy.port
        started = time.monotonic()
        proxy.stop()
        self.assertLess(time.monotonic() - started, 3.0)
        self.assertEqual(read_all(connection), b"")
        with self.assertRaises(ConnectionRefusedError):
            socket.create_connection(("127.0.0.1", port), timeout=2).close()

    def test_proxy_holds_its_port_on_both_loopback_families(self):
        proxy = self.start_proxy()
        for family, address in ((socket.AF_INET, "127.0.0.1"), (socket.AF_INET6, "::1")):
            with self.subTest(address=address):
                rival = socket.socket(family, socket.SOCK_STREAM)
                self.addCleanup(rival.close)
                with self.assertRaises(OSError) as raised:
                    rival.bind((address, proxy.port))
                self.assertEqual(raised.exception.errno, errno.EADDRINUSE)

    def test_upstream_that_resets_leaves_no_descriptor_open(self):
        resetting = ResettingUpstream()
        self.addCleanup(resetting.listener.close)
        before = open_descriptors()
        proxy = RegistryProxy(
            resolver=lambda host, _port: [("127.0.0.1", resetting.port)],
            limits=FAST, log=self.log.append,
        )
        proxy.start()
        for _attempt in range(5):
            with socket.create_connection(("127.0.0.1", proxy.port), timeout=5) as client:
                client.sendall(b"CONNECT pypi.org:443 HTTP/1.1\r\n\r\n")
                self.assertEqual(read_status(client), b"HTTP/1.1 200 Connection Established")
                client.sendall(client_hello_record("pypi.org"))
                read_all(client)
        proxy.stop()
        self.assertLessEqual(open_descriptors(), before)

    def test_failed_first_upstream_write_closes_the_upstream_socket(self):
        # Deterministic form of the reset race: the first write always fails,
        # with a socket error and with an unexpected exception.
        for failure in (ConnectionResetError(54, "reset"), RuntimeError("boom")):
            with self.subTest(failure=type(failure).__name__):
                created = []

                class FailingSocket(socket.socket):
                    def send(self, *_args):
                        raise failure

                    sendall = send

                def factory(family, kind):
                    created.append(FailingSocket(family, kind))
                    return created[-1]

                before = open_descriptors()
                proxy = RegistryProxy(
                    resolver=lambda host, _port: [("127.0.0.1", self.upstream.port)],
                    limits=FAST, log=self.log.append, socket_factory=factory,
                )
                proxy.start()
                with socket.create_connection(("127.0.0.1", proxy.port), timeout=5) as client:
                    client.sendall(b"CONNECT pypi.org:443 HTTP/1.1\r\n\r\n")
                    self.assertEqual(read_status(client), b"HTTP/1.1 200 Connection Established")
                    client.sendall(client_hello_record("pypi.org"))
                    self.assertEqual(read_all(client), b"")
                proxy.stop()
                self.assertEqual([item.fileno() for item in created], [-1])
                self.assertLessEqual(open_descriptors(), before)

    def test_stalled_lookup_releases_its_slot_at_the_resolve_deadline(self):
        stalled = StalledResolver(self.upstream.port)
        self.addCleanup(stalled.release.set)
        proxy = self.start_proxy(
            resolver=stalled,
            limits=ProxyLimits(max_tunnels=1, resolve_timeout=0.3, resolver_threads=1),
        )
        for _attempt in range(3):
            started = time.monotonic()
            _connection, status = self.request(proxy, "CONNECT pypi.org:443 HTTP/1.1")
            # Admitted every time (never 503): the stalled lookup gave its slot back.
            self.assertEqual(status, b"HTTP/1.1 504 Gateway Timeout")
            self.assertLess(time.monotonic() - started, 2.0)
        self.assertEqual(resolver_threads(), 1)
        stalled.release.set()
        time.sleep(0.2)
        self.assertEqual(self.upstream.accepted, 0)  # the late answer was discarded

    def test_stuck_resolvers_never_accumulate_abandoned_lookups(self):
        stalled = StalledResolver(self.upstream.port)
        self.addCleanup(stalled.release.set)
        proxy = RegistryProxy(
            resolver=stalled,
            limits=ProxyLimits(max_tunnels=2, resolve_timeout=0.01, resolver_threads=1),
            log=self.log.append,
        )
        proxy.start()
        self.addCleanup(proxy.stop)
        started = time.monotonic()
        for attempt in range(120):
            request_started = time.monotonic()
            with socket.create_connection(("127.0.0.1", proxy.port), timeout=5) as client:
                client.sendall(b"CONNECT pypi.org:443 HTTP/1.1\r\n\r\n")
                status = read_status(client)
            self.assertEqual(status, b"HTTP/1.1 504 Gateway Timeout", attempt)
            self.assertLess(time.monotonic() - request_started, 1.0, attempt)
            self.assertLessEqual(proxy.pending_lookups, 2, attempt)
        self.assertLess(time.monotonic() - started, 30.0)
        self.assertTrue(stalled.entered.is_set())
        stopped = time.monotonic()
        proxy.stop()
        self.assertLess(time.monotonic() - stopped, 2.0)
        self.assertEqual(proxy.pending_lookups, 0)
        stalled.release.set()
        deadline = time.monotonic() + 5
        while resolver_threads() and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertEqual(resolver_threads(), 0)
        self.assertEqual(self.upstream.accepted, 0)

    def test_stop_abandons_a_stalled_lookup(self):
        stalled = StalledResolver(self.upstream.port)
        self.addCleanup(stalled.release.set)
        proxy = self.start_proxy(
            resolver=stalled, limits=ProxyLimits(resolve_timeout=30.0)
        )
        connection = self.connect(proxy)
        connection.sendall(b"CONNECT pypi.org:443 HTTP/1.1\r\n\r\n")
        self.assertTrue(stalled.entered.wait(5))
        started = time.monotonic()
        proxy.stop()
        self.assertLess(time.monotonic() - started, 2.0)
        self.assertIn(read_status(connection), (b"HTTP/1.1 503 Service Unavailable", b""))
        stalled.release.set()
        time.sleep(0.2)
        self.assertEqual(self.upstream.accepted, 0)

    def test_client_that_stops_reading_loses_its_slot_at_the_lifetime_limit(self):
        flooding = FloodingUpstream()
        self.addCleanup(flooding.listener.close)
        proxy = self.start_proxy(
            resolver=lambda host, _port: [("127.0.0.1", flooding.port)],
            limits=ProxyLimits(max_tunnels=1, idle_timeout=5.0, max_lifetime=0.5),
        )
        stalled_reader = self.tunnel(proxy)
        stalled_reader.sendall(client_hello_record("pypi.org"))
        started = time.monotonic()
        self.wait_for_log("end=lifetime limit", timeout=3.0)
        self.assertLess(time.monotonic() - started, 2.0)
        deadline = time.monotonic() + 3
        while True:  # the slot is free again for the next client
            connection, status = self.request(proxy, "CONNECT pypi.org:443 HTTP/1.1")
            if status.startswith(b"HTTP/1.1 200") or time.monotonic() > deadline:
                break
            connection.close()
            time.sleep(0.05)
        self.assertEqual(status, b"HTTP/1.1 200 Connection Established")

    def test_connect_attempts_share_one_deadline(self):
        timeouts = []

        class StallingSocket(socket.socket):
            def connect(self, address):
                timeouts.append(self.gettimeout())
                time.sleep(self.gettimeout())
                raise TimeoutError("timed out")

        proxy = self.start_proxy(
            resolver=lambda host, _port: [("127.0.0.1", 9), ("127.0.0.2", 9), ("::1", 9)],
            limits=ProxyLimits(connect_timeout=0.6),
            socket_factory=StallingSocket,
        )
        connection = self.tunnel(proxy)
        connection.sendall(client_hello_record("pypi.org"))
        started = time.monotonic()
        self.assertEqual(read_all(connection), b"")
        self.assertLess(time.monotonic() - started, 1.2)
        self.assertLessEqual(sum(timeouts), 0.6 + 0.05)
        self.wait_for_log("connect deadline passed")

    def test_child_environment_names_the_proxy_only_when_it_runs(self):
        self.assertEqual(registry_proxy.child_environment(None), {})
        self.assertEqual(
            registry_proxy.child_environment(4321),
            {
                "HTTPS_PROXY": "http://127.0.0.1:4321",
                "https_proxy": "http://127.0.0.1:4321",
                "PIP_USE_DEPRECATED": "legacy-certs",
            },
        )

    def test_sandbox_context_runs_a_proxy_only_when_enabled(self):
        with registry_proxy.sandbox_registry_proxy(False) as port:
            self.assertIsNone(port)
        log = io.StringIO()
        with redirect_stderr(log), registry_proxy.sandbox_registry_proxy(True) as port:
            socket.create_connection(("127.0.0.1", port), timeout=2).close()
        with self.assertRaises(ConnectionRefusedError):
            socket.create_connection(("127.0.0.1", port), timeout=2).close()


if __name__ == "__main__":
    unittest.main()
