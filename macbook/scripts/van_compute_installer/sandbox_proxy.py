"""Installer probe proving sandboxed jobs reach the network only through the
staged release's registry proxy, without needing the internet."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Iterator

from .models import DeploymentError

HELPER_TIMEOUT = 15.0

# Runs outside the sandbox with the staged interpreter and source. Every
# allowlisted name resolves to a local echo server, so a real tunnel is
# exercised offline; the public-address policy is covered by the test suite.
# On stdin EOF it exits nonzero unless exactly the probe's two tunnels reached
# the echo server, so refused requests provably never got upstream.
PROXY_HOST_PROBE = r'''
import json
import socket
import sys
import threading

from van_compute import registry_proxy

upstream = socket.create_server(("127.0.0.1", 0))
upstream_port = upstream.getsockname()[1]
accepted = []


def echo(connection):
    with connection:
        while True:
            data = connection.recv(65536)
            if not data:
                return
            connection.sendall(data)


def serve():
    while True:
        try:
            connection, _peer = upstream.accept()
        except OSError:
            return
        accepted.append(connection)
        threading.Thread(target=echo, args=(connection,), daemon=True).start()


threading.Thread(target=serve, daemon=True).start()
proxy = registry_proxy.RegistryProxy(
    resolver=lambda host, port: [("127.0.0.1", upstream_port)]
)
proxy.start()
print(json.dumps({"proxy_port": proxy.port, "upstream_port": upstream_port}), flush=True)
sys.stdin.read()
proxy.stop()
upstream.close()
if len(accepted) != 2:
    raise SystemExit(f"echo upstream saw {len(accepted)} connections, expected 2")
'''

# Runs inside the sandbox: the proxy port works on both loopback families and
# relays both ways; the proxy refuses an unlisted host and a wrong port; and
# the profile denies TCP to any other port or address and UDP to the proxy.
PROXY_CLIENT_PROBE = r'''
import errno
import socket
import sys

from van_compute.registry_proxy import client_hello_record

proxy_port, upstream_port = int(sys.argv[1]), int(sys.argv[2])


def proxy_request(family, address, target):
    connection = socket.socket(family, socket.SOCK_STREAM)
    connection.settimeout(10)
    connection.connect((address, proxy_port))
    connection.sendall(f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n".encode())
    head = b""
    while b"\r\n\r\n" not in head:
        chunk = connection.recv(4096)
        if not chunk:
            break
        head += chunk
    return connection, head.split(b"\r\n", 1)[0]


for family, address in ((socket.AF_INET, "127.0.0.1"), (socket.AF_INET6, "::1")):
    connection, status = proxy_request(family, address, "pypi.org:443")
    with connection:
        if not status.startswith(b"HTTP/1.1 200"):
            raise SystemExit(f"proxy refused an allowlisted CONNECT via {address}: {status!r}")
        hello = client_hello_record("pypi.org")
        connection.sendall(hello)
        connection.shutdown(socket.SHUT_WR)
        echoed = b""
        while True:
            chunk = connection.recv(65536)
            if not chunk:
                break
            echoed += chunk
        if echoed != hello:
            raise SystemExit(f"tunnel via {address} did not relay both ways")

for target in ("example.com:443", "pypi.org:80"):
    connection, status = proxy_request(socket.AF_INET, "127.0.0.1", target)
    connection.close()
    if not status.startswith(b"HTTP/1.1 403"):
        raise SystemExit(f"proxy did not refuse CONNECT {target}: {status!r}")


def require_denied(label, family, kind, address):
    probe = socket.socket(family, kind)
    probe.settimeout(10)
    try:
        if kind == socket.SOCK_DGRAM:
            probe.sendto(b"x", address)
        else:
            probe.connect(address)
    except OSError as exc:
        if exc.errno in {errno.EACCES, errno.EPERM}:
            return
        raise
    finally:
        probe.close()
    raise SystemExit(f"sandbox allowed {label}")


require_denied("TCP to another loopback port", socket.AF_INET, socket.SOCK_STREAM,
               ("127.0.0.1", upstream_port))
require_denied("TCP to another ::1 port", socket.AF_INET6, socket.SOCK_STREAM,
               ("::1", upstream_port))
require_denied("TCP to a public address", socket.AF_INET, socket.SOCK_STREAM,
               ("1.1.1.1", 443))
require_denied("UDP to the proxy port", socket.AF_INET, socket.SOCK_DGRAM,
               ("127.0.0.1", proxy_port))
require_denied("UDP to the ::1 proxy port", socket.AF_INET6, socket.SOCK_DGRAM,
               ("::1", proxy_port))
'''


@dataclass(frozen=True)
class ProbePorts:
    proxy: int
    upstream: int


def _probe_ports(announcement: str) -> ProbePorts:
    try:
        payload = json.loads(announcement)
        ports = ProbePorts(int(payload["proxy_port"]), int(payload["upstream_port"]))
    except (ValueError, KeyError, TypeError):
        raise DeploymentError(
            f"registry proxy probe announced an invalid port: {announcement[:200]!r}"
        ) from None
    if not all(1 <= port <= 65535 for port in (ports.proxy, ports.upstream)):
        raise DeploymentError("registry proxy probe announced an invalid port")
    return ports


@contextmanager
def probe_proxy(local, staging: Path) -> Iterator[ProbePorts]:
    """Run the staged proxy beside a local echo upstream for every sandbox
    check, since the profile needs a live proxy port to apply at all."""
    # -E -s keep the caller's environment and user site out; the staged
    # source is importable because -c puts the working directory first.
    helper = local.spawn(
        [str(staging / "venv/bin/python"), "-E", "-s", "-B", "-c", PROXY_HOST_PROBE],
        cwd=staging / "app",
    )
    try:
        ports = _probe_ports(helper.announcement(HELPER_TIMEOUT))
        yield ports
    except BaseException:
        helper.close(HELPER_TIMEOUT)
        raise
    helper.finish(HELPER_TIMEOUT)


def run_proxy_probe(
    local, staging: Path, sandbox_test: Path, prefix: list[str], ports: ProbePorts
) -> None:
    # The prefix ends in an `env -i` assignment list, so this extends it.
    local.run(
        prefix
        + [
            f"PYTHONPATH={staging / 'app'}",
            str(staging / "venv/bin/python"),
            "-s",
            "-B",
            "-c",
            PROXY_CLIENT_PROBE,
            str(ports.proxy),
            str(ports.upstream),
        ],
        cwd=sandbox_test,
    )
