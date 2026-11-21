"""Pytest fixtures for the gateway tests."""

from __future__ import annotations

import socket
import struct

import httpx
import pytest

from app.main import create_app
from app.relay import RelayManager


def _default_interface_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("192.0.2.1", 80))  # UDP: no traffic is actually sent
        return s.getsockname()[0]
    finally:
        s.close()


def _multicast_loopback_ok(if_ip: str) -> bool:
    group, port = "239.254.254.1", 45999
    try:
        rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        try:
            rx.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            rx.bind(("", port))
            rx.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_ADD_MEMBERSHIP,
                struct.pack("=4s4s", socket.inet_aton(group), socket.inet_aton(if_ip)),
            )
            rx.settimeout(1.0)
            tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            try:
                tx.setsockopt(
                    socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(if_ip)
                )
                for _ in range(3):
                    tx.sendto(b"probe", (group, port))
                    try:
                        rx.recvfrom(64)
                        return True
                    except socket.timeout:
                        continue
                return False
            finally:
                tx.close()
        finally:
            rx.close()
    except OSError:
        return False


@pytest.fixture(scope="session")
def if_ip() -> str:
    return _default_interface_ip()


@pytest.fixture(scope="session", autouse=True)
def _require_multicast(if_ip: str) -> None:
    if not _multicast_loopback_ok(if_ip):
        pytest.skip("IPv4 multicast loopback is not available in this environment")


@pytest.fixture
async def relay(if_ip: str):
    manager = RelayManager(interface=if_ip, join_timeout=3.0)
    await manager.start()
    try:
        yield manager
    finally:
        await manager.close()


@pytest.fixture
async def client(relay: RelayManager):
    app = create_app()
    app.state.relay = relay  # lifespan is not run under ASGITransport
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as c:
        yield c


@pytest.fixture(scope="session")
def sender(if_ip: str):
    """A multicast sender bound to the default interface."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(if_ip))
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
    yield sock
    sock.close()
