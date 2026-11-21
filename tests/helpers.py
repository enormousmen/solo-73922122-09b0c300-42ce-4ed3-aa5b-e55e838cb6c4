"""Shared helpers for the gateway tests."""

from __future__ import annotations

import asyncio
import itertools
import socket

_port_counter = itertools.count(30000)


def next_port() -> int:
    """Deterministic, unique-per-run UDP port for test channels."""
    return next(_port_counter)


def free_udp_port() -> int:
    """A currently-free UDP port (small inherent race, fine for tests)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.bind(("", 0))
        return s.getsockname()[1]
    finally:
        s.close()


class UdpSink:
    """A unicast UDP receiver standing in for a subscriber (播出端)."""

    def __init__(self, ip: str = "127.0.0.1") -> None:
        self.ip = ip
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((ip, 0))
        self.sock.setblocking(False)
        self.port = self.sock.getsockname()[1]

    def endpoint(self) -> dict:
        return {"ip": self.ip, "port": self.port}

    async def recv(self, timeout: float = 2.0) -> bytes | None:
        loop = asyncio.get_running_loop()
        try:
            data, _ = await asyncio.wait_for(
                loop.sock_recvfrom(self.sock, 65535), timeout=timeout
            )
            return data
        except TimeoutError:
            return None

    async def drain(self, quiet: float = 0.2, overall: float = 2.0) -> list[bytes]:
        """Read until no datagram arrives for `quiet` seconds."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + overall
        out: list[bytes] = []
        while loop.time() < deadline:
            msg = await self.recv(timeout=quiet)
            if msg is None:
                break
            out.append(msg)
        return out

    async def collect(self, duration: float) -> list[bytes]:
        """Collect every datagram arriving within `duration` seconds."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + duration
        out: list[bytes] = []
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            msg = await self.recv(timeout=remaining)
            if msg is None:
                break
            out.append(msg)
        return out

    def close(self) -> None:
        self.sock.close()


def sub_body(sink: UdpSink, group: str, port: int) -> dict:
    return {
        "subscriber": sink.endpoint(),
        "channel": {"group": group, "port": port},
    }
