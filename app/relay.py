"""Multicast relay engine.

Terminology
-----------
* **channel** -- identified by ``(multicast group, UDP port)``.  Channels
  that share a port but use different groups are fully independent: each
  channel owns exactly one socket and one group membership.
* **subscriber** -- identified by a unicast ``(ip, UDP port)`` pair.  Every
  datagram received on a channel is forwarded to all of its subscribers.

Guarantees
----------
* The first subscriber of a channel triggers exactly one
  ``IP_ADD_MEMBERSHIP``; the membership is dropped (``IP_DROP_MEMBERSHIP``
  plus socket close) only when the *last* subscriber leaves.
* ``switch`` first makes the *new* channel ready to receive -- joined the
  group **and** received its first datagram within ``join_timeout`` -- and
  only then moves the subscriber.  If the join fails or no datagram
  arrives in time, the old subscription is left untouched and the old
  channel keeps being relayed (the switch answers 502).
* After a successful switch or unsubscribe the endpoint does not receive a
  single further datagram of the old channel: the subscriber-set mutations
  are applied synchronously (no ``await`` in between) inside the event
  loop, so the forwarding callback always observes a consistent state.
* All public operations are serialized through one ``asyncio.Lock`` so
  concurrent API calls cannot create duplicate channels or leak
  memberships.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
import struct
import time

Subscriber = tuple[str, int]  # unicast (ip, port)
ChannelKey = tuple[str, int]  # (multicast group, port)

# Linux IP_MULTICAST_ALL; CPython's socket module does not expose it.
# Default is 1, which would leak datagrams of groups joined anywhere on
# the host into every same-port socket -- it must be disabled per socket
# so that channels sharing a port but using different groups stay separate.
_IP_MULTICAST_ALL = getattr(socket, "IP_MULTICAST_ALL", 49)


class RelayError(Exception):
    """Base class for relay errors."""


class ConflictError(RelayError):
    """The request conflicts with the current subscription state."""


class NotFoundError(RelayError):
    """The requested subscription does not exist."""


class JoinError(RelayError):
    """A channel could not be made ready to receive datagrams."""


def _mreq(group: str, interface: str) -> bytes:
    return struct.pack("=4s4s", socket.inet_aton(group), socket.inet_aton(interface))


class _ChannelProtocol(asyncio.DatagramProtocol):
    """Pushes received datagrams into :class:`Channel`."""

    __slots__ = ("_channel",)

    def __init__(self, channel: "Channel") -> None:
        self._channel = channel

    def datagram_received(self, data: bytes, addr) -> None:
        self._channel._on_datagram(data)

    def error_received(self, exc: Exception) -> None:
        self._channel.send_errors += 1


class Channel:
    """One joined multicast group plus the subscribers attached to it."""

    def __init__(self, manager: "RelayManager", key: ChannelKey) -> None:
        self.manager = manager
        self.key = key
        self.group, self.port = key
        self.subscribers: set[Subscriber] = set()
        self.transport: asyncio.DatagramTransport | None = None
        self.created_at = time.monotonic()
        self.last_packet_at: float | None = None
        self.packets_in = 0
        self.packets_out = 0
        self.send_errors = 0
        # Set once the first datagram arrives on this channel; a switch
        # commits only after this event is set.
        self.first_packet = asyncio.Event()
        # Number of in-flight switches waiting on this channel's first
        # datagram.  A channel is dropped only when it has neither
        # subscribers nor pending switches.
        self.pending = 0
        self._sock: socket.socket | None = None

    @property
    def ready(self) -> bool:
        return self.transport is not None

    async def start(self) -> None:
        """Bind the socket and join the group; raise JoinError on failure."""
        try:
            addr = ipaddress.IPv4Address(self.group)
        except ipaddress.AddressValueError as exc:
            raise JoinError(f"{self.group!r} is not a valid IPv4 address") from exc
        if not addr.is_multicast:
            raise JoinError(f"{self.group!r} is not an IPv4 multicast group (224.0.0.0/4)")

        loop = asyncio.get_running_loop()
        sock: socket.socket | None = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            # Deliver only groups joined by *this* socket (see the constant
            # definition above for why this matters).
            try:
                sock.setsockopt(socket.IPPROTO_IP, _IP_MULTICAST_ALL, 0)
            except OSError:
                pass  # non-Linux platform without IP_MULTICAST_ALL
            sock.bind(("", self.port))
            sock.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_ADD_MEMBERSHIP,
                _mreq(self.group, self.manager.interface),
            )
            sock.setblocking(False)
            transport, _ = await loop.create_datagram_endpoint(
                lambda: _ChannelProtocol(self), sock=sock
            )
        except (OSError, asyncio.TimeoutError) as exc:
            if sock is not None:
                sock.close()
            raise JoinError(f"cannot join {self.group}:{self.port}: {exc}") from exc
        self._sock = sock
        self.transport = transport

    def _on_datagram(self, data: bytes) -> None:
        self.packets_in += 1
        self.last_packet_at = time.monotonic()
        self.first_packet.set()
        sender = self.manager.send_transport
        if sender is None:
            return
        # Snapshot: the set can only be mutated between event-loop steps,
        # never during this synchronous loop.
        for ip, port in tuple(self.subscribers):
            try:
                sender.sendto(data, (ip, port))
                self.packets_out += 1
            except (OSError, RuntimeError):
                self.send_errors += 1

    async def aclose(self) -> None:
        """Drop the membership and close the socket (idempotent)."""
        transport, self.transport = self.transport, None
        sock, self._sock = self._sock, None
        if sock is not None:
            try:
                sock.setsockopt(
                    socket.IPPROTO_IP,
                    socket.IP_DROP_MEMBERSHIP,
                    _mreq(self.group, self.manager.interface),
                )
            except OSError:
                pass
        if transport is not None:
            transport.close()
        elif sock is not None:
            sock.close()


class RelayManager:
    """Owns all channels and subscriptions; serializes every mutation."""

    def __init__(self, interface: str = "0.0.0.0", join_timeout: float = 5.0) -> None:
        self.interface = interface
        self.join_timeout = join_timeout
        self.send_transport: asyncio.DatagramTransport | None = None
        self._channels: dict[ChannelKey, Channel] = {}
        self._sub_index: dict[Subscriber, ChannelKey] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol, family=socket.AF_INET
        )
        self.send_transport = transport

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            channels = list(self._channels.values())
            self._channels.clear()
            self._sub_index.clear()
            for channel in channels:
                await channel.aclose()
            if self.send_transport is not None:
                self.send_transport.close()
                self.send_transport = None

    # ------------------------------------------------------------------ API

    async def subscribe(self, subscriber: Subscriber, key: ChannelKey) -> bool:
        """Attach *subscriber* to channel *key*.

        Returns True if a new subscription was created, False if the exact
        same subscription already existed (idempotent).
        """
        async with self._lock:
            self._ensure_open()
            current = self._sub_index.get(subscriber)
            if current == key:
                return False
            if current is not None:
                raise ConflictError(
                    f"{subscriber[0]}:{subscriber[1]} is subscribed to "
                    f"{current[0]}:{current[1]}; use /api/v1/switch to change channel"
                )
            channel = await self._ensure_channel(key)
            channel.subscribers.add(subscriber)
            self._sub_index[subscriber] = key
            return True

    async def unsubscribe(self, subscriber: Subscriber) -> ChannelKey:
        """Detach *subscriber*; returns the channel it was attached to."""
        async with self._lock:
            self._ensure_open()
            key = self._sub_index.get(subscriber)
            if key is None:
                raise NotFoundError(f"{subscriber[0]}:{subscriber[1]} has no active subscription")
            channel = self._channels.get(key)
            # -- atomic point: no await between the mutations ---------------
            if channel is not None:
                channel.subscribers.discard(subscriber)
            del self._sub_index[subscriber]
            # From here on not a single datagram of `key` is forwarded to
            # `subscriber` any more.
            # ----------------------------------------------------------------
            if channel is not None:
                await self._maybe_drop_channel(key, channel)
            return key

    async def switch(
        self,
        subscriber: Subscriber,
        to_key: ChannelKey,
        from_key: ChannelKey | None = None,
    ) -> ChannelKey:
        """Move *subscriber* to *to_key*; returns the previous channel key.

        The new channel must prove it can deliver before anything is
        touched: it is joined (if necessary) and its first datagram must
        arrive within ``join_timeout`` (one shared budget for join +
        first-packet wait).  Only then is the subscriber moved.  If the
        join fails or no datagram arrives in time, JoinError propagates
        (HTTP 502) and the old subscription is fully preserved -- the old
        channel keeps being relayed without interruption.

        The first-packet wait happens *outside* the lock so that a silent
        target channel never blocks unrelated subscribe/switch calls.
        """
        loop = asyncio.get_running_loop()
        # -- phase 1: validate and make the new channel listen --------------
        async with self._lock:
            self._ensure_open()
            current = self._sub_index.get(subscriber)
            if current is None:
                raise NotFoundError(
                    f"{subscriber[0]}:{subscriber[1]} has no active subscription; subscribe first"
                )
            if from_key is not None and from_key != current:
                raise ConflictError(
                    f"subscriber is on {current[0]}:{current[1]}, "
                    f"not on {from_key[0]}:{from_key[1]}"
                )
            if to_key == current:
                return current
            deadline = loop.time() + self.join_timeout
            channel = await self._ensure_channel(
                to_key, timeout=max(deadline - loop.time(), 0.0)
            )
            # Pin the channel so it cannot be dropped while we wait.
            channel.pending += 1

        # -- phase 2: wait for the first datagram (lock not held) -----------
        # While waiting, the old channel keeps relaying undisturbed.
        try:
            if not channel.first_packet.is_set():
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError
                await asyncio.wait_for(channel.first_packet.wait(), remaining)
        except TimeoutError:
            async with self._lock:
                await self._release_pending(to_key, channel)
            raise JoinError(
                f"no datagram received from {to_key[0]}:{to_key[1]} within "
                f"{self.join_timeout:g}s; staying on {current[0]}:{current[1]}"
            ) from None
        except BaseException:
            # CancelledError and friends: roll back and propagate.
            async with self._lock:
                await self._release_pending(to_key, channel)
            raise

        # -- phase 3: commit ------------------------------------------------
        async with self._lock:
            channel.pending -= 1
            if self._closed:
                await self._maybe_drop_channel(to_key, channel)
                raise RelayError("relay manager is closed")
            now = self._sub_index.get(subscriber)
            if now is None:
                await self._maybe_drop_channel(to_key, channel)
                raise NotFoundError(
                    f"{subscriber[0]}:{subscriber[1]} was unsubscribed concurrently"
                )
            if now != current:
                # A concurrent operation moved the subscriber while we
                # were waiting; do not clobber it.
                await self._maybe_drop_channel(to_key, channel)
                raise ConflictError(
                    f"subscriber was moved to {now[0]}:{now[1]} by a "
                    f"concurrent operation"
                )
            old_channel = self._channels.get(current)
            # Atomic handover: no await between the mutations, so the
            # forwarding callback never sees a torn state.
            if old_channel is not None:
                old_channel.subscribers.discard(subscriber)
            channel.subscribers.add(subscriber)
            self._sub_index[subscriber] = to_key
            # Leave the old group only when its last subscriber is gone.
            if old_channel is not None:
                await self._maybe_drop_channel(current, old_channel)
            return current

    # ------------------------------------------------------------- snapshots

    def channels_snapshot(self) -> list[dict]:
        now = time.monotonic()
        out = []
        for (group, port), ch in sorted(self._channels.items()):
            out.append(
                {
                    "group": group,
                    "port": port,
                    "subscribers": [
                        {"ip": ip, "port": p} for ip, p in sorted(ch.subscribers)
                    ],
                    "subscriber_count": len(ch.subscribers),
                    "packets_in": ch.packets_in,
                    "packets_out": ch.packets_out,
                    "send_errors": ch.send_errors,
                    "ready": ch.ready,
                    "first_packet_received": ch.first_packet.is_set(),
                    "pending_switches": ch.pending,
                    "uptime_seconds": round(now - ch.created_at, 3),
                    "last_packet_age_seconds": (
                        round(now - ch.last_packet_at, 3)
                        if ch.last_packet_at is not None
                        else None
                    ),
                }
            )
        return out

    def subscriptions_snapshot(self) -> dict:
        return {
            "subscriptions": [
                {
                    "subscriber": {"ip": ip, "port": p},
                    "channel": {"group": g, "port": pt},
                }
                for (ip, p), (g, pt) in sorted(self._sub_index.items())
            ]
        }

    # ------------------------------------------------------------ internals

    def _ensure_open(self) -> None:
        if self._closed:
            raise RelayError("relay manager is closed")

    async def _ensure_channel(self, key: ChannelKey, timeout: float | None = None) -> Channel:
        """Return the ready channel for *key*, joining it if necessary.

        Must be called with the lock held.  On failure nothing is
        registered and JoinError is raised.  *timeout* bounds the join;
        it defaults to ``self.join_timeout``.
        """
        channel = self._channels.get(key)
        if channel is not None:
            return channel
        channel = Channel(self, key)
        budget = self.join_timeout if timeout is None else timeout
        try:
            await asyncio.wait_for(channel.start(), timeout=budget)
        except (OSError, asyncio.TimeoutError) as exc:
            await channel.aclose()
            raise JoinError(f"cannot join {key[0]}:{key[1]}: {exc}") from exc
        except BaseException:
            # JoinError, CancelledError, ... -> clean up and propagate.
            await channel.aclose()
            raise
        self._channels[key] = channel
        return channel

    async def _release_pending(self, key: ChannelKey, channel: Channel) -> None:
        """Undo a pending-switch pin (lock must be held)."""
        channel.pending -= 1
        await self._maybe_drop_channel(key, channel)

    async def _maybe_drop_channel(self, key: ChannelKey, channel: Channel) -> None:
        """Drop *channel* once nothing needs it (lock must be held).

        A channel lives while it has subscribers or pending switches;
        when both are gone the membership is dropped and the socket
        closed.
        """
        if not channel.subscribers and channel.pending == 0:
            await self._drop_channel(key, channel)

    async def _drop_channel(self, key: ChannelKey, channel: Channel) -> None:
        if self._channels.get(key) is channel:
            del self._channels[key]
        await channel.aclose()
