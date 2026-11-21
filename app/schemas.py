"""Request/response models for the relay gateway API."""

from __future__ import annotations

import ipaddress

from pydantic import BaseModel, Field, field_validator

API = "/api/v1"


def _parse_ipv4(value: str) -> ipaddress.IPv4Address:
    try:
        return ipaddress.IPv4Address(value)
    except ipaddress.AddressValueError as exc:
        raise ValueError(f"invalid IPv4 address: {value!r}") from exc


class Endpoint(BaseModel):
    """Unicast address of a subscriber (播出端)."""

    ip: str = Field(examples=["127.0.0.1"])
    port: int = Field(ge=1, le=65535, examples=[9000])

    @field_validator("ip")
    @classmethod
    def _ipv4(cls, v: str) -> str:
        return str(_parse_ipv4(v))

    def key(self) -> tuple[str, int]:
        return (self.ip, self.port)


class ChannelRef(BaseModel):
    """A channel, identified by IPv4 multicast group + UDP port."""

    group: str = Field(examples=["239.10.0.1"])
    port: int = Field(ge=1, le=65535, examples=[5000])

    @field_validator("group")
    @classmethod
    def _mcast(cls, v: str) -> str:
        addr = _parse_ipv4(v)
        if not addr.is_multicast:
            raise ValueError(f"{v!r} is not an IPv4 multicast group (224.0.0.0/4)")
        return str(addr)

    def key(self) -> tuple[str, int]:
        return (self.group, self.port)


# ------------------------------------------------------------------ requests


class SubscribeRequest(BaseModel):
    subscriber: Endpoint
    channel: ChannelRef


class UnsubscribeRequest(BaseModel):
    subscriber: Endpoint


class SwitchRequest(BaseModel):
    subscriber: Endpoint
    to_channel: ChannelRef
    # Optional guard: when given, it must match the subscriber's current
    # channel, otherwise the switch is rejected with 409.
    from_channel: ChannelRef | None = None


# ----------------------------------------------------------------- responses


class SubscriptionResponse(BaseModel):
    subscriber: Endpoint
    channel: ChannelRef
    already_subscribed: bool
    detail: str


class UnsubscribeResponse(BaseModel):
    subscriber: Endpoint
    channel: ChannelRef
    detail: str


class SwitchResponse(BaseModel):
    subscriber: Endpoint
    from_channel: ChannelRef
    to_channel: ChannelRef
    changed: bool
    detail: str


class ChannelInfo(BaseModel):
    group: str
    port: int
    subscribers: list[Endpoint]
    subscriber_count: int
    packets_in: int
    packets_out: int
    send_errors: int
    ready: bool
    first_packet_received: bool
    pending_switches: int
    uptime_seconds: float
    last_packet_age_seconds: float | None


class SubscriptionInfo(BaseModel):
    subscriber: Endpoint
    channel: ChannelRef


class SubscriptionList(BaseModel):
    subscriptions: list[SubscriptionInfo]
