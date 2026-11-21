"""End-to-end tests for the IPv4 UDP multicast relay gateway.

These tests use real UDP sockets on the default interface (multicast
loopback), exercising subscribe / unsubscribe / switch semantics,
same-port group isolation, failure handling and concurrency rules.
"""

from __future__ import annotations

import asyncio
import socket

import pytest

from app.schemas import API

from .helpers import UdpSink, free_udp_port, next_port, sub_body


async def _delete(client, sink: UdpSink):
    return await client.request(
        "DELETE", f"{API}/subscriptions", json={"subscriber": sink.endpoint()}
    )


async def _switch(client, sink: UdpSink, to_group: str, to_port: int,
                  from_group: str | None = None, from_port: int | None = None):
    body: dict = {
        "subscriber": sink.endpoint(),
        "to_channel": {"group": to_group, "port": to_port},
    }
    if from_group is not None:
        body["from_channel"] = {"group": from_group, "port": from_port}
    return await client.post(f"{API}/switch", json=body)


async def _channels(client) -> list[dict]:
    r = await client.get(f"{API}/channels")
    assert r.status_code == 200
    return r.json()


# ---------------------------------------------------------------- subscribe


async def test_subscribe_and_forward(client, sender):
    sink = UdpSink()
    group, port = "239.60.0.1", next_port()
    r = await client.post(f"{API}/subscriptions", json=sub_body(sink, group, port))
    assert r.status_code == 201, r.text
    assert r.json()["already_subscribed"] is False

    sender.sendto(b"hello-1", (group, port))
    assert await sink.recv(2.0) == b"hello-1"

    chans = await _channels(client)
    assert len(chans) == 1
    assert (chans[0]["group"], chans[0]["port"]) == (group, port)
    assert chans[0]["subscriber_count"] == 1
    assert chans[0]["ready"] is True
    assert chans[0]["packets_in"] >= 1
    assert chans[0]["packets_out"] >= 1
    sink.close()


async def test_subscribe_idempotent(client, sender):
    sink = UdpSink()
    group, port = "239.60.0.2", next_port()
    r1 = await client.post(f"{API}/subscriptions", json=sub_body(sink, group, port))
    r2 = await client.post(f"{API}/subscriptions", json=sub_body(sink, group, port))
    assert r1.status_code == 201
    assert r2.status_code == 200
    assert r2.json()["already_subscribed"] is True
    assert len(await _channels(client)) == 1
    sink.close()


async def test_subscribe_other_channel_conflict(client, sender):
    sink = UdpSink()
    r = await client.post(
        f"{API}/subscriptions", json=sub_body(sink, "239.60.0.3", next_port())
    )
    assert r.status_code == 201
    r = await client.post(
        f"{API}/subscriptions", json=sub_body(sink, "239.60.0.4", next_port())
    )
    assert r.status_code == 409
    assert "switch" in r.json()["detail"]
    sink.close()


async def test_two_subscribers_share_one_membership(client, sender):
    sink1, sink2 = UdpSink(), UdpSink()
    group, port = "239.60.0.5", next_port()
    for s in (sink1, sink2):
        r = await client.post(f"{API}/subscriptions", json=sub_body(s, group, port))
        assert r.status_code == 201

    chans = await _channels(client)
    assert len(chans) == 1  # one channel -> one socket -> one group join
    assert chans[0]["subscriber_count"] == 2

    sender.sendto(b"to-both", (group, port))
    assert await sink1.recv(2.0) == b"to-both"
    assert await sink2.recv(2.0) == b"to-both"
    sink1.close()
    sink2.close()


async def test_same_port_different_groups_are_separate(client, sender):
    sink_a, sink_b = UdpSink(), UdpSink()
    port = next_port()
    ga, gb = "239.60.1.1", "239.60.1.2"
    r = await client.post(f"{API}/subscriptions", json=sub_body(sink_a, ga, port))
    assert r.status_code == 201
    r = await client.post(f"{API}/subscriptions", json=sub_body(sink_b, gb, port))
    assert r.status_code == 201

    chans = await _channels(client)
    assert len(chans) == 2  # same port, different groups -> two channels
    assert {c["port"] for c in chans} == {port}

    sender.sendto(b"for-a", (ga, port))
    sender.sendto(b"for-b", (gb, port))
    assert await sink_a.recv(2.0) == b"for-a"
    assert await sink_b.recv(2.0) == b"for-b"
    # no cross-group leakage
    assert await sink_a.recv(0.4) is None
    assert await sink_b.recv(0.4) is None
    sink_a.close()
    sink_b.close()


# -------------------------------------------------------------- unsubscribe


async def test_unsubscribe_stops_forwarding(client, sender):
    sink = UdpSink()
    group, port = "239.60.2.1", next_port()
    r = await client.post(f"{API}/subscriptions", json=sub_body(sink, group, port))
    assert r.status_code == 201
    sender.sendto(b"before", (group, port))
    assert await sink.recv(2.0) == b"before"

    r = await _delete(client, sink)
    assert r.status_code == 200, r.text
    assert (r.json()["channel"]["group"], r.json()["channel"]["port"]) == (group, port)

    await sink.drain()
    sender.sendto(b"after", (group, port))
    assert await sink.recv(0.5) is None  # nothing forwarded any more
    sink.close()


async def test_last_leaver_drops_membership(client, sender):
    sink1, sink2 = UdpSink(), UdpSink()
    group, port = "239.60.2.2", next_port()
    for s in (sink1, sink2):
        await client.post(f"{API}/subscriptions", json=sub_body(s, group, port))

    # first leave: channel and membership stay
    assert (await _delete(client, sink1)).status_code == 200
    chans = await _channels(client)
    assert len(chans) == 1 and chans[0]["subscriber_count"] == 1
    sender.sendto(b"only-2", (group, port))
    assert await sink2.recv(2.0) == b"only-2"
    assert await sink1.recv(0.4) is None

    # last leave: channel (and its group membership) is dropped
    assert (await _delete(client, sink2)).status_code == 200
    assert await _channels(client) == []
    sink1.close()
    sink2.close()


async def test_unsubscribe_unknown_404(client):
    sink = UdpSink()
    r = await _delete(client, sink)
    assert r.status_code == 404
    sink.close()


# ------------------------------------------------------------------- switch


async def test_switch_moves_traffic(client, sender):
    sink = UdpSink()
    ga, pa = "239.60.3.1", next_port()
    gb, pb = "239.60.3.2", next_port()
    await client.post(f"{API}/subscriptions", json=sub_body(sink, ga, pa))

    stop = asyncio.Event()

    async def spam(group: str, port: int, tag: bytes) -> None:
        i = 0
        while not stop.is_set():
            sender.sendto(b"%s-%d" % (tag, i), (group, port))
            i += 1
            await asyncio.sleep(0.02)

    task_a = asyncio.create_task(spam(ga, pa, b"A"))
    assert (await sink.recv(2.0)).startswith(b"A-")

    r = await _switch(client, sink, gb, pb, from_group=ga, from_port=pa)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["changed"] is True
    assert (body["from_channel"]["group"], body["from_channel"]["port"]) == (ga, pa)

    task_b = asyncio.create_task(spam(gb, pb, b"B"))
    await sink.drain()  # flush in-flight old-channel datagrams
    msgs = await sink.collect(0.8)
    stop.set()
    await asyncio.gather(task_a, task_b)

    assert msgs, "expected traffic from the new channel"
    assert all(m.startswith(b"B-") for m in msgs), msgs

    # old channel had a single subscriber -> membership dropped
    chans = {(c["group"], c["port"]) for c in await _channels(client)}
    assert (ga, pa) not in chans
    assert (gb, pb) in chans
    sink.close()


async def test_switch_failure_keeps_old_channel(client, sender):
    sink = UdpSink()
    ga, pa = "239.60.3.3", next_port()
    gb, pb = "239.60.3.4", free_udp_port()
    await client.post(f"{API}/subscriptions", json=sub_body(sink, ga, pa))
    sender.sendto(b"first", (ga, pa))
    assert await sink.recv(2.0) == b"first"

    # Occupy the target port without SO_REUSEADDR so the gateway's bind
    # for the new channel fails -> join is impossible.
    blocker = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    blocker.bind(("0.0.0.0", pb))
    try:
        r = await _switch(client, sink, gb, pb, from_group=ga, from_port=pa)
        assert r.status_code == 502, r.text
        assert "cannot join" in r.json()["detail"]
    finally:
        blocker.close()

    # the old channel keeps being relayed
    await sink.drain()
    sender.sendto(b"still-A", (ga, pa))
    assert await sink.recv(2.0) == b"still-A"

    # subscription unchanged; failed channel not registered
    subs = (await client.get(f"{API}/subscriptions")).json()["subscriptions"]
    assert subs == [
        {"subscriber": sink.endpoint(), "channel": {"group": ga, "port": pa}}
    ]
    chans = [(c["group"], c["port"]) for c in await _channels(client)]
    assert chans == [(ga, pa)]
    sink.close()


async def test_switch_to_existing_channel_shares_membership(client, sender):
    sink1, sink2 = UdpSink(), UdpSink()
    ga, pa = "239.60.3.5", next_port()
    gb, pb = "239.60.3.6", next_port()
    await client.post(f"{API}/subscriptions", json=sub_body(sink1, gb, pb))
    await client.post(f"{API}/subscriptions", json=sub_body(sink2, ga, pa))

    r = await _switch(client, sink2, gb, pb)
    assert r.status_code == 200

    chans = await _channels(client)
    assert len(chans) == 1  # A dropped, both subscribers share B's join
    assert (chans[0]["group"], chans[0]["port"]) == (gb, pb)
    assert chans[0]["subscriber_count"] == 2

    sender.sendto(b"on-b", (gb, pb))
    assert await sink1.recv(2.0) == b"on-b"
    assert await sink2.recv(2.0) == b"on-b"
    sink1.close()
    sink2.close()


async def test_switch_to_same_channel_is_noop(client, sender):
    sink = UdpSink()
    group, port = "239.60.3.7", next_port()
    await client.post(f"{API}/subscriptions", json=sub_body(sink, group, port))
    r = await _switch(client, sink, group, port)
    assert r.status_code == 200
    assert r.json()["changed"] is False
    sender.sendto(b"still", (group, port))
    assert await sink.recv(2.0) == b"still"
    sink.close()


async def test_switch_from_mismatch_409(client, sender):
    sink = UdpSink()
    ga, pa = "239.60.3.8", next_port()
    await client.post(f"{API}/subscriptions", json=sub_body(sink, ga, pa))
    r = await _switch(
        client, sink, "239.60.3.9", next_port(), from_group="239.60.3.10",
        from_port=next_port(),
    )
    assert r.status_code == 409
    sink.close()


async def test_switch_without_subscription_404(client):
    sink = UdpSink()
    r = await _switch(client, sink, "239.60.3.11", next_port())
    assert r.status_code == 404
    sink.close()


# --------------------------------------------------------------- validation


@pytest.mark.parametrize(
    "group", ["10.0.0.1", "192.168.1.1", "999.1.1.1", "not-an-ip", "::1"]
)
async def test_subscribe_rejects_bad_group(client, group):
    sink = UdpSink()
    r = await client.post(
        f"{API}/subscriptions", json=sub_body(sink, group, next_port())
    )
    assert r.status_code == 422
    sink.close()


@pytest.mark.parametrize("port", [0, 65536, -1])
async def test_subscribe_rejects_bad_port(client, port):
    sink = UdpSink()
    r = await client.post(
        f"{API}/subscriptions", json=sub_body(sink, "239.60.4.1", port)
    )
    assert r.status_code == 422
    sink.close()


async def test_subscribe_rejects_bad_subscriber_ip(client):
    r = await client.post(
        f"{API}/subscriptions",
        json={
            "subscriber": {"ip": "localhost", "port": 9000},
            "channel": {"group": "239.60.4.2", "port": next_port()},
        },
    )
    assert r.status_code == 422


# --------------------------------------------------------------- concurrency


async def test_concurrent_subscribe_same_channel(client, sender):
    sinks = [UdpSink() for _ in range(8)]
    group, port = "239.60.5.1", next_port()
    results = await asyncio.gather(
        *(client.post(f"{API}/subscriptions", json=sub_body(s, group, port)) for s in sinks)
    )
    assert all(r.status_code in (200, 201) for r in results)
    assert sum(r.status_code == 201 for r in results) >= 1

    chans = await _channels(client)
    assert len(chans) == 1  # exactly one join despite the race
    assert chans[0]["subscriber_count"] == len(sinks)

    sender.sendto(b"all", (group, port))
    got = await asyncio.gather(*(s.recv(2.0) for s in sinks))
    assert list(got) == [b"all"] * len(sinks)
    for s in sinks:
        s.close()


async def test_concurrent_unsubscribe_same_subscriber(client, sender):
    sink = UdpSink()
    group, port = "239.60.5.2", next_port()
    await client.post(f"{API}/subscriptions", json=sub_body(sink, group, port))

    results = await asyncio.gather(*(_delete(client, sink) for _ in range(2)))
    assert sorted(r.status_code for r in results) == [200, 404]
    assert await _channels(client) == []
    sink.close()


async def test_concurrent_last_two_switch_away(client, sender):
    sink1, sink2 = UdpSink(), UdpSink()
    ga, pa = "239.60.5.3", next_port()
    gb, pb = "239.60.5.4", next_port()
    gc, pc = "239.60.5.5", next_port()
    for s in (sink1, sink2):
        await client.post(f"{API}/subscriptions", json=sub_body(s, ga, pa))

    r1, r2 = await asyncio.gather(
        _switch(client, sink1, gb, pb),
        _switch(client, sink2, gc, pc),
    )
    assert r1.status_code == 200, r1.text
    assert r2.status_code == 200, r2.text

    chans = {(c["group"], c["port"]) for c in await _channels(client)}
    assert (ga, pa) not in chans  # old channel dropped exactly once
    assert chans == {(gb, pb), (gc, pc)}

    sender.sendto(b"to-b", (gb, pb))
    sender.sendto(b"to-c", (gc, pc))
    assert await sink1.recv(2.0) == b"to-b"
    assert await sink2.recv(2.0) == b"to-c"
    sink1.close()
    sink2.close()


async def test_concurrent_switch_same_subscriber_from_guard(client, sender):
    sink = UdpSink()
    ga, pa = "239.60.5.6", next_port()
    gb, pb = "239.60.5.7", next_port()
    gc, pc = "239.60.5.8", next_port()
    await client.post(f"{API}/subscriptions", json=sub_body(sink, ga, pa))

    # Both switches claim the same source channel; exactly one may win.
    r1, r2 = await asyncio.gather(
        _switch(client, sink, gb, pb, from_group=ga, from_port=pa),
        _switch(client, sink, gc, pc, from_group=ga, from_port=pa),
    )
    assert sorted(r.status_code for r in (r1, r2)) == [200, 409]

    subs = (await client.get(f"{API}/subscriptions")).json()["subscriptions"]
    assert len(subs) == 1
    winner = (subs[0]["channel"]["group"], subs[0]["channel"]["port"])
    assert winner in {(gb, pb), (gc, pc)}

    # the subscriber receives only the winning channel's traffic
    await sink.drain()
    sender.sendto(b"on-b", (gb, pb))
    sender.sendto(b"on-c", (gc, pc))
    msgs = await sink.collect(0.6)
    expected = b"on-b" if winner == (gb, pb) else b"on-c"
    assert msgs == [expected]
    sink.close()
