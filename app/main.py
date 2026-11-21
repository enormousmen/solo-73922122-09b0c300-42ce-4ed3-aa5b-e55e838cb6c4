"""FastAPI application exposing the IPv4 UDP multicast relay gateway."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request, Response
from fastapi.responses import JSONResponse

from . import __version__
from .relay import ConflictError, JoinError, NotFoundError, RelayError, RelayManager
from .schemas import (
    API,
    ChannelInfo,
    ChannelRef,
    SubscribeRequest,
    SubscriptionList,
    SubscriptionResponse,
    SwitchRequest,
    SwitchResponse,
    UnsubscribeRequest,
    UnsubscribeResponse,
)


def get_relay(request: Request) -> RelayManager:
    return request.app.state.relay


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        relay = RelayManager(
            interface=os.getenv("MCAST_INTERFACE", "0.0.0.0"),
            join_timeout=float(os.getenv("JOIN_TIMEOUT", "5")),
        )
        await relay.start()
        app.state.relay = relay
        try:
            yield
        finally:
            await relay.close()

    app = FastAPI(
        title="IPv4 UDP Multicast Relay Gateway",
        version=__version__,
        lifespan=lifespan,
    )

    @app.exception_handler(ConflictError)
    async def conflict_handler(request: Request, exc: ConflictError) -> JSONResponse:
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(NotFoundError)
    async def notfound_handler(request: Request, exc: NotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(JoinError)
    async def join_handler(request: Request, exc: JoinError) -> JSONResponse:
        return JSONResponse(status_code=502, content={"detail": str(exc)})

    @app.exception_handler(RelayError)
    async def relay_handler(request: Request, exc: RelayError) -> JSONResponse:
        return JSONResponse(status_code=500, content={"detail": str(exc)})

    # ------------------------------------------------------------- endpoints

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/")
    async def index() -> dict:
        return {"service": "ipv4-udp-multicast-relay-gateway", "version": __version__, "api": API}

    @app.post(f"{API}/subscriptions", response_model=SubscriptionResponse, status_code=201)
    async def subscribe(
        req: SubscribeRequest,
        response: Response,
        relay: RelayManager = Depends(get_relay),
    ) -> SubscriptionResponse:
        created = await relay.subscribe(req.subscriber.key(), req.channel.key())
        if not created:
            response.status_code = 200
        return SubscriptionResponse(
            subscriber=req.subscriber,
            channel=req.channel,
            already_subscribed=not created,
            detail="subscribed" if created else "already subscribed",
        )

    @app.delete(f"{API}/subscriptions", response_model=UnsubscribeResponse)
    async def unsubscribe(
        req: UnsubscribeRequest,
        relay: RelayManager = Depends(get_relay),
    ) -> UnsubscribeResponse:
        key = await relay.unsubscribe(req.subscriber.key())
        return UnsubscribeResponse(
            subscriber=req.subscriber,
            channel=ChannelRef(group=key[0], port=key[1]),
            detail="unsubscribed; gateway stopped forwarding this channel to the endpoint",
        )

    @app.post(f"{API}/switch", response_model=SwitchResponse)
    async def switch(
        req: SwitchRequest,
        relay: RelayManager = Depends(get_relay),
    ) -> SwitchResponse:
        from_key = req.from_channel.key() if req.from_channel is not None else None
        old = await relay.switch(req.subscriber.key(), req.to_channel.key(), from_key)
        changed = old != req.to_channel.key()
        return SwitchResponse(
            subscriber=req.subscriber,
            from_channel=ChannelRef(group=old[0], port=old[1]),
            to_channel=req.to_channel,
            changed=changed,
            detail="switched" if changed else "already on the requested channel",
        )

    @app.get(f"{API}/channels", response_model=list[ChannelInfo])
    async def channels(relay: RelayManager = Depends(get_relay)) -> list[dict]:
        return relay.channels_snapshot()

    @app.get(f"{API}/subscriptions", response_model=SubscriptionList)
    async def subscriptions(relay: RelayManager = Depends(get_relay)) -> dict:
        return relay.subscriptions_snapshot()

    return app


app = create_app()
