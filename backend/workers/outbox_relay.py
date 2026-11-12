from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import asyncpg
from redis.exceptions import RedisError, ResponseError
from redis.asyncio import Redis
from starlette.websockets import WebSocket, WebSocketDisconnect

from backend.core.routing import redis_stream_key
from backend.db.store import Store
from backend.workers.audit_merkle_sink import AuditMerkleSink

logger = logging.getLogger(__name__)


async def _publish_stream_event(
    redis: Redis,
    stream_key: str,
    event_id: int,
    payload: dict[str, Any],
) -> None:
    entry_id = f"{event_id}-0"
    try:
        await redis.xadd(
            stream_key,
            {"event": json.dumps(payload, separators=(",", ":")), "event_id": str(event_id)},
            id=entry_id,
            maxlen=10_000,
            approximate=True,
        )
    except ResponseError:
        existing = await redis.xrange(stream_key, min=entry_id, max=entry_id, count=1)
        if not existing:
            raise


class WebSocketHub:
    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._clients.add(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self._clients.discard(websocket)

    async def publish(self, event: dict[str, Any]) -> None:
        async with self._lock:
            clients = tuple(self._clients)
        stale: list[WebSocket] = []
        for websocket in clients:
            try:
                await websocket.send_json(event)
            except (WebSocketDisconnect, RuntimeError, OSError):
                stale.append(websocket)
        if stale:
            async with self._lock:
                self._clients.difference_update(stale)


async def run_outbox_relay(
    store: Store,
    audit: AuditMerkleSink,
    hub: WebSocketHub,
    stop: asyncio.Event,
    redis: Redis | None = None,
) -> None:
    while not stop.is_set():
        try:
            events = await store.claim_outbox()
            for event in events:
                event_id = int(event["event_id"])
                payload = event["payload"]
                await audit.append(event_id, payload)
                if redis is not None:
                    for account_id in (payload["sender_id"], payload["receiver_id"]):
                        await _publish_stream_event(
                            redis, redis_stream_key(account_id), event_id, payload,
                        )
                await hub.publish(payload)
                await store.mark_outbox_published(event_id)
        except RedisError:
            logger.exception("Redis outbox publication failed; events remain available for retry")
        except (OSError, RuntimeError, ValueError, asyncpg.PostgresError):
            logger.exception("outbox relay iteration failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.05)
        except TimeoutError:
            continue
