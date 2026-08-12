"""WebSocket endpoint for live push updates, as an alternative to polling
/latest. Subscribes to the Redis pub/sub channel the consumer publishes to
and forwards messages to the connected client.
"""
from __future__ import annotations

import redis.asyncio as aioredis
from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect, status

from api import config
from api.auth import hash_key
from api.redis_client import get_redis
from shared.schemas import redis_apikey_hash_key, redis_pubsub_channel

router = APIRouter(tags=["stream"])


@router.websocket("/v1/stream/{symbol}")
async def stream_symbol(websocket: WebSocket, symbol: str, api_key: str = Query(...)) -> None:
    # WebSocket auth can't reliably use a header dependency the way every
    # REST client can, so the key travels as a query parameter here instead
    # - a real trade-off worth naming rather than hiding, since query-string
    # secrets can end up in access logs and browser history.
    r = get_redis()
    record = r.hget(redis_apikey_hash_key(), hash_key(api_key))
    if record is None:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await websocket.accept()
    aredis = aioredis.Redis.from_url(config.REDIS_URL, decode_responses=True)
    pubsub = aredis.pubsub()
    await pubsub.subscribe(redis_pubsub_channel(symbol))

    try:
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue
            await websocket.send_text(message["data"])
    except WebSocketDisconnect:
        pass
    finally:
        await pubsub.unsubscribe(redis_pubsub_channel(symbol))
        await aredis.close()
