"""Endpoints that serve the state the consumer maintains in Redis.

Free-tier callers see the last *completed* one-minute candle instead of
the live tick - a genuine, structural delay (up to ~60s) built from data
that already exists, rather than a fake sleep() or an arbitrary rejected
request. Pro-tier callers see the true latest trade. This mirrors how
real market-data vendors (Bloomberg, IEX) gate real-time quotes behind a
paid tier: the delay itself is the product differentiator, not the data.
"""
from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException, status

from api.auth import ClientIdentity
from api.models import Candle, LatestTrade
from api.rate_limit import rate_limited_identity
from api.redis_client import get_redis
from shared.schemas import redis_candle_index_key, redis_candle_key, redis_latest_key

router = APIRouter(prefix="/v1/symbols", tags=["symbols"])


@router.get("", response_model=list[str])
def list_symbols(identity: ClientIdentity = Depends(rate_limited_identity)) -> list[str]:
    r = get_redis()
    # KEYS is fine at this scale (a handful of symbols); a larger deployment
    # would maintain an explicit set of active symbols instead and use SCAN.
    keys = r.keys("latest:*")
    return sorted(k.split(":", 1)[1] for k in keys)


@router.get("/{symbol}/latest", response_model=LatestTrade)
def get_latest(
    symbol: str, identity: ClientIdentity = Depends(rate_limited_identity)
) -> LatestTrade:
    r = get_redis()
    symbol = symbol.upper()

    if identity.tier == "free":
        buckets = r.zrevrange(redis_candle_index_key(symbol), 0, 1)
        if len(buckets) < 2:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"not enough history yet for {symbol}")
        completed_bucket = buckets[1]  # skip the in-progress current-minute bucket
        raw = r.get(redis_candle_key(symbol, int(completed_bucket)))
        if raw is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"no data yet for {symbol}")
        candle = json.loads(raw)
        return LatestTrade(
            symbol=symbol,
            price=candle["close"],
            vwap=candle["close"],
            pct_change_window=0.0,
            trade_time_ms=int(completed_bucket) * 1000,
            updated_at_ms=int(completed_bucket) * 1000,
            delayed=True,
        )

    raw = r.get(redis_latest_key(symbol))
    if raw is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"no data yet for {symbol}")
    return LatestTrade(**json.loads(raw), delayed=False)


@router.get("/{symbol}/candles", response_model=list[Candle])
def get_candles(
    symbol: str,
    limit: int = 30,
    identity: ClientIdentity = Depends(rate_limited_identity),
) -> list[Candle]:
    r = get_redis()
    symbol = symbol.upper()
    buckets = r.zrevrange(redis_candle_index_key(symbol), 0, limit - 1)
    candles = []
    for bucket in buckets:
        raw = r.get(redis_candle_key(symbol, int(bucket)))
        if raw:
            candles.append(Candle(symbol=symbol, **json.loads(raw)))
    return candles
