"""Redis-backed sliding-window rate limiter, keyed by the caller's API key
(not IP), so a paid tier genuinely gets a higher limit no matter where the
request originates from. Implemented with a Redis sorted set: each request
adds a timestamp, we trim anything outside the window, and the remaining
count is the request count in that window.

This has to be Redis-backed rather than an in-memory counter because the
API is meant to run as more than one replica behind a load balancer - an
in-memory limiter would let each replica hand out its own separate quota,
silently multiplying the effective limit by the replica count.
"""
from __future__ import annotations

import time

from fastapi import Depends, HTTPException, status

from api import config
from api.auth import ClientIdentity, require_api_key
from api.redis_client import get_redis
from shared.schemas import redis_ratelimit_key

WINDOW_SECONDS = 60

TIER_LIMITS = {
    "free": config.RATE_LIMIT_FREE_PER_MIN,
    "pro": config.RATE_LIMIT_PRO_PER_MIN,
}


def check_rate_limit(identity: ClientIdentity) -> None:
    limit = TIER_LIMITS.get(identity.tier, config.RATE_LIMIT_FREE_PER_MIN)
    key = redis_ratelimit_key(identity.key_hash)
    now = time.time()
    window_start = now - WINDOW_SECONDS

    r = get_redis()
    pipe = r.pipeline()
    pipe.zremrangebyscore(key, 0, window_start)
    pipe.zadd(key, {str(now): now})
    pipe.zcard(key)
    pipe.expire(key, WINDOW_SECONDS)
    _, _, count, _ = pipe.execute()

    if count > limit:
        raise HTTPException(
            status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"rate limit exceeded: {limit} requests/{WINDOW_SECONDS}s for tier '{identity.tier}'",
            headers={"Retry-After": str(WINDOW_SECONDS)},
        )


def rate_limited_identity(identity: ClientIdentity = Depends(require_api_key)) -> ClientIdentity:
    """Composed dependency: resolve the caller's identity, then enforce
    their tier's rate limit. Routes depend on this one function instead of
    stacking two separate dependencies in every route signature.
    """
    check_rate_limit(identity)
    return identity
