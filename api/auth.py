"""API key authentication.

Keys are stored as a Redis hash: field = sha256(raw_key), value = JSON
{"tier": "free"|"pro", "owner": "..."}. Only the hash is ever stored or
logged - never the raw key - so a Redis dump or a log leak doesn't hand
out working credentials.

A relational store (Postgres) is the more natural home for key management
once billing, rotation, and audit history enter the picture. Redis is the
pragmatic choice here to keep the deployment to one moving part instead
of two - a trade-off worth naming explicitly rather than pretending this
is how a mature system would do it.
"""
from __future__ import annotations

import hashlib
import json

from fastapi import Header, HTTPException, status

from api.redis_client import get_redis
from shared.schemas import redis_apikey_hash_key


def hash_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


class ClientIdentity:
    __slots__ = ("key_hash", "owner", "tier")

    def __init__(self, key_hash: str, tier: str, owner: str) -> None:
        self.key_hash = key_hash
        self.tier = tier
        self.owner = owner


def require_api_key(x_api_key: str | None = Header(default=None)) -> ClientIdentity:
    if not x_api_key:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="missing X-API-Key header")

    key_hash = hash_key(x_api_key)
    r = get_redis()
    raw = r.hget(redis_apikey_hash_key(), key_hash)
    if raw is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="invalid API key")

    record = json.loads(raw)
    return ClientIdentity(
        key_hash=key_hash,
        tier=record.get("tier", "free"),
        owner=record.get("owner", "unknown"),
    )
