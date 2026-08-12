"""Shared data schemas used across the producer, consumer, and API.

Keeping this in one module means all three services agree on the wire
format for events on the Redpanda topic and the shape of state stored in
Redis, without duplicating (and drifting on) field names across services
that get deployed and versioned independently.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass

TRADES_TOPIC = "market-trades"


@dataclass(frozen=True, slots=True)
class TradeEvent:
    """A single trade, normalised from an exchange's raw payload."""

    symbol: str
    price: float
    quantity: float
    trade_id: int
    trade_time_ms: int  # exchange-reported trade timestamp
    ingested_at_ms: int  # when the producer received it
    is_buyer_maker: bool

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")

    @classmethod
    def from_json(cls, raw: bytes) -> TradeEvent:
        data = json.loads(raw)
        return cls(**data)


def redis_latest_key(symbol: str) -> str:
    return f"latest:{symbol.upper()}"


def redis_candle_key(symbol: str, minute_bucket_s: int) -> str:
    return f"candle:{symbol.upper()}:{minute_bucket_s}"


def redis_candle_index_key(symbol: str) -> str:
    """Sorted set of recent minute buckets for a symbol, for range queries."""
    return f"candle_index:{symbol.upper()}"


def redis_apikey_hash_key() -> str:
    return "apikeys"


def redis_ratelimit_key(api_key_hash: str) -> str:
    return f"ratelimit:{api_key_hash}"


def redis_pubsub_channel(symbol: str) -> str:
    return f"stream:{symbol.upper()}"


def minute_bucket(epoch_ms: int) -> int:
    """Floor a millisecond timestamp to the start of its minute, in epoch seconds."""
    return (epoch_ms // 1000 // 60) * 60
