"""Consumer: the only service that reads the Redpanda topic. It maintains
rolling VWAP and per-minute OHLC candles per symbol and materialises that
state into Redis. The API never talks to Kafka directly - it only reads
Redis - which keeps API latency independent of consumer lag and lets the
API run as many replicas without each one running its own consumer group
member.

Delivery semantics: Kafka offsets commit only after Redis has durably
recorded the update, using a dedup marker to guard against reprocessing.
If the process crashes between the dedup check and the full Redis write,
the redelivered message is treated as already-seen and dropped rather
than double-counted - losing one trade out of a high-frequency stream is
immaterial for a rolling analytics view, whereas double-counting volume
would quietly corrupt every VWAP and candle downstream. That trade-off
would not be acceptable for a ledger or balance use case, where you'd
want the opposite: dedupe strictly and never silently drop.
"""
from __future__ import annotations

import json
import logging
import time

import redis
from confluent_kafka import Consumer

from consumer import config
from consumer.aggregator import CandleAccumulator, SymbolState
from shared.schemas import (
    TRADES_TOPIC,
    TradeEvent,
    minute_bucket,
    redis_candle_index_key,
    redis_candle_key,
    redis_latest_key,
    redis_pubsub_channel,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("consumer")

CANDLE_HISTORY_KEPT = 120  # minutes of candles retained per symbol
CANDLE_TTL_SECONDS = 60 * 60 * 6
DEDUP_TTL_SECONDS = 300  # only needs to cover redelivery right around a crash, not forever


def build_kafka_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": config.REDPANDA_BOOTSTRAP_SERVERS,
            "group.id": config.CONSUMER_GROUP,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,  # commit only after a successful Redis write
        }
    )


def write_state(
    r: redis.Redis,
    event: TradeEvent,
    state: SymbolState,
    candles: dict[str, CandleAccumulator],
) -> None:
    bucket = minute_bucket(event.trade_time_ms)
    candle_key = f"{event.symbol}:{bucket}"
    candle = candles.get(candle_key)
    if candle is None:
        candle = CandleAccumulator.start(event.price, event.quantity)
        candles[candle_key] = candle
    else:
        candle.update(event.price, event.quantity)

    latest_payload = {
        "symbol": event.symbol,
        "price": event.price,
        "vwap": round(state.vwap, 8),
        "pct_change_window": round(state.pct_change_window, 4),
        "trade_time_ms": event.trade_time_ms,
        "updated_at_ms": int(time.time() * 1000),
    }
    candle_payload = {
        "open": candle.open,
        "high": candle.high,
        "low": candle.low,
        "close": candle.close,
        "volume": round(candle.volume, 8),
        "bucket_start_s": bucket,
    }

    pipe = r.pipeline()
    pipe.set(redis_latest_key(event.symbol), json.dumps(latest_payload))
    pipe.set(redis_candle_key(event.symbol, bucket), json.dumps(candle_payload), ex=CANDLE_TTL_SECONDS)
    pipe.zadd(redis_candle_index_key(event.symbol), {str(bucket): bucket})
    pipe.zremrangebyrank(redis_candle_index_key(event.symbol), 0, -CANDLE_HISTORY_KEPT - 1)
    pipe.publish(redis_pubsub_channel(event.symbol), json.dumps(latest_payload))
    pipe.execute()


def main() -> None:
    consumer = build_kafka_consumer()
    consumer.subscribe([TRADES_TOPIC])
    r = redis.Redis.from_url(config.REDIS_URL, decode_responses=True)

    states: dict[str, SymbolState] = {}
    candles: dict[str, CandleAccumulator] = {}

    log.info("consumer started, group=%s", config.CONSUMER_GROUP)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("consumer error: %s", msg.error())
                continue

            event = TradeEvent.from_json(msg.value())
            dedup_key = f"dedup:{event.symbol}:{event.trade_id}"

            try:
                is_new = r.set(dedup_key, 1, nx=True, ex=DEDUP_TTL_SECONDS)
                if not is_new:
                    # already applied before a crash - just advance past it
                    consumer.commit(msg)
                    continue

                state = states.setdefault(
                    event.symbol, SymbolState(window_trades=config.VWAP_WINDOW_TRADES)
                )
                state.update(event.price, event.quantity)
                write_state(r, event, state, candles)
                consumer.commit(msg)
            except redis.RedisError:
                log.exception("redis write failed; message will be retried on next poll")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
