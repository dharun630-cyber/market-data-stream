"""Producer: ingests live trades from Coinbase's public Exchange WebSocket
feed and publishes them onto a Redpanda topic, keyed by symbol.

Originally written against Binance's public stream. Binance returns
HTTP 451 (geo-blocked) for connections from United States IP addresses,
which includes every GCP free-tier region - so it never worked from the
actual deployment target, even though it worked fine in local testing.
Coinbase, as a US-licensed exchange, has no such restriction. That gap
between "works on my machine" and "works from where it's deployed" is
exactly the kind of thing a real live feed catches that a replay of
historical data never would.

This is still a genuinely live feed with the same failure modes as
before: a dropped connection, a malformed frame, and reconnect-with-
backoff is what makes the "real-time" claim honest.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import signal
import time
from datetime import datetime, timezone

import websockets
from confluent_kafka import Producer

from producer import config
from shared.schemas import TradeEvent, TRADES_TOPIC

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")


def build_kafka_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": config.REDPANDA_BOOTSTRAP_SERVERS,
            "enable.idempotence": True,  # exactly-once delivery to the broker, per partition
            "acks": "all",
            "retries": 5,
            "linger.ms": 20,  # small batching window: keeps latency low, avoids one-request-per-message
        }
    )


def delivery_report(err, msg) -> None:
    if err is not None:
        log.warning("delivery failed for key=%s: %s", msg.key(), err)


def _parse_iso8601_to_ms(ts: str) -> int:
    """Coinbase sends timestamps like '2026-08-12T13:40:00.123456Z'."""
    dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def parse_trade(data: dict) -> TradeEvent | None:
    """Coinbase Exchange 'matches' channel messages look like:

    {"type": "match", "trade_id": 12345, "product_id": "BTC-USD",
     "price": "43250.12", "size": "0.0023", "side": "sell",
     "time": "2026-08-12T13:40:00.123456Z", ...}

    'last_match' is the same shape, sent once immediately after
    subscribing so a new client isn't left with an empty book.
    """
    if data.get("type") not in ("match", "last_match"):
        return None
    return TradeEvent(
        symbol=data["product_id"],
        price=float(data["price"]),
        quantity=float(data["size"]),
        trade_id=int(data["trade_id"]),
        trade_time_ms=_parse_iso8601_to_ms(data["time"]),
        ingested_at_ms=int(time.time() * 1000),
        is_buyer_maker=(data.get("side") == "sell"),
    )


async def stream_trades(producer: Producer, stop_event: asyncio.Event) -> None:
    delay = config.RECONNECT_MIN_DELAY_S
    subscribe_msg = json.dumps(
        {"type": "subscribe", "product_ids": config.SYMBOLS, "channels": ["matches"]}
    )

    while not stop_event.is_set():
        try:
            log.info("connecting to %s", config.COINBASE_WS_URL)
            async with websockets.connect(
                config.COINBASE_WS_URL, ping_interval=20, ping_timeout=20
            ) as ws:
                await ws.send(subscribe_msg)
                delay = config.RECONNECT_MIN_DELAY_S  # reset backoff after a successful connect
                log.info("connected, streaming %s", config.SYMBOLS)
                async for raw in ws:
                    if stop_event.is_set():
                        break
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        log.warning("dropped malformed frame")
                        continue
                    if data.get("type") == "error":
                        log.warning("exchange sent an error message: %s", data)
                        continue
                    event = parse_trade(data)
                    if event is None:
                        continue
                    producer.produce(
                        TRADES_TOPIC,
                        key=event.symbol.encode("utf-8"),
                        value=event.to_json(),
                        callback=delivery_report,
                    )
                    producer.poll(0)  # non-blocking; services delivery callbacks without stalling ingest
        except (websockets.ConnectionClosed, OSError) as exc:
            log.warning("connection dropped (%s); reconnecting in %.1fs", exc, delay)
        except Exception:
            log.exception("unexpected error in stream loop; reconnecting in %.1fs", delay)

        if stop_event.is_set():
            break
        await asyncio.sleep(delay + random.uniform(0, delay * 0.25))  # jitter avoids a thundering herd
        delay = min(delay * 2, config.RECONNECT_MAX_DELAY_S)


async def main() -> None:
    producer = build_kafka_producer()
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    try:
        await stream_trades(producer, stop_event)
    finally:
        log.info("flushing producer before exit")
        producer.flush(10)


if __name__ == "__main__":
    asyncio.run(main())
