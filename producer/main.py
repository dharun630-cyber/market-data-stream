"""Producer: ingests live trades from Binance's public WebSocket feed and
publishes them onto a Redpanda topic, keyed by symbol.

This is a genuinely live feed, not a replay of historical data, which means
it has to handle real failure modes: dropped connections, malformed frames,
and Binance's own 24-hour connection lifetime. The reconnect logic below is
what makes the "real-time" claim honest instead of decorative.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import signal
import time

import websockets
from confluent_kafka import Producer

from producer import config
from shared.schemas import TRADES_TOPIC, TradeEvent

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


def parse_trade(raw_payload: dict) -> TradeEvent | None:
    """Binance combined-stream trade payloads look like:

    {"stream": "btcusdt@trade", "data": {"e": "trade", "s": "BTCUSDT",
     "p": "43250.12", "q": "0.0023", "t": 123456, "T": 1712345678901,
     "m": true, ...}}
    """
    data = raw_payload.get("data")
    if not data or data.get("e") != "trade":
        return None
    return TradeEvent(
        symbol=data["s"],
        price=float(data["p"]),
        quantity=float(data["q"]),
        trade_id=int(data["t"]),
        trade_time_ms=int(data["T"]),
        ingested_at_ms=int(time.time() * 1000),
        is_buyer_maker=bool(data["m"]),
    )


async def stream_trades(producer: Producer, stop_event: asyncio.Event) -> None:
    streams = "/".join(f"{s}@trade" for s in config.SYMBOLS)
    url = f"{config.BINANCE_WS_BASE}?streams={streams}"
    delay = config.RECONNECT_MIN_DELAY_S

    while not stop_event.is_set():
        try:
            log.info("connecting to %s", url)
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                delay = config.RECONNECT_MIN_DELAY_S  # reset backoff after a successful connect
                log.info("connected, streaming %s", config.SYMBOLS)
                async for raw in ws:
                    if stop_event.is_set():
                        break
                    try:
                        payload = json.loads(raw)
                    except json.JSONDecodeError:
                        log.warning("dropped malformed frame")
                        continue
                    event = parse_trade(payload)
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
