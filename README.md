# Live Market Data Stream

A real-time trade-aggregation pipeline: a live exchange feed flows through
Redpanda into a stateful consumer, gets served through a rate-limited,
authenticated FastAPI layer, and is deployed across a self-hosted broker
and a serverless API tier.

The point of this project is the systems work, not the trading signals.
Everything below is written with that emphasis - what the components are
is the easy part; why they're shaped this way is what this README spends
its words on.

## Architecture

```
Binance WebSocket (live trades)
        |
        v
   Producer  -- reconnect + backoff, idempotent Kafka producer
        |
        v
Redpanda topic "market-trades"  -- partitioned by symbol
        |
        v
   Consumer  -- rolling VWAP + OHLC candles, dedup, commit-after-write
        |
        v
      Redis  -- latest state, candle history, pub/sub, rate-limit counters
        |
        v
   FastAPI  -- API-key auth, tiered rate limiting, REST + WebSocket
        |
        v
  Dashboards / partner apps
```

The producer and consumer are the only two services that ever touch
Redpanda. The API only ever reads Redis. That split is deliberate - see
below.

## Why it's built this way

**The API never talks to Kafka.** Only the consumer reads the topic; it
materialises state into Redis, and the API reads Redis. A request-time
Kafka consumer is a common mistake in projects like this - it couples API
latency to consumer lag, and it breaks the moment you run more than one
API replica, since each replica would need its own consumer group member.

**Partitioning by symbol.** Every trade for one symbol lands on the same
partition, which is what lets the consumer maintain correct rolling state
(VWAP, candle high/low) without any reordering logic. Cross-symbol
ordering doesn't matter for this workload, so a single partition key is
enough - no need for a more complex partitioning scheme.

**Commit-after-write, with a dedup guard, not a full exactly-once
pipeline.** The consumer commits its Kafka offset only after Redis has
durably recorded the update, and a short-lived dedup marker (keyed by
`symbol:trade_id`) protects against reprocessing on a crash-and-restart.
If the process dies between the dedup check and the full Redis write, the
redelivered message is treated as already-seen and dropped rather than
reapplied - meaning the failure mode is "lose one trade out of a
high-frequency stream" rather than "silently double-count volume in every
downstream VWAP and candle." That's an acceptable trade for a rolling
analytics view. It would not be acceptable for a ledger or balance -
there you'd want the opposite bias, and would reach for Kafka's
transactional API instead of a best-effort dedup key.

**A genuinely live feed, not a replay.** The producer holds a live
WebSocket connection to Binance's public trade stream rather than
replaying historical data at a controlled rate. That's a real trade-off:
it means the producer has to handle real failure modes - a dropped
connection, a malformed frame, Binance's own 24-hour connection lifetime
- with actual reconnect-and-backoff logic, instead of a replay script
that never has to recover from anything. Reconnect delay grows
exponentially (capped at 60s) with jitter, to avoid hammering the
exchange the moment a network blip resolves.

**Auth is API keys, not OAuth2.** Keys are hashed (SHA-256) before
storage - only the hash is ever persisted or logged, so a Redis dump or a
log leak doesn't hand out working credentials. Storing keys in Redis
rather than a relational store is a scope decision: it keeps the
deployment to one moving part instead of two. A system with billing, key
rotation, and audit history would want Postgres for this; documenting
that rather than pretending Redis is the "correct" long-term answer is
the point.

**Rate limiting is keyed by API key, not IP, and backed by Redis, not
memory.** A paid tier needs to actually get a higher limit no matter
where the request comes from, which rules out IP-keying. And an
in-memory limiter silently stops working the moment the API runs as more
than one replica - each replica would hand out its own separate quota.
The limiter itself is a Redis sorted-set sliding window: each request
adds a timestamp, stale entries outside the window get trimmed, and the
remaining count is compared against the caller's tier limit.

**The free-tier delay is structural, not simulated.** Free-tier callers
to `/latest` are served the last *completed* one-minute candle instead of
the live tick - a real delay built from data that already exists, rather
than a `sleep()` or an arbitrary rejected request. This mirrors how real
market-data vendors (Bloomberg, IEX) gate real-time quotes behind a paid
tier: the delay is the product, not an artificial restriction bolted on
top.

## Running locally

```bash
cp .env.example .env
docker compose up --build
```

This starts Redpanda, Redis, the producer (which immediately starts
pulling live trades from Binance), the consumer, and the API on
`localhost:8000`.

Seed yourself an API key before calling the API:

```bash
pip install -r requirements-dev.txt
python scripts/seed_api_key.py mykey free
# or: python scripts/seed_api_key.py mykey pro
```

```bash
curl -H "X-API-Key: mykey" http://localhost:8000/v1/symbols
curl -H "X-API-Key: mykey" http://localhost:8000/v1/symbols/BTCUSDT/latest
curl -H "X-API-Key: mykey" http://localhost:8000/v1/symbols/BTCUSDT/candles?limit=10
```

Give it a minute after startup for the first candles to accumulate.

## API reference

| Endpoint | Auth | Description |
|---|---|---|
| `GET /v1/symbols` | required | List symbols with live data |
| `GET /v1/symbols/{symbol}/latest` | required | Latest trade (delayed ~60s on the free tier) |
| `GET /v1/symbols/{symbol}/candles?limit=N` | required | Last N one-minute OHLC candles |
| `WS /v1/stream/{symbol}?api_key=...` | required | Live push of every state update |
| `GET /healthz` | none | Liveness check |
| `GET /metrics` | none | Prometheus-format request metrics |

## Deployment

No managed Kafka-API cloud (Redpanda Serverless, Confluent Cloud) currently
offers a lasting free tier - they're all time-boxed trials, which is the
wrong shape for something that needs to stay reachable indefinitely. So:

- **Redpanda, the producer, and the consumer** run continuously and belong
  on a small always-on VM (`infra/deploy/vm-setup.sh` bootstraps one -
  works on a GCP free-tier `e2-micro` or any cheap VPS).
- **The API** is bursty, on-demand traffic, which is exactly what Cloud
  Run's permanent free tier (2M requests/month) is shaped for
  (`infra/deploy/cloudrun-deploy.sh`).
- **Redis** should be a small managed instance (Upstash's free tier
  covers this workload comfortably) rather than living only on the VM, so
  the API can reach it independently of the VM's lifecycle.

## Known limitations / v2

- Events are plain JSON. A v2 would move to Avro plus a schema registry -
  cheap to say, deliberately not built for v1 to keep scope tight.
- `GET /v1/symbols` uses Redis `KEYS`, fine at this scale (a handful of
  symbols) but would move to an explicit tracked set + `SCAN` at higher
  cardinality.
- No load-test report yet. `locust` or `k6` against the rate limiter,
  showing it actually holding under load, is the natural next artifact.
- WebSocket auth passes the API key as a query parameter, which can end
  up in access logs - noted in `api/routers/stream.py` rather than
  hidden. A production system would use a short-lived signed token
  exchanged over REST first.

## Stack

Python 3.12 · FastAPI · Redpanda (Kafka API) · `confluent-kafka` · Redis ·
Docker · GitHub Actions · Google Cloud Run
