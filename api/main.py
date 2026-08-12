"""FastAPI serving layer: the only part of this system that end users or
partner apps talk to directly. Everything upstream - Redpanda, the
consumer - is invisible to callers; this is the product surface.
"""
from __future__ import annotations

import time

from fastapi import FastAPI, Request

from api.routers import health, stream, symbols

app = FastAPI(
    title="Live Market Data API",
    description="Real-time trade aggregates over a live, WebSocket-ingested exchange feed.",
    version="0.1.0",
)

app.include_router(symbols.router)
app.include_router(stream.router)
app.include_router(health.router)


@app.middleware("http")
async def track_requests(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000
    health.REQUEST_COUNTER.labels(path=request.url.path, status=response.status_code).inc()
    response.headers["X-Response-Time-ms"] = f"{duration_ms:.1f}"
    return response
