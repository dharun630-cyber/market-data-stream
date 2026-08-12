import json

from fastapi.testclient import TestClient

from api.auth import hash_key
from api.main import app
from shared.schemas import redis_apikey_hash_key, redis_latest_key


def _seed_key(fake_redis, raw_key: str, tier: str) -> None:
    fake_redis.hset(
        redis_apikey_hash_key(),
        hash_key(raw_key),
        json.dumps({"tier": tier, "owner": "test"}),
    )


def test_latest_requires_api_key(fake_redis):
    client = TestClient(app)
    response = client.get("/v1/symbols/BTCUSDT/latest")
    assert response.status_code == 401


def test_latest_returns_404_when_no_data(fake_redis):
    _seed_key(fake_redis, "prokey", "pro")
    client = TestClient(app)
    response = client.get("/v1/symbols/BTCUSDT/latest", headers={"X-API-Key": "prokey"})
    assert response.status_code == 404


def test_pro_tier_sees_the_live_tick(fake_redis):
    _seed_key(fake_redis, "prokey", "pro")
    fake_redis.set(
        redis_latest_key("BTCUSDT"),
        json.dumps(
            {
                "symbol": "BTCUSDT",
                "price": 43250.12,
                "vwap": 43248.5,
                "pct_change_window": 0.02,
                "trade_time_ms": 1712345678901,
                "updated_at_ms": 1712345678901,
            }
        ),
    )
    client = TestClient(app)
    response = client.get("/v1/symbols/BTCUSDT/latest", headers={"X-API-Key": "prokey"})
    assert response.status_code == 200
    assert response.json()["delayed"] is False


def test_healthz(fake_redis):
    client = TestClient(app)
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
