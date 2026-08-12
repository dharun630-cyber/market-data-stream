import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import fakeredis
import pytest

import api.redis_client as redis_client_module


@pytest.fixture(autouse=True)
def fake_redis(monkeypatch):
    fake = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(redis_client_module, "_client", fake)
    yield fake
