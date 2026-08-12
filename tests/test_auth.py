import json

import pytest
from fastapi import HTTPException

from api.auth import hash_key, require_api_key
from shared.schemas import redis_apikey_hash_key


def test_missing_header_is_rejected(fake_redis):
    with pytest.raises(HTTPException) as exc_info:
        require_api_key(x_api_key=None)
    assert exc_info.value.status_code == 401


def test_unknown_key_is_rejected(fake_redis):
    with pytest.raises(HTTPException) as exc_info:
        require_api_key(x_api_key="not-a-real-key")
    assert exc_info.value.status_code == 401


def test_valid_key_resolves_identity(fake_redis):
    fake_redis.hset(
        redis_apikey_hash_key(),
        hash_key("good-key"),
        json.dumps({"tier": "pro", "owner": "dharun"}),
    )
    identity = require_api_key(x_api_key="good-key")
    assert identity.tier == "pro"
    assert identity.owner == "dharun"
