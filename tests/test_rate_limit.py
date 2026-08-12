import pytest
from fastapi import HTTPException

from api import config
from api.auth import ClientIdentity
from api.rate_limit import check_rate_limit


def test_allows_requests_under_the_limit(fake_redis):
    identity = ClientIdentity(key_hash="abc", tier="free", owner="test")
    for _ in range(5):
        check_rate_limit(identity)  # default free limit is well above 5


def test_blocks_requests_over_the_limit(fake_redis):
    identity = ClientIdentity(key_hash="abc", tier="free", owner="test")
    for _ in range(config.RATE_LIMIT_FREE_PER_MIN):
        check_rate_limit(identity)

    with pytest.raises(HTTPException) as exc_info:
        check_rate_limit(identity)
    assert exc_info.value.status_code == 429


def test_tiers_are_independent(fake_redis):
    free_identity = ClientIdentity(key_hash="free-key", tier="free", owner="a")
    pro_identity = ClientIdentity(key_hash="pro-key", tier="pro", owner="b")

    for _ in range(config.RATE_LIMIT_FREE_PER_MIN):
        check_rate_limit(free_identity)

    # a different key on a higher tier is unaffected by the free key's usage
    check_rate_limit(pro_identity)
