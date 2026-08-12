"""One-off helper: create an API key for local testing.

Usage: python scripts/seed_api_key.py mykey123 free
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import redis

from api import config
from api.auth import hash_key
from shared.schemas import redis_apikey_hash_key


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: python scripts/seed_api_key.py <raw-key> [free|pro]")
        raise SystemExit(1)

    raw_key = sys.argv[1]
    tier = sys.argv[2] if len(sys.argv) > 2 else "free"

    r = redis.Redis.from_url(config.REDIS_URL, decode_responses=True)
    r.hset(
        redis_apikey_hash_key(),
        hash_key(raw_key),
        json.dumps({"tier": tier, "owner": "local-dev"}),
    )
    print(f"seeded API key (tier={tier}) - send it as the X-API-Key header")


if __name__ == "__main__":
    main()
