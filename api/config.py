import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
RATE_LIMIT_FREE_PER_MIN = int(os.getenv("RATE_LIMIT_FREE_PER_MIN", "30"))
RATE_LIMIT_PRO_PER_MIN = int(os.getenv("RATE_LIMIT_PRO_PER_MIN", "300"))
FREE_TIER_DELAY_SECONDS = int(os.getenv("FREE_TIER_DELAY_SECONDS", "60"))
