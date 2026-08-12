import os

REDPANDA_BOOTSTRAP_SERVERS = os.getenv("REDPANDA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "aggregator-group")
VWAP_WINDOW_TRADES = int(os.getenv("VWAP_WINDOW_TRADES", "200"))  # rolling window size, in trade count
