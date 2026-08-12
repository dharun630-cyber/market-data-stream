import os

REDPANDA_BOOTSTRAP_SERVERS = os.getenv("REDPANDA_BOOTSTRAP_SERVERS", "localhost:9092")
# Binance geo-blocks connections from United States IPs (HTTP 451), which
# includes every GCP free-tier region - so this runs against Coinbase's
# public Exchange feed instead, which doesn't have that restriction.
COINBASE_WS_URL = os.getenv("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
SYMBOLS = [
    s.strip().upper()
    for s in os.getenv("SYMBOLS", "BTC-USD,ETH-USD,SOL-USD").split(",")
    if s.strip()
]
RECONNECT_MIN_DELAY_S = float(os.getenv("RECONNECT_MIN_DELAY_S", "1"))
RECONNECT_MAX_DELAY_S = float(os.getenv("RECONNECT_MAX_DELAY_S", "60"))
