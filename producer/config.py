import os

REDPANDA_BOOTSTRAP_SERVERS = os.getenv("REDPANDA_BOOTSTRAP_SERVERS", "localhost:9092")
BINANCE_WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://stream.binance.com:9443/stream")
SYMBOLS = [
    s.strip().lower()
    for s in os.getenv("SYMBOLS", "btcusdt,ethusdt,solusdt").split(",")
    if s.strip()
]
RECONNECT_MIN_DELAY_S = float(os.getenv("RECONNECT_MIN_DELAY_S", "1"))
RECONNECT_MAX_DELAY_S = float(os.getenv("RECONNECT_MAX_DELAY_S", "60"))
