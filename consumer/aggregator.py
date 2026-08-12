"""Pure aggregation logic, kept separate from the Kafka/Redis plumbing so
it's trivially unit-testable without a broker or a cache running.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field


@dataclass
class SymbolState:
    """Rolling state for one symbol, held in the consumer's memory between
    Redis writes. Redis is the durable, externally-visible copy; this is
    just the working set used to compute the next update cheaply.
    """

    window_trades: int
    prices: deque = field(default_factory=deque)
    volumes: deque = field(default_factory=deque)
    latest_price: float = 0.0
    latest_qty: float = 0.0

    def update(self, price: float, qty: float) -> None:
        self.latest_price = price
        self.latest_qty = qty
        self.prices.append(price)
        self.volumes.append(qty)
        while len(self.prices) > self.window_trades:
            self.prices.popleft()
            self.volumes.popleft()

    @property
    def vwap(self) -> float:
        notional = sum(p * v for p, v in zip(self.prices, self.volumes, strict=True))
        volume = sum(self.volumes)
        return notional / volume if volume else self.latest_price

    @property
    def pct_change_window(self) -> float:
        if len(self.prices) < 2 or self.prices[0] == 0:
            return 0.0
        return (self.prices[-1] - self.prices[0]) / self.prices[0] * 100


@dataclass
class CandleAccumulator:
    """OHLC state for a single minute bucket."""

    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0

    @classmethod
    def start(cls, price: float, qty: float) -> CandleAccumulator:
        return cls(open=price, high=price, low=price, close=price, volume=qty)

    def update(self, price: float, qty: float) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += qty
