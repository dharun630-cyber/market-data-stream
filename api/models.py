from pydantic import BaseModel


class LatestTrade(BaseModel):
    symbol: str
    price: float
    vwap: float
    pct_change_window: float
    trade_time_ms: int
    updated_at_ms: int
    delayed: bool = False


class Candle(BaseModel):
    symbol: str
    bucket_start_s: int
    open: float
    high: float
    low: float
    close: float
    volume: float
