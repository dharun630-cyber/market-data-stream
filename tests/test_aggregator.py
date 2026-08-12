from consumer.aggregator import CandleAccumulator, SymbolState


def test_vwap_is_volume_weighted():
    state = SymbolState(window_trades=10)
    state.update(price=100, qty=1)
    state.update(price=200, qty=3)
    # vwap = (100*1 + 200*3) / (1+3) = 700/4 = 175
    assert state.vwap == 175


def test_window_trims_old_trades():
    state = SymbolState(window_trades=2)
    state.update(price=100, qty=1)
    state.update(price=200, qty=1)
    state.update(price=300, qty=1)
    assert len(state.prices) == 2
    assert list(state.prices) == [200, 300]


def test_candle_tracks_high_low_close():
    candle = CandleAccumulator.start(price=100, qty=1)
    candle.update(price=110, qty=1)
    candle.update(price=90, qty=1)
    candle.update(price=105, qty=1)
    assert candle.open == 100
    assert candle.high == 110
    assert candle.low == 90
    assert candle.close == 105
    assert candle.volume == 4
