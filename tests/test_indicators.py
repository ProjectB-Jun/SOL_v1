import math
from datetime import datetime, timezone

from scripts.fetch_binance import (
    OHLCVRecord,
    build_atr_payload,
    build_price_snapshot,
    build_vwap_payload,
    calculate_bollinger,
    calculate_macd,
    calculate_rsi,
)


def sample_records() -> list[OHLCVRecord]:
    base_ts = int(datetime(2025, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    records = []
    price = 100.0
    for idx in range(30):
        high = price + 2
        low = price - 2
        close = price + math.sin(idx / 3)
        volume = 1000 + idx * 10
        records.append(
            OHLCVRecord(
                timestamp=base_ts + idx * 15 * 60 * 1000,
                open=price,
                high=high,
                low=low,
                close=close,
                volume=volume,
            )
        )
        price += 0.5
    return records


def test_calculate_rsi_monotonic():
    closes = [record.close for record in sample_records()]
    rsi = calculate_rsi(closes)
    # ensure most recent RSI is bounded and floats
    assert 0 <= rsi[-1] <= 100


def test_macd_signal_alignment():
    closes = [record.close for record in sample_records()]
    macd_line, signal_line = calculate_macd(closes)
    assert len(macd_line) == len(signal_line) == len(closes)
    assert signal_line[-1] is None or isinstance(signal_line[-1], float)


def test_bollinger_upper_above_middle():
    closes = [record.close for record in sample_records()]
    lower, middle, upper = calculate_bollinger(closes)
    assert len(lower) == len(middle) == len(upper) == len(closes)
    # For the most recent candle the bands should exist and upper > middle > lower
    assert upper[-1] > middle[-1] > lower[-1]


def test_build_price_snapshot_fields():
    snapshot = build_price_snapshot(sample_records())
    assert set(snapshot.keys()) == {"timestamp", "current_price", "ohlcv", "pattern", "indicators"}
    assert len(snapshot["ohlcv"]) == 30
    assert "rsi_14" in snapshot["indicators"]


def test_build_atr_payload_length():
    records = sample_records()
    atr_rows = build_atr_payload(records)
    # ATR requires at least 14 periods, so we should have fewer rows than inputs
    assert len(atr_rows) == len(records) - 13


def test_build_vwap_bias_values():
    rows = build_vwap_payload(sample_records())
    assert rows[-1]["price_vs_vwap"] in {"above", "below", "at"}
    assert rows[0]["vwap"] is None or isinstance(rows[0]["vwap"], float)


def test_detect_pattern_runs():
    snapshot = build_price_snapshot(sample_records())
    assert isinstance(snapshot["pattern"], str)

