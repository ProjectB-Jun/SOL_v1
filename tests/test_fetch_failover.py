"""Tests for fetch_binance resiliency helpers."""
from __future__ import annotations

import types

import pytest

from scripts import fetch_binance


class DummyExchange:
    def __init__(self) -> None:
        self.urls = {
            "api": "https://api.binance.com",
            "fapi": "https://fapi.binance.com",
            "sapi": "https://sapi.binance.com",
        }
        self.calls = 0

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int):  # noqa: D401 - simple stub
        """Return static candle data after one geoblocked attempt."""
        self.calls += 1
        if self.calls == 1:
            raise DummyGeoblockError("code 451 restricted location")
        return [
            [1_700_000_000_000, 100.0, 105.0, 95.0, 102.0, 1200.0],
        ]


class DummyGeoblockError(Exception):
    http_status_code = 451


def test_candidate_api_bases_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    exchange = DummyExchange()
    monkeypatch.setenv("BINANCE_API_BASE", "https://custom.binance.com")
    bases = fetch_binance._candidate_api_bases(exchange)
    assert bases == ["https://custom.binance.com"]


def test_fetch_ohlcv_geoblock_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    exchange = DummyExchange()
    monkeypatch.delenv("BINANCE_API_BASE", raising=False)
    monkeypatch.setattr(
        fetch_binance,
        "ccxt",
        types.SimpleNamespace(ExchangeNotAvailable=DummyGeoblockError),
    )

    candles = fetch_binance.fetch_ohlcv(exchange, debug=True)

    assert exchange.calls == 2
    assert candles[0].close == 102.0
    # urls should be updated to the next candidate host
    assert exchange.urls["api"] != "https://api.binance.com"
