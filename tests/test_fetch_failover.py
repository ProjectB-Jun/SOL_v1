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
        self.api_hosts: list[str] = []
        self.fapi_hosts: list[str] = []

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int):  # noqa: D401 - simple stub
        """Return static candle data after one geoblocked attempt."""
        self.calls += 1
        self.api_hosts.append(self.urls["api"])
        self.fapi_hosts.append(self.urls["fapi"])
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
    # ensure we tried at least two candidate domains overall
    assert len(set(exchange.api_hosts + exchange.fapi_hosts)) >= 2


def test_candidate_api_bases_handles_nested_structure() -> None:
    exchange = DummyExchange()
    exchange.urls["api"] = {
        "public": "https://api.binance.com/api/v3",
        "private": "https://api.binance.com/api/v3",
    }
    exchange.urls["fapi"] = "https://fapi.binance.com/fapi/v1"

    bases = fetch_binance._candidate_api_bases(exchange)

    assert bases[0] == "https://fapi.binance.com"
    assert "https://api.binance.com" in bases
    assert "https://fapi.binancefuture.com" in bases
    assert "https://fapi.binance.me" in bases
    assert "https://fapi.binanceusdt.com" in bases
    assert "https://fapi.binanceusds.com" in bases
    assert "https://fapi.binancezh.cc" in bases


def test_set_exchange_api_preserves_paths() -> None:
    exchange = DummyExchange()
    exchange.urls["api"] = {
        "public": "https://api.binance.com/api/v3",
        "wapi": "https://api.binance.com/wapi/v3",
    }
    exchange.urls["fapi"] = "https://fapi.binance.com/fapi/v1"
    exchange.urls["ws"] = "wss://fstream.binance.com/stream"

    fetch_binance._set_exchange_api(exchange, "https://fapi1.binance.com")

    assert exchange.urls["api"]["public"] == "https://api.binance.com/api/v3"
    assert exchange.urls["api"]["wapi"] == "https://api.binance.com/wapi/v3"
    assert exchange.urls["fapi"] == "https://fapi1.binance.com/fapi/v1"
    # websocket endpoints should remain untouched
    assert exchange.urls["ws"] == "wss://fstream.binance.com/stream"


def test_set_exchange_api_respects_domain_prefix() -> None:
    exchange = DummyExchange()
    exchange.urls["api"] = "https://api.binance.com/api/v3"
    exchange.urls["fapi"] = "https://fapi.binance.com/fapi/v1"

    fetch_binance._set_exchange_api(exchange, "https://api1.binance.com")

    assert exchange.urls["api"] == "https://api1.binance.com/api/v3"
    assert exchange.urls["fapi"] == "https://fapi.binance.com/fapi/v1"
