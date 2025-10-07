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


class DummyTimeoutError(Exception):
    pass


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


def test_fetch_ohlcv_network_timeout_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    class TimeoutExchange(DummyExchange):
        def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int):  # noqa: D401 - stub
            self.calls += 1
            self.api_hosts.append(self.urls["api"])
            self.fapi_hosts.append(self.urls["fapi"])
            if self.calls == 1:
                raise DummyTimeoutError("Read timed out")
            return [
                [1_700_000_000_000, 100.0, 105.0, 95.0, 102.0, 1200.0],
            ]

    exchange = TimeoutExchange()
    monkeypatch.delenv("BINANCE_API_BASE", raising=False)
    monkeypatch.setattr(
        fetch_binance,
        "ccxt",
        types.SimpleNamespace(NetworkError=DummyTimeoutError, ExchangeNotAvailable=DummyGeoblockError),
    )

    candles = fetch_binance.fetch_ohlcv(exchange, debug=True)

    assert exchange.calls == 2
    assert candles[0].close == 102.0
    assert len(set(exchange.api_hosts + exchange.fapi_hosts)) >= 2


def test_fetch_ohlcv_from_binance_vision_parses_zip(monkeypatch: pytest.MonkeyPatch) -> None:
    import csv
    import io
    import zipfile

    from datetime import datetime, timezone, timedelta

    if fetch_binance.requests is None:
        pytest.skip("requests library unavailable")

    rows = [
        [1_699_999_000_000, "100", "110", "90", "105", "500"],
        [1_699_999_900_000, "105", "115", "100", "110", "600"],
    ]

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        previous_day = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        filename = f"SOLUSDT-15m-{previous_day}.csv"
        with archive.open(filename, "w") as handle:
            text_stream = io.TextIOWrapper(handle, encoding="utf-8", newline="")
            writer = csv.writer(text_stream)
            writer.writerows(rows)
            text_stream.flush()

    class DummyResponse:
        def __init__(self, content: bytes) -> None:
            self.content = content

        def raise_for_status(self) -> None:
            return None

    calls: list[str] = []

    http_error = fetch_binance.requests.HTTPError  # type: ignore[assignment]
    request_exception = fetch_binance.requests.RequestException  # type: ignore[assignment]

    def fake_get(url: str, timeout: float = 0.0):
        calls.append(url)
        if previous_day in url:
            return DummyResponse(buffer.getvalue())
        raise http_error(response=types.SimpleNamespace(status_code=404))

    monkeypatch.setattr(
        fetch_binance,
        "requests",
        types.SimpleNamespace(get=fake_get, HTTPError=http_error, RequestException=request_exception),
    )

    records = fetch_binance._fetch_ohlcv_from_binance_vision(
        "SOL/USDT", "15m", 2, debug=False
    )

    assert len(records) == 2
    assert records[-1].close == 110.0
    assert calls


def test_fetch_ohlcv_falls_back_to_binance_vision(monkeypatch: pytest.MonkeyPatch) -> None:
    exchange = DummyExchange()
    monkeypatch.delenv("BINANCE_API_BASE", raising=False)

    class AlwaysFail(Exception):
        pass

    monkeypatch.setattr(
        fetch_binance,
        "ccxt",
        types.SimpleNamespace(NetworkError=AlwaysFail, ExchangeNotAvailable=AlwaysFail),
    )

    def failing_fetch(symbol: str, timeframe: str, limit: int):
        raise AlwaysFail("network unreachable")

    exchange.fetch_ohlcv = failing_fetch  # type: ignore[assignment]

    stub_record = fetch_binance.OHLCVRecord(
        timestamp=1_700_000_000_000,
        open=100.0,
        high=110.0,
        low=95.0,
        close=105.0,
        volume=1_000.0,
    )

    monkeypatch.setattr(
        fetch_binance,
        "_fetch_ohlcv_from_binance_vision",
        lambda *args, **kwargs: [stub_record],
    )

    candles = fetch_binance.fetch_ohlcv(exchange, debug=True)

    assert candles == [stub_record]
