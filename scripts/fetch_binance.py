"""SOL/USDT Binance futures data collector.

This module fetches 15-minute OHLCV data, derives the 10 analytics payloads described in
`README.md`, and persists structured JSON/CSV files. The heavy lifting (network calls)
resides behind lightweight helper classes so we can unit test indicator math without
hitting external services.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from io import BytesIO
import zipfile
from urllib.parse import urlsplit, urlunsplit

try:
    import ccxt  # type: ignore
except ImportError:  # pragma: no cover - dependency optional for testing
    ccxt = None

try:
    import requests
except ImportError:  # pragma: no cover - dependency optional for testing
    requests = None  # type: ignore

DEFAULT_SYMBOL = "SOL/USDT"
DEFAULT_TIMEFRAME = "15m"
DEFAULT_LIMIT = 100
DATA_ROOT = Path(__file__).resolve().parents[1] / "data"
BINANCE_VISION_BASE = "https://data.binance.vision/data/futures/um/daily/klines"

FALLBACK_API_BASES = (
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi.binancefuture.com",
    "https://fapi.binance.me",
    "https://fapi.binanceusdt.com",
    "https://fapi.binanceusds.com",
    "https://fapi.binancezh.cc",
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api-gcp.binance.com",
    "https://api.binance.me",
    "https://api.binanceusdt.com",
    "https://api.binanceusds.com",
    "https://api.binancezh.cc",
)


@dataclass
class OHLCVRecord:
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float

    def as_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": datetime.fromtimestamp(self.timestamp / 1000, tz=timezone.utc).isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }


def _ema(values: Iterable[float], period: int) -> List[float]:
    values = list(values)
    if not values:
        return []
    multiplier = 2 / (period + 1)
    ema_values: List[float] = []
    ema_prev: Optional[float] = None
    for price in values:
        if ema_prev is None:
            ema_prev = price
        else:
            ema_prev = (price - ema_prev) * multiplier + ema_prev
        ema_values.append(ema_prev)
    return ema_values


def calculate_rsi(closes: Iterable[float], period: int = 14) -> List[Optional[float]]:
    closes = list(closes)
    if len(closes) < period + 1:
        return [None] * len(closes)
    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(abs(min(delta, 0.0)))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    rsi: List[Optional[float]] = [None] * period
    if avg_loss == 0:
        rsi.append(100.0)
    else:
        rs = avg_gain / avg_loss
        rsi.append(100 - (100 / (1 + rs)))
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        if avg_loss == 0:
            rsi.append(100.0)
        else:
            rs = avg_gain / avg_loss
            rsi.append(100 - (100 / (1 + rs)))
    rsi.insert(0, None)  # align with closes length
    return rsi[: len(closes)]


def calculate_macd(closes: Iterable[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[List[Optional[float]], List[Optional[float]]]:
    closes = list(closes)
    if len(closes) < slow:
        return [None] * len(closes), [None] * len(closes)
    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)
    macd_line = [f - s if f is not None and s is not None else None for f, s in zip(ema_fast, ema_slow)]
    valid_macd = [m for m in macd_line if m is not None]
    signal_line_raw = _ema(valid_macd, signal)
    signal_line: List[Optional[float]] = []
    idx = 0
    for value in macd_line:
        if value is None or idx >= len(signal_line_raw):
            signal_line.append(None)
        else:
            signal_line.append(signal_line_raw[idx])
            idx += 1
    return macd_line, signal_line


def calculate_bollinger(closes: Iterable[float], period: int = 20, num_std: float = 2.0) -> Tuple[List[Optional[float]], List[Optional[float]], List[Optional[float]]]:
    closes = list(closes)
    middle: List[Optional[float]] = [None] * len(closes)
    upper: List[Optional[float]] = [None] * len(closes)
    lower: List[Optional[float]] = [None] * len(closes)
    for i in range(period - 1, len(closes)):
        window = closes[i - period + 1 : i + 1]
        mean = sum(window) / period
        variance = sum((price - mean) ** 2 for price in window) / period
        std = math.sqrt(variance)
        middle[i] = mean
        upper[i] = mean + num_std * std
        lower[i] = mean - num_std * std
    return lower, middle, upper


def calculate_true_ranges(records: List[OHLCVRecord]) -> List[float]:
    true_ranges: List[float] = []
    for idx, candle in enumerate(records):
        if idx == 0:
            true_ranges.append(candle.high - candle.low)
            continue
        prev_close = records[idx - 1].close
        tr = max(
            candle.high - candle.low,
            abs(candle.high - prev_close),
            abs(candle.low - prev_close),
        )
        true_ranges.append(tr)
    return true_ranges


def calculate_atr(records: List[OHLCVRecord], period: int = 14) -> List[Optional[float]]:
    true_ranges = calculate_true_ranges(records)
    if len(true_ranges) < period:
        return [None] * len(true_ranges)
    atr_values: List[Optional[float]] = [None] * (period - 1)
    atr = sum(true_ranges[:period]) / period
    atr_values.append(atr)
    for tr in true_ranges[period:]:
        atr = (atr * (period - 1) + tr) / period
        atr_values.append(atr)
    return atr_values


def calculate_vwap(records: List[OHLCVRecord]) -> List[Optional[float]]:
    cumulative_price_volume = 0.0
    cumulative_volume = 0.0
    result: List[Optional[float]] = []
    for candle in records:
        typical_price = (candle.high + candle.low + candle.close) / 3
        cumulative_price_volume += typical_price * candle.volume
        cumulative_volume += candle.volume
        if cumulative_volume == 0:
            result.append(None)
        else:
            result.append(cumulative_price_volume / cumulative_volume)
    return result


def detect_price_pattern(records: List[OHLCVRecord]) -> str:
    """Derive a coarse price pattern label based on indicator positioning."""
    closes = [c.close for c in records]
    lower, middle, upper = calculate_bollinger(closes)
    rsi_values = calculate_rsi(closes)
    last_close = closes[-1]
    last_upper = upper[-1]
    last_lower = lower[-1]
    last_rsi = rsi_values[-1]
    if last_upper and last_close > last_upper:
        return "bullish_channel_breakout"
    if last_lower and last_close < last_lower:
        return "bearish_breakdown"
    if last_rsi and last_rsi >= 70:
        return "overbought_pullback"
    if last_rsi and last_rsi <= 30:
        return "oversold_reversal"
    return "range_consolidation"


def load_exchange() -> Any:
    if ccxt is None:
        raise ImportError("ccxt is required to fetch data. Install with `pip install ccxt`." )
    exchange = ccxt.binance({"enableRateLimit": True})
    exchange.options["defaultType"] = "future"
    return exchange


def _maybe_debug(message: str, *, enabled: bool) -> None:
    if enabled:
        print(message)


def _candidate_api_bases(exchange: Any) -> List[str]:
    override = os.getenv("BINANCE_API_BASE")
    if override:
        return [override]

    candidates: List[str] = []

    def _append_from_url(value: Any) -> None:
        if isinstance(value, str):
            parsed = urlsplit(value)
            if not parsed.scheme or not parsed.netloc:
                return
            base = urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))
            if base not in candidates:
                candidates.append(base)
        elif isinstance(value, dict):
            for nested in value.values():
                _append_from_url(nested)

    urls = getattr(exchange, "urls", {}) if hasattr(exchange, "urls") else {}
    if isinstance(urls, dict):
        for key in ("fapi", "api", "sapi"):
            if key in urls:
                _append_from_url(urls[key])

    for base in FALLBACK_API_BASES:
        if base not in candidates:
            candidates.append(base)

    return candidates


def _set_exchange_api(exchange: Any, base_url: str) -> None:
    if not hasattr(exchange, "urls") or not base_url:
        return

    urls = exchange.urls
    parsed_base = urlsplit(base_url)

    def _normalize_prefix(netloc: str) -> str:
        prefix = netloc.split(".")[0]
        return prefix.rstrip("0123456789")

    base_prefix = _normalize_prefix(parsed_base.netloc) if parsed_base.netloc else ""

    def _rewrite(value: Any) -> Any:
        if isinstance(value, str):
            if value.startswith("wss://") or value.startswith("ws://"):
                return value
            parsed_original = urlsplit(value)
            if not parsed_original.scheme or not parsed_original.netloc:
                return value

            original_prefix = _normalize_prefix(parsed_original.netloc)
            if base_prefix and original_prefix and original_prefix != base_prefix:
                netloc = parsed_original.netloc
            else:
                netloc = parsed_base.netloc or parsed_original.netloc

            scheme = parsed_base.scheme or parsed_original.scheme
            return urlunsplit(
                (scheme, netloc, parsed_original.path, parsed_original.query, parsed_original.fragment)
            )
        if isinstance(value, dict):
            return {key: _rewrite(subvalue) for key, subvalue in value.items()}
        return value

    for key in ("api", "fapi", "sapi"):
        if key in urls:
            urls[key] = _rewrite(urls[key])


def _is_geoblock_error(error: Exception) -> bool:
    message = str(error).lower()
    if "451" in message or "restricted location" in message:
        return True
    status = getattr(error, "http_status_code", None)
    return status == 451


def _is_retryable_error(error: Exception) -> bool:
    if _is_geoblock_error(error):
        return True

    if ccxt is not None:
        ccxt_retry_classes = []
        for name in ("NetworkError", "DDoSProtection", "RequestTimeout"):
            candidate = getattr(ccxt, name, None)
            if isinstance(candidate, type):
                ccxt_retry_classes.append(candidate)
        if ccxt_retry_classes and isinstance(error, tuple(ccxt_retry_classes)):
            return True

        exchange_not_available = getattr(ccxt, "ExchangeNotAvailable", None)
        if isinstance(exchange_not_available, type) and isinstance(error, exchange_not_available):
            message = str(error).lower()
            if any(token in message for token in ("timed out", "connection reset", "temporarily unavailable")):
                return True

    if requests is not None:
        timeout_cls = getattr(requests, "Timeout", None)
        if isinstance(timeout_cls, type) and isinstance(error, timeout_cls):
            return True
        exceptions_module = getattr(requests, "exceptions", None)
        if exceptions_module is not None:
            for name in ("Timeout", "ConnectionError"):  # pragma: no branch - tiny tuple
                candidate = getattr(exceptions_module, name, None)
                if isinstance(candidate, type) and isinstance(error, candidate):
                    return True

    message = str(error).lower()
    return any(token in message for token in ("timed out", "timeout", "connection reset"))


def fetch_ohlcv(
    exchange: Any,
    symbol: str = DEFAULT_SYMBOL,
    timeframe: str = DEFAULT_TIMEFRAME,
    limit: int = DEFAULT_LIMIT,
    *,
    debug: bool = False,
) -> List[OHLCVRecord]:
    last_error: Optional[Exception] = None
    for base in _candidate_api_bases(exchange):
        _set_exchange_api(exchange, base)
        _maybe_debug(f"attempting fetch via {base}", enabled=debug)
        try:
            raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            break
        except Exception as exc:  # noqa: BLE001 - propagate after fallbacks
            last_error = exc
            if _is_retryable_error(exc):
                _maybe_debug("retryable error detected, trying next domain", enabled=debug)
                continue
            raise
    else:
        fallback_records: List[OHLCVRecord] = []
        if last_error and _is_retryable_error(last_error):
            fallback_records = _fetch_ohlcv_from_binance_vision(
                symbol, timeframe, limit, debug=debug
            )
            if fallback_records:
                return fallback_records
        if last_error:
            raise last_error
        raise RuntimeError("fetch_ohlcv failed without raising an explicit error")

    return [
        OHLCVRecord(
            timestamp=item[0],
            open=float(item[1]),
            high=float(item[2]),
            low=float(item[3]),
            close=float(item[4]),
            volume=float(item[5]),
        )
        for item in raw
    ]


def _ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _write_json(data: Dict[str, Any], path: Path) -> None:
    _ensure_directory(path.parent)
    path.write_text(json.dumps(data, indent=2))


def _write_csv(rows: Iterable[Dict[str, Any]], fieldnames: List[str], path: Path) -> None:
    _ensure_directory(path.parent)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def build_price_snapshot(records: List[OHLCVRecord]) -> Dict[str, Any]:
    closes = [c.close for c in records]
    rsi = calculate_rsi(closes)
    macd_line, macd_signal = calculate_macd(closes)
    lower, middle, upper = calculate_bollinger(closes)
    pattern = detect_price_pattern(records)
    return {
        "timestamp": _iso_now(),
        "current_price": closes[-1],
        "ohlcv": [record.as_dict() for record in records],
        "pattern": pattern,
        "indicators": {
            "rsi_14": rsi[-1],
            "macd": {"line": macd_line[-1], "signal": macd_signal[-1]},
            "bollinger": {
                "lower": lower[-1],
                "middle": middle[-1],
                "upper": upper[-1],
            },
        },
    }


def build_atr_payload(records: List[OHLCVRecord]) -> List[Dict[str, Any]]:
    atr_values = calculate_atr(records)
    result: List[Dict[str, Any]] = []
    for record, atr in zip(records, atr_values):
        if atr is None:
            continue
        volatility_percent = atr / record.close * 100 if record.close else None
        result.append(
            {
                "timestamp": record.as_dict()["timestamp"],
                "atr_14": atr,
                "volatility_percent": volatility_percent,
            }
        )
    return result


def build_vwap_payload(records: List[OHLCVRecord]) -> List[Dict[str, Any]]:
    vwap_values = calculate_vwap(records)
    result: List[Dict[str, Any]] = []
    for record, vwap in zip(records, vwap_values):
        bias = None
        if vwap is not None:
            if record.close > vwap:
                bias = "above"
            elif record.close < vwap:
                bias = "below"
            else:
                bias = "at"
        result.append(
            {
                "timestamp": record.as_dict()["timestamp"],
                "vwap": vwap,
                "price_vs_vwap": bias,
            }
        )
    return result


def _timeframe_to_minutes(timeframe: str) -> int:
    unit = timeframe[-1]
    value = timeframe[:-1]
    if not value.isdigit():
        raise ValueError(f"unsupported timeframe: {timeframe}")
    multiplier = {"m": 1, "h": 60, "d": 1440}.get(unit)
    if multiplier is None:
        raise ValueError(f"unsupported timeframe unit: {timeframe}")
    return int(value) * multiplier


def _download_binance_vision_day(
    symbol: str,
    timeframe: str,
    day: datetime,
    *,
    debug: bool,
) -> List[OHLCVRecord]:
    if requests is None:
        return []

    symbol_token = symbol.replace("/", "")
    date_str = day.strftime("%Y-%m-%d")
    url = (
        f"{BINANCE_VISION_BASE}/{symbol_token}/{timeframe}/"
        f"{symbol_token}-{timeframe}-{date_str}.zip"
    )
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.HTTPError as exc:
        status = getattr(exc.response, "status_code", None)
        if status == 404:
            _maybe_debug(
                f"binance vision missing dataset for {date_str}", enabled=debug
            )
        else:
            _maybe_debug(
                f"binance vision request failed for {date_str}: {exc}", enabled=debug
            )
        return []
    except requests.RequestException:
        return []

    records: List[OHLCVRecord] = []
    try:
        with zipfile.ZipFile(BytesIO(response.content)) as archive:
            for name in archive.namelist():
                if not name.endswith(".csv"):
                    continue
                with archive.open(name) as handle:
                    text_stream = io.TextIOWrapper(handle, encoding="utf-8")
                    reader = csv.reader(text_stream)
                    for row in reader:
                        if len(row) < 6:
                            continue
                        try:
                            records.append(
                                OHLCVRecord(
                                    timestamp=int(row[0]),
                                    open=float(row[1]),
                                    high=float(row[2]),
                                    low=float(row[3]),
                                    close=float(row[4]),
                                    volume=float(row[5]),
                                )
                            )
                        except (TypeError, ValueError):
                            continue
    except zipfile.BadZipFile:
        _maybe_debug(
            f"failed to parse binance vision archive for {date_str}", enabled=debug
        )
        return []

    if records:
        _maybe_debug(
            f"downloaded {len(records)} candles from binance vision {date_str}",
            enabled=debug,
        )
    return records


def _fetch_ohlcv_from_binance_vision(
    symbol: str,
    timeframe: str,
    limit: int,
    *,
    debug: bool,
) -> List[OHLCVRecord]:
    try:
        interval_minutes = _timeframe_to_minutes(timeframe)
    except ValueError:
        return []

    candles_per_day = max(1, (24 * 60) // interval_minutes)
    required_days = max(2, math.ceil(limit / candles_per_day) + 1)
    today = datetime.now(timezone.utc).date()

    collected: List[OHLCVRecord] = []
    for offset in range(required_days):
        day_date = today - timedelta(days=offset)
        day = datetime.combine(day_date, datetime.min.time(), tzinfo=timezone.utc)
        records = _download_binance_vision_day(
            symbol, timeframe, day, debug=debug
        )
        if records:
            collected.extend(records)
        if len(collected) >= limit + candles_per_day:
            break

    if not collected:
        return []

    collected.sort(key=lambda item: item.timestamp)
    trimmed = collected[-limit:]
    if debug:
        _maybe_debug(
            f"binance vision fallback satisfied with {len(trimmed)} candles",
            enabled=True,
        )
    return trimmed


def _get_json(url: str, *, timeout: float = 10.0) -> Optional[Dict[str, Any]]:
    if requests is None:
        return None
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.RequestException:
        return None


def fetch_altseason_index() -> Optional[Dict[str, Any]]:
    payload = _get_json("https://www.blockchaincenter.net/api/altseason")
    if not payload:
        return None
    return {
        "timestamp": _iso_now(),
        "altseason_index": payload.get("altseasonIndex"),
        "components": payload.get("components"),
        "regime_status": "confirmed" if payload.get("altseasonIndex", 0) > 75 else "neutral",
    }


def _format_exchange_timestamp(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()


def build_funding_payload(entry: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "timestamp": entry.get("timestamp"),
        "current_rate": entry.get("fundingRate"),
        "predicted_rate": entry.get("nextFundingRate"),
        "cost_impact": entry.get("fundingRate") and entry["fundingRate"] / 3,
        "crowding": "long_crowded" if (entry.get("fundingRate") or 0) > 0.0005 else "balanced",
    }


def run(out_dir: Path, *, dry_run: bool = False, debug: bool = False) -> Dict[str, Path]:
    exchange = load_exchange()
    ohlcv = fetch_ohlcv(exchange, debug=debug)
    price_snapshot = build_price_snapshot(ohlcv)
    atr_rows = build_atr_payload(ohlcv)
    vwap_rows = build_vwap_payload(ohlcv)

    timestamp = datetime.now(timezone.utc)
    date_key = timestamp.strftime("%Y-%m-%d")

    outputs: Dict[str, Path] = {}

    price_path = out_dir / "price_patterns" / f"{date_key}.json"
    outputs["price"] = price_path
    atr_path = out_dir / "volatility" / f"{date_key}_atr.csv"
    outputs["atr"] = atr_path
    vwap_path = out_dir / "vwap" / f"{date_key}.json"
    outputs["vwap"] = vwap_path

    if dry_run:
        return outputs

    _write_json(price_snapshot, price_path)
    _write_csv(atr_rows, ["timestamp", "atr_14", "volatility_percent"], atr_path)
    _write_json({"timestamp": _iso_now(), "rows": vwap_rows}, vwap_path)
    return outputs


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch SOL/USDT 15m data and indicators")
    parser.add_argument("--out", type=Path, default=DATA_ROOT, help="Output directory root")
    parser.add_argument("--write", action="store_true", help="Persist computed datasets")
    parser.add_argument("--sleep", type=int, default=0, help="Optional delay before execution")
    parser.add_argument("--debug", action="store_true", help="Print fallback diagnostics during fetch")
    return parser


def main() -> None:
    parser = build_cli()
    args = parser.parse_args()
    if args.sleep:
        time.sleep(args.sleep)
    outputs = run(args.out, dry_run=not args.write, debug=args.debug)
    if args.write:
        for label, path in outputs.items():
            print(f"wrote {label}: {path}")
    else:
        print(json.dumps({k: str(v) for k, v in outputs.items()}, indent=2))


if __name__ == "__main__":
    main()
