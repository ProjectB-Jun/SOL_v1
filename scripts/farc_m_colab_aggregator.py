"""Colab-friendly FAR-CM data aggregation pipeline for SOL/USDT.

This module implements the data collection and aggregation requirements outlined in
the user specification.  The goal is to provide a single copy/paste friendly script
that can run inside Google Colab without additional project scaffolding while still
respecting the repository's engineering practices.

The script focuses on deterministic, pandas-based ETL steps backed by Binance REST
endpoints.  All timestamps are normalised to KST and the resulting artefacts are
stored as <=10 column CSV files under a configurable output directory (default:
``data/colab_exports``).  Each helper function is kept pure to make unit testing
straightforward and to allow downstream notebooks to reuse the computations.

Usage (inside Colab or a local virtualenv)::

    !python scripts/farc_m_colab_aggregator.py --symbol SOLUSDT

The command downloads the latest 1-minute SOL perpetual & spot candles, funding
history, open interest, and a fresh order book snapshot, computes the derived
metrics (basis, volatility, ATR, momentum, regime classification, LOB indicators,
and sizing guidance), then saves up to five CSV files plus a JSON status report.
"""

from __future__ import annotations

import argparse
import json
import math
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Tuple

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

KST = timezone(timedelta(hours=9))

# Binance endpoints
BINANCE_FUTURES_REST = "https://fapi.binance.com"
BINANCE_SPOT_REST = "https://api.binance.com"


def _session(timeout: float = 10.0, max_retries: int = 3) -> requests.Session:
    """Return a configured requests session with retry + timeout defaults."""

    retry = Retry(
        total=max_retries,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess = requests.Session()
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.request = _with_timeout(sess.request, timeout)  # type: ignore[assignment]
    return sess


def _with_timeout(func, timeout: float):
    """Wrap a requests call so the timeout is always supplied."""

    def wrapped(method, url, **kwargs):
        kwargs.setdefault("timeout", timeout)
        return func(method, url, **kwargs)

    return wrapped


def _to_kst(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000, tz=UTC).astimezone(KST)


def fetch_klines(
    session: requests.Session,
    symbol: str,
    limit: int,
    interval: str = "1m",
    futures: bool = True,
) -> pd.DataFrame:
    """Fetch Binance klines and return a KST-indexed DataFrame."""

    base = BINANCE_FUTURES_REST if futures else BINANCE_SPOT_REST
    endpoint = f"{base}/fapi/v1/klines" if futures else f"{base}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    response = session.get(endpoint, params=params)
    response.raise_for_status()
    data = response.json()
    records = []
    for entry in data:
        open_time = _to_kst(entry[0])
        records.append(
            {
                "ts_kst": open_time,
                "open": float(entry[1]),
                "high": float(entry[2]),
                "low": float(entry[3]),
                "close": float(entry[4]),
                "volume": float(entry[5]),
            }
        )
    frame = pd.DataFrame(records).set_index("ts_kst").sort_index()
    return frame


def compute_basis(
    perp: pd.DataFrame,
    spot: pd.DataFrame,
    window: int = 60,
) -> pd.DataFrame:
    """Return basis percentage with rolling stats."""

    joined = perp[["close"]].rename(columns={"close": "perp_close"}).join(
        spot[["close"]].rename(columns={"close": "spot_close"}),
        how="inner",
    )
    joined["basis_pct"] = (joined["perp_close"] - joined["spot_close"]) / joined["spot_close"] * 100
    joined["basis_ma_1h"] = joined["basis_pct"].rolling(window=window, min_periods=1).mean()
    joined["basis_std_1h"] = joined["basis_pct"].rolling(window=window, min_periods=1).std()
    return joined.dropna().tail(window)


def compute_returns(series: pd.Series, periods: Iterable[int]) -> Dict[int, float]:
    """Compute simple returns for the latest timestamp."""

    results: Dict[int, float] = {}
    last_value = series.iloc[-1]
    for period in periods:
        if len(series) <= period:
            continue
        prev_value = series.iloc[-period - 1]
        if prev_value == 0:
            continue
        results[period] = (last_value - prev_value) / prev_value
    return results


def compute_volatility(perp: pd.DataFrame, window: int = 60 * 24) -> float:
    """Compute 24h realised volatility annualised based on 1-minute closes."""

    closes = perp["close"].tail(window + 1)
    if len(closes) < 2:
        return float("nan")
    log_returns = np.log(closes).diff().dropna()
    if log_returns.empty:
        return float("nan")
    vol = log_returns.std(ddof=0) * math.sqrt(60 * 24 * 365)
    return float(vol)


def compute_atr(perp: pd.DataFrame, window: int = 60 * 24) -> float:
    """Average True Range using 1-minute data over a 1-day window."""

    subset = perp.tail(window + 1)
    if len(subset) < 2:
        return float("nan")
    highs = subset["high"]
    lows = subset["low"]
    closes = subset["close"]
    true_ranges: List[float] = []
    prev_close = closes.iloc[0]
    for high, low, close in zip(highs.iloc[1:], lows.iloc[1:], closes.iloc[1:]):
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        true_ranges.append(tr)
        prev_close = close
    if not true_ranges:
        return float("nan")
    return float(pd.Series(true_ranges).rolling(window=window - 1, min_periods=1).mean().iloc[-1])


def fetch_funding_history(session: requests.Session, symbol: str, limit: int = 30) -> pd.DataFrame:
    endpoint = f"{BINANCE_FUTURES_REST}/fapi/v1/fundingRate"
    params = {"symbol": symbol, "limit": limit}
    response = session.get(endpoint, params=params)
    response.raise_for_status()
    data = response.json()
    records = [
        {
            "event_time_kst": _to_kst(int(item["fundingTime"])),
            "funding_rate": float(item["fundingRate"]),
        }
        for item in data
    ]
    frame = pd.DataFrame(records).set_index("event_time_kst").sort_index()
    return frame


def fetch_funding_prediction(session: requests.Session, symbol: str) -> Tuple[datetime, float]:
    endpoint = f"{BINANCE_FUTURES_REST}/fapi/v1/premiumIndex"
    response = session.get(endpoint, params={"symbol": symbol})
    response.raise_for_status()
    payload = response.json()
    event_time = _to_kst(int(payload["time"]))
    predicted = float(payload.get("lastFundingRate", 0.0))
    return event_time, predicted


def compute_funding_zscore(funding: pd.DataFrame, predicted: float, n: int = 30) -> pd.DataFrame:
    window = funding.tail(n)
    if window.empty:
        return funding.assign(
            funding_rate_winsor=np.nan,
            funding_zscore=np.nan,
            predicted_rate=np.nan,
        )
    winsor = window["funding_rate"].clip(lower=window["funding_rate"].quantile(0.05), upper=window["funding_rate"].quantile(0.95))
    mean = winsor.mean()
    std = winsor.std(ddof=0)
    zscore = (window["funding_rate"].iloc[-1] - mean) / std if std > 0 else np.nan
    enriched = window.assign(
        funding_rate_winsor=winsor,
        funding_zscore=pd.Series([np.nan] * (len(window) - 1) + [zscore], index=window.index),
        predicted_rate=pd.Series([np.nan] * (len(window) - 1) + [predicted], index=window.index),
    )
    return enriched.tail(n)


def fetch_open_interest(session: requests.Session, symbol: str, limit: int = 120) -> pd.DataFrame:
    endpoint = f"{BINANCE_FUTURES_REST}/futures/data/openInterestHist"
    params = {"symbol": symbol, "period": "5m", "limit": limit}
    response = session.get(endpoint, params=params)
    response.raise_for_status()
    data = response.json()
    records = [
        {"ts_kst": _to_kst(int(item["timestamp"])), "open_interest": float(item["sumOpenInterest"])}
        for item in data
    ]
    return pd.DataFrame(records).set_index("ts_kst").sort_index()


def compute_oi_quality(oi: pd.DataFrame, lookback: int = 12) -> pd.DataFrame:
    window = oi.tail(lookback)
    if window.empty:
        return oi.assign(oi_median_1h=np.nan, oi_std_1h=np.nan, is_outlier=False)
    median = window["open_interest"].median()
    std = window["open_interest"].std(ddof=0)
    threshold = median + 10 * std if std > 0 else float("inf")
    latest = window.iloc[-1]
    is_outlier = latest["open_interest"] > threshold
    return window.assign(
        oi_median_1h=median,
        oi_std_1h=std,
        is_outlier=[False] * (len(window) - 1) + [bool(is_outlier)],
    )


def fetch_order_book(session: requests.Session, symbol: str, depth: int = 20) -> Dict[str, List[List[str]]]:
    endpoint = f"{BINANCE_FUTURES_REST}/fapi/v1/depth"
    params = {"symbol": symbol, "limit": depth}
    response = session.get(endpoint, params=params)
    response.raise_for_status()
    return response.json()


def compute_lob_metrics(order_book: Mapping[str, Iterable[Iterable[str]]], levels: int = 5) -> Dict[str, float]:
    bids = [(float(price), float(size)) for price, size in list(order_book["bids"])[: levels]]
    asks = [(float(price), float(size)) for price, size in list(order_book["asks"])[: levels]]
    if not bids or not asks:
        return {key: float("nan") for key in ["spread_bp", "microprice", "obi", "obi_10", "bid_notional", "ask_notional", "best_bid", "best_ask"]}

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid = (best_bid + best_ask) / 2
    spread_bp = ((best_ask - best_bid) / mid) * 1e4 if mid else float("nan")

    bid_notional = sum(price * size for price, size in bids)
    ask_notional = sum(price * size for price, size in asks)
    total_bid_size = sum(size for _, size in bids)
    total_ask_size = sum(size for _, size in asks)
    if total_bid_size + total_ask_size == 0:
        microprice = float("nan")
    else:
        microprice = (best_ask * total_bid_size + best_bid * total_ask_size) / (total_bid_size + total_ask_size)

    obi_k = (total_bid_size - total_ask_size) / (total_bid_size + total_ask_size) if (total_bid_size + total_ask_size) else float("nan")

    bids_10 = [(float(price), float(size)) for price, size in list(order_book["bids"])[: min(10, len(order_book["bids"]))]]
    asks_10 = [(float(price), float(size)) for price, size in list(order_book["asks"])[: min(10, len(order_book["asks"]))]]
    total_bid_size_10 = sum(size for _, size in bids_10)
    total_ask_size_10 = sum(size for _, size in asks_10)
    obi_10 = (total_bid_size_10 - total_ask_size_10) / (total_bid_size_10 + total_ask_size_10) if (total_bid_size_10 + total_ask_size_10) else float("nan")

    return {
        "spread_bp": spread_bp,
        "microprice": microprice,
        "obi": obi_k,
        "obi_10": obi_10,
        "bid_notional": bid_notional,
        "ask_notional": ask_notional,
        "best_bid": best_bid,
        "best_ask": best_ask,
    }


def classify_regime(vol_24h: float, ret_1h: float, ret_24h: float) -> Tuple[str, Dict[str, float]]:
    """Simple regime classifier based on volatility and momentum heuristics."""

    vol_threshold_high = 1.5  # Roughly 150% annualised
    vol_threshold_low = 0.8   # Roughly 80%
    state = "CALM"
    probs = {"TREND": 0.2, "CALM": 0.6, "CRISIS": 0.2}

    if not math.isfinite(vol_24h) or not math.isfinite(ret_1h) or not math.isfinite(ret_24h):
        return state, probs

    if vol_24h > vol_threshold_high and ret_1h < 0 and ret_24h < 0:
        state = "CRISIS"
        probs = {"TREND": 0.1, "CALM": 0.1, "CRISIS": 0.8}
    elif vol_24h < vol_threshold_low and ret_24h > 0:
        state = "TREND"
        probs = {"TREND": 0.7, "CALM": 0.25, "CRISIS": 0.05}
    else:
        state = "CALM"
        probs = {"TREND": 0.3, "CALM": 0.5, "CRISIS": 0.2}

    return state, probs


def compute_tsmom_signal(ret_1h: float, ret_24h: float) -> int:
    if ret_1h > 0 and ret_24h > 0:
        return 1
    if ret_1h < 0 and ret_24h < 0:
        return -1
    return 0


def compute_position_size(funding_rate: float, vol_24h: float, k: float = 1.5) -> float:
    if not math.isfinite(funding_rate) or not math.isfinite(vol_24h) or vol_24h == 0:
        return 0.0
    raw = k * abs(funding_rate) / vol_24h
    return max(0.0, min(1.0, raw))


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_csv(data: pd.DataFrame, path: Path) -> None:
    data.to_csv(path, index=True)


def save_json(data: Mapping[str, object], path: Path) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def run(symbol: str = "SOLUSDT", out_dir: Path | None = None, klines_limit: int = 1440) -> Dict[str, object]:
    session = _session()
    out_dir = out_dir or Path("data") / "colab_exports"
    ensure_output_dir(out_dir)

    perp = fetch_klines(session, symbol, limit=klines_limit, futures=True)
    spot_symbol = symbol
    spot = fetch_klines(session, spot_symbol, limit=klines_limit, futures=False)
    basis = compute_basis(perp, spot)

    returns = compute_returns(perp["close"], periods=[60, 60 * 24])
    ret_1h = returns.get(60, float("nan"))
    ret_24h = returns.get(60 * 24, float("nan"))

    vol_24h = compute_volatility(perp)
    atr_1d = compute_atr(perp)

    funding_history = fetch_funding_history(session, symbol)
    _, predicted_rate = fetch_funding_prediction(session, symbol)
    funding = compute_funding_zscore(funding_history, predicted_rate)

    open_interest = fetch_open_interest(session, symbol)
    oi_quality = compute_oi_quality(open_interest)

    order_book = fetch_order_book(session, symbol)
    lob_metrics = compute_lob_metrics(order_book)

    tsmom_signal = compute_tsmom_signal(ret_1h, ret_24h)
    regime_state, regime_probs = classify_regime(vol_24h, ret_1h, ret_24h)
    latest_funding_rate = float(funding["funding_rate"].iloc[-1]) if not funding.empty else float("nan")
    position_size = compute_position_size(latest_funding_rate, vol_24h)

    # Prepare exports (<=10 columns each)
    price_metrics = basis.rename(
        columns={"perp_close": "perp_price", "spot_close": "spot_price"}
    )
    funding_metrics = funding.copy()
    oi_metrics = oi_quality.copy()
    lob_frame = pd.DataFrame([lob_metrics])
    lob_frame.insert(0, "ts_kst", datetime.now(tz=KST))
    lob_frame = lob_frame.set_index("ts_kst")
    vol_mom = pd.DataFrame(
        {
            "vol_24h": [vol_24h],
            "atr_1d": [atr_1d],
            "ret_1h": [ret_1h],
            "ret_24h": [ret_24h],
            "tsmom_signal": [tsmom_signal],
            "regime_state": [regime_state],
            "regime_prob_trend": [regime_probs["TREND"]],
            "regime_prob_calm": [regime_probs["CALM"]],
            "regime_prob_crisis": [regime_probs["CRISIS"]],
        },
        index=[perp.index[-1]],
    )

    save_csv(price_metrics, out_dir / "price_metrics.csv")
    save_csv(funding_metrics, out_dir / "funding_metrics.csv")
    save_csv(oi_metrics, out_dir / "oi_metrics.csv")
    save_csv(vol_mom, out_dir / "vol_momentum.csv")
    save_csv(lob_frame, out_dir / "lob_snapshot.csv")

    status = {
        "symbol": symbol,
        "perp_samples": len(perp),
        "spot_samples": len(spot),
        "last_price_ts_kst": perp.index[-1].isoformat(),
        "funding_event_kst": funding.index[-1].isoformat() if not funding.empty else None,
        "position_size_pct": position_size,
        "tsmom_signal": tsmom_signal,
        "regime_state": regime_state,
        "exports": {
            "price_metrics.csv": str((out_dir / "price_metrics.csv").resolve()),
            "funding_metrics.csv": str((out_dir / "funding_metrics.csv").resolve()),
            "oi_metrics.csv": str((out_dir / "oi_metrics.csv").resolve()),
            "vol_momentum.csv": str((out_dir / "vol_momentum.csv").resolve()),
            "lob_snapshot.csv": str((out_dir / "lob_snapshot.csv").resolve()),
        },
    }

    save_json(status, out_dir / "status.json")
    return status


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FAR-CM Colab data aggregator")
    parser.add_argument("--symbol", default="SOLUSDT", help="Binance symbol, e.g. SOLUSDT")
    parser.add_argument(
        "--output-dir",
        default="data/colab_exports",
        help="Directory to store CSV exports",
    )
    parser.add_argument(
        "--klines-limit",
        type=int,
        default=1440,
        help="Number of 1m klines to request (default: 1440 for 24h)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    status = run(symbol=args.symbol, out_dir=Path(args.output_dir), klines_limit=args.klines_limit)
    print(json.dumps(status, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

