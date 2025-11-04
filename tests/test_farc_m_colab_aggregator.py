"""Unit tests for the Colab FAR-CM aggregator helpers."""

from __future__ import annotations

import pandas as pd

from scripts.farc_m_colab_aggregator import (
    compute_basis,
    compute_funding_zscore,
    compute_lob_metrics,
    compute_position_size,
    compute_tsmom_signal,
    classify_regime,
)


def _frame_from(values):
    periods = len(next(iter(values.values())))
    index = pd.date_range("2024-01-01", periods=periods, freq="min", tz="UTC")
    return pd.DataFrame(values, index=index)


def test_compute_basis_matches_formula():
    perp = _frame_from({"close": [100, 102, 101]})
    spot = _frame_from({"close": [99, 100, 100]})
    basis = compute_basis(perp, spot, window=2)
    last = basis.iloc[-1]
    expected = (101 - 100) / 100 * 100
    assert round(last["basis_pct"], 6) == round(expected, 6)


def test_compute_funding_zscore_uses_winsorized_stats():
    history = pd.DataFrame(
        {
            "funding_rate": [0.0001] * 29 + [0.002],
        },
        index=pd.date_range("2024-01-01", periods=30, freq="8h", tz="UTC"),
    )
    enriched = compute_funding_zscore(history, predicted=0.001, n=30)
    assert enriched["funding_zscore"].iloc[-1] > 0
    assert enriched["predicted_rate"].iloc[-1] == 0.001


def test_compute_lob_metrics_returns_expected_values():
    order_book = {
        "bids": [["100", "5"], ["99", "5"]],
        "asks": [["101", "5"], ["102", "5"]],
    }
    metrics = compute_lob_metrics(order_book, levels=2)
    assert metrics["best_bid"] == 100
    assert metrics["best_ask"] == 101
    assert metrics["spread_bp"] > 0
    assert metrics["obi"] == 0


def test_classify_regime_distinguishes_states():
    crisis_state, _ = classify_regime(vol_24h=2.0, ret_1h=-0.01, ret_24h=-0.05)
    trend_state, _ = classify_regime(vol_24h=0.5, ret_1h=0.02, ret_24h=0.03)
    assert crisis_state == "CRISIS"
    assert trend_state == "TREND"


def test_position_size_clamped_between_zero_and_one():
    assert compute_position_size(0.05, 0.5, k=2.0) == 0.2
    assert compute_position_size(5, 0.1, k=2.0) == 1.0
    assert compute_position_size(float("nan"), 0.1) == 0.0


def test_tsmom_signal_sign_rules():
    assert compute_tsmom_signal(0.1, 0.2) == 1
    assert compute_tsmom_signal(-0.1, -0.2) == -1
    assert compute_tsmom_signal(0.1, -0.2) == 0

