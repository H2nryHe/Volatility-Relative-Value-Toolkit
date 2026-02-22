from __future__ import annotations

import pandas as pd

from risk.drawdown import compute_drawdown, summarize_drawdown
from risk.stress import compute_stress_report
from risk.var_cvar import compute_historical_var_cvar


def test_drawdown_matches_equity_curve() -> None:
    equity = pd.Series([100.0, 110.0, 105.0, 90.0, 95.0, 115.0])
    dd = compute_drawdown(equity)
    summary = summarize_drawdown(dd)

    assert round(summary["max_drawdown"], 6) == round((90.0 / 110.0) - 1.0, 6)
    assert summary["max_drawdown_duration"] >= 1


def test_var_cvar_ordering_loss_positive_convention() -> None:
    returns = pd.Series([0.01, -0.02, 0.005, -0.03, 0.002, -0.01])
    result = compute_historical_var_cvar(returns, confidence=0.95, horizon_days=1)

    assert result["var"] >= 0
    assert result["cvar"] >= result["var"]


def test_stress_windows_slice_and_preserve_labels() -> None:
    dates = pd.date_range("2024-01-01", periods=5, freq="B")
    pnl = pd.DataFrame(
        {
            "date": dates,
            "net_pnl": [10.0, -5.0, 7.0, -2.0, 3.0],
            "equity": [1000.0, 995.0, 1002.0, 1000.0, 1003.0],
            "daily_return": [0.0, -0.005, 0.007, -0.002, 0.003],
        }
    )

    windows = [
        {"name": "w1", "start": "2024-01-01", "end": "2024-01-03"},
        {"name": "w2", "start": "2024-02-01", "end": "2024-02-05"},
    ]

    stress = compute_stress_report(pnl, windows)

    assert set(stress["window"]) == {"w1", "w2"}
    assert int(stress.loc[stress["window"] == "w1", "observations"].iloc[0]) > 0
    assert int(stress.loc[stress["window"] == "w2", "observations"].iloc[0]) == 0
