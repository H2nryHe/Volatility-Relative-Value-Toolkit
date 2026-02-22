from __future__ import annotations

import pandas as pd
import pytest

from backtest.engine import run_backtest


def _toy_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dates = pd.date_range("2024-01-01", periods=8, freq="B")

    signals = pd.DataFrame(
        {
            "date": dates,
            "signal_term_structure_slope": [0.0, 2.0, 2.0, -2.0, -2.0, 1.0, 1.0, 0.0],
            "signal_carry_roll_down": [0.0, 0.1, 0.1, -0.1, -0.1, 0.05, 0.05, 0.0],
        }
    )

    market = pd.DataFrame(
        {
            "date": dates,
            "price": [100, 101, 100, 102, 101, 103, 104, 103],
        }
    )
    market["daily_return"] = market["price"].pct_change().fillna(0.0)

    roll_log = pd.DataFrame(
        {
            "date": [pd.Timestamp("2024-01-05")],
            "from_contract": ["VX_F1"],
            "to_contract": ["VX_F2"],
            "reason": ["roll_2bd_before_expiry"],
            "root_symbol": ["VX"],
            "source_file": ["fixture"],
        }
    )

    return signals, market, roll_log


def _base_config() -> dict:
    return {
        "backtest": {
            "initial_capital": 1_000_000,
            "signal_column": "signal_term_structure_slope",
            "carry_signal_column": "signal_carry_roll_down",
            "signal_execution_lag_days": 1,
            "enforce_next_bar_execution": True,
            "signal_scale": 2.0,
            "primary_symbol": "SPY",
        },
        "costs": {"commission_bps": 1.0, "slippage_bps": 1.0, "roll_cost_bps": 3.0},
        "risk_controls": {
            "position_cap_abs": 0.75,
            "leverage_cap": 0.75,
            "enable_risk_target": True,
            "target_volatility": 0.15,
            "vol_window": 3,
        },
    }


def test_accounting_and_constraints_and_attribution_identity() -> None:
    signals, market, roll_log = _toy_inputs()
    cfg = _base_config()

    trades, positions, pnl, attribution, summary = run_backtest(signals, market, roll_log, cfg)

    # Position updates match trade accumulation.
    merged = positions.merge(
        trades.loc[trades["trade_type"] == "rebalance"].groupby("date")["trade_qty"].sum().reset_index(),
        on="date",
        how="left",
    )
    merged["trade_qty"] = merged["trade_qty"].fillna(0.0)
    reconstructed = merged["trade_qty"].cumsum()
    assert (reconstructed.round(10) == merged["position"].round(10)).all()

    # Position cap is respected.
    assert (positions["position"].abs() <= cfg["risk_controls"]["position_cap_abs"] + 1e-12).all()

    # Attribution identity.
    comp_sum = (
        attribution["carry_roll_pnl"]
        + attribution["spot_curve_move_pnl"]
        + attribution["costs_pnl"]
        + attribution["convexity_proxy_pnl"]
        + attribution["residual_pnl"]
    )
    assert (attribution["pnl_total"] - comp_sum).abs().max() < 1e-8
    assert summary["metrics"]["attribution_max_abs_error"] < 1e-8


def test_cost_application_changes_net_pnl() -> None:
    signals, market, roll_log = _toy_inputs()

    cfg_cost = _base_config()
    _, _, pnl_cost, _, _ = run_backtest(signals, market, roll_log, cfg_cost)

    cfg_zero = _base_config()
    cfg_zero["costs"] = {"commission_bps": 0.0, "slippage_bps": 0.0, "roll_cost_bps": 0.0}
    _, _, pnl_zero, _, _ = run_backtest(signals, market, roll_log, cfg_zero)

    assert pnl_zero["net_pnl"].sum() != pnl_cost["net_pnl"].sum()


def test_no_lookahead_guard_and_roll_trade_dates() -> None:
    signals, market, roll_log = _toy_inputs()
    cfg = _base_config()

    # Guard should fail for same-bar execution when enforcement is on.
    bad_cfg = _base_config()
    bad_cfg["backtest"]["signal_execution_lag_days"] = 0
    with pytest.raises(ValueError, match="No-lookahead guard"):
        run_backtest(signals, market, roll_log, bad_cfg)

    trades, _, _, _, _ = run_backtest(signals, market, roll_log, cfg)
    roll_dates_in_trades = set(pd.to_datetime(trades.loc[trades["trade_type"] == "roll", "date"]))
    roll_dates_expected = set(pd.to_datetime(roll_log["date"]))
    assert roll_dates_expected.issubset(roll_dates_in_trades)
