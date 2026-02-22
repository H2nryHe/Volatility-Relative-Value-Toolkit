"""Backtest data contracts and validation helpers."""

from __future__ import annotations

from typing import Iterable, List

import pandas as pd

REQUIRED_SIGNAL_INPUT_COLUMNS: List[str] = ["date"]

REQUIRED_TRADES_COLUMNS: List[str] = [
    "date",
    "signal_date",
    "symbol",
    "trade_type",
    "target_position",
    "position_before",
    "position_after",
    "trade_qty",
    "price",
    "notional",
    "regular_cost",
    "roll_cost",
    "total_cost",
]

REQUIRED_POSITIONS_COLUMNS: List[str] = [
    "date",
    "symbol",
    "signal_date",
    "signal_value",
    "target_position",
    "position",
    "daily_return",
    "is_roll_date",
]

REQUIRED_PNL_COLUMNS: List[str] = [
    "date",
    "symbol",
    "position_prev",
    "daily_return",
    "gross_pnl",
    "costs_pnl",
    "net_pnl",
    "equity",
]

REQUIRED_ATTRIBUTION_COLUMNS: List[str] = [
    "date",
    "symbol",
    "pnl_total",
    "carry_roll_pnl",
    "spot_curve_move_pnl",
    "costs_pnl",
    "convexity_proxy_pnl",
    "residual_pnl",
]


def _missing_columns(df: pd.DataFrame, required: Iterable[str]) -> List[str]:
    return [c for c in required if c not in df.columns]


def validate_columns(df: pd.DataFrame, required: Iterable[str], frame_name: str) -> None:
    missing = _missing_columns(df, required)
    if missing:
        raise ValueError(f"{frame_name} missing required columns: {missing}")


def validate_signal_input(df: pd.DataFrame, required_signal_columns: List[str]) -> None:
    validate_columns(df, REQUIRED_SIGNAL_INPUT_COLUMNS, "signals")
    validate_columns(df, required_signal_columns, "signals")


def validate_backtest_outputs(
    trades: pd.DataFrame,
    positions: pd.DataFrame,
    pnl: pd.DataFrame,
    attribution: pd.DataFrame,
) -> None:
    validate_columns(trades, REQUIRED_TRADES_COLUMNS, "trades")
    validate_columns(positions, REQUIRED_POSITIONS_COLUMNS, "positions")
    validate_columns(pnl, REQUIRED_PNL_COLUMNS, "pnl")
    validate_columns(attribution, REQUIRED_ATTRIBUTION_COLUMNS, "attribution")
