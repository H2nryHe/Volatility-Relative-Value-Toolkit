"""Execution timing model with explicit anti-lookahead mapping."""

from __future__ import annotations

from typing import Dict

import pandas as pd


def map_signal_to_execution(
    signals_df: pd.DataFrame,
    market_df: pd.DataFrame,
    signal_column: str,
    lag_days: int,
) -> pd.DataFrame:
    """Map each signal timestamp to execution timestamp using trading dates.

    Anti-lookahead rule:
    - Execution uses date index + lag_days on market trading calendar.
    - For lag_days > 0, execution_date must be strictly greater than signal_date.
    """

    if lag_days < 0:
        raise ValueError("signal_execution_lag_days must be >= 0")

    work_sig = signals_df.loc[:, ["date", signal_column]].copy()
    work_sig.rename(columns={"date": "signal_date", signal_column: "signal_value"}, inplace=True)
    work_sig["signal_date"] = pd.to_datetime(work_sig["signal_date"])

    trading_dates = pd.Series(sorted(pd.to_datetime(market_df["date"]).unique()))
    date_to_idx: Dict[pd.Timestamp, int] = {pd.Timestamp(d): i for i, d in enumerate(trading_dates.tolist())}

    execution_rows = []
    for row in work_sig.itertuples(index=False):
        signal_date = pd.Timestamp(row.signal_date)
        signal_value = row.signal_value
        if pd.isna(signal_value):
            continue
        if signal_date not in date_to_idx:
            continue
        exec_idx = date_to_idx[signal_date] + lag_days
        if exec_idx >= len(trading_dates):
            continue

        execution_date = pd.Timestamp(trading_dates.iloc[exec_idx])
        if lag_days > 0 and execution_date <= signal_date:
            raise ValueError(
                f"Lookahead guard failed: execution_date={execution_date} signal_date={signal_date} lag={lag_days}"
            )

        execution_rows.append(
            {
                "signal_date": signal_date,
                "execution_date": execution_date,
                "signal_value": float(signal_value),
            }
        )

    return pd.DataFrame(execution_rows)
