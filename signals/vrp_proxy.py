"""Variance risk premium proxy: IV - RV."""

from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd

from signals.base import apply_signal_lag, ensure_signal_prefix


def compute_vrp_proxy(df: pd.DataFrame, config: Dict[str, object]) -> pd.DataFrame:
    """Compute VRP proxy with explicit anti-leakage shift.

    IV proxy:
      uses configured implied-vol symbol's close series scaled by `iv_scale`.
      Example: VIX level percent uses iv_scale=100.

    RV proxy:
      rolling std of underlying returns over `rv_window`, annualized by sqrt(trading_days_per_year),
      then shifted by 1 day to avoid using same-day return in signal timestamp.
    """

    iv_symbol = str(config.get("iv_symbol", "VIXY"))
    rv_symbol = str(config.get("rv_symbol", "SPY"))
    price_col = str(config.get("price_column", "close"))
    rv_window = int(config.get("rv_window", 20))
    trading_days_per_year = int(config.get("trading_days_per_year", 252))
    iv_scale = float(config.get("iv_scale", 100.0))

    iv = (
        df.loc[df["symbol"] == iv_symbol, ["date", price_col]]
        .rename(columns={price_col: "iv_raw"})
        .sort_values("date")
    )
    rv_price = (
        df.loc[df["symbol"] == rv_symbol, ["date", price_col]]
        .rename(columns={price_col: "rv_price"})
        .sort_values("date")
    )

    base = pd.DataFrame({"date": sorted(df["date"].unique())})
    base = base.merge(iv, on="date", how="left").merge(rv_price, on="date", how="left")

    base["iv_ann"] = base["iv_raw"] / iv_scale
    returns = base["rv_price"].pct_change()
    base["rv_ann"] = returns.rolling(window=rv_window, min_periods=rv_window).std(ddof=0) * np.sqrt(trading_days_per_year)
    base["rv_ann"] = base["rv_ann"].shift(1)

    vrp_col = ensure_signal_prefix("vrp_proxy")
    iv_col = ensure_signal_prefix("iv_proxy_ann")
    rv_col = ensure_signal_prefix("rv_proxy_ann")

    base[iv_col] = base["iv_ann"]
    base[rv_col] = base["rv_ann"]
    base[vrp_col] = base[iv_col] - base[rv_col]

    lag_days = int(config.get("lag_days", 0))
    base = apply_signal_lag(base, [iv_col, rv_col, vrp_col], lag_days=lag_days)
    return base[["date", iv_col, rv_col, vrp_col]]
