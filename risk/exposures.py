"""Exposure proxy calculations from backtest pnl/positions."""

from __future__ import annotations

from typing import Dict

import pandas as pd


def _rolling_beta(y: pd.Series, x: pd.Series, window: int) -> pd.Series:
    cov = y.rolling(window=window, min_periods=window).cov(x)
    var = x.rolling(window=window, min_periods=window).var(ddof=0)
    return cov / var.replace(0.0, pd.NA)


def compute_exposures(
    pnl_df: pd.DataFrame,
    positions_df: pd.DataFrame,
    config: Dict[str, object],
) -> pd.DataFrame:
    """Compute beta/vega/gamma proxy exposures by date.

    Units:
    - beta_proxy: rolling sensitivity of strategy return to benchmark return.
    - vega_proxy: rolling sensitivity to abs(benchmark return) as vol-level proxy.
    - gamma_proxy: rolling sensitivity to squared benchmark return.
    """

    window = int(config.get("window", 20))
    bench_col = str(config.get("benchmark_return_column", "daily_return"))

    pnl = pnl_df.sort_values("date").copy()
    pos = positions_df.sort_values("date").copy()

    strategy_ret = pnl["net_pnl"] / pnl["equity"].shift(1).replace(0.0, pd.NA)
    strategy_ret = strategy_ret.fillna(0.0)
    benchmark_ret = pnl[bench_col].astype(float)

    out = pd.DataFrame({"date": pnl["date"]})
    out["strategy_return"] = strategy_ret.values
    out["benchmark_return"] = benchmark_ret.values
    out["position_abs"] = pos["position"].abs().values if "position" in pos.columns else 0.0

    out["beta_proxy"] = _rolling_beta(out["strategy_return"], out["benchmark_return"], window)
    out["vega_proxy"] = _rolling_beta(out["strategy_return"], out["benchmark_return"].abs(), window)
    out["gamma_proxy"] = _rolling_beta(out["strategy_return"], out["benchmark_return"] ** 2, window)

    return out
