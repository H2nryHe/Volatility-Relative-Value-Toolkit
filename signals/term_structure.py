"""Term structure signals: slope and curvature."""

from __future__ import annotations

from typing import Dict, List

import numpy as np
import pandas as pd

from signals.base import apply_signal_lag, ensure_signal_prefix


def build_term_structure_matrix(df: pd.DataFrame, config: Dict[str, object]) -> pd.DataFrame:
    """Create date x tenor price matrix from long input data."""

    price_col = str(config.get("price_column", "close"))
    tenor_col = config.get("tenor_column")
    symbol_to_tenor = config.get("symbol_to_tenor", {})

    if tenor_col and tenor_col in df.columns:
        work = df.loc[:, ["date", tenor_col, price_col]].copy()
        work.rename(columns={tenor_col: "tenor", price_col: "value"}, inplace=True)
    else:
        if not symbol_to_tenor:
            return pd.DataFrame(index=pd.Index(sorted(df["date"].unique()), name="date"))
        work = df.loc[:, ["date", "symbol", price_col]].copy()
        work["tenor"] = work["symbol"].map(symbol_to_tenor)
        work = work.dropna(subset=["tenor"])
        work.rename(columns={price_col: "value"}, inplace=True)

    if work.empty:
        return pd.DataFrame(index=pd.Index(sorted(df["date"].unique()), name="date"))

    work["tenor"] = pd.to_numeric(work["tenor"], errors="coerce")
    work = work.dropna(subset=["tenor"])

    matrix = work.pivot_table(index="date", columns="tenor", values="value", aggfunc="last").sort_index()
    matrix.columns = [int(c) if float(c).is_integer() else float(c) for c in matrix.columns]
    return matrix


def _maybe_zscore(series: pd.Series, window: int | None) -> pd.Series:
    if not window or window < 2:
        return series
    mean = series.rolling(window=window, min_periods=window).mean()
    std = series.rolling(window=window, min_periods=window).std(ddof=0)
    z = (series - mean) / std.replace(0, np.nan)
    return z


def compute_slope(df: pd.DataFrame, config: Dict[str, object]) -> pd.DataFrame:
    """Slope = (front - back) / back across configured tenors."""

    matrix = build_term_structure_matrix(df, config)
    short_tenor = int(config.get("slope_short_tenor", 1))
    long_tenor = int(config.get("slope_long_tenor", 2))

    out = pd.DataFrame({"date": sorted(df["date"].unique())})
    out = out.merge(matrix[[c for c in matrix.columns if c in {short_tenor, long_tenor}]], left_on="date", right_index=True, how="left")

    name = ensure_signal_prefix("term_structure_slope")
    if short_tenor in out.columns and long_tenor in out.columns:
        out[name] = (out[short_tenor] - out[long_tenor]) / out[long_tenor]
    else:
        out[name] = np.nan

    z_window = config.get("zscore_window")
    z_name = ensure_signal_prefix("term_structure_slope_z")
    out[z_name] = _maybe_zscore(out[name], int(z_window)) if z_window else out[name]

    lag_days = int(config.get("lag_days", 0))
    out = apply_signal_lag(out, [name, z_name], lag_days=lag_days)
    return out[["date", name, z_name]]


def compute_curvature(df: pd.DataFrame, config: Dict[str, object]) -> pd.DataFrame:
    """Curvature uses 3-point shape: 2*mid - front - back."""

    matrix = build_term_structure_matrix(df, config)
    front = int(config.get("curvature_front_tenor", 1))
    mid = int(config.get("curvature_mid_tenor", 2))
    back = int(config.get("curvature_back_tenor", 3))

    out = pd.DataFrame({"date": sorted(df["date"].unique())})
    cols = [c for c in matrix.columns if c in {front, mid, back}]
    out = out.merge(matrix[cols], left_on="date", right_index=True, how="left")

    name = ensure_signal_prefix("term_structure_curvature")
    if all(c in out.columns for c in [front, mid, back]):
        out[name] = 2.0 * out[mid] - out[front] - out[back]
    else:
        out[name] = np.nan

    z_window = config.get("zscore_window")
    z_name = ensure_signal_prefix("term_structure_curvature_z")
    out[z_name] = _maybe_zscore(out[name], int(z_window)) if z_window else out[name]

    lag_days = int(config.get("lag_days", 0))
    out = apply_signal_lag(out, [name, z_name], lag_days=lag_days)
    return out[["date", name, z_name]]
