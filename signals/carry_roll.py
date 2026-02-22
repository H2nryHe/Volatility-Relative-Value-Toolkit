"""Carry and roll-down proxy signals."""

from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd

from signals.base import apply_signal_lag, ensure_signal_prefix
from signals.term_structure import build_term_structure_matrix


def compute_carry_roll_down(df: pd.DataFrame, config: Dict[str, object]) -> pd.DataFrame:
    """Carry proxy from front/back structure; roll-down as normalized back-front spread.

    Units:
    - carry: annualized fraction (using trading_days_per_year)
    - roll-down: fraction of front price
    """

    matrix = build_term_structure_matrix(df, config)
    front_tenor = int(config.get("front_tenor", 1))
    next_tenor = int(config.get("next_tenor", 2))
    tenor_gap_months = float(config.get("tenor_gap_months", 1.0))
    trading_days_per_year = int(config.get("trading_days_per_year", 252))

    out = pd.DataFrame({"date": sorted(df["date"].unique())})
    cols = [c for c in matrix.columns if c in {front_tenor, next_tenor}]
    out = out.merge(matrix[cols], left_on="date", right_index=True, how="left")

    carry_col = ensure_signal_prefix("carry_roll_down")
    roll_col = ensure_signal_prefix("roll_down_proxy")

    if front_tenor in out.columns and next_tenor in out.columns:
        raw_carry = (out[next_tenor] - out[front_tenor]) / out[front_tenor]
        annualizer = trading_days_per_year / max(tenor_gap_months * 21.0, 1.0)
        out[carry_col] = raw_carry * annualizer
        out[roll_col] = (out[next_tenor] - out[front_tenor]) / out[front_tenor]
    else:
        out[carry_col] = np.nan
        out[roll_col] = np.nan

    lag_days = int(config.get("lag_days", 0))
    out = apply_signal_lag(out, [carry_col, roll_col], lag_days=lag_days)
    return out[["date", carry_col, roll_col]]
