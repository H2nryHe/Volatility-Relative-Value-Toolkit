"""Historical VaR/CVaR computation with explicit loss convention."""

from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd


def compute_historical_var_cvar(
    returns: pd.Series,
    confidence: float,
    horizon_days: int = 1,
) -> Dict[str, float]:
    """Compute historical VaR/CVaR under loss-positive convention.

    Convention:
    - returns are arithmetic strategy returns (positive = gain, negative = loss)
    - losses = -returns
    - VaR/CVaR reported as positive loss magnitudes.
    """

    if not 0 < confidence < 1:
        raise ValueError("confidence must be between 0 and 1")
    if horizon_days < 1:
        raise ValueError("horizon_days must be >= 1")

    clean = returns.dropna().astype(float)
    if clean.empty:
        return {"var": 0.0, "cvar": 0.0}

    if horizon_days > 1:
        agg = clean.rolling(window=horizon_days, min_periods=horizon_days).sum().dropna()
    else:
        agg = clean

    if agg.empty:
        return {"var": 0.0, "cvar": 0.0}

    losses = -agg
    var_level = float(np.quantile(losses, confidence))
    tail = losses[losses >= var_level]
    cvar_level = float(tail.mean()) if not tail.empty else var_level

    return {"var": var_level, "cvar": cvar_level}
