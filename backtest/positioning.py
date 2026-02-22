"""Signal-to-position mapping with caps and risk targeting."""

from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd


def signal_to_target_position(signal_value: float, config: Dict[str, object]) -> float:
    signal_scale = float(config.get("signal_scale", 1.0))
    if signal_scale <= 0:
        raise ValueError("signal_scale must be > 0")
    raw = float(signal_value) / signal_scale
    return float(np.tanh(raw))


def apply_position_constraints(
    target_series: pd.Series,
    returns_series: pd.Series,
    risk_config: Dict[str, object],
) -> pd.Series:
    cap_abs = float(risk_config.get("position_cap_abs", 1.0))
    leverage_cap = float(risk_config.get("leverage_cap", cap_abs))
    enable_risk_target = bool(risk_config.get("enable_risk_target", True))
    target_vol = float(risk_config.get("target_volatility", 0.10))
    vol_window = int(risk_config.get("vol_window", 20))

    constrained = target_series.astype(float).clip(-cap_abs, cap_abs)

    if enable_risk_target:
        realized_vol = returns_series.rolling(window=vol_window, min_periods=max(vol_window, 2)).std(ddof=0)
        realized_vol = realized_vol.shift(1)
        scale = (target_vol / realized_vol.replace(0.0, np.nan)).clip(upper=leverage_cap)
        scale = scale.fillna(1.0)
        constrained = (constrained * scale).clip(-cap_abs, cap_abs)

    return constrained.clip(-leverage_cap, leverage_cap)
