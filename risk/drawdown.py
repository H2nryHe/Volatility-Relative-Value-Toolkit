"""Drawdown analytics for strategy equity curves."""

from __future__ import annotations

from typing import Dict

import pandas as pd


def compute_drawdown(equity: pd.Series) -> pd.DataFrame:
    """Return drawdown series with high-water mark and duration."""

    if equity.empty:
        return pd.DataFrame(columns=["equity", "running_max", "drawdown", "drawdown_duration"])

    running_max = equity.cummax()
    drawdown = (equity / running_max) - 1.0

    duration = []
    current = 0
    for value in drawdown:
        if value < 0:
            current += 1
        else:
            current = 0
        duration.append(current)

    return pd.DataFrame(
        {
            "equity": equity.values,
            "running_max": running_max.values,
            "drawdown": drawdown.values,
            "drawdown_duration": duration,
        },
        index=equity.index,
    )


def summarize_drawdown(drawdown_df: pd.DataFrame) -> Dict[str, float | int | None]:
    if drawdown_df.empty:
        return {
            "max_drawdown": 0.0,
            "max_drawdown_duration": 0,
            "recovery_periods": None,
        }

    max_dd = float(drawdown_df["drawdown"].min())
    max_dd_duration = int(drawdown_df["drawdown_duration"].max())

    trough_idx = int(drawdown_df["drawdown"].idxmin())
    recovery_periods = None
    post = drawdown_df.iloc[trough_idx:]
    recovered = post.index[post["drawdown"] >= 0]
    if len(recovered) > 0:
        recovery_periods = int(recovered[0] - trough_idx)

    return {
        "max_drawdown": max_dd,
        "max_drawdown_duration": max_dd_duration,
        "recovery_periods": recovery_periods,
    }
