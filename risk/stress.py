"""Stress window analytics for strategy returns and pnl."""

from __future__ import annotations

from typing import Dict, List

import numpy as np
import pandas as pd


def compute_stress_report(
    pnl_df: pd.DataFrame,
    windows: List[Dict[str, str]],
) -> pd.DataFrame:
    """Compute stress stats for configured date windows."""

    pnl = pnl_df.sort_values("date").copy()
    pnl["date"] = pd.to_datetime(pnl["date"])
    pnl["strategy_return"] = pnl["net_pnl"] / pnl["equity"].shift(1).replace(0.0, pd.NA)
    pnl["strategy_return"] = pnl["strategy_return"].fillna(0.0)

    rows = []
    for window in windows:
        name = str(window.get("name", "unnamed"))
        start = pd.to_datetime(window.get("start"))
        end = pd.to_datetime(window.get("end"))

        slice_df = pnl[(pnl["date"] >= start) & (pnl["date"] <= end)].copy()
        if slice_df.empty:
            rows.append(
                {
                    "window": name,
                    "start": start,
                    "end": end,
                    "observations": 0,
                    "total_pnl": 0.0,
                    "mean_return": 0.0,
                    "volatility": 0.0,
                    "sharpe": None,
                }
            )
            continue

        mean_ret = float(slice_df["strategy_return"].mean())
        vol = float(slice_df["strategy_return"].std(ddof=0))
        sharpe = float(np.sqrt(252.0) * mean_ret / vol) if vol > 0 else None

        rows.append(
            {
                "window": name,
                "start": start,
                "end": end,
                "observations": int(len(slice_df)),
                "total_pnl": float(slice_df["net_pnl"].sum()),
                "mean_return": mean_ret,
                "volatility": vol,
                "sharpe": sharpe,
            }
        )

    return pd.DataFrame(rows)
