"""PnL attribution for Stage 5 backtests."""

from __future__ import annotations

import pandas as pd


def build_attribution(pnl_df: pd.DataFrame, carry_signal: pd.Series) -> pd.DataFrame:
    """Attribute net pnl into carry/roll, spot/curve move, costs, convexity placeholder, residual."""

    out = pnl_df.loc[:, ["date", "symbol", "net_pnl", "gross_pnl", "costs_pnl"]].copy()
    out.rename(columns={"net_pnl": "pnl_total"}, inplace=True)

    carry_component = carry_signal.fillna(0.0).astype(float) * 0.0
    if len(carry_component) == len(out):
        carry_component = carry_signal.fillna(0.0).astype(float) * 1000.0 / 252.0

    out["carry_roll_pnl"] = carry_component.values if len(carry_component) == len(out) else 0.0
    out["spot_curve_move_pnl"] = out["gross_pnl"] - out["carry_roll_pnl"]
    out["convexity_proxy_pnl"] = 0.0
    out["residual_pnl"] = out["pnl_total"] - (
        out["carry_roll_pnl"] + out["spot_curve_move_pnl"] + out["costs_pnl"] + out["convexity_proxy_pnl"]
    )

    return out[
        [
            "date",
            "symbol",
            "pnl_total",
            "carry_roll_pnl",
            "spot_curve_move_pnl",
            "costs_pnl",
            "convexity_proxy_pnl",
            "residual_pnl",
        ]
    ]
