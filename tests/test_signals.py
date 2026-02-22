from __future__ import annotations

import numpy as np
import pandas as pd

from signals.carry_roll import compute_carry_roll_down
from signals.pca_factors import compute_pca_factors
from signals.term_structure import compute_curvature, compute_slope
from signals.vrp_proxy import compute_vrp_proxy


def _toy_term_structure_df() -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=6, freq="B")
    rows = []
    for i, d in enumerate(dates):
        rows.extend(
            [
                {"date": d, "symbol": "VX1", "close": 20.0 + i},
                {"date": d, "symbol": "VX2", "close": 21.0 + i},
                {"date": d, "symbol": "VX3", "close": 22.0 + i},
            ]
        )
    return pd.DataFrame(rows)


def test_slope_curvature_formulae() -> None:
    df = _toy_term_structure_df()
    cfg = {
        "price_column": "close",
        "symbol_to_tenor": {"VX1": 1, "VX2": 2, "VX3": 3},
        "slope_short_tenor": 1,
        "slope_long_tenor": 2,
        "curvature_front_tenor": 1,
        "curvature_mid_tenor": 2,
        "curvature_back_tenor": 3,
        "lag_days": 0,
    }

    slope = compute_slope(df, cfg)
    curvature = compute_curvature(df, cfg)

    day0_slope = slope.loc[slope["date"] == pd.Timestamp("2024-01-01"), "signal_term_structure_slope"].iloc[0]
    assert np.isclose(day0_slope, (20.0 - 21.0) / 21.0)

    # 2*21 - 20 - 22 == 0
    day0_curv = curvature.loc[
        curvature["date"] == pd.Timestamp("2024-01-01"), "signal_term_structure_curvature"
    ].iloc[0]
    assert np.isclose(day0_curv, 0.0)


def test_vrp_has_explicit_lag_no_leakage() -> None:
    dates = pd.date_range("2024-01-01", periods=8, freq="B")
    rows = []
    for i, d in enumerate(dates):
        rows.append({"date": d, "symbol": "SPY", "close": 100 + i})
        rows.append({"date": d, "symbol": "VIXY", "close": 20 + 0.1 * i})
    df = pd.DataFrame(rows)

    vrp = compute_vrp_proxy(
        df,
        {
            "iv_symbol": "VIXY",
            "rv_symbol": "SPY",
            "price_column": "close",
            "rv_window": 3,
            "trading_days_per_year": 252,
            "iv_scale": 100.0,
            "lag_days": 0,
        },
    )

    rv_col = "signal_rv_proxy_ann"
    first_valid_idx = vrp[rv_col].first_valid_index()
    # With rv_window=3 and explicit shift(1), first valid index occurs after at least 4 rows.
    assert first_valid_idx is not None
    assert first_valid_idx >= 4


def test_pca_output_dimensions() -> None:
    df = _toy_term_structure_df()
    factors, diagnostics = compute_pca_factors(
        df,
        {
            "price_column": "close",
            "symbol_to_tenor": {"VX1": 1, "VX2": 2, "VX3": 3},
            "n_components": 2,
            "min_obs": 3,
            "lag_days": 0,
        },
    )

    factor_cols = [c for c in factors.columns if c.startswith("signal_pca_factor_")]
    assert len(factor_cols) == 2
    assert len(diagnostics["explained_variance_ratio"]) <= 2

    loadings = diagnostics["loadings"]
    assert "signal_pca_factor_1" in loadings
    assert len(loadings["signal_pca_factor_1"]) == 3


def test_signal_interfaces_share_date_key() -> None:
    df = _toy_term_structure_df()
    cfg = {
        "price_column": "close",
        "symbol_to_tenor": {"VX1": 1, "VX2": 2, "VX3": 3},
        "lag_days": 0,
    }
    slope = compute_slope(df, cfg)
    carry = compute_carry_roll_down(df, cfg)

    assert "date" in slope.columns
    assert "date" in carry.columns
    assert all(c.startswith("signal_") for c in slope.columns if c != "date")
    assert all(c.startswith("signal_") for c in carry.columns if c != "date")
