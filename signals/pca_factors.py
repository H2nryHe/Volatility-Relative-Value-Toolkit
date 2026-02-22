"""Term-structure PCA factors with no-lookahead fitting."""

from __future__ import annotations

from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA

from signals.base import apply_signal_lag, ensure_signal_prefix
from signals.term_structure import build_term_structure_matrix


def compute_pca_factors(df: pd.DataFrame, config: Dict[str, object]) -> Tuple[pd.DataFrame, Dict[str, object]]:
    """Compute PCA factor scores using expanding historical fit up to t-1."""

    matrix = build_term_structure_matrix(df, config)
    n_components = int(config.get("n_components", 2))
    min_obs = int(config.get("min_obs", max(n_components + 2, 5)))

    dates = sorted(df["date"].unique())
    out = pd.DataFrame({"date": dates})
    factor_cols = [ensure_signal_prefix(f"pca_factor_{i+1}") for i in range(n_components)]
    for col in factor_cols:
        out[col] = np.nan

    explained_variance: List[float] | None = None
    loadings: Dict[str, Dict[str, float]] | None = None

    if matrix.empty or matrix.shape[1] < n_components:
        diagnostics = {
            "explained_variance_ratio": [],
            "loadings": {},
            "components": n_components,
            "tenor_columns": [str(c) for c in matrix.columns.tolist()],
            "min_obs": min_obs,
            "fitted_rows": 0,
        }
        return out, diagnostics

    matrix = matrix.sort_index()
    tenor_columns = [str(c) for c in matrix.columns.tolist()]

    fitted_rows = 0
    for i, current_date in enumerate(matrix.index):
        history = matrix.iloc[:i].dropna()
        current_row = matrix.loc[[current_date]].dropna()
        if len(history) < min_obs or current_row.empty:
            continue

        pca = PCA(n_components=n_components)
        pca.fit(history.values)
        factors = pca.transform(current_row.values)[0]
        out_idx = out.index[out["date"] == current_date]
        if len(out_idx) == 0:
            continue
        out.loc[out_idx, factor_cols] = factors

        fitted_rows += 1
        explained_variance = pca.explained_variance_ratio_.tolist()
        loadings = {
            factor_cols[j]: {tenor_columns[k]: float(pca.components_[j, k]) for k in range(len(tenor_columns))}
            for j in range(n_components)
        }

    lag_days = int(config.get("lag_days", 0))
    out = apply_signal_lag(out, factor_cols, lag_days=lag_days)

    diagnostics = {
        "explained_variance_ratio": explained_variance or [],
        "loadings": loadings or {},
        "components": n_components,
        "tenor_columns": tenor_columns,
        "min_obs": min_obs,
        "fitted_rows": fitted_rows,
    }
    return out, diagnostics
