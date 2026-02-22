"""Outlier detection for Stage 3 (mark-only MVP)."""

from __future__ import annotations

from typing import Dict, List, Tuple

import pandas as pd


def detect_outliers_zscore(df: pd.DataFrame, outlier_config: Dict[str, object]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Detect outliers with per-symbol z-score and return marks + report."""

    marked = df.copy()
    fields: List[str] = list(outlier_config.get("fields", ["close"]))
    threshold = float(outlier_config.get("zscore_threshold", 3.0))
    min_obs = int(outlier_config.get("min_obs", 3))

    report_frames = []

    for field in fields:
        if field not in marked.columns:
            continue

        z_col = f"zscore_{field}"
        out_col = f"is_outlier_{field}"

        marked[z_col] = 0.0
        marked[out_col] = False

        for symbol, sdf in marked.groupby("symbol"):
            idx = sdf.index
            series = sdf[field].astype(float)
            valid = series.dropna()
            if len(valid) < min_obs:
                continue
            mean = valid.mean()
            std = valid.std(ddof=0)
            if std == 0 or pd.isna(std):
                continue
            zscores = (series - mean) / std
            marked.loc[idx, z_col] = zscores.fillna(0.0)
            marked.loc[idx, out_col] = zscores.abs() >= threshold

        outliers = marked.loc[marked[out_col], ["date", "symbol", field, z_col, out_col]].copy()
        outliers.rename(columns={field: "value", z_col: "zscore", out_col: "is_outlier"}, inplace=True)
        outliers["field"] = field
        report_frames.append(outliers)

    if report_frames:
        outlier_report = pd.concat(report_frames, axis=0, ignore_index=True)
        outlier_report = outlier_report[["date", "symbol", "field", "value", "zscore", "is_outlier"]]
    else:
        outlier_report = pd.DataFrame(columns=["date", "symbol", "field", "value", "zscore", "is_outlier"])

    return marked, outlier_report
