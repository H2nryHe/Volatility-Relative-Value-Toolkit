"""Shared signal interfaces and anti-leakage helpers."""

from __future__ import annotations

from typing import Dict, Iterable, List, Sequence

import pandas as pd

SIGNAL_PREFIX = "signal_"
ZSCORE_PREFIX = "z_"

REQUIRED_OUTPUT_COLUMNS = ["date"]


def ensure_signal_prefix(name: str) -> str:
    return name if name.startswith(SIGNAL_PREFIX) else f"{SIGNAL_PREFIX}{name}"


def apply_signal_lag(
    df: pd.DataFrame,
    signal_columns: Sequence[str],
    lag_days: int,
    group_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Lag signal columns to prevent same-bar leakage into execution."""

    if lag_days <= 0:
        return df

    lagged = df.copy()
    if group_columns:
        for col in signal_columns:
            lagged[col] = lagged.groupby(list(group_columns), group_keys=False)[col].shift(lag_days)
    else:
        for col in signal_columns:
            lagged[col] = lagged[col].shift(lag_days)
    return lagged


def summarize_signal_columns(df: pd.DataFrame, signal_columns: List[str]) -> Dict[str, Dict[str, float | None]]:
    """Return mean/std/min/max/missing diagnostics per signal column."""

    diagnostics: Dict[str, Dict[str, float | None]] = {}
    total_rows = max(len(df), 1)

    def _safe(value: float) -> float | None:
        return None if pd.isna(value) else float(value)

    for col in signal_columns:
        if col not in df.columns:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        diagnostics[col] = {
            "mean": _safe(series.mean()) if series.notna().any() else None,
            "std": _safe(series.std(ddof=0)) if series.notna().any() else None,
            "min": _safe(series.min()) if series.notna().any() else None,
            "max": _safe(series.max()) if series.notna().any() else None,
            "missing": float(series.isna().sum()) / total_rows,
        }

    return diagnostics
