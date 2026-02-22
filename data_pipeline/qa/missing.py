"""Missing-data handling and fill auditing for Stage 3."""

from __future__ import annotations

from typing import Dict, List, Tuple

import pandas as pd


def apply_fill_rules(df: pd.DataFrame, fill_config: Dict[str, object]) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    """Apply configurable fills and return filled frame + row-level audit report."""

    filled = df.copy()
    fields: List[str] = list(fill_config.get("fields", ["open", "high", "low", "close"]))
    method = str(fill_config.get("method", "ffill"))
    limit = fill_config.get("limit")
    fill_volume = bool(fill_config.get("fill_volume", False))

    if fill_volume and "volume" not in fields:
        fields = [*fields, "volume"]

    missing_rows = []
    fill_counts: Dict[str, int] = {}

    for field in fields:
        if field not in filled.columns:
            continue

        before_na = filled[field].isna()

        if method == "ffill":
            filled[field] = filled.groupby("symbol", group_keys=False)[field].ffill(limit=limit)
        elif method == "bfill":
            filled[field] = filled.groupby("symbol", group_keys=False)[field].bfill(limit=limit)
        else:
            raise ValueError(f"Unsupported fill method: {method}")

        after_na = filled[field].isna()
        marker_col = f"is_filled_{field}"
        filled[marker_col] = before_na & ~after_na

        fill_counts[field] = int(filled[marker_col].sum())

        field_rows = filled.loc[:, ["date", "symbol", marker_col]].copy()
        field_rows["field"] = field
        field_rows["missing_before_fill"] = before_na.astype(bool).values
        field_rows["missing_after_fill"] = after_na.astype(bool).values
        field_rows["filled"] = field_rows[marker_col].astype(bool)
        field_rows.drop(columns=[marker_col], inplace=True)
        missing_rows.append(field_rows)

    if not fill_volume and "volume" in filled.columns:
        filled["is_filled_volume"] = False
        fill_counts.setdefault("volume", 0)

    missing_report = pd.concat(missing_rows, axis=0, ignore_index=True) if missing_rows else pd.DataFrame(
        columns=["date", "symbol", "field", "missing_before_fill", "missing_after_fill", "filled"]
    )

    for col in ["is_data_missing", "is_market_closed"]:
        if col in filled.columns:
            filled[col] = filled[col].astype(bool)

    return filled, missing_report, fill_counts
