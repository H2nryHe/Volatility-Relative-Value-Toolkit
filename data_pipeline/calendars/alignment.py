"""Trading calendar alignment utilities for Stage 3."""

from __future__ import annotations

from typing import Dict, Tuple

import pandas as pd


def build_target_calendar(df: pd.DataFrame, calendar_config: Dict[str, object]) -> pd.DatetimeIndex:
    """Build target calendar index using config and observed data bounds."""

    if "date" not in df.columns:
        raise ValueError("Alignment requires 'date' column.")

    frequency = str(calendar_config.get("frequency", "B"))
    start_cfg = calendar_config.get("start")
    end_cfg = calendar_config.get("end")

    start = pd.Timestamp(start_cfg) if start_cfg else pd.Timestamp(df["date"].min())
    end = pd.Timestamp(end_cfg) if end_cfg else pd.Timestamp(df["date"].max())

    if pd.isna(start) or pd.isna(end):
        raise ValueError("Cannot build calendar with missing start/end dates.")

    if start > end:
        raise ValueError(f"Calendar start {start} is after end {end}.")

    return pd.date_range(start=start, end=end, freq=frequency)


def align_to_calendar(
    df: pd.DataFrame,
    calendar_config: Dict[str, object],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Align each symbol to target calendar and mark data-missing placeholders."""

    if df.empty:
        aligned = df.copy()
        aligned["is_data_missing"] = pd.Series(dtype="bool")
        aligned["is_market_closed"] = pd.Series(dtype="bool")
        return aligned, pd.DataFrame(columns=["date", "symbol", "is_data_missing", "is_market_closed"])

    if "symbol" not in df.columns:
        raise ValueError("Alignment requires 'symbol' column.")

    calendar = build_target_calendar(df, calendar_config)
    aligned_frames = []

    for symbol, sdf in df.groupby("symbol", sort=True):
        sdf = sdf.sort_values("date").copy()
        # Stage 3 alignment requires one row per symbol/date for reindexing.
        # Keep the last observed record on duplicate dates; duplicates are summarized in QA report.
        sdf = sdf.drop_duplicates(subset=["date"], keep="last")
        sdf = sdf.set_index("date")
        reindexed = sdf.reindex(calendar)
        reindexed.index.name = "date"
        reindexed = reindexed.reset_index()
        reindexed["symbol"] = symbol

        # Stage 3 MVP uses business-day index; market-closed is placeholder False.
        reindexed["is_data_missing"] = reindexed["close"].isna()
        reindexed["is_market_closed"] = False

        aligned_frames.append(reindexed)

    aligned_df = pd.concat(aligned_frames, axis=0, ignore_index=True)
    aligned_df.sort_values(["symbol", "date"], inplace=True)
    aligned_df.reset_index(drop=True, inplace=True)

    missing_flags = aligned_df.loc[:, ["date", "symbol", "is_data_missing", "is_market_closed"]].copy()
    return aligned_df, missing_flags
