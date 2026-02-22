"""Standardization transforms from source-native columns to canonical schema."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

import pandas as pd

from data_pipeline.schema import REQUIRED_COLUMNS


def _normalize_date_column(series: pd.Series, target_timezone: str) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    converted = parsed.dt.tz_convert(target_timezone)
    return converted.dt.floor("D").dt.tz_localize(None)


def standardize_source_dataframe(
    raw_df: pd.DataFrame,
    source_config: Dict[str, Any],
    pipeline_timezone: str,
    asof_timestamp: datetime | None = None,
) -> pd.DataFrame:
    """Transform source-native DataFrame into canonical standardized schema."""

    if raw_df.empty:
        raise ValueError(f"Source '{source_config.get('name', 'unknown')}' produced empty raw DataFrame.")

    date_column = source_config.get("date_column", "date")
    mapping = source_config.get("column_mapping", {})

    standardized = pd.DataFrame(index=raw_df.index)

    if date_column not in raw_df.columns:
        raise ValueError(
            f"Source '{source_config.get('name', 'unknown')}' missing configured date column '{date_column}'."
        )

    standardized["date"] = _normalize_date_column(raw_df[date_column], target_timezone=pipeline_timezone)

    symbol_column = source_config.get("symbol_column")
    if symbol_column:
        if symbol_column not in raw_df.columns:
            raise ValueError(
                f"Source '{source_config.get('name', 'unknown')}' missing configured symbol column '{symbol_column}'."
            )
        standardized["symbol"] = raw_df[symbol_column].astype("string")
    else:
        default_symbol = source_config.get("symbol")
        if not default_symbol:
            raise ValueError(
                f"Source '{source_config.get('name', 'unknown')}' must set symbol_column or symbol in config."
            )
        standardized["symbol"] = str(default_symbol)

    standardized["asset_type"] = str(source_config.get("asset_type", "unknown"))

    for canonical_col in ["open", "high", "low", "close", "volume"]:
        source_col = mapping.get(canonical_col)
        if source_col and source_col in raw_df.columns:
            standardized[canonical_col] = pd.to_numeric(raw_df[source_col], errors="coerce")
        else:
            standardized[canonical_col] = pd.Series([float("nan")] * len(raw_df), dtype="float64")

    standardized["source"] = str(source_config.get("source", source_config.get("name", "unknown")))

    ts = asof_timestamp or datetime.now(timezone.utc)
    standardized["asof_timestamp"] = pd.to_datetime(ts, utc=True).tz_localize(None)

    standardized = standardized[REQUIRED_COLUMNS].copy()
    standardized.sort_values(["date", "symbol"], inplace=True)
    standardized.reset_index(drop=True, inplace=True)

    return standardized
