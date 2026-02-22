"""Canonical schema contract for standardized market data."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype, is_object_dtype

# Required canonical columns for Stage 2 outputs.
REQUIRED_COLUMNS: List[str] = [
    "date",
    "symbol",
    "asset_type",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
    "asof_timestamp",
]

# Nullable contract for canonical columns.
# - `date`, `symbol`, `asset_type`, `source`, `asof_timestamp` should be non-null.
# - price/volume fields may be null at this stage but must be numeric dtype.
NULLABLE_RULES: Dict[str, bool] = {
    "date": False,
    "symbol": False,
    "asset_type": False,
    "open": True,
    "high": True,
    "low": True,
    "close": True,
    "volume": True,
    "source": False,
    "asof_timestamp": False,
}


@dataclass(frozen=True)
class SchemaValidationResult:
    valid: bool
    errors: List[str]


def _validate_dtypes(df: pd.DataFrame) -> List[str]:
    errors: List[str] = []

    if "date" in df.columns and not is_datetime64_any_dtype(df["date"]):
        errors.append("Column 'date' must be datetime dtype.")

    if "asof_timestamp" in df.columns and not is_datetime64_any_dtype(df["asof_timestamp"]):
        errors.append("Column 'asof_timestamp' must be datetime dtype.")

    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns and not is_numeric_dtype(df[col]):
            errors.append(f"Column '{col}' must be numeric dtype.")

    for col in ["symbol", "asset_type", "source"]:
        if col in df.columns and not (is_object_dtype(df[col]) or str(df[col].dtype).startswith("string")):
            errors.append(f"Column '{col}' must be string/object dtype.")

    return errors


def get_schema_validation_errors(df: pd.DataFrame, allow_extra_columns: bool = True) -> List[str]:
    """Return schema validation errors for a standardized DataFrame."""

    errors: List[str] = []
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")

    if not allow_extra_columns:
        extras = [col for col in df.columns if col not in REQUIRED_COLUMNS]
        if extras:
            errors.append(f"Unexpected extra columns: {extras}")

    errors.extend(_validate_dtypes(df))

    for col, nullable in NULLABLE_RULES.items():
        if col in df.columns and not nullable:
            null_count = int(df[col].isna().sum())
            if null_count > 0:
                errors.append(f"Column '{col}' has {null_count} null values but is non-nullable.")

    return errors


def validate_standardized_schema(df: pd.DataFrame, allow_extra_columns: bool = True) -> SchemaValidationResult:
    """Validate standardized schema and raise on failure for strict pipeline behavior."""

    errors = get_schema_validation_errors(df=df, allow_extra_columns=allow_extra_columns)
    if errors:
        raise ValueError("Standardized schema validation failed: " + " | ".join(errors))
    return SchemaValidationResult(valid=True, errors=[])
