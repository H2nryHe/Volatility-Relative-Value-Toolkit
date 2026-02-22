from __future__ import annotations

import pandas as pd
import pytest

from data_pipeline.schema import validate_standardized_schema


def test_validate_standardized_schema_passes_on_valid_frame() -> None:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02"]),
            "symbol": ["SPY"],
            "asset_type": ["etf"],
            "open": [1.0],
            "high": [1.1],
            "low": [0.9],
            "close": [1.0],
            "volume": [100.0],
            "source": ["unit_test"],
            "asof_timestamp": pd.to_datetime(["2024-01-03T00:00:00"]),
        }
    )

    result = validate_standardized_schema(df)
    assert result.valid is True


def test_validate_standardized_schema_fails_on_missing_column() -> None:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02"]),
            "symbol": ["SPY"],
            "asset_type": ["etf"],
            "open": [1.0],
            "high": [1.1],
            "low": [0.9],
            "close": [1.0],
            "source": ["unit_test"],
            "asof_timestamp": pd.to_datetime(["2024-01-03T00:00:00"]),
        }
    )

    with pytest.raises(ValueError, match="Missing required columns"):
        validate_standardized_schema(df)
