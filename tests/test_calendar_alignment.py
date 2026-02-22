from __future__ import annotations

import pandas as pd

from data_pipeline.calendars import align_to_calendar


def test_calendar_alignment_preserves_chronological_order_and_marks_missing() -> None:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-04", "2024-01-02"]),
            "symbol": ["SPY", "SPY", "VIXY"],
            "open": [100.0, 102.0, 20.0],
            "high": [101.0, 103.0, 21.0],
            "low": [99.0, 101.0, 19.0],
            "close": [100.5, 102.5, 20.5],
            "volume": [1000.0, 1100.0, 500.0],
            "asset_type": ["etf", "etf", "etf"],
            "source": ["test", "test", "test"],
            "asof_timestamp": pd.to_datetime(["2024-01-05", "2024-01-05", "2024-01-05"]),
        }
    )

    aligned, flags = align_to_calendar(df, {"frequency": "B", "start": "2024-01-02", "end": "2024-01-04"})

    # Each symbol is aligned to 3 business days.
    assert len(aligned.loc[aligned["symbol"] == "SPY"]) == 3
    assert len(aligned.loc[aligned["symbol"] == "VIXY"]) == 3

    spy_dates = aligned.loc[aligned["symbol"] == "SPY", "date"].tolist()
    assert spy_dates == sorted(spy_dates)

    missing_spy = flags.loc[(flags["symbol"] == "SPY") & (flags["date"] == pd.Timestamp("2024-01-03"))]
    assert len(missing_spy) == 1
    assert bool(missing_spy.iloc[0]["is_data_missing"]) is True
    assert bool(missing_spy.iloc[0]["is_market_closed"]) is False
