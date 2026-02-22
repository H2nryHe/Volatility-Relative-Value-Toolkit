from __future__ import annotations

import pandas as pd

from data_pipeline.rolls import build_continuous_series


def _make_roll_fixture() -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", "2024-01-10", freq="B")
    rows = []

    for d in dates:
        rows.append(
            {
                "date": d,
                "symbol": "VX_F1",
                "contract": "VX_F1",
                "root_symbol": "VX",
                "expiry": pd.Timestamp("2024-01-08"),
                "close": 20.0,
            }
        )

        # Second contract becomes available from Jan-04 onward.
        if d >= pd.Timestamp("2024-01-04"):
            rows.append(
                {
                    "date": d,
                    "symbol": "VX_F2",
                    "contract": "VX_F2",
                    "root_symbol": "VX",
                    "expiry": pd.Timestamp("2024-02-14"),
                    "close": 21.0,
                }
            )

    return pd.DataFrame(rows)


def test_roll_triggers_expected_date_without_lookahead() -> None:
    df = _make_roll_fixture()
    continuous, roll_log = build_continuous_series(
        df,
        {
            "contract_column": "contract",
            "expiry_column": "expiry",
            "root_column": "root_symbol",
            "n_days_before_expiry": 2,
        },
    )

    assert not continuous.empty
    assert not roll_log.empty

    # No lookahead: cannot roll before VX_F2 is visible on Jan-04.
    first_roll_date = roll_log.iloc[0]["date"]
    assert pd.Timestamp(first_roll_date) == pd.Timestamp("2024-01-04")

    # Active contract after first roll should be VX_F2.
    active_on_roll_day = continuous.loc[continuous["date"] == pd.Timestamp("2024-01-04"), "active_contract"].iloc[0]
    assert active_on_roll_day == "VX_F2"


def test_no_roll_metadata_returns_empty_roll_log() -> None:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-02", "2024-01-03"]),
            "symbol": ["SPY", "SPY"],
            "close": [100.0, 101.0],
        }
    )

    continuous, roll_log = build_continuous_series(df, {"n_days_before_expiry": 2})
    assert len(roll_log) == 0
    assert "active_contract" in continuous.columns
