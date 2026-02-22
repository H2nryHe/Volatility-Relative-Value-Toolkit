"""Offline-friendly yfinance stub for Stage 2."""

from __future__ import annotations

from typing import Any, Dict, List

import numpy as np
import pandas as pd


DEFAULT_COLUMNS = ["Date", "Open", "High", "Low", "Close", "Volume", "Ticker"]


def load_yfinance_stub(source_config: Dict[str, Any]) -> pd.DataFrame:
    """Return deterministic synthetic OHLCV data when yfinance access is unavailable."""

    symbols: List[str] = source_config.get("symbols", [source_config.get("symbol", "SPY")])
    start = source_config.get("start", "2024-01-02")
    end = source_config.get("end", "2024-01-10")

    dates = pd.date_range(start=start, end=end, freq="B")
    rows = []
    for symbol in symbols:
        base = float(source_config.get("base_price", 20.0))
        for idx, day in enumerate(dates):
            price = base + idx * 0.2
            rows.append(
                {
                    "Date": day.strftime("%Y-%m-%d"),
                    "Open": price,
                    "High": price * 1.01,
                    "Low": price * 0.99,
                    "Close": price * (1.0 + 0.001 * np.sin(idx)),
                    "Volume": int(1000 + idx * 25),
                    "Ticker": symbol,
                }
            )

    if not rows:
        return pd.DataFrame(columns=DEFAULT_COLUMNS)

    return pd.DataFrame(rows, columns=DEFAULT_COLUMNS)
