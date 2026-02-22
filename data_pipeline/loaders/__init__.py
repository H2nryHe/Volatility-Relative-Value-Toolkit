"""Source loader dispatch for Stage 2 pipeline."""

from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from .csv_loader import load_csv_source
from .yfinance_loader import load_yfinance_stub


def load_source_dataframe(source_config: Dict[str, Any]) -> pd.DataFrame:
    loader = str(source_config.get("loader", "")).lower().strip()
    if not loader:
        raise ValueError(f"Source '{source_config.get('name', 'unknown')}' missing required field: loader")

    if loader == "csv":
        return load_csv_source(source_config)
    if loader == "yfinance":
        return load_yfinance_stub(source_config)

    raise ValueError(f"Unsupported loader '{loader}' for source '{source_config.get('name', 'unknown')}'")
