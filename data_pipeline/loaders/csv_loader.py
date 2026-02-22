"""CSV loader utilities for Stage 2 data ingestion."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pandas as pd


def load_csv_source(source_config: Dict[str, Any]) -> pd.DataFrame:
    """Load one CSV source using source-specific config."""

    path_value = source_config.get("path")
    if not path_value:
        raise ValueError(f"CSV source '{source_config.get('name', 'unknown')}' missing required field: path")

    csv_path = Path(path_value)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV source file not found: {csv_path}")

    read_kwargs = source_config.get("read_csv_kwargs", {})
    df = pd.read_csv(csv_path, **read_kwargs)
    if df.empty:
        raise ValueError(f"CSV source '{source_config.get('name', 'unknown')}' loaded empty data: {csv_path}")

    return df
