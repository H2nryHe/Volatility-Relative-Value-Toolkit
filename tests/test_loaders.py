from __future__ import annotations

import pytest

from data_pipeline.loaders import load_source_dataframe


def test_csv_loader_missing_file_raises_file_not_found() -> None:
    source_cfg = {
        "name": "missing_csv",
        "loader": "csv",
        "path": "does/not/exist.csv",
    }

    with pytest.raises(FileNotFoundError):
        load_source_dataframe(source_cfg)
