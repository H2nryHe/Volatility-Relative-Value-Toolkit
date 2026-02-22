from __future__ import annotations

from pathlib import Path

import yaml

from data_pipeline.build_dataset import run_pipeline


def test_run_pipeline_generates_required_stage2_artifacts(tmp_path: Path) -> None:
    output_dir = tmp_path / "outputs" / "data"
    config = {
        "data": {
            "timezone": "UTC",
            "output_dir": str(output_dir),
            "raw_sources": [
                {
                    "name": "sample_market_csv",
                    "enabled": True,
                    "loader": "csv",
                    "path": "data_pipeline/samples/sample_prices.csv",
                    "asset_type": "etf",
                    "source": "local_sample_csv",
                    "date_column": "Date",
                    "symbol_column": "Ticker",
                    "column_mapping": {
                        "open": "Open",
                        "high": "High",
                        "low": "Low",
                        "close": "Close",
                        "volume": "Volume",
                    },
                }
            ],
        },
        "qa": {
            "duplicate_key": ["date", "symbol"],
            "zero_heavy_fields": ["volume"],
            "zero_heavy_threshold": 0.20,
        },
    }

    cfg_path = tmp_path / "data.yaml"
    cfg_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    summary = run_pipeline(str(cfg_path))

    assert summary["totals"]["rows"] > 0
    assert (output_dir / "raw" / "sample_market_csv.parquet").exists()
    assert (output_dir / "standardized" / "sample_market_csv.parquet").exists()
    assert (output_dir / "metadata" / "source_summary.json").exists()
    assert (output_dir / "metadata" / "duplicate_report.parquet").exists()
    assert (output_dir / "metadata" / "negative_price_report.parquet").exists()
    assert (output_dir / "metadata" / "sanity_report.json").exists()
