"""Stage 2 dataset builder: load -> standardize -> validate -> report."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import yaml

from data_pipeline.loaders import load_source_dataframe
from data_pipeline.schema import REQUIRED_COLUMNS, get_schema_validation_errors, validate_standardized_schema
from data_pipeline.standardize import standardize_source_dataframe

LOGGER = logging.getLogger("data_pipeline.build_dataset")


def _load_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    if not isinstance(config, dict):
        raise ValueError(f"Config must parse to a dictionary: {config_path}")
    return config


def _ensure_output_dirs(base_output_dir: Path) -> Dict[str, Path]:
    raw_dir = base_output_dir / "raw"
    standardized_dir = base_output_dir / "standardized"
    metadata_dir = base_output_dir / "metadata"

    raw_dir.mkdir(parents=True, exist_ok=True)
    standardized_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    return {"raw": raw_dir, "standardized": standardized_dir, "metadata": metadata_dir}


def _schema_signature(df: pd.DataFrame) -> Dict[str, str]:
    return {col: str(df[col].dtype) for col in REQUIRED_COLUMNS}


def _format_date(value: Any) -> str | None:
    if pd.isna(value):
        return None
    return pd.Timestamp(value).date().isoformat()


def _summarize_source(source_name: str, standardized_df: pd.DataFrame) -> Dict[str, Any]:
    missing_counts = {col: int(standardized_df[col].isna().sum()) for col in REQUIRED_COLUMNS}
    rows = int(len(standardized_df))
    unique_symbols = sorted(standardized_df["symbol"].dropna().astype(str).unique().tolist())

    return {
        "source": source_name,
        "rows": rows,
        "unique_symbols": unique_symbols,
        "date_start": _format_date(standardized_df["date"].min()) if rows else None,
        "date_end": _format_date(standardized_df["date"].max()) if rows else None,
        "missing_counts": missing_counts,
    }


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def _detect_duplicates(df: pd.DataFrame, duplicate_keys: List[str], metadata_dir: Path) -> Dict[str, Any]:
    duplicated = df[df.duplicated(subset=duplicate_keys, keep=False)].copy()
    duplicate_path = metadata_dir / "duplicate_report.parquet"
    duplicated.to_parquet(duplicate_path, index=False)
    return {
        "keys": duplicate_keys,
        "count": int(len(duplicated)),
        "report_path": str(duplicate_path),
    }


def _negative_price_report(df: pd.DataFrame, metadata_dir: Path) -> Dict[str, Any]:
    price_cols = ["open", "high", "low", "close"]
    negative_mask = (df[price_cols] < 0).any(axis=1)
    negatives = df[negative_mask].copy()
    negative_path = metadata_dir / "negative_price_report.parquet"
    negatives.to_parquet(negative_path, index=False)

    return {
        "count": int(len(negatives)),
        "report_path": str(negative_path),
    }


def _null_rate_summary(df: pd.DataFrame) -> Dict[str, float]:
    total_rows = max(len(df), 1)
    return {col: round(float(df[col].isna().sum()) / total_rows, 6) for col in REQUIRED_COLUMNS}


def _zero_heavy_summary(df: pd.DataFrame, fields: List[str], threshold: float) -> List[Dict[str, Any]]:
    warnings: List[Dict[str, Any]] = []
    for col in fields:
        if col not in df.columns:
            continue
        non_null = df[col].dropna()
        if non_null.empty:
            zero_ratio = 0.0
        else:
            zero_ratio = float((non_null == 0).mean())
        if zero_ratio >= threshold:
            warnings.append(
                {
                    "column": col,
                    "zero_ratio": round(zero_ratio, 6),
                    "threshold": threshold,
                    "message": f"Column '{col}' exceeds zero-heavy threshold.",
                }
            )
    return warnings


def run_pipeline(config_path: str) -> Dict[str, Any]:
    config = _load_config(config_path)

    data_cfg = config.get("data", {})
    qa_cfg = config.get("qa", {})

    sources = data_cfg.get("raw_sources", [])
    if not sources:
        raise ValueError("No raw_sources configured in config/data.yaml under data.raw_sources")

    timezone_name = data_cfg.get("timezone", "UTC")
    output_dir = Path(data_cfg.get("output_dir", "outputs/data"))
    dirs = _ensure_output_dirs(output_dir)

    asof_timestamp = datetime.now(timezone.utc)
    standardized_frames: List[pd.DataFrame] = []
    source_summaries: List[Dict[str, Any]] = []

    for source in sources:
        if not source.get("enabled", True):
            LOGGER.info("Skipping disabled source: %s", source.get("name", "unknown"))
            continue

        source_name = source.get("name", source.get("source", "unknown_source"))
        LOGGER.info("Loading source: %s", source_name)

        raw_df = load_source_dataframe(source)
        raw_path = dirs["raw"] / f"{source_name}.parquet"
        raw_df.to_parquet(raw_path, index=False)

        standardized_df = standardize_source_dataframe(
            raw_df=raw_df,
            source_config=source,
            pipeline_timezone=timezone_name,
            asof_timestamp=asof_timestamp,
        )

        validate_standardized_schema(standardized_df)
        standardized_path = dirs["standardized"] / f"{source_name}.parquet"
        standardized_df.to_parquet(standardized_path, index=False)

        standardized_frames.append(standardized_df)
        source_summaries.append(_summarize_source(source_name=source_name, standardized_df=standardized_df))

    if not standardized_frames:
        raise ValueError("No standardized outputs were generated. Ensure at least one source is enabled.")

    schema_signatures = [_schema_signature(df) for df in standardized_frames]
    schema_consistent = all(sig == schema_signatures[0] for sig in schema_signatures[1:])
    if not schema_consistent:
        raise ValueError("Schema consistency check failed across standardized outputs.")

    combined = pd.concat(standardized_frames, axis=0, ignore_index=True)

    duplicate_keys = qa_cfg.get("duplicate_key", ["date", "symbol"])
    duplicates_info = _detect_duplicates(df=combined, duplicate_keys=duplicate_keys, metadata_dir=dirs["metadata"])
    if duplicates_info["count"] > 0:
        LOGGER.warning(
            "Duplicate key rows detected for keys %s: %s",
            duplicate_keys,
            duplicates_info["count"],
        )

    negatives_info = _negative_price_report(df=combined, metadata_dir=dirs["metadata"])
    allow_negative_prices = bool(qa_cfg.get("allow_negative_prices", False))
    if negatives_info["count"] > 0:
        LOGGER.warning("Negative price rows detected: %s", negatives_info["count"])
        if not allow_negative_prices:
            LOGGER.warning("allow_negative_prices=false; anomaly persisted for investigation.")

    zero_heavy_fields = qa_cfg.get("zero_heavy_fields", ["volume"])
    zero_heavy_threshold = float(qa_cfg.get("zero_heavy_threshold", 0.5))
    zero_heavy_warnings = _zero_heavy_summary(
        df=combined,
        fields=zero_heavy_fields,
        threshold=zero_heavy_threshold,
    )
    for warning in zero_heavy_warnings:
        LOGGER.warning(
            "Zero-heavy warning | column=%s ratio=%.6f threshold=%.6f",
            warning["column"],
            warning["zero_ratio"],
            warning["threshold"],
        )

    null_rates = _null_rate_summary(combined)

    sanity_report = {
        "generated_at_utc": asof_timestamp.isoformat(),
        "negative_price_check": negatives_info,
        "zero_heavy_warnings": zero_heavy_warnings,
        "null_rates": null_rates,
    }
    sanity_report_path = dirs["metadata"] / "sanity_report.json"
    _write_json(sanity_report_path, sanity_report)

    all_schema_errors = get_schema_validation_errors(combined, allow_extra_columns=False)

    summary = {
        "generated_at_utc": asof_timestamp.isoformat(),
        "config_path": config_path,
        "output_dir": str(output_dir),
        "totals": {
            "rows": int(len(combined)),
            "sources": int(len(source_summaries)),
            "symbols": sorted(combined["symbol"].dropna().astype(str).unique().tolist()),
            "date_start": _format_date(combined["date"].min()),
            "date_end": _format_date(combined["date"].max()),
        },
        "by_source": source_summaries,
        "duplicates": duplicates_info,
        "schema_consistency": {
            "consistent": schema_consistent,
            "signature": schema_signatures[0],
            "combined_errors": all_schema_errors,
        },
        "sanity_report_path": str(sanity_report_path),
    }

    summary_path = dirs["metadata"] / "source_summary.json"
    _write_json(summary_path, summary)
    LOGGER.info("Stage 2 data pipeline completed. Summary written to %s", summary_path)

    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Stage 2 standardized dataset snapshots.")
    parser.add_argument("--config", default="config/data.yaml", help="Path to YAML config file.")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    run_pipeline(config_path=args.config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
