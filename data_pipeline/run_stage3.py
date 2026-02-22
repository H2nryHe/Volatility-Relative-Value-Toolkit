"""Stage 3 pipeline: calendar alignment, QA, and roll processing."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import yaml

from data_pipeline.calendars import align_to_calendar
from data_pipeline.qa import apply_fill_rules, detect_outliers_zscore
from data_pipeline.rolls import build_continuous_series

LOGGER = logging.getLogger("data_pipeline.run_stage3")


def _load_config(config_path: str) -> Dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    if not isinstance(cfg, dict):
        raise ValueError(f"Config must be a dictionary: {config_path}")
    return cfg


def _ensure_dirs(data_cfg: Dict[str, Any]) -> Dict[str, Path]:
    output_dir = Path(data_cfg.get("output_dir", "outputs/data"))
    stage3_cfg = data_cfg.get("stage3", {})

    standardized_input = Path(stage3_cfg.get("standardized_input_dir", output_dir / "standardized"))
    clean_dir = Path(stage3_cfg.get("clean_output_dir", output_dir / "clean"))
    continuous_dir = Path(stage3_cfg.get("continuous_output_dir", output_dir / "continuous"))
    qa_dir = Path(stage3_cfg.get("qa_output_dir", output_dir / "qa"))

    clean_dir.mkdir(parents=True, exist_ok=True)
    continuous_dir.mkdir(parents=True, exist_ok=True)
    qa_dir.mkdir(parents=True, exist_ok=True)

    return {
        "standardized_input": standardized_input,
        "clean": clean_dir,
        "continuous": continuous_dir,
        "qa": qa_dir,
    }


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _duplicate_count(df: pd.DataFrame, keys: List[str]) -> int:
    if not all(key in df.columns for key in keys):
        return 0
    return int(df.duplicated(subset=keys, keep=False).sum())


def _stage3_for_source(
    source_name: str,
    df: pd.DataFrame,
    config: Dict[str, Any],
    dirs: Dict[str, Path],
) -> Dict[str, Any]:
    calendar_cfg = config.get("calendar", {})
    qa_cfg = config.get("qa", {})
    fill_cfg = qa_cfg.get("fill", {"fields": ["open", "high", "low", "close"], "method": "ffill", "limit": 1})
    outlier_cfg = qa_cfg.get("outlier", {"fields": ["close"], "method": "zscore", "zscore_threshold": 3.0, "min_obs": 3})
    roll_cfg = config.get("roll", {})

    input_duplicate_rows = _duplicate_count(df, ["date", "symbol"])

    aligned_df, _ = align_to_calendar(df=df, calendar_config=calendar_cfg)
    aligned_df["source_file"] = source_name

    if not bool(fill_cfg.get("enabled", True)):
        fill_cfg = {**fill_cfg, "fields": []}
    filled_df, missing_report, fill_counts = apply_fill_rules(aligned_df, fill_config=fill_cfg)
    filled_df["source_file"] = source_name

    if not missing_report.empty:
        missing_report = missing_report.copy()
        missing_report["source_file"] = source_name
        missing_report = missing_report.loc[
            missing_report["missing_before_fill"] | missing_report["filled"]
        ].reset_index(drop=True)

    marked_df, outlier_report = detect_outliers_zscore(filled_df, outlier_config=outlier_cfg)
    marked_df["source_file"] = source_name
    if not outlier_report.empty:
        outlier_report = outlier_report.copy()
        outlier_report["source_file"] = source_name

    clean_path = dirs["clean"] / f"{source_name}.parquet"
    marked_df.to_parquet(clean_path, index=False)

    continuous_df, roll_log = build_continuous_series(marked_df, roll_config=roll_cfg)
    if continuous_df.empty:
        continuous_df = marked_df.copy()
        continuous_df["active_contract"] = continuous_df["symbol"].astype(str)
        continuous_df["roll_reason"] = "no_roll_metadata"
    continuous_df["source_file"] = source_name

    continuous_path = dirs["continuous"] / f"{source_name}.parquet"
    continuous_df.to_parquet(continuous_path, index=False)

    if not roll_log.empty:
        roll_log = roll_log.copy()
        roll_log["source_file"] = source_name

    missing_before = int(aligned_df[["open", "high", "low", "close", "volume"]].isna().sum().sum())
    missing_after = int(marked_df[["open", "high", "low", "close", "volume"]].isna().sum().sum())

    monotonic_by_symbol = (
        marked_df.sort_values(["symbol", "date"]).groupby("symbol")["date"].apply(lambda s: s.is_monotonic_increasing).all()
    )

    return {
        "source": source_name,
        "clean_path": str(clean_path),
        "continuous_path": str(continuous_path),
        "missing_report": missing_report,
        "outlier_report": outlier_report,
        "roll_log": roll_log,
        "stats": {
            "rows_aligned": int(len(aligned_df)),
            "rows_clean": int(len(marked_df)),
            "rows_continuous": int(len(continuous_df)),
            "missing_before": missing_before,
            "missing_after": missing_after,
            "fill_counts": fill_counts,
            "outlier_count": int(len(outlier_report)),
            "roll_events": int(len(roll_log)),
            "duplicate_rows_input": input_duplicate_rows,
            "is_market_closed_available": True,
            "is_data_missing_available": True,
            "calendar_monotonic": bool(monotonic_by_symbol),
        },
    }


def run_stage3(config_path: str) -> Dict[str, Any]:
    config = _load_config(config_path)
    data_cfg = config.get("data", {})
    dirs = _ensure_dirs(data_cfg)

    standardized_files = sorted(dirs["standardized_input"].glob("*.parquet"))
    if not standardized_files:
        raise FileNotFoundError(f"No standardized parquet files found in {dirs['standardized_input']}")

    per_source: List[Dict[str, Any]] = []
    all_missing = []
    all_outliers = []
    all_roll_logs = []
    all_clean = []

    for path in standardized_files:
        source_name = path.stem
        LOGGER.info("Stage 3 processing source: %s", source_name)
        sdf = pd.read_parquet(path)
        result = _stage3_for_source(source_name=source_name, df=sdf, config=config, dirs=dirs)
        per_source.append(result)

        all_clean.append(pd.read_parquet(result["clean_path"]))
        if not result["missing_report"].empty:
            all_missing.append(result["missing_report"])
        if not result["outlier_report"].empty:
            all_outliers.append(result["outlier_report"])
        if not result["roll_log"].empty:
            all_roll_logs.append(result["roll_log"])

    missing_report = pd.concat(all_missing, axis=0, ignore_index=True) if all_missing else pd.DataFrame(
        columns=[
            "date",
            "symbol",
            "field",
            "missing_before_fill",
            "missing_after_fill",
            "filled",
            "source_file",
        ]
    )
    outlier_report = pd.concat(all_outliers, axis=0, ignore_index=True) if all_outliers else pd.DataFrame(
        columns=["date", "symbol", "field", "value", "zscore", "is_outlier", "source_file"]
    )
    roll_log = pd.concat(all_roll_logs, axis=0, ignore_index=True) if all_roll_logs else pd.DataFrame(
        columns=["date", "from_contract", "to_contract", "reason", "root_symbol", "source_file"]
    )

    missing_path = dirs["qa"] / "missing_report.parquet"
    outlier_path = dirs["qa"] / "outlier_report.parquet"
    roll_log_path = dirs["qa"] / "roll_log.parquet"

    missing_report.to_parquet(missing_path, index=False)
    outlier_report.to_parquet(outlier_path, index=False)
    roll_log.to_parquet(roll_log_path, index=False)

    combined_clean = pd.concat(all_clean, axis=0, ignore_index=True)
    duplicate_keys = config.get("qa", {}).get("duplicate_key", ["date", "symbol"])

    qa_report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "config_path": config_path,
        "artifacts": {
            "missing_report": str(missing_path),
            "outlier_report": str(outlier_path),
            "roll_log": str(roll_log_path),
            "clean_dir": str(dirs["clean"]),
            "continuous_dir": str(dirs["continuous"]),
        },
        "summary": {
            "sources": len(per_source),
            "total_clean_rows": int(len(combined_clean)),
            "duplicate_rows_clean": _duplicate_count(combined_clean, duplicate_keys),
            "outlier_rows": int(len(outlier_report)),
            "missing_rows_filled": int(len(missing_report)),
            "roll_events": int(len(roll_log)),
        },
        "by_source": [
            {
                "source": item["source"],
                **item["stats"],
            }
            for item in per_source
        ],
    }

    qa_report_path = dirs["qa"] / "qa_report.json"
    _write_json(qa_report_path, qa_report)
    LOGGER.info("Stage 3 QA report written to %s", qa_report_path)

    return qa_report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Stage 3 QA/alignment/roll pipeline.")
    parser.add_argument("--config", default="config/data.yaml", help="Path to YAML config.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    run_stage3(config_path=args.config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
