"""Signal orchestration entrypoint for Stage 4."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import yaml

from signals.base import summarize_signal_columns
from signals.carry_roll import compute_carry_roll_down
from signals.pca_factors import compute_pca_factors
from signals.term_structure import compute_curvature, compute_slope
from signals.vrp_proxy import compute_vrp_proxy

LOGGER = logging.getLogger("signals.signal_registry")


def _load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    if not isinstance(cfg, dict):
        raise ValueError(f"YAML must parse to dictionary: {path}")
    return cfg


def _read_stage3_inputs(data_config: Dict[str, Any]) -> pd.DataFrame:
    output_dir = Path(data_config.get("data", {}).get("output_dir", "outputs/data"))
    stage3 = data_config.get("data", {}).get("stage3", {})
    continuous_dir = Path(stage3.get("continuous_output_dir", output_dir / "continuous"))

    files = sorted(continuous_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Stage 3 continuous files found in {continuous_dir}")

    frames = []
    for f in files:
        sdf = pd.read_parquet(f)
        sdf["source_file"] = f.name
        frames.append(sdf)

    df = pd.concat(frames, axis=0, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values(["date", "symbol"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def _merge_signals(date_frame: pd.DataFrame, components: List[pd.DataFrame]) -> pd.DataFrame:
    out = date_frame.copy()
    for comp in components:
        out = out.merge(comp, on="date", how="left")
    out.sort_values("date", inplace=True)
    out.reset_index(drop=True, inplace=True)
    return out


def build_signals(signals_config_path: str, data_config_path: str) -> Dict[str, Any]:
    signal_cfg = _load_yaml(signals_config_path)
    data_cfg = _load_yaml(data_config_path)

    df = _read_stage3_inputs(data_cfg)

    output_dir = Path(data_cfg.get("data", {}).get("output_dir", "outputs/data"))
    output_dir.mkdir(parents=True, exist_ok=True)
    signal_path = output_dir / "signals.parquet"
    diagnostics_path = output_dir / "signal_diagnostics.json"
    pca_loadings_path = output_dir / "pca_loadings.json"

    execution_lag = int(signal_cfg.get("execution", {}).get("signal_shift_days", 1))

    enabled_cfg = signal_cfg.get("signals", {})
    components: List[pd.DataFrame] = []
    diagnostics: Dict[str, Any] = {}

    date_frame = pd.DataFrame({"date": sorted(df["date"].unique())})

    if enabled_cfg.get("term_structure_slope", {}).get("enabled", False):
        cfg = {**signal_cfg.get("term_structure", {}), **enabled_cfg.get("term_structure_slope", {})}
        cfg.setdefault("lag_days", execution_lag)
        comp = compute_slope(df, cfg)
        components.append(comp)

    if enabled_cfg.get("term_structure_curvature", {}).get("enabled", False):
        cfg = {**signal_cfg.get("term_structure", {}), **enabled_cfg.get("term_structure_curvature", {})}
        cfg.setdefault("lag_days", execution_lag)
        comp = compute_curvature(df, cfg)
        components.append(comp)

    if enabled_cfg.get("carry_roll_down", {}).get("enabled", False):
        cfg = {**signal_cfg.get("term_structure", {}), **enabled_cfg.get("carry_roll_down", {})}
        cfg.setdefault("lag_days", execution_lag)
        comp = compute_carry_roll_down(df, cfg)
        components.append(comp)

    if enabled_cfg.get("vrp_proxy", {}).get("enabled", False):
        cfg = {**enabled_cfg.get("vrp_proxy", {}), **signal_cfg.get("vrp_proxy", {})}
        cfg.setdefault("lag_days", execution_lag)
        comp = compute_vrp_proxy(df, cfg)
        components.append(comp)

    pca_diagnostics: Dict[str, Any] = {}
    if enabled_cfg.get("pca_factors", {}).get("enabled", False):
        cfg = {**signal_cfg.get("term_structure", {}), **enabled_cfg.get("pca_factors", {})}
        cfg.setdefault("lag_days", execution_lag)
        comp, pca_diagnostics = compute_pca_factors(df, cfg)
        components.append(comp)

    signals_df = _merge_signals(date_frame, components)
    signal_cols = [c for c in signals_df.columns if c.startswith("signal_")]
    diagnostics["signals"] = summarize_signal_columns(signals_df, signal_cols)
    diagnostics["pca"] = pca_diagnostics
    diagnostics["artifacts"] = {"pca_loadings": str(pca_loadings_path)}

    signals_df.to_parquet(signal_path, index=False)
    diagnostics_path.write_text(json.dumps(diagnostics, indent=2, sort_keys=True), encoding="utf-8")
    pca_loadings_path.write_text(json.dumps(pca_diagnostics, indent=2, sort_keys=True), encoding="utf-8")

    LOGGER.info("Signals written to %s", signal_path)
    LOGGER.info("Diagnostics written to %s", diagnostics_path)

    return {
        "signal_path": str(signal_path),
        "diagnostics_path": str(diagnostics_path),
        "signal_columns": signal_cols,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Stage 4 signal outputs.")
    parser.add_argument("--signals-config", default="config/signals.yaml", help="Path to signal config YAML")
    parser.add_argument("--data-config", default="config/data.yaml", help="Path to data config YAML")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    build_signals(args.signals_config, args.data_config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
