"""Stage 6 risk pipeline orchestrator."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import yaml

from risk.drawdown import compute_drawdown, summarize_drawdown
from risk.exposures import compute_exposures
from risk.stress import compute_stress_report
from risk.var_cvar import compute_historical_var_cvar

LOGGER = logging.getLogger("risk.run_risk")


def _load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    if not isinstance(cfg, dict):
        raise ValueError(f"Config must be a mapping: {path}")
    return cfg


def run_risk_pipeline(risk_config_path: str, backtest_output_dir: str = "outputs/backtests") -> Dict[str, str]:
    cfg = _load_yaml(risk_config_path)
    out_dir = Path(backtest_output_dir)

    pnl_path = out_dir / "pnl.parquet"
    positions_path = out_dir / "positions.parquet"

    if not pnl_path.exists() or not positions_path.exists():
        raise FileNotFoundError("Stage 5 outputs missing: require outputs/backtests/pnl.parquet and positions.parquet")

    pnl_df = pd.read_parquet(pnl_path)
    positions_df = pd.read_parquet(positions_path)
    pnl_df["date"] = pd.to_datetime(pnl_df["date"])
    positions_df["date"] = pd.to_datetime(positions_df["date"])

    pnl_df = pnl_df.sort_values("date").reset_index(drop=True)
    positions_df = positions_df.sort_values("date").reset_index(drop=True)

    strategy_ret = pnl_df["net_pnl"] / pnl_df["equity"].shift(1).replace(0.0, pd.NA)
    strategy_ret = strategy_ret.fillna(0.0)

    risk_cfg = cfg.get("risk", {})
    var_conf = float(risk_cfg.get("var_confidence", 0.95))
    cvar_conf = float(risk_cfg.get("cvar_confidence", var_conf))
    horizon_days = int(risk_cfg.get("horizon_days", 1))

    var_metrics = compute_historical_var_cvar(strategy_ret, confidence=var_conf, horizon_days=horizon_days)
    cvar_metrics = compute_historical_var_cvar(strategy_ret, confidence=cvar_conf, horizon_days=horizon_days)

    drawdown_df = compute_drawdown(pnl_df["equity"]) 
    drawdown_summary = summarize_drawdown(drawdown_df)

    exposure_cfg = cfg.get("exposures", {})
    exposures_df = compute_exposures(pnl_df=pnl_df, positions_df=positions_df, config=exposure_cfg)

    stress_windows = cfg.get("stress", {}).get("windows", [])
    stress_df = compute_stress_report(pnl_df=pnl_df, windows=stress_windows)

    exposures_path = out_dir / "exposures.parquet"
    stress_path = out_dir / "stress_report.parquet"
    risk_metrics_path = out_dir / "risk_metrics.json"

    exposures_df.to_parquet(exposures_path, index=False)
    stress_df.to_parquet(stress_path, index=False)

    risk_metrics = {
        "conventions": {
            "loss_sign": "losses are reported as positive values via losses = -returns",
            "horizon_days": horizon_days,
            "var_confidence": var_conf,
            "cvar_confidence": cvar_conf,
        },
        "var_cvar": {
            "var": var_metrics["var"],
            "cvar": cvar_metrics["cvar"],
        },
        "drawdown": drawdown_summary,
        "exposure_summary": {
            "beta_proxy_latest": float(exposures_df["beta_proxy"].dropna().iloc[-1]) if exposures_df["beta_proxy"].dropna().any() else None,
            "vega_proxy_latest": float(exposures_df["vega_proxy"].dropna().iloc[-1]) if exposures_df["vega_proxy"].dropna().any() else None,
            "gamma_proxy_latest": float(exposures_df["gamma_proxy"].dropna().iloc[-1]) if exposures_df["gamma_proxy"].dropna().any() else None,
        },
        "stress_windows": stress_windows,
        "artifacts": {
            "exposures": str(exposures_path),
            "stress_report": str(stress_path),
        },
    }

    risk_metrics_path.write_text(json.dumps(risk_metrics, indent=2, sort_keys=True), encoding="utf-8")

    LOGGER.info("Risk artifacts written to %s", out_dir)
    return {
        "risk_metrics": str(risk_metrics_path),
        "stress_report": str(stress_path),
        "exposures": str(exposures_path),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Stage 6 risk analytics pipeline")
    parser.add_argument("--risk-config", default="config/risk.yaml", help="Risk config path")
    parser.add_argument("--backtest-output-dir", default="outputs/backtests", help="Backtest outputs path")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    run_risk_pipeline(args.risk_config, args.backtest_output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
