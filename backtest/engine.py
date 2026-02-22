"""Stage 5 backtest engine entrypoint."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
import yaml

from backtest.attribution import build_attribution
from backtest.contracts import validate_backtest_outputs, validate_signal_input
from backtest.execution import map_signal_to_execution
from backtest.positioning import apply_position_constraints, signal_to_target_position

LOGGER = logging.getLogger("backtest.engine")


def _load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    if not isinstance(cfg, dict):
        raise ValueError(f"YAML config must be mapping: {path}")
    return cfg


def _read_stage_inputs(data_cfg: Dict[str, Any], signals_path: str | None) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    data_root = Path(data_cfg.get("data", {}).get("output_dir", "outputs/data"))
    stage3 = data_cfg.get("data", {}).get("stage3", {})

    continuous_dir = Path(stage3.get("continuous_output_dir", data_root / "continuous"))
    qa_dir = Path(stage3.get("qa_output_dir", data_root / "qa"))

    signal_file = Path(signals_path) if signals_path else data_root / "signals.parquet"
    if not signal_file.exists():
        raise FileNotFoundError(f"Signals file not found: {signal_file}")

    signals_df = pd.read_parquet(signal_file)
    signals_df["date"] = pd.to_datetime(signals_df["date"])

    continuous_files = sorted(continuous_dir.glob("*.parquet"))
    if not continuous_files:
        raise FileNotFoundError(f"No Stage 3 continuous files found in {continuous_dir}")
    continuous_df = pd.concat([pd.read_parquet(f) for f in continuous_files], axis=0, ignore_index=True)
    continuous_df["date"] = pd.to_datetime(continuous_df["date"])

    roll_log_path = qa_dir / "roll_log.parquet"
    if roll_log_path.exists():
        roll_log = pd.read_parquet(roll_log_path)
        if "date" in roll_log.columns:
            roll_log["date"] = pd.to_datetime(roll_log["date"])
    else:
        roll_log = pd.DataFrame(columns=["date", "from_contract", "to_contract", "reason", "root_symbol", "source_file"])

    return signals_df, continuous_df, roll_log


def _build_market_series(continuous_df: pd.DataFrame, symbol: str, price_column: str) -> pd.DataFrame:
    subset = continuous_df.loc[continuous_df["symbol"] == symbol, ["date", price_column]].copy()
    if subset.empty:
        raise ValueError(f"Primary symbol '{symbol}' not found in Stage 3 continuous data.")

    subset = subset.sort_values("date")
    subset = subset.drop_duplicates(subset=["date"], keep="last")
    subset.rename(columns={price_column: "price"}, inplace=True)
    subset["daily_return"] = subset["price"].pct_change().fillna(0.0)
    return subset


def run_backtest(
    signals_df: pd.DataFrame,
    market_df: pd.DataFrame,
    roll_log_df: pd.DataFrame,
    config: Dict[str, Any],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    bt_cfg = config.get("backtest", {})
    risk_cfg = config.get("risk_controls", {})
    cost_cfg = config.get("costs", {})

    initial_capital = float(bt_cfg.get("initial_capital", 1_000_000))
    signal_column = str(bt_cfg.get("signal_column", "signal_term_structure_slope"))
    lag_days = int(bt_cfg.get("signal_execution_lag_days", 1))
    enforce_next_bar = bool(bt_cfg.get("enforce_next_bar_execution", True))

    if enforce_next_bar and lag_days < 1:
        raise ValueError("No-lookahead guard: signal_execution_lag_days must be >= 1 when enforce_next_bar_execution=true")

    validate_signal_input(signals_df, [signal_column])

    execution_plan = map_signal_to_execution(
        signals_df=signals_df,
        market_df=market_df,
        signal_column=signal_column,
        lag_days=lag_days,
    )

    dates = market_df["date"].sort_values().reset_index(drop=True)
    positions = pd.DataFrame({"date": dates})
    positions = positions.merge(market_df[["date", "price", "daily_return"]], on="date", how="left")

    execution_map = execution_plan.set_index("execution_date")["signal_value"].to_dict() if not execution_plan.empty else {}
    signal_date_map = execution_plan.set_index("execution_date")["signal_date"].to_dict() if not execution_plan.empty else {}

    positions["signal_value"] = positions["date"].map(execution_map)
    positions["signal_date"] = positions["date"].map(signal_date_map)

    target_raw = positions["signal_value"].apply(
        lambda x: signal_to_target_position(x, bt_cfg) if pd.notna(x) else np.nan
    )
    positions["target_position_raw"] = target_raw.ffill().fillna(0.0)

    positions["target_position"] = apply_position_constraints(
        target_series=positions["target_position_raw"],
        returns_series=positions["daily_return"],
        risk_config=risk_cfg,
    )
    positions["position"] = positions["target_position"]
    positions["position_prev"] = positions["position"].shift(1).fillna(0.0)

    roll_dates = set()
    if not roll_log_df.empty and "date" in roll_log_df.columns:
        roll_dates = {pd.Timestamp(d) for d in roll_log_df["date"].dropna().unique().tolist()}
    positions["is_roll_date"] = positions["date"].isin(roll_dates)

    positions["trade_qty"] = positions["position"] - positions["position_prev"]
    positions["symbol"] = str(bt_cfg.get("primary_symbol", "SPY"))

    commission_bps = float(cost_cfg.get("commission_bps", 0.0))
    slippage_bps = float(cost_cfg.get("slippage_bps", 0.0))
    roll_cost_bps = float(cost_cfg.get("roll_cost_bps", commission_bps + slippage_bps))
    regular_bps = commission_bps + slippage_bps

    trade_rows: List[Dict[str, Any]] = []
    for row in positions.itertuples(index=False):
        if abs(float(row.trade_qty)) > 0:
            notional = abs(float(row.trade_qty)) * initial_capital
            regular_cost = notional * regular_bps / 10000.0
            trade_rows.append(
                {
                    "date": row.date,
                    "signal_date": row.signal_date,
                    "symbol": row.symbol,
                    "trade_type": "rebalance",
                    "target_position": float(row.target_position),
                    "position_before": float(row.position_prev),
                    "position_after": float(row.position),
                    "trade_qty": float(row.trade_qty),
                    "price": float(row.price),
                    "notional": float(notional),
                    "regular_cost": float(regular_cost),
                    "roll_cost": 0.0,
                    "total_cost": float(regular_cost),
                }
            )

        if bool(row.is_roll_date) and abs(float(row.position)) > 0:
            notional_roll = abs(float(row.position)) * initial_capital
            roll_cost = notional_roll * roll_cost_bps / 10000.0
            trade_rows.append(
                {
                    "date": row.date,
                    "signal_date": row.signal_date,
                    "symbol": row.symbol,
                    "trade_type": "roll",
                    "target_position": float(row.target_position),
                    "position_before": float(row.position),
                    "position_after": float(row.position),
                    "trade_qty": float(row.position),
                    "price": float(row.price),
                    "notional": float(notional_roll),
                    "regular_cost": 0.0,
                    "roll_cost": float(roll_cost),
                    "total_cost": float(roll_cost),
                }
            )

    trades = pd.DataFrame(
        trade_rows,
        columns=[
            "date",
            "signal_date",
            "symbol",
            "trade_type",
            "target_position",
            "position_before",
            "position_after",
            "trade_qty",
            "price",
            "notional",
            "regular_cost",
            "roll_cost",
            "total_cost",
        ],
    )

    if trades.empty:
        trades = pd.DataFrame(
            columns=[
                "date",
                "signal_date",
                "symbol",
                "trade_type",
                "target_position",
                "position_before",
                "position_after",
                "trade_qty",
                "price",
                "notional",
                "regular_cost",
                "roll_cost",
                "total_cost",
            ]
        )

    daily_costs = trades.groupby("date")["total_cost"].sum() if not trades.empty else pd.Series(dtype="float64")

    pnl = positions.loc[:, ["date", "symbol", "position_prev", "daily_return"]].copy()
    pnl["gross_pnl"] = pnl["position_prev"] * pnl["daily_return"] * initial_capital
    pnl["costs"] = pnl["date"].map(daily_costs).fillna(0.0)
    pnl["costs_pnl"] = -pnl["costs"]
    pnl["net_pnl"] = pnl["gross_pnl"] + pnl["costs_pnl"]
    pnl["equity"] = initial_capital + pnl["net_pnl"].cumsum()
    pnl = pnl[["date", "symbol", "position_prev", "daily_return", "gross_pnl", "costs_pnl", "net_pnl", "equity"]]

    carry_col = str(bt_cfg.get("carry_signal_column", "signal_carry_roll_down"))
    carry_signal = signals_df.set_index("date")[carry_col] if carry_col in signals_df.columns else pd.Series(dtype="float64")
    carry_aligned = pd.Series(pnl["date"]).map(carry_signal).fillna(0.0)
    attribution = build_attribution(pnl_df=pnl, carry_signal=carry_aligned)

    positions_out = positions[
        [
            "date",
            "symbol",
            "signal_date",
            "signal_value",
            "target_position",
            "position",
            "daily_return",
            "is_roll_date",
        ]
    ].copy()

    validate_backtest_outputs(trades=trades, positions=positions_out, pnl=pnl, attribution=attribution)

    attribution_sum = (
        attribution["carry_roll_pnl"]
        + attribution["spot_curve_move_pnl"]
        + attribution["costs_pnl"]
        + attribution["convexity_proxy_pnl"]
        + attribution["residual_pnl"]
    )
    accounting_error = float((attribution["pnl_total"] - attribution_sum).abs().max()) if not attribution.empty else 0.0

    daily_returns = pnl["net_pnl"] / initial_capital
    sharpe = None
    if len(daily_returns) > 1 and daily_returns.std(ddof=0) > 0:
        sharpe = float(np.sqrt(252.0) * daily_returns.mean() / daily_returns.std(ddof=0))

    summary = {
        "config_snapshot": config,
        "metrics": {
            "initial_capital": initial_capital,
            "total_net_pnl": float(pnl["net_pnl"].sum()),
            "final_equity": float(pnl["equity"].iloc[-1]) if not pnl.empty else initial_capital,
            "total_cost": float(trades["total_cost"].sum()) if not trades.empty else 0.0,
            "turnover": float(trades["notional"].sum() / initial_capital) if not trades.empty else 0.0,
            "hit_rate": float((pnl["net_pnl"] > 0).mean()) if not pnl.empty else None,
            "sharpe": sharpe,
            "regular_trade_count": int((trades["trade_type"] == "rebalance").sum()) if not trades.empty else 0,
            "roll_trade_count": int((trades["trade_type"] == "roll").sum()) if not trades.empty else 0,
            "attribution_max_abs_error": accounting_error,
        },
    }

    return trades, positions_out, pnl, attribution, summary


def run_backtest_from_configs(backtest_config_path: str, data_config_path: str, signals_path: str | None = None) -> Dict[str, str]:
    bt_cfg = _load_yaml(backtest_config_path)
    data_cfg = _load_yaml(data_config_path)

    signals_df, continuous_df, roll_log_df = _read_stage_inputs(data_cfg, signals_path)

    primary_symbol = str(bt_cfg.get("backtest", {}).get("primary_symbol", "SPY"))
    price_col = str(bt_cfg.get("backtest", {}).get("price_column", "close"))
    market_df = _build_market_series(continuous_df=continuous_df, symbol=primary_symbol, price_column=price_col)

    trades, positions, pnl, attribution, summary = run_backtest(
        signals_df=signals_df,
        market_df=market_df,
        roll_log_df=roll_log_df,
        config=bt_cfg,
    )

    out_dir = Path(bt_cfg.get("paths", {}).get("output_dir", "outputs/backtests"))
    out_dir.mkdir(parents=True, exist_ok=True)

    trades_path = out_dir / "trades.parquet"
    positions_path = out_dir / "positions.parquet"
    pnl_path = out_dir / "pnl.parquet"
    attribution_path = out_dir / "attribution.parquet"
    summary_path = out_dir / "summary.json"

    trades.to_parquet(trades_path, index=False)
    positions.to_parquet(positions_path, index=False)
    pnl.to_parquet(pnl_path, index=False)
    attribution.to_parquet(attribution_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    LOGGER.info("Backtest artifacts written to %s", out_dir)

    return {
        "trades": str(trades_path),
        "positions": str(positions_path),
        "pnl": str(pnl_path),
        "attribution": str(attribution_path),
        "summary": str(summary_path),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Stage 5 backtest engine.")
    parser.add_argument("--backtest-config", default="config/backtest.yaml", help="Backtest config path")
    parser.add_argument("--data-config", default="config/data.yaml", help="Data config path")
    parser.add_argument("--signals", default=None, help="Optional explicit signals parquet path")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    run_backtest_from_configs(
        backtest_config_path=args.backtest_config,
        data_config_path=args.data_config,
        signals_path=args.signals,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
