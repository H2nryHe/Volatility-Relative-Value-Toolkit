#!/usr/bin/env python3
"""Minimal end-to-end RV demo pipeline.

This script does four things:
1) generate/read 2y daily demo VX near/far prices
2) compute two RV signals: slope + carry/roll-down proxy
3) run a minimal threshold backtest (long/short/flat)
4) output manifest + summary + simple HTML report
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def generate_demo_data(start: str, periods: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start=start, periods=periods)

    # Mean-reverting synthetic vol level with noise.
    level = np.empty(len(dates), dtype=float)
    level[0] = 20.0
    for i in range(1, len(dates)):
        shock = rng.normal(0.0, 0.7)
        level[i] = level[i - 1] + 0.05 * (20.0 - level[i - 1]) + shock
        level[i] = max(level[i], 8.0)

    term_premium = 1.2 + 0.4 * np.sin(np.linspace(0, 10 * np.pi, len(dates))) + rng.normal(0.0, 0.12, len(dates))
    vx1 = level + rng.normal(0.0, 0.25, len(dates))
    vx2 = vx1 + np.maximum(term_premium, 0.2)

    df = pd.DataFrame(
        {
            "date": dates,
            "vx1_close": vx1,
            "vx2_close": vx2,
        }
    )
    return df


def compute_signals(df: pd.DataFrame, z_window: int) -> pd.DataFrame:
    out = df.copy()
    out["signal_slope"] = (out["vx1_close"] - out["vx2_close"]) / out["vx2_close"]
    out["signal_carry_roll_down"] = (out["vx2_close"] - out["vx1_close"]) / out["vx1_close"]

    for col in ["signal_slope", "signal_carry_roll_down"]:
        mean = out[col].rolling(z_window, min_periods=z_window).mean()
        std = out[col].rolling(z_window, min_periods=z_window).std(ddof=0)
        out[f"z_{col}"] = (out[col] - mean) / std.replace(0.0, np.nan)

    out["signal_composite"] = 0.5 * out["z_signal_carry_roll_down"] - 0.5 * out["z_signal_slope"]
    return out


def run_backtest(df: pd.DataFrame, threshold: float) -> pd.DataFrame:
    bt = df.copy()

    bt["position_raw"] = 0
    bt.loc[bt["signal_composite"] > threshold, "position_raw"] = 1
    bt.loc[bt["signal_composite"] < -threshold, "position_raw"] = -1

    # Explicit anti-lookahead: use t signal, execute at t+1.
    bt["position"] = bt["position_raw"].shift(1).fillna(0).astype(int)
    bt["vx1_ret"] = bt["vx1_close"].pct_change().fillna(0.0)
    bt["strategy_ret"] = bt["position"] * bt["vx1_ret"]
    bt["equity"] = (1.0 + bt["strategy_ret"]).cumprod()
    bt["trade"] = bt["position"].diff().abs().fillna(abs(bt["position"]))

    return bt


def max_drawdown(equity: pd.Series) -> float:
    running_max = equity.cummax()
    dd = equity / running_max - 1.0
    return float(dd.min())


def build_summary(bt: pd.DataFrame) -> dict:
    ann_factor = np.sqrt(252.0)
    ret = bt["strategy_ret"].astype(float)

    total_return = float(bt["equity"].iloc[-1] - 1.0)
    ann_return = float((1.0 + total_return) ** (252.0 / max(len(bt), 1)) - 1.0)
    ann_vol = float(ret.std(ddof=0) * ann_factor)
    sharpe = float(ret.mean() / ret.std(ddof=0) * ann_factor) if ret.std(ddof=0) > 0 else None

    return {
        "rows": int(len(bt)),
        "start_date": str(bt["date"].min().date()),
        "end_date": str(bt["date"].max().date()),
        "total_return": total_return,
        "annualized_return": ann_return,
        "annualized_volatility": ann_vol,
        "sharpe": sharpe,
        "max_drawdown": max_drawdown(bt["equity"]),
        "hit_rate": float((ret > 0).mean()),
        "turnover": float(bt["trade"].sum()),
        "trades": int((bt["trade"] > 0).sum()),
    }


def write_charts(bt: pd.DataFrame, assets_dir: Path) -> dict:
    assets_dir.mkdir(parents=True, exist_ok=True)

    equity_path = assets_dir / "equity.png"
    signal_path = assets_dir / "signals.png"

    fig, ax = plt.subplots(figsize=(8, 3.5))
    ax.plot(bt["date"], bt["equity"], color="#1f77b4")
    ax.set_title("Demo Strategy Equity")
    ax.set_xlabel("Date")
    ax.set_ylabel("Equity")
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(equity_path, dpi=130)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 3.5))
    ax.plot(bt["date"], bt["signal_slope"], label="slope", color="#d62728")
    ax.plot(bt["date"], bt["signal_carry_roll_down"], label="carry/roll", color="#2ca02c")
    ax.plot(bt["date"], bt["signal_composite"], label="composite", color="#9467bd", alpha=0.7)
    ax.legend(loc="best")
    ax.set_title("RV Signals")
    ax.set_xlabel("Date")
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(signal_path, dpi=130)
    plt.close(fig)

    return {
        "equity": equity_path.name,
        "signals": signal_path.name,
    }


def write_html_report(output_dir: Path, summary: dict, charts: dict, params: dict) -> Path:
    html_path = output_dir / "report.html"
    html = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <title>RV Demo Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #1f2937; }}
    h1, h2 {{ color: #0f172a; }}
    table {{ border-collapse: collapse; width: 100%; max-width: 760px; }}
    th, td {{ border: 1px solid #d1d5db; padding: 8px; text-align: left; }}
    th {{ background: #f3f4f6; }}
    img {{ max-width: 900px; width: 100%; border: 1px solid #e5e7eb; margin-bottom: 14px; }}
    code {{ background: #f3f4f6; padding: 2px 4px; }}
  </style>
</head>
<body>
  <h1>RV Demo Report</h1>
  <p>Minimal reproducible demo using synthetic VX near/far prices, two RV signals, and threshold long/short/flat backtest.</p>

  <h2>Summary</h2>
  <table>
    <tr><th>Metric</th><th>Value</th></tr>
    <tr><td>Rows</td><td>{summary['rows']}</td></tr>
    <tr><td>Date Range</td><td>{summary['start_date']} to {summary['end_date']}</td></tr>
    <tr><td>Total Return</td><td>{summary['total_return']:.6f}</td></tr>
    <tr><td>Annualized Return</td><td>{summary['annualized_return']:.6f}</td></tr>
    <tr><td>Annualized Volatility</td><td>{summary['annualized_volatility']:.6f}</td></tr>
    <tr><td>Sharpe</td><td>{summary['sharpe'] if summary['sharpe'] is not None else 'N/A'}</td></tr>
    <tr><td>Max Drawdown</td><td>{summary['max_drawdown']:.6f}</td></tr>
    <tr><td>Hit Rate</td><td>{summary['hit_rate']:.6f}</td></tr>
    <tr><td>Turnover</td><td>{summary['turnover']:.2f}</td></tr>
    <tr><td>Trades</td><td>{summary['trades']}</td></tr>
  </table>

  <h2>Rules</h2>
  <p><code>signal_composite &gt; threshold</code> =&gt; long, <code>&lt; -threshold</code> =&gt; short, else flat. Position executes at next bar.</p>
  <p>Params: seed={params['seed']}, z_window={params['z_window']}, threshold={params['threshold']}</p>

  <h2>Charts</h2>
  <img src=\"assets/{charts['equity']}\" alt=\"equity\" />
  <img src=\"assets/{charts['signals']}\" alt=\"signals\" />
</body>
</html>
"""
    html_path.write_text(html, encoding="utf-8")
    return html_path


def write_manifest(output_dir: Path, summary: dict, params: dict, data_csv: Path, report_path: Path) -> Path:
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": platform.python_version(),
        "script": "scripts/reproduce_demo.py",
        "params": params,
        "inputs": {
            "demo_csv": str(data_csv),
            "demo_csv_sha256": hashlib.sha256(data_csv.read_bytes()).hexdigest(),
        },
        "outputs": {
            "summary_json": str(output_dir / "summary.json"),
            "report_html": str(report_path),
            "backtest_parquet": str(output_dir / "backtest.parquet"),
            "signals_parquet": str(output_dir / "signals.parquet"),
        },
        "summary": summary,
    }
    path = output_dir / "manifest.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="Minimal RV demo reproducibility script")
    parser.add_argument("--output-dir", default="outputs/demo", help="Demo output directory")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start", default="2023-01-02", help="Business-day start date")
    parser.add_argument("--periods", type=int, default=504, help="Business-day periods (~2 years)")
    parser.add_argument("--z-window", type=int, default=20)
    parser.add_argument("--threshold", type=float, default=0.5)
    parser.add_argument("--force-refresh", action="store_true")
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    assets_dir = out_dir / "assets"
    out_dir.mkdir(parents=True, exist_ok=True)

    data_csv = out_dir / "demo_vix_futures.csv"
    if data_csv.exists() and not args.force_refresh:
        data = pd.read_csv(data_csv, parse_dates=["date"])
    else:
        data = generate_demo_data(start=args.start, periods=args.periods, seed=args.seed)
        data.to_csv(data_csv, index=False)

    signals = compute_signals(data, z_window=args.z_window)
    backtest = run_backtest(signals, threshold=args.threshold)

    signals.to_parquet(out_dir / "signals.parquet", index=False)
    backtest.to_parquet(out_dir / "backtest.parquet", index=False)

    summary = build_summary(backtest)
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    charts = write_charts(backtest, assets_dir)
    report_path = write_html_report(
        output_dir=out_dir,
        summary=summary,
        charts=charts,
        params={"seed": args.seed, "z_window": args.z_window, "threshold": args.threshold},
    )

    manifest_path = write_manifest(
        output_dir=out_dir,
        summary=summary,
        params={
            "seed": args.seed,
            "start": args.start,
            "periods": args.periods,
            "z_window": args.z_window,
            "threshold": args.threshold,
            "force_refresh": args.force_refresh,
        },
        data_csv=data_csv,
        report_path=report_path,
    )

    print("RV demo complete")
    print(f" - data: {data_csv}")
    print(f" - signals: {out_dir / 'signals.parquet'}")
    print(f" - backtest: {out_dir / 'backtest.parquet'}")
    print(f" - summary: {out_dir / 'summary.json'}")
    print(f" - manifest: {manifest_path}")
    print(f" - report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
