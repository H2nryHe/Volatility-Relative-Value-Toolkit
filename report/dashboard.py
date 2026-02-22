"""Stage 7 dashboard report builder (HTML required, PDF best effort)."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import matplotlib.pyplot as plt
import pandas as pd
import yaml
from matplotlib.backends.backend_pdf import PdfPages

LOGGER = logging.getLogger("report.dashboard")

REQUIRED_SECTIONS = [
    "RV logic",
    "Data QA summary",
    "Backtest assumptions",
    "Results table",
    "PnL attribution",
    "Risk summary",
    "Charts",
]


def _load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    if not isinstance(cfg, dict):
        raise ValueError(f"Config must be a mapping: {path}")
    return cfg


def _require_file(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact '{label}': {path}")
    return path


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def assemble_report_context(report_cfg: Dict[str, Any]) -> Dict[str, Any]:
    paths = report_cfg.get("paths", {})

    qa_report_path = _require_file(Path(paths.get("qa_report", "outputs/data/qa/qa_report.json")), "qa_report")
    bt_summary_path = _require_file(Path(paths.get("backtest_summary", "outputs/backtests/summary.json")), "backtest_summary")
    pnl_path = _require_file(Path(paths.get("pnl", "outputs/backtests/pnl.parquet")), "pnl")
    attr_path = _require_file(Path(paths.get("attribution", "outputs/backtests/attribution.parquet")), "attribution")
    risk_metrics_path = _require_file(Path(paths.get("risk_metrics", "outputs/backtests/risk_metrics.json")), "risk_metrics")
    stress_path = _require_file(Path(paths.get("stress_report", "outputs/backtests/stress_report.parquet")), "stress_report")
    exposures_path = _require_file(Path(paths.get("exposures", "outputs/backtests/exposures.parquet")), "exposures")

    qa = _read_json(qa_report_path)
    bt_summary = _read_json(bt_summary_path)
    risk_metrics = _read_json(risk_metrics_path)

    pnl = pd.read_parquet(pnl_path).sort_values("date")
    attribution = pd.read_parquet(attr_path).sort_values("date")
    stress = pd.read_parquet(stress_path)
    exposures = pd.read_parquet(exposures_path).sort_values("date")

    pnl["date"] = pd.to_datetime(pnl["date"])
    attribution["date"] = pd.to_datetime(attribution["date"])
    exposures["date"] = pd.to_datetime(exposures["date"])

    results_table = {
        "Sharpe": bt_summary.get("metrics", {}).get("sharpe"),
        "MaxDD": risk_metrics.get("drawdown", {}).get("max_drawdown"),
        "Turnover": bt_summary.get("metrics", {}).get("turnover"),
        "Hit Rate": bt_summary.get("metrics", {}).get("hit_rate"),
        "Total Net PnL": bt_summary.get("metrics", {}).get("total_net_pnl"),
    }

    attribution_summary = {
        "carry_roll_pnl": float(attribution["carry_roll_pnl"].sum()) if not attribution.empty else 0.0,
        "spot_curve_move_pnl": float(attribution["spot_curve_move_pnl"].sum()) if not attribution.empty else 0.0,
        "costs_pnl": float(attribution["costs_pnl"].sum()) if not attribution.empty else 0.0,
        "convexity_proxy_pnl": float(attribution["convexity_proxy_pnl"].sum()) if not attribution.empty else 0.0,
        "residual_pnl": float(attribution["residual_pnl"].sum()) if not attribution.empty else 0.0,
    }

    context = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "title": report_cfg.get("report", {}).get("title", "Volatility Relative Value Toolkit Report"),
        "author": report_cfg.get("branding", {}).get("author", "Research"),
        "rv_logic": report_cfg.get("sections", {}).get(
            "rv_logic",
            "Relative-value framework using term structure, carry/roll-down, VRP proxy, and PCA factors; this is not a directional prediction model.",
        ),
        "qa": qa,
        "backtest_summary": bt_summary,
        "risk_metrics": risk_metrics,
        "results_table": results_table,
        "attribution_summary": attribution_summary,
        "stress": stress,
        "pnl": pnl,
        "attribution": attribution,
        "exposures": exposures,
        "assumptions": report_cfg.get("assumptions", {}),
    }
    return context


def _save_equity_chart(pnl: pd.DataFrame, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 3.8))
    ax.plot(pnl["date"], pnl["equity"], label="Equity", color="#1f77b4")
    ax.set_title("Equity Curve")
    ax.set_xlabel("Date")
    ax.set_ylabel("Equity")
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(path, dpi=140)
    plt.close(fig)


def _save_drawdown_chart(pnl: pd.DataFrame, path: Path) -> None:
    equity = pnl["equity"]
    running_max = equity.cummax()
    drawdown = (equity / running_max) - 1.0

    fig, ax = plt.subplots(figsize=(8, 3.8))
    ax.fill_between(pnl["date"], drawdown, 0.0, color="#d62728", alpha=0.35)
    ax.plot(pnl["date"], drawdown, color="#d62728", linewidth=1.2)
    ax.set_title("Drawdown Curve")
    ax.set_xlabel("Date")
    ax.set_ylabel("Drawdown")
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(path, dpi=140)
    plt.close(fig)


def _save_attribution_chart(attribution: pd.DataFrame, path: Path) -> None:
    grouped = attribution.groupby("date")[["carry_roll_pnl", "spot_curve_move_pnl", "costs_pnl", "residual_pnl"]].sum()

    fig, ax = plt.subplots(figsize=(8, 3.8))
    grouped.plot(kind="bar", stacked=True, ax=ax, width=0.8)
    ax.set_title("PnL Attribution by Date")
    ax.set_xlabel("Date")
    ax.set_ylabel("PnL")
    ax.grid(axis="y", alpha=0.2)
    fig.tight_layout()
    fig.savefig(path, dpi=140)
    plt.close(fig)


def _save_exposures_chart(exposures: pd.DataFrame, path: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 3.8))
    for col, color in [("beta_proxy", "#9467bd"), ("vega_proxy", "#2ca02c"), ("gamma_proxy", "#ff7f0e")]:
        if col in exposures.columns:
            ax.plot(exposures["date"], exposures[col], label=col, linewidth=1.6, color=color)
    ax.set_title("Exposure Proxies")
    ax.set_xlabel("Date")
    ax.set_ylabel("Exposure")
    ax.legend(loc="best")
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(path, dpi=140)
    plt.close(fig)


def generate_charts(context: Dict[str, Any], output_dir: Path) -> Dict[str, str]:
    assets_dir = output_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    equity_path = assets_dir / "equity_curve.png"
    drawdown_path = assets_dir / "drawdown_curve.png"
    attribution_path = assets_dir / "attribution_chart.png"
    exposures_path = assets_dir / "exposures_chart.png"

    _save_equity_chart(context["pnl"], equity_path)
    _save_drawdown_chart(context["pnl"], drawdown_path)
    _save_attribution_chart(context["attribution"], attribution_path)
    _save_exposures_chart(context["exposures"], exposures_path)

    return {
        "equity": str(equity_path.relative_to(output_dir)),
        "drawdown": str(drawdown_path.relative_to(output_dir)),
        "attribution": str(attribution_path.relative_to(output_dir)),
        "exposures": str(exposures_path.relative_to(output_dir)),
    }


def _fmt(value: Any) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def render_html(context: Dict[str, Any], charts: Dict[str, str], output_path: Path) -> None:
    qa_summary = context["qa"].get("summary", {})
    metrics = context["backtest_summary"].get("metrics", {})
    risk = context["risk_metrics"]

    html = f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <title>{context['title']}</title>
  <style>
    body {{ font-family: 'Georgia', serif; margin: 24px; color: #14213d; }}
    h1, h2 {{ color: #0b2545; }}
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 18px; }}
    th, td {{ border: 1px solid #d9d9d9; padding: 8px; text-align: left; }}
    th {{ background: #f2f5f9; }}
    .meta {{ color: #4b5563; font-size: 0.92rem; }}
    .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
    img {{ max-width: 100%; border: 1px solid #ddd; }}
    .section {{ margin-top: 22px; }}
    code {{ background: #f7f7f7; padding: 2px 4px; }}
  </style>
</head>
<body>
  <h1>{context['title']}</h1>
  <p class=\"meta\">Generated: {context['generated_at_utc']} | Author: {context['author']}</p>

  <h2>RV logic</h2>
  <p>{context['rv_logic']}</p>

  <div class=\"section\">
    <h2>Data QA summary</h2>
    <table>
      <tr><th>Metric</th><th>Value</th></tr>
      <tr><td>Sources</td><td>{qa_summary.get('sources')}</td></tr>
      <tr><td>Outlier rows</td><td>{qa_summary.get('outlier_rows')}</td></tr>
      <tr><td>Missing rows filled</td><td>{qa_summary.get('missing_rows_filled')}</td></tr>
      <tr><td>Roll events</td><td>{qa_summary.get('roll_events')}</td></tr>
      <tr><td>Duplicate rows (clean)</td><td>{qa_summary.get('duplicate_rows_clean')}</td></tr>
    </table>
  </div>

  <div class=\"section\">
    <h2>Backtest assumptions</h2>
    <p>Costs/slippage/roll parameters are config-driven and recorded in <code>outputs/backtests/summary.json</code>.</p>
    <table>
      <tr><th>Assumption</th><th>Value</th></tr>
      <tr><td>Commission bps</td><td>{_fmt(context['backtest_summary']['config_snapshot']['costs'].get('commission_bps'))}</td></tr>
      <tr><td>Slippage bps</td><td>{_fmt(context['backtest_summary']['config_snapshot']['costs'].get('slippage_bps'))}</td></tr>
      <tr><td>Roll cost bps</td><td>{_fmt(context['backtest_summary']['config_snapshot']['costs'].get('roll_cost_bps'))}</td></tr>
      <tr><td>Execution lag days</td><td>{_fmt(context['backtest_summary']['config_snapshot']['backtest'].get('signal_execution_lag_days'))}</td></tr>
    </table>
  </div>

  <div class=\"section\">
    <h2>Results table</h2>
    <table>
      <tr><th>Metric</th><th>Value</th></tr>
      <tr><td>Sharpe</td><td>{_fmt(context['results_table']['Sharpe'])}</td></tr>
      <tr><td>MaxDD</td><td>{_fmt(context['results_table']['MaxDD'])}</td></tr>
      <tr><td>Turnover</td><td>{_fmt(context['results_table']['Turnover'])}</td></tr>
      <tr><td>Hit-rate</td><td>{_fmt(context['results_table']['Hit Rate'])}</td></tr>
      <tr><td>Total Net PnL</td><td>{_fmt(context['results_table']['Total Net PnL'])}</td></tr>
    </table>
  </div>

  <div class=\"section\">
    <h2>PnL attribution</h2>
    <table>
      <tr><th>Component</th><th>Total</th></tr>
      <tr><td>carry/roll</td><td>{_fmt(context['attribution_summary']['carry_roll_pnl'])}</td></tr>
      <tr><td>spot/curve move</td><td>{_fmt(context['attribution_summary']['spot_curve_move_pnl'])}</td></tr>
      <tr><td>costs</td><td>{_fmt(context['attribution_summary']['costs_pnl'])}</td></tr>
      <tr><td>convexity proxy</td><td>{_fmt(context['attribution_summary']['convexity_proxy_pnl'])}</td></tr>
      <tr><td>residual</td><td>{_fmt(context['attribution_summary']['residual_pnl'])}</td></tr>
    </table>
  </div>

  <div class=\"section\">
    <h2>Risk summary</h2>
    <table>
      <tr><th>Metric</th><th>Value</th></tr>
      <tr><td>VaR</td><td>{_fmt(risk['var_cvar']['var'])}</td></tr>
      <tr><td>CVaR</td><td>{_fmt(risk['var_cvar']['cvar'])}</td></tr>
      <tr><td>Max drawdown</td><td>{_fmt(risk['drawdown']['max_drawdown'])}</td></tr>
      <tr><td>Drawdown duration</td><td>{_fmt(risk['drawdown']['max_drawdown_duration'])}</td></tr>
      <tr><td>Stress windows</td><td>{len(context['stress'])}</td></tr>
    </table>
  </div>

  <div class=\"section\">
    <h2>Charts</h2>
    <div class=\"grid\">
      <div><h3>Equity</h3><img src=\"{charts['equity']}\" alt=\"equity chart\"/></div>
      <div><h3>Drawdown</h3><img src=\"{charts['drawdown']}\" alt=\"drawdown chart\"/></div>
      <div><h3>Attribution</h3><img src=\"{charts['attribution']}\" alt=\"attribution chart\"/></div>
      <div><h3>Exposures</h3><img src=\"{charts['exposures']}\" alt=\"exposures chart\"/></div>
    </div>
  </div>
</body>
</html>
"""
    output_path.write_text(html, encoding="utf-8")


def render_pdf_best_effort(context: Dict[str, Any], charts: Dict[str, str], output_dir: Path) -> Dict[str, Any]:
    pdf_path = output_dir / "latest_report.pdf"
    limitation = None
    try:
        with PdfPages(pdf_path) as pdf:
            fig, ax = plt.subplots(figsize=(11, 8.5))
            ax.axis("off")
            ax.text(0.01, 0.95, context["title"], fontsize=16, fontweight="bold")
            ax.text(0.01, 0.90, "RV logic (not prediction)", fontsize=12)
            ax.text(0.01, 0.86, context["rv_logic"], fontsize=10, wrap=True)
            ax.text(0.01, 0.78, f"Sharpe: {_fmt(context['results_table']['Sharpe'])}")
            ax.text(0.01, 0.75, f"MaxDD: {_fmt(context['results_table']['MaxDD'])}")
            ax.text(0.01, 0.72, f"Turnover: {_fmt(context['results_table']['Turnover'])}")
            pdf.savefig(fig)
            plt.close(fig)

            for key in ["equity", "drawdown", "attribution", "exposures"]:
                fig, ax = plt.subplots(figsize=(11, 8.5))
                ax.axis("off")
                img = plt.imread(output_dir / charts[key])
                ax.imshow(img)
                ax.set_title(key.capitalize())
                pdf.savefig(fig)
                plt.close(fig)

        return {"pdf_generated": True, "pdf_path": str(pdf_path), "limitation": limitation}
    except Exception as exc:  # best effort
        limitation = f"PDF export failed: {exc}"
        LOGGER.warning(limitation)
        return {"pdf_generated": False, "pdf_path": str(pdf_path), "limitation": limitation}


def build_report(report_config_path: str) -> Dict[str, Any]:
    cfg = _load_yaml(report_config_path)
    out_dir = Path(cfg.get("report", {}).get("output_dir", "outputs/reports"))
    out_dir.mkdir(parents=True, exist_ok=True)

    context = assemble_report_context(cfg)
    charts = generate_charts(context, out_dir)

    html_path = out_dir / "latest_report.html"
    render_html(context, charts, html_path)

    pdf_info = render_pdf_best_effort(context, charts, out_dir)

    build_info = {
        "html_path": str(html_path),
        "charts": charts,
        **pdf_info,
    }

    meta_path = out_dir / "report_build.json"
    meta_path.write_text(json.dumps(build_info, indent=2, sort_keys=True), encoding="utf-8")
    LOGGER.info("Report HTML written to %s", html_path)
    if pdf_info["pdf_generated"]:
        LOGGER.info("Report PDF written to %s", pdf_info["pdf_path"])
    else:
        LOGGER.warning("Report PDF not generated. Limitation recorded in report_build.json")

    return build_info


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Stage 7 dashboard report")
    parser.add_argument("--report-config", default="config/report.yaml", help="Report config path")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    build_report(args.report_config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
