# Volatility Relative Value Toolkit

A reproducible, production-style research toolkit for volatility relative value strategies (VIX / variance risk premium / term structure carry). The framework is RV logic, not directional prediction.

## Pipeline targets

```bash
make build-data
make build-signals
make run-backtest
make run-risk
make build-report
make reproduce
```

`make reproduce` executes the full pipeline and writes a run manifest.

## Local setup

```bash
python3 -m venv .venv
source .venv/bin/activate
make setup
```

## One-command reproducibility

```bash
make reproduce
```

Expected final artifacts:
- `outputs/run_manifest.json`
- `outputs/reports/latest_report.html`
- `outputs/reports/latest_report.pdf` (best effort)
- `outputs/backtests/*.parquet` + `outputs/backtests/*.json`
- `outputs/data/**` stage artifacts

## Reproducibility and cache behavior

- Cache controller: `config/data.yaml` -> `data.cache.force_refresh`
- `force_refresh: false`: reuse existing stage outputs when required files exist and config hashes match prior manifest
- `force_refresh: true`: rebuild all selected stages
- Manifest: `outputs/run_manifest.json` contains:
  - UTC timestamp
  - Python version
  - git commit hash (if available)
  - seed
  - config snapshots + SHA256 hashes
  - per-stage execution status (`executed` / `cache_hit`)

## Assumptions summary

- Data quality and fill actions are auditable in `outputs/data/qa/`
- Signals use explicit anti-leak lagging
- Backtest includes configurable commission/slippage/roll costs
- Roll trades are generated from Stage 3 roll log when roll events exist
- Risk metrics use loss-positive sign convention (`loss = -return`) for VaR/CVaR

## Artifact locations

- Data/QA: `outputs/data/`
- Signals: `outputs/data/signals.parquet`, `outputs/data/signal_diagnostics.json`
- Backtest: `outputs/backtests/trades.parquet`, `positions.parquet`, `pnl.parquet`, `attribution.parquet`, `summary.json`
- Risk: `outputs/backtests/risk_metrics.json`, `stress_report.parquet`, `exposures.parquet`
- Report: `outputs/reports/latest_report.html`, `outputs/reports/latest_report.pdf`

## Tests

```bash
make test
pytest -q tests/test_reproducibility.py
```

## Troubleshooting (Mac M2)

- `pyarrow` CPU/sysctl warnings in sandboxed environments are non-fatal for this project.
- Matplotlib may warn about cache directory permissions; set `MPLCONFIGDIR` to a writable path if needed.
- If PDF generation fails on your machine, HTML report is still produced and limitation is recorded in `outputs/reports/report_build.json`.
