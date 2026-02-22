# TASK_BOARD.md — Volatility Relative Value Toolkit

> Companion task board for `PROJECT_SPEC.md`  
> Purpose: break each stage into small, agent-executable tasks with dependencies, outputs, and checks.

---

## How to use this board

- Work **top-down** by Stage.
- Complete **one Stage at a time** unless explicitly approved to parallelize.
- For each task:
  - Mark status (`[ ]` -> `[x]`)
  - Record file changes
  - Run listed checks
  - Save outputs to `outputs/`
- If blocked, document blocker and downgrade to MVP per `PROJECT_SPEC.md` priority rules.

### Status Legend
- `[ ]` Not started
- `[-]` In progress
- `[x]` Done
- `[!]` Blocked
- `[~]` Partial / MVP only

---

# Stage 1 — Repo Scaffold & Reproducible Environment

## Goal
Create runnable project scaffold, dependency management, Makefile, and test harness.

## Exit Criteria (DoD)
- `make test` passes
- `make reproduce` runs successfully (stub allowed)
- README includes project goal + stage roadmap
- Modules import cleanly

### S1.1 Create repository skeleton
- [x] Create directories: `data_pipeline/`, `signals/`, `backtest/`, `risk/`, `report/`, `config/`, `tests/`, `outputs/`, `notebooks/`
- [x] Add `__init__.py` files to Python packages
- [x] Add `.gitignore` for Python/data outputs
- [x] Add `outputs/.gitkeep` (optional)

**Outputs**
- Repo directory structure exists

**Checks**
- [x] `find . -maxdepth 2 -type d` shows required folders
- [x] `python -c "import data_pipeline, signals, backtest, risk, report"`

---

### S1.2 Set up Python project metadata
- [x] Add `pyproject.toml` (preferred) or `requirements.txt`
- [x] Pin core dependencies (pandas, numpy, pyarrow, pytest, pyyaml, scikit-learn, matplotlib, etc.)
- [x] Add dev dependencies (ruff/black optional but recommended)

**Outputs**
- `pyproject.toml` or `requirements*.txt`

**Checks**
- [~] Fresh env install succeeds
- [ ] `python -c "import pandas, numpy, pyarrow, pytest"`

---

### S1.3 Create Makefile commands (MVP)
- [x] `make setup`
- [x] `make test`
- [x] `make lint` (can be no-op initially)
- [x] `make clean`
- [x] `make reproduce` (stub pipeline)

**Outputs**
- `Makefile`

**Checks**
- [x] `make test` returns 0
- [x] `make reproduce` returns 0 and prints stage placeholders

---

### S1.4 Add config placeholders
- [x] Create `config/data.yaml`
- [x] Create `config/signals.yaml`
- [x] Create `config/backtest.yaml`
- [x] Create `config/risk.yaml`
- [x] Create `config/report.yaml`

**Outputs**
- YAML placeholder configs with comments

**Checks**
- [x] YAML files parse: `python -c "import yaml,glob; [yaml.safe_load(open(f)) for f in glob.glob('config/*.yaml')]"` 

---

### S1.5 Add tests + smoke test
- [x] Create `tests/test_smoke.py`
- [x] Add at least 1 passing test
- [x] Add test command in Makefile

**Outputs**
- `tests/test_smoke.py`

**Checks**
- [x] `pytest -q`

---

### S1.6 README initial version
- [x] Project summary (RV toolkit, not prediction)
- [x] Stage roadmap
- [x] Local setup instructions (Mac M2)
- [x] How to run `make test` / `make reproduce`

**Outputs**
- `README.md` initial draft

**Checks**
- [x] README includes exact command examples
- [~] New user can follow setup steps end-to-end

---

# Stage 2 — Data Pipeline v1 (Load + Standardize)

## Goal
Build a minimal data pipeline that ingests raw data and produces standardized parquet outputs.

## Exit Criteria (DoD)
- Standardized parquet snapshot generated
- Coverage/metadata report produced
- Schema consistent and basic sanity checks performed

### S2.1 Define data schema contract
- [x] Create `data_pipeline/schema.py` (or constants module)
- [x] Define required standardized columns
- [x] Document dtypes and nullable rules
- [x] Define validation helper (`validate_standardized_schema(df)`)

**Outputs**
- Schema contract module

**Checks**
- [x] Validation helper passes on toy DataFrame
- [x] Fails on missing required column

---

### S2.2 Implement raw loaders (MVP)
- [x] Create `data_pipeline/loaders/` package
- [x] Implement CSV loader utility
- [x] Implement optional yfinance loader stub (or real if available)
- [x] Add source config parsing from `config/data.yaml`

**Outputs**
- Raw loader modules
- Optional sample raw data fixtures

**Checks**
- [x] Loader returns DataFrame with source-native columns
- [x] Error handling for missing file/path

---

### S2.3 Implement standardization transforms
- [x] Create `data_pipeline/standardize/` package
- [x] Map source columns -> canonical columns
- [x] Normalize dates / timezone handling
- [x] Cast dtypes
- [x] Add `source` and `asof_timestamp`

**Outputs**
- Standardization functions

**Checks**
- [x] Standardized output matches schema contract
- [x] Dates sortable and parse correctly

---

### S2.4 Build pipeline entrypoint
- [x] Implement `data_pipeline/build_dataset.py`
- [x] Read config and run loaders + standardizers
- [x] Write `outputs/data/raw/*.parquet`
- [x] Write `outputs/data/standardized/*.parquet`

**Outputs**
- Raw and standardized parquet files

**Checks**
- [x] Script runs from CLI
- [x] Output folders created automatically
- [x] Parquet files readable

---

### S2.5 Metadata / coverage reporting
- [x] Generate `outputs/data/metadata/source_summary.json`
- [x] Include rows, symbols, date ranges, missing counts
- [x] Add duplicate key check on `(date, symbol)` and report counts

**Outputs**
- Source summary JSON
- Duplicate report (JSON/CSV/parquet acceptable)

**Checks**
- [x] Metadata file exists and valid JSON
- [x] Duplicate detection runs even when no duplicates

---

### S2.6 Data sanity checks (MVP)
- [x] Negative price check
- [x] Zero-heavy field check (warning only)
- [x] Null rate summary by column

**Outputs**
- Sanity check logs / summary report

**Checks**
- [x] Pipeline does not silently ignore anomalies
- [x] Warnings are visible and persisted

---

# Stage 3 — Data QA / Calendar Alignment / Roll Rules

## Goal
Add auditable QA, trading calendar alignment, and VX roll rule logic.

## Exit Criteria (DoD)
- QA reports generated
- Continuous/rolled series produced
- Roll log generated and inspectable
- No lookahead in roll decision logic

### S3.1 Implement trading calendar alignment
- [x] Create `data_pipeline/calendars/` utilities
- [x] Define target trading calendar (config-driven)
- [x] Align per-symbol data to calendar
- [x] Mark `is_market_closed` vs `is_data_missing` (if source allows; otherwise placeholder flags)

**Outputs**
- Aligned datasets
- Missing classification flags

**Checks**
- [x] Aligned index is monotonic and calendar-consistent
- [x] Missing flags populated (or explicitly unavailable)

---

### S3.2 Missing data handling (configurable)
- [x] Implement fill rules for price fields (limited, config-based)
- [x] Prevent blind fill for volume by default
- [x] Add fill audit columns/logs (`is_filled_close`, etc.)

**Outputs**
- Cleaned dataset with fill markers
- Fill audit log/report

**Checks**
- [x] Fill actions counted and reported
- [x] Volume untouched unless config explicitly enables

---

### S3.3 Outlier detection
- [x] Implement z-score or MAD-based outlier detection (MVP)
- [x] Output outlier records without deleting by default
- [x] Add optional config for handling policy (mark-only/drop/winsorize later)

**Outputs**
- `outputs/data/qa/outlier_report.parquet`

**Checks**
- [x] Outlier report produced even if empty
- [x] No silent deletion in MVP

---

### S3.4 Implement VX roll rule engine
- [x] Create `data_pipeline/rolls/`
- [x] Implement rule: roll before expiry by configurable N trading days (MVP)
- [x] Produce selected active contract series
- [x] Generate `roll_log` with `date/from_contract/to_contract/reason`

**Outputs**
- Continuous/active-contract dataset
- `outputs/data/qa/roll_log.parquet`

**Checks**
- [x] Roll log non-empty when multiple contracts exist
- [x] No contract overlap inconsistency
- [x] Roll decision uses only same-day available metadata

---

### S3.5 Build QA reports
- [x] Generate `qa_report.json` (missing/duplicates/outliers summary)
- [x] Generate `missing_report.parquet`
- [x] Write clean and continuous outputs

**Outputs**
- `outputs/data/qa/qa_report.json`
- `outputs/data/qa/missing_report.parquet`
- `outputs/data/clean/*.parquet`
- `outputs/data/continuous/*.parquet`

**Checks**
- [x] All expected files exist
- [x] QA report readable and references generated artifacts

---

### S3.6 Add tests for calendar + roll logic
- [x] Unit test: alignment preserves chronological order
- [x] Unit test: roll rule triggers on expected dates
- [x] Unit test: no lookahead in roll selection

**Outputs**
- `tests/test_roll_rules.py`
- `tests/test_calendar_alignment.py`

**Checks**
- [x] `pytest -q tests/test_roll_rules.py`
- [x] Edge-case fixtures included

---

# Stage 4 — Signals (RV Logic, Not Prediction)

## Goal
Implement interpretable volatility RV signals and unified signal outputs.

## Exit Criteria (DoD)
- Signals parquet generated
- Diagnostics summary generated
- Signal interfaces standardized
- No lookahead in feature computation

### S4.1 Define signal interface contract
- [x] Create `signals/base.py` (or equivalent)
- [x] Define expected input/output columns
- [x] Establish naming conventions (`signal_*`, `z_*`)
- [x] Add helper for lagging/anti-leakage (`apply_signal_lag`)

**Outputs**
- Signal interface utilities

**Checks**
- [x] Two toy signals conform to same schema

---

### S4.2 Implement term structure slope signal
- [x] `signals/term_structure.py::compute_slope`
- [x] Configurable contract pairs / maturities
- [x] Optional rolling z-score normalization

**Outputs**
- Slope signal columns

**Checks**
- [x] Values generated for valid dates
- [x] Missing values only where expected (insufficient inputs)

---

### S4.3 Implement curvature signal
- [x] `signals/term_structure.py::compute_curvature`
- [x] Define 3-point curve formula
- [x] Optional normalization

**Outputs**
- Curvature signal columns

**Checks**
- [x] Formula documented in code comments/docstring
- [x] Curvature computed only when required tenors available

---

### S4.4 Implement carry / roll-down proxy
- [x] `signals/carry_roll.py`
- [x] Define carry proxy and roll-down approximation
- [x] Make assumptions configurable and documented

**Outputs**
- Carry / roll-down signal columns

**Checks**
- [x] Signal names and units documented
- [x] No forward-looking fields used

---

### S4.5 Implement VRP proxy (IV - RV)
- [x] `signals/vrp_proxy.py`
- [x] IV proxy from VIX (or configured source)
- [x] RV proxy from realized vol window (configurable)
- [x] Annualization convention documented

**Outputs**
- VRP proxy column(s)

**Checks**
- [x] Rolling window shift/lag is explicit
- [x] Initial NaNs are expected and reported

---

### S4.6 Implement PCA factors
- [x] `signals/pca_factors.py`
- [x] Fit PCA on term structure matrix
- [x] Output factor scores + explained variance ratios
- [x] Save loadings metadata/artifact

**Outputs**
- PCA factor columns
- PCA diagnostics metadata

**Checks**
- [x] Explained variance ratio sums to <= 1
- [x] Loadings dimensions correct

---

### S4.7 Signal orchestration + diagnostics
- [x] `signals/signal_registry.py` or pipeline entrypoint
- [x] Produce `outputs/data/signals.parquet`
- [x] Produce `outputs/data/signal_diagnostics.json` (stats summary, missing rates)
- [x] Ensure downstream-compatible field names

**Outputs**
- Signals parquet
- Diagnostics JSON

**Checks**
- [x] Each configured signal appears in output
- [x] Diagnostics include mean/std/min/max/missing per signal
- [x] Backtest module can read output without renaming

---

### S4.8 Signal tests
- [x] Unit tests for slope/curvature formulas
- [x] Unit tests for VRP lagging/no-leakage
- [x] Unit tests for PCA output dimensions

**Outputs**
- `tests/test_signals.py`

**Checks**
- [x] `pytest -q tests/test_signals.py`

---

# Stage 5 — Backtest Engine (Execution + Costs + Roll + Attribution)

## Goal
Implement configurable backtest engine with explicit execution assumptions and PnL attribution.

## Exit Criteria (DoD)
- Trades, positions, pnl, attribution files generated
- Cost assumptions configurable and visible
- Roll execution handled
- Basic accounting consistency checks pass

### S5.1 Define backtest data contract
- [x] Create `backtest/contracts.py` (optional)
- [x] Define schemas for trades / positions / pnl / attribution
- [x] Define required signal input fields

**Outputs**
- Backtest schema contract

**Checks**
- [x] Validation helpers pass/fail correctly on toy data

---

### S5.2 Implement execution timing model
- [x] `backtest/execution.py`
- [x] Make signal timestamp vs execution timestamp explicit
- [x] MVP execution price rule (e.g., next-period close/open proxy)
- [x] Anti-lookahead assertions

**Outputs**
- Execution model functions
- Timing assumptions documented in code + config

**Checks**
- [x] Unit test fails if same-bar future data is used
- [x] Execution lag configurable

---

### S5.3 Implement position sizing & constraints
- [x] `backtest/positioning.py`
- [x] Signal -> target position mapping
- [x] Position cap
- [x] Optional leverage cap
- [x] Risk targeting (target vol, MVP acceptable)

**Outputs**
- Target and realized position series

**Checks**
- [x] Positions respect caps
- [x] Risk targeting can be disabled/enabled by config

---

### S5.4 Implement transaction costs & slippage (MVP)
- [x] Fixed bps cost model
- [x] Slippage proxy (optional simple model)
- [x] Separate roll costs vs regular trade costs

**Outputs**
- Cost columns in trades/pnl

**Checks**
- [x] Zero-cost config changes net PnL materially (vs non-zero config)
- [x] Costs never silently omitted

---

### S5.5 Implement roll-aware trade generation
- [x] Integrate Stage 3 roll outputs
- [x] Generate roll trades on roll dates
- [x] Carry forward positions across contracts correctly

**Outputs**
- Roll events reflected in trades/positions

**Checks**
- [x] Roll dates in trades match `roll_log`
- [x] No “ghost positions” in expired contracts

---

### S5.6 Implement PnL attribution (MVP + residual)
- [x] `backtest/attribution.py`
- [x] Attribute PnL into carry/roll, spot-curve move, costs, residual
- [x] Add `convexity_proxy_pnl` placeholder column if full implementation deferred

**Outputs**
- `outputs/backtests/attribution.parquet`

**Checks**
- [x] `PnL_total` approximately equals sum of attribution components
- [x] Residual is reported (not hidden)

---

### S5.7 Orchestrate backtest run
- [x] `backtest/engine.py`
- [x] Read config, signals, continuous data
- [x] Write trades/positions/pnl/attribution/summary outputs
- [x] Include parameter snapshot in summary

**Outputs**
- `outputs/backtests/trades.parquet`
- `outputs/backtests/positions.parquet`
- `outputs/backtests/pnl.parquet`
- `outputs/backtests/attribution.parquet`
- `outputs/backtests/summary.json`

**Checks**
- [x] All files created
- [x] Summary includes turnover/hit-rate/sharpe placeholders or computed values

---

### S5.8 Backtest tests
- [x] Unit test for position/trade accounting consistency
- [x] Unit test for cost application
- [x] Unit test for no-lookahead execution timing
- [x] Unit test for attribution summation identity

**Outputs**
- `tests/test_backtest.py`

**Checks**
- [x] `pytest -q tests/test_backtest.py`

---

# Stage 6 — Risk Analytics (VaR/CVaR/Exposure/Stress)

## Goal
Generate risk metrics and regime/stress diagnostics from backtest outputs.

## Exit Criteria (DoD)
- Risk metrics JSON generated
- Stress report generated
- Exposure series generated and aligned by date

### S6.1 Implement drawdown analytics
- [x] `risk/drawdown.py`
- [x] MaxDD, drawdown series, duration, recovery time (MVP)
- [x] Utility to attach to report pipeline

**Outputs**
- Drawdown metrics and series

**Checks**
- [x] MaxDD consistent with equity curve in toy test

---

### S6.2 Implement VaR/CVaR (historical)
- [x] `risk/var_cvar.py`
- [x] 95% and/or 99% historical VaR/CVaR
- [x] Configurable horizon (MVP = 1 day)

**Outputs**
- VaR/CVaR metrics for risk report

**Checks**
- [x] CVaR magnitude >= VaR magnitude (loss convention documented)

---

### S6.3 Implement exposures (proxy-based)
- [x] `risk/exposures.py`
- [x] Beta proxy (e.g., to SPX/returns)
- [x] Vega proxy (e.g., sensitivity to VIX level moves)
- [x] Gamma proxy (nonlinear proxy or placeholder with documented formula)

**Outputs**
- `outputs/backtests/exposures.parquet`

**Checks**
- [x] Exposure series indexed by date
- [x] Column names/units documented

---

### S6.4 Implement stress/regime analysis
- [x] `risk/stress.py`
- [x] Predefined windows from config (e.g., crisis windows)
- [x] Performance/risk stats by window
- [x] Optional rolling regime summaries

**Outputs**
- `outputs/backtests/stress_report.parquet`

**Checks**
- [x] Stress report generated even for partial overlap windows
- [x] Window labels preserved

---

### S6.5 Orchestrate risk pipeline
- [x] Risk entrypoint (`risk/run_risk.py` or module function)
- [x] Read pnl/positions inputs
- [x] Write `risk_metrics.json`, `stress_report.parquet`, `exposures.parquet`

**Outputs**
- Risk artifacts

**Checks**
- [x] Artifacts exist
- [x] JSON is valid and includes core metrics

---

### S6.6 Risk tests
- [x] Unit test for drawdown
- [x] Unit test for VaR/CVaR ordering
- [x] Unit test for stress window slicing

**Outputs**
- `tests/test_risk.py`

**Checks**
- [x] `pytest -q tests/test_risk.py`

---

# Stage 7 — Report Generator (HTML/PDF Dashboard)

## Goal
Auto-generate HTML/PDF dashboard after pipeline runs, including assumptions and key metrics.

## Exit Criteria (DoD)
- HTML report generated
- PDF report generated if environment supports it (else documented fallback)
- Required content sections included

### S7.1 Define report data model
- [x] Enumerate required inputs from Stage 3/5/6
- [x] Build report context assembler
- [x] Add config version/sample window metadata

**Outputs**
- Report context object/dict builder

**Checks**
- [x] Missing required artifact yields actionable error

---

### S7.2 Implement metrics tables and summaries
- [x] Compute/display Sharpe, MaxDD, turnover, hit-rate
- [x] Include PnL attribution summary table
- [x] Include QA summary and backtest assumptions section

**Outputs**
- Report tables data structures

**Checks**
- [x] Required metrics present (placeholder allowed if documented)

---

### S7.3 Generate plots
- [x] Equity curve
- [x] Drawdown curve
- [x] Attribution chart
- [x] Exposure chart(s)
- [x] Optional signal diagnostics plots

**Outputs**
- Plot images or embedded charts in report build directory

**Checks**
- [x] Titles, axes, units, sample dates visible
- [x] Plots render without manual notebook steps

---

### S7.4 Build HTML report
- [x] `report/dashboard.py` + template(s)
- [x] Render `outputs/reports/latest_report.html`
- [x] Include links/paths to generated artifacts if helpful

**Outputs**
- HTML report

**Checks**
- [x] HTML opens locally and sections are present
- [x] No broken image references

---

### S7.5 Build PDF export (best effort)
- [x] Implement PDF export path (WeasyPrint / wkhtmltopdf / reportlab fallback)
- [x] If unsupported, document fallback and still pass Stage with HTML-only + logged limitation

**Outputs**
- `outputs/reports/latest_report.pdf` (or documented fallback)

**Checks**
- [x] PDF file exists OR limitation is clearly recorded in logs/readme

---

### S7.6 Report tests / content validation
- [x] Add content validation checks (required headings/sections in HTML)
- [x] Smoke test for report generation with minimal artifacts

**Outputs**
- `tests/test_report.py`

**Checks**
- [x] `pytest -q tests/test_report.py`

---

# Stage 8 — Reproducibility Hardening (`make reproduce`)

## Goal
Wire all stages into one reproducible command and add regression/smoke tests.

## Exit Criteria (DoD)
- `make reproduce` runs full pipeline from scratch (or cached data path)
- `run_manifest.json` saved
- Reproducibility smoke test passes
- README matches actual commands

### S8.1 Create end-to-end pipeline commands
- [x] Add Makefile targets:
  - `build-data`
  - `build-signals`
  - `run-backtest`
  - `run-risk`
  - `build-report`
  - `reproduce` (depends on all above)
- [x] Ensure commands call scripts/modules with config paths

**Outputs**
- Finalized Makefile workflow

**Checks**
- [x] Running each target individually works
- [x] `make reproduce` executes in correct order

---

### S8.2 Add run manifest + config snapshotting
- [x] Write `outputs/run_manifest.json`
- [x] Include timestamp, git commit hash (if available), config file hashes/contents, Python version
- [x] Save random seed and key parameters

**Outputs**
- `outputs/run_manifest.json`

**Checks**
- [x] Manifest valid JSON
- [x] Config snapshot present and readable

---

### S8.3 Add caching strategy (MVP)
- [x] Avoid re-downloading data when raw cache exists (configurable `force_refresh`)
- [x] Log cache hits/misses
- [x] Keep cache behavior deterministic

**Outputs**
- Cache behavior documented and implemented

**Checks**
- [x] Repeat run uses cache when expected
- [x] Force refresh path works

---

### S8.4 Add reproducibility smoke test
- [x] `tests/test_reproducibility.py`
- [x] Run small-sample pipeline
- [x] Assert output files exist
- [x] Assert key metrics are finite and within broad ranges (not exact equality)

**Outputs**
- Reproducibility smoke test

**Checks**
- [x] `pytest -q tests/test_reproducibility.py`

---

### S8.5 README final hardening
- [x] Add exact `make reproduce` command
- [x] Document assumptions (data quality, costs/slippage/roll)
- [x] Add results table example and artifact locations
- [x] Add troubleshooting section for Mac M2 / PDF export issues

**Outputs**
- Final README

**Checks**
- [x] README commands match Makefile exactly
- [x] New user can reproduce report with documented steps

---

# Cross-Stage QA Checklist (run before major merges)

- [ ] No core logic only in notebooks
- [ ] Config-driven parameters (not hardcoded magic numbers)
- [ ] No lookahead in signals/backtest/roll logic
- [ ] Data cleaning and fill actions are auditable
- [ ] Outputs written to `outputs/` with stable paths
- [ ] Stage artifacts are reusable by later stages
- [ ] README and `PROJECT_SPEC.md` remain aligned

---

# Optional Parallelization Map (only after Stage 1 is stable)

> If using multiple agents, split by module boundaries and merge carefully.

## Safe-ish parallel work after Stage 2
- Agent A: Stage 3 QA + calendar alignment
- Agent B: Stage 3 roll rule engine
- Agent C: Signal interface + term structure signals (Stage 4)

## Safe-ish parallel work after Stage 4
- Agent A: Backtest execution/positioning
- Agent B: Backtest attribution
- Agent C: Risk analytics scaffolding
- Agent D: Report templates + chart rendering

**Merge guardrails**
- Shared schema contracts must be finalized first
- Use consistent column names from contracts
- Run full test suite after each merge

---

# Per-Task Execution Note Template (copy into agent replies)

```markdown
Task: <Task ID / Name>
Status: [x] Done / [~] Partial / [!] Blocked

Files changed:
- ...

Commands run:
- ...

Validation:
- [PASS] ...
- [FAIL] ...

Artifacts:
- ...

Blockers / limitations:
- ...

Next task:
- ...
```

---

# Suggested First Execution Order (practical)

1. S1.1 → S1.6
2. S2.1 → S2.6
3. S3.1 → S3.6
4. S4.1 → S4.8
5. S5.1 → S5.8
6. S6.1 → S6.6
7. S7.1 → S7.6
8. S8.1 → S8.5

---

## File name
Save this file as: `TASK_BOARD.md`
