# Volatility Relative Value Toolkit — Agent Execution Plan (PROJECT_SPEC)

## 0) Agent 工作方式（必须遵守）

### 全局目标
构建一个可复现的 `Volatility Relative Value Toolkit`（VIX / variance risk premium / term structure carry），支持：

- 数据拉取、清洗、对齐、换月规则
- 信号生成（term structure / carry / VRP / PCA）
- 回测（成本 / 风险目标 / 换月 / PnL attribution）
- 风险分析（VaR/CVaR/drawdown/exposure/stress）
- 一键生成 HTML/PDF 报告
- `make reproduce` 一条命令复现

---

## 1) Agent 执行规则（非常重要）

### 1.1 每个 stage 必须输出
- 修改的文件列表
- 运行命令
- 关键日志摘要
- 验收结果（Pass / Fail）
- 已知限制（Known limitations）

### 1.2 每个 stage 必须先做再说
先实现最小可运行版本（MVP），再增强，不要一次写完“完美版本”。

### 1.3 严禁行为
- 不允许把核心逻辑只放在 notebook
- 不允许 silently fill missing data（必须记录规则）
- 不允许使用未来数据（lookahead）
- 不允许只给净值图不解释回测假设

### 1.4 输出规范
- 数据中间结果统一写入 `outputs/`
- 配置统一从 `config/*.yaml` 读取
- 核心函数尽量纯函数（输入->输出明确）
- 所有脚本可通过命令行运行（CLI 或 `python -m ...`）

---

# 2) Stage 列表（Agent 可执行版）

---

## Stage 1 — Repo Scaffold & Reproducible Environment

### 目标
创建可运行的项目骨架、环境依赖、Makefile、测试框架。

### 输入
- 无（从零开始）

### 输出（必须生成）
- 目录结构（见下）
- `pyproject.toml`（或 `requirements.txt`）
- `Makefile`
- `README.md` 初版
- `config/` 占位配置
- `tests/` 基础测试（至少 1 个 smoke test）

### 必须创建的目录
```text
vol-rv-toolkit/
  data_pipeline/
  signals/
  backtest/
  risk/
  report/
  config/
  tests/
  outputs/
  notebooks/
```

### 建议命令（Makefile）
至少包含：
- `make setup`
- `make lint`
- `make test`
- `make reproduce`（可先 stub）
- `make clean`

### 验收检查（Agent 必须执行）
- [ ] `make test` 成功
- [ ] `make reproduce` 成功（即使当前只打印 stub 流程）
- [ ] `README.md` 写明项目目标和后续 stages
- [ ] 所有 Python 模块可 import，无路径错误

### 通过标准（Pass Criteria）
新机器 clone 后，按 README 操作可以跑通 `make test` 和 `make reproduce`。

### 若失败，优先修复
1. import/path 问题  
2. 依赖冲突  
3. Makefile 命令缺失  

---

## Stage 2 — Data Pipeline v1 (Load + Standardize)

### 目标
实现最小可用数据管线：拉取/读取数据并统一 schema，输出标准化 parquet。

### 输入
- 公开数据源（或本地 CSV）
- 配置文件 `config/data.yaml`

### 输出（必须生成）
- `outputs/data/raw/*.parquet`
- `outputs/data/standardized/*.parquet`
- `outputs/data/metadata/source_summary.json`

### Agent 必须实现模块
- `data_pipeline/loaders/`：读取原始数据（可以先支持 CSV + yfinance）
- `data_pipeline/standardize/`：字段映射与类型转换
- `data_pipeline/build_dataset.py`：执行入口

### 标准化 schema（至少）
- `date`
- `symbol`
- `asset_type`
- `open`, `high`, `low`, `close`, `volume`
- `source`
- `asof_timestamp`
- （期货可选）`contract_month`, `expiry`, `days_to_expiry`

### 验收检查（Agent 必须执行）
- [ ] 所有输出文件 schema 一致（列名 + dtypes）
- [ ] `date` 可排序、无明显格式错误
- [ ] `(date, symbol)` 重复记录检测并输出报告
- [ ] 价格字段无负值（若有，必须记录异常）
- [ ] 输出 coverage 摘要（每个 symbol 起止日期、行数、缺失率）

### 通过标准
可以稳定生成一份标准化主数据快照（parquet），且有 metadata/coverage 输出。

### Known limitations（必须记录）
例如：VX futures 暂时使用本地样例 CSV；尚未处理换月；尚未做异常值检测。

---

## Stage 3 — Data QA / Calendar Alignment / Roll Rules（核心）

### 目标
实现数据质量检查、交易日历对齐、VX 合约换月规则，并生成可审计日志。

### 输入
- Stage 2 标准化数据
- `config/data.yaml` 中 QA/roll 配置

### 输出（必须生成）
- `outputs/data/clean/*.parquet`
- `outputs/data/continuous/*.parquet`
- `outputs/data/qa/qa_report.json`
- `outputs/data/qa/missing_report.parquet`
- `outputs/data/qa/outlier_report.parquet`
- `outputs/data/qa/roll_log.parquet`

### Agent 必须实现功能
1. **交易日历对齐**
   - 对齐到统一交易日历
   - 区分 market-closed vs data-missing（至少预留字段）

2. **缺失值处理**
   - 对价格字段有限规则填充（必须可配置）
   - volume 默认不盲填
   - 所有填充行为写日志/标记列（如 `is_filled`）

3. **异常值检测**
   - z-score/MAD（二选一先实现）
   - 输出异常点，不要直接删除（删除规则另配置）

4. **换月规则（VX）**
   - 明确触发条件（如到期前 N 交易日）
   - 输出连续合约/主力合约序列
   - 输出 `roll_log`

### 验收检查（Agent 必须执行）
- [ ] QA 报告包含：缺失率、重复值、异常值数量
- [ ] 随机抽样 5~10 个 roll 事件，日志可读
- [ ] 连续序列无“断档”
- [ ] 无未来数据使用（roll 决策只用当日可得信息）
- [ ] 所有数据处理规则都出现在配置或代码注释中

### 通过标准
README 可以明确回答“数据质量怎么保证”，且有 QA 文件支撑。

### 失败优先级
若时间不够，优先保证：
1. roll rule 正确  
2. missing/outlier 报告存在  
3. 对齐逻辑可审计  

---

## Stage 4 — Signals (RV Logic, not Prediction)

### 目标
实现可解释的 volatility relative value 信号，并统一输出接口。

### 输入
- Stage 3 clean/continuous 数据
- `config/signals.yaml`

### 输出（必须生成）
- `outputs/data/signals.parquet`
- `outputs/data/signal_diagnostics.json`

### Agent 必须实现信号
1. `term_structure_slope`
2. `term_structure_curvature`
3. `carry_roll_down`（proxy 可接受）
4. `vrp_proxy = IV - RV`
5. `pca_factors`（term structure PCA）

### 接口要求
每个信号函数应：
- 接收 DataFrame + config
- 返回带 `date` 的 DataFrame
- 明确是否需要 `shift(1)` 防止泄漏

### 验收检查（Agent 必须执行）
- [ ] 每个信号有统计摘要（mean/std/min/max/missing）
- [ ] rolling 初始窗口缺失合理
- [ ] PCA 输出载荷与解释方差比
- [ ] 所有信号时序无 lookahead（代码中显式 `shift` 或注释说明）
- [ ] 信号结果可被 backtest 模块直接消费（字段命名统一）

### 通过标准
每个信号都有：定义 + 参数 + 输出字段 + 基础诊断结果。

---

## Stage 5 — Backtest Engine (Execution + Costs + Roll + Attribution)

### 目标
实现可配置回测引擎，支持真实研究流程中的关键假设，并输出 PnL attribution。

### 输入
- Stage 4 signals
- Stage 3 continuous / roll 数据
- `config/backtest.yaml`

### 输出（必须生成）
- `outputs/backtests/trades.parquet`
- `outputs/backtests/positions.parquet`
- `outputs/backtests/pnl.parquet`
- `outputs/backtests/attribution.parquet`
- `outputs/backtests/summary.json`

### Agent 必须实现功能
1. **执行时序**
   - 信号生成时点与执行时点明确（例如 t signal -> t+1 execution）
2. **交易成本**
   - 固定 bps 成本（MVP）
   - 滑点模型（可先简单）
3. **仓位约束**
   - 仓位上限
   - leverage cap（可选）
   - risk targeting（目标波动率）
4. **换月执行**
   - 调用 Stage 3 roll 规则
   - roll 成本单独计入
5. **PnL attribution**
   - 至少拆分：
     - carry / roll
     - spot/curve move
     - costs
     - residual
   - convexity proxy 可先做占位字段 + 文档说明

### 验收检查（Agent 必须执行）
- [ ] `positions` 与 `trades` 一致（持仓更新逻辑正确）
- [ ] `PnL_total ≈ attribution components sum`
- [ ] 成本设置为 0 与 非 0 结果有差异
- [ ] 仓位不超上限
- [ ] roll 日有明确交易/成本记录
- [ ] 无 lookahead（执行价格与信号时点不冲突）

### 通过标准
除了净值曲线，还能输出交易记录、仓位记录、PnL 分解和参数配置。

---

## Stage 6 — Risk Analytics (VaR/CVaR/Exposure/Stress)

### 目标
对策略结果做风险与暴露分析，输出结构化风险报告。

### 输入
- Stage 5 pnl / positions
- `config/risk.yaml`

### 输出（必须生成）
- `outputs/backtests/risk_metrics.json`
- `outputs/backtests/stress_report.parquet`
- `outputs/backtests/exposures.parquet`

### Agent 必须实现
1. `VaR / CVaR`（历史法，95/99 至少一个）
2. `drawdown`（MaxDD, duration）
3. `exposures`（beta / vega proxy / gamma proxy）
4. `stress`（预定义窗口，如 crisis windows）

### 验收检查（Agent 必须执行）
- [ ] CVaR 与 VaR 关系合理
- [ ] MaxDD 与净值曲线一致
- [ ] stress 报告可按窗口输出汇总
- [ ] exposure 时间序列可与日期对齐

### 通过标准
可以回答“策略怕什么市场、在哪些 regime 表现差”。

---

## Stage 7 — Report Generator (HTML/PDF Dashboard)

### 目标
每次运行后自动生成 HTML/PDF dashboard，包含关键假设与结果。

### 输入
- Stage 5/6 所有输出
- `config/report.yaml`

### 输出（必须生成）
- `outputs/reports/latest_report.html`
- `outputs/reports/latest_report.pdf`（若环境允许；否则至少 HTML 并记录限制）

### 报告必须包含（强制）
1. 策略/Repo简介（RV逻辑，不是预测）
2. 数据质量摘要（QA结果）
3. 回测假设（成本/滑点/换月）
4. 结果表（Sharpe, MaxDD, turnover, hit-rate, PnL 分解）
5. 风险分析摘要（VaR/CVaR/DD/Stress）
6. 图表（净值、回撤、attribution、风险暴露等）

### 验收检查（Agent 必须执行）
- [ ] HTML 成功生成
- [ ] 报告包含上述 6 类内容
- [ ] 图表标题/时间轴/单位清晰
- [ ] 报告标注样本区间与配置版本

### 通过标准
非开发者打开 `latest_report.html` 就能看懂策略逻辑和结果可信度。

---

## Stage 8 — Reproducibility Hardening (`make reproduce`)

### 目标
把整个流程串起来，一条命令复现，并加基础回归测试。

### 输入
- Stage 1~7 所有模块

### 输出（必须生成）
- `make reproduce` 全流程可运行
- `outputs/reports/latest_report.html`（最终产物）
- `outputs/run_manifest.json`（记录配置、版本、时间戳）
- `tests/test_reproducibility.py`

### `make reproduce` 建议步骤（强制按顺序）
1. `build-data`
2. `build-signals`
3. `run-backtest`
4. `run-risk`
5. `build-report`

### Agent 必须实现的稳定性机制
- 配置快照保存到 `run_manifest.json`
- 数据缓存（避免重复下载）
- 固定随机种子（如果存在随机步骤）
- 最小 smoke test（小样本快速跑）

### 验收检查（Agent 必须执行）
- [ ] `make reproduce` 从头执行成功
- [ ] 关键输出文件均存在
- [ ] 失败时错误信息可定位
- [ ] 重复运行结果在允许误差内稳定（或完全一致）

### 通过标准
README 中的 `make reproduce` 命令和实际行为一致。

---

# 3) Agent 每个 Stage 的输出模板（直接复用）

你可以要求 agent 每次都按这个格式回复：

```markdown
## Stage X Completed: <name>

### Files Changed
- path/to/file1.py
- path/to/file2.yaml
- ...

### What Was Implemented
- ...
- ...

### Commands Run
- make test
- python -m data_pipeline.build_dataset --config config/data.yaml
- ...

### Validation Results
- [PASS] ...
- [PASS] ...
- [FAIL] ... (if any)

### Output Artifacts
- outputs/data/...
- outputs/backtests/...

### Known Limitations
- ...
- ...

### Next Recommended Step
- Stage X+1 ...
```

---

# 4) Agent 的“优先级决策规则”（避免卡死）

如果 agent 在某 stage 卡住，按以下优先级降级：

### P0（必须保证）
- 流程可跑通
- 输入输出清晰
- 无未来函数
- 有日志/报告/结果落地

### P1（优先做）
- QA 报告完整
- 回测假设透明
- PnL attribution 可解释

### P2（可以后补）
- 更复杂成本模型
- 更精细 exposure proxy
- 更漂亮的 dashboard 样式
- PDF 导出兼容性增强

---

# 5) 你可以直接给 Agent 的启动指令（可复制）

下面这段你可以直接贴给 coding agent：

```text
Build this repository in stages. Follow the stage plan strictly.

Rules:
1) Implement the minimum viable version for each stage first, then improve.
2) After each stage, run validation checks and report PASS/FAIL.
3) Write all intermediate outputs to outputs/.
4) Do not put core logic only in notebooks.
5) Avoid lookahead bias. Be explicit about signal timestamp vs execution timestamp.
6) Log data cleaning/roll rules decisions so they are auditable.
7) Keep configs in config/*.yaml and make pipeline reproducible via `make reproduce`.

Start with Stage 1 (Repo Scaffold & Reproducible Environment).
Return:
- files changed
- commands run
- validation results
- output artifacts
- known limitations
- next step
```

---

# 6) 额外建议（让 agent 更稳）

如果你是用 Claude Code / Cursor / OpenAI agent 类工具，建议你再加两条约束：

1. **每次只做一个 stage，不要跨 stage**  
避免 agent 一口气乱改太多，难排错。

2. **先写测试/检查脚本，再写实现（至少对 Stage 3/5）**  
尤其是 roll rules 和 backtest 时序，先写 check 更不容易埋雷。

---

## 文件名建议
将本文保存为：`PROJECT_SPEC.md`

## 使用方式建议
- 作为 agent 的主指令文件放在 repo 根目录
- 同时配一个 `TASK_BOARD.md`（按 Stage 拆分为更小任务）
- 每完成一个 stage，在 PR 描述中引用对应验收项
