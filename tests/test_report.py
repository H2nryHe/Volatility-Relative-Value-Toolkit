from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from report.dashboard import REQUIRED_SECTIONS, build_report


def test_report_contains_required_sections_and_assets() -> None:
    result = build_report("config/report.yaml")
    html_path = Path(result["html_path"])
    assert html_path.exists()

    html = html_path.read_text(encoding="utf-8")
    for section in REQUIRED_SECTIONS:
        assert section in html

    for rel in result["charts"].values():
        assert (html_path.parent / rel).exists()


def test_missing_artifact_raises_actionable_error() -> None:
    with tempfile.TemporaryDirectory() as td:
        cfg = Path(td) / "report.yaml"
        cfg.write_text(
            """
report:
  title: test
  output_dir: outputs/reports
paths:
  qa_report: missing/qa.json
  backtest_summary: missing/summary.json
  pnl: missing/pnl.parquet
  attribution: missing/attr.parquet
  risk_metrics: missing/risk.json
  stress_report: missing/stress.parquet
  exposures: missing/exposures.parquet
""",
            encoding="utf-8",
        )

        with pytest.raises(FileNotFoundError, match="Missing required artifact"):
            build_report(str(cfg))
