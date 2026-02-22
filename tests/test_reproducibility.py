from __future__ import annotations

import json
import math
from pathlib import Path

from scripts.reproduce import run_pipeline


def test_reproducibility_smoke_pipeline_outputs_and_finite_metrics() -> None:
    manifest = run_pipeline(target="reproduce", force_refresh=True)

    assert "stages" in manifest
    assert (Path("outputs") / "run_manifest.json").exists()
    assert (Path("outputs") / "reports" / "latest_report.html").exists()

    summary = json.loads((Path("outputs") / "backtests" / "summary.json").read_text(encoding="utf-8"))
    risk = json.loads((Path("outputs") / "backtests" / "risk_metrics.json").read_text(encoding="utf-8"))

    total_net_pnl = summary["metrics"]["total_net_pnl"]
    final_equity = summary["metrics"]["final_equity"]
    var_value = risk["var_cvar"]["var"]
    cvar_value = risk["var_cvar"]["cvar"]

    for value in [total_net_pnl, final_equity, var_value, cvar_value]:
        assert value is not None
        assert math.isfinite(float(value))

    assert float(final_equity) > 0
    assert float(cvar_value) >= float(var_value)
