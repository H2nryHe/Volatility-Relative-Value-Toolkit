from __future__ import annotations

import importlib
from pathlib import Path

import yaml


def test_packages_import_cleanly() -> None:
    for module in ["data_pipeline", "signals", "backtest", "risk", "report"]:
        importlib.import_module(module)


def test_config_placeholders_parse() -> None:
    for path in Path("config").glob("*.yaml"):
        with path.open("r", encoding="utf-8") as handle:
            parsed = yaml.safe_load(handle)
        assert isinstance(parsed, dict), f"Config file must parse into mapping: {path}"
