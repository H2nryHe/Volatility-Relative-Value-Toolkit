"""End-to-end reproducibility orchestrator with stage caching and run manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import random
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List

import numpy as np
import yaml

from backtest.engine import run_backtest_from_configs
from data_pipeline.build_dataset import run_pipeline as run_stage2
from data_pipeline.run_stage3 import run_stage3
from report.dashboard import build_report
from risk.run_risk import run_risk_pipeline
from signals.signal_registry import build_signals

ROOT = Path(__file__).resolve().parents[1]
OUTPUTS_DIR = ROOT / "outputs"
MANIFEST_PATH = OUTPUTS_DIR / "run_manifest.json"


@dataclass
class StageSpec:
    name: str
    command_name: str
    outputs: List[Path]
    config_paths: List[Path]
    runner: Callable[[], Any]


def _read_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Expected YAML mapping: {path}")
    return data


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _snapshot_configs(config_paths: List[Path]) -> Dict[str, Dict[str, Any]]:
    snapshot: Dict[str, Dict[str, Any]] = {}
    for path in config_paths:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        snapshot[str(path.relative_to(ROOT))] = {
            "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "content": text,
        }
    return snapshot


def _git_commit_hash() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip() or None
    except Exception:
        return None


def _load_previous_manifest() -> Dict[str, Any]:
    if not MANIFEST_PATH.exists():
        return {}
    try:
        return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _has_outputs(paths: List[Path]) -> bool:
    return all(path.exists() for path in paths)


def _stage_cache_hit(
    stage: StageSpec,
    previous_manifest: Dict[str, Any],
    force_refresh: bool,
    upstream_reran: bool,
) -> bool:
    if force_refresh or upstream_reran:
        return False
    if not _has_outputs(stage.outputs):
        return False

    previous_stages = previous_manifest.get("stages", {})
    prev = previous_stages.get(stage.command_name, {})
    prev_config_hashes = prev.get("config_hashes", {})

    current_hashes = {
        str(path.relative_to(ROOT)): _sha256_file(path)
        for path in stage.config_paths
        if path.exists()
    }
    return bool(prev) and prev_config_hashes == current_hashes


def _build_stage_specs() -> List[StageSpec]:
    cfg_data = ROOT / "config" / "data.yaml"
    cfg_signals = ROOT / "config" / "signals.yaml"
    cfg_backtest = ROOT / "config" / "backtest.yaml"
    cfg_risk = ROOT / "config" / "risk.yaml"
    cfg_report = ROOT / "config" / "report.yaml"

    return [
        StageSpec(
            name="Stage 2+3 Data Build",
            command_name="build-data",
            outputs=[
                ROOT / "outputs" / "data" / "standardized" / "sample_market_csv.parquet",
                ROOT / "outputs" / "data" / "qa" / "qa_report.json",
            ],
            config_paths=[cfg_data],
            runner=lambda: (run_stage2(str(cfg_data)), run_stage3(str(cfg_data))),
        ),
        StageSpec(
            name="Stage 4 Signals",
            command_name="build-signals",
            outputs=[ROOT / "outputs" / "data" / "signals.parquet"],
            config_paths=[cfg_data, cfg_signals],
            runner=lambda: build_signals(str(cfg_signals), str(cfg_data)),
        ),
        StageSpec(
            name="Stage 5 Backtest",
            command_name="run-backtest",
            outputs=[
                ROOT / "outputs" / "backtests" / "trades.parquet",
                ROOT / "outputs" / "backtests" / "summary.json",
            ],
            config_paths=[cfg_data, cfg_backtest],
            runner=lambda: run_backtest_from_configs(str(cfg_backtest), str(cfg_data), None),
        ),
        StageSpec(
            name="Stage 6 Risk",
            command_name="run-risk",
            outputs=[
                ROOT / "outputs" / "backtests" / "risk_metrics.json",
                ROOT / "outputs" / "backtests" / "stress_report.parquet",
                ROOT / "outputs" / "backtests" / "exposures.parquet",
            ],
            config_paths=[cfg_risk],
            runner=lambda: run_risk_pipeline(str(cfg_risk), "outputs/backtests"),
        ),
        StageSpec(
            name="Stage 7 Report",
            command_name="build-report",
            outputs=[ROOT / "outputs" / "reports" / "latest_report.html"],
            config_paths=[cfg_report],
            runner=lambda: build_report(str(cfg_report)),
        ),
    ]


def _update_manifest(
    stage_results: Dict[str, Dict[str, Any]],
    force_refresh: bool,
    selected_target: str,
) -> Dict[str, Any]:
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    config_paths = sorted((ROOT / "config").glob("*.yaml"))

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": platform.python_version(),
        "git_commit_hash": _git_commit_hash(),
        "seed": 42,
        "force_refresh": force_refresh,
        "selected_target": selected_target,
        "config_snapshots": _snapshot_configs(config_paths),
        "stages": stage_results,
    }
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return manifest


def run_pipeline(target: str = "reproduce", force_refresh: bool = False) -> Dict[str, Any]:
    random.seed(42)
    np.random.seed(42)

    stage_specs = _build_stage_specs()
    order = [s.command_name for s in stage_specs]
    if target not in [*order, "reproduce"]:
        raise ValueError(f"Unknown target '{target}'. Expected one of: {order + ['reproduce']}")

    if target == "reproduce":
        selected = stage_specs
    else:
        selected = stage_specs[: order.index(target) + 1]

    data_cfg = _read_yaml(ROOT / "config" / "data.yaml")
    force_refresh_cfg = bool(data_cfg.get("data", {}).get("cache", {}).get("force_refresh", False))
    effective_force_refresh = force_refresh or force_refresh_cfg

    previous_manifest = _load_previous_manifest()

    print("Volatility Relative Value Toolkit :: make reproduce")
    print(f"Target: {target} | force_refresh={effective_force_refresh}")

    stage_results: Dict[str, Dict[str, Any]] = {}
    upstream_reran = False

    for stage in selected:
        cache_hit = _stage_cache_hit(
            stage=stage,
            previous_manifest=previous_manifest,
            force_refresh=effective_force_refresh,
            upstream_reran=upstream_reran,
        )

        config_hashes = {
            str(path.relative_to(ROOT)): _sha256_file(path)
            for path in stage.config_paths
            if path.exists()
        }

        if cache_hit:
            print(f" - [{stage.command_name}] cache hit")
            status = "cache_hit"
            ran = False
        else:
            print(f" - [{stage.command_name}] running")
            stage.runner()
            status = "executed"
            ran = True
            upstream_reran = True

        stage_results[stage.command_name] = {
            "name": stage.name,
            "status": status,
            "ran": ran,
            "outputs": [str(p.relative_to(ROOT)) for p in stage.outputs],
            "outputs_exist": _has_outputs(stage.outputs),
            "config_hashes": config_hashes,
        }

    manifest = _update_manifest(
        stage_results=stage_results,
        force_refresh=effective_force_refresh,
        selected_target=target,
    )

    print("Reproduce summary:")
    for key, item in manifest["stages"].items():
        print(f"   {key}: {item['status']}")
    print(f"Manifest: {MANIFEST_PATH.relative_to(ROOT)}")

    return manifest


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run staged reproducibility workflow")
    parser.add_argument(
        "--target",
        default="reproduce",
        choices=["build-data", "build-signals", "run-backtest", "run-risk", "build-report", "reproduce"],
        help="Pipeline target to execute",
    )
    parser.add_argument("--force-refresh", action="store_true", help="Ignore cache and rebuild selected stages")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    run_pipeline(target=args.target, force_refresh=args.force_refresh)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
