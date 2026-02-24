.PHONY: setup lint test clean build-data build-signals run-backtest run-risk build-report reproduce

PYTHON ?= python3

setup:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e ".[dev]"

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

test:
	$(PYTHON) -m pytest -q

clean:
	find . -name "__pycache__" -type d -prune -exec rm -rf {} +
	find . -name "*.pyc" -delete

build-data:
	$(PYTHON) -m scripts.reproduce --target build-data

build-signals: build-data
	$(PYTHON) -m scripts.reproduce --target build-signals

run-backtest: build-signals
	$(PYTHON) -m scripts.reproduce --target run-backtest

run-risk: run-backtest
	$(PYTHON) -m scripts.reproduce --target run-risk

build-report: run-risk
	$(PYTHON) -m scripts.reproduce --target build-report

reproduce: build-report
	$(PYTHON) -m scripts.reproduce --target reproduce
