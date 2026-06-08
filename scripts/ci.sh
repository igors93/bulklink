#!/usr/bin/env sh
set -eu

rm -rf build dist src/*.egg-info
python -m ruff format --check .
python -m ruff check .
python -m mypy src
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest \
  -p pytest_asyncio.plugin \
  -p pytest_cov.plugin \
  --cov=bulklink \
  --cov-report=term-missing
python -m benchmarks.run --iterations 200 --rounds 1 --waiters 100
python -m build
python scripts/verify_release.py
