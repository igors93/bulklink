#!/usr/bin/env sh
set -eu

python -m ruff format --check .
python -m ruff check .
python -m mypy src
python -m pytest --cov=bulklink --cov-report=term-missing
python -m build
