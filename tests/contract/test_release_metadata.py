from __future__ import annotations

import re
from pathlib import Path

import bulklink

ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = ROOT / "pyproject.toml"


def test_project_and_runtime_versions_match() -> None:
    text = PYPROJECT.read_text(encoding="utf-8")
    match = re.search(r'^version = "([^"]+)"$', text, flags=re.MULTILINE)

    assert match is not None
    assert match.group(1) == bulklink.__version__


def test_distribution_metadata_declares_supported_contract() -> None:
    text = PYPROJECT.read_text(encoding="utf-8")

    assert 'requires-python = ">=3.10"' in text
    assert 'license = "MIT"' in text
    assert 'license-files = ["LICENSE"]' in text
    assert '"Programming Language :: Python :: 3.14"' in text
    assert '"Typing :: Typed"' in text
    assert (ROOT / "src/bulklink/py.typed").is_file()
