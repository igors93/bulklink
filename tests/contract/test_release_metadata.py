from __future__ import annotations

import re
from pathlib import Path

import bulklink

ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = ROOT / "pyproject.toml"
CHANGELOG = ROOT / "CHANGELOG.md"
CI_WORKFLOW = ROOT / ".github/workflows/ci.yml"
RELEASE_WORKFLOW = ROOT / ".github/workflows/release.yml"
PUBLIC_CONTRACT = ROOT / "docs/reference/public-contract.md"


def _project_version() -> str:
    text = PYPROJECT.read_text(encoding="utf-8")
    match = re.search(r'^version = "([^"]+)"$', text, flags=re.MULTILINE)
    assert match is not None
    return match.group(1)


def test_project_and_runtime_versions_match() -> None:
    assert _project_version() == bulklink.__version__


def test_stable_release_version_and_contract_document_are_present() -> None:
    assert _project_version() == "0.3.0"
    assert PUBLIC_CONTRACT.is_file()
    assert "Stable `0.3.x` contract" in (ROOT / "docs/reference/compatibility.md").read_text(
        encoding="utf-8"
    )


def test_changelog_contains_current_version() -> None:
    version = _project_version()
    text = CHANGELOG.read_text(encoding="utf-8")
    pattern = rf"^## {re.escape(version)} - \d{{4}}-\d{{2}}-\d{{2}}$"

    assert re.search(pattern, text, flags=re.MULTILINE) is not None


def test_distribution_metadata_declares_supported_contract() -> None:
    text = PYPROJECT.read_text(encoding="utf-8")

    assert 'requires-python = ">=3.10"' in text
    assert 'license = "MIT"' in text
    assert 'license-files = ["LICENSE"]' in text
    assert '"Development Status :: 4 - Beta"' in text
    assert '"Programming Language :: Python :: 3.14"' in text
    assert '"Typing :: Typed"' in text
    assert (ROOT / "src/bulklink/py.typed").is_file()


def test_ci_covers_supported_versions_platforms_and_release_verification() -> None:
    text = CI_WORKFLOW.read_text(encoding="utf-8")

    for version in ("3.10", "3.11", "3.12", "3.13", "3.14"):
        assert f'"{version}"' in text

    assert "windows-latest" in text
    assert "macos-latest" in text
    assert "python -m benchmarks.run" in text
    assert "python scripts/verify_release.py" in text
    assert "actions/upload-artifact@v4" in text
    assert "platforms" in text
    assert "compatibility" in text


def test_release_workflow_uses_verified_artifacts_and_trusted_publishing() -> None:
    text = RELEASE_WORKFLOW.read_text(encoding="utf-8")

    assert 'tags:\n      - "v*"' in text
    assert "BULKLINK_RELEASE_TAG" in text
    assert "./scripts/ci.sh" in text
    assert "needs: build" in text
    assert "actions/download-artifact@v4" in text
    assert "pypa/gh-action-pypi-publish@release/v1" in text
    assert "name: pypi" in text
    assert "id-token: write" in text
    assert "password:" not in text
    assert "PYPI_TOKEN" not in text
    assert "TWINE_PASSWORD" not in text
