#!/usr/bin/env python3
"""Verify built distributions from the perspective of an installed consumer."""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tarfile
import tempfile
import venv
import zipfile
from email.parser import BytesParser
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"
TYPING_CONFIG = ROOT / "tests/typing/mypy.ini"
TYPING_CONSUMER = ROOT / "tests/typing/consumer.py"
CHANGELOG = ROOT / "CHANGELOG.md"
_RELEASE_TAG_ENV = "BULKLINK_RELEASE_TAG"

_REQUIRED_WHEEL_FILES = {
    "bulklink/__init__.py",
    "bulklink/bulkhead.py",
    "bulklink/py.typed",
    "bulklink/registry.py",
}
_REQUIRED_SDIST_SUFFIXES = {
    "LICENSE",
    "README.md",
    "pyproject.toml",
    "src/bulklink/py.typed",
}


def _single_artifact(pattern: str) -> Path:
    artifacts = sorted(DIST.glob(pattern))
    if len(artifacts) != 1:
        raise RuntimeError(f"expected one {pattern!r} artifact, found {len(artifacts)}")
    return artifacts[0]


def _declared_version() -> str:
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r'^version = "([^"]+)"$', text, flags=re.MULTILINE)
    if match is None:
        raise RuntimeError("project version is missing from pyproject.toml")
    return match.group(1)


def _runtime_version() -> str:
    text = (ROOT / "src/bulklink/__init__.py").read_text(encoding="utf-8")
    match = re.search(r'^__version__ = "([^"]+)"$', text, flags=re.MULTILINE)
    if match is None:
        raise RuntimeError("runtime version is missing from bulklink.__init__")
    return match.group(1)


def _verify_changelog(version: str) -> None:
    text = CHANGELOG.read_text(encoding="utf-8")
    pattern = rf"^## {re.escape(version)} - \d{{4}}-\d{{2}}-\d{{2}}$"
    if re.search(pattern, text, flags=re.MULTILINE) is None:
        raise RuntimeError(f"changelog is missing a dated section for {version}")


def _verify_release_tag(version: str) -> None:
    tag = os.environ.get(_RELEASE_TAG_ENV)
    if tag is None:
        return

    expected = f"v{version}"
    if tag != expected:
        raise RuntimeError(f"release tag {tag!r} does not match expected tag {expected!r}")


def _verify_safe_archive_names(names: list[str]) -> None:
    for name in names:
        path = PurePosixPath(name)
        if path.is_absolute() or ".." in path.parts:
            raise RuntimeError(f"unsafe archive path: {name}")
        if "__pycache__" in path.parts or name.endswith((".pyc", ".pyo")):
            raise RuntimeError(f"cache file included in distribution: {name}")


def _verify_wheel(wheel: Path, version: str) -> None:
    if not wheel.name.endswith("-py3-none-any.whl"):
        raise RuntimeError(f"wheel is not platform independent: {wheel.name}")

    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        _verify_safe_archive_names(names)
        missing = sorted(_REQUIRED_WHEEL_FILES.difference(names))
        if missing:
            raise RuntimeError(f"wheel is missing required files: {missing}")
        if any(name.startswith("tests/") for name in names):
            raise RuntimeError("wheel unexpectedly contains the test suite")

        metadata_names = [name for name in names if name.endswith(".dist-info/METADATA")]
        if len(metadata_names) != 1:
            raise RuntimeError("wheel must contain exactly one METADATA file")
        metadata = BytesParser().parsebytes(archive.read(metadata_names[0]))

    if metadata["Name"] != "bulklink":
        raise RuntimeError("wheel project name is not bulklink")
    if metadata["Version"] != version:
        raise RuntimeError("wheel version does not match the source version")
    if metadata["Requires-Python"] != ">=3.10":
        raise RuntimeError("wheel has an unexpected Requires-Python value")
    if metadata["License-Expression"] != "MIT":
        raise RuntimeError("wheel is missing the MIT SPDX license expression")
    if "LICENSE" not in (metadata.get_all("License-File") or []):
        raise RuntimeError("wheel metadata does not reference LICENSE")


def _verify_sdist(sdist: Path) -> None:
    with tarfile.open(sdist, mode="r:gz") as archive:
        members = archive.getmembers()
        names = [member.name for member in members]
        _verify_safe_archive_names(names)
        if any(member.issym() or member.islnk() for member in members):
            raise RuntimeError("sdist unexpectedly contains symbolic or hard links")

    suffixes = {name.split("/", 1)[-1] for name in names if "/" in name}
    missing = sorted(_REQUIRED_SDIST_SUFFIXES.difference(suffixes))
    if missing:
        raise RuntimeError(f"sdist is missing required files: {missing}")


def _venv_python(environment: Path) -> Path:
    if os.name == "nt":
        return environment / "Scripts/python.exe"
    return environment / "bin/python"


def _clean_environment() -> dict[str, str]:
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    environment.pop("PYTHONHOME", None)
    return environment


def _run(command: list[str], *, cwd: Path, environment: dict[str, str]) -> None:
    subprocess.run(command, cwd=cwd, env=environment, check=True)


def _verify_installed_wheel(wheel: Path, version: str) -> None:
    smoke = f"""from __future__ import annotations

import asyncio

import bulklink
from bulklink import AsyncBulkhead, BulkheadEventKind, BulkheadRegistry

assert bulklink.__version__ == {version!r}


async def main() -> None:
    events = []
    gate = AsyncBulkhead(label="installed", parallelism=1, waiting_room=1)
    gate.add_event_handler(events.append)
    assert await gate.execute(asyncio.sleep, 0, result="ok") == "ok"
    await gate.resize(2)
    assert (await gate.status()).parallelism == 2
    assert (await gate.capacity_report()).status.label == "installed"
    await gate.close_and_wait()
    assert events[0].kind is BulkheadEventKind.ADMITTED

    registry = BulkheadRegistry()
    registry.create("member", parallelism=1)
    await registry.close_and_wait()
    assert (await registry.statuses())[0].is_drained


asyncio.run(main())
"""

    with tempfile.TemporaryDirectory(prefix="bulklink-release-") as temporary:
        directory = Path(temporary)
        environment_path = directory / "venv"
        venv.EnvBuilder(with_pip=True, clear=True).create(environment_path)
        python = _venv_python(environment_path)
        environment = _clean_environment()

        _run(
            [
                str(python),
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "--no-index",
                "--no-deps",
                str(wheel.resolve()),
            ],
            cwd=directory,
            environment=environment,
        )

        smoke_path = directory / "smoke.py"
        smoke_path.write_text(smoke, encoding="utf-8")
        _run([str(python), str(smoke_path)], cwd=directory, environment=environment)

        _run(
            [
                sys.executable,
                "-m",
                "mypy",
                "--config-file",
                str(TYPING_CONFIG),
                "--python-executable",
                str(python),
                str(TYPING_CONSUMER),
            ],
            cwd=directory,
            environment=environment,
        )


def main() -> None:
    version = _declared_version()
    if _runtime_version() != version:
        raise RuntimeError("pyproject.toml and bulklink.__version__ disagree")

    _verify_changelog(version)
    _verify_release_tag(version)

    wheel = _single_artifact("*.whl")
    sdist = _single_artifact("*.tar.gz")
    _verify_wheel(wheel, version)
    _verify_sdist(sdist)
    _verify_installed_wheel(wheel, version)
    print(f"verified {wheel.name} and {sdist.name}")


if __name__ == "__main__":
    main()
