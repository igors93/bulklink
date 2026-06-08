from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = (
    "basic.py",
    "isolated_services.py",
    "overload_handling.py",
    "absolute_deadline.py",
    "interval_metrics.py",
    "weighted_capacity.py",
    "graceful_shutdown.py",
    "dynamic_capacity.py",
    "observability.py",
    "registry.py",
)


@pytest.mark.parametrize("filename", EXAMPLES)
def test_documented_example_runs_to_completion(filename: str) -> None:
    environment = os.environ.copy()
    source_path = str(ROOT / "src")
    existing = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        source_path if not existing else f"{source_path}{os.pathsep}{existing}"
    )
    environment["PYTHONUTF8"] = "1"

    result = subprocess.run(
        [sys.executable, str(ROOT / "examples" / filename)],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )

    assert result.returncode == 0, (
        f"example {filename} failed\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
