# Release checklist

1. Update `CHANGELOG.md`.
2. Update the version in `pyproject.toml` and `bulklink.__version__`.
3. Run `./scripts/ci.sh`.
4. Inspect the wheel contents.
5. Install the wheel in a clean environment.
6. Verify documented root imports.
7. Tag `vMAJOR.MINOR.PATCH`.
8. Publish to PyPI.
