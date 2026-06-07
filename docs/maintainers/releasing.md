# Release checklist

1. Confirm the working tree contains only intended release changes.
2. Update `CHANGELOG.md` with the release date and final notes.
3. Update the version in `pyproject.toml` and `bulklink.__version__`.
4. Run `./scripts/ci.sh` from a clean development environment.
5. Confirm `scripts/verify_release.py` validates both artifacts and the installed wheel.
6. Confirm GitHub Actions passes on Python 3.10 through 3.14.
7. Inspect `dist/` and verify that it contains exactly one wheel and one source archive.
8. Confirm the wheel contains `bulklink/py.typed` and does not contain tests or caches.
9. Create and push the `vMAJOR.MINOR.PATCH` tag.
10. Publish the already verified files from `dist/`; do not rebuild them during upload.

## Version consistency

The project version is intentionally present in both `pyproject.toml` and
`bulklink.__version__`. Contract tests and the release verifier fail when the values do
not match.

## Distribution verification

The release verifier checks:

- safe archive paths and absence of cache files;
- SPDX license metadata and the packaged `LICENSE` file;
- inclusion of the PEP 561 `py.typed` marker;
- installation of the wheel into a temporary virtual environment;
- runtime use of execution, resizing, diagnostics, events, shutdown, and the registry;
- strict type checking of a consumer against the installed wheel rather than `src/`.
