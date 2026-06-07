# Contributing

## Principles

1. Keep the public API small.
2. Prefer explicit control flow over clever abstractions.
3. Keep mutable runtime state private.
4. Preserve FIFO admission.
5. Never leak an execution slot after exceptions or cancellation.
6. Never retry user code.
7. Keep Bulklink terminology separate from Relinker terminology.
8. Add behavioral tests before changing concurrency semantics.
9. Keep runtime dependencies at zero unless strongly justified.
10. Make production behavior explainable.

## Local verification

Install the development dependencies and run the same release-oriented checks used by
maintainers:

```bash
python -m pip install -e ".[dev]"
./scripts/ci.sh
```

The script verifies formatting, linting, strict typing, tests with coverage, source and
wheel builds, archive contents, installation into a clean virtual environment, a runtime
smoke test, and typing from the perspective of an installed consumer.

GitHub Actions additionally runs the full test suite on Python 3.10, 3.11, 3.12, 3.13,
and 3.14.
