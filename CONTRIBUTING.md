# Contributing

## Principles

1. Keep the public API small.
2. Prefer explicit control flow over clever abstractions.
3. Keep mutable runtime state private.
4. Preserve strict FIFO admission, including weighted costs.
5. Never leak an execution slot after exceptions or cancellation.
6. Never retry user code.
7. Keep Bulklink terminology separate from Relinker terminology.
8. Add behavioral tests before changing concurrency semantics.
9. Keep runtime dependencies at zero unless strongly justified.
10. Make production behavior explainable.
11. Weighted changes must preserve atomic cost allocation and prevent impossible queued work.

## Stable patch discipline

The `0.5.x` patch line preserves the documented public contract. Changes to root exports,
public enum values, frozen record fields, exception inheritance, or primary calling
conventions require a future minor release and matching contract documentation.

Release candidates freeze new public APIs. Candidate updates should focus on defects,
security, documentation, compatibility, and release validation.

## Local verification

Run all checks with:

```bash
./scripts/ci.sh
```

On Windows PowerShell, run the equivalent commands directly:

```powershell
python -m ruff format --check .
python -m ruff check .
python -m mypy src
python -m pytest --cov=bulklink --cov-report=term-missing
python -m benchmarks.run --iterations 200 --rounds 1 --waiters 100
python -m build
python scripts\verify_release.py
```

Model-oriented and stress tests must be deterministic, bounded by explicit timeouts,
and leave no pending work, queued entries, or allocated slots. Benchmarks record a
baseline and must not use noisy timing thresholds as release gates.
