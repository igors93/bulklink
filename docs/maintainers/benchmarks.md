# Benchmarks

Bulklink benchmarks are dependency-free and record a baseline rather than enforcing a
fragile timing threshold.

Run the default suite:

```bash
python -m benchmarks.run
```

Use a smaller smoke run:

```bash
python -m benchmarks.run --iterations 200 --rounds 1 --waiters 100
```

Write machine-readable output:

```bash
python -m benchmarks.run --output benchmark-results.json
```

The suite measures direct async calls, `execute()`, `slot()`, FIFO handoff, lifecycle
events, status snapshots, and approximate memory allocated per queued waiter.

Compare results only on equivalent Python versions, operating systems, hardware, and
power settings. Do not reject a release from a single noisy timing sample. Investigate
persistent regressions across repeated runs instead.
