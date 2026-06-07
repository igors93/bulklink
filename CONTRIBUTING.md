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

Run all checks with:

```bash
./scripts/ci.sh
```
